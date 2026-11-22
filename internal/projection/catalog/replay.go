package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/planner"
)

func (s *Store) ApplyChanges(ctx context.Context, expected State, page ChangePage) (bool, error) {
	return s.apply(ctx, expected, "replay", func(tx pgx.Tx, st *State) (bool, error) {
		if st.Rebuild || st.ActiveGeneration == 0 {
			return false, ErrConcurrent
		}
		if page.Watermark == nil || page.Through == nil || page.Changes == nil || len(page.Changes) > PageLimit {
			return false, &rejected{revision: st.Through, reason: "invalid_replay_page"}
		}
		w, through := *page.Watermark, *page.Through
		if through < st.Through || through > w ||
			(st.ReplayWatermark != nil && w != *st.ReplayWatermark) ||
			(page.More && (through == st.Through || through >= w)) || (!page.More && through != w) {
			return false, &rejected{revision: st.Through, reason: "invalid_replay_checkpoint"}
		}
		events := slices.Clone(page.Changes)
		sort.SliceStable(events, func(i, j int) bool { return events[i].Revision < events[j].Revision })
		if page.More && (len(events) == 0 || events[len(events)-1].Revision != through) {
			return false, &rejected{revision: st.Through, reason: "incomplete_replay_page"}
		}
		if err := applyEvents(ctx, tx, *st, through, events); err != nil {
			return false, err
		}
		next := through != st.Through || st.ReplayWatermark != nil
		st.Through = through
		st.ReplayWatermark = nil
		if page.More {
			st.ReplayWatermark = &w
		} else if _, err := tx.Exec(ctx, `
			UPDATE catalog_projection_state SET last_synced_at=statement_timestamp()
			WHERE singleton AND (last_synced_at IS NULL OR last_synced_at<statement_timestamp()-interval '5 seconds')`); err != nil {
			return false, err
		}

		return next, nil
	})
}

type edgeKey struct {
	topic  string
	target string
}

func applyEvents(ctx context.Context, tx pgx.Tx, st State, through int64, events []Event) error {
	batch := &pgx.Batch{}
	seen := make(map[string][sha256.Size]byte, len(events))
	queued := make([]Event, 0, len(events))
	for _, event := range events {
		if event.Revision < 1 || event.Revision > through {
			return &rejected{revision: st.Through, reason: "invalid_event_revision"}
		}
		if event.Revision <= st.Through {
			continue
		}
		if !planner.ValidID(event.ID) || event.AggregateVersion < 1 {
			return &rejected{revision: event.Revision, reason: "invalid_event_identity"}
		}
		if err := event.Record.validate(); err != nil {
			return markRejected(err, event.Revision, event.ID)
		}
		body, err := json.Marshal(event)
		if err != nil {
			return err
		}
		fingerprint := sha256.Sum256(body)
		if previous, ok := seen[event.ID]; ok {
			if previous != fingerprint {
				return markRejected(invalid("conflicting_event_identity"), event.Revision, event.ID)
			}

			continue
		}
		seen[event.ID] = fingerprint
		queued = append(queued, event)
		batch.Queue(`INSERT INTO catalog_projection_inbox(event_id,revision,fingerprint)
			VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, event.ID, event.Revision, fingerprint[:])

		r := event.Record
		if r.Kind == kindTopic && !r.Tombstone {
			batch.Queue(`SELECT COALESCE((SELECT tombstone FROM catalog_projected_topics
				WHERE generation=$1 AND topic_id=$2 AND revision<$3),false)`, st.ActiveGeneration, r.TopicID, event.Revision)
		}
		query, args, err := recordQuery(st.ActiveGeneration, event.Row, true)
		if err != nil {
			return err
		}
		batch.Queue(query, args...)
	}
	if len(queued) == 0 {
		return nil
	}
	edges, restored, err := writeEvents(ctx, tx, batch, queued)
	if err != nil {
		return err
	}
	if restored != nil {
		return markRejected(validateGraph(ctx, tx, st.ActiveGeneration), restored.Revision, restored.ID)
	}
	toCheck := make([]Event, 0, len(edges))
	for _, event := range queued {
		r := event.Record
		if r.Kind == kindPrerequisite && !r.Tombstone &&
			edges[edgeKey{r.TopicID, r.PrerequisiteID}] == event.Revision {
			toCheck = append(toCheck, event)
		}
	}

	return validateEdges(ctx, tx, st.ActiveGeneration, toCheck)
}

func writeEvents(ctx context.Context, tx pgx.Tx, batch *pgx.Batch, queued []Event) (map[edgeKey]int64, *Event, error) {
	results := tx.SendBatch(ctx, batch)
	defer func() { _ = results.Close() }()
	edges := make(map[edgeKey]int64)
	var restored *Event
	for _, event := range queued {
		tag, err := results.Exec()
		if err != nil {
			return nil, nil, err
		}
		if tag.RowsAffected() != 1 {
			return nil, nil, markRejected(invalid("conflicting_event_identity"), event.Revision, event.ID)
		}
		r := event.Record
		if r.Kind == kindTopic && !r.Tombstone {
			var tombstone bool
			if scanErr := results.QueryRow().Scan(&tombstone); scanErr != nil {
				return nil, nil, scanErr
			}
			if tombstone {
				restored = &event
			}
		}
		tag, err = results.Exec()
		if err != nil {
			return nil, nil, err
		}
		if r.Kind == kindPrerequisite && tag.RowsAffected() == 1 {
			edges[edgeKey{r.TopicID, r.PrerequisiteID}] = event.Revision
		}
	}

	return edges, restored, results.Close()
}
