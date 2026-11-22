package resources

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/overmindv/recommendations/internal/planner"
)

func (s *Store) applyTasks(ctx context.Context, expected checkpoint, page ChangePage) error {
	if expected.Origin != Tasks || expected.Pending != 0 || expected.Active == 0 {
		return errNotReady
	}
	if len(page.Events) > pageLimit || page.Through < expected.Through ||
		(page.More && page.Through >= page.Watermark) || (!page.More && page.Through != page.Watermark) ||
		(expected.ReplayCeiling != nil && page.Watermark != *expected.ReplayCeiling) {
		return errInvalidPage
	}
	last := expected.Through
	for _, event := range page.Events {
		if event.Record.Version <= last || event.Record.Version > page.Through {
			return errInvalidPage
		}
		last = event.Record.Version
	}
	if page.More && (len(page.Events) == 0 || last != page.Through) {
		return errInvalidPage
	}
	if page.Through == expected.Through && len(page.Events) == 0 && expected.ReplayCeiling == nil && !expected.Lagging {
		return touchProjection(ctx, s.pool, expected, 0)
	}

	return s.write(ctx, expected, func(tx pgx.Tx, current *checkpoint) error {
		if err := applyEvents(ctx, tx, *current, page.Events); err != nil {
			return err
		}
		current.Through, current.ReplayCeiling = page.Through, nil
		current.Lagging = page.More
		if page.More {
			current.ReplayCeiling = &page.Watermark

			return nil
		}

		return touchProjection(ctx, tx, *current, 0)
	})
}

func (s *Store) applyContent(ctx context.Context, expected checkpoint, next KafkaPosition, events []Event, observedAt time.Time) error {
	if expected.Origin != Content || expected.Active == 0 || expected.Pending != 0 || expected.Kafka == nil {
		return errNotReady
	}
	if len(events) > pageLimit || next.TopicID != expected.Kafka.TopicID || len(next.Offsets) != len(expected.Kafka.Offsets) {
		return errInvalidPage
	}
	for partition, prior := range expected.Kafka.Offsets {
		if pos, ok := next.Offsets[partition]; !ok || pos < prior {
			return errInvalidPage
		}
	}
	same := maps.Equal(next.Offsets, expected.Kafka.Offsets)
	lagging := observedAt.IsZero()
	if same {
		if len(events) != 0 {
			return errInvalidPage
		}
		if expected.Lagging == lagging {
			if lagging {
				return nil
			}

			return touchProjection(ctx, s.pool, expected, time.Since(observedAt))
		}
	}

	return s.write(ctx, expected, func(tx pgx.Tx, current *checkpoint) error {
		if err := applyEvents(ctx, tx, *current, events); err != nil {
			return err
		}
		current.Kafka = &next
		current.Lagging = lagging
		if !lagging {
			return touchProjection(ctx, tx, *current, time.Since(observedAt))
		}

		return nil
	})
}

type projectionExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func touchProjection(ctx context.Context, writer projectionExecer, st checkpoint, age time.Duration) error {
	_, err := writer.Exec(ctx, `
		UPDATE resource_projection_checkpoints
		SET last_synced_at=statement_timestamp()-$3::bigint*interval '1 microsecond'
		WHERE origin=$1 AND revision=$2 AND (last_synced_at IS NULL OR
		    last_synced_at<statement_timestamp()-$3::bigint*interval '1 microsecond'-interval '5 seconds')`,
		st.Origin, st.Revision, max(0, age.Microseconds()))

	return err
}

type eventIdentity struct {
	ID          string
	Version     int64
	Fingerprint [32]byte
}

func applyEvents(ctx context.Context, tx pgx.Tx, state checkpoint, incoming []Event) error {
	batch := &pgx.Batch{}
	events := make([]Event, 0, len(incoming))
	ids, versions := make([]string, 0, len(incoming)), make([]int64, 0, len(incoming))
	for _, event := range incoming {
		if !planner.ValidID(event.ID) || event.Record.Version < 1 {
			return errInvalidEvent
		}
		record, err := event.Record.normalized(state.Origin)
		if err != nil {
			return err
		}
		if record.Version <= state.Floor {
			continue
		}
		event.Record = record
		events = append(events, event)
		ids, versions = append(ids, event.ID), append(versions, record.Version)
	}
	if len(events) == 0 {
		return nil
	}
	prior, err := existingIdentities(ctx, tx, state, ids, versions)
	if err != nil {
		return err
	}
	byID := make(map[string]eventIdentity, len(prior)+len(events))
	byVersion := make(map[int64]eventIdentity, len(prior)+len(events))
	for _, identity := range prior {
		byID[identity.ID], byVersion[identity.Version] = identity, identity
	}

	const insertInbox = `
		INSERT INTO resource_projection_inbox (origin, generation, event_id, resource_version, fingerprint)
		VALUES ($1, $2, $3, $4, $5)`
	records := make(map[string]Record, len(events))
	for _, event := range events {
		identity := eventIdentity{ID: event.ID, Version: event.Record.Version, Fingerprint: event.Fingerprint}
		if existing, ok := byID[event.ID]; ok {
			if existing != identity {
				return errEventConflict
			}

			continue
		}
		if _, ok := byVersion[identity.Version]; ok {
			return errEventConflict
		}
		byID[identity.ID], byVersion[identity.Version] = identity, identity
		batch.Queue(insertInbox, state.Origin, state.Active, identity.ID, identity.Version, identity.Fingerprint[:])
		// В одной пачке применяется только последняя версия каждого ресурса.
		if previous, ok := records[event.Record.ID]; !ok || previous.Version < identity.Version {
			records[event.Record.ID] = event.Record
		}
	}
	if err := queueRecords(batch, state.Origin, state.Active, records); err != nil {
		return err
	}

	return sendResourceBatch(ctx, tx, batch)
}

func existingIdentities(ctx context.Context, tx pgx.Tx, state checkpoint, ids []string, versions []int64) ([]eventIdentity, error) {
	const query = `
		SELECT event_id::text, resource_version, fingerprint
		FROM resource_projection_inbox
		WHERE origin = $1 AND generation = $2 AND (event_id = ANY($3::uuid[]) OR resource_version = ANY($4::bigint[]))`
	rows, err := tx.Query(ctx, query, state.Origin, state.Active, ids, versions)
	if err != nil {
		return nil, fmt.Errorf("load resource event identities: %w", err)
	}

	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (eventIdentity, error) {
		var identity eventIdentity
		var fingerprint []byte
		err := row.Scan(&identity.ID, &identity.Version, &fingerprint)
		copy(identity.Fingerprint[:], fingerprint)

		return identity, err
	})
}
