package catalog

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/planner"
)

type State struct {
	Revision          int64
	ActiveGeneration  int64
	Through           int64
	ReplayWatermark   *int64
	PendingGeneration int64
	PendingWatermark  int64
	PendingCursor     string
	PendingRows       int64
	Rebuild           bool
	RejectedWatermark int64
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

const selectState = `SELECT revision, active_generation, applied_through, replay_watermark,
	pending_generation, pending_watermark, pending_cursor, pending_rows, rebuild, rejected_watermark
	FROM catalog_projection_state WHERE singleton`

func scanState(row pgx.Row) (State, error) {
	var st State
	err := row.Scan(&st.Revision, &st.ActiveGeneration, &st.Through, &st.ReplayWatermark, &st.PendingGeneration,
		&st.PendingWatermark, &st.PendingCursor, &st.PendingRows, &st.Rebuild, &st.RejectedWatermark)
	return st, err
}

func (s *Store) State(ctx context.Context) (State, error) {
	return scanState(s.pool.QueryRow(ctx, selectState))
}

func saveState(ctx context.Context, tx pgx.Tx, previous, st State) error {
	for _, generation := range []int64{previous.ActiveGeneration, previous.PendingGeneration} {
		if generation != 0 && generation != st.ActiveGeneration && generation != st.PendingGeneration {
			if _, err := tx.Exec(ctx, "INSERT INTO catalog_projection_retired(generation) VALUES ($1)", generation); err != nil {
				return err
			}
		}
	}
	_, err := tx.Exec(ctx, `UPDATE catalog_projection_state SET revision=revision+1,
		active_generation=$1, applied_through=$2, replay_watermark=$3, pending_generation=$4,
		pending_watermark=$5, pending_cursor=$6, pending_rows=$7, rebuild=$8, rejected_watermark=$9
		WHERE singleton`, st.ActiveGeneration, st.Through, st.ReplayWatermark, st.PendingGeneration,
		st.PendingWatermark, st.PendingCursor, st.PendingRows, st.Rebuild, st.RejectedWatermark)
	return err
}

func (st *State) clearPending() {
	st.PendingGeneration, st.PendingWatermark, st.PendingRows = 0, 0, 0
	st.PendingCursor = ""
}

type applyPage func(pgx.Tx, *State) (bool, error)

func (s *Store) apply(ctx context.Context, expected State, phase string, apply applyPage) (bool, error) {
	var progress bool
	var rejectedPage *rejected
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		st, err := scanState(tx.QueryRow(ctx, selectState+" FOR UPDATE"))
		if err != nil {
			return err
		}
		if st.Revision != expected.Revision {
			return ErrConcurrent
		}
		next := st
		// Savepoint отделяет откат страницы от записи причины отказа.
		err = pgx.BeginFunc(ctx, tx, func(pageTx pgx.Tx) error {
			var err error
			progress, err = apply(pageTx, &next)
			return err
		})
		if errors.As(err, &rejectedPage) {
			_, err = tx.Exec(ctx, `INSERT INTO catalog_projection_quarantine(phase,revision,event_id,reason)
				VALUES ($1,$2,NULLIF($3,'')::uuid,$4)`, phase, rejectedPage.revision, rejectedPage.eventID, rejectedPage.reason)
			if err != nil {
				return err
			}
			next = st
			next.Rebuild = true
			next.ReplayWatermark = nil
			next.clearPending()
			floor := rejectedPage.revision
			if phase == "replay" {
				floor--
			}
			next.RejectedWatermark = max(st.RejectedWatermark, floor)
			return saveState(ctx, tx, st, next)
		}
		if err != nil || !progress {
			return err
		}
		return saveState(ctx, tx, st, next)
	})
	if err != nil {
		return false, err
	}
	if rejectedPage != nil {
		return false, rejectedPage
	}
	return progress, nil
}

const upsertTopic = `INSERT INTO catalog_projected_topics(generation,topic_id,revision,tombstone,metadata)
	VALUES ($1,$2,$3,$4,$5)
	ON CONFLICT (generation,topic_id) DO UPDATE
	SET revision=excluded.revision,tombstone=excluded.tombstone,metadata=excluded.metadata
	WHERE $6 AND catalog_projected_topics.revision < excluded.revision`

const upsertPrerequisite = `INSERT INTO catalog_projected_prerequisites(generation,topic_id,prerequisite_id,revision,tombstone)
	VALUES ($1,$2,$3,$4,$5)
	ON CONFLICT (generation,topic_id,prerequisite_id) DO UPDATE
	SET revision=excluded.revision,tombstone=excluded.tombstone
	WHERE $6 AND catalog_projected_prerequisites.revision < excluded.revision`

func recordQuery(generation int64, row Row, replay bool) (string, []any, error) {
	r := row.Record
	if r.Kind == "prerequisite" {
		return upsertPrerequisite, []any{generation, r.TopicID, r.PrerequisiteID, row.Revision, r.Tombstone, replay}, nil
	}
	var metadata []byte
	if r.Topic != nil {
		var err error
		metadata, err = json.Marshal(r.Topic)
		if err != nil {
			return "", nil, err
		}
	}
	return upsertTopic, []any{generation, r.TopicID, row.Revision, r.Tombstone, metadata, replay}, nil
}

func (s *Store) Topic(ctx context.Context, id string) (Topic, error) {
	if !planner.ValidID(id) {
		return Topic{}, ErrNotFound
	}
	var generation int64
	var topic *Topic
	err := s.pool.QueryRow(ctx, `SELECT s.active_generation,t.metadata FROM catalog_projection_state s
		LEFT JOIN catalog_projected_topics t ON t.generation=s.active_generation AND t.topic_id=$1 AND NOT t.tombstone
		WHERE s.singleton`, id).Scan(&generation, &topic)
	if err != nil {
		return Topic{}, err
	}
	if generation == 0 {
		return Topic{}, ErrNotReady
	}
	if topic == nil {
		return Topic{}, ErrNotFound
	}
	return *topic, nil
}

const zeroUUID = "00000000-0000-0000-0000-000000000000"

func (s *Store) Prerequisites(ctx context.Context, id, after string, limit int) ([]string, error) {
	if !planner.ValidID(id) || (after != "" && !planner.ValidID(after)) || limit < 1 || limit > PageLimit {
		return nil, ErrNotFound
	}
	if after == "" {
		after = zeroUUID
	}
	rows, err := s.pool.Query(ctx, `SELECT s.active_generation,t.topic_id IS NOT NULL,e.prerequisite_id::text
		FROM catalog_projection_state s
		LEFT JOIN catalog_projected_topics t ON t.generation=s.active_generation AND t.topic_id=$1 AND NOT t.tombstone
		LEFT JOIN LATERAL (
			SELECT e.prerequisite_id FROM catalog_projected_prerequisites e
			JOIN catalog_projected_topics p ON p.generation=e.generation AND p.topic_id=e.prerequisite_id
			WHERE e.generation=t.generation AND e.topic_id=t.topic_id AND e.prerequisite_id>$2
			  AND NOT (e.tombstone OR p.tombstone)
			ORDER BY e.prerequisite_id LIMIT $3
		) e ON true WHERE s.singleton ORDER BY e.prerequisite_id`, id, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]string, 0, limit)
	for rows.Next() {
		var generation int64
		var exists bool
		var prerequisite *string
		if err := rows.Scan(&generation, &exists, &prerequisite); err != nil {
			return nil, err
		}
		if generation == 0 {
			return nil, ErrNotReady
		}
		if !exists {
			return nil, ErrNotFound
		}
		if prerequisite != nil {
			result = append(result, *prerequisite)
		}
	}
	return result, rows.Err()
}

func (s *Store) Prune(ctx context.Context) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var generation int64
		err := tx.QueryRow(ctx, `SELECT generation FROM catalog_projection_retired
			ORDER BY generation LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&generation)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		var remaining bool
		for _, query := range []string{
			`DELETE FROM catalog_projected_prerequisites WHERE ctid IN (
				SELECT ctid FROM catalog_projected_prerequisites WHERE generation=$1
				ORDER BY topic_id,prerequisite_id LIMIT 500)`,
			`DELETE FROM catalog_projected_topics WHERE ctid IN (
				SELECT ctid FROM catalog_projected_topics WHERE generation=$1
				ORDER BY topic_id LIMIT 500)`,
		} {
			tag, err := tx.Exec(ctx, query, generation)
			if err != nil {
				return err
			}
			remaining = remaining || tag.RowsAffected() == 500
		}
		if remaining {
			return nil
		}
		_, err = tx.Exec(ctx, "DELETE FROM catalog_projection_retired WHERE generation=$1", generation)
		return err
	})
}
