package resources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	errConcurrent    = errors.New("resource checkpoint changed")
	errNotReady      = errors.New("resource projection not initialized")
	errEventConflict = errors.New("conflicting resource event")
)

const maxSnapshotRows = 1000000
const pruneLimit = 500

type KafkaPosition struct {
	TopicID string          `json:"topic_id"`
	Offsets map[int32]int64 `json:"offsets"`
}

type checkpointProgress struct {
	Pending       int64          `json:"pending,omitempty"`
	Watermark     *int64         `json:"watermark,omitempty"`
	Cursor        string         `json:"cursor,omitempty"`
	Count         int            `json:"count,omitempty"`
	Floor         int64          `json:"floor,omitempty"`
	Through       int64          `json:"through,omitempty"`
	ReplayCeiling *int64         `json:"replay_ceiling,omitempty"`
	Kafka         *KafkaPosition `json:"kafka,omitempty"`
}

type checkpoint struct {
	Origin   Origin
	Revision int64
	Active   int64
	checkpointProgress
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

const checkpointSQL = "SELECT revision, active_generation, progress FROM resource_projection_checkpoints WHERE origin = $1"

func scanCheckpoint(row pgx.Row, origin Origin) (checkpoint, error) {
	out := checkpoint{Origin: origin}
	var raw []byte
	if err := row.Scan(&out.Revision, &out.Active, &raw); err != nil {
		return checkpoint{}, fmt.Errorf("read resource checkpoint: %w", err)
	}
	if err := json.Unmarshal(raw, &out.checkpointProgress); err != nil {
		return checkpoint{}, fmt.Errorf("decode resource checkpoint: %w", err)
	}
	return out, nil
}

func (s *Store) load(ctx context.Context, origin Origin) (checkpoint, error) {
	return scanCheckpoint(s.pool.QueryRow(ctx, checkpointSQL, origin), origin)
}

func (s *Store) write(ctx context.Context, expected checkpoint, apply func(pgx.Tx, *checkpoint) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		current, err := scanCheckpoint(tx.QueryRow(ctx, checkpointSQL+" FOR UPDATE", expected.Origin), expected.Origin)
		if err != nil {
			return err
		}
		if current.Revision != expected.Revision {
			return errConcurrent
		}
		if err := apply(tx, &current); err != nil {
			return err
		}
		progress, err := json.Marshal(current.checkpointProgress)
		if err != nil {
			return fmt.Errorf("encode resource checkpoint: %w", err)
		}
		_, err = tx.Exec(ctx, `
			UPDATE resource_projection_checkpoints
			SET revision = revision + 1, active_generation = $2, progress = $3
			WHERE origin = $1`, current.Origin, current.Active, progress)
		return err
	})
}

func (s *Store) beginSnapshot(ctx context.Context, expected checkpoint, position *KafkaPosition) error {
	if (expected.Origin == Content) != (position != nil) {
		return errInvalidPage
	}
	return s.write(ctx, expected, func(tx pgx.Tx, current *checkpoint) error {
		if err := retireGeneration(ctx, tx, current.Origin, current.Pending); err != nil {
			return err
		}
		current.checkpointProgress = checkpointProgress{Pending: current.Revision + 1, Kafka: position}
		return nil
	})
}

func (s *Store) snapshotPage(ctx context.Context, expected checkpoint, page SnapshotPage) error {
	if expected.Pending == 0 {
		return errNotReady
	}
	if page.Watermark < 0 || expected.Count+len(page.Records) > maxSnapshotRows {
		return errInvalidPage
	}
	if err := validateSnapshot(page, expected.Watermark, expected.Cursor); err != nil {
		return err
	}
	return s.write(ctx, expected, func(tx pgx.Tx, current *checkpoint) error {
		records := make(map[string]Record, len(page.Records))
		for _, row := range page.Records {
			normalized, err := row.normalized(current.Origin)
			if err != nil {
				return err
			}
			records[normalized.ID] = normalized
		}
		batch := &pgx.Batch{}
		if err := queueRecords(batch, current.Origin, current.Pending, records); err != nil {
			return err
		}
		if err := sendResourceBatch(ctx, tx, batch); err != nil {
			return err
		}
		if page.NextCursor != "" {
			current.Watermark, current.Cursor = &page.Watermark, page.NextCursor
			current.Count += len(page.Records)
			return nil
		}
		if err := retireGeneration(ctx, tx, current.Origin, current.Active); err != nil {
			return err
		}
		current.Active, current.Pending = current.Pending, 0
		current.Floor, current.Through = page.Watermark, page.Watermark
		current.Watermark, current.Cursor, current.Count = nil, "", 0
		return nil
	})
}

const writeResourceSQL = `
	WITH updated AS (
		INSERT INTO resource_projection_items (origin, generation, resource_id, version, revision_id, metadata)
		VALUES ($1, $2, $3, $4, NULLIF($5, '')::uuid, $6::jsonb)
		ON CONFLICT (origin, generation, resource_id) DO UPDATE SET
		    version = EXCLUDED.version, revision_id = EXCLUDED.revision_id, metadata = EXCLUDED.metadata
		WHERE resource_projection_items.version < EXCLUDED.version
		RETURNING resource_id
	), removed AS (
		DELETE FROM resource_projection_topics AS t USING updated AS u
		WHERE t.origin = $1 AND t.generation = $2 AND t.resource_id = u.resource_id
		    AND NOT (t.topic_id = ANY($7::uuid[]))
	)
	INSERT INTO resource_projection_topics (origin, generation, resource_id, topic_id, weight)
	SELECT $1, $2, u.resource_id, t.id, t.weight
	FROM updated AS u CROSS JOIN unnest($7::uuid[], $8::integer[]) AS t(id, weight)
	ON CONFLICT (origin, generation, resource_id, topic_id) DO UPDATE SET weight = EXCLUDED.weight`

func queueRecords(batch *pgx.Batch, origin Origin, generation int64, records map[string]Record) error {
	for _, record := range records {
		var revision string
		var payload []byte
		ids, weights := []string{}, []int{}
		if m := record.Metadata; m != nil {
			revision = m.RevisionID
			var err error
			payload, err = json.Marshal(m)
			if err != nil {
				return fmt.Errorf("encode resource metadata: %w", err)
			}
			for _, topic := range m.Topics {
				ids, weights = append(ids, topic.ID), append(weights, topic.Weight)
			}
		}
		batch.Queue(writeResourceSQL, origin, generation, record.ID, record.Version, revision, payload, ids, weights)
	}
	return nil
}

func sendResourceBatch(ctx context.Context, tx pgx.Tx, batch *pgx.Batch) error {
	if batch.Len() == 0 {
		return nil
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("write resource batch: %w", err)
	}
	return nil
}

func (s *Store) prune(ctx context.Context, origin Origin) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var generation int64
		err := tx.QueryRow(ctx, `
			SELECT generation FROM resource_projection_retired WHERE origin = $1
			ORDER BY generation LIMIT 1 FOR UPDATE SKIP LOCKED`, origin).Scan(&generation)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("load retired resource generation: %w", err)
		}
		items, err := tx.Exec(ctx, `
			DELETE FROM resource_projection_items
			WHERE origin = $1 AND generation = $2 AND resource_id IN (
			    SELECT resource_id FROM resource_projection_items WHERE origin = $1 AND generation = $2
			    ORDER BY resource_id LIMIT $3)`, origin, generation, pruneLimit)
		if err != nil {
			return fmt.Errorf("prune retired resources: %w", err)
		}
		inbox, err := tx.Exec(ctx, `
			DELETE FROM resource_projection_inbox
			WHERE origin = $1 AND generation = $2 AND event_id IN (
			    SELECT event_id FROM resource_projection_inbox WHERE origin = $1 AND generation = $2
			    ORDER BY event_id LIMIT $3)`, origin, generation, pruneLimit)
		if err != nil {
			return fmt.Errorf("prune retired resource events: %w", err)
		}
		if items.RowsAffected() < pruneLimit && inbox.RowsAffected() < pruneLimit {
			_, err = tx.Exec(ctx, "DELETE FROM resource_projection_retired WHERE origin = $1 AND generation = $2", origin, generation)
		}
		return err
	})
}

func retireGeneration(ctx context.Context, tx pgx.Tx, origin Origin, generation int64) error {
	if generation == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, "INSERT INTO resource_projection_retired (origin, generation) VALUES ($1, $2)", origin, generation)
	if err != nil {
		return fmt.Errorf("retire resource generation: %w", err)
	}
	return nil
}
