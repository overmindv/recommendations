package consumer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/mastery"
)

const batchLimit = 32

type Postgres struct {
	pool         *pgxpool.Pool
	rebuildDelay time.Duration
	baselines    *mastery.Postgres
}

func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

func NewPostgresWithPlanChanges(pool *pgxpool.Pool, delay time.Duration, baselines *mastery.Postgres) (*Postgres, error) {
	if delay < time.Second || delay > 5*time.Minute || baselines == nil {
		return nil, errors.New("invalid study plan change configuration")
	}

	return &Postgres{pool: pool, rebuildDelay: delay, baselines: baselines}, nil
}

const inboxSQL = `
	INSERT INTO learning_evidence_inbox (source,event_id,fingerprint) VALUES ($1,$2,$3)
	ON CONFLICT DO NOTHING RETURNING event_id`

const outcomeSQL = `
	WITH new_event AS (` + inboxSQL + `), applied AS (
	    INSERT INTO learning_task_outcomes AS o (event_id,submission_id,learner_id,task_id,task_version_id,
	                                             topic_ids,verdict,evidence_kind,graded_at,correction_version,topic_weights)
	    SELECT event_id,$4::uuid,$5::uuid,$6::uuid,$7::uuid,$8::uuid[],$9::text,$10::text,$11::timestamptz,$12::bigint,
	           (SELECT array_agg(t.weight ORDER BY t.topic_id)
	            FROM resource_projection_checkpoints AS c
	            JOIN resource_projection_items AS i
	              ON i.origin = c.origin AND i.generation = c.active_generation
	             AND i.resource_id = $6::uuid AND i.revision_id = $7::uuid
	            JOIN resource_projection_topics AS t
	              ON t.origin = i.origin AND t.generation = i.generation AND t.resource_id = i.resource_id
	            WHERE c.origin = 'tasks' AND c.active_generation > 0 AND t.topic_id = ANY($8::uuid[])
	            HAVING count(*) = cardinality($8::uuid[]))
	    FROM new_event
	    ON CONFLICT (submission_id) DO UPDATE
	    SET event_id=EXCLUDED.event_id, verdict=EXCLUDED.verdict, evidence_kind=EXCLUDED.evidence_kind,
	        graded_at=EXCLUDED.graded_at, correction_version=EXCLUDED.correction_version
	    WHERE o.correction_version < EXCLUDED.correction_version
	      AND (o.learner_id,o.task_id,o.task_version_id,o.topic_ids)
	          = (EXCLUDED.learner_id,EXCLUDED.task_id,EXCLUDED.task_version_id,EXCLUDED.topic_ids)
	    RETURNING 1
	)
	SELECT EXISTS (SELECT 1 FROM new_event), EXISTS (SELECT 1 FROM applied)`

const materialSQL = `
	WITH new_event AS (` + inboxSQL + `), applied AS (
	    INSERT INTO learning_material_reads AS r (event_id,learner_id,material_id,revision_id,confirmed_at)
	    SELECT event_id,$4::uuid,$5::uuid,$6::uuid,$7::timestamptz FROM new_event
	    ON CONFLICT (learner_id,material_id,revision_id) DO UPDATE
	    SET event_id=EXCLUDED.event_id, confirmed_at=EXCLUDED.confirmed_at
	    WHERE (EXCLUDED.confirmed_at,EXCLUDED.event_id) < (r.confirmed_at,r.event_id)
	    RETURNING 1
	)
	SELECT EXISTS (SELECT 1 FROM new_event), EXISTS (SELECT 1 FROM applied)`

const rejectSQL = `
	INSERT INTO learning_evidence_rejections (topic,partition,message_offset,reason) VALUES ($1,$2,$3,$4)
	ON CONFLICT DO NOTHING`

func (p *Postgres) apply(ctx context.Context, messages []message) error {
	if len(messages) < 1 || len(messages) > batchLimit {
		return errors.New("invalid evidence batch size")
	}

	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		checks, submissions, err := applyBatch(ctx, tx, messages, p.rebuildDelay > 0)
		if err != nil {
			return fmt.Errorf("write evidence batch: %w", err)
		}
		for _, check := range checks {
			if err := check.verify(ctx, tx); err != nil {
				return fmt.Errorf("check evidence replay: %w", err)
			}
		}
		if len(submissions) != 0 {
			if rebuildErr := updateLearningPlans(ctx, tx, submissions, p.rebuildDelay, p.baselines); rebuildErr != nil {
				return fmt.Errorf("apply evidence to study plans: %w", rebuildErr)
			}
		}

		return nil
	})
}

type replayCheck struct {
	msg       message
	duplicate bool
}

func applyBatch(ctx context.Context, tx pgx.Tx, messages []message, collect bool) ([]replayCheck, []string, error) {
	batch := &pgx.Batch{}
	for _, msg := range messages {
		switch {
		case msg.reject != "":
			batch.Queue(rejectSQL, msg.topic, msg.partition, msg.offset, msg.reject)
		case msg.outcome != nil:
			p := msg.outcome
			batch.Queue(outcomeSQL, msg.source, msg.id, msg.fingerprint[:], p.SubmissionID, p.LearnerID, p.TaskID,
				p.VersionID, p.Topics, p.Verdict, p.kind(), p.GradedAt, p.Correction)
		default:
			r := msg.read
			batch.Queue(materialSQL, msg.source, msg.id, msg.fingerprint[:], r.LearnerID, r.MaterialID, r.RevisionID, r.At)
		}
	}
	results := tx.SendBatch(ctx, batch)
	defer func() { _ = results.Close() }()
	var checks []replayCheck
	var submissions []string
	for _, msg := range messages {
		if msg.reject != "" {
			if _, err := results.Exec(); err != nil {
				return nil, nil, err
			}

			continue
		}
		var inserted, applied bool
		if err := results.QueryRow().Scan(&inserted, &applied); err != nil {
			return nil, nil, err
		}
		if !inserted || (msg.outcome != nil && !applied) {
			checks = append(checks, replayCheck{msg, !inserted})
		}
		if collect && applied && msg.outcome != nil && (msg.outcome.kind() == independentEvidence || msg.outcome.Correction > 1) {
			submissions = append(submissions, msg.outcome.SubmissionID)
		}
	}

	return checks, submissions, results.Close()
}

func (c replayCheck) verify(ctx context.Context, tx pgx.Tx) error {
	msg := c.msg
	reason := ""
	if c.duplicate {
		var digest []byte
		const query = `SELECT fingerprint FROM learning_evidence_inbox WHERE source=$1 AND event_id=$2`
		if err := tx.QueryRow(ctx, query, msg.source, msg.id).Scan(&digest); err != nil {
			return err
		}
		if !bytes.Equal(digest, msg.fingerprint[:]) {
			reason = "event_id_conflict"
		}
	} else {
		p := msg.outcome
		const query = `
			SELECT (learner_id,task_id,task_version_id,topic_ids) = ($2::uuid,$3::uuid,$4::uuid,$5::uuid[])
			   AND (correction_version > $6 OR (correction_version = $6 AND (verdict,evidence_kind,graded_at) = ($7,$8,$9)))
			FROM learning_task_outcomes WHERE submission_id=$1`
		var consistent bool
		if err := tx.QueryRow(ctx, query, p.SubmissionID, p.LearnerID, p.TaskID, p.VersionID, p.Topics,
			p.Correction, p.Verdict, p.kind(), p.GradedAt).Scan(&consistent); err != nil {
			return err
		}
		if !consistent {
			reason = "outcome_conflict"
		}
	}
	if reason != "" {
		_, err := tx.Exec(ctx, rejectSQL, msg.topic, msg.partition, msg.offset, reason)

		return err
	}

	return nil
}
