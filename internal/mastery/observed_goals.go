package mastery

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

const observedSnapshotBatch = 8

type observedGoal struct {
	id      string
	actor   string
	version int64
	topics  []string
}

func (p *Postgres) RefreshObservedGoals(ctx context.Context, tx pgx.Tx, ids []string) ([]string, error) {
	if len(ids) == 0 || len(ids) > 1024 {
		return nil, errors.New("invalid affected goal count")
	}
	rows, err := tx.Query(ctx, `
		SELECT id::text,user_id::text,version,target_topics FROM learner_goals
		WHERE id=ANY($1::uuid[]) AND active ORDER BY user_id,id FOR UPDATE`, ids)
	if err != nil {
		return nil, err
	}
	goals, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (observedGoal, error) {
		var g observedGoal
		scanErr := row.Scan(&g.id, &g.actor, &g.version, &g.topics)

		return g, scanErr
	})
	if err != nil || len(goals) == 0 {
		return nil, err
	}
	var computed time.Time
	if timestampErr := tx.QueryRow(ctx, "SELECT statement_timestamp()").Scan(&computed); timestampErr != nil {
		return nil, timestampErr
	}
	batch := &pgx.Batch{}
	active := make([]string, 0, len(goals))
	var samples []evidence
	var actor string
	var truncated bool
	for _, g := range goals {
		if g.actor != actor {
			actor = g.actor
			clear(samples)
			samples, truncated, err = loadEvidenceInto(ctx, tx, actor, computed, samples[:0])
			if err != nil {
				return nil, err
			}
		}
		scores, used := summarize(p.policy, g.topics, samples, truncated)
		topics, marshalErr := json.Marshal(scores)
		if marshalErr != nil {
			return nil, marshalErr
		}
		events, submissions := make([]pgtype.UUID, len(used)), make([]pgtype.UUID, len(used))
		for i, row := range used {
			if err := events[i].Scan(row.EventID); err != nil {
				return nil, err
			}
			if err := submissions[i].Scan(row.SubmissionID); err != nil {
				return nil, err
			}
		}
		batch.Queue(saveObservedSnapshotSQL, g.id, g.version, p.policy.Revision, computed, topics,
			events, truncated, submissions, g.actor, maxEvidence).Exec(checkObservedSnapshot)
		active = append(active, g.id)
		if batch.Len() == observedSnapshotBatch {
			if flushErr := tx.SendBatch(ctx, batch).Close(); flushErr != nil {
				return nil, flushErr
			}
			batch = &pgx.Batch{}
		}
	}
	if batch.Len() != 0 {
		if flushErr := tx.SendBatch(ctx, batch).Close(); flushErr != nil {
			return nil, flushErr
		}
	}

	return active, nil
}

func checkObservedSnapshot(tag pgconn.CommandTag) error {
	if tag.RowsAffected() != 1 {
		return errors.New("mastery evidence changed while saving snapshot")
	}

	return nil
}

// Занимаем только слоты, на которые не ссылается новый снимок.
const saveObservedSnapshotSQL = `
	WITH requested AS MATERIALIZED (
	    SELECT event_id,submission_id FROM unnest($6::uuid[],$8::uuid[]) AS ids(event_id,submission_id)
	), existing AS MATERIALIZED (
	    SELECT slot,event_id,submission_id FROM mastery_observed_evidence WHERE goal_id=$1
	), kept AS MATERIALIZED (
	    SELECT e.slot FROM existing e JOIN requested r USING (event_id,submission_id)
	), missing AS (
	    SELECT r.*,row_number() OVER (ORDER BY r.event_id) AS n FROM requested r
	    WHERE NOT EXISTS (
	        SELECT 1 FROM existing e WHERE e.event_id=r.event_id AND e.submission_id=r.submission_id
	    )
	), free AS (
	    SELECT slot,row_number() OVER (ORDER BY slot) AS n FROM generate_series(1,$10::integer) AS slots(slot)
	    WHERE NOT EXISTS (SELECT 1 FROM kept k WHERE k.slot=slots.slot)
	), written AS (
	    INSERT INTO mastery_observed_evidence (goal_id,slot,event_id,submission_id,proof)
	    SELECT $1,f.slot,o.event_id,o.submission_id,
	        jsonb_strip_nulls(jsonb_build_object(
	            'task_id',o.task_id,'task_version_id',o.task_version_id,'submission_id',o.submission_id,
	            'event_id',o.event_id,'correction_version',o.correction_version,'graded_at',o.graded_at,
	            'verdict',o.verdict,'topic_ids',o.topic_ids,'topic_weights',o.topic_weights))
	    FROM missing m JOIN free f USING (n)
	    JOIN learning_task_outcomes o ON o.submission_id=m.submission_id AND o.event_id=m.event_id
	    WHERE o.learner_id=$9 AND o.evidence_kind='independent'
	    ON CONFLICT (goal_id,slot) DO UPDATE SET
	        event_id=EXCLUDED.event_id, submission_id=EXCLUDED.submission_id, proof=EXCLUDED.proof
	    RETURNING 1
	)
	INSERT INTO mastery_baselines (goal_id,goal_version,policy_revision,computed_at,topics,evidence,truncated,evidence_refs)
	SELECT $1,$2,$3,$4,$5::jsonb,'[]'::jsonb,$7,$6::uuid[]
	WHERE (SELECT count(*) FROM kept)+(SELECT count(*) FROM written)=cardinality($6::uuid[])
	ON CONFLICT (goal_id) DO UPDATE SET
	    goal_version=EXCLUDED.goal_version, policy_revision=EXCLUDED.policy_revision,
	    computed_at=EXCLUDED.computed_at, topics=EXCLUDED.topics, evidence=EXCLUDED.evidence,
	    truncated=EXCLUDED.truncated, evidence_refs=EXCLUDED.evidence_refs`
