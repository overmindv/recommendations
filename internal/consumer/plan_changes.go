package consumer

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/mastery"
)

const impactSQL = `
	WITH changed AS MATERIALIZED (
	    SELECT o.learner_id,o.topic_ids
	    FROM unnest($1::uuid[]) AS wanted(id)
	    JOIN learning_task_outcomes AS o ON o.submission_id=wanted.id
	    WHERE o.correction_version>1 OR (
	        o.evidence_kind='independent' AND (
	            SELECT earlier.submission_id FROM learning_task_outcomes AS earlier
	            WHERE earlier.learner_id=o.learner_id AND earlier.task_id=o.task_id
	              AND earlier.evidence_kind='independent'
	              AND (earlier.graded_at,earlier.submission_id)<(o.graded_at,o.submission_id)
	            ORDER BY earlier.graded_at,earlier.submission_id LIMIT 1
	        ) IS NULL
	    )
	)
	SELECT DISTINCT g.id::text
	FROM changed AS c JOIN learner_goals AS g ON g.user_id=c.learner_id
	LEFT JOIN study_plan_heads AS h ON h.goal_id=g.id
	LEFT JOIN study_plan_versions AS v ON v.goal_id=h.goal_id AND v.version=h.version
	WHERE g.active AND (g.target_topics && c.topic_ids OR v.scope_topics && c.topic_ids OR EXISTS (
	        SELECT 1 FROM study_plan_items AS item WHERE item.goal_id=g.id AND item.plan_version=h.version
	          AND (item.topic_id=ANY(c.topic_ids) OR item.requires_checks && c.topic_ids)
	))
	ORDER BY g.id::text LIMIT 1025`

const rebuildSQL = `
	INSERT INTO study_plan_rebuilds AS q (goal_id,first_requested_at,requested_at,available_at)
	SELECT h.goal_id,statement_timestamp(),statement_timestamp(),statement_timestamp()+$2::bigint*interval '1 microsecond'
	FROM study_plan_heads AS h
	LEFT JOIN study_plan_feedback_state AS f ON f.goal_id=h.goal_id
	WHERE h.goal_id=ANY($1::uuid[]) AND f.paused_at IS NULL ORDER BY h.goal_id
	ON CONFLICT (goal_id) DO UPDATE SET revision=q.revision+1,
	    requested_at=GREATEST(q.requested_at,EXCLUDED.requested_at),
	    available_at=CASE WHEN q.priority THEN q.available_at ELSE
	        LEAST(q.first_requested_at+4*$2::bigint*interval '1 microsecond', GREATEST(q.available_at,EXCLUDED.available_at)) END`

func updateLearningPlans(ctx context.Context, tx pgx.Tx, submissions []string, delay time.Duration, baselines *mastery.Postgres) error {
	rows, err := tx.Query(ctx, impactSQL, submissions)
	if err != nil {
		return err
	}
	goals, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || len(goals) == 0 {
		return err
	}
	active, err := baselines.RefreshObservedGoals(ctx, tx, goals)
	if err != nil || len(active) == 0 {
		return err
	}
	_, err = tx.Exec(ctx, rebuildSQL, active, delay.Microseconds())

	return err
}
