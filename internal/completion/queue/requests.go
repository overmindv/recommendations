package queue

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func Request(ctx context.Context, tx pgx.Tx, learners []string) error {
	if len(learners) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO goal_completion_checks AS q (learner_id)
		SELECT wanted.id FROM (SELECT DISTINCT id FROM unnest($1::uuid[]) AS input(id)) AS wanted
		WHERE EXISTS (
		    SELECT 1 FROM learner_goals AS g JOIN study_plan_heads AS h ON h.goal_id=g.id
		    WHERE g.user_id=wanted.id AND g.active
		) OR EXISTS (
		    SELECT 1 FROM goal_completion_state AS s WHERE s.learner_id=wanted.id AND s.verified
		)
		ORDER BY wanted.id
		ON CONFLICT (learner_id) DO UPDATE SET token=EXCLUDED.token,
		    ready_at=LEAST(q.ready_at,EXCLUDED.ready_at)`, learners)
	return err
}
