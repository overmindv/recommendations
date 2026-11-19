package diagnostic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/overmindv/recommendations/internal/learnergoals"
)

type Postgres struct{ pool *pgxpool.Pool }

func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

const sessionSQL = `
	SELECT s.id::text, s.goal_id::text, s.goal_version,
	       CASE WHEN s.status = 'ready' AND s.expires_at <= statement_timestamp() THEN 'expired' ELSE s.status END,
	       s.policy_version, s.max_tasks, s.budget_minutes, s.estimated_minutes,
	       s.tasks, s.started_at, s.expires_at, s.skipped_at, s.finalized_at, s.report
	FROM learner_goals AS g JOIN diagnostic_sessions AS s ON s.goal_id = g.id AND s.goal_version = g.version
	WHERE g.user_id = $1 AND g.id = $2 AND g.version = $3`

func readSession(row pgx.Row) (Session, error) {
	var s Session
	var raw, report []byte
	err := row.Scan(&s.ID, &s.GoalID, &s.GoalVersion, &s.Status, &s.StopRule.PolicyVersion,
		&s.StopRule.MaxTasks, &s.StopRule.BudgetMinutes, &s.EstimatedMinutes, &raw,
		&s.StartedAt, &s.ExpiresAt, &s.SkippedAt, &s.FinalizedAt, &report)
	if errors.Is(err, pgx.ErrNoRows) {
		return Session{}, ErrNotStarted
	}
	if err != nil {
		return Session{}, fmt.Errorf("read diagnostic session: %w", err)
	}
	if err := json.Unmarshal(raw, &s.Tasks); err != nil {
		return Session{}, fmt.Errorf("read diagnostic tasks: %w", err)
	}
	if report != nil {
		s.Report = new(Report)
		if err := json.Unmarshal(report, s.Report); err != nil {
			return Session{}, fmt.Errorf("read diagnostic report: %w", err)
		}
	}
	s.Baseline = "unknown"
	return s, nil
}

func (p *Postgres) get(ctx context.Context, goal learnergoals.Goal) (Session, error) {
	return readSession(p.pool.QueryRow(ctx, sessionSQL, goal.UserID, goal.ID, goal.Version))
}

const saveSessionSQL = `
	INSERT INTO diagnostic_sessions (goal_id, goal_version, status, policy_version, max_tasks, budget_minutes,
	                                 estimated_minutes, tasks, started_at, expires_at, skipped_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, statement_timestamp(),
	        statement_timestamp() + $6::smallint * interval '1 minute',
	        CASE WHEN $3 = 'skipped' THEN statement_timestamp() END)
	ON CONFLICT (goal_id) DO UPDATE
	SET id = EXCLUDED.id, goal_version = EXCLUDED.goal_version, status = EXCLUDED.status,
	    policy_version = EXCLUDED.policy_version, max_tasks = EXCLUDED.max_tasks,
	    budget_minutes = EXCLUDED.budget_minutes, estimated_minutes = EXCLUDED.estimated_minutes,
	    tasks = EXCLUDED.tasks, started_at = EXCLUDED.started_at, expires_at = EXCLUDED.expires_at, skipped_at = EXCLUDED.skipped_at,
	    finalized_at = NULL, report = NULL
	WHERE diagnostic_sessions.goal_version < EXCLUDED.goal_version`

func (p *Postgres) save(ctx context.Context, goal learnergoals.Goal, selection Selection, skip bool) (Session, error) {
	if !selection.valid(skip) {
		return Session{}, ErrInvalidSelection
	}
	tasks, err := json.Marshal(selection.Tasks)
	if err != nil {
		return Session{}, fmt.Errorf("encode diagnostic tasks: %w", err)
	}
	var session Session
	err = pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		// Версия цели не должна измениться между подбором и записью сессии.
		if err := lockGoalVersion(ctx, tx, goal); err != nil {
			return err
		}
		status := Ready
		if skip {
			status = Skipped
		} else if len(selection.Tasks) == 0 {
			status = NoTasks
		}
		if _, err := tx.Exec(ctx, saveSessionSQL, goal.ID, goal.Version, status, selection.StopRule.PolicyVersion,
			selection.StopRule.MaxTasks, selection.StopRule.BudgetMinutes, selection.EstimatedMinutes, tasks); err != nil {
			return err
		}
		if skip {
			if _, err := tx.Exec(ctx, `
				UPDATE diagnostic_sessions SET status = 'skipped', skipped_at = statement_timestamp()
				WHERE goal_id = $1 AND goal_version = $2 AND status NOT IN ('skipped', 'finalized')`, goal.ID, goal.Version); err != nil {
				return err
			}
		}
		var err error
		session, err = readSession(tx.QueryRow(ctx, sessionSQL, goal.UserID, goal.ID, goal.Version))
		return err
	})
	if err != nil {
		return Session{}, fmt.Errorf("save diagnostic session: %w", err)
	}
	return session, nil
}

func lockGoalVersion(ctx context.Context, tx pgx.Tx, goal learnergoals.Goal) error {
	var version int64
	err := tx.QueryRow(ctx, "SELECT version FROM learner_goals WHERE id = $1 AND user_id = $2 FOR SHARE",
		goal.ID, goal.UserID).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return learnergoals.ErrNotFound
	}
	if err != nil {
		return err
	}
	if version != goal.Version {
		return learnergoals.ErrConflict
	}
	return nil
}
