package studyplans

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	completionqueue "github.com/overmindv/recommendations/internal/completion/queue"
	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/planner"
)

var (
	ErrLeaseLost    = errors.New("study rebuild lease is no longer current")
	ErrStaleRebuild = errors.New("study rebuild requires a new manual plan version")
)

type RebuildJob struct {
	GoalID   string
	Actor    string
	Token    string
	Revision int64
}

func (job RebuildJob) valid() bool {
	return planner.ValidID(job.GoalID) && planner.ValidID(job.Actor) && planner.ValidID(job.Token) && job.Revision > 0
}

type RebuildBase struct {
	Source Sources
	Plan   View
}

const readyRebuildAt = "GREATEST(available_at, COALESCE(retry_at, '-infinity'::timestamptz), COALESCE(leased_until, '-infinity'::timestamptz))"

func (p *Postgres) ClaimRebuild(ctx context.Context, lifetime time.Duration) (RebuildJob, bool, error) {
	if lifetime < time.Second || lifetime > 5*time.Minute {
		return RebuildJob{}, false, ErrInvalid
	}
	var job RebuildJob
	err := p.pool.QueryRow(ctx, `
		WITH ready AS (
		    SELECT goal_id FROM study_plan_rebuilds
		    WHERE `+readyRebuildAt+` <= statement_timestamp()
		    ORDER BY `+readyRebuildAt+`, goal_id LIMIT 1 FOR UPDATE SKIP LOCKED
		), claimed AS (
		    UPDATE study_plan_rebuilds AS q SET lease_token=gen_random_uuid(),
		        leased_until=statement_timestamp()+$1::bigint*interval '1 microsecond'
		    FROM ready WHERE q.goal_id=ready.goal_id
		    RETURNING q.goal_id,q.lease_token,q.revision
		)
		SELECT c.goal_id::text,g.user_id::text,c.lease_token::text,c.revision
		FROM claimed AS c JOIN learner_goals AS g ON g.id=c.goal_id`, lifetime.Microseconds()).
		Scan(&job.GoalID, &job.Actor, &job.Token, &job.Revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return RebuildJob{}, false, nil
	}
	if err != nil {
		return RebuildJob{}, false, fmt.Errorf("claim study rebuild: %w", err)
	}

	return job, true, nil
}

func (p *Postgres) ReleaseRebuild(ctx context.Context, job RebuildJob, retry time.Duration) error {
	if !job.valid() || retry < 0 || retry > 5*time.Minute {
		return ErrInvalid
	}
	_, err := p.pool.Exec(ctx, `
		UPDATE study_plan_rebuilds
		SET lease_token=NULL,leased_until=NULL,
		    retry_at=CASE WHEN revision=$3 AND $4::bigint>0
		        THEN statement_timestamp()+$4::bigint*interval '1 microsecond' ELSE retry_at END
		WHERE goal_id=$1 AND lease_token=$2`, job.GoalID, job.Token, job.Revision, retry.Microseconds())

	return err
}

func (p *Postgres) DiscardRebuild(ctx context.Context, job RebuildJob) error {
	if !job.valid() {
		return ErrInvalid
	}

	return acknowledgeRebuild(ctx, p.pool, job)
}

func (p *Postgres) LoadRebuild(ctx context.Context, job RebuildJob) (RebuildBase, error) {
	if !job.valid() {
		return RebuildBase{}, ErrInvalid
	}
	var base RebuildBase
	err := pgx.BeginTxFunc(ctx, p.pool, readOnly, func(tx pgx.Tx) error {
		if err := checkRebuildLease(ctx, tx, job, false); err != nil {
			return err
		}
		source, err := readSources(ctx, tx, job.Actor, job.GoalID)
		if err != nil {
			return err
		}
		if !source.Active || source.PlanVersion == 0 {
			return ErrNotFound
		}
		if source.PlanVersion >= MaxVersionsPerGoal {
			return ErrLimit
		}
		view, err := readView(ctx, tx, job.GoalID, source.PlanVersion, source)
		if err != nil {
			return err
		}
		if !canRebuild(source, view) {
			return ErrStaleRebuild
		}
		if err := observeNextCheck(ctx, tx, job.Actor, &view); err != nil {
			return err
		}
		base = RebuildBase{Source: source, Plan: view}

		return nil
	})
	if err != nil {
		return RebuildBase{}, err
	}

	return base, nil
}

func (p *Postgres) CommitRebuild(ctx context.Context, job RebuildJob, base RebuildBase, draft *Draft) error {
	if !job.valid() || base.Plan.GoalID != job.GoalID || base.Source.actor != job.Actor ||
		base.Plan.Version < 1 || base.Plan.Version != base.Source.PlanVersion {
		return ErrInvalid
	}
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if err := learnergoals.LockBudget(ctx, tx, job.Actor); err != nil {
			return err
		}
		// Прием оценок тоже блокирует цель до записи в очередь.
		if err := lockPlanGoal(ctx, tx, job.Actor, job.GoalID); err != nil {
			return err
		}
		if leaseErr := checkRebuildLease(ctx, tx, job, true); leaseErr != nil {
			return leaseErr
		}
		current, err := readSources(ctx, tx, job.Actor, job.GoalID)
		if err != nil {
			return err
		}
		if current.Expected != base.Source.Expected || current.Budget.ReservedMinutes != base.Source.Budget.ReservedMinutes ||
			current.feedbackRevision != base.Source.feedbackRevision || !current.Active {
			return ErrConflict
		}
		view, err := readView(ctx, tx, job.GoalID, current.PlanVersion, current)
		if err != nil {
			return err
		}
		if err := observeNextCheck(ctx, tx, job.Actor, &view); err != nil {
			return err
		}
		if !canRebuild(current, view) || !sameProtected(view, base.Plan) {
			return ErrConflict
		}
		if draft != nil {
			if err := saveRebuiltVersion(ctx, tx, current, view, *draft); err != nil {
				return err
			}
			if p.completionChecks {
				if err := completionqueue.Request(ctx, tx, []string{job.Actor}); err != nil {
					return err
				}
			}
		}

		return acknowledgeRebuild(ctx, tx, job)
	})
	if err != nil {
		return fmt.Errorf("commit study rebuild: %w", err)
	}

	return nil
}

func canRebuild(source Sources, view View) bool {
	return source.Active && source.pausedAt == nil && (!view.Stale || source.approvedBudgetChange(view))
}

func (s Sources) approvedBudgetChange(view View) bool {
	return s.budgetFromPlan == view.Version && s.budgetGoalVersion == s.GoalVersion &&
		s.AvailabilityVersion == view.AvailabilityVersion && s.GoalVersion > view.GoalVersion
}

type rebuildExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func acknowledgeRebuild(ctx context.Context, q rebuildExecutor, job RebuildJob) error {
	tag, err := q.Exec(ctx, `DELETE FROM study_plan_rebuilds WHERE goal_id=$1 AND lease_token=$2
		AND revision=$3 AND leased_until>statement_timestamp()`, job.GoalID, job.Token, job.Revision)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}

	return nil
}

func checkRebuildLease(ctx context.Context, tx pgx.Tx, job RebuildJob, lock bool) error {
	query := `SELECT true FROM study_plan_rebuilds WHERE goal_id=$1 AND lease_token=$2
		AND revision=$3 AND leased_until>statement_timestamp()`
	if lock {
		query += " FOR UPDATE"
	}
	var found bool
	err := tx.QueryRow(ctx, query, job.GoalID, job.Token, job.Revision).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}

	return err
}
