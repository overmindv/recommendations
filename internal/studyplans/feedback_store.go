package studyplans

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/learnergoals"
)

const maxFeedbackPerHour = 64

type FeedbackReceipt struct {
	RequestID     string         `json:"request_id"`
	PlanVersion   int64          `json:"plan_version"`
	Action        FeedbackAction `json:"action"`
	Ordinal       *int           `json:"ordinal,omitempty"`
	WeeklyMinutes *int           `json:"weekly_minutes,omitempty"`
	AcceptedAt    time.Time      `json:"accepted_at"`
}

func (p *Postgres) SubmitFeedback(ctx context.Context, actor, goal string, in Feedback) (FeedbackReceipt, error) {
	if err := in.Validate(actor, goal); err != nil {
		return FeedbackReceipt{}, err
	}
	var receipt FeedbackReceipt
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if err := learnergoals.LockBudget(ctx, tx, actor); err != nil {
			return err
		}
		// Прием оценок и пересчет тоже берут цель раньше записи в очередь.
		if err := lockPlanGoal(ctx, tx, actor, goal); err != nil {
			return err
		}
		source, err := readSources(ctx, tx, actor, goal)
		if err != nil {
			return err
		}
		receipt, err = readFeedback(ctx, tx, goal, in.RequestID)
		if err == nil {
			if receipt.PlanVersion != in.PlanVersion || receipt.Action != in.Action ||
				!sameFeedbackValue(receipt.Ordinal, in.Ordinal) || !sameFeedbackValue(receipt.WeeklyMinutes, in.WeeklyMinutes) {
				return ErrRequestUsed
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if !source.Active || source.PlanVersion != in.PlanVersion {
			return ErrConflict
		}
		if in.Action != FeedbackResume {
			var limit bool
			err = tx.QueryRow(ctx, `
				SELECT EXISTS (SELECT 1 FROM study_plan_feedback
				    WHERE goal_id=$1 AND accepted_at>=statement_timestamp()-interval '1 hour'
				    ORDER BY accepted_at DESC OFFSET $2 LIMIT 1)`, goal, maxFeedbackPerHour-1).Scan(&limit)
			if err != nil {
				return err
			}
			if limit {
				return ErrFeedbackLimit
			}
		}
		if err := applyFeedback(ctx, tx, actor, goal, source, in); err != nil {
			return err
		}
		receipt = FeedbackReceipt{RequestID: in.RequestID, PlanVersion: in.PlanVersion, Action: in.Action,
			Ordinal: in.Ordinal, WeeklyMinutes: in.WeeklyMinutes}
		return tx.QueryRow(ctx, `
			INSERT INTO study_plan_feedback (goal_id,request_id,plan_version,action,ordinal,weekly_minutes)
			VALUES ($1,$2,$3,$4,$5,$6) RETURNING accepted_at`,
			goal, in.RequestID, in.PlanVersion, in.Action, in.Ordinal, in.WeeklyMinutes).Scan(&receipt.AcceptedAt)
	})
	if err != nil {
		return FeedbackReceipt{}, fmt.Errorf("save study plan feedback: %w", err)
	}
	return receipt, nil
}

func readFeedback(ctx context.Context, tx pgx.Tx, goal, request string) (FeedbackReceipt, error) {
	out := FeedbackReceipt{RequestID: request}
	err := tx.QueryRow(ctx, `
		SELECT plan_version,action,ordinal,weekly_minutes,accepted_at FROM study_plan_feedback
		WHERE goal_id=$1 AND request_id=$2`, goal, request).
		Scan(&out.PlanVersion, &out.Action, &out.Ordinal, &out.WeeklyMinutes, &out.AcceptedAt)
	return out, err
}

func sameFeedbackValue(a, b *int) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func applyFeedback(ctx context.Context, tx pgx.Tx, actor, goal string, source Sources, in Feedback) error {
	paused := source.pausedAt
	var budgetVersion int64
	switch in.Action {
	case FeedbackPause:
		if paused != nil {
			return ErrStepBlocked
		}
	case FeedbackResume:
		if paused == nil {
			return ErrStepBlocked
		}
	default:
		if source.PlanVersion >= MaxVersionsPerGoal {
			return ErrLimit
		}
		view, err := readView(ctx, tx, goal, source.PlanVersion, source)
		if err != nil {
			return err
		}
		if view.Stale && (in.Action != FeedbackSetBudget || !source.approvedBudgetChange(view)) {
			return ErrConflict
		}
		if in.Action == FeedbackSetBudget {
			budgetVersion, err = resizeStudyBudget(ctx, tx, actor, goal, source, *in.WeeklyMinutes)
			if err != nil {
				return err
			}
		} else if err := applyStepFeedback(ctx, tx, goal, view, in); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO study_plan_feedback_state AS s (goal_id,paused_at,budget_from_plan,budget_goal_version)
		VALUES ($1,CASE WHEN $2='pause' THEN statement_timestamp() ELSE $3::timestamptz END,
		        CASE WHEN $4::bigint>0 THEN $5 ELSE 0 END,$4)
		ON CONFLICT (goal_id) DO UPDATE SET revision=s.revision+1,
		    paused_at=CASE WHEN $2='resume' THEN NULL ELSE EXCLUDED.paused_at END,
		    budget_from_plan=CASE WHEN $4>0 THEN EXCLUDED.budget_from_plan ELSE s.budget_from_plan END,
		    budget_goal_version=CASE WHEN $4>0 THEN EXCLUDED.budget_goal_version ELSE s.budget_goal_version END`,
		goal, in.Action, paused, budgetVersion, in.PlanVersion); err != nil {
		return err
	}
	if in.Action == FeedbackPause {
		_, err := tx.Exec(ctx, "DELETE FROM study_plan_rebuilds WHERE goal_id=$1", goal)
		return err
	}
	if paused != nil && in.Action != FeedbackResume {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO study_plan_rebuilds AS q (goal_id,first_requested_at,requested_at,available_at,priority)
		VALUES ($1,statement_timestamp(),statement_timestamp(),statement_timestamp(),true)
		ON CONFLICT (goal_id) DO UPDATE SET revision=q.revision+1,priority=true,retry_at=NULL,
		    requested_at=GREATEST(q.requested_at,EXCLUDED.requested_at),
		    available_at=GREATEST(q.first_requested_at,EXCLUDED.available_at)`, goal)
	return err
}

func applyStepFeedback(ctx context.Context, tx pgx.Tx, goal string, view View, in Feedback) error {
	ordinal := *in.Ordinal
	if view.NextStep == nil || *view.NextStep != ordinal {
		return ErrStepBlocked
	}
	item := findItem(view, ordinal)
	if item == nil {
		return ErrNotFound
	}
	if in.Action != FeedbackSkip && in.Action != FeedbackNotRelevant {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO study_plan_resource_cooldowns AS c (goal_id,kind,resource_id,expires_at)
		VALUES ($1,$2,$3,statement_timestamp()+interval '7 days')
		ON CONFLICT (goal_id,kind,resource_id) DO UPDATE SET expires_at=GREATEST(c.expires_at,EXCLUDED.expires_at)`,
		goal, item.Kind, item.ID)
	return err
}

func resizeStudyBudget(ctx context.Context, tx pgx.Tx, actor, goal string, source Sources, minutes int) (int64, error) {
	if minutes > source.Budget.AvailableMinutes-source.Budget.ReservedMinutes {
		return 0, ErrBudget
	}
	if minutes == source.Budget.GoalMinutes {
		return 0, nil
	}
	var version int64
	err := tx.QueryRow(ctx, `
		UPDATE learner_goals SET weekly_minutes=$4,version=version+1,updated_at=statement_timestamp()
		WHERE id=$1 AND user_id=$2 AND version=$3 AND active RETURNING version`,
		goal, actor, source.GoalVersion, minutes).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrConflict
	}
	return version, err
}
