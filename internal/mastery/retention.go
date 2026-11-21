package mastery

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/planner"
)

const (
	MinReviewInterval = time.Hour
	MaxReviewInterval = 365 * 24 * time.Hour
	MaxReviewMinutes  = 20
	reviewLifetime    = 7 * 24 * time.Hour
)

const (
	reviewDue      = "due"
	reviewAssigned = "assigned"
	reviewChecked  = "checked"
	reviewExpired  = "expired"
)

var (
	ErrReviewNotDue  = errors.New("retention review is not due")
	ErrReviewCatalog = errors.New("retention catalog unavailable")
)

type ReviewTask struct {
	TaskID    string `json:"task_id"`
	VersionID string `json:"task_version_id"`
	Minutes   int    `json:"estimated_minutes"`
}

type ReviewCatalog interface {
	SelectRetentionTask(context.Context, string, string, int) (*ReviewTask, error)
}

type ReviewResult struct {
	Verdict      string    `json:"verdict"`
	GradedAt     time.Time `json:"graded_at"`
	WeightMicros int64     `json:"weight_micros"`
}

type ReviewCheck struct {
	ReviewTask
	DueAt      time.Time     `json:"due_at"`
	AssignedAt time.Time     `json:"assigned_at"`
	ExpiresAt  time.Time     `json:"expires_at"`
	Result     *ReviewResult `json:"result,omitempty"`
}

type TopicReview struct {
	TopicID string       `json:"topic_id"`
	Status  string       `json:"status"`
	DueAt   *time.Time   `json:"due_at,omitempty"`
	Check   *ReviewCheck `json:"check,omitempty"`
}

type ReviewPlan struct {
	GoalVersion int64         `json:"goal_version"`
	Truncated   bool          `json:"truncated"`
	Topics      []TopicReview `json:"topics"`
}

type Retention struct {
	baseline *Postgres
	catalog  ReviewCatalog
	interval time.Duration
	now      func() time.Time
}

func NewRetention(baseline *Postgres, catalog ReviewCatalog, interval time.Duration) *Retention {
	return &Retention{baseline: baseline, catalog: catalog, interval: interval, now: time.Now}
}

func (r *Retention) Get(ctx context.Context, actor, goal string) (ReviewPlan, error) {
	if !planner.ValidID(actor) || !planner.ValidID(goal) {
		return ReviewPlan{}, learnergoals.ErrInvalid
	}
	view, err := r.inspect(ctx, actor, goal, 0)

	return view.plan, err
}

func (r *Retention) Assign(ctx context.Context, actor, goal, topic string, version int64, dueAt time.Time) (TopicReview, error) {
	if version < 1 || dueAt.IsZero() || !planner.ValidID(actor) || !planner.ValidID(goal) || !planner.ValidID(topic) {
		return TopicReview{}, learnergoals.ErrInvalid
	}
	view, err := r.inspect(ctx, actor, goal, version)
	if err != nil {
		return TopicReview{}, err
	}
	review, replay, err := view.forAssignment(topic, dueAt)
	if err != nil || replay {
		return review, err
	}
	task, err := r.catalog.SelectRetentionTask(ctx, actor, topic, view.budget)
	if err != nil {
		return TopicReview{}, errors.Join(ErrReviewCatalog, err)
	}
	if task == nil {
		review.Status = "no_new_task"

		return review, nil
	}
	if !planner.ValidID(task.TaskID) || !planner.ValidID(task.VersionID) || task.Minutes < 1 || task.Minutes > view.budget {
		return TopicReview{}, ErrReviewCatalog
	}

	err = pgx.BeginFunc(ctx, r.baseline.pool, func(tx pgx.Tx) error {
		locked, readErr := r.read(ctx, tx, actor, goal, version, true)
		if readErr != nil {
			return readErr
		}
		current, alreadyAssigned, assignErr := locked.forAssignment(topic, dueAt)
		review = current
		if assignErr != nil || alreadyAssigned {
			return assignErr
		}
		check := ReviewCheck{
			ReviewTask: *task, DueAt: *review.DueAt,
			AssignedAt: locked.now, ExpiresAt: locked.now.Add(reviewLifetime),
		}
		if saveErr := saveReviewCheck(ctx, tx, goal, topic, version, check); saveErr != nil {
			return saveErr
		}
		review.Status, review.Check = reviewAssigned, &check

		return nil
	})
	if err != nil {
		return TopicReview{}, err
	}

	return review, nil
}

type reviewView struct {
	plan   ReviewPlan
	now    time.Time
	budget int
}

func (v reviewView) forAssignment(topic string, dueAt time.Time) (TopicReview, bool, error) {
	i := slices.IndexFunc(v.plan.Topics, func(review TopicReview) bool { return review.TopicID == topic })
	if i < 0 {
		return TopicReview{}, false, learnergoals.ErrNotFound
	}
	review := v.plan.Topics[i]
	if v.budget == 0 {
		return TopicReview{}, false, ErrReviewNotDue
	}
	if review.Check != nil && review.Check.DueAt.Equal(dueAt) &&
		(review.Check.Result != nil || v.now.Before(review.Check.ExpiresAt)) {
		return review, true, nil
	}
	if review.Status != reviewDue || review.DueAt == nil || !review.DueAt.Equal(dueAt) {
		return TopicReview{}, false, ErrReviewNotDue
	}

	return review, false, nil
}
