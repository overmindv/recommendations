package mastery

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/planner"
)

func (r *Retention) inspect(ctx context.Context, actor, goal string, version int64) (reviewView, error) {
	var view reviewView
	err := pgx.BeginTxFunc(ctx, r.baseline.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var err error
		view, err = r.read(ctx, tx, actor, goal, version, false)
		return err
	})
	return view, err
}

func (r *Retention) read(ctx context.Context, tx pgx.Tx, actor, goal string, version int64, lock bool) (reviewView, error) {
	query := "SELECT version,target_topics,weekly_minutes,active FROM learner_goals WHERE id=$1 AND user_id=$2"
	if lock {
		query += " FOR UPDATE"
	}
	var view reviewView
	var topics []string
	var active bool
	err := tx.QueryRow(ctx, query, goal, actor).Scan(&view.plan.GoalVersion, &topics, &view.budget, &active)
	if errors.Is(err, pgx.ErrNoRows) {
		return reviewView{}, learnergoals.ErrNotFound
	}
	if err != nil {
		return reviewView{}, fmt.Errorf("read review goal: %w", err)
	}
	if version != 0 && version != view.plan.GoalVersion {
		return reviewView{}, learnergoals.ErrConflict
	}
	view.budget = min(view.budget, MaxReviewMinutes)
	if !active {
		view.budget = 0
	}
	view.now = r.now().UTC().Truncate(time.Microsecond)
	rows, truncated, err := loadEvidence(ctx, tx, actor, view.now)
	if err != nil {
		return reviewView{}, fmt.Errorf("read review evidence: %w", err)
	}
	view.plan.Truncated = truncated
	scores, _ := summarize(r.baseline.policy, topics, rows, truncated)
	checks, err := readReviewChecks(ctx, tx, actor, goal, view.plan.GoalVersion, view.now)
	if err != nil {
		return reviewView{}, fmt.Errorf("read review checks: %w", err)
	}
	view.plan.Topics = reviewTopics(scores, checks, view.now, r.interval, active)
	return view, nil
}

func reviewTopics(scores []Topic, checks map[string]*ReviewCheck, now time.Time, interval time.Duration, active bool) []TopicReview {
	items := make([]TopicReview, len(scores))
	anchors := make([]planner.RetentionEvidence, len(scores))
	index := make(map[string]int, len(scores))
	for i, row := range scores {
		anchors[i] = planner.RetentionEvidence{TopicID: row.ID, LastEvidenceAt: row.LastEvidenceAt}
		index[row.ID] = i
		item := TopicReview{TopicID: row.ID, Status: "unknown", Check: checks[row.ID]}
		if row.LastEvidenceAt != nil {
			item.Status = "current"
		}
		if item.Check != nil {
			switch {
			case item.Check.Result != nil:
				item.Status = "checked"
			case now.Before(item.Check.ExpiresAt):
				item.Status = "assigned"
			default:
				item.Status = "expired"
			}
		}
		items[i] = item
	}
	if active {
		for _, due := range planner.DueReviews(now, interval, anchors) {
			item := &items[index[due.TopicID]]
			if item.Status != "assigned" {
				item.DueAt = new(due.DueAt)
				item.Status = "due"
			}
		}
	}
	return items
}

const reviewChecksSQL = `
	SELECT c.topic_id::text,c.due_at,c.assigned_at,c.expires_at,c.task_id::text,c.task_version_id::text,c.estimated_minutes,
	       o.verdict,o.graded_at,o.topic_ids,o.topic_weights
	FROM retention_checks AS c
	LEFT JOIN LATERAL (
	    SELECT verdict,graded_at,topic_ids,topic_weights,task_version_id
	    FROM learning_task_outcomes
	    WHERE learner_id=$1 AND task_id=c.task_id AND evidence_kind='independent' AND graded_at <= $4
	    ORDER BY graded_at,submission_id LIMIT 1
	) AS o ON o.graded_at >= c.assigned_at AND o.graded_at < c.expires_at
	      AND o.task_version_id=c.task_version_id AND c.topic_id=ANY(o.topic_ids)
	WHERE c.goal_id=$2 AND c.goal_version=$3`

func readReviewChecks(ctx context.Context, tx pgx.Tx, actor, goal string, version int64, now time.Time) (map[string]*ReviewCheck, error) {
	rows, err := tx.Query(ctx, reviewChecksSQL, actor, goal, version, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	checks := make(map[string]*ReviewCheck)
	for rows.Next() {
		var id string
		var c ReviewCheck
		var e evidence
		var verdict *string
		var gradedAt *time.Time
		if err := rows.Scan(&id, &c.DueAt, &c.AssignedAt, &c.ExpiresAt, &c.TaskID, &c.VersionID, &c.Minutes,
			&verdict, &gradedAt, &e.Topics, &e.Weights); err != nil {
			return nil, err
		}
		if verdict != nil && gradedAt != nil {
			i := slices.Index(e.Topics, id)
			if i < 0 {
				return nil, errors.New("retention grade has no assigned topic")
			}
			c.Result = &ReviewResult{Verdict: *verdict, GradedAt: *gradedAt, WeightMicros: shares(e)[i]}
		}
		checks[id] = &c
	}
	return checks, rows.Err()
}

func saveReviewCheck(ctx context.Context, tx pgx.Tx, goal, topic string, version int64, c ReviewCheck) error {
	// Закрепления старой версии цели больше недоступны. Независимые оценки не удаляются.
	if _, err := tx.Exec(ctx, "DELETE FROM retention_checks WHERE goal_id=$1 AND goal_version<>$2", goal, version); err != nil {
		return fmt.Errorf("retire old review checks: %w", err)
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO retention_checks (goal_id,topic_id,goal_version,due_at,assigned_at,expires_at,task_id,task_version_id,estimated_minutes)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (goal_id,topic_id) DO UPDATE
		SET goal_version=EXCLUDED.goal_version,due_at=EXCLUDED.due_at,assigned_at=EXCLUDED.assigned_at,
		    expires_at=EXCLUDED.expires_at,task_id=EXCLUDED.task_id,task_version_id=EXCLUDED.task_version_id,
		    estimated_minutes=EXCLUDED.estimated_minutes`,
		goal, topic, version, c.DueAt, c.AssignedAt, c.ExpiresAt, c.TaskID, c.VersionID, c.Minutes)
	if err != nil {
		return fmt.Errorf("save review check: %w", err)
	}
	return nil
}
