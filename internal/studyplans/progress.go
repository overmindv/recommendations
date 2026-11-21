package studyplans

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/internal/planner"
)

const (
	acceptedVerdict    = "accepted"
	sufficientEvidence = "sufficient_for_policy"
	failedCheck        = "check_failed"
)

func (p *Postgres) CompleteActivity(ctx context.Context, actor, goal string, version int64, ordinal int) (View, error) {
	return p.updateProgress(ctx, actor, goal, version, ordinal, false)
}

func (p *Postgres) Assess(ctx context.Context, actor, goal string, version int64, ordinal int) (View, error) {
	return p.updateProgress(ctx, actor, goal, version, ordinal, true)
}

func (p *Postgres) updateProgress(ctx context.Context, actor, goal string, version int64, ordinal int, assessment bool) (View, error) {
	if !planner.ValidID(actor) || !planner.ValidID(goal) || version < 1 || ordinal < 1 || ordinal > planner.MaxLearningItems {
		return View{}, ErrInvalid
	}
	var result View
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if err := learnergoals.LockBudget(ctx, tx, actor); err != nil {
			return err
		}
		source, err := readSources(ctx, tx, actor, goal)
		if err != nil {
			return err
		}
		if version != source.PlanVersion {
			return ErrConflict
		}
		result, err = readView(ctx, tx, goal, version, source)
		if err != nil {
			return err
		}
		if result.Stale {
			return ErrConflict
		}
		item := findItem(result, ordinal)
		if item == nil {
			return ErrNotFound
		}
		if assessment != (item.Stage == planner.StudyCheck) {
			return ErrStepBlocked
		}
		if (assessment && item.Assessment != nil) || (!assessment && item.ActivityDone != nil) {
			return nil
		}
		if result.NextStep == nil || *result.NextStep != ordinal {
			return ErrStepBlocked
		}
		if assessment {
			if assessmentErr := recordAssessment(ctx, tx, actor, result, *item); assessmentErr != nil {
				return assessmentErr
			}
		} else if _, activityErr := tx.Exec(ctx, `
			INSERT INTO study_plan_activity (goal_id,plan_version,ordinal)
			VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, goal, version, ordinal); activityErr != nil {
			return activityErr
		}
		result, err = readView(ctx, tx, goal, version, source)

		return err
	})
	if err != nil {
		return View{}, fmt.Errorf("save study progress: %w", err)
	}

	return result, nil
}

func recordAssessment(ctx context.Context, tx pgx.Tx, actor string, view View, item Item) error {
	tag, err := tx.Exec(ctx, `
		INSERT INTO study_plan_assessments (goal_id,plan_version,ordinal,submission_id,event_id,correction_version,verdict,graded_at)
		SELECT $1,$2,$3,first.submission_id,first.event_id,first.correction_version,first.verdict,first.graded_at
		FROM (
		    SELECT submission_id,event_id,correction_version,verdict,graded_at,task_version_id,topic_ids
		    FROM learning_task_outcomes
		    WHERE learner_id=$4 AND task_id=$5 AND evidence_kind='independent'
		      AND graded_at <= statement_timestamp()
		    ORDER BY graded_at,submission_id LIMIT 1
		) AS first
		WHERE first.task_version_id=$6 AND $7=ANY(first.topic_ids) AND first.graded_at >= $8
		ON CONFLICT (goal_id,plan_version,ordinal) DO UPDATE SET
		    submission_id=EXCLUDED.submission_id,event_id=EXCLUDED.event_id,
		    correction_version=EXCLUDED.correction_version,verdict=EXCLUDED.verdict,graded_at=EXCLUDED.graded_at`,
		view.GoalID, view.Version, item.Ordinal, actor, item.ID, item.RevisionID, item.TopicID, view.CreatedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrPending
	}

	return nil
}

func findItem(view View, ordinal int) *Item {
	for i := range view.Weeks {
		if index := slices.IndexFunc(view.Weeks[i].Items, func(item Item) bool { return ordinal == item.Ordinal }); index >= 0 {
			return &view.Weeks[i].Items[index]
		}
	}

	return nil
}

func describeProgress(ctx context.Context, tx pgx.Tx, actor string, view *View) error {
	if !view.Current || view.Stale {
		view.BlockedReason = "stale_plan"

		return nil
	}
	accepted := make(map[string]bool)
	goalPrerequisites := make(map[string]bool)
	goalChecksPassed := !slices.ContainsFunc(view.UnmetOutcomes, func(outcome planner.UnmetOutcome) bool {
		return slices.Contains(view.TargetTopics, outcome.TopicID)
	})
	var next *Item
	for _, week := range view.Weeks {
		for i := range week.Items {
			item := &week.Items[i]
			target := slices.Contains(view.TargetTopics, item.TopicID)
			if target {
				for _, prerequisite := range item.RequiresCheck {
					goalPrerequisites[prerequisite] = true
				}
			}
			if item.Stage == planner.StudyCheck {
				passed := item.Assessment != nil && item.Assessment.Verdict == acceptedVerdict
				accepted[item.TopicID] = accepted[item.TopicID] || passed
				if !passed && target {
					goalChecksPassed = false
				}
				if !passed && next == nil {
					next = item
				}
			} else if item.ActivityDone == nil && next == nil {
				next = item
			}
		}
	}

	var topics []string
	for id := range goalPrerequisites {
		if !accepted[id] {
			goalChecksPassed = false

			break
		}
	}
	if goalChecksPassed {
		topics = append(topics, view.TargetTopics...)
		for id := range goalPrerequisites {
			topics = append(topics, id)
		}
	}
	if next != nil {
		for _, target := range next.RequiresCheck {
			if accepted[target] {
				topics = append(topics, target)
			}
		}
	}
	slices.Sort(topics)
	topics = slices.Compact(topics)
	verified := make(map[string]int, len(topics))
	if len(topics) != 0 {
		evidence, err := mastery.ReadPlanningEvidence(ctx, tx, actor, topics, view.EvidencePolicy, time.Now().UTC())
		if err != nil {
			return fmt.Errorf("read current study evidence: %w", err)
		}
		for _, topic := range evidence.Topics {
			if topic.ScoreBPS != nil && topic.Uncertainty == sufficientEvidence {
				verified[topic.ID] = *topic.ScoreBPS
			}
		}
	}
	view.GoalDemonstrated = goalChecksPassed
	for _, target := range view.TargetTopics {
		if verified[target] < view.RankingPolicy.TargetBPS {
			view.GoalDemonstrated = false
		}
	}
	for id := range goalPrerequisites {
		if verified[id] < view.RankingPolicy.PrerequisiteMinBPS {
			view.GoalDemonstrated = false
		}
	}
	if next == nil {
		return nil
	}
	if next.Assessment != nil && next.Assessment.Verdict != acceptedVerdict {
		view.BlockedReason = failedCheck

		return nil
	}
	for _, target := range next.RequiresCheck {
		if !accepted[target] || verified[target] < view.RankingPolicy.PrerequisiteMinBPS {
			view.BlockedReason = "unverified_prerequisite"

			return nil
		}
	}
	view.NextStep = new(next.Ordinal)

	return nil
}
