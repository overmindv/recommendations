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

type progressAction uint8

const (
	startActivity progressAction = iota
	completeActivity
	assessActivity
)

func (p *Postgres) StartActivity(ctx context.Context, actor, goal string, version int64, ordinal int) (View, error) {
	return p.updateProgress(ctx, actor, goal, version, ordinal, startActivity)
}

func (p *Postgres) CompleteActivity(ctx context.Context, actor, goal string, version int64, ordinal int) (View, error) {
	return p.updateProgress(ctx, actor, goal, version, ordinal, completeActivity)
}

func (p *Postgres) Assess(ctx context.Context, actor, goal string, version int64, ordinal int) (View, error) {
	return p.updateProgress(ctx, actor, goal, version, ordinal, assessActivity)
}

func (p *Postgres) updateProgress(ctx context.Context, actor, goal string, version int64, ordinal int, action progressAction) (View, error) {
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
		if item.Superseded || item.Skipped {
			return ErrStepBlocked
		}
		check := item.Stage == planner.StudyCheck
		if (action == assessActivity && !check) || (action == completeActivity && check) {
			return ErrStepBlocked
		}
		if (action == assessActivity && item.Assessment != nil) || (action == completeActivity && item.ActivityDone != nil) ||
			(action == startActivity && (item.ActivityStarted != nil || item.Assessment != nil)) {
			return nil
		}
		if result.NextStep == nil || *result.NextStep != ordinal {
			return ErrStepBlocked
		}
		switch action {
		case assessActivity:
			if assessmentErr := recordAssessment(ctx, tx, actor, result, *item); assessmentErr != nil {
				return assessmentErr
			}
		case startActivity, completeActivity:
			if _, activityErr := tx.Exec(ctx, `
				INSERT INTO study_plan_activity AS a (goal_id,plan_version,ordinal,started_at,completed_at)
				VALUES ($1,$2,$3,statement_timestamp(),CASE WHEN $4::boolean THEN statement_timestamp() END)
				ON CONFLICT (goal_id,plan_version,ordinal) DO UPDATE SET
				    started_at=COALESCE(a.started_at,a.completed_at,EXCLUDED.started_at),
				    completed_at=COALESCE(a.completed_at,CASE WHEN EXCLUDED.completed_at IS NOT NULL
				        THEN GREATEST(a.started_at,EXCLUDED.completed_at) END)`, goal, version, ordinal, action == completeActivity); activityErr != nil {
				return activityErr
			}
		}
		result, err = readView(ctx, tx, goal, version, source)

		return err
	})
	if err != nil {
		return View{}, fmt.Errorf("save study progress: %w", err)
	}

	return result, nil
}

const eligibleCheck = `
	FROM (
	    SELECT submission_id,event_id,correction_version,verdict,graded_at,task_version_id,topic_ids
	    FROM learning_task_outcomes
	    WHERE learner_id=$1 AND task_id=$2 AND evidence_kind='independent'
	      AND graded_at <= statement_timestamp()
	    ORDER BY graded_at,submission_id LIMIT 1
	) AS first
	WHERE first.task_version_id=$3 AND $4=ANY(first.topic_ids) AND first.graded_at >= $5`

func recordAssessment(ctx context.Context, tx pgx.Tx, actor string, view View, item Item) error {
	tag, err := tx.Exec(ctx, `
		INSERT INTO study_plan_assessments (goal_id,plan_version,ordinal,submission_id,event_id,correction_version,verdict,graded_at)
		SELECT $6,$7,$8,first.submission_id,first.event_id,first.correction_version,first.verdict,first.graded_at
		`+eligibleCheck+`
		ON CONFLICT (goal_id,plan_version,ordinal) DO UPDATE SET
		    submission_id=EXCLUDED.submission_id,event_id=EXCLUDED.event_id,
		    correction_version=EXCLUDED.correction_version,verdict=EXCLUDED.verdict,graded_at=EXCLUDED.graded_at`,
		actor, item.ID, item.RevisionID, item.TopicID, item.evidenceSince, view.GoalID, view.Version, item.Ordinal)
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

func describeProgress(ctx context.Context, tx pgx.Tx, actor string, view *View, now time.Time) error {
	if !view.Current || view.Stale {
		view.BlockedReason = "stale_plan"

		return nil
	}
	accepted := make(map[string]bool)
	goalPrerequisites := make(map[string]bool)
	goalChecksPassed := !slices.ContainsFunc(view.UnmetOutcomes, func(outcome planner.UnmetOutcome) bool {
		return slices.Contains(view.TargetTopics, outcome.TopicID)
	})
	var pending []*Item
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
				if item.Superseded {
					continue
				}
				if !passed && target {
					goalChecksPassed = false
				}
				if !passed {
					pending = append(pending, item)
				}
			} else if item.ActivityDone == nil {
				pending = append(pending, item)
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
	if len(pending) != 0 && len(pending[0].RequiresCheck) != 0 {
		for _, item := range pending {
			for _, target := range item.RequiresCheck {
				if accepted[target] {
					topics = append(topics, target)
				}
			}
		}
	}
	slices.Sort(topics)
	topics = slices.Compact(topics)
	verified := make(map[string]int, len(topics))
	if len(topics) != 0 {
		evidence, err := mastery.ReadPlanningEvidence(ctx, tx, actor, topics, view.EvidencePolicy, now)
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
	if view.PausedAt != nil {
		view.BlockedReason = planPaused

		return nil
	}
	if len(pending) == 0 {
		return nil
	}
	next := pending[0]
	if next.Skipped {
		view.BlockedReason = planReplanning

		return nil
	}
	if next.Assessment != nil && next.Assessment.Verdict != acceptedVerdict {
		view.BlockedReason = failedCheck

		return nil
	}
	passes := func(topic string) bool {
		return accepted[topic] && verified[topic] >= view.RankingPolicy.PrerequisiteMinBPS
	}
	if slices.ContainsFunc(next.RequiresCheck, func(topic string) bool { return !passes(topic) }) {
		if repair := prerequisiteRepair(pending, passes); repair != nil {
			if repair.Skipped {
				view.BlockedReason = planReplanning
			} else {
				view.NextStep = new(repair.Ordinal)
			}
		} else {
			view.BlockedReason = "unverified_prerequisite"
		}

		return nil
	}
	view.NextStep = new(next.Ordinal)

	return nil
}

func prerequisiteRepair(pending []*Item, passes func(string) bool) *Item {
	needed := make(map[string]bool)
	for _, topic := range pending[0].RequiresCheck {
		needed[topic] = !passes(topic)
	}
	for {
		expanded := false
		for _, item := range pending[1:] {
			if !needed[item.TopicID] {
				continue
			}
			for _, topic := range item.RequiresCheck {
				if !passes(topic) && !needed[topic] {
					needed[topic], expanded = true, true
				}
			}
		}
		if !expanded {
			break
		}
	}
	for _, item := range pending[1:] {
		if needed[item.TopicID] && item.Assessment == nil &&
			!slices.ContainsFunc(item.RequiresCheck, func(topic string) bool { return !passes(topic) }) {
			return item
		}
	}

	return nil
}
