package studyplans

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

type protectedStep struct {
	item    Item
	from    string
	through string
}

func observeNextCheck(ctx context.Context, tx pgx.Tx, actor string, view *View) error {
	if view.NextStep == nil {
		return nil
	}
	item := findItem(*view, *view.NextStep)
	if item == nil || item.Stage != planner.StudyCheck || item.Assessment != nil {
		return nil
	}
	var grade Assessment
	err := tx.QueryRow(ctx, "SELECT first.verdict,first.graded_at "+eligibleCheck,
		actor, item.ID, item.RevisionID, item.TopicID, item.evidenceSince).Scan(&grade.Verdict, &grade.GradedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	item.Assessment, item.observedGrade = &grade, true
	return nil
}

func protectedSteps(view View) []protectedStep {
	var steps []protectedStep
	for _, week := range view.Weeks {
		for _, item := range week.Items {
			if item.Skipped {
				continue
			}
			if item.ActivityStarted != nil || item.ActivityDone != nil || item.hasReceipt || item.observedGrade {
				steps = append(steps, protectedStep{item: item, from: week.From, through: week.Through})
			}
		}
	}
	return steps
}

func sameProtected(a, b View) bool {
	return slices.EqualFunc(protectedSteps(a), protectedSteps(b), func(a, b protectedStep) bool {
		left, right := a.item, b.item
		return left.Ordinal == right.Ordinal && left.hasReceipt == right.hasReceipt && left.observedGrade == right.observedGrade &&
			equalTimes(left.ActivityStarted, right.ActivityStarted) && equalTimes(left.ActivityDone, right.ActivityDone) &&
			equalAssessments(left.Assessment, right.Assessment)
	})
}

func equalTimes(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

func equalAssessments(a, b *Assessment) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Verdict == b.Verdict && a.GradedAt.Equal(b.GradedAt)
}

type itemDestination struct {
	item    planner.StudyItem
	ordinal int16
	from    string
	through string
}

func carryOrdinals(before View, after planner.LearningPlan) ([]int16, []int16, error) {
	kept := protectedSteps(before)
	if len(kept) == 0 {
		return nil, nil, nil
	}
	destinations := make(map[ranking.ResourceKey]itemDestination, len(kept))
	var ordinal int16
	for _, week := range after.Weeks {
		for _, item := range week.Items {
			if ordinal == planner.MaxLearningItems {
				return nil, nil, ErrInvalid
			}
			ordinal++
			destinations[item.ResourceKey] = itemDestination{item: item, ordinal: ordinal, from: week.From, through: week.Through}
		}
	}
	oldOrd, newOrd := make([]int16, 0, len(kept)), make([]int16, 0, len(kept))
	var last int16
	for _, saved := range kept {
		target, exists := destinations[saved.item.ResourceKey]
		canSupersede := saved.item.Assessment != nil && saved.item.Assessment.Verdict != "accepted"
		if !exists || target.ordinal <= last || saved.from != target.from || saved.through != target.through ||
			saved.item.Ranked != target.item.Ranked || saved.item.Stage != target.item.Stage ||
			(saved.item.Superseded && !target.item.Superseded) ||
			(!saved.item.Superseded && target.item.Superseded && !canSupersede) ||
			!slices.Equal(saved.item.RequiresCheck, target.item.RequiresCheck) {
			return nil, nil, ErrStepBlocked
		}
		oldOrd, newOrd = append(oldOrd, int16(saved.item.Ordinal)), append(newOrd, target.ordinal)
		last = target.ordinal
	}
	return oldOrd, newOrd, nil
}

func saveRebuiltVersion(ctx context.Context, tx pgx.Tx, source Sources, view View, draft Draft) error {
	if view.Version >= MaxVersionsPerGoal {
		return ErrLimit
	}
	if draft.Plan.RankingPolicy != view.RankingPolicy || draft.EvidencePolicy != view.EvidencePolicy {
		return ErrStaleRebuild
	}
	oldOrd, newOrd, err := carryOrdinals(view, draft.Plan)
	if err != nil {
		return err
	}
	version := view.Version + 1
	if err := saveDraft(ctx, tx, view.GoalID, version, source, draft, newOrd); err != nil {
		return err
	}
	if len(oldOrd) != 0 {
		if err := carryProgress(ctx, tx, view, version, oldOrd, newOrd); err != nil {
			return err
		}
		for i, step := range protectedSteps(view) {
			if step.item.observedGrade {
				item := step.item
				item.Ordinal = int(newOrd[i])
				target := View{GoalID: view.GoalID, Version: version}
				if err := recordAssessment(ctx, tx, source.actor, target, item); err != nil {
					return err
				}
			}
		}
	}
	tag, err := tx.Exec(ctx, "UPDATE study_plan_heads SET version=$3 WHERE goal_id=$1 AND version=$2",
		view.GoalID, view.Version, version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

func carryProgress(ctx context.Context, tx pgx.Tx, view View, version int64, oldOrd, newOrd []int16) error {
	batch := &pgx.Batch{}
	batch.Queue(`
		UPDATE study_plan_items AS target SET evidence_since=COALESCE(source.evidence_since,$6::timestamptz)
		FROM unnest($4::smallint[],$5::smallint[]) AS pairs(previous,current)
		JOIN study_plan_items AS source ON source.goal_id=$1 AND source.plan_version=$2 AND source.ordinal=pairs.previous
		WHERE target.goal_id=$1 AND target.plan_version=$3 AND target.ordinal=pairs.current`,
		view.GoalID, view.Version, version, oldOrd, newOrd, view.CreatedAt)
	batch.Queue(`
		INSERT INTO study_plan_activity (goal_id,plan_version,ordinal,started_at,completed_at)
		SELECT $1,$3,pairs.current,a.started_at,a.completed_at
		FROM unnest($4::smallint[],$5::smallint[]) AS pairs(previous,current)
		JOIN study_plan_activity AS a ON a.goal_id=$1 AND a.plan_version=$2 AND a.ordinal=pairs.previous`,
		view.GoalID, view.Version, version, oldOrd, newOrd)
	batch.Queue(`
		INSERT INTO study_plan_assessments (goal_id,plan_version,ordinal,submission_id,event_id,correction_version,verdict,graded_at)
		SELECT $1,$3,pairs.current,a.submission_id,a.event_id,a.correction_version,a.verdict,a.graded_at
		FROM unnest($4::smallint[],$5::smallint[]) AS pairs(previous,current)
		JOIN study_plan_assessments AS a ON a.goal_id=$1 AND a.plan_version=$2 AND a.ordinal=pairs.previous`,
		view.GoalID, view.Version, version, oldOrd, newOrd)
	batch.Queue(`
		INSERT INTO study_plan_explanations (goal_id,plan_version,ordinal,details)
		SELECT $1,$3,pairs.current,e.details
		FROM unnest($4::smallint[],$5::smallint[]) AS pairs(previous,current)
		JOIN study_plan_explanations AS e ON e.goal_id=$1 AND e.plan_version=$2 AND e.ordinal=pairs.previous`,
		view.GoalID, view.Version, version, oldOrd, newOrd)
	return tx.SendBatch(ctx, batch).Close()
}
