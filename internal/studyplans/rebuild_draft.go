package studyplans

import (
	"context"
	"maps"
	"slices"

	"github.com/overmindv/recommendations/internal/planner"
)

func (b *ProjectedBuilder) Rebuild(ctx context.Context, base RebuildBase) (*Draft, error) {
	before := base.Plan
	input, now, evidence, err := b.inputs(ctx, base.Source.actor, before.GoalID, base.Source, before.RankingPolicy, before.EvidencePolicy)
	if err != nil {
		return nil, err
	}
	keep, observed := continuation(before)
	keep.Rebudget = base.Source.approvedBudgetChange(before)
	plan, err := planner.ContinueLearningPlan(ctx, now, input, keep)
	if err != nil {
		return nil, err
	}
	scope := slices.Sorted(maps.Keys(input.Topics))
	if !observed && !keep.Rebudget && (len(before.scopeTopics) == 0 || slices.Equal(before.scopeTopics, scope)) && sameSchedule(before, plan) {
		return nil, nil
	}

	return &Draft{Plan: plan, EvidenceWatermark: evidence.Watermark, EvidencePolicy: before.EvidencePolicy, ScopeTopics: scope,
		EvidenceTopics: evidence.Topics, EvidenceCaptured: now}, nil
}

func continuation(view View) (planner.LearningContinuation, bool) {
	keep := planner.LearningContinuation{Weeks: make([]planner.LearningWeek, len(view.Weeks)), Remedial: make(map[string]bool)}
	failed := make(map[string]int)
	theory := make(map[string]int)
	var observed bool
	for i, week := range view.Weeks {
		keep.Weeks[i].StudyWeek = week.StudyWeek
		for _, row := range week.Items {
			if row.Skipped {
				continue
			}
			if row.ActivityStarted == nil && row.ActivityDone == nil && !row.hasReceipt && !row.observedGrade {
				continue
			}
			item := row.StudyItem
			if item.Stage == planner.StudyCheck && row.Assessment != nil && row.Assessment.Verdict != acceptedVerdict {
				item.Superseded = true
				failed[item.TopicID] = row.Ordinal
			}
			if item.Stage == planner.StudyTheory {
				theory[item.TopicID] = row.Ordinal
			}
			keep.Weeks[i].Items = append(keep.Weeks[i].Items, item)
			observed = observed || row.observedGrade
		}
	}
	for topic, last := range failed {
		keep.Remedial[topic] = theory[topic] < last
	}

	return keep, observed
}

func sameSchedule(view View, next planner.LearningPlan) bool {
	if !slices.EqualFunc(view.UnmetOutcomes, next.UnmetOutcomes, func(a, b planner.UnmetOutcome) bool {
		return a.TopicID == b.TopicID && a.Stage == b.Stage && a.Reason == b.Reason && slices.Equal(a.BlockedBy, b.BlockedBy)
	}) || len(view.Weeks) != len(next.Weeks) {
		return false
	}
	for i, week := range view.Weeks {
		other := next.Weeks[i]
		if week.From != other.From || week.Through != other.Through ||
			!slices.EqualFunc(week.Items, other.Items, func(a Item, b planner.StudyItem) bool {
				return !a.Skipped && a.ResourceKey == b.ResourceKey && a.TopicID == b.TopicID && a.Stage == b.Stage &&
					a.Minutes == b.Minutes && a.Superseded == b.Superseded && slices.Equal(a.RequiresCheck, b.RequiresCheck)
			}) {
			return false
		}
	}

	return true
}
