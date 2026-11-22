package evaluation

import (
	"context"
	"slices"

	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

func staticPlan(ctx context.Context, s snapshot, units []unit) (planner.LearningPlan, error) {
	plan := planner.LearningPlan{
		Timezone: s.calendar.Timezone, WeeklyMinutes: s.calendar.WeeklyMinutes,
		HorizonLimited: s.calendar.HorizonLimited, Weeks: make([]planner.LearningWeek, len(s.calendar.Weeks)),
	}
	for i, week := range s.calendar.Weeks {
		plan.Weeks[i].StudyWeek = week
	}
	checked := make(map[string]bool, len(units))
	cursor, count := 0, 0
	for _, row := range units {
		if err := ctx.Err(); err != nil {
			return planner.LearningPlan{}, err
		}
		topicID := scopedID("topic", row.Topic)
		if _, inScope := s.request.Topics[topicID]; !inScope {
			continue
		}
		guards := s.guards(topicID)
		var blocked []string
		for _, prerequisite := range guards {
			if !checked[prerequisite] {
				blocked = append(blocked, prerequisite)
			}
		}
		if len(blocked) > 0 {
			plan.UnmetOutcomes = append(plan.UnmetOutcomes, planner.UnmetOutcome{TopicID: topicID, Reason: "prerequisite", BlockedBy: blocked})

			continue
		}
		if count+len(row.Sequence) > planner.MaxLearningItems {
			plan.UnmetOutcomes = append(plan.UnmetOutcomes, planner.UnmetOutcome{TopicID: topicID, Reason: "plan_limit"})

			continue
		}
		last, missing, placed := placeUnit(plan.Weeks, cursor, s, topicID, row.Sequence, guards)
		if missing != nil {
			plan.UnmetOutcomes = append(plan.UnmetOutcomes, *missing)

			continue
		}
		count += placed
		cursor, checked[topicID] = last, true
	}

	return plan, nil
}

func placeUnit(weeks []planner.LearningWeek, cursor int, s snapshot, topic string, sequence, guards []string) (int, *planner.UnmetOutcome, int) {
	stages := [...]planner.StudyStage{planner.StudyTheory, planner.StudyPractice, planner.StudyCheck}
	if len(sequence) != len(stages) {
		return 0, &planner.UnmetOutcome{TopicID: topic, Reason: "missing_resource"}, 0
	}
	var choices [len(stages)]candidate
	for i, stage := range stages {
		entry, ok := s.byName[sequence[i]]
		if !ok || !s.admissible(entry) {
			return 0, &planner.UnmetOutcome{TopicID: topic, Stage: stage, Reason: "missing_resource"}, 0
		}
		choices[i] = entry
	}
	type reservation struct {
		week  *planner.LearningWeek
		entry candidate
	}
	var buffer [len(stages)]reservation
	reserved := buffer[:0]
	for _, entry := range choices {
		for cursor < len(weeks) && weeks[cursor].CapacityMinutes-weeks[cursor].PlannedMinutes < entry.item.Minutes {
			cursor++
		}
		if cursor == len(weeks) {
			for _, prior := range reserved {
				prior.week.PlannedMinutes -= prior.entry.item.Minutes
			}

			return 0, &planner.UnmetOutcome{TopicID: topic, Stage: entry.spec.Stage, Reason: "time_budget"}, 0
		}
		week := &weeks[cursor]
		week.PlannedMinutes += entry.item.Minutes
		reserved = append(reserved, reservation{week: week, entry: entry})
	}
	for _, choice := range reserved {
		entry := choice.entry
		choice.week.Items = append(choice.week.Items, planner.StudyItem{
			Ranked: ranking.Ranked{ResourceKey: entry.item.ResourceKey, TopicID: topic, Minutes: entry.item.Minutes},
			Stage:  entry.spec.Stage, RequiresCheck: slices.Clone(guards),
		})
	}

	return cursor, nil, len(stages)
}
