package evaluation

import (
	"slices"

	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

type planStats struct {
	Targets          int         `json:"targets"`
	Covered          int         `json:"covered"`
	AvailableMinutes int         `json:"available_minutes"`
	PlannedMinutes   int         `json:"planned_minutes"`
	ScheduledTopics  int         `json:"scheduled_topics"`
	ResourceKinds    int         `json:"resource_kinds"`
	DifficultyLevels int         `json:"difficulty_levels"`
	Steps            []planStep  `json:"steps"`
	Gaps             []gap       `json:"gaps"`
	Violations       []violation `json:"violations"`
	checks           map[string]bool
}

type planStep struct {
	Resource      string             `json:"resource"`
	Revision      string             `json:"revision"`
	Topic         string             `json:"topic"`
	Week          int                `json:"week"`
	Stage         planner.StudyStage `json:"stage"`
	Minutes       int                `json:"minutes"`
	RequiresCheck []string           `json:"requires_checks,omitempty"`
}

type violation struct {
	Code  string `json:"code"`
	Topic string `json:"topic,omitempty"`
	Step  int    `json:"step,omitempty"`
}

func auditPlan(s snapshot, plan planner.LearningPlan) planStats {
	out := planStats{Targets: len(s.request.TargetTopicIDs), Steps: []planStep{},
		Gaps: []gap{}, Violations: []violation{}, checks: make(map[string]bool)}
	for _, week := range s.calendar.Weeks {
		out.AvailableMinutes += week.CapacityMinutes
	}
	if plan.Timezone != s.calendar.Timezone || plan.WeeklyMinutes != s.calendar.WeeklyMinutes || len(plan.Weeks) != len(s.calendar.Weeks) {
		out.Violations = append(out.Violations, violation{Code: "calendar_mismatch"})
	}
	seen := make(map[string]bool)
	scheduled := make(map[string]bool)
	for _, week := range plan.Weeks {
		for _, item := range week.Items {
			scheduled[item.ID] = true
		}
	}
	topics := make(map[string]bool)
	kinds := make(map[ranking.Kind]bool)
	difficulties := make(map[string]bool)
	guards := make(map[string][]string, len(s.request.Topics))
	stages := make(map[string]planner.StudyStage, len(s.request.Topics))
	step := 0
	for i, week := range plan.Weeks {
		minutes := 0
		for _, item := range week.Items {
			step++
			minutes += item.Minutes
			entry, present := s.byKey[item.ResourceKey]
			label := s.aliases[item.TopicID]
			if !present || !s.admissible(entry) || entry.item.TopicID != item.TopicID || entry.spec.Stage != item.Stage ||
				entry.item.Minutes != item.Minutes || item.Superseded {
				out.Violations = append(out.Violations, violation{Code: "prohibited_resource", Topic: label, Step: step})

				continue
			}
			if seen[item.ID] {
				out.Violations = append(out.Violations, violation{Code: "duplicate_resource", Topic: label, Step: step})

				continue
			}
			seen[item.ID] = true
			if item.Decision != nil && item.Decision.Alternative != nil {
				alt := item.Decision.Alternative
				alternative, ok := s.byKey[alt.ResourceKey]
				if !ok || !s.admissible(alternative) || scheduled[alt.ID] || alternative.item.TopicID != item.TopicID ||
					alternative.spec.Stage != item.Stage || alternative.item.Minutes != alt.Minutes ||
					i >= len(s.calendar.Weeks) || week.PlannedMinutes-item.Minutes+alt.Minutes > s.calendar.Weeks[i].CapacityMinutes {
					out.Violations = append(out.Violations, violation{Code: "prohibited_alternative", Topic: label, Step: step})
				}
			}
			needed, ok := guards[item.TopicID]
			if !ok {
				needed = s.guards(item.TopicID)
				guards[item.TopicID] = needed
			}
			valid := true
			if slices.ContainsFunc(needed, func(pre string) bool { return !out.checks[s.aliases[pre]] }) {
				out.Violations = append(out.Violations, violation{Code: "prerequisite_order", Topic: label, Step: step})
				valid = false
			}
			if !slices.Equal(needed, item.RequiresCheck) {
				out.Violations = append(out.Violations, violation{Code: "prerequisite_gate", Topic: label, Step: step})
				valid = false
			}
			if !nextStage(stages[item.TopicID], item.Stage) {
				out.Violations = append(out.Violations, violation{Code: "stage_order", Topic: label, Step: step})
				valid = false
			}
			if !valid {
				continue
			}
			stages[item.TopicID] = item.Stage
			if item.Stage == planner.StudyCheck {
				out.checks[label] = true
			}
			stepDetails := planStep{Resource: entry.spec.ID, Revision: entry.spec.Revision, Topic: label,
				Week: i + 1, Stage: item.Stage, Minutes: item.Minutes}
			for _, guard := range needed {
				stepDetails.RequiresCheck = append(stepDetails.RequiresCheck, s.aliases[guard])
			}
			out.Steps = append(out.Steps, stepDetails)
			topics[item.TopicID], kinds[item.Kind] = true, true
			if entry.spec.Difficulty != "" {
				difficulties[entry.spec.Difficulty] = true
			}
		}
		out.PlannedMinutes += minutes
		if i >= len(s.calendar.Weeks) || week.StudyWeek != s.calendar.Weeks[i] {
			out.Violations = append(out.Violations, violation{Code: "calendar_mismatch"})
		}
		if week.PlannedMinutes != minutes || (i < len(s.calendar.Weeks) && minutes > s.calendar.Weeks[i].CapacityMinutes) {
			out.Violations = append(out.Violations, violation{Code: "budget_exceeded"})
		}
	}
	if step > planner.MaxLearningItems {
		out.Violations = append(out.Violations, violation{Code: "plan_limit"})
	}
	for _, id := range s.request.TargetTopicIDs {
		if out.checks[s.aliases[id]] || s.known(id, s.request.Ranking.TargetBPS) {
			out.Covered++
		}
	}
	for _, outcome := range plan.UnmetOutcomes {
		out.Gaps = append(out.Gaps, gap{Topic: s.aliases[outcome.TopicID], Reason: outcome.Reason})
	}
	out.ScheduledTopics, out.ResourceKinds, out.DifficultyLevels = len(topics), len(kinds), len(difficulties)

	return out
}

func nextStage(previous, current planner.StudyStage) bool {
	switch previous {
	case "":
		return current == planner.StudyTheory
	case planner.StudyTheory:
		return current == planner.StudyPractice
	case planner.StudyPractice:
		return current == planner.StudyCheck
	default:
		return false
	}
}

func checkExpectations(stats planStats, expect expectations) []violation {
	failed := []violation{}
	for _, topic := range expect.Checks {
		if !stats.checks[topic] {
			failed = append(failed, violation{Code: "expected_check_missing", Topic: topic})
		}
	}
	for _, missing := range expect.Gaps {
		if !slices.Contains(stats.Gaps, missing) || stats.checks[missing.Topic] {
			failed = append(failed, violation{Code: "expected_gap_missing", Topic: missing.Topic})
		}
	}

	return failed
}
