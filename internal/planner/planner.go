package planner

import (
	"context"
)

type outcome struct{ accepted, failed bool }
type candidate struct {
	task    Task
	minutes int
	reason  string
	failed  bool
	fit     int
}

// Build — чистая детерминированная политика. Сформированный план не считается evidence.
func Build(ctx context.Context, in Request) (Plan, error) {
	if err := ctx.Err(); err != nil {
		return Plan{}, err
	}
	if err := Validate(in); err != nil {
		return Plan{}, err
	}
	plan := Plan{PolicyVersion: PolicyVersion, Warnings: []string{}, Items: []Item{}}
	if in.InputTruncated {
		plan.Warnings = append(plan.Warnings, "PARTIAL_DATA")
	}
	tasks := make(map[string]Task, len(in.Tasks))
	for _, task := range in.Tasks {
		tasks[task.ID] = task
	}
	results := make(map[string]outcome)
	for _, e := range in.Evidence {
		task, exists := tasks[e.TaskID]
		if !exists || task.TaskVersionID != e.TaskVersionID {
			continue
		}
		result := results[e.TaskID]
		switch e.Verdict {
		case "accepted":
			result.accepted = true
		case "wrong_answer", "compilation_error", "runtime_error", "time_limit_exceeded", "memory_limit_exceeded", "output_limit_exceeded":
			result.failed = true
		}
		results[e.TaskID] = result
	}
	accepted, failed := make(map[string]int), make(map[string]int)
	for taskID, result := range results {
		if result.accepted {
			accepted[tasks[taskID].TopicID]++
		} else if result.failed {
			failed[tasks[taskID].TopicID]++
		}
	}
	topics := make(map[string]Topic, len(in.Topics))
	for _, topic := range in.Topics {
		topics[topic.ID] = topic
	}
	unlocked := make(map[string]bool)
	checked := make(map[string]bool)
	var isUnlocked func(string) bool
	isUnlocked = func(id string) bool {
		if checked[id] {
			return unlocked[id]
		}
		checked[id] = true
		unlocked[id] = true
		for _, prerequisite := range topics[id].PrerequisiteIDs {
			if accepted[prerequisite] < ReadinessTasks || !isUnlocked(prerequisite) {
				unlocked[id] = false
			}
		}
		return unlocked[id]
	}
	eligible := make(map[string]bool)
	prerequisites := make(map[string]bool)
	visited := make(map[string]bool)
	blocked := false
	var addFrontier func(string, bool)
	addFrontier = func(id string, prerequisite bool) {
		if prerequisite {
			prerequisites[id] = true
		}
		if visited[id] {
			return
		}
		visited[id] = true
		if isUnlocked(id) {
			eligible[id] = true
			return
		}
		blocked = true
		for _, pre := range topics[id].PrerequisiteIDs {
			if accepted[pre] < ReadinessTasks || !isUnlocked(pre) {
				addFrontier(pre, true)
			}
		}
	}
	for _, id := range in.TopicIDs {
		addFrontier(id, false)
	}
	if blocked {
		plan.Warnings = append(plan.Warnings, "PREREQUISITE_GAP")
	}
	candidates := make([]candidate, 0, len(in.Tasks))
	for _, task := range in.Tasks {
		if !eligible[task.TopicID] || results[task.ID].accepted {
			continue
		}
		level := 1
		if accepted[task.TopicID] >= 3 {
			level = 2
		}
		if accepted[task.TopicID] >= 6 {
			level = 3
		}
		if failed[task.TopicID] > 0 && level > 1 {
			level--
		}
		d := difficulty(task.Difficulty)
		if d > level {
			continue
		}
		reason := "START_TOPIC"
		if accepted[task.TopicID] > 0 {
			reason = "NEXT_CHALLENGE"
		}
		if failed[task.TopicID] > 0 {
			reason = "PRACTICE_GAP"
		}
		if prerequisites[task.TopicID] {
			reason = "PREREQUISITE"
		}
		minutes := []int{0, 10, 15, 20}[d]
		if task.TaskType == "programming" {
			minutes = []int{0, 20, 35, 50}[d]
		}
		candidates = append(candidates, candidate{task: task, minutes: minutes, reason: reason, failed: results[task.ID].failed, fit: level - d})
	}
	selectedCount := make(map[string]int)
	selectedTypeCount := make(map[string]map[string]int)
	used := make(map[string]bool)
	for len(plan.Items) < MaxItems {
		if err := ctx.Err(); err != nil {
			return Plan{}, err
		}
		best := -1
		for i, c := range candidates {
			if used[c.task.ID] || plan.TotalMinutes+c.minutes > in.Minutes {
				continue
			}
			if best == -1 || betterCandidate(c, candidates[best], selectedCount, selectedTypeCount) {
				best = i
			}
		}
		if best == -1 {
			break
		}
		c := candidates[best]
		plan.Items = append(plan.Items, Item{TaskID: c.task.ID, TaskVersionID: c.task.TaskVersionID, TopicID: c.task.TopicID, Title: c.task.Title, Difficulty: c.task.Difficulty, TaskType: c.task.TaskType, EstimatedMinutes: c.minutes, ReasonCode: c.reason})
		plan.TotalMinutes += c.minutes
		used[c.task.ID] = true
		selectedCount[c.task.TopicID]++
		if selectedTypeCount[c.task.TopicID] == nil {
			selectedTypeCount[c.task.TopicID] = make(map[string]int)
		}
		selectedTypeCount[c.task.TopicID][c.task.TaskType]++
	}
	if len(plan.Items) == 0 {
		warning := "NO_ELIGIBLE_TASKS"
		if len(candidates) > 0 {
			warning = "BUDGET_TOO_SMALL"
		}
		plan.Warnings = append(plan.Warnings, warning)
	}
	return plan, nil
}

// betterCandidate сохраняет приоритет новой практики после ошибки и выравнивает
// форматы внутри темы. Последнее сравнение по ID делает выбор воспроизводимым.
func betterCandidate(a, b candidate, selectedCount map[string]int, selectedTypeCount map[string]map[string]int) bool {
	if selectedCount[a.task.TopicID] != selectedCount[b.task.TopicID] {
		return selectedCount[a.task.TopicID] < selectedCount[b.task.TopicID]
	}
	if a.failed != b.failed {
		return !a.failed
	}
	aTypes, bTypes := selectedTypeCount[a.task.TopicID], selectedTypeCount[b.task.TopicID]
	if aTypes[a.task.TaskType] != bTypes[b.task.TaskType] {
		return aTypes[a.task.TaskType] < bTypes[b.task.TaskType]
	}
	if a.fit != b.fit {
		return a.fit < b.fit
	}
	return a.task.ID < b.task.ID
}
