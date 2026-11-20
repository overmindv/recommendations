package planner

import "sort"

func recommendedLevel(accepted, failed int) int {
	level := 1
	if accepted >= 3 {
		level = 2
	}
	if accepted >= 6 {
		level = 3
	}
	if failed > 0 && level > 1 {
		level--
	}

	return level
}

func estimatedMinutes(task Task) int {
	d := difficulty(task.Difficulty)
	if task.TaskType == taskTypeProgramming {
		return []int{0, 20, 35, 50}[d]
	}

	return []int{0, 10, 15, 20}[d]
}

// topicInsights объясняет только наблюдаемые результаты; запланированное не считается знанием.
func topicInsights(in Request, topics map[string]Topic, results map[string]outcome, accepted, failed, planned map[string]int) []TopicInsight {
	targets := make(map[string]bool)
	for _, id := range in.TopicIDs {
		targets[id] = true
	}
	ordered := []string{}
	visited := make(map[string]bool)
	var visit func(string)
	visit = func(id string) {
		if visited[id] {
			return
		}
		visited[id] = true
		prerequisites := append([]string{}, topics[id].PrerequisiteIDs...)
		sort.Strings(prerequisites)
		for _, pre := range prerequisites {
			visit(pre)
		}
		ordered = append(ordered, id)
	}
	selected := append([]string{}, in.TopicIDs...)
	sort.Strings(selected)
	for _, id := range selected {
		visit(id)
	}
	insights := make([]TopicInsight, 0, len(ordered))
	for _, id := range ordered {
		level := recommendedLevel(accepted[id], failed[id])
		insight := TopicInsight{TopicID: id, IsTarget: targets[id], AcceptedTasks: accepted[id], FailedTasks: failed[id], PlannedTasks: planned[id], RemainingToReadiness: max(0, ReadinessTasks-accepted[id]), RecommendedDifficulty: []string{"", "easy", "medium", difficultyHard}[level], BlockedBy: []string{}}
		ancestors := make(map[string]bool)
		var collect func(string)
		collect = func(current string) {
			for _, pre := range topics[current].PrerequisiteIDs {
				if ancestors[pre] {
					continue
				}
				ancestors[pre] = true
				if accepted[pre] < ReadinessTasks {
					insight.BlockedBy = append(insight.BlockedBy, pre)
				}
				collect(pre)
			}
		}
		collect(id)
		sort.Strings(insight.BlockedBy)
		published, unsolved := 0, 0
		for _, task := range in.Tasks {
			if task.TopicID != id {
				continue
			}
			published++
			if results[task.ID].accepted {
				continue
			}
			unsolved++
			if difficulty(task.Difficulty) > level {
				continue
			}
			insight.EligibleTasks++
			minutes := estimatedMinutes(task)
			if insight.MinimumMinutes == 0 || minutes < insight.MinimumMinutes {
				insight.MinimumMinutes = minutes
			}
		}
		switch {
		case len(insight.BlockedBy) > 0:
			insight.NextAction = actionCompletePrerequisites
		case published == 0:
			insight.NextAction = "ADD_CONTENT"
		case unsolved == 0:
			insight.NextAction = "NO_NEW_TASKS"
		case insight.EligibleTasks == 0:
			insight.NextAction = "NEED_EASIER_TASKS"
		case insight.MinimumMinutes > in.Minutes:
			insight.NextAction = "INCREASE_BUDGET"
		default:
			insight.NextAction = "PRACTICE"
		}
		insights = append(insights, insight)
	}

	return insights
}
