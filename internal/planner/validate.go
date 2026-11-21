package planner

import (
	"errors"
	"regexp"
	"strings"
	"unicode/utf8"
)

var ErrInvalidInput = errors.New("invalid practice plan request")
var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func ValidID(value string) bool {
	return value != "00000000-0000-0000-0000-000000000000" && uuidPattern.MatchString(value)
}

// Validate отвергает неполные графы: отсутствие prerequisite не означает готовность.
func Validate(in Request) error {
	if !utf8.ValidString(in.Goal) || utf8.RuneCountInString(in.Goal) > 1000 {
		return ErrInvalidInput
	}
	if in.SessionCount < 0 || in.SessionCount > MaxSessions {
		return ErrInvalidInput
	}
	if len(in.TopicIDs) < 1 || len(in.TopicIDs) > 5 || in.Minutes < 10 || in.Minutes > 240 || len(in.Topics) < 1 || len(in.Topics) > MaxTopics || len(in.Tasks) > MaxTasks || len(in.Evidence) > MaxEvidence {
		return ErrInvalidInput
	}
	topics := make(map[string]Topic, len(in.Topics))
	for _, topic := range in.Topics {
		if !utf8.ValidString(topic.Title) || len(topic.Title) > 2000 || !ValidID(topic.ID) || len(topic.PrerequisiteIDs) >= MaxTopics {
			return ErrInvalidInput
		}
		if _, exists := topics[topic.ID]; exists {
			return ErrInvalidInput
		}
		topics[topic.ID] = topic
	}
	selected := make(map[string]bool)
	for _, id := range in.TopicIDs {
		if _, exists := topics[id]; !exists || selected[id] {
			return ErrInvalidInput
		}
		selected[id] = true
	}
	for _, topic := range in.Topics {
		seen := make(map[string]bool)
		for _, id := range topic.PrerequisiteIDs {
			if _, exists := topics[id]; !exists || seen[id] {
				return ErrInvalidInput
			}
			seen[id] = true
		}
	}
	colors := make(map[string]int)
	var visit func(string) bool
	visit = func(id string) bool {
		if colors[id] == 1 {
			return false
		}
		if colors[id] == 2 {
			return true
		}
		colors[id] = 1
		for _, prerequisite := range topics[id].PrerequisiteIDs {
			if !visit(prerequisite) {
				return false
			}
		}
		colors[id] = 2

		return true
	}
	for id := range topics {
		if !visit(id) {
			return ErrInvalidInput
		}
	}
	tasks := make(map[string]bool)
	versions := make(map[string]bool)
	for _, task := range in.Tasks {
		_, topicExists := topics[task.TopicID]
		if !ValidID(task.ID) || !ValidID(task.TaskVersionID) || tasks[task.ID] || versions[task.TaskVersionID] || !topicExists || strings.TrimSpace(task.Title) == "" || !utf8.ValidString(task.Title) || len(task.Title) > 2000 || difficulty(task.Difficulty) == 0 {
			return ErrInvalidInput
		}
		if task.CatalogMinutes < 0 || task.CatalogMinutes > 1440 {
			return ErrInvalidInput
		}
		switch task.TaskType {
		case "single_choice", "multiple_choice", taskTypeProgramming:
		default:
			return ErrInvalidInput
		}
		tasks[task.ID], versions[task.TaskVersionID] = true, true
	}
	for _, e := range in.Evidence {
		if !ValidID(e.TaskID) || !ValidID(e.TaskVersionID) || e.OccurredAt.IsZero() {
			return ErrInvalidInput
		}
		switch e.Verdict {
		case verdictAccepted, "wrong_answer", verdictCompilationError, "runtime_error", "time_limit_exceeded", "memory_limit_exceeded", "output_limit_exceeded", "infrastructure_error", "checker_error", "canceled":
		default:
			return ErrInvalidInput
		}
	}

	return nil
}

func difficulty(value string) int {
	switch value {
	case difficultyEasy:
		return 1
	case "medium":
		return 2
	case difficultyHard:
		return 3
	}

	return 0
}
