package diagnostic

import (
	"cmp"
	"errors"
	"slices"

	"github.com/overmindv/recommendations/internal/planner"
)

const (
	PolicyVersion = "diagnostic-v1"
	MaxTasks      = 4
	MaxMinutes    = 20
	MaxTopics     = 32
	MaxCandidates = 256
)

var ErrInvalidSelection = errors.New("invalid diagnostic selection input")

type Scope struct {
	Targets       []string
	Prerequisites []string
}

// Candidate уже прошел проверку версии и истории в Tasks.
type Candidate struct {
	TaskID    string `json:"task_id"`
	VersionID string `json:"task_version_id"`
	TopicID   string `json:"topic_id"`
	Minutes   int    `json:"estimated_minutes"`
}

type StopRule struct {
	PolicyVersion string `json:"policy_version"`
	MaxTasks      int    `json:"max_tasks"`
	BudgetMinutes int    `json:"budget_minutes"`
}

type Selection struct {
	Tasks            []Candidate `json:"tasks"`
	EstimatedMinutes int         `json:"estimated_minutes"`
	StopRule         StopRule    `json:"stop_rule"`
}

const (
	target = iota + 1
	prerequisite
)

func Select(scope Scope, candidates []Candidate, budget int) (Selection, error) {
	if budget < 1 || budget > MaxMinutes || len(scope.Targets) == 0 ||
		len(scope.Targets)+len(scope.Prerequisites) > MaxTopics || len(candidates) > MaxCandidates {
		return Selection{}, ErrInvalidSelection
	}
	topics := make(map[string]int, len(scope.Targets)+len(scope.Prerequisites))
	for _, group := range []struct {
		ids  []string
		role int
	}{{scope.Targets, target}, {scope.Prerequisites, prerequisite}} {
		for _, id := range group.ids {
			if !planner.ValidID(id) || topics[id] != 0 {
				return Selection{}, ErrInvalidSelection
			}
			topics[id] = group.role
		}
	}

	ranked := make([]Candidate, 0, len(candidates))
	for _, c := range candidates {
		if topics[c.TopicID] == 0 || !planner.ValidID(c.TaskID) || !planner.ValidID(c.VersionID) ||
			c.Minutes < 0 || c.Minutes > 1440 {
			return Selection{}, ErrInvalidSelection
		}
		if c.Minutes > 0 && c.Minutes <= budget {
			ranked = append(ranked, c)
		}
	}
	slices.SortFunc(ranked, compareCandidate)
	result := Selection{
		Tasks:    make([]Candidate, 0, MaxTasks),
		StopRule: StopRule{PolicyVersion: PolicyVersion, MaxTasks: MaxTasks, BudgetMinutes: budget},
	}
	var addedTasks, addedTopics [MaxTasks]string
	part := target
	for len(result.Tasks) < MaxTasks {
		n := len(result.Tasks)
		left := budget - result.EstimatedMinutes
		i := nextCandidate(ranked, topics, part, left, addedTasks[:n], addedTopics[:n])
		if i < 0 {
			i = nextCandidate(ranked, topics, otherRole(part), left, addedTasks[:n], addedTopics[:n])
		}
		if i < 0 {
			break
		}
		c := ranked[i]
		result.Tasks = append(result.Tasks, c)
		result.EstimatedMinutes += c.Minutes
		addedTasks[n], addedTopics[n] = c.TaskID, c.TopicID
		part = otherRole(topics[c.TopicID])
	}
	return result, nil
}

func nextCandidate(rows []Candidate, topics map[string]int, role, left int, usedTasks, usedTopics []string) int {
	for i, c := range rows {
		if c.Minutes > left {
			break
		}
		if topics[c.TopicID] == role && !slices.Contains(usedTasks, c.TaskID) && !slices.Contains(usedTopics, c.TopicID) {
			return i
		}
	}
	return -1
}

func otherRole(role int) int {
	if role == target {
		return prerequisite
	}
	return target
}

func compareCandidate(a, b Candidate) int {
	return cmp.Or(
		cmp.Compare(a.Minutes, b.Minutes),
		cmp.Compare(a.TopicID, b.TopicID),
		cmp.Compare(a.TaskID, b.TaskID),
		cmp.Compare(a.VersionID, b.VersionID),
	)
}
