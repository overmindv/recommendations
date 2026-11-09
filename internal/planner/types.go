package planner

import "time"

const (
	PolicyVersion  = "practice-v2"
	MaxTopics      = 32
	MaxTasks       = 2000
	MaxEvidence    = 2000
	MaxItems       = 20
	ReadinessTasks = 3
)

type Request struct {
	TopicIDs       []string   `json:"topic_ids"`
	Minutes        int        `json:"minutes"`
	Topics         []Topic    `json:"topics"`
	Tasks          []Task     `json:"tasks"`
	Evidence       []Evidence `json:"evidence"`
	InputTruncated bool       `json:"input_truncated"`
}

type Topic struct {
	ID              string   `json:"id"`
	PrerequisiteIDs []string `json:"prerequisite_ids"`
}

type Task struct {
	ID            string `json:"id"`
	TaskVersionID string `json:"task_version_id"`
	TopicID       string `json:"topic_id"`
	Title         string `json:"title"`
	Difficulty    string `json:"difficulty"`
	TaskType      string `json:"task_type"`
}

type Evidence struct {
	TaskID        string    `json:"task_id"`
	TaskVersionID string    `json:"task_version_id"`
	Verdict       string    `json:"verdict"`
	OccurredAt    time.Time `json:"occurred_at"`
}

type Plan struct {
	PolicyVersion string   `json:"policy_version"`
	TotalMinutes  int      `json:"total_minutes"`
	Warnings      []string `json:"warnings"`
	Items         []Item   `json:"items"`
}

type Item struct {
	TaskID           string `json:"task_id"`
	TaskVersionID    string `json:"task_version_id"`
	TopicID          string `json:"topic_id"`
	Title            string `json:"title"`
	Difficulty       string `json:"difficulty"`
	TaskType         string `json:"task_type"`
	EstimatedMinutes int    `json:"estimated_minutes"`
	ReasonCode       string `json:"reason_code"`
}
