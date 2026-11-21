package planner

import "time"

const (
	PolicyVersion  = "practice-v3"
	MaxTopics      = 32
	MaxTasks       = 2000
	MaxEvidence    = 2000
	MaxItems       = 20
	MaxSessions    = 8
	ReadinessTasks = 3
)

type Request struct {
	Goal           string     `json:"goal,omitempty"`
	TopicIDs       []string   `json:"topic_ids"`
	Minutes        int        `json:"minutes"`
	SessionCount   int        `json:"session_count,omitempty"`
	Topics         []Topic    `json:"topics"`
	Tasks          []Task     `json:"tasks"`
	Evidence       []Evidence `json:"evidence"`
	InputTruncated bool       `json:"input_truncated"`
}

type Topic struct {
	Title           string   `json:"title,omitempty"`
	ID              string   `json:"id"`
	PrerequisiteIDs []string `json:"prerequisite_ids"`
}

type Task struct {
	CatalogMinutes int    `json:"-"`
	ID             string `json:"id"`
	TaskVersionID  string `json:"task_version_id"`
	TopicID        string `json:"topic_id"`
	Title          string `json:"title"`
	Difficulty     string `json:"difficulty"`
	TaskType       string `json:"task_type"`
}

type Evidence struct {
	TaskID        string    `json:"task_id"`
	TaskVersionID string    `json:"task_version_id"`
	Verdict       string    `json:"verdict"`
	OccurredAt    time.Time `json:"occurred_at"`
}

type Plan struct {
	AIStatus            string         `json:"ai_status,omitempty"`
	PolicyVersion       string         `json:"policy_version"`
	TotalMinutes        int            `json:"total_minutes"`
	Warnings            []string       `json:"warnings"`
	Items               []Item         `json:"items"`
	RequestedSessions   int            `json:"requested_sessions"`
	TotalPlannedMinutes int            `json:"total_planned_minutes"`
	Sessions            []Session      `json:"sessions"`
	TopicInsights       []TopicInsight `json:"topic_insights"`
}

type Session struct {
	Number       int    `json:"number"`
	TotalMinutes int    `json:"total_minutes"`
	Items        []Item `json:"items"`
}

type TopicInsight struct {
	TopicID               string   `json:"topic_id"`
	IsTarget              bool     `json:"is_target"`
	AcceptedTasks         int      `json:"accepted_tasks"`
	FailedTasks           int      `json:"failed_tasks"`
	EligibleTasks         int      `json:"eligible_tasks"`
	PlannedTasks          int      `json:"planned_tasks"`
	RemainingToReadiness  int      `json:"remaining_to_readiness"`
	RecommendedDifficulty string   `json:"recommended_difficulty"`
	MinimumMinutes        int      `json:"minimum_minutes"`
	BlockedBy             []string `json:"blocked_by"`
	NextAction            string   `json:"next_action"`
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
