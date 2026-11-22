package goalcompletion

import (
	"errors"
	"regexp"
	"slices"
	"time"
)

const (
	Topic         = "learning.goal.v1"
	SchemaVersion = 1
	maxTopics     = 16
)

type Type string

const (
	Completed Type = "goal.completed"
	Reopened  Type = "goal.reopened"
)

type Event struct {
	EventID          string    `json:"event_id"`
	Type             Type      `json:"event_type"`
	SchemaVersion    int       `json:"schema_version"`
	AggregateID      string    `json:"aggregate_id"`
	AggregateVersion int64     `json:"aggregate_version"`
	OccurredAt       time.Time `json:"occurred_at"`
	CorrelationID    *string   `json:"correlation_id,omitempty"`
	Payload          Payload   `json:"payload"`
}

type Payload struct {
	LearnerID         string   `json:"learner_id"`
	BusinessAwardKey  string   `json:"business_award_key"`
	CompletionVersion int64    `json:"completion_version"`
	GoalVersion       int64    `json:"goal_version"`
	PlanVersion       int64    `json:"plan_version"`
	TargetTopicIDs    []string `json:"target_topic_ids"`
}

var (
	ErrInvalid = errors.New("invalid goal completion event")
	uuid       = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

func BusinessAwardKey(goalID string) string {
	return "learning_goal:" + goalID
}

func (e Event) Validate() error {
	_, offset := e.OccurredAt.Zone()
	if !validID(e.EventID) || !validID(e.AggregateID) || !validID(e.Payload.LearnerID) ||
		e.SchemaVersion != SchemaVersion || (e.Type != Completed && e.Type != Reopened) ||
		e.AggregateVersion < 1 || e.Payload.CompletionVersion != e.AggregateVersion || e.OccurredAt.IsZero() || offset != 0 ||
		(e.CorrelationID != nil && !validID(*e.CorrelationID)) ||
		e.Payload.GoalVersion < 1 || e.Payload.PlanVersion < 1 || e.Payload.BusinessAwardKey != BusinessAwardKey(e.AggregateID) ||
		len(e.Payload.TargetTopicIDs) < 1 || len(e.Payload.TargetTopicIDs) > maxTopics || !slices.IsSorted(e.Payload.TargetTopicIDs) {
		return ErrInvalid
	}
	for i, id := range e.Payload.TargetTopicIDs {
		if !validID(id) || (i > 0 && id == e.Payload.TargetTopicIDs[i-1]) {
			return ErrInvalid
		}
	}

	return nil
}

func validID(id string) bool {
	return id != "00000000-0000-0000-0000-000000000000" && uuid.MatchString(id)
}
