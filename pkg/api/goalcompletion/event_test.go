package goalcompletion

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestReopenedUsesTheSameAwardKeyAndASeparateEventID(t *testing.T) {
	t.Parallel()

	const (
		goal    = "abcdef00-1234-4000-8000-000000000001"
		learner = "abcdef00-1234-4000-8000-000000000002"
		topic   = "abcdef00-1234-4000-8000-000000000003"
	)
	completed := Event{
		EventID: "abcdef00-1234-4000-8000-000000000004", Type: Completed, OccurredAt: time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC),
		SchemaVersion: SchemaVersion, AggregateID: goal, AggregateVersion: 1,
		Payload: Payload{LearnerID: learner, BusinessAwardKey: BusinessAwardKey(goal), CompletionVersion: 1,
			GoalVersion: 3, PlanVersion: 7, TargetTopicIDs: []string{topic}},
	}
	reopened := completed
	reopened.Type, reopened.EventID = Reopened, "abcdef00-1234-4000-8000-000000000005"
	reopened.AggregateVersion, reopened.Payload.CompletionVersion = 2, 2
	if err := completed.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Validate(); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(reopened)
	if err != nil {
		t.Fatal(err)
	}
	var actual Event
	if err := json.Unmarshal(data, &actual); err != nil || !reflect.DeepEqual(actual, reopened) ||
		actual.Payload.BusinessAwardKey != completed.Payload.BusinessAwardKey || actual.EventID == completed.EventID {
		t.Fatal("reopen changed the business identity or lost its event version:", err)
	}
	actual.Payload.CompletionVersion = 1
	if err := actual.Validate(); err == nil {
		t.Fatal("event accepted mismatched completion and aggregate versions")
	}
}
