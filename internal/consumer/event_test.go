package consumer

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/overmindv/content/pkg/api/activity"
	"github.com/twmb/franz-go/pkg/kgo"
)

const fixtureNamespace = "12345678-abcd-4123-8123"

const (
	verdictAccepted = "accepted"
	verdictWrong    = "wrong_answer"
	verdictInfra    = "infrastructure_error"
)

func fixtureID(namespace string, id int) string {
	return fmt.Sprintf("%s-%012x", namespace, id)
}

func outcomeFixture(namespace string, event, submission, learner, task int) taskEnvelope {
	at := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)

	return taskEnvelope{
		ID: fixtureID(namespace, event), Type: TasksTopic, Schema: 1, AggregateID: fixtureID(namespace, submission), Version: 1, OccurredAt: at,
		Payload: taskResult{SubmissionID: fixtureID(namespace, submission), LearnerID: fixtureID(namespace, learner),
			TaskID: fixtureID(namespace, task), VersionID: fixtureID(namespace, 40), Topics: []string{fixtureID(namespace, 41)},
			Verdict: verdictWrong, GradedAt: at, Independent: new(true), Correction: 1},
	}
}

func evidenceRecord(t testing.TB, topic, id string, offset int64, payload any) *kgo.Record {
	t.Helper()
	value, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}

	return &kgo.Record{Topic: topic, Key: []byte(id), Value: value, Offset: offset}
}

func TestOutcomeEnvelopeRejectsUntrustedShapes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		change func(*taskEnvelope)
	}{
		{"future schema", func(e *taskEnvelope) { e.Schema++ }},
		{"inconsistent correction", func(e *taskEnvelope) { e.Payload.Correction++ }},
		{"missing independence", func(e *taskEnvelope) { e.Payload.Independent = nil }},
		{"infrastructure", func(e *taskEnvelope) { e.Payload.Verdict = verdictInfra }},
		{"checker", func(e *taskEnvelope) { e.Payload.Verdict = "checker_error" }},
		{"invalid learner", func(e *taskEnvelope) { e.Payload.LearnerID = "learner" }},
		{"duplicate topic", func(e *taskEnvelope) { e.Payload.Topics = append(e.Payload.Topics, e.Payload.Topics[0]) }},
		{"missing topics", func(e *taskEnvelope) { e.Payload.Topics = nil }},
		{"bad grading time", func(e *taskEnvelope) { e.Payload.GradedAt = e.OccurredAt.Add(time.Second) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			wire := outcomeFixture(fixtureNamespace, 1, 2, 3, 4)
			tc.change(&wire)
			msg := decodeMessage(evidenceRecord(t, TasksTopic, wire.AggregateID, 3, wire), activity.DefaultLearningTopic)
			if msg.reject != invalidEvent || msg.outcome != nil {
				t.Fatal("unusable event reached the outcome projection:", msg)
			}
		})
	}
	wire := outcomeFixture(fixtureNamespace, 1, 2, 3, 4)
	record := evidenceRecord(t, TasksTopic, wire.AggregateID, 3, wire)
	bad := *record
	bad.Key = []byte(wire.Payload.TaskID)
	if got := decodeMessage(&bad, activity.DefaultLearningTopic); got.reject == "" {
		t.Fatal("mismatched Kafka key accepted")
	}
	for _, tc := range []struct {
		name  string
		value []byte
	}{
		{"unsupported field", append(append([]byte{}, record.Value[:len(record.Value)-1]...), []byte(`,"run_mode":"samples"}`)...)},
		{"trailing record", append(append([]byte{}, record.Value...), []byte(`{}`)...)},
		{"oversized record", append(append([]byte{}, record.Value...), strings.Repeat(" ", maxEventBytes)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			malformed := *record
			malformed.Value = tc.value
			if got := decodeMessage(&malformed, activity.DefaultLearningTopic); got.reject == "" {
				t.Fatal("malformed record accepted")
			}
		})
	}
	wire.Payload.Topics = []string{}
	if got := decodeMessage(evidenceRecord(t, TasksTopic, wire.AggregateID, 4, wire), activity.DefaultLearningTopic); got.reject != "" {
		t.Fatal("an untagged graded outcome should still be stored")
	}
}

func readFixture(namespace string, event, learner, material, revision int) activity.Event[activity.ReadConfirmation] {
	return activity.Event[activity.ReadConfirmation]{
		EventID: fixtureID(namespace, event), EventType: activity.MaterialReadConfirmed, SchemaVersion: activity.SchemaVersion,
		OccurredAt: time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC), AggregateID: fixtureID(namespace, material),
		AggregateVersion: 2, RevisionID: fixtureID(namespace, revision),
		Payload: activity.ReadConfirmation{UserID: fixtureID(namespace, learner), Evidence: activity.SelfReported},
	}
}

func TestMaterialEvidenceDoesNotClaimKnowledge(t *testing.T) {
	t.Parallel()

	wire := readFixture(fixtureNamespace, 1, 2, 3, 4)
	record := evidenceRecord(t, activity.DefaultLearningTopic, wire.AggregateID, 3, wire)
	msg := decodeMessage(record, activity.DefaultLearningTopic)
	if msg.reject != "" || msg.read == nil || msg.outcome != nil || msg.read.RevisionID != wire.RevisionID {
		t.Fatal("material confirmation not accepted as self-reported")
	}
	wire.Payload.Evidence = "verified"
	if msg := decodeMessage(evidenceRecord(t, activity.DefaultLearningTopic, wire.AggregateID, 3, wire), activity.DefaultLearningTopic); msg.reject == "" {
		t.Fatal("unverified Content evidence promoted to a task outcome")
	}
}
