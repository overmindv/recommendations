package consumer

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"time"

	"github.com/overmindv/content/pkg/api/activity"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	TasksTopic    = "learning.outcome.v1"
	maxEventBytes = 8 << 10
	sourceTasks   = "tasks"
	sourceContent = "content"
	invalidEvent  = "invalid_event"
)

type taskResult struct {
	SubmissionID string    `json:"submission_id"`
	LearnerID    string    `json:"learner_id"`
	TaskID       string    `json:"task_id"`
	VersionID    string    `json:"task_version_id"`
	Topics       []string  `json:"topic_ids"`
	Verdict      string    `json:"verdict"`
	GradedAt     time.Time `json:"graded_at"`
	Independent  *bool     `json:"first_independent_attempt"`
	Correction   int64     `json:"correction_version"`
}

type taskEnvelope struct {
	ID            string     `json:"event_id"`
	Type          string     `json:"event_type"`
	Schema        int        `json:"schema_version"`
	AggregateID   string     `json:"aggregate_id"`
	Version       int64      `json:"aggregate_version"`
	OccurredAt    time.Time  `json:"occurred_at"`
	CorrelationID *string    `json:"correlation_id,omitempty"`
	Payload       taskResult `json:"payload"`
}

type materialRead struct {
	LearnerID  string
	MaterialID string
	RevisionID string
	At         time.Time
}

type message struct {
	topic       string
	partition   int32
	offset      int64
	source      string
	id          string
	fingerprint [sha256.Size]byte
	outcome     *taskResult
	read        *materialRead
	reject      string
}

func decodeMessage(record *kgo.Record, contentTopic string) message {
	msg := message{topic: record.Topic, partition: record.Partition, offset: record.Offset, reject: invalidEvent}
	if len(record.Value) == 0 || len(record.Value) > maxEventBytes || len(record.Key) != 36 {
		return msg
	}
	switch record.Topic {
	case TasksTopic:
		var wire taskEnvelope
		if !decodeJSON(record.Value, &wire) || !wire.valid(string(record.Key)) {
			return msg
		}
		slices.Sort(wire.Payload.Topics)
		msg.source, msg.id, msg.outcome = sourceTasks, wire.ID, &wire.Payload
	case contentTopic:
		var wire activity.Event[activity.ReadConfirmation]
		if !decodeJSON(record.Value, &wire) || wire.EventType != activity.MaterialReadConfirmed ||
			wire.Payload.Evidence != activity.SelfReported || wire.SchemaVersion != activity.SchemaVersion ||
			wire.AggregateID != string(record.Key) || wire.AggregateVersion < 1 || wire.OccurredAt.IsZero() ||
			!planner.ValidID(wire.EventID) || !planner.ValidID(wire.Payload.UserID) ||
			!planner.ValidID(wire.AggregateID) || !planner.ValidID(wire.RevisionID) {
			return msg
		}
		msg.source, msg.id = sourceContent, wire.EventID
		msg.read = &materialRead{LearnerID: wire.Payload.UserID, MaterialID: wire.AggregateID, RevisionID: wire.RevisionID, At: wire.OccurredAt.UTC()}
	default:
		return msg
	}
	msg.reject = ""
	msg.fingerprint = sha256.Sum256(record.Value)

	return msg
}

func (e taskEnvelope) valid(key string) bool {
	p := e.Payload
	if e.Schema != 1 || e.Type != TasksTopic || e.AggregateID != p.SubmissionID || e.AggregateID != key ||
		e.Version != p.Correction || e.Version < 1 || (e.CorrelationID != nil && !planner.ValidID(*e.CorrelationID)) ||
		e.OccurredAt.IsZero() || p.GradedAt.IsZero() || p.GradedAt.After(e.OccurredAt) || p.Independent == nil ||
		p.Topics == nil || len(p.Topics) > 8 || !validVerdict(p.Verdict) {
		return false
	}
	for _, id := range []string{e.ID, p.SubmissionID, p.LearnerID, p.TaskID, p.VersionID} {
		if !planner.ValidID(id) {
			return false
		}
	}
	for i, id := range p.Topics {
		if !planner.ValidID(id) || slices.Contains(p.Topics[:i], id) {
			return false
		}
	}

	return true
}

func (p taskResult) kind() string {
	if *p.Independent {
		return "independent"
	}

	return "non_independent"
}

func validVerdict(verdict string) bool {
	switch verdict {
	case "accepted", "wrong_answer", "compilation_error", "runtime_error",
		"time_limit_exceeded", "memory_limit_exceeded", "output_limit_exceeded":
		return true
	default:
		return false
	}
}

func decodeJSON(raw []byte, target any) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	return decoder.Decode(target) == nil && errors.Is(decoder.Decode(new(any)), io.EOF)
}
