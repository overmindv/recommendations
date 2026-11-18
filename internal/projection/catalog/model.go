package catalog

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/overmindv/recommendations/internal/planner"
)

const (
	PageLimit       = 200
	maxSnapshotRows = 1000000
	maxGraphEdges   = 250000
)

var (
	ErrConcurrent = errors.New("catalog checkpoint changed")
	ErrNotReady   = errors.New("catalog graph not initialized")
	ErrNotFound   = errors.New("catalog topic not found")
)

type LearningMetadata struct {
	EstimatedMinutes *int     `json:"estimated_minutes"`
	Outcomes         []string `json:"outcomes"`
}

type Topic struct {
	Title            string            `json:"title"`
	CourseID         *string           `json:"course_id"`
	ParentTopicID    *string           `json:"parent_topic_id"`
	OrderIndex       int               `json:"order_index"`
	Difficulty       string            `json:"difficulty"`
	Status           string            `json:"status"`
	LearningMetadata *LearningMetadata `json:"learning_metadata,omitempty"`
}

type Record struct {
	Kind           string `json:"kind"`
	TopicID        string `json:"topic_id"`
	PrerequisiteID string `json:"prerequisite_topic_id,omitempty"`
	Tombstone      bool   `json:"tombstone"`
	Topic          *Topic `json:"topic,omitempty"`
}

type Row struct {
	Revision int64  `json:"catalog_revision"`
	Record   Record `json:"record"`
}

type Event struct {
	ID               string `json:"event_id"`
	AggregateVersion int64  `json:"aggregate_version"`
	Row
}

type SnapshotPage struct {
	Watermark  *int64 `json:"watermark"`
	Records    []Row  `json:"records"`
	NextCursor string `json:"next_cursor"`
}

type ChangePage struct {
	Watermark *int64  `json:"watermark"`
	Through   *int64  `json:"through"`
	Changes   []Event `json:"changes"`
	More      bool    `json:"more"`
}

type rejected struct {
	revision int64
	eventID  string
	reason   string
}

func (r *rejected) Error() string { return "catalog graph quarantined: " + r.reason }

func invalid(reason string) *rejected { return &rejected{reason: reason} }

func (r Record) validate() error {
	if !planner.ValidID(r.TopicID) {
		return invalid("invalid_topic_id")
	}
	switch r.Kind {
	case "topic":
		if r.PrerequisiteID != "" || r.Tombstone == (r.Topic != nil) {
			return invalid("invalid_topic")
		}
		if r.Topic != nil && !r.Topic.valid() {
			return invalid("invalid_topic_metadata")
		}
	case "prerequisite":
		if r.Topic != nil || !planner.ValidID(r.PrerequisiteID) || r.TopicID == r.PrerequisiteID {
			return invalid("invalid_prerequisite")
		}
	default:
		return invalid("unknown_record_kind")
	}
	return nil
}

func (t Topic) valid() bool {
	if t.Title == "" || !utf8.ValidString(t.Title) || utf8.RuneCountInString(t.Title) > 256 ||
		(t.CourseID != nil && !planner.ValidID(*t.CourseID)) ||
		(t.ParentTopicID != nil && !planner.ValidID(*t.ParentTopicID)) {
		return false
	}
	switch t.Status {
	case "draft", "active", "hidden", "archived":
	default:
		return false
	}
	switch t.Difficulty {
	case "intro", "basic", "medium", "hard", "advanced":
	default:
		return false
	}
	m := t.LearningMetadata
	if m == nil {
		return true
	}
	if (m.EstimatedMinutes != nil && (*m.EstimatedMinutes < 1 || *m.EstimatedMinutes > 1200)) || len(m.Outcomes) > 8 {
		return false
	}
	seen := make(map[string]struct{}, len(m.Outcomes))
	for _, outcome := range m.Outcomes {
		if outcome == "" || strings.TrimSpace(outcome) != outcome || len(outcome) > 256 ||
			!utf8.ValidString(outcome) || strings.ContainsFunc(outcome, unicode.IsControl) {
			return false
		}
		key := strings.ToLower(outcome)
		if _, exists := seen[key]; exists {
			return false
		}
		seen[key] = struct{}{}
	}
	return true
}
