package studyactivity

import (
	"errors"
	"time"

	"github.com/overmindv/recommendations/internal/planner"
)

const (
	Trust           = "self_reported"
	View            = "view"
	Start           = "start"
	Heartbeat       = "heartbeat"
	Finish          = "finish"
	MinInterval     = 5 * time.Second
	MaxOffline      = 24 * time.Hour
	MaxClockSkew    = 2 * time.Minute
	MaxHourlyEvents = 600
)

var (
	ErrInvalid  = errors.New("invalid study activity")
	ErrConflict = errors.New("activity idempotency conflict")
	ErrRate     = errors.New("study activity rate exceeded")
	ErrResource = errors.New("study resource unavailable")
)

type Input struct {
	ClientEventID      string    `json:"client_event_id"`
	SessionID          string    `json:"session_id"`
	ResourceKind       string    `json:"resource_kind"`
	ResourceID         string    `json:"resource_id"`
	ResourceRevisionID string    `json:"resource_revision_id"`
	Kind               string    `json:"kind"`
	OccurredAt         time.Time `json:"occurred_at"`
	DurationSeconds    int64     `json:"duration_seconds"`
	Visible            bool      `json:"visible"`
	Active             bool      `json:"active"`
}

type Receipt struct {
	ClientEventID   string    `json:"client_event_id"`
	AcceptedSeconds int64     `json:"accepted_seconds"`
	Trust           string    `json:"trust"`
	ExpiresAt       time.Time `json:"expires_at"`
}

func (in Input) Validate(actor string, now time.Time, ttl time.Duration) error {
	if in.validateShape(actor) != nil || in.OccurredAt.After(now.Add(MaxClockSkew)) ||
		!in.OccurredAt.After(now.Add(-min(ttl, MaxOffline))) {
		return ErrInvalid
	}

	return nil
}

func (in Input) validateShape(actor string) error {
	if !planner.ValidID(actor) || !planner.ValidID(in.ClientEventID) || !planner.ValidID(in.SessionID) ||
		!planner.ValidID(in.ResourceID) || !planner.ValidID(in.ResourceRevisionID) ||
		(in.ResourceKind != "tasks" && in.ResourceKind != "content") ||
		(in.Kind != View && in.Kind != Start && in.Kind != Heartbeat && in.Kind != Finish) ||
		in.DurationSeconds < 0 || in.OccurredAt.IsZero() {
		return ErrInvalid
	}

	return nil
}

// AcceptedTime ограничивает самоотчёт двумя часами: клиента и сервера.
// Перерыв, новая вкладка/ревизия или offline событие не восстанавливают пропущенное время.
func AcceptedTime(previous, current Input, receivedBefore, receivedNow time.Time) int64 {
	serverGap := receivedNow.Sub(receivedBefore)
	clientGap := current.OccurredAt.Sub(previous.OccurredAt)
	if (current.Kind != Heartbeat && current.Kind != Finish) ||
		(previous.Kind != Start && previous.Kind != Heartbeat) ||
		!current.Visible || !current.Active || !previous.Visible || !previous.Active ||
		current.SessionID != previous.SessionID || current.ResourceKind != previous.ResourceKind ||
		current.ResourceID != previous.ResourceID || current.ResourceRevisionID != previous.ResourceRevisionID ||
		serverGap < MinInterval || serverGap > 90*time.Second || clientGap <= 0 || clientGap > 90*time.Second ||
		!fresh(current.OccurredAt, receivedNow) || !fresh(previous.OccurredAt, receivedBefore) {
		return 0
	}

	return min(current.DurationSeconds, 30, int64(serverGap/time.Second), int64(clientGap/time.Second))
}

func fresh(occurred, received time.Time) bool {
	return !occurred.After(received) && received.Sub(occurred) <= MaxClockSkew
}
