package goals

import (
	"context"
	"errors"
	"time"

	"github.com/overmindv/recommendations/internal/planner"
)

var ErrNotFound = errors.New("practice goal not found")
var ErrInvalid = errors.New("invalid practice goal")

type Goal struct {
	TopicIDs     []string  `json:"topic_ids"`
	Minutes      int       `json:"minutes"`
	SessionCount int       `json:"session_count"`
	Revision     int64     `json:"revision"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type Input struct {
	TopicIDs     []string `json:"topic_ids"`
	Minutes      int      `json:"minutes"`
	SessionCount int      `json:"session_count,omitempty"`
}

type Store interface {
	Get(context.Context, string) (Goal, error)
	Save(context.Context, string, Input) (Goal, error)
	Delete(context.Context, string) error
}

func Validate(input Input) error {
	if input.SessionCount < 0 || input.SessionCount > planner.MaxSessions {
		return ErrInvalid
	}
	if len(input.TopicIDs) < 1 || len(input.TopicIDs) > 5 || input.Minutes < 10 || input.Minutes > 240 {
		return ErrInvalid
	}
	seen := make(map[string]bool, len(input.TopicIDs))
	for _, id := range input.TopicIDs {
		if !planner.ValidID(id) || seen[id] {
			return ErrInvalid
		}
		seen[id] = true
	}
	return nil
}
