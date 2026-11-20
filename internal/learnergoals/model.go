package learnergoals

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"
	_ "time/tzdata"
	"unicode"
	"unicode/utf8"

	"github.com/overmindv/recommendations/internal/planner"
)

const levelUnknown = "unknown"

const (
	MaxWeeklyMinutes = 7 * 24 * 60
	MaxGoals         = 32
	MaxTargetTopics  = 16
	idempotencyTTL   = 24 * time.Hour
	maxRequests      = 256
	dateLayout       = "2006-01-02"
)

var (
	ErrInvalid      = errors.New("invalid learner goal input")
	ErrNotFound     = errors.New("learner goal or availability not found")
	ErrConflict     = errors.New("learner goal or availability version changed")
	ErrBudget       = errors.New("weekly allocations exceed available minutes")
	ErrGoalLimit    = errors.New("learner goal limit reached")
	ErrRequestUsed  = errors.New("request ID already used with different input")
	ErrRequestLimit = errors.New("learner goal write limit reached")
	slug            = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	template        = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_./:@-]{0,127}$`)
)

type AvailabilityInput struct {
	WeeklyMinutes int    `json:"weekly_minutes"`
	Timezone      string `json:"timezone"`
}

type Availability struct {
	UserID string `json:"user_id"`
	AvailabilityInput
	Version   int64     `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}

type SelfAssessment struct {
	TopicID string `json:"topic_id"`
	Level   string `json:"level"`
}

type Preferences struct {
	Languages       []string         `json:"languages"`
	Formats         []string         `json:"formats"`
	SelfAssessments []SelfAssessment `json:"self_assessments"`
}

type Input struct {
	Title           string   `json:"title"`
	TemplateVersion string   `json:"template_version,omitempty"`
	TargetTopics    []string `json:"target_topics"`
	Deadline        string   `json:"deadline,omitempty"`
	WeeklyMinutes   int      `json:"weekly_minutes"`
	Active          bool     `json:"active"`
	Preferences
}

type Goal struct {
	ID     string `json:"id"`
	UserID string `json:"user_id"`
	Input
	Timezone  string    `json:"timezone"`
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Write struct {
	RequestID       string `json:"request_id"`
	ExpectedVersion int64  `json:"expected_version"`
}

type Store interface {
	GetAvailability(ctx context.Context, actor string) (Availability, error)
	SaveAvailability(ctx context.Context, actor string, write Write, input AvailabilityInput) (Availability, error)
	List(ctx context.Context, actor string) ([]Goal, error)
	Get(ctx context.Context, actor, id string) (Goal, error)
	Create(ctx context.Context, actor, requestID string, input Input) (Goal, error)
	Update(ctx context.Context, actor, id string, write Write, input Input) (Goal, error)
	Delete(ctx context.Context, actor, id string, write Write) error
}

func (in AvailabilityInput) validate() error {
	if in.WeeklyMinutes < 0 || in.WeeklyMinutes > MaxWeeklyMinutes || len(in.Timezone) > 64 ||
		(in.Timezone != "UTC" && !strings.Contains(in.Timezone, "/")) {
		return ErrInvalid
	}
	if _, err := time.LoadLocation(in.Timezone); err != nil {
		return ErrInvalid
	}

	return nil
}

func normalize(in Input) (Input, error) {
	in.Title = strings.TrimSpace(in.Title)
	if in.Title == "" || len(in.Title) > 256 || !utf8.ValidString(in.Title) || strings.ContainsFunc(in.Title, unicode.IsControl) ||
		(in.TemplateVersion != "" && !template.MatchString(in.TemplateVersion)) ||
		len(in.TargetTopics) < 1 || len(in.TargetTopics) > MaxTargetTopics ||
		in.WeeklyMinutes < 0 || in.WeeklyMinutes > MaxWeeklyMinutes || (in.Active && in.WeeklyMinutes == 0) ||
		!validLabels(in.Languages) || !validLabels(in.Formats) {
		return Input{}, ErrInvalid
	}
	if in.Deadline != "" {
		date, err := time.Parse(dateLayout, in.Deadline)
		if err != nil || date.Year() < 1 {
			return Input{}, ErrInvalid
		}
	}
	levels := make(map[string]string, len(in.TargetTopics))
	for _, id := range in.TargetTopics {
		if !planner.ValidID(id) || levels[id] != "" {
			return Input{}, ErrInvalid
		}
		levels[id] = levelUnknown
	}
	if len(in.SelfAssessments) > len(in.TargetTopics) {
		return Input{}, ErrInvalid
	}
	seen := make(map[string]bool, len(in.SelfAssessments))
	for _, entry := range in.SelfAssessments {
		if levels[entry.TopicID] == "" || seen[entry.TopicID] {
			return Input{}, ErrInvalid
		}
		switch entry.Level {
		case levelUnknown, "beginner", "intermediate", "advanced":
			levels[entry.TopicID], seen[entry.TopicID] = entry.Level, true
		default:
			return Input{}, ErrInvalid
		}
	}
	in.SelfAssessments = make([]SelfAssessment, 0, len(in.TargetTopics))
	for _, id := range in.TargetTopics {
		in.SelfAssessments = append(in.SelfAssessments, SelfAssessment{TopicID: id, Level: levels[id]})
	}
	in.Languages = append([]string{}, in.Languages...)
	in.Formats = append([]string{}, in.Formats...)

	return in, nil
}

func validLabels(values []string) bool {
	if len(values) > 8 {
		return false
	}
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if !slug.MatchString(value) || seen[value] {
			return false
		}
		seen[value] = true
	}

	return true
}

func checkDeadline(in Input, availability Availability, now time.Time) error {
	if !in.Active || in.Deadline == "" {
		return nil
	}
	location, err := time.LoadLocation(availability.Timezone)
	if err != nil || in.Deadline < now.In(location).Format(dateLayout) {
		return ErrInvalid
	}

	return nil
}
