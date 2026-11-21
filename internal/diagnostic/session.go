package diagnostic

import (
	"context"
	"errors"
	"time"

	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/planner"
)

const (
	Ready           = "ready"
	NoTasks         = "no_tasks"
	Skipped         = "skipped"
	Expired         = "expired"
	Finalized       = "finalized"
	BaselineUnknown = "unknown"
)

var (
	ErrNotStarted         = errors.New("diagnostic session not started")
	ErrTopicUnavailable   = errors.New("diagnostic target topic unavailable")
	ErrTaskUnavailable    = errors.New("diagnostic task is no longer available")
	ErrCatalogUnavailable = errors.New("diagnostic catalog unavailable")
	ErrBudget             = errors.New("diagnostic budget exceeds goal time")
	ErrSessionChanged     = errors.New("diagnostic session changed")
)

type Session struct {
	ID          string `json:"id"`
	GoalID      string `json:"goal_id"`
	GoalVersion int64  `json:"goal_version"`
	Status      string `json:"status"`
	Baseline    string `json:"baseline"`
	Selection
	StartedAt   time.Time  `json:"started_at"`
	ExpiresAt   time.Time  `json:"expires_at"`
	SkippedAt   *time.Time `json:"skipped_at,omitempty"`
	FinalizedAt *time.Time `json:"finalized_at,omitempty"`
	Report      *Report    `json:"report,omitempty"`
}

type Catalog interface {
	SelectDiagnostic(ctx context.Context, actor string, targets []string, budget int) (Selection, error)
	CheckDiagnostic(ctx context.Context, tasks []Candidate) error
}

type GoalReader interface {
	Get(ctx context.Context, actor, id string) (learnergoals.Goal, error)
}

type Service struct {
	goals   GoalReader
	store   *Postgres
	catalog Catalog
}

func NewService(goals GoalReader, store *Postgres, catalog Catalog) *Service {
	return &Service{goals: goals, store: store, catalog: catalog}
}

func (s *Service) Get(ctx context.Context, actor, goalID string) (Session, error) {
	goal, err := s.goals.Get(ctx, actor, goalID)
	if err != nil {
		return Session{}, err
	}
	session, err := s.store.get(ctx, goal)
	if err != nil {
		return Session{}, err
	}

	return s.present(ctx, session)
}

func (s *Service) Start(ctx context.Context, actor, goalID string, version int64, budget int) (Session, error) {
	if budget < 0 || budget > MaxMinutes {
		return Session{}, ErrInvalidSelection
	}
	goal, err := s.goalForWrite(ctx, actor, goalID, version)
	if err != nil {
		return Session{}, err
	}
	session, err := s.store.get(ctx, goal)
	if err == nil {
		return s.present(ctx, session)
	}
	if !errors.Is(err, ErrNotStarted) {
		return Session{}, err
	}
	if budget == 0 {
		budget = min(MaxMinutes, goal.WeeklyMinutes)
	}
	if budget < 1 || budget > goal.WeeklyMinutes {
		return Session{}, ErrBudget
	}
	if s.catalog == nil {
		return Session{}, ErrCatalogUnavailable
	}
	selection, err := s.catalog.SelectDiagnostic(ctx, actor, goal.TargetTopics, budget)
	if err != nil {
		return Session{}, err
	}
	session, err = s.store.save(ctx, goal, selection, false)
	if err != nil {
		return Session{}, err
	}

	return s.present(ctx, session)
}

func (s *Service) Skip(ctx context.Context, actor, goalID string, version int64) (Session, error) {
	goal, err := s.goalForWrite(ctx, actor, goalID, version)
	if err != nil {
		return Session{}, err
	}
	selection := Selection{Tasks: []Candidate{}, StopRule: StopRule{PolicyVersion: PolicyVersion, MaxTasks: MaxTasks}}
	session, err := s.store.save(ctx, goal, selection, true)
	if err != nil {
		return Session{}, err
	}

	return s.present(ctx, session)
}

func (s *Service) Finalize(ctx context.Context, actor, goalID, sessionID string, version int64, allowPartial bool) (Session, error) {
	if !planner.ValidID(sessionID) {
		return Session{}, ErrInvalidSelection
	}
	goal, err := s.goalForWrite(ctx, actor, goalID, version)
	if err != nil {
		return Session{}, err
	}
	session, err := s.store.finalize(ctx, goal, sessionID, allowPartial)
	if err != nil {
		return Session{}, err
	}
	session.Tasks = []Candidate{}

	return session, nil
}

func (s *Service) goalForWrite(ctx context.Context, actor, goalID string, version int64) (learnergoals.Goal, error) {
	if version < 1 {
		return learnergoals.Goal{}, ErrInvalidSelection
	}
	goal, err := s.goals.Get(ctx, actor, goalID)
	if err != nil {
		return learnergoals.Goal{}, err
	}
	if goal.Version != version {
		return learnergoals.Goal{}, learnergoals.ErrConflict
	}

	return goal, nil
}

func (s *Service) present(ctx context.Context, session Session) (Session, error) {
	if session.Status != Ready {
		session.Tasks = []Candidate{}

		return session, nil
	}
	if s.catalog == nil {
		return Session{}, ErrCatalogUnavailable
	}
	if err := s.catalog.CheckDiagnostic(ctx, session.Tasks); err != nil {
		return Session{}, err
	}

	return session, nil
}

func (s Selection) valid(skip bool) bool {
	if s.StopRule.PolicyVersion != PolicyVersion || s.StopRule.MaxTasks != MaxTasks ||
		s.StopRule.BudgetMinutes < 0 || s.StopRule.BudgetMinutes > MaxMinutes ||
		(!skip && s.StopRule.BudgetMinutes == 0) || s.Tasks == nil || len(s.Tasks) > MaxTasks ||
		(skip && len(s.Tasks) != 0) {
		return false
	}
	tasks, topics := make(map[string]bool, len(s.Tasks)), make(map[string]bool, len(s.Tasks))
	total := 0
	for _, row := range s.Tasks {
		if !planner.ValidID(row.TaskID) || !planner.ValidID(row.VersionID) || !planner.ValidID(row.TopicID) ||
			row.Minutes < 1 || row.Minutes > MaxMinutes || tasks[row.TaskID] || topics[row.TopicID] {
			return false
		}
		tasks[row.TaskID], topics[row.TopicID] = true, true
		total += row.Minutes
	}

	return total == s.EstimatedMinutes && total <= s.StopRule.BudgetMinutes
}
