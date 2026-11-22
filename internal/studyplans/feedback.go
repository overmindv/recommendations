package studyplans

import (
	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/planner"
)

type FeedbackAction string

const (
	planPaused     = "paused"
	planReplanning = "replanning"
)

const (
	FeedbackTooEasy     FeedbackAction = "too_easy"
	FeedbackTooHard     FeedbackAction = "too_hard"
	FeedbackNotRelevant FeedbackAction = "not_relevant"
	FeedbackSkip        FeedbackAction = "skip"
	FeedbackPause       FeedbackAction = "pause"
	FeedbackResume      FeedbackAction = "resume"
	FeedbackSetBudget   FeedbackAction = "set_budget"
)

type Feedback struct {
	RequestID     string
	PlanVersion   int64
	Action        FeedbackAction
	Ordinal       *int
	WeeklyMinutes *int
}

func (f Feedback) Validate(actor, goal string) error {
	if !planner.ValidID(actor) || !planner.ValidID(goal) || !planner.ValidID(f.RequestID) ||
		f.PlanVersion < 1 || f.PlanVersion > MaxVersionsPerGoal {
		return ErrInvalid
	}
	switch f.Action {
	case FeedbackTooEasy, FeedbackTooHard, FeedbackNotRelevant, FeedbackSkip:
		if f.Ordinal != nil && *f.Ordinal >= 1 && *f.Ordinal <= planner.MaxLearningItems && f.WeeklyMinutes == nil {
			return nil
		}
	case FeedbackPause, FeedbackResume:
		if f.Ordinal == nil && f.WeeklyMinutes == nil {
			return nil
		}
	case FeedbackSetBudget:
		if f.Ordinal == nil && f.WeeklyMinutes != nil && *f.WeeklyMinutes >= 1 && *f.WeeklyMinutes <= learnergoals.MaxWeeklyMinutes {
			return nil
		}
	}

	return ErrInvalid
}
