package studyplans

import (
	"errors"
	"testing"
)

func TestFeedbackAcceptsOnlyFieldsForItsAction(t *testing.T) {
	t.Parallel()

	const (
		actor = "abcdef00-1234-4000-8000-000000000001"
		goal  = "abcdef00-1234-4000-8000-000000000002"
		key   = "abcdef00-1234-4000-8000-000000000003"
	)
	for _, tc := range []struct {
		name    string
		command Feedback
		valid   bool
	}{
		{"difficulty", Feedback{key, 1, FeedbackTooHard, new(5), nil}, true},
		{"skip", Feedback{key, 1, FeedbackSkip, new(1), nil}, true},
		{"pause", Feedback{key, 1, FeedbackPause, nil, nil}, true},
		{"resume", Feedback{key, 1, FeedbackResume, nil, nil}, true},
		{"budget", Feedback{key, 1, FeedbackSetBudget, nil, new(120)}, true},
		{"unknown_action", Feedback{key, 1, "done", new(5), nil}, false},
		{"missing_step", Feedback{key, 1, FeedbackSkip, nil, nil}, false},
		{"step_limit", Feedback{key, 1, FeedbackSkip, new(129), nil}, false},
		{"pause_with_step", Feedback{key, 1, FeedbackPause, new(1), nil}, false},
		{"budget_with_step", Feedback{key, 1, FeedbackSetBudget, new(1), new(120)}, false},
		{"zero_budget", Feedback{key, 1, FeedbackSetBudget, nil, new(0)}, false},
		{"no_previous_plan", Feedback{key, 0, FeedbackPause, nil, nil}, false},
		{"missing_key", Feedback{"", 1, FeedbackResume, nil, nil}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.command.Validate(actor, goal)
			if (tc.valid && err != nil) || (!tc.valid && !errors.Is(err, ErrInvalid)) {
				t.Fatal("unexpected feedback validation:", err)
			}
		})
	}
}
