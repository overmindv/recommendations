package completion

import (
	"testing"

	"github.com/overmindv/recommendations/pkg/api/goalcompletion"
)

func TestCompletionChangesVersionOnlyOnTransition(t *testing.T) {
	t.Parallel()

	var state State
	for _, tc := range []struct {
		verified bool
		version  int64
		kind     goalcompletion.Type
	}{
		{false, 0, ""},
		{true, 1, goalcompletion.Completed},
		{true, 1, ""},
		{false, 2, goalcompletion.Reopened},
		{false, 2, ""},
		{true, 3, goalcompletion.Completed},
	} {
		next, kind, err := state.Advance(tc.verified)
		if err != nil || next.Version != tc.version || next.Verified != tc.verified || kind != tc.kind {
			t.Fatal("unexpected completion transition:", state, next, kind, err)
		}
		state = next
	}
}
