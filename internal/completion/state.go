package completion

import (
	"math"

	"github.com/overmindv/recommendations/pkg/api/goalcompletion"
)

type State struct {
	Version  int64
	Verified bool
}

func (s State) Advance(verified bool) (State, goalcompletion.Type, error) {
	if s.Version < 0 || (s.Version == 0 && s.Verified) {
		return State{}, "", goalcompletion.ErrInvalid
	}
	if s.Verified == verified {
		return s, "", nil
	}
	if s.Version == math.MaxInt64 {
		return State{}, "", goalcompletion.ErrInvalid
	}
	kind := goalcompletion.Reopened
	if verified {
		kind = goalcompletion.Completed
	}

	return State{Version: s.Version + 1, Verified: verified}, kind, nil
}
