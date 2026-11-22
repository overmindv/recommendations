package studyactivity

import (
	"math"
	"testing"
	"time"
)

const testID = "00000000-0000-0000-0000-000000000001"

func inputAt(at time.Time) Input {
	return Input{ClientEventID: testID, SessionID: testID, ResourceKind: "content", ResourceID: testID,
		ResourceRevisionID: testID, Kind: Heartbeat, OccurredAt: at, DurationSeconds: math.MaxInt64, Visible: true, Active: true}
}

func TestAcceptedTimeBounds(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	previous := inputAt(now.Add(-20 * time.Second))
	previous.Kind = Start
	for _, tc := range []struct {
		name   string
		mutate func(*Input)
		want   int64
	}{
		{"huge", func(*Input) {}, 20},
		{"small", func(in *Input) { in.DurationSeconds = 7 }, 7},
		{"hidden", func(in *Input) { in.Visible = false }, 0},
		{"idle", func(in *Input) { in.Active = false }, 0},
		{"offline", func(in *Input) { in.OccurredAt = now.Add(-time.Hour) }, 0},
		{"future", func(in *Input) { in.OccurredAt = now.Add(time.Second) }, 0},
		{"session", func(in *Input) { in.SessionID += "x" }, 0},
		{"revision", func(in *Input) { in.ResourceRevisionID += "x" }, 0},
		{"resource", func(in *Input) { in.ResourceID += "x" }, 0},
		{View, func(in *Input) { in.Kind = View }, 0},
		{"start", func(in *Input) { in.Kind = Start }, 0},
		{"finish", func(in *Input) { in.Kind = Finish }, 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := inputAt(now)
			tc.mutate(&in)
			if got := AcceptedTime(previous, in, previous.OccurredAt, now); got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
	for _, gap := range []time.Duration{time.Second, 60 * time.Second, 120 * time.Second} {
		prior := inputAt(now.Add(-gap))
		want := int64(0)
		if gap == time.Minute {
			want = 30
		}
		if got := AcceptedTime(prior, inputAt(now), prior.OccurredAt, now); got != want {
			t.Fatal(gap, got)
		}
	}
	idlePrevious := previous
	idlePrevious.Active = false
	if AcceptedTime(idlePrevious, inputAt(now), previous.OccurredAt, now) != 0 {
		t.Fatal("idle predecessor credited")
	}
}

func TestInputBounds(t *testing.T) {
	t.Parallel()
	now := time.Now()
	for _, age := range []time.Duration{-3 * time.Minute, time.Hour, 25 * time.Hour} {
		if inputAt(now.Add(-age)).Validate(testID, now, time.Hour) == nil {
			t.Fatal("accepted age", age)
		}
	}
	if err := inputAt(now).Validate(testID, now, time.Hour); err != nil {
		t.Fatal(err)
	}
	if inputAt(now).Validate("actor-from-body", now, time.Hour) == nil {
		t.Fatal("invalid actor")
	}
}
