package planner

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"slices"
	"testing"
)

func TestRoadmapBudgetsUniqueTasksAndLegacyFirstSession(t *testing.T) {
	in := base()
	in.SessionCount, in.Minutes = 3, 20
	in.Tasks = nil
	in.TopicIDs = append(in.TopicIDs, id(2))
	in.Topics = append(in.Topics, Topic{ID: id(2)})
	for n := 101; n <= 110; n++ {
		in.Tasks = append(in.Tasks, task(n, 1+n%2, "easy", "single_choice"))
	}
	got := build(t, in)
	if len(got.Sessions) != 3 || got.TotalPlannedMinutes != 60 || !reflect.DeepEqual(got.Items, got.Sessions[0].Items) || got.TotalMinutes != 20 {
		t.Fatalf("invalid roadmap: %+v", got)
	}
	seen := map[string]bool{}
	for i, session := range got.Sessions {
		if session.Number != i+1 || session.TotalMinutes > in.Minutes || len(session.Items) > MaxItems {
			t.Fatalf("invalid session: %+v", session)
		}
		if session.Items[0].TopicID == session.Items[1].TopicID {
			t.Fatal("topics not balanced")
		}
		for _, item := range session.Items {
			if seen[item.TaskID] {
				t.Fatal("task repeated across sessions")
			}
			seen[item.TaskID] = true
		}
	}
	for _, insight := range got.TopicInsights {
		if insight.PlannedTasks != 3 || insight.AcceptedTasks != 0 || insight.RemainingToReadiness != 3 {
			t.Fatalf("plan became evidence: %+v", insight)
		}
	}
	rng := rand.New(rand.NewSource(17))
	for range 10 {
		rng.Shuffle(len(in.Tasks), func(a, b int) { in.Tasks[a], in.Tasks[b] = in.Tasks[b], in.Tasks[a] })
		slices.Reverse(in.Topics)
		slices.Reverse(in.TopicIDs)
		if !reflect.DeepEqual(got, build(t, in)) {
			t.Fatal("roadmap depends on input order")
		}
	}
}

func TestRoadmapNeverUnlocksFromPlannedPractice(t *testing.T) {
	in := base()
	in.TopicIDs = []string{id(3)}
	in.SessionCount, in.Minutes = 8, 10
	in.Topics = append(in.Topics, Topic{ID: id(2), PrerequisiteIDs: []string{id(1)}}, Topic{ID: id(3), PrerequisiteIDs: []string{id(2), id(1)}})
	in.Tasks = []Task{task(101, 1, "easy", "single_choice"), task(102, 1, "easy", "single_choice"), task(103, 1, "easy", "single_choice"), task(201, 2, "easy", "single_choice"), task(301, 3, "easy", "single_choice")}
	got := build(t, in)
	if len(got.Sessions) != 3 || !slices.Contains(got.Warnings, "SHORT_HORIZON") {
		t.Fatalf("invalid horizon: %+v", got)
	}
	for _, session := range got.Sessions {
		if session.Items[0].TopicID != id(1) {
			t.Fatal("planned tasks unlocked dependent")
		}
	}
	if !reflect.DeepEqual(got.TopicInsights[2].BlockedBy, []string{id(1), id(2)}) || got.TopicInsights[2].NextAction != "COMPLETE_PREREQUISITES" {
		t.Fatalf("missing transitive blockers: %+v", got.TopicInsights)
	}
}

func TestRoadmapActionableCatalogAndBudgetDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, action string
		tasks        []Task
		solved       bool
		minimum      int
	}{
		{"no content", "ADD_CONTENT", nil, false, 0},
		{"all accepted", "NO_NEW_TASKS", []Task{task(101, 1, "easy", "single_choice")}, true, 0},
		{"too hard", "NEED_EASIER_TASKS", []Task{task(101, 1, "hard", "programming")}, false, 0},
		{"time", "INCREASE_BUDGET", []Task{task(101, 1, "easy", "programming")}, false, 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := base()
			in.Tasks = tc.tasks
			in.Minutes = 10
			if tc.solved {
				in.Evidence = []Evidence{evidence(in.Tasks[0], "accepted")}
			}
			insight := build(t, in).TopicInsights[0]
			if insight.NextAction != tc.action || insight.MinimumMinutes != tc.minimum {
				t.Fatalf("wrong diagnosis: %+v", insight)
			}
		})
	}
}

func TestRoadmapCoverageAcrossShortSessionsAndRecoveryPriority(t *testing.T) {
	in := base()
	in.Minutes = 10
	in.SessionCount = 2
	in.TopicIDs = append(in.TopicIDs, id(2))
	in.Topics = append(in.Topics, Topic{ID: id(2)})
	in.Tasks = append(in.Tasks, task(201, 2, "easy", "single_choice"), task(202, 2, "easy", "single_choice"))
	in.Evidence = []Evidence{evidence(in.Tasks[2], "wrong_answer")}
	got := build(t, in)
	if got.Sessions[0].Items[0].TaskID != id(201) || got.Sessions[1].Items[0].TopicID != id(1) {
		t.Fatalf("recovery or coverage lost: %+v", got.Sessions)
	}
}

func TestRoadmapSessionLimitsAndDefault(t *testing.T) {
	for _, count := range []int{-1, 9} {
		in := base()
		in.SessionCount = count
		if _, err := Build(context.Background(), in); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("accepted %d sessions", count)
		}
	}
	in := base()
	got := build(t, in)
	if got.RequestedSessions != 1 || len(got.Sessions) != 1 {
		t.Fatal("legacy request not defaulted")
	}
	in.SessionCount = 8
	in.Minutes = 240
	in.Tasks = nil
	for n := 101; n < 301; n++ {
		in.Tasks = append(in.Tasks, task(n, 1, "easy", "single_choice"))
	}
	got = build(t, in)
	if len(got.Sessions) != 8 || got.TotalPlannedMinutes != 1600 {
		t.Fatal("incorrect full horizon")
	}
	for _, session := range got.Sessions {
		if len(session.Items) != MaxItems {
			t.Fatal("per-session limit lost")
		}
	}
}
