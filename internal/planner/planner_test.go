package planner

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"testing"
	"time"
)

func id(n int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012x", n) }
func task(n, topic int, level, kind string) Task {
	return Task{ID: id(n), TaskVersionID: id(n + 10000), TopicID: id(topic), Title: "Задача", Difficulty: level, TaskType: kind}
}
func evidence(t Task, verdict string) Evidence {
	return Evidence{TaskID: t.ID, TaskVersionID: t.TaskVersionID, Verdict: verdict, OccurredAt: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
}
func base() Request {
	return Request{TopicIDs: []string{id(1)}, Minutes: 60, Topics: []Topic{{ID: id(1)}}, Tasks: []Task{task(101, 1, "easy", "single_choice")}}
}
func build(t *testing.T, in Request) Plan {
	t.Helper()
	got, err := Build(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestColdStartAndBudget(t *testing.T) {
	in := base()
	in.Tasks = append(in.Tasks, task(102, 1, "hard", "single_choice"), task(103, 1, "easy", "programming"))
	in.Minutes = 20
	got := build(t, in)
	if got.PolicyVersion != "practice-v3" || got.TotalMinutes != 10 || len(got.Items) != 1 || got.Items[0].TaskID != id(101) || got.Items[0].ReasonCode != "START_TOPIC" {
		t.Fatalf("unexpected plan: %+v", got)
	}
	if len(got.Warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", got.Warnings)
	}
}

func TestPrerequisiteReadinessUsesDistinctCurrentAccepted(t *testing.T) {
	for _, count := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			in := base()
			in.TopicIDs = []string{id(2)}
			in.Topics = append(in.Topics, Topic{ID: id(2), PrerequisiteIDs: []string{id(1)}})
			in.Tasks = []Task{task(101, 1, "easy", "single_choice"), task(102, 1, "easy", "single_choice"), task(103, 1, "easy", "single_choice"), task(104, 1, "easy", "single_choice"), task(201, 2, "easy", "single_choice")}
			for i := 0; i < count; i++ {
				for repeat := 0; repeat < 10; repeat++ {
					in.Evidence = append(in.Evidence, evidence(in.Tasks[i], "accepted"))
				}
			}
			old := evidence(in.Tasks[3], "accepted")
			old.TaskVersionID = id(9999)
			in.Evidence = append(in.Evidence, old)
			got := build(t, in)
			if len(got.Items) == 0 {
				t.Fatal("expected available practice")
			}
			for _, item := range got.Items {
				if count < 3 && (item.TopicID != id(1) || item.ReasonCode != "PREREQUISITE") {
					t.Fatalf("dependent topic unlocked early: %+v", got)
				}
				if count == 3 && item.TopicID != id(2) {
					t.Fatalf("dependent topic should be available: %+v", got)
				}
			}
			if slices.Contains(got.Warnings, "PREREQUISITE_GAP") != (count < 3) {
				t.Fatalf("unexpected warnings: %v", got.Warnings)
			}
		})
	}
}

func TestTransitivePrerequisitesAndPlanAreNotEvidence(t *testing.T) {
	in := base()
	in.TopicIDs = []string{id(3)}
	in.Topics = append(in.Topics, Topic{ID: id(2), PrerequisiteIDs: []string{id(1)}}, Topic{ID: id(3), PrerequisiteIDs: []string{id(2)}})
	in.Tasks = []Task{task(101, 1, "easy", "single_choice"), task(102, 1, "easy", "single_choice"), task(103, 1, "easy", "single_choice"), task(201, 2, "easy", "single_choice"), task(202, 2, "easy", "single_choice"), task(203, 2, "easy", "single_choice"), task(301, 3, "easy", "single_choice")}
	for i := 3; i < 6; i++ {
		in.Evidence = append(in.Evidence, evidence(in.Tasks[i], "accepted"))
	}
	got := build(t, in)
	if len(got.Items) != 3 {
		t.Fatalf("expected three base tasks: %+v", got)
	}
	for _, item := range got.Items {
		if item.TopicID != id(1) {
			t.Fatalf("unready ancestor bypassed: %+v", got)
		}
	}
}

func TestEvidenceSemantics(t *testing.T) {
	neutral := []string{"infrastructure_error", "checker_error", "cancelled"}
	for _, verdict := range neutral {
		t.Run(verdict, func(t *testing.T) {
			in := base()
			in.Evidence = []Evidence{evidence(in.Tasks[0], verdict)}
			got := build(t, in)
			if got.Items[0].ReasonCode != "START_TOPIC" {
				t.Fatalf("neutral verdict changed cold start: %+v", got)
			}
		})
	}
	for _, verdict := range []string{"wrong_answer", "compilation_error", "runtime_error", "time_limit_exceeded", "memory_limit_exceeded", "output_limit_exceeded"} {
		t.Run(verdict, func(t *testing.T) {
			in := base()
			in.Tasks = append(in.Tasks, task(102, 1, "easy", "single_choice"))
			in.Evidence = []Evidence{evidence(in.Tasks[1], verdict)}
			got := build(t, in)
			if got.Items[0].TaskID != id(101) || got.Items[0].ReasonCode != "PRACTICE_GAP" || got.Items[1].TaskID != id(102) {
				t.Fatalf("expected fresh practice before a failed task: %+v", got)
			}
		})
	}
	t.Run("accepted remains solved", func(t *testing.T) {
		in := base()
		old := evidence(in.Tasks[0], "accepted")
		wrong := evidence(in.Tasks[0], "wrong_answer")
		wrong.OccurredAt = wrong.OccurredAt.Add(time.Hour)
		in.Evidence = []Evidence{old, wrong}
		got := build(t, in)
		if len(got.Items) != 0 || !slices.Contains(got.Warnings, "NO_ELIGIBLE_TASKS") {
			t.Fatalf("accepted should exclude task: %+v", got)
		}
	})
	t.Run("old version ignored", func(t *testing.T) {
		in := base()
		old := evidence(in.Tasks[0], "accepted")
		old.TaskVersionID = id(9999)
		in.Evidence = []Evidence{old}
		got := build(t, in)
		if len(got.Items) != 1 || got.Items[0].ReasonCode != "START_TOPIC" {
			t.Fatalf("old version used: %+v", got)
		}
	})
}

func TestDifficultyProgressionAndWrongAnswerFallback(t *testing.T) {
	in := base()
	in.Tasks = nil
	for i := 101; i < 107; i++ {
		task := task(i, 1, "easy", "single_choice")
		in.Tasks = append(in.Tasks, task)
		in.Evidence = append(in.Evidence, evidence(task, "accepted"))
	}
	in.Tasks = append(in.Tasks, task(201, 1, "easy", "single_choice"), task(202, 1, "medium", "single_choice"), task(203, 1, "hard", "single_choice"))
	got := build(t, in)
	if got.Items[0].TaskID != id(203) || got.Items[0].ReasonCode != "NEXT_CHALLENGE" {
		t.Fatalf("expected harder challenge: %+v", got)
	}
	in.Evidence = append(in.Evidence, evidence(in.Tasks[8], "wrong_answer"))
	got = build(t, in)
	if got.Items[0].TaskID != id(202) || got.Items[0].ReasonCode != "PRACTICE_GAP" || got.Items[1].TaskID != id(201) {
		t.Fatalf("expected fresh suitable practice after a failed hard task: %+v", got)
	}
	for _, item := range got.Items {
		if item.Difficulty == "hard" {
			t.Fatal("hard task retained after gap")
		}
	}
}

func TestFailedTaskRemainsFallback(t *testing.T) {
	in := base()
	in.Evidence = []Evidence{evidence(in.Tasks[0], "wrong_answer")}
	got := build(t, in)
	if len(got.Items) != 1 || got.Items[0].TaskID != in.Tasks[0].ID || got.Items[0].ReasonCode != "PRACTICE_GAP" {
		t.Fatalf("failed task should remain eligible when no alternative exists: %+v", got)
	}
}

func TestBalancesTaskTypesWithinTopic(t *testing.T) {
	in := base()
	in.Minutes = 40
	in.Tasks = []Task{
		task(101, 1, "easy", "single_choice"),
		task(102, 1, "easy", "single_choice"),
		task(103, 1, "easy", "multiple_choice"),
		task(104, 1, "easy", "programming"),
	}
	got := build(t, in)
	if len(got.Items) != 3 || got.Items[0].TaskID != id(101) || got.Items[1].TaskID != id(103) || got.Items[2].TaskID != id(104) || got.TotalMinutes != 40 {
		t.Fatalf("budget or type balance violated: %+v", got)
	}
}

func TestDiversityDeterminismAndDuplicateCap(t *testing.T) {
	in := base()
	in.TopicIDs = append(in.TopicIDs, id(2))
	in.Topics = append(in.Topics, Topic{ID: id(2)})
	in.Minutes = 40
	in.Tasks = append(in.Tasks, task(102, 1, "easy", "single_choice"), task(103, 1, "easy", "single_choice"), task(201, 2, "easy", "single_choice"), task(202, 2, "easy", "single_choice"), task(203, 2, "easy", "single_choice"))
	in.Evidence = []Evidence{evidence(in.Tasks[2], "wrong_answer")}
	want := build(t, in)
	counts := map[string]int{}
	for _, item := range want.Items {
		counts[item.TopicID]++
	}
	if counts[id(1)] != 2 || counts[id(2)] != 2 || want.TotalMinutes != 40 {
		t.Fatalf("unbalanced or over budget: %+v", want)
	}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 30; i++ {
		in.Evidence = append(in.Evidence, in.Evidence[0])
		rng.Shuffle(len(in.Tasks), func(a, b int) { in.Tasks[a], in.Tasks[b] = in.Tasks[b], in.Tasks[a] })
		rng.Shuffle(len(in.Topics), func(a, b int) { in.Topics[a], in.Topics[b] = in.Topics[b], in.Topics[a] })
		rng.Shuffle(len(in.TopicIDs), func(a, b int) { in.TopicIDs[a], in.TopicIDs[b] = in.TopicIDs[b], in.TopicIDs[a] })
		if got := build(t, in); !reflect.DeepEqual(want, got) {
			t.Fatalf("input order or repeats affected policy: want %+v got %+v", want, got)
		}
	}
}

func TestWarningsAndItemCap(t *testing.T) {
	in := base()
	in.Tasks = nil
	in.InputTruncated = true
	got := build(t, in)
	if !reflect.DeepEqual(got.Warnings, []string{"PARTIAL_DATA", "NO_ELIGIBLE_TASKS"}) || got.Items == nil {
		t.Fatalf("unexpected empty plan: %+v", got)
	}
	in.Tasks = []Task{task(101, 1, "easy", "programming")}
	in.Minutes = 10
	got = build(t, in)
	if !reflect.DeepEqual(got.Warnings, []string{"PARTIAL_DATA", "BUDGET_TOO_SMALL"}) {
		t.Fatalf("unexpected budget plan: %+v", got)
	}
	in.Tasks = nil
	in.Minutes = 240
	for i := 101; i < 131; i++ {
		in.Tasks = append(in.Tasks, task(i, 1, "easy", "single_choice"))
	}
	got = build(t, in)
	if len(got.Items) != 20 || got.TotalMinutes != 200 {
		t.Fatalf("item cap violated: %+v", got)
	}
}

func TestValidation(t *testing.T) {
	cases := map[string]func(*Request){
		"no topics selected":   func(r *Request) { r.TopicIDs = nil },
		"duplicate selected":   func(r *Request) { r.TopicIDs = append(r.TopicIDs, r.TopicIDs[0]) },
		"six selected":         func(r *Request) { r.TopicIDs = make([]string, 6) },
		"unknown selected":     func(r *Request) { r.TopicIDs = []string{id(9)} },
		"low budget":           func(r *Request) { r.Minutes = 9 },
		"high budget":          func(r *Request) { r.Minutes = 241 },
		"too many topics":      func(r *Request) { r.Topics = make([]Topic, 33) },
		"duplicate topics":     func(r *Request) { r.Topics = append(r.Topics, r.Topics[0]) },
		"missing prerequisite": func(r *Request) { r.Topics[0].PrerequisiteIDs = []string{id(9)} },
		"duplicate prerequisite": func(r *Request) {
			r.Topics = append(r.Topics, Topic{ID: id(2), PrerequisiteIDs: []string{id(1), id(1)}})
		},
		"cycle": func(r *Request) {
			r.Topics[0].PrerequisiteIDs = []string{id(2)}
			r.Topics = append(r.Topics, Topic{ID: id(2), PrerequisiteIDs: []string{id(1)}})
		},
		"nil id":             func(r *Request) { r.Tasks[0].ID = id(0) },
		"malformed id":       func(r *Request) { r.Tasks[0].ID = "not-id" },
		"unknown task topic": func(r *Request) { r.Tasks[0].TopicID = id(9) },
		"duplicate task":     func(r *Request) { r.Tasks = append(r.Tasks, r.Tasks[0]) },
		"duplicate version":  func(r *Request) { other := r.Tasks[0]; other.ID = id(999); r.Tasks = append(r.Tasks, other) },
		"too many tasks":     func(r *Request) { r.Tasks = make([]Task, MaxTasks+1) },
		"too many evidence":  func(r *Request) { r.Evidence = make([]Evidence, MaxEvidence+1) },
		"empty title":        func(r *Request) { r.Tasks[0].Title = " \t" },
		"huge title":         func(r *Request) { r.Tasks[0].Title = string(make([]byte, 2001)) },
		"invalid difficulty": func(r *Request) { r.Tasks[0].Difficulty = "expert" },
		"invalid type":       func(r *Request) { r.Tasks[0].TaskType = "essay" },
		"invalid verdict":    func(r *Request) { r.Evidence = []Evidence{evidence(r.Tasks[0], "pending")} },
		"missing time": func(r *Request) {
			e := evidence(r.Tasks[0], "accepted")
			e.OccurredAt = time.Time{}
			r.Evidence = []Evidence{e}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := base()
			mutate(&in)
			if _, err := Build(context.Background(), in); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("expected invalid input, got %v", err)
			}
		})
	}
}

func TestCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Build(ctx, base()); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func BenchmarkBuild(b *testing.B) {
	in := base()
	in.Minutes = 240
	in.Topics = nil
	in.Tasks = nil
	for i := 1; i <= MaxTopics; i++ {
		in.Topics = append(in.Topics, Topic{ID: id(i)})
	}
	in.TopicIDs = []string{id(1), id(2), id(3), id(4), id(5)}
	for i := 0; i < MaxTasks; i++ {
		task := task(i+100, 1+i%5, "easy", "single_choice")
		in.Tasks = append(in.Tasks, task)
		in.Evidence = append(in.Evidence, evidence(task, "wrong_answer"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Build(context.Background(), in); err != nil {
			b.Fatal(err)
		}
	}
}
