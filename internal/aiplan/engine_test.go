package aiplan

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/overmindv/recommendations/internal/planner"
)

type rankFunc func(context.Context, []byte) ([]int, error)

func (f rankFunc) Rank(c context.Context, b []byte) ([]int, error) { return f(c, b) }
func testID(n int) string                                          { return fmt.Sprintf("00000000-0000-0000-0000-%012d", n) }
func input() planner.Request {
	r := planner.Request{Goal: "Подготовка к ML", TopicIDs: []string{testID(1)}, Minutes: 10, SessionCount: 2, Topics: []planner.Topic{{ID: testID(1), Title: "Регрессия"}}}
	for i := 2; i < 6; i++ {
		r.Tasks = append(r.Tasks, planner.Task{ID: testID(i), TaskVersionID: testID(i + 100), TopicID: testID(1), Title: fmt.Sprintf("Задача %d", i), Difficulty: "easy", TaskType: "single_choice"})
	}

	return r
}
func TestFallbackCacheIsolationAndEvidenceInvalidation(t *testing.T) {
	t.Parallel()

	calls := 0
	first := 0
	e := New(rankFunc(func(context.Context, []byte) ([]int, error) {
		first++

		return nil, errUnavailable
	}), rankFunc(func(_ context.Context, b []byte) ([]int, error) {
		calls++
		var data compactInput
		if err := json.Unmarshal(b, &data); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), testID(1)) || strings.Contains(string(b), "occurred_at") {
			t.Fatal("raw identity or history leaked")
		}
		ids := make([]int, 0, len(data.Candidates))
		for i := len(data.Candidates); i > 0; i-- {
			ids = append(ids, i)
		}

		return ids, nil
	}))
	r := input()
	plan, err := e.Build(context.Background(), r, "actor1")
	if err != nil || plan.AIStatus != aiStatusGenerated || plan.Items[0].TaskID != testID(5) || len(plan.Sessions) != 2 {
		t.Fatalf("bad AI plan: %+v %v", plan, err)
	}
	cached, _ := e.Build(context.Background(), r, "actor1")
	if cached.AIStatus != aiStatusCached || calls != 1 || first != 1 {
		t.Fatal("cache missed")
	}
	_, _ = e.Build(context.Background(), r, "actor2")
	if calls != 2 {
		t.Fatal("shared actor cache")
	}
	r.Evidence = []planner.Evidence{{TaskID: testID(5), TaskVersionID: testID(105), Verdict: "accepted", OccurredAt: time.Now()}}
	updated, _ := e.Build(context.Background(), r, "actor1")
	if calls != 3 || updated.Items[0].TaskID == testID(5) {
		t.Fatal("stale evidence cache")
	}
	r.Tasks[0].Title = "Changed"
	_, _ = e.Build(context.Background(), r, "actor1")
	if calls != 4 {
		t.Fatal("stale catalog cache")
	}
}
func TestInvalidAndRefusedAnswersDoNotBecomeAIPlan(t *testing.T) {
	t.Parallel()

	for _, ids := range [][]int{{999}, {1, 1, 3, 4}, {1, 2}, {1, 2, 3, 4, 5}} {
		e := New(rankFunc(func(context.Context, []byte) ([]int, error) { return ids, nil }))
		p, err := e.Build(context.Background(), input(), "actor")
		if err != nil || p.AIStatus != aiStatusUnavailable || p.Items[0].TaskID != testID(2) {
			t.Fatal("invalid ranking accepted")
		}
	}
	called := false
	e := New(rankFunc(func(context.Context, []byte) ([]int, error) { return nil, ErrRefused }), rankFunc(func(context.Context, []byte) ([]int, error) {
		called = true

		return []int{1, 2, 3, 4}, nil
	}))
	p, _ := e.Build(context.Background(), input(), "actor")
	if p.AIStatus != aiStatusRefused || called {
		t.Fatal("refusal routed around")
	}
}
func TestSingleFlight(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	e := New(rankFunc(func(context.Context, []byte) ([]int, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release

		return []int{1, 2, 3, 4}, nil
	}))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			p, err := e.Build(context.Background(), input(), "actor")
			if err != nil || (p.AIStatus != aiStatusGenerated && p.AIStatus != aiStatusCached) {
				t.Errorf("request failed: %v %s", err, p.AIStatus)
			}
		})
	}
	<-entered
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatal("duplicate provider calls")
	}
}
func TestBudgetPrerequisiteAndNeutralErrors(t *testing.T) {
	t.Parallel()

	r := input()
	r.TopicIDs = []string{testID(50)}
	r.Topics = append(r.Topics, planner.Topic{ID: testID(50), PrerequisiteIDs: []string{testID(1)}})
	r.Tasks = append(r.Tasks, planner.Task{ID: testID(60), TaskVersionID: testID(160), TopicID: testID(50), Title: "Blocked", Difficulty: "easy", TaskType: "single_choice"})
	r.Evidence = []planner.Evidence{{TaskID: testID(2), TaskVersionID: testID(102), Verdict: "infrastructure_error", OccurredAt: time.Now()}}
	e := New(rankFunc(func(_ context.Context, b []byte) ([]int, error) {
		if strings.Contains(string(b), "Blocked") {
			t.Error("blocked task sent")
		}

		return []int{4, 3, 2, 1}, nil
	}))
	p, err := e.Build(context.Background(), r, "actor")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, s := range p.Sessions {
		if s.TotalMinutes > 10 {
			t.Fatal("budget exceeded")
		}
		for _, item := range s.Items {
			if seen[item.TaskID] || item.TopicID != testID(1) {
				t.Fatal("invalid task")
			}
			seen[item.TaskID] = true
		}
	}
	if p.TopicInsights[0].FailedTasks != 0 || len(p.TopicInsights[1].BlockedBy) != 1 {
		t.Fatal("invalid evidence")
	}
}
func TestUserQuotaAndCacheTTL(t *testing.T) {
	t.Parallel()

	e := New(rankFunc(func(context.Context, []byte) ([]int, error) { return []int{1, 2, 3, 4}, nil }))
	now := time.Now()
	e.now = func() time.Time { return now }
	r := input()
	for i := range 10 {
		r.Goal = fmt.Sprint(i)
		p, _ := e.Build(context.Background(), r, "actor")
		if p.AIStatus != aiStatusGenerated {
			t.Fatal(p.AIStatus)
		}
	}
	r.Goal = "last"
	p, _ := e.Build(context.Background(), r, "actor")
	if p.AIStatus != aiStatusLimitReached {
		t.Fatal("quota ignored")
	}
	now = now.Add(time.Hour)
	p, _ = e.Build(context.Background(), r, "actor")
	if p.AIStatus != aiStatusGenerated {
		t.Fatal("quota did not reset")
	}
	now = now.Add(16 * time.Minute)
	p, _ = e.Build(context.Background(), r, "actor")
	if p.AIStatus != aiStatusGenerated {
		t.Fatal("expired cache used")
	}
}

func TestCandidatePoolRespectsActualBudgetAndKeepsTail(t *testing.T) {
	t.Parallel()

	r := input()
	r.SessionCount = 8
	r.Tasks = nil
	for i := 10; i < 200; i++ {
		difficulty := "medium"
		if i >= 140 {
			difficulty = "easy"
		}
		r.Tasks = append(r.Tasks, planner.Task{ID: testID(i), TaskVersionID: testID(i + 1000), TopicID: testID(1), Title: "Task", Difficulty: difficulty, TaskType: "single_choice"})
		if i < 13 {
			r.Evidence = append(r.Evidence, planner.Evidence{TaskID: testID(i), TaskVersionID: testID(i + 1000), Verdict: "accepted", OccurredAt: time.Now()})
		}
	}
	provider := rankFunc(func(_ context.Context, data []byte) ([]int, error) {
		var c compactInput
		if err := json.Unmarshal(data, &c); err != nil {
			t.Fatal(err)
		}
		ids := make([]int, 0, len(c.Candidates))
		for _, item := range c.Candidates {
			if item.Minutes > r.Minutes {
				t.Fatal("overbudget candidate")
			}
			ids = append(ids, item.ID)
		}

		return ids, nil
	})
	plan, err := New(provider).Build(context.Background(), r, "actor")
	if err != nil || plan.AIStatus != aiStatusGenerated || len(plan.Sessions) != 8 {
		t.Fatalf("shortened plan: %v %+v", err, plan)
	}
	r.Minutes = 240
	plan, err = New(provider).Build(context.Background(), r, "actor")
	if err != nil || plan.AIStatus != aiStatusGenerated {
		t.Fatal(err, plan.AIStatus)
	}
	count := 0
	for _, session := range plan.Sessions {
		count += len(session.Items)
	}
	if count <= 40 {
		t.Fatalf("deterministic tail lost: %d", count)
	}
}
