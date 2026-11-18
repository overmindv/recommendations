package learnergoals

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testStore(t testing.TB) (*Postgres, string) {
	t.Helper()
	dsn := os.Getenv("GOALS_TEST_DSN")
	if dsn == "" {
		t.Skip("GOALS_TEST_DSN is not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || (!strings.HasSuffix(cfg.ConnConfig.Database, "_test") && !strings.HasSuffix(cfg.ConnConfig.Database, "_component")) {
		t.Fatal("GOALS_TEST_DSN must point to a dedicated test or component database")
	}
	cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var actor string
	if err := pool.QueryRow(t.Context(), "SELECT gen_random_uuid()::text").Scan(&actor); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for _, query := range []string{
			"DELETE FROM learner_goal_requests WHERE user_id = $1",
			"DELETE FROM learner_goals WHERE user_id = $1",
			"DELETE FROM learner_availability WHERE user_id = $1",
		} {
			if _, err := pool.Exec(ctx, query, actor); err != nil {
				t.Error(err)
			}
		}
	})
	return NewPostgres(pool), actor
}

func TestPostgresLearnerGoalLifecycle(t *testing.T) {
	store, actor := testStore(t)
	foreignStore, foreign := testStore(t)
	ctx := t.Context()
	now := time.Date(2026, 11, 19, 2, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if _, err := store.GetAvailability(ctx, actor); !errors.Is(err, ErrNotFound) {
		t.Fatal("new learner already has availability", err)
	}
	if items, err := store.List(ctx, actor); err != nil || len(items) != 0 {
		t.Fatal("new learner already has goals", err)
	}
	in := AvailabilityInput{WeeklyMinutes: 120, Timezone: "America/Los_Angeles"}
	availability, err := store.SaveAvailability(ctx, actor, Write{RequestID: testID(10)}, in)
	if err != nil || availability.Version != 1 || availability.Timezone != in.Timezone {
		t.Fatal("availability was not stored", availability, err)
	}
	if _, err := foreignStore.SaveAvailability(ctx, foreign, Write{RequestID: testID(10)}, in); err != nil {
		t.Fatal("request IDs should be scoped to owner", err)
	}
	goal := validGoal()
	goal.Deadline = "2026-11-18"
	created, err := store.Create(ctx, actor, testID(11), goal)
	if err != nil || created.Version != 1 || created.UserID != actor || created.Deadline != goal.Deadline ||
		created.Timezone != in.Timezone || len(created.SelfAssessments) != 2 || created.SelfAssessments[1].Level != "unknown" {
		t.Fatal("invalid persisted goal", created, err)
	}
	if _, err := store.Get(ctx, foreign, created.ID); !errors.Is(err, ErrNotFound) {
		t.Error("foreign read did not return not found", err)
	}
	if _, err := store.Update(ctx, foreign, created.ID, Write{testID(12), 1}, goal); !errors.Is(err, ErrNotFound) {
		t.Error("foreign update did not return not found", err)
	}
	if err := store.Delete(ctx, foreign, created.ID, Write{testID(12), 1}); !errors.Is(err, ErrNotFound) {
		t.Error("foreign delete did not return not found", err)
	}
	goal.WeeklyMinutes = 90
	if _, err := store.Create(ctx, actor, testID(11), goal); !errors.Is(err, ErrRequestUsed) {
		t.Error("request ID accepted a different payload", err)
	}
	write := Write{testID(13), 1}
	updated, err := store.Update(ctx, actor, created.ID, write, goal)
	if err != nil || updated.WeeklyMinutes != 90 || updated.Version != 2 {
		t.Fatal("goal update did not advance version", updated, err)
	}
	if replay, err := store.Update(ctx, actor, created.ID, write, goal); err != nil || replay.Version != 2 {
		t.Error("saved update was not replayed", replay, err)
	}
	if _, err := store.Update(ctx, actor, created.ID, Write{testID(14), 1}, goal); !errors.Is(err, ErrConflict) {
		t.Error("stale update did not conflict", err)
	}
	if err := store.Delete(ctx, actor, created.ID, Write{testID(15), 1}); !errors.Is(err, ErrConflict) {
		t.Error("stale delete did not conflict", err)
	}
	in.WeeklyMinutes = 80
	if _, err := store.SaveAvailability(ctx, actor, Write{testID(16), 1}, in); !errors.Is(err, ErrBudget) {
		t.Error("availability dropped below active allocations", err)
	}
	write = Write{testID(17), 2}
	if err := store.Delete(ctx, actor, created.ID, write); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, actor, created.ID, write); err != nil {
		t.Error("delete retry failed", err)
	}
	goal.WeeklyMinutes = 60
	if replay, err := store.Create(ctx, actor, testID(11), goal); err != nil || replay.ID != created.ID || replay.Version != 1 {
		t.Error("create retry did not preserve the first response", replay, err)
	}
	in.WeeklyMinutes = 20
	if next, err := store.SaveAvailability(ctx, actor, Write{testID(18), 1}, in); err != nil || next.Version != 2 {
		t.Error("delete did not release budget", next, err)
	}
	if _, err := store.SaveAvailability(ctx, actor, Write{testID(19), 1}, in); !errors.Is(err, ErrConflict) {
		t.Error("stale availability update did not conflict", err)
	}
	if items, err := store.List(ctx, actor); err != nil || len(items) != 0 {
		t.Error("retry recreated the deleted goal", items, err)
	}
}

func TestPostgresConcurrentAllocations(t *testing.T) {
	store, actor := testStore(t)
	ctx := t.Context()
	if _, err := store.SaveAvailability(ctx, actor, Write{RequestID: testID(21)}, AvailabilityInput{110, "UTC"}); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	type result struct {
		goal Goal
		err  error
	}
	resultCh := make(chan result, 2)
	for _, requestID := range []string{testID(22), testID(23)} {
		go func() {
			<-start
			input := validGoal()
			input.WeeklyMinutes = 70
			goal, err := store.Create(ctx, actor, requestID, input)
			resultCh <- result{goal, err}
		}()
	}
	close(start)
	first, second := <-resultCh, <-resultCh
	if first.err != nil {
		first, second = second, first
	}
	if first.err != nil || !errors.Is(second.err, ErrBudget) {
		t.Fatal("expected one successful allocation", first.err, second.err)
	}
	inactive := first.goal.Input
	inactive.Active = false
	if _, err := store.Update(ctx, actor, first.goal.ID, Write{testID(24), 1}, inactive); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, actor, testID(25), first.goal.Input); err != nil {
		t.Fatal("deactivation did not release the budget", err)
	}
	if _, err := store.Create(ctx, actor, testID(26), Input{Title: "Вторая тема", TargetTopics: []string{testID(3)}, Active: true, WeeklyMinutes: 40}); err != nil {
		t.Fatal("second active goal within budget should be accepted", err)
	}
}

func TestPostgresConcurrentVersionsAndExpiry(t *testing.T) {
	store, actor := testStore(t)
	ctx := t.Context()
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if _, err := store.SaveAvailability(ctx, actor, Write{RequestID: testID(30)}, AvailabilityInput{180, "UTC"}); err != nil {
		t.Fatal(err)
	}
	in := validGoal()
	createID := testID(31)
	start := make(chan struct{})
	type createResult struct {
		goal Goal
		err  error
	}
	created := make(chan createResult, 2)
	for range 2 {
		go func() {
			<-start
			goal, err := store.Create(ctx, actor, createID, in)
			created <- createResult{goal, err}
		}()
	}
	close(start)
	first, second := <-created, <-created
	if first.err != nil || second.err != nil || first.goal.ID == "" || first.goal.ID != second.goal.ID {
		t.Fatal("concurrent retries must return the same goal", first.err, second.err)
	}
	start = make(chan struct{})
	updates := make(chan error, 2)
	for _, write := range []Write{{testID(32), 1}, {testID(33), 1}} {
		go func() {
			<-start
			_, err := store.Update(ctx, actor, first.goal.ID, write, in)
			updates <- err
		}()
	}
	close(start)
	a, b := <-updates, <-updates
	if a != nil {
		a, b = b, a
	}
	if a != nil || !errors.Is(b, ErrConflict) {
		t.Fatal("expected exactly one version update", a, b)
	}
	now = now.Add(idempotencyTTL)
	reused, err := store.Create(ctx, actor, createID, in)
	if err != nil || reused.ID == first.goal.ID || reused.Version != 1 {
		t.Error("expired request key was not reusable", reused, err)
	}
}
