package studyplans

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func cleanupFeedbackQueue(t *testing.T, pool *pgxpool.Pool, goal string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DELETE FROM study_plan_rebuilds WHERE goal_id=$1", goal); err != nil {
			t.Error("remove test rebuild:", err)
		}
	})
}

//nolint:paralleltest // Воркер читает общую очередь пересчета в выделенной БД.
func TestPostgresFeedbackSkipKeepsHistoryWithoutGrantingMastery(t *testing.T) {
	ctx, pool := t.Context(), studyTestPool(t)
	store := NewPostgres(pool)
	id, worker := rebuildFixture(t, pool, store)
	actor, goal := id(1), id(3)
	cleanupFeedbackQueue(t, pool, goal)
	started, startErr := store.StartActivity(ctx, actor, goal, 1, 1)
	if startErr != nil {
		t.Fatal(startErr)
	}
	skip := Feedback{RequestID: id(66), PlanVersion: 1, Action: FeedbackSkip, Ordinal: new(1)}
	if _, err := store.SubmitFeedback(ctx, id(2), goal, skip); !errors.Is(err, ErrNotFound) {
		t.Fatal("a foreign learner submitted feedback:", err)
	}
	var writes sync.WaitGroup
	var receipts [2]FeedbackReceipt
	var failures [2]error
	for i := range receipts {
		writes.Go(func() { receipts[i], failures[i] = store.SubmitFeedback(ctx, actor, goal, skip) })
	}
	writes.Wait()
	if failures[0] != nil || failures[1] != nil || !receipts[0].AcceptedAt.Equal(receipts[1].AcceptedAt) {
		t.Fatal("concurrent replay did not return the original receipt:", failures)
	}
	changed := skip
	changed.Action = FeedbackNotRelevant
	if _, err := store.SubmitFeedback(ctx, actor, goal, changed); !errors.Is(err, ErrRequestUsed) {
		t.Fatal("same key accepted a different action:", err)
	}
	repeated := skip
	repeated.RequestID = id(67)
	if _, err := store.SubmitFeedback(ctx, actor, goal, repeated); !errors.Is(err, ErrStepBlocked) {
		t.Fatal("a new key let the learner skip the same step twice:", err)
	}
	waiting, readErr := store.Current(ctx, actor, goal)
	if readErr != nil || waiting.NextStep != nil || waiting.BlockedReason != planReplanning || waiting.GoalDemonstrated ||
		!findItem(waiting, 1).Skipped || findItem(waiting, 1).ActivityDone != nil {
		t.Fatal("skip opened a later step or was counted as completion:", readErr)
	}
	if _, err := store.CompleteActivity(ctx, actor, goal, 1, 1); !errors.Is(err, ErrStepBlocked) {
		t.Fatal("skipped step still accepted completion:", err)
	}
	if found, err := worker.step(ctx); err != nil || !found {
		t.Fatal("worker did not process feedback:", found, err)
	}
	next, readErr := store.Current(ctx, actor, goal)
	if readErr != nil || next.Version != 2 || next.NextStep == nil || next.GoalDemonstrated ||
		findItem(next, 1).ID != id(33) || findItem(next, 1).Skipped {
		t.Fatal("plan reused the skipped resource or lost the replacement:", next.Version, readErr)
	}
	replay, replayErr := store.SubmitFeedback(ctx, actor, goal, skip)
	if replayErr != nil || !replay.AcceptedAt.Equal(receipts[0].AcceptedAt) {
		t.Fatal("replay was not preserved across plan versions:", replayErr)
	}
	if _, err := store.SubmitFeedback(ctx, actor, goal, repeated); !errors.Is(err, ErrConflict) {
		t.Fatal("stale feedback changed the new plan:", err)
	}
	gen := Generation{Expected: Expected{GoalVersion: 1, AvailabilityVersion: 1}, RequestID: id(90)}
	_, old, err := store.Prepare(ctx, actor, goal, gen)
	if err != nil || old == nil || old.Current || !findItem(*old, 1).Skipped ||
		!equalTimes(findItem(*old, 1).ActivityStarted, findItem(started, 1).ActivityStarted) ||
		findItem(*old, 1).ActivityDone != nil {
		t.Fatal("previous version did not retain the original activity and skip:", err)
	}
	var outcomes, requests int
	var cooldown bool
	err = pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM learning_task_outcomes WHERE learner_id=$1),
		       (SELECT count(*) FROM study_plan_feedback WHERE goal_id=$2),
		       EXISTS (SELECT 1 FROM study_plan_resource_cooldowns WHERE goal_id=$2 AND kind='theory'
		           AND resource_id=$3 AND expires_at>statement_timestamp()+interval '6 days')`, actor, goal, id(30)).
		Scan(&outcomes, &requests, &cooldown)
	if err != nil || outcomes != 0 || requests != 1 || !cooldown {
		t.Fatal("skip created learning evidence or applied cooldown more than once:", outcomes, requests, cooldown, err)
	}
}

//nolint:paralleltest // Тест захватывает записи из общей очереди пересчета в выделенной БД.
func TestPostgresFeedbackPauseResumeAndResizeKeepStartedSteps(t *testing.T) {
	ctx, pool := t.Context(), studyTestPool(t)
	store := NewPostgres(pool)
	id, worker := rebuildFixture(t, pool, store)
	actor, goal := id(1), id(3)
	cleanupFeedbackQueue(t, pool, goal)
	if _, err := store.CompleteActivity(ctx, actor, goal, 1, 1); err != nil {
		t.Fatal(err)
	}
	started, startErr := store.StartActivity(ctx, actor, goal, 1, 2)
	if startErr != nil {
		t.Fatal(startErr)
	}
	submit := func(key int, action FeedbackAction, ordinal, minutes *int) error {
		t.Helper()
		_, err := store.SubmitFeedback(ctx, actor, goal,
			Feedback{RequestID: id(key), PlanVersion: 1, Action: action, Ordinal: ordinal, WeeklyMinutes: minutes})

		return err
	}
	if err := submit(75, FeedbackTooHard, new(2), nil); err != nil {
		t.Fatal(err)
	}
	job, base := takeFixtureRebuild(t, store)
	if err := submit(76, FeedbackPause, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitRebuild(ctx, job, base, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("worker overwrote a paused plan:", err)
	}
	paused, readErr := store.Current(ctx, actor, goal)
	if readErr != nil || paused.PausedAt == nil || paused.NextStep != nil || paused.BlockedReason != planPaused {
		t.Fatal("paused plan still exposed a next step:", readErr)
	}
	if _, err := store.CompleteActivity(ctx, actor, goal, 1, 2); !errors.Is(err, ErrStepBlocked) {
		t.Fatal("pause did not stop progress writes:", err)
	}
	if err := submit(77, FeedbackSetBudget, nil, new(121)); !errors.Is(err, ErrBudget) {
		t.Fatal("budget exceeded available time reserved by other goals:", err)
	}
	if err := submit(78, FeedbackSetBudget, nil, new(8)); err != nil {
		t.Fatal(err)
	}
	if err := submit(80, FeedbackSetBudget, nil, new(7)); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.ClaimRebuild(ctx, 30*time.Second); err != nil || found {
		t.Fatal("changing a paused goal started a rebuild:", found, err)
	}
	if err := submit(79, FeedbackResume, nil, nil); err != nil {
		t.Fatal(err)
	}
	if found, err := worker.step(ctx); err != nil || !found {
		t.Fatal("resume did not request recalculation:", found, err)
	}
	updated, err := store.Current(ctx, actor, goal)
	if err != nil || updated.Version != 2 || updated.Stale || updated.PausedAt != nil || updated.WeeklyMinutes != 7 ||
		updated.GoalVersion != 3 || updated.NextStep == nil || *updated.NextStep != 2 {
		t.Fatal("resume did not publish the resized plan:", updated.Version, updated.WeeklyMinutes, err)
	}
	theory, practice := findItem(updated, 1), findItem(updated, 2)
	if theory == nil || practice == nil || theory.ActivityDone == nil ||
		!equalTimes(practice.ActivityStarted, findItem(started, 2).ActivityStarted) || practice.ActivityDone != nil {
		t.Fatal("resizing erased existing activity")
	}
	for _, week := range updated.Weeks {
		if week.PlannedMinutes > 7 && (len(week.Items) != 2 || week.Items[0].Ordinal != 1 || week.Items[1].Ordinal != 2) {
			t.Fatal("resizing added work beyond the new budget:", week)
		}
	}
}

//nolint:paralleltest // Воркер читает общую очередь пересчета в выделенной БД.
func TestPostgresFeedbackSkipCheckStillRequiresAnIndependentGrade(t *testing.T) {
	ctx, pool := t.Context(), studyTestPool(t)
	store := NewPostgres(pool)
	id, worker := rebuildFixture(t, pool, store)
	actor, goal := id(1), id(3)
	cleanupFeedbackQueue(t, pool, goal)
	for _, ordinal := range []int{1, 2} {
		if _, err := store.CompleteActivity(ctx, actor, goal, 1, ordinal); err != nil {
			t.Fatal(err)
		}
	}
	skip := Feedback{RequestID: id(68), PlanVersion: 1, Action: FeedbackSkip, Ordinal: new(3)}
	if _, err := store.SubmitFeedback(ctx, actor, goal, skip); err != nil {
		t.Fatal(err)
	}
	if found, err := worker.step(ctx); err != nil || !found {
		t.Fatal("worker did not process the skipped check:", found, err)
	}
	next, err := store.Current(ctx, actor, goal)
	if err != nil || next.Version != 2 || next.NextStep == nil || *next.NextStep != 3 || next.GoalDemonstrated ||
		findItem(next, 3).ID != id(34) || findItem(next, 3).Assessment != nil {
		t.Fatal("replacing a check satisfied the independent assessment:", next.Version, err)
	}
	if _, err := store.Assess(ctx, actor, goal, 2, 3); !errors.Is(err, ErrPending) {
		t.Fatal("replacement was assessed without a grade:", err)
	}
}
