package studyplans

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/planner"
)

func enqueueFixture(t *testing.T, pool *pgxpool.Pool, goal string) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `
		INSERT INTO study_plan_rebuilds (goal_id,first_requested_at,requested_at,available_at)
		VALUES ($1,now()-interval '3 seconds',now()-interval '2 seconds',now()-interval '1 second')`, goal)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, "DELETE FROM study_plan_rebuilds WHERE goal_id=$1", goal)
	})
}

func readyFixturePlan(t *testing.T, pool *pgxpool.Pool, store *Postgres) (func(int) string, Draft) {
	t.Helper()
	id := studySeed(t, pool)
	gen := Generation{Expected: Expected{GoalVersion: 1, AvailabilityVersion: 1}, RequestID: id(90)}
	source, _, err := store.Prepare(t.Context(), id(1), id(3), gen)
	if err != nil {
		t.Fatal(err)
	}
	draft := testDraft(t, source, id)
	if _, err := store.Save(t.Context(), id(1), id(3), gen, source, draft); err != nil {
		t.Fatal(err)
	}
	return id, draft
}

func takeFixtureRebuild(t *testing.T, store *Postgres) (RebuildJob, RebuildBase) {
	t.Helper()
	job, found, err := store.ClaimRebuild(t.Context(), 30*time.Second)
	if err != nil || !found {
		t.Fatal("could not claim eligible rebuild:", found, err)
	}
	base, err := store.LoadRebuild(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	return job, base
}

func TestPostgresRebuildLeaseDoesNotLoseNewEvidence(t *testing.T) {
	ctx, pool := t.Context(), studyTestPool(t)
	store := NewPostgres(pool)
	id, _ := readyFixturePlan(t, pool, store)
	goal := id(3)
	enqueueFixture(t, pool, goal)
	var workers sync.WaitGroup
	jobs, found, errs := make([]RebuildJob, 2), make([]bool, 2), make([]error, 2)
	for i := range jobs {
		workers.Go(func() { jobs[i], found[i], errs[i] = store.ClaimRebuild(ctx, 30*time.Second) })
	}
	workers.Wait()
	if errs[0] != nil || errs[1] != nil || found[0] == found[1] {
		t.Fatal("queue was not claimed by exactly one worker:", found, errs)
	}
	oldJob := jobs[0]
	if found[1] {
		oldJob = jobs[1]
	}
	if oldJob.GoalID != goal {
		t.Fatal("queue test picked another fixture; use a database dedicated to study plan tests")
	}
	base, err := store.LoadRebuild(ctx, oldJob)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE study_plan_rebuilds SET leased_until=now()-interval '1 second' WHERE goal_id=$1", goal); err != nil {
		t.Fatal(err)
	}
	renewed, current := takeFixtureRebuild(t, store)
	if renewed.Token == oldJob.Token || renewed.Revision != oldJob.Revision {
		t.Fatal("expired lease did not receive a distinct worker token")
	}
	if err := store.CommitRebuild(ctx, oldJob, base, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("expired job updated or acknowledged a plan:", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE study_plan_rebuilds SET revision=revision+1 WHERE goal_id=$1", goal); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitRebuild(ctx, renewed, current, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("old request acknowledged a newer revision:", err)
	}
	if err := store.ReleaseRebuild(ctx, renewed, time.Minute); err != nil {
		t.Fatal(err)
	}
	latest, latestBase := takeFixtureRebuild(t, store)
	if latest.Revision != renewed.Revision+1 {
		t.Fatal("changed evidence was not requeued:", latest.Revision)
	}
	if err := store.CommitRebuild(ctx, latest, latestBase, nil); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.ClaimRebuild(ctx, 30*time.Second); err != nil || found {
		t.Fatal("successful no-op did not remove pending work:", found, err)
	}
}

func TestPostgresRebuildCarriesStartedAndCompletedSteps(t *testing.T) {
	ctx, pool := t.Context(), studyTestPool(t)
	store := NewPostgres(pool)
	id, draft := readyFixturePlan(t, pool, store)
	actor, goal := id(1), id(3)
	first, err := store.CompleteActivity(ctx, actor, goal, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	enqueueFixture(t, pool, goal)
	job, base := takeFixtureRebuild(t, store)
	started, err := store.StartActivity(ctx, actor, goal, 1, 2)
	if err != nil || findItem(started, 2).ActivityStarted == nil || findItem(started, 2).ActivityDone != nil {
		t.Fatal("start was not kept separate from completion:", started, err)
	}
	if err := store.CommitRebuild(ctx, job, base, &draft); !errors.Is(err, ErrConflict) {
		t.Fatal("rebuild overwrote a step started during calculation:", err)
	}
	if err := store.ReleaseRebuild(ctx, job, 0); err != nil {
		t.Fatal(err)
	}
	job, base = takeFixtureRebuild(t, store)
	editDraftStep(t, &draft, 2, func(item *planner.StudyItem) { item.RevisionID = id(86) })
	if err := store.CommitRebuild(ctx, job, base, &draft); !errors.Is(err, ErrStepBlocked) {
		t.Fatal("rebuild replaced a protected version:", err)
	}
	editDraftStep(t, &draft, 2, func(item *planner.StudyItem) { item.RevisionID = id(41) })
	editDraftStep(t, &draft, 3, func(item *planner.StudyItem) { item.ID, item.RevisionID = id(87), id(88) })
	draft.EvidenceWatermark = sha256.Sum256([]byte("new verified input"))
	if err := store.CommitRebuild(ctx, job, base, &draft); err != nil {
		t.Fatal(err)
	}
	reloaded, err := store.Current(ctx, actor, goal)
	if err != nil || reloaded.Version != 2 || reloaded.NextStep == nil || *reloaded.NextStep != 2 {
		t.Fatal("current plan did not advance while preserving next step:", reloaded, err)
	}
	if !equalTimes(findItem(reloaded, 1).ActivityDone, findItem(first, 1).ActivityDone) ||
		!equalTimes(findItem(reloaded, 2).ActivityStarted, findItem(started, 2).ActivityStarted) ||
		findItem(reloaded, 2).ActivityDone != nil || findItem(reloaded, 3).ID != id(87) {
		t.Fatal("progress changed or an unstarted step could not be replaced:", reloaded)
	}
	if _, err := store.CompleteActivity(ctx, actor, goal, 1, 2); !errors.Is(err, ErrConflict) {
		t.Fatal("superseded version still accepted progress:", err)
	}
}

func editDraftStep(t *testing.T, draft *Draft, ordinal int, edit func(*planner.StudyItem)) {
	t.Helper()
	for w := range draft.Plan.Weeks {
		for i := range draft.Plan.Weeks[w].Items {
			ordinal--
			if ordinal == 0 {
				edit(&draft.Plan.Weeks[w].Items[i])
				return
			}
		}
	}
	t.Fatal("missing fixture plan step")
}
