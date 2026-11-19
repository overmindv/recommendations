package studyplans

import (
	"context"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

func rebuildFixture(t *testing.T, pool *pgxpool.Pool, store *Postgres) (func(int) string, *RebuildWorker) {
	t.Helper()
	id, draft := readyFixturePlan(t, pool, store)
	if _, err := pool.Exec(t.Context(), `
		WITH graph AS (
		    UPDATE catalog_projection_state SET active_generation=active_generation+1 WHERE singleton RETURNING active_generation
		)
		INSERT INTO catalog_projected_topics (generation,topic_id,revision,tombstone,metadata)
		SELECT active_generation,$1,1,false,'{"status":"active"}' FROM graph`, id(4)); err != nil {
		t.Fatal(err)
	}
	owner := studySourceFunc(func(context.Context, string, string, []string) ([]planner.StudyResource, error) {
		rows := testResources(id)
		theory, check := rows[0], rows[2]
		theory.ID, theory.RevisionID = id(33), id(43)
		check.ID, check.RevisionID = id(34), id(44)
		return append(rows, theory, check), nil
	})
	builder := NewProjectedBuilder(pool, owner, draft.Plan.RankingPolicy, draft.EvidencePolicy)
	return id, NewRebuildWorker(store, builder, slog.New(slog.DiscardHandler))
}

func writeRebuildGrade(t *testing.T, pool *pgxpool.Pool, id func(int) string, nonce, task, version int, verdict string) {
	t.Helper()
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO learning_task_outcomes
		    (submission_id,learner_id,task_id,task_version_id,topic_ids,verdict,evidence_kind,graded_at,event_id,correction_version)
		VALUES ($1,$2,$3,$4,ARRAY[$5::uuid],$6,'independent',clock_timestamp(),$7,1)`,
		id(100+nonce), id(1), id(task), id(version), id(4), verdict, id(200+nonce)); err != nil {
		t.Fatal(err)
	}
}

func runRebuildFixture(t *testing.T, pool *pgxpool.Pool, worker *RebuildWorker, goal string) {
	t.Helper()
	enqueueFixture(t, pool, goal)
	if found, err := worker.step(t.Context()); err != nil || !found {
		t.Fatal("adaptation did not process the ready plan:", found, err)
	}
}

func TestPostgresRebuildAddsRemedialTheoryAfterFailedCheck(t *testing.T) {
	ctx, pool := t.Context(), studyTestPool(t)
	store := NewPostgres(pool)
	id, worker := rebuildFixture(t, pool, store)
	actor, goal := id(1), id(3)
	for step := 1; step <= 2; step++ {
		if _, err := store.CompleteActivity(ctx, actor, goal, 1, step); err != nil {
			t.Fatal(err)
		}
	}
	started, err := store.StartActivity(ctx, actor, goal, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	writeRebuildGrade(t, pool, id, 1, 32, 42, "wrong_answer")
	writeRebuildGrade(t, pool, id, 2, 32, 42, "wrong_answer")
	runRebuildFixture(t, pool, worker, goal)

	view, err := store.Current(ctx, actor, goal)
	if err != nil || view.Version != 2 || view.NextStep == nil || *view.NextStep != 4 || view.GoalDemonstrated {
		t.Fatal("failed check did not advance to remediation:", view.Version, view.NextStep, view.BlockedReason, err)
	}
	failed, theory, check := findItem(view, 3), findItem(view, 4), findItem(view, 5)
	if failed == nil || !failed.Superseded || failed.Assessment == nil || failed.Assessment.Verdict != "wrong_answer" ||
		!failed.Assessment.GradedAt.Before(view.CreatedAt) || !equalTimes(failed.ActivityStarted, findItem(started, 3).ActivityStarted) ||
		theory == nil || theory.ID != id(33) || theory.Stage != planner.StudyTheory ||
		check == nil || check.ID != id(34) || check.Stage != planner.StudyCheck {
		t.Fatal("remedial material, alternate check or original proof missing")
	}
	writeRebuildGrade(t, pool, id, 3, 32, 42, "wrong_answer")
	runRebuildFixture(t, pool, worker, goal)
	view, err = store.Current(ctx, actor, goal)
	if err != nil || view.Version != 2 || view.NextStep == nil || *view.NextStep != 4 {
		t.Fatal("repeated failure changed an identical plan:", view.Version, view.NextStep, err)
	}
}

func TestPostgresRebuildAcceleratesWithoutDroppingStartedStep(t *testing.T) {
	ctx, pool := t.Context(), studyTestPool(t)
	store := NewPostgres(pool)
	id, worker := rebuildFixture(t, pool, store)
	actor, goal := id(1), id(3)
	started, err := store.StartActivity(ctx, actor, goal, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	writeRebuildGrade(t, pool, id, 1, 65, 75, "accepted")
	writeRebuildGrade(t, pool, id, 2, 66, 76, "accepted")
	runRebuildFixture(t, pool, worker, goal)

	view, err := store.Current(ctx, actor, goal)
	if err != nil || view.Version != 2 || view.NextStep == nil || *view.NextStep != 1 || !view.GoalDemonstrated {
		t.Fatal("verified knowledge did not shorten plan:", view.Version, view.NextStep, view.GoalDemonstrated, err)
	}
	if findItem(view, 1) == nil || !equalTimes(findItem(view, 1).ActivityStarted, findItem(started, 1).ActivityStarted) ||
		findItem(view, 2) != nil || findItem(view, 3) != nil {
		t.Fatal("ongoing theory was removed or an unnecessary step was retained")
	}
}

func TestPrerequisiteRepairDoesNotSkipToUnrelatedTopics(t *testing.T) {
	step := func(ordinal int, topic string, guards ...string) *Item {
		return &Item{Ordinal: ordinal, StudyItem: planner.StudyItem{
			Ranked: ranking.Ranked{TopicID: topic}, RequiresCheck: guards,
		}}
	}
	pending := []*Item{
		step(1, "sorting", "arrays"), step(2, "trees"), step(3, "arrays", "loops"), step(4, "loops"),
	}
	if got := prerequisiteRepair(pending, func(string) bool { return false }); got != pending[3] {
		t.Fatal("did not choose the unmet prerequisite first:", got)
	}
	if got := prerequisiteRepair(pending, func(topic string) bool { return topic == "loops" }); got != pending[2] {
		t.Fatal("did not return to the next prerequisite:", got)
	}
}
