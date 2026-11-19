package studyplans

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/overmindv/recommendations/internal/planner"
)

func TestPostgresExplanationKeepsOriginalEvidenceAndChecksOwnership(t *testing.T) {
	ctx, pool := t.Context(), studyTestPool(t)
	id, store := studySeed(t, pool), NewPostgres(pool)
	actor, peer, goal := id(1), id(2), id(3)
	input := Generation{Expected: Expected{GoalVersion: 1, AvailabilityVersion: 1}, RequestID: id(80)}
	source, _, err := store.Prepare(ctx, actor, goal, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		WITH graph AS (
		    UPDATE catalog_projection_state SET active_generation=active_generation+1 WHERE singleton RETURNING active_generation
		)
		INSERT INTO catalog_projected_topics (generation,topic_id,revision,tombstone,metadata)
		SELECT active_generation,$1,1,false,'{"status":"active"}' FROM graph`, id(4)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO learning_task_outcomes
		    (submission_id,learner_id,task_id,task_version_id,topic_ids,verdict,evidence_kind,graded_at,event_id,correction_version)
		VALUES ($1,$2,$3,$4,ARRAY[$5::uuid],'wrong_answer','independent',now(),$6,1)`,
		id(110), peer, id(65), id(75), id(4), id(210)); err != nil {
		t.Fatal(err)
	}
	fixture := testDraft(t, source, id)
	owner := studySourceFunc(func(context.Context, string, string, []string) ([]planner.StudyResource, error) {
		rows := testResources(id)
		other := rows[0]
		other.ID, other.RevisionID = id(33), id(43)
		return append(rows, other), nil
	})
	builder := NewProjectedBuilder(pool, owner, fixture.Plan.RankingPolicy, fixture.EvidencePolicy)
	service := NewService(store, builder)
	initial, err := service.Generate(ctx, actor, goal, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Explain(ctx, peer, goal, 1); !errors.Is(err, ErrNotFound) {
		t.Fatal("a foreign requester read the explanation:", err)
	}
	first, err := service.Explain(ctx, actor, goal, 1)
	if err != nil || len(first.Steps) != 3 {
		t.Fatal("expected an explanation for each saved step:", err)
	}
	theory := first.Steps[0]
	if theory.Alternative == nil || theory.Alternative.ID != id(33) || first.Steps[2].Alternative != nil ||
		theory.Evidence.Uncertainty != "no_evidence" || theory.Evidence.ScoreBPS != nil || theory.Evidence.SampleCount != 0 ||
		theory.Evidence.Ref == nil || theory.Evidence.Ref.Watermark != initial.EvidenceWatermark || theory.Evidence.Ref.CapturedAt.After(initial.CreatedAt) {
		t.Fatal("explanation leaked peer evidence or lost the selected version:", theory)
	}
	if _, err := service.StartActivity(ctx, actor, goal, 1, 1); err != nil {
		t.Fatal(err)
	}
	writeRebuildGrade(t, pool, id, 1, 61, 71, "accepted")
	writeRebuildGrade(t, pool, id, 2, 62, 72, "accepted")
	old, err := service.Explain(ctx, actor, goal, 1)
	if err != nil || len(old.Steps) != 3 || old.Steps[0].Alternative != nil || old.Steps[0].Evidence.ScoreBPS != nil {
		t.Fatal("a newer grade rewrote the reason or started step offered a replacement:", err)
	}
	worker := NewRebuildWorker(store, builder, slog.New(slog.DiscardHandler))
	runRebuildFixture(t, pool, worker, goal)
	if _, err := service.Explain(ctx, actor, goal, 1); !errors.Is(err, ErrConflict) {
		t.Fatal("explanation accepted an obsolete version:", err)
	}
	next, err := service.Explain(ctx, actor, goal, 2)
	if err != nil || len(next.Steps) != 1 || next.Steps[0].Evidence.Ref == nil ||
		*next.Steps[0].Evidence.Ref != *theory.Evidence.Ref || next.Steps[0].Evidence.Uncertainty != "no_evidence" {
		t.Fatal("adaptation did not preserve the original explanation for a started step:", err)
	}
}

func TestPostgresExplanationDoesNotInventEvidenceForOldPlan(t *testing.T) {
	pool := studyTestPool(t)
	store := NewPostgres(pool)
	id, _ := readyFixturePlan(t, pool, store)
	view, err := store.Explain(t.Context(), id(1), id(3), 1)
	if err != nil || len(view.Steps) == 0 || view.Steps[0].Evidence.Uncertainty != "not_recorded" ||
		view.Steps[0].Evidence.ScoreBPS != nil || view.Steps[0].Evidence.Ref != nil {
		t.Fatal("old version was assigned an evidence summary it did not record:", err)
	}
}
