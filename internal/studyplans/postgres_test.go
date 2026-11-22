package studyplans

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/projection/catalog"
	"github.com/overmindv/recommendations/internal/ranking"
)

func studyTestPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("STUDY_PLANS_TEST_DSN")
	if dsn == "" {
		t.Skip("STUDY_PLANS_TEST_DSN is required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("STUDY_PLANS_TEST_DSN must point to a dedicated _test database")
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func studySeed(t testing.TB, pool *pgxpool.Pool) func(int) string {
	t.Helper()
	data := make([]byte, 8)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	data[6] = data[6]&0x3f | 0x80
	prefix := fmt.Sprintf("%x-%x-4000-%x", data[:4], data[4:6], data[6:])
	id := func(n int) string { return fmt.Sprintf("%s-%012x", prefix, n) }
	batch := &pgx.Batch{}
	batch.Queue("INSERT INTO learner_availability (user_id,weekly_minutes,timezone) VALUES ($1,180,'UTC'),($2,60,'UTC')", id(1), id(2))
	batch.Queue(`INSERT INTO learner_goals (id,user_id,title,target_topics,weekly_minutes,active,preferences)
		VALUES ($1,$2,'Test goal',ARRAY[$3::uuid],120,true,'{"languages":["python"]}'),
		       ($4,$2,'Other goal',ARRAY[$3::uuid],60,true,'{}')`, id(3), id(1), id(4), id(5))
	if err := pool.SendBatch(t.Context(), batch).Close(); err != nil {
		t.Fatal(err)
	}

	return id
}

func testDraft(t testing.TB, source Sources, id func(int) string) Draft {
	t.Helper()
	plan, err := planner.BuildLearningPlan(t.Context(), time.Now().UTC(), planner.LearningRequest{
		TargetTopicIDs: source.TargetTopics, Topics: map[string]ranking.Topic{id(4): {}}, Resources: testResources(id),
		AllowedLanguages: source.AllowedLanguages, Budget: source.Budget,
		Ranking: ranking.Policy{Version: "test-ranking", TargetBPS: 8000, UnknownGapBPS: 4000, PrerequisiteMinBPS: 7500,
			MediumDifficultyBPS: 5000, HardDifficultyBPS: 9500, Weights: ranking.Factors{Gap: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := mastery.Policy{
		Revision: "test-evidence", Algorithm: mastery.AlgorithmVersion, GapMaxBPS: 2500, DemonstratedBPS: 7500, MinWeightedTasks: 2,
	}

	return Draft{Plan: plan, EvidencePolicy: policy, EvidenceWatermark: sha256.Sum256([]byte("fixture independent evidence"))}
}

func testResources(id func(int) string) []planner.StudyResource {
	stages := [...]planner.StudyStage{planner.StudyTheory, planner.StudyPractice, planner.StudyCheck}
	resources := make([]planner.StudyResource, 0, len(stages))
	for i, stage := range stages {
		kind, difficulty := ranking.Practice, "easy"
		if stage == planner.StudyTheory {
			kind, difficulty = ranking.Theory, ""
		}
		resources = append(resources, planner.StudyResource{
			Candidate: ranking.Candidate{
				ResourceKey: ranking.ResourceKey{Kind: kind, ID: id(30 + i), RevisionID: id(40 + i)},
				TopicID:     id(4), Minutes: 5, Difficulty: difficulty, Available: true,
			},
			IndependentCheck: stage == planner.StudyCheck,
		})
	}

	return resources
}

func TestPostgresPlanCASAndReplay(t *testing.T) {
	t.Parallel()

	ctx, pool := t.Context(), studyTestPool(t)
	id := studySeed(t, pool)
	store := NewPostgres(pool)
	actor, peer, goal := id(1), id(2), id(3)
	generate := Generation{Expected: Expected{GoalVersion: 1, AvailabilityVersion: 1}, RequestID: id(10)}
	source, replay, err := store.Prepare(ctx, actor, goal, generate)
	if err != nil || replay != nil || source.Budget.ReservedMinutes != 60 || !reflect.DeepEqual(source.AllowedLanguages, []string{"python"}) {
		t.Fatal("could not load a new generation with the correct budget:", source, replay, err)
	}
	if _, _, foreignErr := store.Prepare(ctx, peer, goal, generate); !errors.Is(foreignErr, ErrNotFound) {
		t.Fatal("foreign goal was visible:", foreignErr)
	}
	draft := testDraft(t, source, id)
	requests := []Generation{generate, {Expected: generate.Expected, RequestID: id(11)}}
	results := make([]View, len(requests))
	failures := make([]error, len(requests))
	var group sync.WaitGroup
	for i, request := range requests {
		group.Go(func() { results[i], failures[i] = store.Save(ctx, actor, goal, request, source, draft) })
	}
	group.Wait()
	var winner Generation
	var got View
	for i, err := range failures {
		if err == nil && winner.RequestID == "" {
			winner, got = requests[i], results[i]
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal("concurrent plan replaced the head or returned the wrong error:", results, failures)
		}
	}
	if got.Version != 1 || got.NextStep == nil || *got.NextStep != 1 || got.GoalDemonstrated ||
		got.RankingPolicy != draft.Plan.RankingPolicy || got.EvidencePolicy != draft.EvidencePolicy {
		t.Fatal("saved view lost its schedule or rules:", got)
	}
	_, replay, err = store.Prepare(ctx, actor, goal, winner)
	if err != nil || replay == nil || !reflect.DeepEqual(*replay, got) {
		t.Fatal("idempotent prepare did not return the saved version:", replay, err)
	}
	wrong := winner
	wrong.PlanVersion++
	if _, _, keyErr := store.Prepare(ctx, actor, goal, wrong); !errors.Is(keyErr, ErrRequestUsed) {
		t.Fatal("idempotency key reused with different request:", keyErr)
	}

	next := Generation{Expected: Expected{GoalVersion: 1, AvailabilityVersion: 1, PlanVersion: 1}, RequestID: id(12)}
	source, replay, err = store.Prepare(ctx, actor, goal, next)
	if err != nil || replay != nil {
		t.Fatal("could not prepare next version:", replay, err)
	}
	if _, budgetErr := pool.Exec(ctx, "UPDATE learner_goals SET weekly_minutes=30,version=version+1 WHERE id=$1", id(5)); budgetErr != nil {
		t.Fatal(budgetErr)
	}
	if _, saveErr := store.Save(ctx, actor, goal, next, source, draft); !errors.Is(saveErr, ErrConflict) {
		t.Fatal("saved with a changed allocation of another goal:", saveErr)
	}
	stale, err := store.Current(ctx, actor, goal)
	if err != nil || stale.Version != 1 || !stale.Stale || stale.NextStep != nil {
		t.Fatal("a stale snapshot was rebuilt or still had actions:", stale, err)
	}
	source, replay, err = store.Prepare(ctx, actor, goal, next)
	if err != nil || replay != nil {
		t.Fatal("could not prepare with changed allocation:", replay, err)
	}
	draft = testDraft(t, source, id)
	for i := range results {
		group.Go(func() { results[i], failures[i] = store.Save(ctx, actor, goal, next, source, draft) })
	}
	group.Wait()
	if failures[0] != nil || failures[1] != nil || results[0].Version != 2 || !reflect.DeepEqual(results[0], results[1]) {
		t.Fatal("parallel request replay created different versions:", failures, results[0].Version, results[1].Version)
	}
	_, historical, err := store.Prepare(ctx, actor, goal, winner)
	if err != nil || historical == nil || historical.Version != 1 || historical.Current || historical.CreatedAt != got.CreatedAt {
		t.Fatal("retry stopped referring to its original version:", historical, err)
	}
	var versions, keys int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM study_plan_versions WHERE goal_id=$1),
		(SELECT count(*) FROM study_plan_requests WHERE goal_id=$1)`, goal).Scan(&versions, &keys); err != nil || versions != 2 || keys != 2 {
		t.Fatal("version and request rows were not atomic:", versions, keys, err)
	}
}

func TestPostgresPlanProgressNeedsIndependentEvidence(t *testing.T) {
	t.Parallel()

	ctx, pool := t.Context(), studyTestPool(t)
	id := studySeed(t, pool)
	actor, peer, goal := id(1), id(2), id(3)
	store := NewPostgres(pool)
	command := Generation{Expected: Expected{GoalVersion: 1, AvailabilityVersion: 1}, RequestID: id(14)}
	source, _, err := store.Prepare(ctx, actor, goal, command)
	if err != nil {
		t.Fatal(err)
	}
	draft := testDraft(t, source, id)
	plan, err := store.Save(ctx, actor, goal, command, source, draft)
	if err != nil {
		t.Fatal(err)
	}
	if _, stepErr := store.CompleteActivity(ctx, actor, goal, 1, 2); !errors.Is(stepErr, ErrStepBlocked) {
		t.Fatal("activity was allowed out of order:", stepErr)
	}
	for n := 1; n <= 2; n++ {
		plan, err = store.CompleteActivity(ctx, actor, goal, 1, n)
		if err != nil || findItem(plan, n).ActivityDone == nil || plan.GoalDemonstrated || plan.NextStep == nil || *plan.NextStep != n+1 {
			t.Fatal("activity did not advance cleanly or was counted as mastery:", plan, err)
		}
	}
	if _, stepErr := store.CompleteActivity(ctx, actor, goal, 1, 3); !errors.Is(stepErr, ErrStepBlocked) {
		t.Fatal("checkpoint could be self-attested:", stepErr)
	}
	record := func(n int, learner, task, rev, verdict, kind string) {
		t.Helper()
		if _, resultErr := pool.Exec(ctx, `INSERT INTO learning_task_outcomes
			(submission_id,learner_id,task_id,task_version_id,topic_ids,verdict,evidence_kind,graded_at,event_id,correction_version)
			VALUES ($1,$2,$3,$4,ARRAY[$5::uuid],$6,$7,clock_timestamp(),$8,1)`,
			id(100+n), learner, task, rev, id(4), verdict, kind, id(200+n)); resultErr != nil {
			t.Fatal(resultErr)
		}
	}
	record(1, peer, id(32), id(42), acceptedVerdict, "independent")
	record(2, actor, id(32), id(42), acceptedVerdict, "non_independent")
	if _, assessErr := store.Assess(ctx, actor, goal, 1, 3); !errors.Is(assessErr, ErrPending) {
		t.Fatal("foreign or non-independent activity counted:", assessErr)
	}
	record(3, actor, id(32), id(42), acceptedVerdict, "independent")
	plan, err = store.Assess(ctx, actor, goal, 1, 3)
	if err != nil || plan.GoalDemonstrated || findItem(plan, 3).Assessment == nil || plan.NextStep != nil {
		t.Fatal("assessment not saved or small sample declared mastery:", plan, err)
	}
	record(4, actor, id(35), id(45), acceptedVerdict, "independent")
	reopened := NewPostgres(studyTestPool(t))
	reloaded, err := reopened.Current(ctx, actor, goal)
	if err != nil || !reloaded.GoalDemonstrated || reloaded.Version != 1 || reloaded.EvidenceWatermark != plan.EvidenceWatermark ||
		findItem(reloaded, 1).ActivityDone == nil || findItem(reloaded, 2).ActivityDone == nil || findItem(reloaded, 3).Assessment == nil {
		t.Fatal("reload lost evidence or progress:", reloaded, err)
	}
	if _, correctionErr := pool.Exec(ctx, `UPDATE learning_task_outcomes SET event_id=$2,correction_version=2,verdict='wrong_answer'
		WHERE submission_id=$1`, id(103), id(250)); correctionErr != nil {
		t.Fatal(correctionErr)
	}
	corrected, err := reopened.Current(ctx, actor, goal)
	if err != nil || corrected.GoalDemonstrated || corrected.NextStep == nil || *corrected.NextStep != 3 || findItem(corrected, 3).Assessment != nil {
		t.Fatal("stale receipt remained valid after correction:", corrected, err)
	}
	failed, err := reopened.Assess(ctx, actor, goal, 1, 3)
	if err != nil || failed.GoalDemonstrated || failed.BlockedReason != failedCheck ||
		findItem(failed, 3).Assessment == nil || findItem(failed, 3).Assessment.Verdict != "wrong_answer" {
		t.Fatal("failed assessment did not block the plan:", failed, err)
	}
}

//nolint:paralleltest // Тест меняет поколение проекции в выделенной БД.
func TestPostgresStudyScopeDoesNotUsePartialGraph(t *testing.T) {
	ctx, pool := t.Context(), studyTestPool(t)
	id := studySeed(t, pool)
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var generation int64
		if err := tx.QueryRow(ctx, `
			UPDATE catalog_projection_state SET active_generation=active_generation+1
			WHERE singleton RETURNING active_generation`).Scan(&generation); err != nil {
			return err
		}
		batch := &pgx.Batch{}
		for i := 1; i <= 65; i++ {
			batch.Queue(`INSERT INTO catalog_projected_topics (generation,topic_id,revision,tombstone,metadata)
				VALUES ($1,$2,1,false,'{"status":"active"}')`, generation, id(300+i))
		}
		batch.Queue(`INSERT INTO catalog_projected_prerequisites (generation,topic_id,prerequisite_id,revision,tombstone)
			VALUES ($1,$2,$3,1,false),($1,$3,$4,1,false)`, generation, id(301), id(302), id(303))
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return err
		}
		scope, ids, err := catalog.ReadStudyScope(ctx, tx, []string{id(301)})
		if err != nil || len(ids) != 3 || !reflect.DeepEqual(scope[id(302)].Prerequisites, []string{id(303)}) {
			return fmt.Errorf("read transitive scope: %v %w", ids, err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE catalog_projected_topics SET metadata='{"status":"archived"}' WHERE generation=$1 AND topic_id=$2`,
			generation, id(303)); err != nil {
			return err
		}
		if _, _, err := catalog.ReadStudyScope(ctx, tx, []string{id(301)}); !errors.Is(err, catalog.ErrNotFound) {
			return fmt.Errorf("accepted inactive prerequisite: %w", err)
		}
		batch = &pgx.Batch{}
		for i := 4; i <= 65; i++ {
			batch.Queue(`INSERT INTO catalog_projected_prerequisites (generation,topic_id,prerequisite_id,revision,tombstone)
				VALUES ($1,$2,$3,1,false)`, generation, id(301), id(300+i))
		}
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return err
		}
		if _, _, err := catalog.ReadStudyScope(ctx, tx, []string{id(301)}); !errors.Is(err, catalog.ErrStudyScopeLimit) {
			return fmt.Errorf("accepted oversized scope: %w", err)
		}

		return errSyntheticRollback
	})
	if !errors.Is(err, errSyntheticRollback) {
		t.Fatal(err)
	}
}

type studySourceFunc func(context.Context, string, string, []string) ([]planner.StudyResource, error)

func (f studySourceFunc) StudyResources(ctx context.Context, actor, goal string, topics []string) ([]planner.StudyResource, error) {
	return f(ctx, actor, goal, topics)
}

//nolint:paralleltest // Тест меняет поколение проекции в выделенной БД.
func TestPostgresGenerationUsesProjectedEvidence(t *testing.T) {
	ctx, pool := t.Context(), studyTestPool(t)
	id := studySeed(t, pool)
	store := NewPostgres(pool)
	actor, goal := id(1), id(3)
	input := Generation{Expected: Expected{GoalVersion: 1, AvailabilityVersion: 1}, RequestID: id(16)}
	source, _, err := store.Prepare(ctx, actor, goal, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, seedErr := pool.Exec(ctx, `
		WITH graph AS (
		    UPDATE catalog_projection_state SET active_generation=active_generation+1 WHERE singleton RETURNING active_generation
		)
		INSERT INTO catalog_projected_topics (generation,topic_id,revision,tombstone,metadata)
		SELECT active_generation,$1,1,false,'{"status":"active"}' FROM graph`, id(4)); seedErr != nil {
		t.Fatal(seedErr)
	}
	fixture := testDraft(t, source, id)
	calls := 0
	owner := studySourceFunc(func(_ context.Context, learner, goalID string, topics []string) ([]planner.StudyResource, error) {
		calls++
		if learner != actor || goalID != goal || !reflect.DeepEqual(topics, source.TargetTopics) {
			return nil, errors.New("incorrect study resource scope")
		}

		return testResources(id), nil
	})
	service := NewService(store, NewProjectedBuilder(pool, owner, fixture.Plan.RankingPolicy, fixture.EvidencePolicy))
	initial, err := service.Generate(ctx, actor, goal, input)
	if err != nil || initial.NextStep == nil || initial.GoalDemonstrated {
		t.Fatal("empty history was treated as verified:", initial, err)
	}
	if _, evidenceErr := pool.Exec(ctx, `INSERT INTO learning_task_outcomes
		(submission_id,learner_id,task_id,task_version_id,topic_ids,verdict,evidence_kind,graded_at,event_id,correction_version)
		SELECT gen_random_uuid(),$1,gen_random_uuid(),gen_random_uuid(),ARRAY[$2::uuid],'accepted','independent',
		       statement_timestamp(),gen_random_uuid(),1 FROM generate_series(1,2)`, actor, id(4)); evidenceErr != nil {
		t.Fatal(evidenceErr)
	}
	replay, err := service.Generate(ctx, actor, goal, input)
	if err != nil || !reflect.DeepEqual(replay, initial) || calls != 1 {
		t.Fatal("retry recalculated after new evidence:", calls, replay, err)
	}
	input.RequestID, input.PlanVersion = id(17), 1
	refreshed, err := service.Generate(ctx, actor, goal, input)
	if err != nil || refreshed.Version != 2 || !refreshed.GoalDemonstrated || refreshed.NextStep != nil ||
		refreshed.EvidenceWatermark == initial.EvidenceWatermark || calls != 2 {
		t.Fatal("new generation did not use the updated evidence:", refreshed, err)
	}
}

var errSyntheticRollback = errors.New("discard test graph")

func BenchmarkCurrentStudyPlan(b *testing.B) {
	store, actor, goal := maxStudyPlanFixture(b)
	b.ReportAllocs()
	for b.Loop() {
		view, err := store.Current(b.Context(), actor, goal)
		if err != nil || view.Version != 1 || findItem(view, planner.MaxLearningItems) == nil {
			b.Fatal("current plan was not loaded:", err)
		}
	}
}

func BenchmarkExplainStudyPlan(b *testing.B) {
	store, actor, goal := maxStudyPlanFixture(b)
	b.ReportAllocs()
	for b.Loop() {
		view, err := store.Explain(b.Context(), actor, goal, 1)
		if err != nil || view.PlanVersion != 1 || len(view.Steps) != planner.MaxLearningItems {
			b.Fatal("plan explanation was not loaded:", err)
		}
	}
}

func maxStudyPlanFixture(b *testing.B) (*Postgres, string, string) {
	b.Helper()
	ctx, pool := b.Context(), studyTestPool(b)
	id := studySeed(b, pool)
	actor, goal := id(1), id(3)
	store := NewPostgres(pool)
	input := Generation{Expected: Expected{GoalVersion: 1, AvailabilityVersion: 1}, RequestID: id(15)}
	source, _, err := store.Prepare(ctx, actor, goal, input)
	if err != nil {
		b.Fatal(err)
	}
	draft := testDraft(b, source, id)
	draft.EvidenceCaptured = time.Now().UTC()
	draft.EvidenceTopics = []mastery.Topic{{ID: id(4), Uncertainty: testNoEvidence}}
	ordinal := 0
	for i := range draft.Plan.Weeks {
		week := &draft.Plan.Weeks[i]
		week.Items, week.PlannedMinutes = nil, 0
		for ordinal < planner.MaxLearningItems && week.PlannedMinutes < week.CapacityMinutes {
			ordinal++
			item := planner.StudyItem{
				Ranked: ranking.Ranked{
					ResourceKey: ranking.ResourceKey{Kind: ranking.Practice, ID: id(400 + ordinal), RevisionID: id(600 + ordinal)},
					TopicID:     id(4), Minutes: 1,
				},
				Stage: planner.StudyPractice,
			}
			if ordinal == planner.MaxLearningItems {
				item.Stage = planner.StudyCheck
			}
			week.Items = append(week.Items, item)
			week.PlannedMinutes++
		}
	}
	if ordinal != planner.MaxLearningItems {
		b.Fatal("benchmark did not fill the maximum plan size")
	}
	if _, err := store.Save(ctx, actor, goal, input, source, draft); err != nil {
		b.Fatal(err)
	}

	return store, actor, goal
}
