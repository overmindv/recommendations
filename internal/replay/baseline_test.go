package replay

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/completion"
	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/internal/ranking"
	"github.com/overmindv/recommendations/pkg/api/goalcompletion"
)

//nolint:paralleltest // Проверяет атомарный baseline refresh в выделенной БД.
func TestReplayRefreshesAffectedBaseline(t *testing.T) {
	pool := replayDB(t)
	ctx := t.Context()
	actor, goal, submission, task, topic := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO learner_availability(user_id,weekly_minutes,timezone)VALUES($1,120,'UTC')`, actor); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO learner_goals(id,user_id,title,target_topics,weekly_minutes,active,preferences)VALUES($1,$2,'Replay goal',$3,120,true,'{}')`, goal, actor, []string{topic}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanup, `DELETE FROM learner_goals WHERE id=$1`, goal)
		_, _ = pool.Exec(cleanup, `DELETE FROM learner_availability WHERE user_id=$1`, actor)
	})
	policy := mastery.Policy{Revision: "replay-refresh-test", Algorithm: mastery.AlgorithmVersion, GapMaxBPS: 3000, DemonstratedBPS: 8000, MinWeightedTasks: 1}
	baselines, err := mastery.NewPostgres(ctx, pool, policy)
	if err != nil {
		t.Fatal(err)
	}
	before, err := baselines.Refresh(ctx, actor, goal, 1)
	if err != nil || before.Topics[0].SampleCount != 0 {
		t.Fatal(before, err)
	}
	first := eventFixture(t, actor, submission, task, topic, 1, "wrong_answer")
	corrected := eventFixture(t, actor, submission, task, topic, 2, "accepted")
	source := sourceFor(t, first, corrected)
	store := NewStore(pool, source)
	id, err := store.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = store.Step(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.Activate(ctx, id, ActivationOptions{}); err == nil {
		t.Fatal("activation without required policy succeeded")
	}
	unchanged, err := baselines.Get(ctx, actor, goal)
	if err != nil || unchanged.Topics[0].SampleCount != 0 {
		t.Fatal("partial baseline switch", unchanged, err)
	}
	if err = store.Activate(ctx, id, ActivationOptions{Mastery: &policy}); err != nil {
		t.Fatal(err)
	}
	after, err := baselines.Get(ctx, actor, goal)
	if err != nil || after.Topics[0].SampleCount != 1 || after.Topics[0].ScoreBPS == nil || *after.Topics[0].ScoreBPS != 10000 {
		t.Fatal("baseline not recomputed", after, err)
	}
	if err = store.Activate(ctx, id, ActivationOptions{Mastery: &policy}); err != nil {
		t.Fatal("activation retry", err)
	}
}

//nolint:paralleltest // Восстановление меняет общую проекцию, а воркер читает общую очередь в тестовой БД.
func TestReplayReopensVerifiedGoalWithoutSchedulingPausedPlan(t *testing.T) {
	ctx, pool := t.Context(), replayDB(t)
	actor, goal, submission, task, topic := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var batch pgx.Batch
		batch.Queue("DELETE FROM goal_completion_checks WHERE learner_id=$1", actor)
		batch.Queue("DELETE FROM goal_completion_outbox WHERE goal_id=$1", goal)
		batch.Queue("DELETE FROM goal_completion_state WHERE goal_id=$1", goal)
		batch.Queue("DELETE FROM learner_goals WHERE id=$1", goal)
		batch.Queue("DELETE FROM learner_availability WHERE user_id=$1", actor)
		if err := pool.SendBatch(cleanup, &batch).Close(); err != nil {
			t.Error("remove replay fixtures:", err)
		}
	})
	rank := ranking.Policy{Version: "replay-goal", TargetBPS: 8000, PrerequisiteMinBPS: 7000,
		MediumDifficultyBPS: 5000, HardDifficultyBPS: 9000, Weights: ranking.Factors{Gap: 1}}
	policy := mastery.Policy{Revision: "replay-goal", Algorithm: mastery.AlgorithmVersion, GapMaxBPS: 3000,
		DemonstratedBPS: 8000, MinWeightedTasks: 1}
	var seed pgx.Batch
	seed.Queue("INSERT INTO learner_availability(user_id,weekly_minutes,timezone) VALUES($1,120,'UTC')", actor)
	seed.Queue(`INSERT INTO learner_goals (id,user_id,title,target_topics,weekly_minutes,active,preferences)
		VALUES ($1,$2,'Replay goal',ARRAY[$3::uuid],60,true,'{}')`, goal, actor, topic)
	seed.Queue(`INSERT INTO study_plan_versions (goal_id,version,goal_version,availability_version,reserved_minutes,target_topics,
		planner_version,ranking_algorithm,policy_version,ranking_policy,evidence_watermark,evidence_policy,calendar,created_at)
		VALUES ($1,1,1,1,0,ARRAY[$2::uuid],'test','test',$3,$4,decode(repeat('01',32),'hex'),$5,'{}','2026-10-01')`,
		goal, topic, rank.Version, rank, policy)
	seed.Queue(`INSERT INTO study_plan_items (goal_id,plan_version,ordinal,week_number,topic_id,resource_id,resource_version_id,
		kind,stage,estimated_minutes,score,score_components,requires_checks)
		VALUES ($1,1,1,1,$2,$3,$3,'practice','independent_check',5,0,'{}','{}')`, goal, topic, task)
	seed.Queue("INSERT INTO study_plan_heads (goal_id,version) VALUES ($1,1)", goal)
	seed.Queue("INSERT INTO study_plan_feedback_state (goal_id,paused_at) VALUES ($1,now())", goal)
	if err := pool.SendBatch(ctx, &seed).Close(); err != nil {
		t.Fatal(err)
	}
	activate := func(options ActivationOptions, events ...[]byte) {
		t.Helper()
		store := NewStore(pool, sourceFor(t, events...))
		id, err := store.Start(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for range events {
			if err = store.Step(ctx, id); err != nil {
				t.Fatal(err)
			}
		}
		if err = store.Activate(ctx, id, options); err != nil {
			t.Fatal(err)
		}
	}
	worker := completion.NewWorker(pool, slog.New(slog.DiscardHandler))
	assertEvent := func(version int, want goalcompletion.Type) {
		t.Helper()
		if found, err := worker.Step(ctx); err != nil || !found {
			t.Fatal("completion check was not processed:", found, err)
		}
		var raw []byte
		if err := pool.QueryRow(ctx, "SELECT payload FROM goal_completion_outbox WHERE goal_id=$1 AND completion_version=$2",
			goal, version).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var event goalcompletion.Event
		if err := json.Unmarshal(raw, &event); err != nil || event.Type != want || event.Validate() != nil {
			t.Fatal("wrong completion event:", event.Type, err)
		}
	}
	first := eventFixture(t, actor, submission, task, topic, 1, "accepted")
	activate(ActivationOptions{CompletionChecks: true}, first)
	assertEvent(1, goalcompletion.Completed)
	correction := eventFixture(t, actor, submission, task, topic, 2, "wrong_answer")
	activate(ActivationOptions{}, first, correction)
	assertEvent(2, goalcompletion.Reopened)
	var queued bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM study_plan_rebuilds WHERE goal_id=$1)", goal).Scan(&queued); err != nil || queued {
		t.Fatal("paused plan was scheduled:", queued, err)
	}
}
