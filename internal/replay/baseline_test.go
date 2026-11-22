package replay

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/recommendations/internal/mastery"
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
	if err = store.Activate(ctx, id, nil); err == nil {
		t.Fatal("activation without required policy succeeded")
	}
	unchanged, err := baselines.Get(ctx, actor, goal)
	if err != nil || unchanged.Topics[0].SampleCount != 0 {
		t.Fatal("partial baseline switch", unchanged, err)
	}
	if err = store.Activate(ctx, id, &policy); err != nil {
		t.Fatal(err)
	}
	after, err := baselines.Get(ctx, actor, goal)
	if err != nil || after.Topics[0].SampleCount != 1 || after.Topics[0].ScoreBPS == nil || *after.Topics[0].ScoreBPS != 10000 {
		t.Fatal("baseline not recomputed", after, err)
	}
	if err = store.Activate(ctx, id, &policy); err != nil {
		t.Fatal("activation retry", err)
	}
}
