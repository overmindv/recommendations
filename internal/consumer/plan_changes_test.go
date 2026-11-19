package consumer

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/content/pkg/api/activity"
	"github.com/overmindv/recommendations/internal/mastery"
)

func TestPostgresPlanChangesCoalesceAcceptedEvidence(t *testing.T) {
	ctx, pool, ns := t.Context(), evidenceDB(t), namespace(t)
	id := func(n int) string { return fixtureID(ns, n) }
	actor, goal, unrelated := id(3), id(70), id(71)
	batch := &pgx.Batch{}
	batch.Queue("INSERT INTO learner_availability (user_id,weekly_minutes,timezone) VALUES ($1,120,'UTC')", actor)
	for _, entry := range []struct{ goal, topic string }{{goal, id(41)}, {unrelated, id(42)}} {
		batch.Queue(`INSERT INTO learner_goals (id,user_id,title,target_topics,weekly_minutes,active,preferences)
			VALUES ($1,$2,'Goal',ARRAY[$3::uuid],60,true,'{}')`, entry.goal, actor, entry.topic)
		batch.Queue(`INSERT INTO study_plan_versions (goal_id,version,goal_version,availability_version,reserved_minutes,target_topics,
		    planner_version,ranking_algorithm,policy_version,ranking_policy,evidence_watermark,evidence_policy,calendar)
		    VALUES ($1,1,1,1,60,ARRAY[$2::uuid],'test','test','test','{"policy_version":"test"}',decode(repeat('01',32),'hex'),'{}','{}')`,
			entry.goal, entry.topic)
		batch.Queue("INSERT INTO study_plan_heads (goal_id,version) VALUES ($1,1)", entry.goal)
	}
	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		t.Fatal(err)
	}
	baselines, err := mastery.NewPostgres(ctx, pool, mastery.Policy{
		Revision: id(75), Algorithm: mastery.AlgorithmVersion,
		GapMaxBPS: 2500, DemonstratedBPS: 7500, MinWeightedTasks: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresWithPlanChanges(pool, time.Second, baselines)
	if err != nil {
		t.Fatal(err)
	}
	origin := time.Now().UnixNano()
	apply := func(events ...taskEnvelope) {
		t.Helper()
		var messages []message
		for _, event := range events {
			origin++
			messages = append(messages, decodeMessage(evidenceRecord(t, TasksTopic, event.AggregateID, origin, event), activity.DefaultLearningTopic))
		}
		if err := store.apply(ctx, messages); err != nil {
			t.Fatal(err)
		}
	}
	assertRevision := func(want int) (time.Time, time.Time) {
		t.Helper()
		var revision, count int
		var first, available time.Time
		if err := pool.QueryRow(ctx, `SELECT revision,first_requested_at,available_at,
		    (SELECT count(*) FROM study_plan_rebuilds WHERE goal_id=$2)
		    FROM study_plan_rebuilds WHERE goal_id=$1`, goal, unrelated).Scan(&revision, &first, &available, &count); err != nil ||
			revision != want || count != 0 {
			t.Fatal("unexpected rebuild scope or revision:", revision, want, count, err)
		}
		return first, available
	}
	first := outcomeFixture(ns, 1, 2, 3, 4)
	apply(first)
	assertRevision(1)
	baseline, err := baselines.Get(ctx, actor, goal)
	if err != nil || baseline.Topics[0].ScoreBPS == nil || *baseline.Topics[0].ScoreBPS != 0 || baseline.Topics[0].SampleCount != 1 {
		t.Fatal("applied grade did not update mastery:", baseline, err)
	}
	repeat := outcomeFixture(ns, 11, 12, 3, 4)
	repeat.OccurredAt = first.OccurredAt.Add(time.Minute)
	repeat.Payload.GradedAt = repeat.OccurredAt
	noise := outcomeFixture(ns, 13, 14, 3, 15)
	noise.Payload.Independent = new(false)
	apply(first, repeat, noise)
	assertRevision(1)

	corrected := first
	corrected.ID, corrected.Version, corrected.Payload.Correction, corrected.Payload.Independent = id(16), 2, 2, new(false)
	another := outcomeFixture(ns, 21, 22, 3, 23)
	apply(corrected, another)
	assertRevision(2)

	if _, err := pool.Exec(ctx, `UPDATE study_plan_rebuilds SET first_requested_at=now()-interval '10 seconds',
		requested_at=now()-interval '9 seconds', available_at=now()-interval '8 seconds' WHERE goal_id=$1`, goal); err != nil {
		t.Fatal(err)
	}
	last := outcomeFixture(ns, 24, 25, 3, 26)
	msg := decodeMessage(evidenceRecord(t, TasksTopic, last.AggregateID, origin+1, last), activity.DefaultLearningTopic)
	if err := store.apply(ctx, []message{msg, {topic: TasksTopic, offset: -1, reject: invalidEvent}}); err == nil {
		t.Fatal("evidence and rebuild schedule did not roll back together")
	}
	assertRevision(2)
	apply(last)
	started, available := assertRevision(3)
	if !available.Equal(started.Add(4 * time.Second)) {
		t.Fatal("a continuous burst extended the deadline past the cap:", started, available)
	}
	if _, err := pool.Exec(ctx, "UPDATE study_plan_versions SET scope_topics=ARRAY[$2::uuid] WHERE goal_id=$1", goal, id(72)); err != nil {
		t.Fatal(err)
	}
	prerequisite := outcomeFixture(ns, 27, 28, 3, 29)
	prerequisite.Payload.Topics = []string{id(72)}
	apply(prerequisite)
	assertRevision(4)
}
