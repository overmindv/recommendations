package consumer

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/content/pkg/api/activity"
	"github.com/overmindv/recommendations/internal/mastery"
)

func TestPostgresPlanChangesCoalesceAcceptedEvidence(t *testing.T) {
	t.Parallel()

	ctx, pool, ns := t.Context(), evidenceDB(t), namespace(t)
	id := func(n int) string { return fixtureID(ns, n) }
	actor, goal, unrelated := id(3), id(70), id(71)
	defer func() {
		if _, cleanupErr := pool.Exec(ctx, "DELETE FROM study_plan_rebuilds WHERE goal_id IN ($1,$2)", goal, unrelated); cleanupErr != nil {
			t.Error("remove test rebuilds:", cleanupErr)
		}
	}()
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
		messages := make([]message, 0, len(events))
		for _, event := range events {
			origin++
			messages = append(messages, decodeMessage(evidenceRecord(t, TasksTopic, event.AggregateID, origin, event), activity.DefaultLearningTopic))
		}
		if applyErr := store.apply(ctx, messages); applyErr != nil {
			t.Fatal(applyErr)
		}
	}
	assertRevision := func(want int) (time.Time, time.Time) {
		t.Helper()
		var revision, count int
		var first, available time.Time
		if queryErr := pool.QueryRow(ctx, `SELECT revision,first_requested_at,available_at,
		    (SELECT count(*) FROM study_plan_rebuilds WHERE goal_id=$2)
		    FROM study_plan_rebuilds WHERE goal_id=$1`, goal, unrelated).Scan(&revision, &first, &available, &count); queryErr != nil ||
			revision != want || count != 0 {
			t.Fatal("unexpected rebuild scope or revision:", revision, want, count, queryErr)
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
	readProof := func() []string {
		t.Helper()
		var events []string
		if err := pool.QueryRow(ctx, `SELECT ARRAY(SELECT item->>'event_id'
			FROM jsonb_array_elements(evidence) AS items(item))
			FROM mastery_baseline_evidence WHERE goal_id=$1`, goal).Scan(&events); err != nil {
			t.Fatal(err)
		}

		return events
	}
	if events := readProof(); len(events) != 1 || events[0] != first.ID {
		t.Fatal("unexpected evidence for first grade:", events)
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
	if events := readProof(); len(events) != 2 || events[0] != another.ID || events[1] != repeat.ID {
		t.Fatal("correction left stale evidence:", events)
	}
	readFactRevision := func() string {
		t.Helper()
		var revision string
		if err := pool.QueryRow(ctx, `SELECT xmin::text FROM mastery_observed_evidence
			WHERE goal_id=$1 AND event_id=$2`, goal, another.ID).Scan(&revision); err != nil {
			t.Fatal(err)
		}

		return revision
	}
	factRevision := readFactRevision()

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
	if revision := readFactRevision(); revision != factRevision {
		t.Fatal("unchanged evidence was rewritten:", revision, factRevision)
	}
	if _, err := pool.Exec(ctx, "UPDATE study_plan_rebuilds SET priority=true WHERE goal_id=$1", goal); err != nil {
		t.Fatal(err)
	}
	_, priorityDue := assertRevision(4)
	apply(outcomeFixture(ns, 30, 31, 3, 32))
	_, nextDue := assertRevision(5)
	if !priorityDue.Equal(nextDue) {
		t.Fatal("new evidence delayed a rebuild requested by the learner")
	}
	if _, err := pool.Exec(ctx, "DELETE FROM study_plan_rebuilds WHERE goal_id=$1", goal); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO study_plan_feedback_state (goal_id,paused_at) VALUES ($1,now())", goal); err != nil {
		t.Fatal(err)
	}
	whilePaused := outcomeFixture(ns, 33, 34, 3, 35)
	apply(whilePaused)
	var queued, recorded, used bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM study_plan_rebuilds WHERE goal_id=$1),
		       EXISTS (SELECT 1 FROM learning_task_outcomes WHERE submission_id=$2),
		       EXISTS (SELECT 1 FROM mastery_baselines WHERE goal_id=$1 AND $3::uuid=ANY(evidence_refs))`,
		goal, whilePaused.Payload.SubmissionID, whilePaused.ID).Scan(&queued, &recorded, &used); err != nil || queued || !recorded || !used {
		t.Fatal("paused goal dropped evidence or started a rebuild:", queued, recorded, used, err)
	}
}

func BenchmarkPlanChangesBatch(b *testing.B) {
	pool := evidenceDB(b)
	for _, tc := range []struct {
		name    string
		enabled bool
	}{{"without_adaptation", false}, {"with_adaptation", true}} {
		b.Run(tc.name, func(b *testing.B) {
			ns := namespace(b)
			ctx := b.Context()
			topics := fixtureID(ns, 41)
			actors, goals := make([]string, batchLimit), make([]string, batchLimit)
			for i := range actors {
				actors[i], goals[i] = fixtureID(ns, 100+i), fixtureID(ns, 200+i)
			}
			var seed pgx.Batch
			seed.Queue(`INSERT INTO learner_availability (user_id,weekly_minutes,timezone)
				SELECT actor,60,'UTC' FROM unnest($1::uuid[]) AS actors(actor)`, actors)
			seed.Queue(`INSERT INTO learner_goals (id,user_id,title,target_topics,weekly_minutes,active,preferences)
				SELECT goal,actor,'Goal',ARRAY[$3::uuid],60,true,'{}'
				FROM unnest($1::uuid[],$2::uuid[]) AS learners(actor,goal)`, actors, goals, topics)
			seed.Queue(`INSERT INTO study_plan_versions (goal_id,version,goal_version,availability_version,reserved_minutes,target_topics,
				planner_version,ranking_algorithm,policy_version,ranking_policy,evidence_watermark,evidence_policy,calendar)
				SELECT goal,1,1,1,0,ARRAY[$2::uuid],'test','test','test','{"policy_version":"test"}',decode(repeat('01',32),'hex'),'{}','{}'
				FROM unnest($1::uuid[]) AS goals(goal)`, goals, topics)
			seed.Queue(`INSERT INTO study_plan_heads (goal_id,version)
				SELECT goal,1 FROM unnest($1::uuid[]) AS goals(goal)`, goals)
			seed.Queue(`INSERT INTO learning_task_outcomes (submission_id,learner_id,task_id,task_version_id,topic_ids,
				verdict,evidence_kind,graded_at,event_id,correction_version)
				SELECT gen_random_uuid(),actor,gen_random_uuid(),gen_random_uuid(),ARRAY[$2::uuid],
					'accepted','independent',statement_timestamp() - n * interval '1 minute',gen_random_uuid(),1
				FROM unnest($1::uuid[]) AS learners(actor) CROSS JOIN generate_series(1,1563) AS history(n)`, actors, topics)
			if err := pool.SendBatch(ctx, &seed).Close(); err != nil {
				b.Fatal(err)
			}
			if _, err := pool.Exec(ctx, "ANALYZE learning_task_outcomes, learner_goals, study_plan_versions"); err != nil {
				b.Fatal(err)
			}

			store := NewPostgres(pool)
			if tc.enabled {
				policy := mastery.Policy{Revision: fixtureID(ns, 75), Algorithm: mastery.AlgorithmVersion,
					GapMaxBPS: 2500, DemonstratedBPS: 7500, MinWeightedTasks: 1}
				baselines, err := mastery.NewPostgres(ctx, pool, policy)
				if err != nil {
					b.Fatal(err)
				}
				adaptive, err := NewPostgresWithPlanChanges(pool, time.Second, baselines)
				if err != nil {
					b.Fatal(err)
				}
				store = adaptive
			}
			offset := time.Now().UnixNano()
			graded := time.Now().UTC()
			seq := 1000
			apply := func() {
				messages := make([]message, batchLimit)
				for i := range messages {
					event := outcomeFixture(ns, seq, seq+1, 100+i, seq+2)
					event.OccurredAt, event.Payload.GradedAt = graded, graded
					messages[i] = decodeMessage(evidenceRecord(b, TasksTopic, event.AggregateID, offset, event), activity.DefaultLearningTopic)
					seq += 3
					offset++
				}
				if err := store.apply(ctx, messages); err != nil {
					b.Fatal(err)
				}
			}
			apply()
			b.ReportAllocs()
			for b.Loop() {
				apply()
			}
			b.ReportMetric(float64(b.N*batchLimit)/b.Elapsed().Seconds(), "events/s")
		})
	}
}
