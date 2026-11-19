package consumer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/content/pkg/api/activity"
	"github.com/overmindv/recommendations/internal/completion"
	completionqueue "github.com/overmindv/recommendations/internal/completion/queue"
	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
	"github.com/overmindv/recommendations/internal/studyplans"
	"github.com/overmindv/recommendations/pkg/api/goalcompletion"
)

func TestPostgresGoalCompletionRollsBackAndReopensOnCorrection(t *testing.T) {
	ctx, pool, ns := t.Context(), evidenceDB(t), namespace(t)
	id := func(n int) string { return fixtureID(ns, n) }
	actor, goal, topic := id(3), id(70), id(41)
	store := NewPostgres(pool).WithCompletionChecks()
	worker := completion.NewWorker(pool, slog.New(slog.DiscardHandler))
	other := outcomeFixture(ns, 1, 2, 3, 11)
	check := outcomeFixture(ns, 4, 5, 3, 12)
	replacement := outcomeFixture(ns, 7, 8, 3, 13)
	older := outcomeFixture(ns, 21, 22, 3, 12)
	older.Payload.Independent = new(false)
	check.Payload.Verdict, other.Payload.Verdict, older.Payload.Verdict = "accepted", "accepted", "accepted"
	if _, err := pool.Exec(ctx, "INSERT INTO learner_availability (user_id,weekly_minutes,timezone) VALUES ($1,90,'UTC')", actor); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO learner_goals (id,user_id,title,target_topics,weekly_minutes,active,preferences)
		VALUES ($1,$2,'Test goal',ARRAY[$3::uuid],60,true,'{}')`, goal, actor, topic); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanup, "DELETE FROM goal_completion_checks WHERE learner_id=$1", actor)
	})
	planStore := studyplans.NewPostgres(pool).WithCompletionChecks()
	saveVerificationPlan(t, pool, planStore, actor, goal, topic, id(71), check.Payload, 0)
	step := func() {
		t.Helper()
		if found, err := worker.Step(ctx); err != nil || !found {
			t.Fatal("could not evaluate completion:", found, err)
		}
	}
	step()
	sequence := time.Now().UnixNano()
	send := func(wire taskEnvelope, setClock bool) taskEnvelope {
		t.Helper()
		if setClock {
			if err := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&wire.OccurredAt); err != nil {
				t.Fatal(err)
			}
			wire.OccurredAt = wire.OccurredAt.UTC()
			wire.Payload.GradedAt = wire.OccurredAt
		}
		sequence++
		msg := decodeMessage(evidenceRecord(t, TasksTopic, wire.AggregateID, sequence, wire), activity.DefaultLearningTopic)
		if msg.outcome == nil {
			t.Fatal("test event did not pass the input contract")
		}
		if err := store.apply(ctx, []message{msg}); err != nil {
			t.Fatal(err)
		}
		return wire
	}
	events := func() []goalcompletion.Event {
		t.Helper()
		rows, err := pool.Query(ctx, `
			SELECT payload FROM goal_completion_outbox WHERE goal_id=$1 ORDER BY completion_version`, goal)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []goalcompletion.Event
		for rows.Next() {
			var raw []byte
			var event goalcompletion.Event
			if err := rows.Scan(&raw); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &event); err != nil {
				t.Fatal(err)
			}
			if err := event.Validate(); err != nil {
				t.Fatal(err)
			}
			out = append(out, event)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	send(older, true)
	send(other, true)
	step()
	if got := events(); len(got) != 0 {
		t.Fatal("independent samples on another task replaced the assigned check:", len(got))
	}
	check = send(check, true)
	var group sync.WaitGroup
	var claimed [2]bool
	var failures [2]error
	for i := range claimed {
		group.Go(func() { claimed[i], failures[i] = worker.Step(ctx) })
	}
	group.Wait()
	if claimed[0] == claimed[1] || failures[0] != nil || failures[1] != nil {
		t.Fatal("concurrent evaluators did not fence the learner:", claimed, failures)
	}
	first := events()
	if len(first) != 1 || first[0].Type != goalcompletion.Completed || first[0].AggregateVersion != 1 ||
		first[0].Payload.PlanVersion != 1 || first[0].Payload.LearnerID != actor {
		t.Fatal("verified goal did not produce one completion event:", first)
	}
	send(check, false)
	if found, err := worker.Step(ctx); err != nil || found {
		t.Fatal("duplicate delivery scheduled another completion:", found, err)
	}
	saveVerificationPlan(t, pool, planStore, actor, goal, topic, id(72), replacement.Payload, 1)
	step()
	if got := events(); len(got) != 1 {
		t.Fatal("a new plan erased the previously demonstrated goal:", len(got))
	}

	blocker := id(73)
	if _, err := pool.Exec(ctx, `
		INSERT INTO goal_completion_outbox (event_id,goal_id,completion_version,payload)
		VALUES ($1,$2,2,$3)`, blocker, goal, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	correction := check
	correction.ID, correction.Version, correction.Payload.Correction, correction.Payload.Verdict = id(74), 2, 2, "wrong_answer"
	correction.Payload.Independent = new(false)
	send(correction, false)
	if found, err := worker.Step(ctx); !found || err == nil {
		t.Fatal("outbox uniqueness failure did not fail the transaction:", found, err)
	}
	var version int
	var verified, queued bool
	if err := pool.QueryRow(ctx, `
		SELECT completion_version,verified,EXISTS (SELECT 1 FROM goal_completion_checks WHERE learner_id=$2)
		FROM goal_completion_state WHERE goal_id=$1`, goal, actor).Scan(&version, &verified, &queued); err != nil ||
		version != 1 || !verified || !queued {
		t.Fatal("failed outbox write changed the achievement or lost the check:", version, verified, queued, err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM goal_completion_outbox WHERE event_id=$1 AND goal_id=$2", blocker, goal); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE goal_completion_checks SET ready_at=statement_timestamp() WHERE learner_id=$1", actor); err != nil {
		t.Fatal(err)
	}
	step()
	replacement.Payload.Verdict = "accepted"
	send(replacement, true)
	step()
	goalStore := learnergoals.NewPostgres(pool).WithCompletionChecks()
	if err := goalStore.Delete(ctx, actor, goal, learnergoals.Write{RequestID: id(75), ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	step()
	all := events()
	want := []goalcompletion.Type{goalcompletion.Completed, goalcompletion.Reopened, goalcompletion.Completed, goalcompletion.Reopened}
	if len(all) != len(want) {
		t.Fatal("missing state transitions:", len(all))
	}
	for i, event := range all {
		if event.Type != want[i] || event.AggregateVersion != int64(i+1) ||
			event.Payload.BusinessAwardKey != first[0].Payload.BusinessAwardKey {
			t.Fatal("transition did not preserve award identity or order:", event)
		}
	}
}

func BenchmarkPostgresGoalCompletion(b *testing.B) {
	ctx, pool, ns := b.Context(), evidenceDB(b), namespace(b)
	actor, topic := fixtureID(ns, 3), fixtureID(ns, 41)
	if _, err := pool.Exec(ctx, "INSERT INTO learner_availability (user_id,weekly_minutes,timezone) VALUES ($1,480,'UTC')", actor); err != nil {
		b.Fatal(err)
	}
	plans := studyplans.NewPostgres(pool)
	for i := range learnergoals.MaxGoals {
		goal := fixtureID(ns, 200+i)
		if _, err := pool.Exec(ctx, `
			INSERT INTO learner_goals (id,user_id,title,target_topics,weekly_minutes,active,preferences)
			VALUES ($1,$2,'Goal',ARRAY[$3::uuid],10,true,'{}')`, goal, actor, topic); err != nil {
			b.Fatal(err)
		}
		wire := outcomeFixture(ns, 1000+i, 2000+i, 3, 3000+i)
		saveVerificationPlan(b, pool, plans, actor, goal, topic, fixtureID(ns, 300+i), wire.Payload, 0)
		if _, err := pool.Exec(ctx, `
			INSERT INTO learning_task_outcomes
			    (submission_id,learner_id,task_id,task_version_id,topic_ids,verdict,evidence_kind,graded_at,event_id,correction_version)
			VALUES ($1,$2,$3,$4,ARRAY[$5::uuid],'accepted','independent',clock_timestamp(),$6,1)`,
			wire.Payload.SubmissionID, actor, wire.Payload.TaskID, wire.Payload.VersionID, topic, wire.ID); err != nil {
			b.Fatal(err)
		}
	}
	worker := completion.NewWorker(pool, slog.New(slog.DiscardHandler))
	check := func() {
		b.Helper()
		if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			return completionqueue.Request(ctx, tx, []string{actor})
		}); err != nil {
			b.Fatal(err)
		}
		if found, err := worker.Step(ctx); !found || err != nil {
			b.Fatal("completion check failed:", found, err)
		}
	}
	check()
	var verified int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM goal_completion_state WHERE learner_id=$1 AND verified", actor).
		Scan(&verified); err != nil || verified != learnergoals.MaxGoals {
		b.Fatal("benchmark did not reach the goal limit:", verified, err)
	}
	b.ReportAllocs()
	for b.Loop() {
		check()
	}
}

func saveVerificationPlan(t testing.TB, pool *pgxpool.Pool, store *studyplans.Postgres, actor, goal, topic, request string, check taskResult, previous int64) {
	t.Helper()
	ctx := t.Context()
	input := studyplans.Generation{
		Expected: studyplans.Expected{GoalVersion: 1, AvailabilityVersion: 1, PlanVersion: previous}, RequestID: request,
	}
	source, _, err := store.Prepare(ctx, actor, goal, input)
	if err != nil {
		t.Fatal(err)
	}
	var now time.Time
	if err := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		t.Fatal(err)
	}
	cal, err := planner.StudyWeeks(now.UTC(), source.Budget)
	if err != nil {
		t.Fatal(err)
	}
	plan := planner.LearningPlan{
		Version: planner.LearningPlanVersion, RankingAlgorithm: ranking.AlgorithmVersion,
		RankingPolicy: ranking.Policy{
			Version: "test-goal-ranking", TargetBPS: 8000, PrerequisiteMinBPS: 7000, UnknownGapBPS: 4000,
			MediumDifficultyBPS: 5000, HardDifficultyBPS: 9000, Weights: ranking.Factors{Gap: 1},
		},
		Timezone: cal.Timezone, WeeklyMinutes: cal.WeeklyMinutes, HorizonLimited: cal.HorizonLimited,
	}
	for _, week := range cal.Weeks {
		if week.CapacityMinutes >= 5 {
			plan.Weeks = []planner.LearningWeek{{StudyWeek: week, PlannedMinutes: 5, Items: []planner.StudyItem{{
				Ranked: ranking.Ranked{ResourceKey: ranking.ResourceKey{Kind: ranking.Practice, ID: check.TaskID, RevisionID: check.VersionID},
					TopicID: topic, Minutes: 5}, Stage: planner.StudyCheck,
			}}}}
			break
		}
	}
	if len(plan.Weeks) != 1 {
		t.Fatal("no week for independent assessment")
	}
	draft := studyplans.Draft{
		Plan: plan, EvidenceWatermark: sha256.Sum256([]byte("goal completion test")),
		EvidencePolicy: mastery.Policy{Revision: "test-goal-evidence", Algorithm: mastery.AlgorithmVersion, GapMaxBPS: 2500,
			DemonstratedBPS: 8000, MinWeightedTasks: 1},
	}
	if _, err := store.Save(ctx, actor, goal, input, source, draft); err != nil {
		t.Fatal(err)
	}
}
