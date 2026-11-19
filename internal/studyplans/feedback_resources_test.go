package studyplans

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

func BenchmarkResourceFeedback(b *testing.B) {
	ctx, pool := b.Context(), studyTestPool(b)
	id := studySeed(b, pool)
	goal, topic := id(3), id(4)
	var now time.Time
	if err := pool.QueryRow(ctx, "SELECT statement_timestamp()").Scan(&now); err != nil {
		b.Fatal(err)
	}
	const history, candidates = 10_000, 640
	rows := make([][]any, history)
	for i := range rows {
		expiry := now.Add(-time.Hour)
		if i%8 == 0 {
			expiry = now.Add(time.Hour)
		}
		rows[i] = []any{goal, "practice", id(1000 + i), expiry}
	}
	if _, err := pool.CopyFrom(ctx, pgx.Identifier{"study_plan_resource_cooldowns"},
		[]string{"goal_id", "kind", "resource_id", "expires_at"}, pgx.CopyFromRows(rows)); err != nil {
		b.Fatal(err)
	}
	var input planner.LearningRequest
	for i := range candidates {
		input.Resources = append(input.Resources, planner.StudyResource{Candidate: ranking.Candidate{
			ResourceKey: ranking.ResourceKey{Kind: ranking.Practice, ID: id(1000 + i), RevisionID: id(20000 + i)}, Available: true,
		}})
	}
	b.ReportAllocs()
	for b.Loop() {
		draft := input
		if err := applyResourceFeedback(ctx, pool, goal, []string{topic}, &draft, now); err != nil ||
			draft.Resources[0].Available || !draft.Resources[1].Available || !input.Resources[0].Available {
			b.Fatal("invalid cooldown lookup:", err)
		}
	}
}

func TestPostgresResourceFeedbackUsesLatestHintAndExactResource(t *testing.T) {
	ctx, pool := t.Context(), studyTestPool(t)
	store := NewPostgres(pool)
	id, _ := readyFixturePlan(t, pool, store)
	actor, goal, topic := id(1), id(3), id(4)
	cleanupFeedbackQueue(t, pool, goal)
	if _, err := store.CompleteActivity(ctx, actor, goal, 1, 1); err != nil {
		t.Fatal(err)
	}
	for i, action := range []FeedbackAction{FeedbackTooEasy, FeedbackTooHard, FeedbackNotRelevant} {
		if _, err := store.SubmitFeedback(ctx, actor, goal, Feedback{
			RequestID: id(74 + i), PlanVersion: 1, Action: action, Ordinal: new(2),
		}); err != nil {
			t.Fatal(err)
		}
	}
	rows := testResources(id)
	var now time.Time
	if err := pool.QueryRow(ctx, "SELECT statement_timestamp()").Scan(&now); err != nil {
		t.Fatal(err)
	}
	input := planner.LearningRequest{Resources: rows}
	if err := applyResourceFeedback(ctx, pool, goal, []string{topic}, &input, now); err != nil {
		t.Fatal(err)
	}
	if input.DifficultyHints[topic] != -1 || !input.Resources[0].Available || input.Resources[1].Available ||
		!input.Resources[2].Available || !rows[1].Available {
		t.Fatal("feedback was not applied to its topic and resource")
	}
}
