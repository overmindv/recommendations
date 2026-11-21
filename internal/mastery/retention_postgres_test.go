package mastery

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestRetentionReopensExpiredCheck(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000000, 0).UTC()
	const interval = 24 * time.Hour
	anchor := now.Add(-2 * interval)
	dueAt := anchor.Add(interval)
	scores := []Topic{{ID: "a", LastEvidenceAt: &anchor}}
	check := &ReviewCheck{DueAt: dueAt, AssignedAt: dueAt, ExpiresAt: now}
	checks := map[string]*ReviewCheck{"a": check}
	active := reviewTopics(scores, checks, now.Add(-time.Microsecond), interval, true)[0]
	expired := reviewTopics(scores, checks, now, interval, true)[0]
	inactive := reviewTopics(scores, checks, now, interval, false)[0]
	if active.Status != reviewAssigned || expired.Status != reviewDue || expired.DueAt == nil ||
		!expired.DueAt.Equal(dueAt) || inactive.Status != reviewExpired {
		t.Fatal("expiration should reopen only an active goal:", active, expired, inactive)
	}
}

func BenchmarkRetentionRead(b *testing.B) {
	tx, actor, until := evidenceWindowFixture(b, 50_000)
	ctx := b.Context()
	batch := &pgx.Batch{}
	batch.Queue("CREATE TEMP TABLE learner_goals (LIKE public.learner_goals INCLUDING ALL) ON COMMIT DROP")
	batch.Queue("CREATE TEMP TABLE retention_checks (LIKE public.retention_checks INCLUDING ALL) ON COMMIT DROP")
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		b.Fatal(err)
	}
	var goal string
	var topics []string
	if err := tx.QueryRow(ctx, `INSERT INTO learner_goals (user_id,title,target_topics,weekly_minutes,active,preferences)
		SELECT $1,'Goal',array_agg(topic_ids[1]),20,true,'{}'
		FROM (SELECT topic_ids FROM learning_task_outcomes ORDER BY graded_at DESC OFFSET 1 LIMIT 16) AS evidence
		RETURNING id::text,target_topics`, actor).Scan(&goal, &topics); err != nil {
		b.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO retention_checks
		(goal_id,topic_id,goal_version,due_at,assigned_at,expires_at,task_id,task_version_id,estimated_minutes)
		SELECT $1,topic_ids[1],1,graded_at,graded_at,graded_at+interval '7 days',task_id,task_version_id,5
		FROM learning_task_outcomes WHERE topic_ids[1]=ANY($2::uuid[])`, goal, topics); err != nil {
		b.Fatal(err)
	}
	policy := samplePolicy()
	retention := &Retention{baseline: &Postgres{policy: policy}, interval: 24 * time.Hour, now: func() time.Time { return until }}
	b.Run("evidence_and_mastery", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rows, truncated, err := loadEvidence(ctx, tx, actor, until)
			if err != nil {
				b.Fatal(err)
			}
			scored, _ := summarize(policy, topics, rows, truncated)
			if len(scored) != 16 {
				b.Fatal("expected sixteen scored topics")
			}
		}
	})
	b.Run("retention", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			view, err := retention.read(ctx, tx, actor, goal, 1, false)
			if err != nil || len(view.plan.Topics) != 16 || !view.plan.Truncated || view.plan.Topics[0].Status != reviewChecked {
				b.Fatal("could not read sixteen reviews from bounded evidence:", err)
			}
		}
	})
}
