package mastery

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func evidenceWindowFixture(t testing.TB, size int) (pgx.Tx, string, time.Time) {
	t.Helper()
	dsn := os.Getenv("MASTERY_TEST_DSN")
	if dsn == "" {
		t.Skip("MASTERY_TEST_DSN is required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("MASTERY_TEST_DSN must point to a dedicated _test database")
	}
	ctx := t.Context()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(ctx)
	})
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE learning_task_outcomes
		(LIKE public.learning_task_outcomes INCLUDING ALL) ON COMMIT DROP`); err != nil {
		t.Fatal(err)
	}
	var actor, repeated string
	var until time.Time
	if err := tx.QueryRow(ctx, "SELECT gen_random_uuid()::text,gen_random_uuid()::text,now()").Scan(&actor, &repeated, &until); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO learning_task_outcomes (submission_id,learner_id,task_id,task_version_id,topic_ids,
		verdict,evidence_kind,graded_at,event_id,correction_version)
		SELECT gen_random_uuid(),$1,CASE WHEN n IN (1,$3) THEN $4 ELSE gen_random_uuid() END,
		       gen_random_uuid(),ARRAY[gen_random_uuid()],'accepted','independent',
		       $2::timestamptz - ($3-n) * interval '1 second',gen_random_uuid(),1
		FROM generate_series(1,$3::integer) AS n`, actor, until, size, repeated); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "ANALYZE pg_temp.learning_task_outcomes"); err != nil {
		t.Fatal(err)
	}
	return tx, actor, until
}

func TestPostgresMasteryWindowDoesNotCountOldRepeats(t *testing.T) {
	tx, actor, until := evidenceWindowFixture(t, maxEvidence+2)
	rows, truncated, err := loadEvidence(t.Context(), tx, actor, until)
	if err != nil || !truncated || len(rows) != maxEvidence-1 {
		t.Fatal("expected a truncated window without the newest repeat:", len(rows), truncated, err)
	}
	if rows[0].GradedAt.Equal(until) || !rows[0].GradedAt.Equal(until.Add(-time.Second)) {
		t.Fatal("newer repeat was counted instead of the first attempt")
	}
}

func BenchmarkMasteryEvidenceWindow(b *testing.B) {
	tx, actor, until := evidenceWindowFixture(b, 50_000)
	b.ReportAllocs()
	for b.Loop() {
		rows, truncated, err := loadEvidence(b.Context(), tx, actor, until)
		if err != nil || !truncated || len(rows) != maxEvidence-1 {
			b.Fatal("unexpected evidence window:", len(rows), truncated, err)
		}
	}
}
