package diagnostic

import (
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func BenchmarkDiagnosticAssessments(b *testing.B) {
	dsn := os.Getenv("DIAGNOSTIC_TEST_DSN")
	if dsn == "" {
		b.Skip("DIAGNOSTIC_TEST_DSN is required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		b.Fatal("DIAGNOSTIC_TEST_DSN must point to a dedicated _test database")
	}
	ctx := b.Context()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()
	rows, err := pool.Query(ctx, "SELECT gen_random_uuid()::text FROM generate_series(1,13)")
	if err != nil {
		b.Fatal(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		b.Fatal(err)
	}
	actor := ids[0]
	session := Session{Selection: Selection{Tasks: make([]Candidate, 4)}}
	for i := range session.Tasks {
		session.Tasks[i] = Candidate{TaskID: ids[1+i], VersionID: ids[5+i], TopicID: ids[9+i], Minutes: 4}
	}
	if err := pool.QueryRow(ctx, "SELECT now()-interval '5 minute', now()+interval '15 minute'").
		Scan(&session.StartedAt, &session.ExpiresAt); err != nil {
		b.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO learning_task_outcomes (submission_id, learner_id, task_id, task_version_id, topic_ids,
		                                   verdict, evidence_kind, graded_at, event_id, correction_version)
		SELECT gen_random_uuid(),$1,task,version,ARRAY[topic],'accepted','independent',
		       $5::timestamptz-n*interval '1 minute',gen_random_uuid(),1
		FROM unnest($2::uuid[],$3::uuid[],$4::uuid[]) AS p(task,version,topic)
		CROSS JOIN generate_series(0,12500) AS n`, actor, ids[1:5], ids[5:9], ids[9:13], session.StartedAt); err != nil {
		b.Fatal(err)
	}
	for _, mode := range []struct {
		label string
		batch int
	}{{"separate", 1}, {"batch", 4}} {
		b.Run(mode.label, func(b *testing.B) {
			tx, err := pool.Begin(b.Context())
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = tx.Rollback(b.Context()) }()
			for b.Loop() {
				for first := 0; first < len(session.Tasks); first += mode.batch {
					input := session
					input.Tasks = session.Tasks[first : first+mode.batch]
					got, err := assessments(b.Context(), tx, actor, input)
					if err != nil || len(got) != mode.batch {
						b.Fatal("incorrect assessment lookup:", len(got), err)
					}
				}
			}
		})
	}
}
