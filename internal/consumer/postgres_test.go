package consumer

import (
	"crypto/rand"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/content/pkg/api/activity"
)

func evidenceDB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("LEARNING_EVIDENCE_TEST_DSN")
	if dsn == "" {
		t.Skip("LEARNING_EVIDENCE_TEST_DSN is required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("LEARNING_EVIDENCE_TEST_DSN must point to a dedicated _test database")
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func namespace(t testing.TB) string {
	t.Helper()
	var data [8]byte
	if _, err := rand.Read(data[:]); err != nil {
		t.Fatal(err)
	}
	data[6] = data[6]&0x3f | 0x80

	return fmt.Sprintf("%x-%x-4000-%x", data[:4], data[4:6], data[6:])
}

func TestPostgresEvidenceRollback(t *testing.T) {
	t.Parallel()

	pool := evidenceDB(t)
	store := NewPostgres(pool)
	ctx := t.Context()
	wire := outcomeFixture(namespace(t), 1, 2, 3, 4)
	msg := decodeMessage(evidenceRecord(t, TasksTopic, wire.AggregateID, 1, wire), activity.DefaultLearningTopic)
	if err := store.apply(ctx, []message{msg, {topic: TasksTopic, offset: -1, reject: invalidEvent}}); err == nil {
		t.Fatal("invalid database batch did not roll back")
	}
	var inbox, outcomes int
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM learning_evidence_inbox WHERE event_id=$1),
		       (SELECT count(*) FROM learning_task_outcomes WHERE submission_id=$2)`, wire.ID, wire.Payload.SubmissionID).Scan(&inbox, &outcomes); err != nil {
		t.Fatal(err)
	}
	if inbox != 0 || outcomes != 0 {
		t.Fatal("batch left partially committed evidence:", inbox, outcomes)
	}
	if err := store.apply(ctx, []message{msg}); err != nil {
		t.Fatal("could not retry rolled-back event:", err)
	}
}

func TestPostgresEvidenceCorrectionAndWeightCap(t *testing.T) {
	t.Parallel()

	pool := evidenceDB(t)
	store := NewPostgres(pool)
	ctx := t.Context()
	ns := namespace(t)
	start := time.Now().UnixNano()
	id := func(n int) string { return fixtureID(ns, n) }
	first := outcomeFixture(ns, 1, 2, 3, 4)
	corrected := first
	corrected.ID, corrected.Version, corrected.Payload.Correction = id(5), 2, 2
	sameVersion := corrected
	sameVersion.ID, sameVersion.Payload.Verdict = id(6), verdictAccepted
	wrongOwner := corrected
	wrongOwner.ID, wrongOwner.Version, wrongOwner.Payload.Correction, wrongOwner.Payload.LearnerID = id(7), 3, 3, id(9)
	conflictingID := corrected
	conflictingID.Payload.Verdict = verdictAccepted
	followup := outcomeFixture(ns, 10, 11, 3, 4)
	followup.Payload.Verdict, followup.Payload.Independent = verdictAccepted, new(false)
	anomalous := outcomeFixture(ns, 12, 13, 3, 4)
	anomalous.OccurredAt = first.OccurredAt.Add(time.Minute)
	anomalous.Payload.GradedAt, anomalous.Payload.Verdict = anomalous.OccurredAt, verdictAccepted
	unsupported := outcomeFixture(ns, 14, 15, 3, 4)
	unsupported.Payload.Verdict = verdictInfra
	taskMessage := func(w taskEnvelope, offset int64) message {
		return decodeMessage(evidenceRecord(t, TasksTopic, w.AggregateID, start+offset, w), activity.DefaultLearningTopic)
	}
	msgs := make([]message, 0, 12)
	msgs = append(msgs,
		taskMessage(corrected, 0), taskMessage(corrected, 1), taskMessage(first, 2),
		taskMessage(sameVersion, 3), taskMessage(wrongOwner, 4), taskMessage(conflictingID, 5),
		taskMessage(followup, 6), taskMessage(anomalous, 7), taskMessage(unsupported, 8),
	)
	read := readFixture(ns, 16, 3, 17, 18)
	readAgain := read
	readAgain.EventID, readAgain.OccurredAt = id(19), read.OccurredAt.Add(time.Minute)
	oldRevision := read
	oldRevision.EventID, oldRevision.RevisionID = id(20), id(21)
	for i, w := range []activity.Event[activity.ReadConfirmation]{readAgain, read, oldRevision} {
		msgs = append(msgs, decodeMessage(evidenceRecord(t, activity.DefaultLearningTopic, w.AggregateID, start+int64(i), w), activity.DefaultLearningTopic))
	}
	if err := store.apply(ctx, msgs); err != nil {
		t.Fatal(err)
	}
	var version, count int
	var eventID, verdict string
	if err := pool.QueryRow(ctx, `
		SELECT correction_version,event_id::text,verdict FROM learning_task_outcomes WHERE submission_id=$1`,
		first.Payload.SubmissionID).Scan(&version, &eventID, &verdict); err != nil {
		t.Fatal(err)
	}
	if version != 2 || eventID != corrected.ID || verdict != verdictWrong {
		t.Fatal("projection lost the latest valid correction:", version, eventID, verdict)
	}
	if err := pool.QueryRow(ctx, `
		SELECT sum(evidence_weight),min(submission_id::text) FROM learning_independent_task_evidence WHERE learner_id=$1`,
		id(3)).Scan(&count, &eventID); err != nil || count != 1 || eventID != first.Payload.SubmissionID {
		t.Fatal("retries increased or replaced independent evidence:", count, eventID, err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM learning_task_outcomes WHERE learner_id=$1", id(3)).Scan(&count); err != nil || count != 3 {
		t.Fatal("missing valid history or counted technical result:", count, err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM learning_material_reads WHERE learner_id=$1 AND evidence_kind='self_reported'`,
		id(3)).Scan(&count); err != nil || count != 2 {
		t.Fatal("expected two distinct material revisions:", count, err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT event_id::text FROM learning_material_reads WHERE learner_id=$1 AND material_id=$2 AND revision_id=$3`,
		id(3), read.AggregateID, read.RevisionID).Scan(&eventID); err != nil || eventID != read.EventID {
		t.Fatal("material duplicate replaced the earliest read time:", eventID, err)
	}
	for i, want := range map[int64]string{3: "outcome_conflict", 4: "outcome_conflict", 5: "event_id_conflict", 8: invalidEvent} {
		var reason string
		if err := pool.QueryRow(ctx, `
			SELECT reason FROM learning_evidence_rejections WHERE topic=$1 AND partition=0 AND message_offset=$2`,
			TasksTopic, start+i).Scan(&reason); err != nil || reason != want {
			t.Fatal("unsafe event was not quarantined:", i, reason, err)
		}
	}
}

//nolint:paralleltest // Тест меняет checkpoint проекции в выделенной БД.
func TestPostgresEvidenceKeepsWeightsFromTaskVersion(t *testing.T) {
	pool, ns := evidenceDB(t), namespace(t)
	ctx := t.Context()
	id := func(n int) string { return fixtureID(ns, n) }
	first := outcomeFixture(ns, 1, 2, 3, 4)
	first.Payload.Topics = []string{id(42), id(41)}
	batch := &pgx.Batch{}
	batch.Queue("UPDATE resource_projection_checkpoints SET active_generation=1 WHERE origin='tasks'")
	batch.Queue(`INSERT INTO resource_projection_items (origin,generation,resource_id,version,revision_id,metadata)
		VALUES ('tasks',1,$1,1,$2,'{}')`, id(4), id(40))
	batch.Queue(`INSERT INTO resource_projection_topics (origin,generation,resource_id,topic_id,weight,diagnostic_minutes)
		VALUES ('tasks',1,$1,$2,3,2), ('tasks',1,$1,$3,1,2)`, id(4), id(41), id(42))
	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		t.Fatal(err)
	}
	store := NewPostgres(pool)
	apply := func(w taskEnvelope) {
		t.Helper()
		msg := decodeMessage(evidenceRecord(t, TasksTopic, w.AggregateID, time.Now().UnixNano(), w), activity.DefaultLearningTopic)
		if msg.reject != "" {
			t.Fatal("fixture rejected:", msg.reject)
		}
		if err := store.apply(ctx, []message{msg}); err != nil {
			t.Fatal(err)
		}
	}
	check := func(submission string, want []int) {
		t.Helper()
		var got []int
		if err := pool.QueryRow(ctx, "SELECT topic_weights FROM learning_task_outcomes WHERE submission_id=$1", submission).
			Scan(&got); err != nil || !slices.Equal(got, want) {
			t.Fatal("unexpected topic weights:", got, want, err)
		}
	}
	apply(first)
	check(first.Payload.SubmissionID, []int{3, 1})
	if _, err := pool.Exec(ctx, `UPDATE resource_projection_items SET version=2,revision_id=$2
		WHERE origin='tasks' AND generation=1 AND resource_id=$1`, id(4), id(43)); err != nil {
		t.Fatal(err)
	}
	first.ID, first.Version, first.Payload.Correction, first.Payload.Verdict = id(5), 2, 2, verdictAccepted
	apply(first)
	check(first.Payload.SubmissionID, []int{3, 1})
	next := outcomeFixture(ns, 6, 7, 3, 4)
	next.Payload.Topics = first.Payload.Topics
	apply(next)
	check(next.Payload.SubmissionID, nil)
}

func BenchmarkPostgresEvidenceWrites(b *testing.B) {
	pool := evidenceDB(b)
	store := NewPostgres(pool)
	ctx := b.Context()
	if _, err := pool.Exec(ctx, `
		INSERT INTO learning_task_outcomes (submission_id,learner_id,task_id,task_version_id,topic_ids,verdict,evidence_kind,
		                                   graded_at,event_id,correction_version)
		SELECT gen_random_uuid(),gen_random_uuid(),gen_random_uuid(),gen_random_uuid(),'{}','accepted','independent',
		       now(),gen_random_uuid(),1
		FROM generate_series(1,50000)`); err != nil {
		b.Fatal(err)
	}
	for _, layout := range []struct {
		name string
		size int
	}{{"single", 1}, {"batch", batchLimit}} {
		b.Run(layout.name, func(b *testing.B) {
			ns := namespace(b)
			seq := 0
			for b.Loop() {
				msgs := make([]message, batchLimit)
				for i := range msgs {
					seq++
					wire := outcomeFixture(ns, seq, seq, 1, seq)
					msgs[i] = decodeMessage(evidenceRecord(b, TasksTopic, wire.AggregateID, int64(seq), wire), activity.DefaultLearningTopic)
				}
				for start := 0; start < len(msgs); start += layout.size {
					if err := store.apply(b.Context(), msgs[start:start+layout.size]); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(b.N*batchLimit)/b.Elapsed().Seconds(), "events/s")
		})
	}
}
