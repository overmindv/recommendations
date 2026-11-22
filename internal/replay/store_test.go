package replay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/mastery"
)

type fixtureSource struct {
	manifest Manifest
	events   []Entry
}

func (f *fixtureSource) Manifest(context.Context, int64, int64) (Manifest, error) {
	return f.manifest, nil
}
func (f *fixtureSource) Page(_ context.Context, m Manifest, after int64) (Page, error) {
	p := Page{Watermark: m.Watermark, PrivacyEpoch: m.PrivacyEpoch, Through: m.Watermark, Events: []Entry{}}
	for _, e := range f.events {
		if e.Position > after {
			p.Events = append(p.Events, e)
			p.More = e.Position < m.Watermark
			p.Through = e.Position

			break
		}
	}

	return p, nil
}
func sourceFor(t *testing.T, raw ...[]byte) *fixtureSource {
	t.Helper()
	f := &fixtureSource{manifest: Manifest{Watermark: int64(len(raw)), PrivacyEpoch: 1, Count: int64(len(raw)), CompleteSince: time.Now().UTC()}}
	hash := sha256.New()
	for i, event := range raw {
		sum, err := Digest(event)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = hash.Write(sum)
		f.events = append(f.events, Entry{Position: int64(i + 1), SHA256: hex.EncodeToString(sum), Event: event})
	}
	f.manifest.SHA256 = hex.EncodeToString(hash.Sum(nil))

	return f
}
func replayDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("REPLAY_TEST_DSN")
	if dsn == "" {
		t.Skip("REPLAY_TEST_DSN required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("dedicated _test database required")
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	return pool
}
func eventFixture(t *testing.T, actor, submission, task, topic string, version int, verdict string) []byte {
	t.Helper()
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	raw, err := json.Marshal(map[string]any{"event_id": uuid.NewString(), "event_type": "learning.outcome.v1", "schema_version": 1, "aggregate_id": submission, "aggregate_version": version, "occurred_at": at, "payload": map[string]any{"submission_id": submission, "learner_id": actor, "task_id": task, "task_version_id": task, "topic_ids": []string{topic}, "verdict": verdict, "graded_at": at, "first_independent_attempt": true, "correction_version": version}})
	if err != nil {
		t.Fatal(err)
	}

	return raw
}

//nolint:paralleltest // Проверяет глобальное переключение модели в отдельной fixture БД.
func TestReplayInterruptedAndShuffledParity(t *testing.T) {
	pool := replayDB(t)
	ctx := t.Context()
	actor, submission, task, topic := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	first := eventFixture(t, actor, submission, task, topic, 1, "wrong_answer")
	corrected := eventFixture(t, actor, submission, task, topic, 2, "accepted")
	other := eventFixture(t, actor, uuid.NewString(), uuid.NewString(), topic, 1, "wrong_answer")
	var previous []byte
	for _, order := range [][][]byte{{first, other, corrected}, {corrected, first, other}} {
		source := sourceFor(t, order...)
		store := NewStore(pool, source)
		id, err := store.Start(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = store.Step(ctx, id); err != nil {
			t.Fatal(err)
		}
		status, err := store.Status(ctx, id)
		if err != nil || status.Through != 1 {
			t.Fatal("cursor", status, err)
		}
		if err = store.Activate(ctx, id, ActivationOptions{}); err == nil {
			t.Fatal("partial activation")
		}
		// Новый процесс продолжает durable cursor; чтение до activation остаётся старым.
		store = NewStore(pool, source)
		for range 2 {
			if err = store.Step(ctx, id); err != nil {
				t.Fatal(err)
			}
		}
		if err = store.Activate(ctx, id, ActivationOptions{}); err != nil {
			t.Fatal(err)
		}
		var raw []byte
		if err = pool.QueryRow(ctx, `SELECT jsonb_agg(to_jsonb(o) ORDER BY submission_id) FROM learning_task_outcomes o WHERE learner_id=$1`, actor).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if previous != nil && string(previous) != string(raw) {
			t.Fatal("shuffled model diverged")
		}
		previous = raw
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		score, err := mastery.ReadPlanningEvidence(ctx, tx, actor, []string{topic}, mastery.Policy{Revision: "replay-test", Algorithm: mastery.AlgorithmVersion, GapMaxBPS: 3000, DemonstratedBPS: 8000, MinWeightedTasks: 1}, time.Now().UTC())
		_ = tx.Rollback(ctx)
		if err != nil || len(score.Topics) != 1 || score.Topics[0].SampleCount != 2 || score.Topics[0].ScoreBPS == nil || *score.Topics[0].ScoreBPS != 5000 {
			t.Fatal("correction counted as attempt", score, err)
		}
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM learning_outcome_history WHERE learner_id=$1`, actor).Scan(&count); err != nil || count != 3 {
		t.Fatal("old correction lost", count, err)
	}
}

//nolint:paralleltest // Проверяет глобальный maintenance lease в отдельной fixture БД.
func TestReplayRejectsChangedEpochHashAndExpiredLease(t *testing.T) {
	pool := replayDB(t)
	ctx := t.Context()
	for _, scenario := range []string{"epoch", "hash", "lease", "live_revision"} {
		source := sourceFor(t, eventFixture(t, uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), 1, "accepted"))
		store := NewStore(pool, source)
		id, err := store.Start(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = store.Step(ctx, id); err != nil {
			t.Fatal(err)
		}
		switch scenario {
		case "epoch":
			source.manifest.PrivacyEpoch++
		case "hash":
			_, err = pool.Exec(ctx, `UPDATE evidence_replay_entries SET digest=decode(repeat('00',32),'hex') WHERE version_id=$1`, id)
		case "lease":
			_, err = pool.Exec(ctx, `UPDATE evidence_replay_clock SET paused_until=now()-interval '1 second' WHERE singleton`)
		case "live_revision":
			_, err = pool.Exec(ctx, `UPDATE evidence_replay_clock SET live_revision=live_revision+1 WHERE singleton`)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = store.Activate(ctx, id, ActivationOptions{}); err == nil {
			t.Fatal("invalid activation", scenario)
		}
		if err = store.Cancel(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
}
