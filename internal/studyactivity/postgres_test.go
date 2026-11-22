package studyactivity

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/projection/resources"
)

type availableCatalog struct{}

func (availableCatalog) VerifyVersions(_ context.Context, _ resources.Origin, keys []resources.VersionKey) (map[resources.VersionKey]resources.Candidate, error) {
	return map[resources.VersionKey]resources.Candidate{keys[0]: {Key: keys[0]}}, nil
}

func activityPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("STUDY_ACTIVITY_TEST_DSN")
	if dsn == "" {
		t.Skip("STUDY_ACTIVITY_TEST_DSN is not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("dedicated _test database required")
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func TestPostgresCapture(t *testing.T) {
	t.Parallel()
	pool := activityPool(t)
	ctx := context.Background()
	store, err := NewPostgres(pool, time.Hour, availableCatalog{})
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.NewString()
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM study_activity WHERE actor_id=$1", actor)
		_, _ = pool.Exec(ctx, "DELETE FROM study_activity_rate WHERE actor_id=$1", actor)
	})
	first := inputAt(time.Now().UTC())
	first.Kind = Start
	result, err := store.Capture(ctx, actor, first)
	if err != nil || result.AcceptedSeconds != 0 || result.Trust != Trust {
		t.Fatal(result, err)
	}
	// Повтор не зависит от текущего каталога и возвращает точный первоначальный receipt.
	duplicate, err := store.Capture(ctx, actor, first)
	if err != nil || duplicate != result {
		t.Fatal(duplicate, result, err)
	}
	changed := first
	changed.Visible = false
	if _, err = store.Capture(ctx, actor, changed); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	second := inputAt(time.Now().UTC())
	second.ClientEventID = uuid.NewString()
	if _, err = store.Capture(ctx, actor, second); !errors.Is(err, ErrRate) {
		t.Fatal(err)
	}
	// Управляем только собственным fixture: имитируем прошедшие 20 секунд без sleep.
	previous := time.Now().UTC().Add(-21 * time.Second).Truncate(time.Microsecond)
	first.OccurredAt = previous
	raw, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, "UPDATE study_activity SET payload=$2, received_at=$3 WHERE actor_id=$1", actor, raw, previous); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, "UPDATE study_activity_rate SET received_at=$2 WHERE actor_id=$1", actor, previous); err != nil {
		t.Fatal(err)
	}
	second.OccurredAt = previous.Add(20 * time.Second)
	result, err = store.Capture(ctx, actor, second)
	if err != nil || result.AcceptedSeconds != 20 {
		t.Fatal(result, err)
	}
	if _, err = pool.Exec(ctx, "UPDATE study_activity_rate SET received_at=$2,event_count=600 WHERE actor_id=$1", actor, previous); err != nil {
		t.Fatal(err)
	}
	third := second
	third.ClientEventID = uuid.NewString()
	if _, err = store.Capture(ctx, actor, third); !errors.Is(err, ErrRate) {
		t.Fatal(err)
	}
	// TTL не удаляет живое событие и не сбрасывает счётчик при удалении offline события.
	if _, err = pool.Exec(ctx, "UPDATE study_activity SET expires_at=clock_timestamp()-interval '1 second' WHERE actor_id=$1 AND client_event_id=$2", actor, first.ClientEventID); err != nil {
		t.Fatal(err)
	}
	if err = store.Prune(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM study_activity WHERE actor_id=$1", actor).Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	if _, err = store.Capture(ctx, actor, third); !errors.Is(err, ErrRate) {
		t.Fatal(err)
	}
	old := inputAt(time.Now().Add(-2 * time.Hour))
	if _, err = store.Capture(ctx, actor, old); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestPostgresConcurrentDuplicate(t *testing.T) {
	t.Parallel()
	pool := activityPool(t)
	store, err := NewPostgres(pool, time.Hour, availableCatalog{})
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.NewString()
	ctx := context.Background()
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM study_activity WHERE actor_id=$1", actor)
		_, _ = pool.Exec(ctx, "DELETE FROM study_activity_rate WHERE actor_id=$1", actor)
	})
	in := inputAt(time.Now().UTC().Add(-10 * time.Minute))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			r, captureErr := store.Capture(ctx, actor, in)
			if captureErr != nil || r.AcceptedSeconds != 0 {
				t.Error(r, captureErr)
			}
		})
	}
	wg.Wait()
	var count int
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM study_activity WHERE actor_id=$1", actor).Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
}

func TestPostgresReplayWindow(t *testing.T) {
	t.Parallel()
	pool := activityPool(t)
	ctx := context.Background()
	store, err := NewPostgres(pool, 48*time.Hour, availableCatalog{})
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.NewString()
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM study_activity WHERE actor_id=$1", actor)
		_, _ = pool.Exec(ctx, "DELETE FROM study_activity_rate WHERE actor_id=$1", actor)
	})
	old := inputAt(time.Now().UTC().Add(-25 * time.Hour).Truncate(time.Microsecond))
	raw, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	expires := old.OccurredAt.Add(48 * time.Hour)
	_, err = pool.Exec(ctx, `INSERT INTO study_activity(actor_id,client_event_id,payload_hash,payload,accepted_seconds,received_at,expires_at)
		VALUES($1,$2,$3,$4,0,$5,$6)`, actor, old.ClientEventID, hash[:], raw, old.OccurredAt, expires)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := store.Capture(ctx, actor, old)
	if err != nil || !replayed.ExpiresAt.Equal(expires) {
		t.Fatal("live receipt after 24h", replayed, err)
	}
	// Для допустимого future skew срок совпадает с возрастной границей приёма:
	// после удаления строки этот же occurred_at уже слишком стар для новой записи.
	future := inputAt(time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond))
	future.ClientEventID = uuid.NewString()
	result, err := store.Capture(ctx, actor, future)
	if err != nil || !result.ExpiresAt.Equal(future.OccurredAt.Add(48*time.Hour)) {
		t.Fatal("expiry/replay boundary mismatch", result, err)
	}
}
