package goals

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Компонентный тест запускается только на выделенной GOALS_TEST_DSN.
func TestPostgresGoalLifecycle(t *testing.T) {
	dsn := os.Getenv("GOALS_TEST_DSN")
	if dsn == "" {
		t.Skip("GOALS_TEST_DSN is not set")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil || (!strings.Contains(config.ConnConfig.Database, "test") && !strings.Contains(config.ConnConfig.Database, "component")) {
		t.Fatal("GOALS_TEST_DSN must point to a dedicated test or component database")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := NewPostgres(pool)
	ctx := context.Background()
	user := "00000000-0000-4000-8000-000000000071"
	other := "00000000-0000-4000-8000-000000000072"
	t.Cleanup(func() {
		_ = store.Delete(context.Background(), user)
		_ = store.Delete(context.Background(), other)
	})
	_ = store.Delete(ctx, user)
	_ = store.Delete(ctx, other)
	input := Input{TopicIDs: []string{"00000000-0000-4000-8000-000000000073"}, Minutes: 60}
	for wantRevision := int64(1); wantRevision <= 2; wantRevision++ {
		if wantRevision == 2 {
			input.SessionCount = 8
		}
		goal, err := store.Save(ctx, user, input)
		if err != nil || goal.Revision != wantRevision || goal.SessionCount != max(1, input.SessionCount) || goal.Minutes != 60 || !reflect.DeepEqual(goal.TopicIDs, input.TopicIDs) || goal.UpdatedAt.IsZero() {
			t.Fatalf("save revision %d: %+v %v", wantRevision, goal, err)
		}
		loaded, err := store.Get(ctx, user)
		if err != nil || loaded.SessionCount != goal.SessionCount {
			t.Fatalf("session count not persisted: %+v %v", loaded, err)
		}
	}
	if goal, err := store.Get(ctx, other); goal.Revision != 0 || !errors.Is(err, ErrNotFound) {
		t.Fatalf("other actor saw goal: %+v %v", goal, err)
	}
	if err := store.Delete(ctx, user); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, user); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted goal remained: %v", err)
	}
}
