package savedplans

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Требует отдельной БД с применёнными миграциями; Docker автоматически не запускается.
func TestPostgresPlanLifecycle(t *testing.T) {
	dsn := os.Getenv("SAVED_PLANS_TEST_DSN")
	if dsn == "" {
		t.Skip("SAVED_PLANS_TEST_DSN is not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || (!strings.Contains(cfg.ConnConfig.Database, "test") && !strings.Contains(cfg.ConnConfig.Database, "component")) {
		t.Fatal("dedicated test database required")
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx := context.Background()
	store := &Postgres{Pool: pool}
	owner := "00000000-0000-4000-8000-000000000081"
	other := "00000000-0000-4000-8000-000000000082"
	id := "00000000-0000-4000-8000-000000000083"
	copyID := "00000000-0000-4000-8000-000000000084"
	defer func() {
		_, _ = pool.Exec(ctx, "DELETE FROM saved_learning_plans WHERE id=ANY($1::uuid[])", []string{id, copyID})
	}()
	p, err := store.Create(ctx, owner, Input{ID: id, Document: sample()})
	if err != nil || p.Public || !p.IsOwner || p.Document.Materials[0].Status != "TODO" {
		t.Fatal(p, err)
	}
	if _, err = store.Get(ctx, id, other); !errors.Is(err, ErrNotFound) {
		t.Fatal("private read allowed", err)
	}
	if _, err = store.Change(ctx, id, other, "visibility", Input{Revision: 1, Public: true}); !errors.Is(err, ErrNotFound) {
		t.Fatal("cross-owner write")
	}
	p, err = store.Change(ctx, id, owner, "material", Input{Revision: p.Revision, MaterialID: "python", Status: "DONE"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Change(ctx, id, owner, "material", Input{Revision: 1, MaterialID: "python", Status: "TODO"}); !errors.Is(err, ErrConflict) {
		t.Fatal("stale write accepted")
	}
	p, err = store.Change(ctx, id, owner, "visibility", Input{Revision: p.Revision, Public: true})
	if err != nil {
		t.Fatal(err)
	}
	public, err := store.Get(ctx, id, "")
	if err != nil || public.IsOwner || public.Document.Materials[0].Status != "TODO" {
		t.Fatal("public progress leaked")
	}
	clone, err := store.Clone(ctx, id, other, copyID)
	if err != nil || clone.Public || clone.Document.Materials[0].Status != "TODO" {
		t.Fatal(clone, err)
	}
	p, err = store.Change(ctx, id, owner, "visibility", Input{Revision: p.Revision, Public: false})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Get(ctx, id, other); !errors.Is(err, ErrNotFound) {
		t.Fatal("revoked read")
	}
	if _, err = store.Clone(ctx, id, other, "00000000-0000-4000-8000-000000000085"); !errors.Is(err, ErrNotFound) {
		t.Fatal("revoked clone")
	}
	if _, err = store.Get(ctx, copyID, other); err != nil {
		t.Fatal("copy depends on source")
	}
}
