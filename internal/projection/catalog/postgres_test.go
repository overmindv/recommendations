package catalog

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func projectionPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("GOALS_TEST_DSN")
	if dsn == "" {
		t.Skip("GOALS_TEST_DSN is not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("GOALS_TEST_DSN must point to a dedicated _test database")
	}
	ctx := t.Context()
	root, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		t.Fatal(err)
	}
	schema := "projection_" + hex.EncodeToString(seed[:])
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := root.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"catalog_projection_state", "catalog_projected_topics",
		"catalog_projected_prerequisites", "catalog_projection_inbox", "catalog_projection_quarantine",
		"catalog_projection_retired"} {
		name := pgx.Identifier{table}.Sanitize()
		if _, err := root.Exec(ctx, "CREATE TABLE "+quoted+"."+name+" (LIKE public."+name+" INCLUDING ALL)"); err != nil {
			t.Fatal(err)
		}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, "INSERT INTO catalog_projection_state DEFAULT VALUES"); err != nil {
		t.Fatal(err)
	}
	return pool
}

func fixtureID(n int64) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", n) }

func topicRow(id, revision int64, title string) Row {
	return Row{Revision: revision, Record: Record{
		Kind: "topic", TopicID: fixtureID(id), Tombstone: title == "",
		Topic: liveTopic(title),
	}}
}

func liveTopic(title string) *Topic {
	if title == "" {
		return nil
	}
	return &Topic{Title: title, Difficulty: "basic", Status: "active"}
}

func edgeRow(from, to, revision int64) Row {
	return Row{Revision: revision, Record: Record{
		Kind: "prerequisite", TopicID: fixtureID(from), PrerequisiteID: fixtureID(to),
	}}
}

func eventRow(row Row) Event {
	return Event{ID: fixtureID(100 + row.Revision), AggregateVersion: 1, Row: row}
}

func snapshotPage(w int64, cursor string, rows ...Row) SnapshotPage {
	return SnapshotPage{Watermark: &w, NextCursor: cursor, Records: rows}
}

func replayPage(w, through int64, more bool, events ...Event) ChangePage {
	return ChangePage{Watermark: &w, Through: &through, More: more, Changes: events}
}

func TestPostgresCurriculumProjection(t *testing.T) {
	ctx := t.Context()
	pool := projectionPool(t)
	store := NewStore(pool)
	a, b, c := topicRow(1, 1, "A"), topicRow(2, 2, "B"), topicRow(3, 3, "C")
	edge := edgeRow(1, 2, 4)
	updatedC := topicRow(3, 5, "C updated")
	minutes := 90
	updatedC.Record.Topic.LearningMetadata = &LearningMetadata{EstimatedMinutes: &minutes, Outcomes: []string{"Compare algorithms"}}
	dropB := topicRow(2, 7, "")
	newEdge := edgeRow(1, 3, 8)
	d := topicRow(4, 10, "D")
	cycle := edgeRow(3, 1, 11)
	hole := edgeRow(1, 5, 13)
	type response struct {
		url  string
		data any
	}
	bad := snapshotPage(11, "", a, dropB, updatedC, d, newEdge, cycle)
	ready := snapshotPage(12, "", a, dropB, updatedC, d, newEdge)
	fixture := []response{
		{url: "/v1/internal/catalog/snapshot?limit=200", data: snapshotPage(4, "second", a, b)},
		{url: "/v1/internal/catalog/snapshot?cursor=second&limit=200&watermark=4", data: snapshotPage(4, "", c, edge)},
		{url: "/v1/internal/catalog/changes?after=4&limit=200", data: replayPage(9, 7, true, eventRow(dropB), eventRow(updatedC), eventRow(updatedC))},
		{url: "/v1/internal/catalog/changes?after=7&limit=200&watermark=9", data: replayPage(9, 9, false, eventRow(newEdge), eventRow(updatedC))},
		{url: "/v1/internal/catalog/changes?after=9&limit=200", data: replayPage(11, 11, false, eventRow(d), eventRow(cycle))},
		{url: "/v1/internal/catalog/snapshot?limit=200", data: bad},
		{url: "/v1/internal/catalog/snapshot?limit=200", data: bad},
		{url: "/v1/internal/catalog/snapshot?limit=200", data: ready},
		{url: "/v1/internal/catalog/changes?after=12&limit=200", data: replayPage(13, 13, false, eventRow(hole))},
	}
	var requests atomic.Int32
	token := strings.Repeat("k", 32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(requests.Add(1)) - 1
		if i >= len(fixture) || r.URL.RequestURI() != fixture[i].url || r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("unexpected catalog request", i, r.URL.RequestURI())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(fixture[i].data); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, token)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	worker := NewWorker(store, client, logger)
	step := func(moved bool, reason string) {
		t.Helper()
		got, err := worker.Step(ctx)
		var rejection *rejected
		if got != moved || (reason == "" && err != nil) ||
			(reason != "" && (!errors.As(err, &rejection) || rejection.reason != reason)) {
			t.Fatal("unexpected projection step", requests.Load(), got, err)
		}
	}
	checkpoint := func() State {
		t.Helper()
		got, err := store.State(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	assertEdges := func(want ...string) {
		t.Helper()
		got, err := store.Prerequisites(ctx, fixtureID(1), "", PageLimit)
		if err != nil || !slices.Equal(got, want) {
			t.Fatal("unexpected prerequisites", got, want, err)
		}
	}
	count := func(query string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, query).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	start := checkpoint()
	step(true, "")
	if _, err := store.Topic(ctx, fixtureID(1)); !errors.Is(err, ErrNotReady) {
		t.Fatal("partial snapshot exposed topics", err)
	}
	if _, err := store.Prerequisites(ctx, fixtureID(1), "", 20); !errors.Is(err, ErrNotReady) {
		t.Fatal("partial snapshot exposed prerequisites", err)
	}
	if _, err := store.ApplySnapshot(ctx, start, fixture[0].data.(SnapshotPage)); !errors.Is(err, ErrConcurrent) {
		t.Fatal("stale writer changed snapshot", err)
	}
	worker = NewWorker(NewStore(pool), client, logger)
	step(true, "")
	assertEdges(fixtureID(2))
	known := checkpoint()
	if known.Through != 4 || known.PendingGeneration != 0 {
		t.Fatal("snapshot did not publish at its watermark", known)
	}
	step(true, "")
	assertEdges()
	worker = NewWorker(NewStore(pool), client, logger)
	step(true, "")
	assertEdges(fixtureID(3))
	found, err := store.Topic(ctx, fixtureID(3))
	if err != nil || found.Title != "C updated" || found.LearningMetadata == nil ||
		found.LearningMetadata.EstimatedMinutes == nil || *found.LearningMetadata.EstimatedMinutes != minutes {
		t.Fatal("topic lost metadata during replay", found, err)
	}
	step(false, "prerequisite_cycle")
	assertEdges(fixtureID(3))
	if _, err := store.Topic(ctx, fixtureID(4)); !errors.Is(err, ErrNotFound) {
		t.Fatal("invalid batch exposed its first event", err)
	}
	if st := checkpoint(); st.Through != 9 || !st.Rebuild || count("SELECT count(*) FROM catalog_projection_inbox") != 3 {
		t.Fatal("failed page committed part of its state", st)
	}

	step(false, "prerequisite_cycle")
	step(false, "")
	if n := count("SELECT count(*) FROM catalog_projection_quarantine"); n != 2 {
		t.Fatal("invalid snapshot retried at the same watermark", n)
	}
	step(true, "")
	if st := checkpoint(); st.Through != 12 || st.Rebuild || st.ActiveGeneration == known.ActiveGeneration {
		t.Fatal("valid replacement did not become active", st)
	}
	if err := store.Prune(ctx); err != nil {
		t.Fatal(err)
	}
	if n := count("SELECT count(*) FROM catalog_projection_retired"); n != 0 {
		t.Fatal("retired snapshot not pruned", n)
	}
	assertEdges(fixtureID(3))
	step(false, "missing_prerequisite_topic")
	assertEdges(fixtureID(3))
	if n := count("SELECT count(*) FROM catalog_projection_inbox"); n != 3 {
		t.Fatal("orphan event committed an inbox row", n)
	}
}

func BenchmarkSnapshotPage200(b *testing.B) {
	ctx := b.Context()
	store := NewStore(projectionPool(b))
	b.ReportAllocs()
	for n := int64(0); b.Loop(); n++ {
		st, err := store.State(ctx)
		if err != nil {
			b.Fatal(err)
		}
		rows := make([]Row, 0, PageLimit)
		for i := range int64(PageLimit) {
			rows = append(rows, topicRow(n*PageLimit+i+1, 0, "Graph topic"))
		}
		if moved, err := store.ApplySnapshot(ctx, st, snapshotPage(0, fmt.Sprint(n), rows...)); err != nil || !moved {
			b.Fatal(moved, err)
		}
	}
}

func TestPostgresConcurrentSnapshot(t *testing.T) {
	store := NewStore(projectionPool(t))
	ctx := t.Context()
	st, err := store.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := store.ApplySnapshot(ctx, st, snapshotPage(1, "continue", topicRow(1, 1, "A")))
			result <- err
		}()
	}
	var applied, stale int
	for range 2 {
		err := <-result
		switch {
		case err == nil:
			applied++
		case errors.Is(err, ErrConcurrent):
			stale++
		default:
			t.Fatal(err)
		}
	}
	got, err := store.State(ctx)
	if err != nil || applied != 1 || stale != 1 || got.PendingRows != 1 {
		t.Fatal("concurrent writers changed one page more than once", applied, stale, got, err)
	}
}

func TestPostgresQuarantinesConflictingEvent(t *testing.T) {
	store := NewStore(projectionPool(t))
	ctx := t.Context()
	st, err := store.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplySnapshot(ctx, st, snapshotPage(1, "", topicRow(1, 1, "Original"))); err != nil {
		t.Fatal(err)
	}
	st, err = store.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	one := eventRow(topicRow(1, 2, "First value"))
	two := eventRow(topicRow(1, 2, "Different value"))
	var rejectedPage *rejected
	_, err = store.ApplyChanges(ctx, st, replayPage(2, 2, false, one, two))
	if !errors.As(err, &rejectedPage) || rejectedPage.reason != "conflicting_event_identity" {
		t.Fatal("event ID reused for a different record", err)
	}
	got, err := store.Topic(ctx, fixtureID(1))
	if err != nil || got.Title != "Original" {
		t.Fatal("conflicting event changed the graph", got, err)
	}
}

func BenchmarkReplayPage200(b *testing.B) {
	ctx := b.Context()
	store := NewStore(projectionPool(b))
	st, err := store.State(ctx)
	if err != nil {
		b.Fatal(err)
	}
	rows := make([]Row, 0, PageLimit)
	for i := range int64(PageLimit) {
		rows = append(rows, topicRow(i+1, 0, "Graph topic"))
	}
	if _, err := store.ApplySnapshot(ctx, st, snapshotPage(0, "", rows...)); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		st, err := store.State(ctx)
		if err != nil {
			b.Fatal(err)
		}
		events := make([]Event, 0, PageLimit)
		for i := range int64(PageLimit) {
			events = append(events, eventRow(topicRow(i+1, st.Through+i+1, "Updated topic")))
		}
		through := st.Through + PageLimit
		if moved, err := store.ApplyChanges(ctx, st, replayPage(through, through, false, events...)); err != nil || !moved {
			b.Fatal(moved, err)
		}
	}
}
