package resources

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	secondEventID = "66666666-6666-4666-8666-666666666666"
	thirdEventID  = "77777777-7777-4777-8777-777777777777"
)

func resourceTestStore(t testing.TB) (context.Context, *Store) {
	t.Helper()
	dsn := os.Getenv("RESOURCE_PROJECTION_TEST_DSN")
	if dsn == "" {
		t.Skip("RESOURCE_PROJECTION_TEST_DSN is required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid resource test connection string")
	}
	if !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("RESOURCE_PROJECTION_TEST_DSN must point to a dedicated _test database")
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return ctx, NewStore(pool)
}

func readProgress(t testing.TB, ctx context.Context, store *Store, origin Origin) checkpoint {
	t.Helper()
	state, err := store.load(ctx, origin)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func testProjectionRecord(id string, version int64, topic string, origin Origin) Record {
	m := &Metadata{RevisionID: revisionID, Title: "Exercise", Topics: []TopicWeight{{ID: topic, Weight: 1}}}
	if origin == Content {
		m.Type, m.Format = "note", "markdown"
	} else {
		m.Type, m.Difficulty = "single_choice", "easy"
	}
	return Record{ID: id, Version: version, Metadata: m}
}

func testProjectionEvent(id string, row Record) Event {
	return Event{ID: id, Record: row, Fingerprint: sha256.Sum256([]byte(id))}
}

func assertProjectedRow(t *testing.T, ctx context.Context, store *Store, state checkpoint, id string, version int64, hasMetadata bool, topics int) {
	t.Helper()
	var gotVersion int64
	var gotMetadata bool
	var gotTopics int
	const query = `
		SELECT version, metadata IS NOT NULL,
		       (SELECT count(*) FROM resource_projection_topics AS t
		        WHERE t.origin = r.origin AND t.generation = r.generation AND t.resource_id = r.resource_id)
		FROM resource_projection_items AS r
		WHERE origin = $1 AND generation = $2 AND resource_id = $3`
	err := store.pool.QueryRow(ctx, query, state.Origin, state.Active, id).Scan(&gotVersion, &gotMetadata, &gotTopics)
	if err != nil || gotVersion != version || gotMetadata != hasMetadata || gotTopics != topics {
		t.Fatal("wrong projection row:", gotVersion, gotMetadata, gotTopics, err)
	}
}

func TestTasksProjectionSwitchesSnapshotAndReplaysAtomically(t *testing.T) {
	ctx, store := resourceTestStore(t)
	initial := readProgress(t, ctx, store, Tasks)
	if err := store.beginSnapshot(ctx, initial, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.beginSnapshot(ctx, initial, nil); !errors.Is(err, errConcurrent) {
		t.Fatal("a second writer reused an old checkpoint:", err)
	}
	state := readProgress(t, ctx, store, Tasks)
	firstRecord := testProjectionRecord(resourceID, 3, topicID, Tasks)
	firstRecord.Metadata.EstimatedMinutes = new(7)
	first := SnapshotPage{Watermark: 5, NextCursor: resourceID, Records: []Record{firstRecord}}
	if err := store.snapshotPage(ctx, state, first); err != nil {
		t.Fatal(err)
	}
	restarted := NewStore(store.pool)
	state = readProgress(t, ctx, restarted, Tasks)
	if state.Active != initial.Active || state.Cursor != resourceID || state.Watermark == nil || *state.Watermark != 5 {
		t.Fatal("partial snapshot was exposed or not resumable:", state)
	}
	next := SnapshotPage{Watermark: 5, Records: []Record{testProjectionRecord(revisionID, 4, otherTopic, Tasks)}}
	moved := next
	moved.Watermark = 6
	if err := restarted.snapshotPage(ctx, state, moved); !errors.Is(err, errInvalidPage) {
		t.Fatal("changed snapshot watermark accepted:", err)
	}
	if err := restarted.snapshotPage(ctx, state, next); err != nil {
		t.Fatal(err)
	}
	state = readProgress(t, ctx, restarted, Tasks)
	if state.Active == initial.Active || state.Pending != 0 || state.Through != 5 {
		t.Fatal("snapshot has not gone live:", state)
	}
	assertProjectedRow(t, ctx, restarted, state, resourceID, 3, true, 1)
	const minutesSQL = `
		SELECT diagnostic_minutes FROM resource_projection_topics WHERE origin='tasks' AND generation=$1 AND resource_id=$2`
	var minutes *int
	if err := store.pool.QueryRow(ctx, minutesSQL, state.Active, resourceID).Scan(&minutes); err != nil || minutes == nil || *minutes != 7 {
		t.Fatal("estimate missing from diagnostic index:", minutes, err)
	}

	version := testProjectionRecord(resourceID, 6, otherTopic, Tasks)
	version.Metadata.EstimatedMinutes = new(21)
	if err := store.applyTasks(ctx, state, ChangePage{Watermark: 7, Through: 6, More: true,
		Events: []Event{testProjectionEvent(sourceEventID, version)}}); err != nil {
		t.Fatal(err)
	}
	state = readProgress(t, ctx, restarted, Tasks)
	if state.ReplayCeiling == nil || *state.ReplayCeiling != 7 || state.Through != 6 {
		t.Fatal("pinned replay position not persisted:", state)
	}
	var newTopic string
	err := store.pool.QueryRow(ctx, `SELECT topic_id::text FROM resource_projection_topics
		WHERE origin = 'tasks' AND generation = $1 AND resource_id = $2`, state.Active, resourceID).Scan(&newTopic)
	if err != nil || newTopic != otherTopic {
		t.Fatal("updated task should replace old topic indexing:", newTopic, err)
	}
	if err := store.pool.QueryRow(ctx, minutesSQL, state.Active, resourceID).Scan(&minutes); err != nil || minutes != nil {
		t.Fatal("long task remained in diagnostic index:", minutes, err)
	}
	version.Version, version.Metadata.Topics = 7, nil
	if err := store.applyTasks(ctx, state, ChangePage{Watermark: 7, Through: 7,
		Events: []Event{testProjectionEvent(secondEventID, version)}}); err != nil {
		t.Fatal(err)
	}
	state = readProgress(t, ctx, store, Tasks)
	assertProjectedRow(t, ctx, store, state, resourceID, 7, false, 0)
}

func TestContentProjectionDeduplicatesAndRejectsConflictingVersions(t *testing.T) {
	ctx, store := resourceTestStore(t)
	state := readProgress(t, ctx, store, Content)
	pos := KafkaPosition{TopicID: "content-topic", Offsets: map[int32]int64{0: 11, 1: 15}}
	if err := store.beginSnapshot(ctx, state, &pos); err != nil {
		t.Fatal(err)
	}
	state = readProgress(t, ctx, store, Content)
	if err := store.snapshotPage(ctx, state, SnapshotPage{Watermark: 20,
		Records: []Record{testProjectionRecord(resourceID, 0, topicID, Content)}}); err != nil {
		t.Fatal(err)
	}
	state = readProgress(t, ctx, store, Content)
	old := testProjectionEvent(sourceEventID, testProjectionRecord(resourceID, 22, topicID, Content))
	remove := testProjectionEvent(secondEventID, Record{ID: resourceID, Version: 23})
	covered := testProjectionEvent(thirdEventID, testProjectionRecord(revisionID, 19, topicID, Content))
	next := KafkaPosition{TopicID: pos.TopicID, Offsets: map[int32]int64{0: 13, 1: 17}}
	if err := store.applyContent(ctx, state, next, []Event{remove, covered, old, old}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	state = readProgress(t, ctx, store, Content)
	if state.Kafka.Offsets[0] != 13 || state.Floor != 20 {
		t.Fatal("content replay offset not persisted:", state)
	}
	assertProjectedRow(t, ctx, store, state, resourceID, 23, false, 0)
	var count int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM resource_projection_inbox
		WHERE origin = 'content' AND generation = $1`, state.Active).Scan(&count); err != nil || count != 2 {
		t.Fatal("duplicate or snapshot-covered event added to inbox:", count, err)
	}
	valid := testProjectionEvent(thirdEventID, testProjectionRecord(revisionID, 24, otherTopic, Content))
	conflict := remove
	conflict.Fingerprint[0] ^= 1
	next = KafkaPosition{TopicID: pos.TopicID, Offsets: map[int32]int64{0: 14, 1: 18}}
	if err := store.applyContent(ctx, state, next, []Event{valid, conflict}, time.Time{}); !errors.Is(err, errEventConflict) {
		t.Fatal("conflicting replay was not fenced:", err)
	}
	got := readProgress(t, ctx, store, Content)
	if got.Revision != state.Revision || got.Kafka.Offsets[0] != 13 {
		t.Fatal("checkpoint advanced on rollback:", got)
	}
	assertProjectedRow(t, ctx, store, got, resourceID, 23, false, 0)
}

func TestPruneKeepsActiveAndPendingGenerations(t *testing.T) {
	ctx, store := resourceTestStore(t)
	rebuild := func() {
		t.Helper()
		if err := store.beginSnapshot(ctx, readProgress(t, ctx, store, Tasks), nil); err != nil {
			t.Fatal(err)
		}
	}
	finish := func() {
		t.Helper()
		state := readProgress(t, ctx, store, Tasks)
		if err := store.snapshotPage(ctx, state, SnapshotPage{Watermark: 1,
			Records: []Record{testProjectionRecord(resourceID, 1, topicID, Tasks)}}); err != nil {
			t.Fatal(err)
		}
	}
	rebuild()
	finish()
	old := readProgress(t, ctx, store, Tasks)
	rebuild()
	finish()
	active := readProgress(t, ctx, store, Tasks)
	rebuild()
	pending := readProgress(t, ctx, store, Tasks)
	if err := store.snapshotPage(ctx, pending, SnapshotPage{Watermark: 1, NextCursor: resourceID,
		Records: []Record{testProjectionRecord(resourceID, 1, topicID, Tasks)}}); err != nil {
		t.Fatal(err)
	}
	for range 30 {
		if err := store.prune(ctx, Tasks); err != nil {
			t.Fatal(err)
		}
		var exists bool
		err := store.pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM resource_projection_retired WHERE origin = 'tasks' AND generation <= $1)`, old.Active).Scan(&exists)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			var left int
			err := store.pool.QueryRow(ctx, `SELECT count(*) FROM resource_projection_items
				WHERE origin = 'tasks' AND generation IN ($1,$2,$3)`, old.Active, active.Active, pending.Pending).Scan(&left)
			if left != 2 || err != nil {
				t.Fatal("prune lost live entries or retained retired entries:", left, err)
			}
			return
		}
	}
	t.Fatal("retired generation was not cleaned")
}

func BenchmarkResourceReplay(b *testing.B) {
	for _, size := range []int{1, pageLimit} {
		b.Run(fmt.Sprintf("transaction_%d", size), func(b *testing.B) {
			ctx, store := resourceTestStore(b)
			if err := store.beginSnapshot(ctx, readProgress(b, ctx, store, Tasks), nil); err != nil {
				b.Fatal(err)
			}
			if err := store.snapshotPage(ctx, readProgress(b, ctx, store, Tasks), SnapshotPage{}); err != nil {
				b.Fatal(err)
			}
			state := readProgress(b, ctx, store, Tasks)
			events := make([]Event, pageLimit)
			b.ReportAllocs()
			for b.Loop() {
				for i := range events {
					version := state.Through + int64(i) + 1
					key := fmt.Sprintf("a1000000-0000-4000-8000-%012x", version)
					id := fmt.Sprintf("71000000-0000-4000-8000-%012x", i+1)
					events[i] = testProjectionEvent(key, testProjectionRecord(id, version, topicID, Tasks))
				}
				for start := 0; start < len(events); start += size {
					part := events[start : start+size]
					last := part[len(part)-1].Record.Version
					if err := store.applyTasks(ctx, state, ChangePage{Watermark: last, Through: last, Events: part}); err != nil {
						b.Fatal(err)
					}
					state.Through, state.Revision = last, state.Revision+1
				}
			}
		})
	}
}
