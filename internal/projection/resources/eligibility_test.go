package resources

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/overmindv/recommendations/internal/materialref"
	"github.com/overmindv/recommendations/internal/planner"
)

type availabilityFunc func(context.Context, []VersionKey) (map[VersionKey]bool, error)

func (f availabilityFunc) Available(ctx context.Context, keys []VersionKey) (map[VersionKey]bool, error) {
	return f(ctx, keys)
}

func TestEligibilityUsesActiveTopicsAndOwnerVersions(t *testing.T) {
	ctx, store := resourceTestStore(t)
	const generation = 9001
	if _, err := store.pool.Exec(ctx, `UPDATE catalog_projection_state SET active_generation = $1`, generation); err != nil {
		t.Fatal(err)
	}
	topic := func(id, status string) {
		t.Helper()
		_, err := store.pool.Exec(ctx, `
			INSERT INTO catalog_projected_topics (generation,topic_id,revision,tombstone,metadata)
			VALUES ($1,$2,0,false,jsonb_build_object('status',$3::text))
			ON CONFLICT (generation,topic_id) DO UPDATE SET metadata=EXCLUDED.metadata`, generation, id, status)
		if err != nil {
			t.Fatal(err)
		}
	}
	topic(topicID, "active")
	topic(otherTopic, "archived")

	const staleID = "88888888-8888-4888-8888-888888888888"
	const hiddenID = "99999999-9999-4999-8999-999999999999"
	const unknownID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	const activeMaterial = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	const deniedMaterial = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	estimated := 17
	rows := []Record{
		testProjectionRecord(resourceID, 1, topicID, Tasks),
		testProjectionRecord(staleID, 2, topicID, Tasks),
		testProjectionRecord(hiddenID, 3, otherTopic, Tasks),
		testProjectionRecord(unknownID, 4, topicID, Tasks),
	}
	for i := range rows[:3] {
		rows[i].Metadata.EstimatedMinutes = &estimated
	}
	state := readProgress(t, ctx, store, Tasks)
	if err := store.beginSnapshot(ctx, state, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.snapshotPage(ctx, readProgress(t, ctx, store, Tasks), SnapshotPage{Watermark: 4, Records: rows}); err != nil {
		t.Fatal(err)
	}

	owner := availabilityFunc(func(_ context.Context, keys []VersionKey) (map[VersionKey]bool, error) {
		if !slices.Equal(keys, []VersionKey{{resourceID, revisionID}, {staleID, revisionID}}) {
			t.Error("unknown or inactive resources queried:", keys)
		}
		return map[VersionKey]bool{{resourceID, revisionID}: true, {staleID, revisionID}: false}, nil
	})
	materialOwner := availabilityFunc(func(_ context.Context, keys []VersionKey) (map[VersionKey]bool, error) {
		if !slices.Equal(keys, []VersionKey{{activeMaterial, revisionID}, {deniedMaterial, revisionID}}) {
			t.Error("material order or owner keys changed:", keys)
		}
		return map[VersionKey]bool{{activeMaterial, revisionID}: true}, nil
	})
	e := newEligibility(store, owner, materialOwner)
	eligible, err := e.PracticeTasks(ctx, []planner.Task{
		{ID: resourceID, TaskVersionID: revisionID, TopicID: topicID, Title: "Untrusted title"},
		{ID: staleID, TaskVersionID: revisionID, TopicID: topicID},
		{ID: hiddenID, TaskVersionID: revisionID, TopicID: otherTopic},
		{ID: unknownID, TaskVersionID: revisionID, TopicID: topicID},
	})
	if err != nil || len(eligible) != 1 || eligible[0].ID != resourceID ||
		eligible[0].CatalogMinutes != estimated || eligible[0].Title != "Exercise" {
		t.Fatal("wrong set of eligible tasks:", eligible, err)
	}
	materialRows := []Record{
		testProjectionRecord(activeMaterial, 1, topicID, Content),
		testProjectionRecord(deniedMaterial, 2, topicID, Content),
	}
	for _, row := range materialRows {
		row.Metadata.EstimatedMinutes = &estimated
	}
	position := KafkaPosition{TopicID: "fixture", Offsets: map[int32]int64{0: 0}}
	if err := store.beginSnapshot(ctx, readProgress(t, ctx, store, Content), &position); err != nil {
		t.Fatal(err)
	}
	if err := store.snapshotPage(ctx, readProgress(t, ctx, store, Content), SnapshotPage{Watermark: 2, Records: materialRows}); err != nil {
		t.Fatal(err)
	}
	selected, err := e.OutlineMaterials(ctx, []string{otherTopic, topicID}, []string{materialref.Encode(activeMaterial, otherTopic)})
	if err != nil || len(selected) != 1 || selected[0].Key.ID != activeMaterial {
		t.Fatal("outline uses stale materials or loses a new revision:", selected, err)
	}
	if err := store.Ready(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerChecksAreBoundedAndDoNotReturnPartialApproval(t *testing.T) {
	keys := make([]VersionKey, planner.MaxTasks)
	for i := range keys {
		keys[i] = VersionKey{ID: fmt.Sprintf("a1000000-0000-4000-8000-%012x", i+1), RevisionID: revisionID}
	}
	var inFlight, peak atomic.Int32
	check := availabilityFunc(func(ctx context.Context, batch []VersionKey) (map[VersionKey]bool, error) {
		if len(batch) > pageLimit {
			t.Error("owner batch exceeded its limit")
		}
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for p := peak.Load(); p < n && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		if batch[0] == keys[pageLimit] {
			return nil, errors.New("owner failed")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Millisecond):
			return map[VersionKey]bool{batch[0]: true}, nil
		}
	})
	e := newEligibility(nil, check, check)
	result, err := e.check(t.Context(), check.Available, keys)
	if !errors.Is(err, ErrUnavailable) || result != nil || peak.Load() > 4 {
		t.Fatal("owner failure leaked a partial allowlist or ran unbounded:", peak.Load(), err)
	}
}

func BenchmarkProjectedTaskLookup(b *testing.B) {
	ctx, store := resourceTestStore(b)
	const fixtureGeneration = 90000000
	const count = 50000
	if _, err := store.pool.Exec(ctx, `UPDATE catalog_projection_state SET active_generation = $1`, fixtureGeneration); err != nil {
		b.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO catalog_projected_topics (generation,topic_id,revision,tombstone,metadata)
		VALUES ($1,$2,0,false,'{"status":"active"}') ON CONFLICT DO NOTHING`, fixtureGeneration, topicID); err != nil {
		b.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE resource_projection_checkpoints SET active_generation = $1 WHERE origin='tasks'`, fixtureGeneration); err != nil {
		b.Fatal(err)
	}
	bulk := `
		INSERT INTO resource_projection_items (origin,generation,resource_id,version,revision_id,metadata)
		SELECT 'tasks', $1, md5('task-' || n)::uuid, n, md5('revision-' || n)::uuid,
		    jsonb_build_object('Title','A fixture task','Type','single_choice','Difficulty','easy','EstimatedMinutes',17)
		FROM generate_series(1,$2) AS n ON CONFLICT DO NOTHING`
	if _, err := store.pool.Exec(ctx, bulk, fixtureGeneration, count); err != nil {
		b.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO resource_projection_topics (origin,generation,resource_id,topic_id,weight)
		SELECT origin,generation,resource_id,$2,1 FROM resource_projection_items
		WHERE origin='tasks' AND generation=$1 ON CONFLICT DO NOTHING`, fixtureGeneration, topicID); err != nil {
		b.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `ANALYZE resource_projection_items; ANALYZE resource_projection_topics`); err != nil {
		b.Fatal(err)
	}
	for _, size := range []int{100, planner.MaxTasks} {
		b.Run(fmt.Sprintf("keys_%d", size), func(b *testing.B) {
			rows, err := store.pool.Query(ctx, `
				SELECT resource_id::text,revision_id::text
				FROM resource_projection_items WHERE origin='tasks' AND generation=$1 ORDER BY version DESC LIMIT $2`, fixtureGeneration, size)
			if err != nil {
				b.Fatal(err)
			}
			keys := make([]lookupKey, 0, size)
			for rows.Next() {
				key := lookupKey{TopicID: topicID}
				if err := rows.Scan(&key.ID, &key.RevisionID); err != nil {
					rows.Close()
					b.Fatal(err)
				}
				keys = append(keys, key)
			}
			rows.Close()
			if rows.Err() != nil || len(keys) != size {
				b.Fatal("benchmark fixture lookup failed:", len(keys), rows.Err())
			}
			b.ReportAllocs()
			for b.Loop() {
				got, err := store.lookup(ctx, Tasks, keys)
				if err != nil || len(got) != size {
					b.Fatal("bench lookup lost fixture resources:", len(got), err)
				}
			}
		})
	}
}
