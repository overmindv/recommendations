package resources

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/overmindv/recommendations/internal/ranking"
)

func TestStudyAlternativeRequiresOwnerConfirmation(t *testing.T) {
	t.Parallel()

	key := VersionKey{ID: resourceID, RevisionID: revisionID}
	task := ranking.ResourceKey{ID: key.ID, RevisionID: key.RevisionID, Kind: ranking.Practice}
	confirmed := availabilityFunc(func(_ context.Context, keys []VersionKey) (map[VersionKey]bool, error) {
		if len(keys) != 1 || keys[0] != key {
			return nil, errors.New("unexpected resource version")
		}

		return map[VersionKey]bool{key: true}, nil
	})
	unavailable := availabilityFunc(func(context.Context, []VersionKey) (map[VersionKey]bool, error) {
		return nil, ErrUnavailable
	})
	source := NewStudySource(newEligibility(nil, confirmed, unavailable), time.Hour)
	if !source.StudyAlternativeAvailable(t.Context(), task) {
		t.Fatal("confirmed task was hidden")
	}
	task.Kind = ranking.Theory
	if source.StudyAlternativeAvailable(t.Context(), task) {
		t.Fatal("an unavailable owner allowed a material")
	}
}

func TestStudyAlternativeOwnerTimeout(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		owner := availabilityFunc(func(ctx context.Context, _ []VersionKey) (map[VersionKey]bool, error) {
			<-ctx.Done()

			return nil, ctx.Err()
		})
		started := time.Now()
		source := NewStudySource(newEligibility(nil, owner, owner), time.Hour)
		task := ranking.ResourceKey{ID: resourceID, RevisionID: revisionID, Kind: ranking.Practice}
		if source.StudyAlternativeAvailable(t.Context(), task) || time.Since(started) != time.Second {
			t.Fatal("owner timeout did not hide alternative within one second")
		}
	})
}

func BenchmarkStudyCandidateRead(b *testing.B) {
	ctx, store := resourceTestStore(b)
	var generation int64
	var actor, goal string
	if err := store.pool.QueryRow(ctx, `
		UPDATE catalog_projection_state SET active_generation=active_generation+1
		WHERE singleton RETURNING active_generation,gen_random_uuid()::text,gen_random_uuid()::text`).
		Scan(&generation, &actor, &goal); err != nil {
		b.Fatal(err)
	}
	var topics []string
	if err := store.pool.QueryRow(ctx, `WITH seeded AS (
		    INSERT INTO catalog_projected_topics (generation,topic_id,revision,tombstone,metadata)
		    SELECT $1,gen_random_uuid(),1,false,'{"status":"active"}' FROM generate_series(1,64)
		    RETURNING topic_id
		) SELECT array_agg(topic_id ORDER BY topic_id) FROM seeded`, generation).Scan(&topics); err != nil {
		b.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, "UPDATE resource_projection_checkpoints SET active_generation=$1", generation); err != nil {
		b.Fatal(err)
	}
	for _, origin := range []Origin{Tasks, Content} {
		const query = `
			WITH seed AS MATERIALIZED (
			    SELECT gen_random_uuid() AS resource_id,t.id AS topic_id
			    FROM unnest($3::uuid[]) AS t(id) CROSS JOIN generate_series(1,10)
			), inserted AS (
			    INSERT INTO resource_projection_items (origin,generation,resource_id,version,revision_id,metadata)
			    SELECT $1,$2,resource_id,1,gen_random_uuid(),'{"Type":"single_choice","Difficulty":"easy","EstimatedMinutes":5}'
			    FROM seed RETURNING resource_id
			)
			INSERT INTO resource_projection_topics (origin,generation,resource_id,topic_id,weight)
			SELECT $1,$2,s.resource_id,s.topic_id,1 FROM seed s JOIN inserted i USING(resource_id)`
		if _, err := store.pool.Exec(ctx, query, origin, generation, topics); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO learning_task_outcomes (submission_id,learner_id,task_id,task_version_id,topic_ids,verdict,evidence_kind,
		                                    graded_at,event_id,correction_version)
		SELECT gen_random_uuid(),$1,gen_random_uuid(),gen_random_uuid(),'{}',
		       CASE WHEN n%3=0 THEN 'accepted' ELSE 'wrong_answer' END,
		       'independent',now()-n*interval '1 minute',gen_random_uuid(),1
		FROM generate_series(1,50000) AS n`, actor); err != nil {
		b.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, "ANALYZE learning_task_outcomes, resource_projection_topics, resource_projection_items"); err != nil {
		b.Fatal(err)
	}
	source := NewStudySource(&Eligibility{diagnostic: store}, 24*time.Hour)
	b.ReportAllocs()
	for b.Loop() {
		rows, err := source.read(ctx, actor, goal, topics)
		if err != nil || len(rows) != 1280 {
			b.Fatal("unexpected study candidate window:", len(rows), err)
		}
	}
}
