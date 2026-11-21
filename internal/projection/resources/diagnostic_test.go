package resources

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/diagnostic"
)

func BenchmarkDiagnosticCatalogProjection(b *testing.B) {
	ctx, store := resourceTestStore(b)
	rows, err := store.pool.Query(ctx, "SELECT gen_random_uuid()::text FROM generate_series(1, $1::integer)", diagnostic.MaxTopics)
	if err != nil {
		b.Fatal(err)
	}
	topics, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		b.Fatal(err)
	}
	const generation = 9001
	batch := &pgx.Batch{}
	batch.Queue("UPDATE catalog_projection_state SET active_generation=$1 WHERE singleton", generation)
	batch.Queue("UPDATE resource_projection_checkpoints SET active_generation=$1 WHERE origin='tasks'", generation)
	batch.Queue(`INSERT INTO catalog_projected_topics (generation, topic_id, revision, tombstone, metadata)
		SELECT $1, t, 1, false, '{"status":"active"}' FROM unnest($2::uuid[]) AS t`, generation, topics)
	batch.Queue(`INSERT INTO catalog_projected_prerequisites (generation, topic_id, prerequisite_id, revision, tombstone)
		SELECT $1, $2, t, 1, false FROM unnest($3::uuid[]) AS t`, generation, topics[0], topics[1:])
	batch.Queue(`
		WITH seed AS MATERIALIZED (
		    SELECT gen_random_uuid() AS id, ($2::uuid[])[(n % 32) + 1] AS topic,
		           CASE WHEN n % 17 = 0 THEN (n % 20) + 1 ELSE 90 END AS minutes
		    FROM generate_series(1, 50000) AS n
		), written AS (
		    INSERT INTO resource_projection_items (origin, generation, resource_id, version, revision_id, metadata)
		    SELECT 'tasks', $1, id, 1, id, jsonb_build_object('EstimatedMinutes', minutes) FROM seed
		    RETURNING resource_id
		)
		INSERT INTO resource_projection_topics (origin, generation, resource_id, topic_id, weight, diagnostic_minutes)
		SELECT 'tasks', $1, id, topic, 1, CASE WHEN minutes <= 20 THEN minutes END
		FROM seed JOIN written ON written.resource_id = seed.id`, generation, topics)
	batch.Queue("ANALYZE resource_projection_topics")
	if err := store.pool.SendBatch(ctx, batch).Close(); err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		scope, candidates, err := store.diagnosticCandidates(ctx, topics[:1], 10, diagnostic.MaxTopics-1, diagnosticPerTopic)
		if err != nil || len(scope.Targets) != 1 || len(scope.Prerequisites) != diagnostic.MaxTopics-1 ||
			len(candidates) != diagnostic.MaxCandidates {
			b.Fatal("incorrect diagnostic candidate bounds:", len(scope.Targets), len(scope.Prerequisites), len(candidates), err)
		}
	}
}
