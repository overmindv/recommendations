package resources

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/planner"
)

func (s *Store) Ready(ctx context.Context) error {
	var ready bool
	err := s.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) = 2 FROM resource_projection_checkpoints WHERE active_generation > 0)
		   AND EXISTS (SELECT 1 FROM catalog_projection_state WHERE singleton AND active_generation > 0)`).Scan(&ready)
	if err != nil || !ready {
		return ErrUnavailable
	}
	return nil
}

const candidateFields = `r.resource_id::text, r.revision_id::text,
		r.metadata->>'Title', r.metadata->>'Type', r.metadata->>'Difficulty',
		(r.metadata->>'EstimatedMinutes')::integer`

func (s *Store) lookup(ctx context.Context, origin Origin, keys []lookupKey) (map[VersionKey]Candidate, error) {
	const query = `SELECT ` + candidateFields + `
		FROM resource_projection_checkpoints AS a
		CROSS JOIN catalog_projection_state AS graph
		CROSS JOIN unnest($2::uuid[], $3::uuid[], $4::text[]) AS wanted(id, version, topic)
		JOIN LATERAL (
		    SELECT i.resource_id, i.revision_id, i.metadata
		    FROM resource_projection_items AS i
		    WHERE i.origin = a.origin AND i.generation = a.active_generation
		      AND i.resource_id = wanted.id AND i.revision_id = wanted.version
		      AND EXISTS (
		        SELECT 1 FROM resource_projection_topics AS t
		        JOIN catalog_projected_topics AS top ON top.generation = graph.active_generation AND top.topic_id = t.topic_id
		        WHERE t.origin = i.origin AND t.generation = i.generation AND t.resource_id = i.resource_id
		            AND (wanted.topic = '' OR t.topic_id = NULLIF(wanted.topic, '')::uuid)
		            AND NOT top.tombstone AND top.metadata->>'status' = 'active'
		      )
		    LIMIT 1
		) AS r ON true
		WHERE a.origin = $1 AND graph.singleton AND graph.active_generation > 0`
	ids, versions, topics := make([]string, 0, len(keys)), make([]string, 0, len(keys)), make([]string, 0, len(keys))
	for _, key := range keys {
		if !planner.ValidID(key.ID) || !planner.ValidID(key.RevisionID) ||
			(key.TopicID != "" && !planner.ValidID(key.TopicID)) {
			return nil, errInvalidPage
		}
		ids, versions, topics = append(ids, key.ID), append(versions, key.RevisionID), append(topics, key.TopicID)
	}
	if err := s.readyOrigin(ctx, origin); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, query, origin, ids, versions, topics)
	if err != nil {
		return nil, fmt.Errorf("look up projected resources: %w", err)
	}
	byKey := make(map[VersionKey]Candidate, len(keys))
	defer rows.Close()
	for rows.Next() {
		candidate, err := scanCandidate(rows)
		if err != nil {
			return nil, err
		}
		byKey[candidate.Key] = candidate
	}
	return byKey, rows.Err()
}

func (s *Store) readyOrigin(ctx context.Context, origin Origin) error {
	var ready bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM resource_projection_checkpoints WHERE origin = $1 AND active_generation > 0)
		   AND EXISTS (SELECT 1 FROM catalog_projection_state WHERE singleton AND active_generation > 0)`, origin).Scan(&ready)
	if err != nil {
		return fmt.Errorf("check active resource generation: %w", err)
	}
	if !ready {
		return errNotReady
	}
	return nil
}

func (s *Store) materials(ctx context.Context, topics []string) ([]Candidate, error) {
	const query = `SELECT ` + candidateFields + `
		FROM resource_projection_checkpoints AS a
		CROSS JOIN catalog_projection_state AS graph
		CROSS JOIN unnest($1::uuid[]) WITH ORDINALITY AS wanted(topic_id, position)
		JOIN catalog_projected_topics AS top ON top.topic_id = wanted.topic_id
		JOIN LATERAL (
		    SELECT t.resource_id, t.weight
		    FROM resource_projection_topics AS t
		    WHERE t.origin = a.origin AND t.generation = a.active_generation AND t.topic_id = wanted.topic_id
		    ORDER BY t.weight DESC, t.resource_id
		    LIMIT $2
		) AS matched ON true
		JOIN resource_projection_items AS r
		  ON r.origin = a.origin AND r.generation = a.active_generation AND r.resource_id = matched.resource_id
		WHERE a.origin = 'content' AND graph.singleton AND a.active_generation > 0
		  AND top.generation = graph.active_generation AND NOT top.tombstone AND top.metadata->>'status' = 'active'
		ORDER BY wanted.position, matched.weight DESC, r.resource_id`
	for _, id := range topics {
		if !planner.ValidID(id) {
			return nil, errInvalidPage
		}
	}
	if err := s.readyOrigin(ctx, Content); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, query, topics, maxPerTopic)
	if err != nil {
		return nil, fmt.Errorf("list projected materials: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Candidate, error) { return scanCandidate(row) })
}

func scanCandidate(row pgx.Row) (Candidate, error) {
	var candidate Candidate
	err := row.Scan(&candidate.Key.ID, &candidate.Key.RevisionID, &candidate.Title, &candidate.Type,
		&candidate.Difficulty, &candidate.EstimatedMinutes)
	return candidate, err
}
