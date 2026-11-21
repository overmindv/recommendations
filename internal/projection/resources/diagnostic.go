package resources

import (
	"context"
	"fmt"
	"time"

	"github.com/overmindv/recommendations/internal/diagnostic"
	"github.com/overmindv/recommendations/internal/planner"
)

const diagnosticPerTopic = diagnostic.MaxCandidates / diagnostic.MaxTopics

func (e *Eligibility) SelectDiagnostic(ctx context.Context, actor string, targets []string, budget int) (diagnostic.Selection, error) {
	if !planner.ValidID(actor) || budget < 1 || budget > diagnostic.MaxMinutes ||
		len(targets) < 1 || len(targets) > diagnostic.MaxTopics {
		return diagnostic.Selection{}, diagnostic.ErrInvalidSelection
	}
	for _, id := range targets {
		if !planner.ValidID(id) {
			return diagnostic.Selection{}, diagnostic.ErrInvalidSelection
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	scope, candidates, err := e.diagnostic.diagnosticCandidates(ctx, targets, budget)
	if err != nil {
		return diagnostic.Selection{}, ErrUnavailable
	}
	if len(scope.Targets) != len(targets) {
		return diagnostic.Selection{}, diagnostic.ErrTopicUnavailable
	}
	seen := make(map[VersionKey]bool, len(candidates))
	keys := make([]VersionKey, 0, len(candidates))
	for _, row := range candidates {
		key := VersionKey{row.TaskID, row.VersionID}
		if !seen[key] {
			keys = append(keys, key)
			seen[key] = true
		}
	}
	available, err := e.check(ctx, func(ctx context.Context, keys []VersionKey) (map[VersionKey]bool, error) {
		return e.diagnosticCheck(ctx, actor, keys)
	}, keys)
	if err != nil {
		return diagnostic.Selection{}, err
	}
	eligible := candidates[:0]
	for _, row := range candidates {
		if available[VersionKey{row.TaskID, row.VersionID}] {
			eligible = append(eligible, row)
		}
	}

	return diagnostic.Select(scope, eligible, budget)
}

func (e *Eligibility) CheckDiagnostic(ctx context.Context, tasks []diagnostic.Candidate) error {
	keys := make([]lookupKey, 0, len(tasks))
	for _, row := range tasks {
		keys = append(keys, lookupKey{VersionKey: VersionKey{row.TaskID, row.VersionID}, TopicID: row.TopicID})
	}
	available, err := e.verified(ctx, Tasks, keys, false)
	if err != nil {
		return err
	}
	if len(available) != len(keys) {
		return diagnostic.ErrTaskUnavailable
	}

	return nil
}

const diagnosticCandidatesSQL = `
	WITH graph AS (
	    SELECT active_generation AS generation FROM catalog_projection_state
	    WHERE singleton AND active_generation > 0 LIMIT 1
	), active_tasks AS (
	    SELECT active_generation FROM resource_projection_checkpoints
	    WHERE origin = 'tasks' AND active_generation > 0 LIMIT 1
	), targets AS (
	    SELECT t.topic_id
	    FROM graph AS g CROSS JOIN unnest($1::uuid[]) AS wanted(id)
	    CROSS JOIN LATERAL (
	        SELECT topic_id FROM catalog_projected_topics
	        WHERE generation = g.generation AND topic_id = wanted.id AND NOT tombstone AND metadata->>'status' = 'active'
	        LIMIT 1
	    ) AS t
	), prerequisites AS (
	    SELECT DISTINCT next.topic_id
	    FROM targets AS t CROSS JOIN graph AS g
	    CROSS JOIN LATERAL (
	        SELECT e.prerequisite_id AS topic_id FROM catalog_projected_prerequisites AS e
	        WHERE e.generation = g.generation AND e.topic_id = t.topic_id AND NOT e.tombstone
	          AND e.prerequisite_id <> ALL($1::uuid[])
	          AND (
	              SELECT 1 FROM catalog_projected_topics AS p
	              WHERE p.generation = g.generation AND p.topic_id = e.prerequisite_id
	                AND NOT p.tombstone AND p.metadata->>'status' = 'active'
	              LIMIT 1
	          ) IS NOT NULL
	        ORDER BY e.prerequisite_id LIMIT $2
	    ) AS next
	    ORDER BY next.topic_id LIMIT $2
	), topics AS (
	    SELECT topic_id, true AS target FROM targets
	    UNION ALL SELECT topic_id, false FROM prerequisites
	)
	SELECT t.topic_id::text, t.target, r.resource_id::text, i.revision_id::text, r.diagnostic_minutes
	FROM topics AS t CROSS JOIN active_tasks AS a
	LEFT JOIN LATERAL (
	    SELECT m.resource_id, m.diagnostic_minutes FROM resource_projection_topics AS m
	    WHERE m.origin = 'tasks' AND m.generation = a.active_generation AND m.topic_id = t.topic_id
	      AND m.diagnostic_minutes BETWEEN 1 AND $3::smallint
	    ORDER BY m.diagnostic_minutes, m.weight DESC, m.resource_id LIMIT $4
	) AS r ON true
	LEFT JOIN LATERAL (
	    SELECT revision_id FROM resource_projection_items
	    WHERE origin = 'tasks' AND generation = a.active_generation AND resource_id = r.resource_id
	      AND (metadata->>'EstimatedMinutes')::integer = r.diagnostic_minutes
	    LIMIT 1
	) AS i ON true`

func (s *Store) diagnosticCandidates(ctx context.Context, targets []string, budget int) (diagnostic.Scope, []diagnostic.Candidate, error) {
	if err := s.readyOrigin(ctx, Tasks); err != nil {
		return diagnostic.Scope{}, nil, err
	}
	rows, err := s.pool.Query(ctx, diagnosticCandidatesSQL, targets, diagnostic.MaxTopics-len(targets), budget, diagnosticPerTopic)
	if err != nil {
		return diagnostic.Scope{}, nil, fmt.Errorf("look up diagnostic candidates: %w", err)
	}
	defer rows.Close()
	var scope diagnostic.Scope
	var candidates []diagnostic.Candidate
	seen := make(map[string]bool)
	for rows.Next() {
		var topic string
		var target bool
		var id, revision *string
		var minutes *int
		if err := rows.Scan(&topic, &target, &id, &revision, &minutes); err != nil {
			return diagnostic.Scope{}, nil, err
		}
		if !seen[topic] {
			if target {
				scope.Targets = append(scope.Targets, topic)
			} else {
				scope.Prerequisites = append(scope.Prerequisites, topic)
			}
			seen[topic] = true
		}
		if id != nil && revision != nil && minutes != nil {
			candidates = append(candidates, diagnostic.Candidate{TaskID: *id, VersionID: *revision, TopicID: topic, Minutes: *minutes})
		}
	}

	return scope, candidates, rows.Err()
}
