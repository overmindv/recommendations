package catalog

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

var ErrStudyScopeLimit = errors.New("study scope exceeds supported topic count")

const studyScopeSQL = `
	WITH RECURSIVE reachable(id) AS (
	    SELECT target FROM unnest($1::uuid[]) AS wanted(target)
	    UNION
	    SELECT pre.prerequisite_id
	    FROM reachable AS r
	    CROSS JOIN LATERAL (
	        SELECT e.prerequisite_id FROM catalog_projected_prerequisites AS e
	        WHERE e.generation=$2 AND e.topic_id=r.id AND NOT e.tombstone
	        ORDER BY e.prerequisite_id LIMIT $3
	    ) AS pre
	), bounded AS MATERIALIZED (
	    SELECT id FROM reachable LIMIT $3
	)
	SELECT b.id::text,COALESCE(NOT t.tombstone AND t.metadata->>'status'='active',false),
	       ARRAY(SELECT e.prerequisite_id FROM catalog_projected_prerequisites AS e
	             WHERE e.generation=$2 AND e.topic_id=b.id AND NOT e.tombstone
	             ORDER BY e.prerequisite_id LIMIT $3)
	FROM bounded AS b LEFT JOIN catalog_projected_topics AS t ON t.generation=$2 AND t.topic_id=b.id
	ORDER BY b.id`

func ReadStudyScope(ctx context.Context, tx pgx.Tx, targets []string) (map[string]ranking.Topic, []string, error) {
	if len(targets) < 1 || len(targets) > 16 {
		return nil, nil, ErrNotFound
	}
	for _, target := range targets {
		if !planner.ValidID(target) {
			return nil, nil, ErrNotFound
		}
	}
	var generation int64
	if err := tx.QueryRow(ctx, "SELECT active_generation FROM catalog_projection_state WHERE singleton").Scan(&generation); err != nil {
		return nil, nil, fmt.Errorf("read active catalog generation: %w", err)
	}
	if generation == 0 {
		return nil, nil, ErrNotReady
	}
	rows, err := tx.Query(ctx, studyScopeSQL, targets, generation, ranking.MaxTopics+1)
	if err != nil {
		return nil, nil, fmt.Errorf("read study scope: %w", err)
	}
	defer rows.Close()
	scope := make(map[string]ranking.Topic)
	ids := make([]string, 0, len(targets))
	var missing bool
	for rows.Next() {
		if len(ids) == ranking.MaxTopics {
			return nil, nil, ErrStudyScopeLimit
		}
		var id string
		var active bool
		var prerequisites []string
		if err := rows.Scan(&id, &active, &prerequisites); err != nil {
			return nil, nil, err
		}
		missing = missing || !active
		scope[id], ids = ranking.Topic{Prerequisites: prerequisites}, append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if missing {
		return nil, nil, ErrNotFound
	}

	return scope, ids, nil
}
