package studyplans

import (
	"context"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

type resourceID struct {
	kind ranking.Kind
	id   string
}

func applyResourceFeedback(ctx context.Context, pool *pgxpool.Pool, goal string, topics []string, input *planner.LearningRequest, now time.Time) error {
	if len(input.Resources) == 0 {
		return nil
	}
	if len(input.Resources) > ranking.MaxCandidates {
		return ErrInvalid
	}
	return pgx.BeginTxFunc(ctx, pool, readOnly, func(tx pgx.Tx) error {
		hints, err := readDifficultyHints(ctx, tx, goal, topics, now)
		if err != nil {
			return err
		}
		excluded, err := readCooldowns(ctx, tx, goal, input.Resources, now)
		if err != nil {
			return err
		}
		input.DifficultyHints = hints
		if len(excluded) != 0 {
			input.Resources = slices.Clone(input.Resources)
			for i := range input.Resources {
				row := &input.Resources[i]
				if excluded[resourceID{row.Kind, row.ID}] {
					row.Available = false
				}
			}
		}
		return nil
	})
}

func readDifficultyHints(ctx context.Context, tx pgx.Tx, goal string, topics []string, now time.Time) (map[string]int, error) {
	rows, err := tx.Query(ctx, `
		WITH recent AS MATERIALIZED (
		    SELECT plan_version,ordinal,action,accepted_at,request_id
		    FROM study_plan_feedback WHERE goal_id=$1 AND action IN ('too_easy','too_hard')
		      AND accepted_at >= $3::timestamptz-interval '7 days'
		    ORDER BY accepted_at DESC,request_id LIMIT 128
		)
		SELECT DISTINCT ON (i.topic_id) i.topic_id::text,CASE WHEN f.action='too_easy' THEN 1 ELSE -1 END
		FROM recent AS f JOIN study_plan_items AS i
		  ON i.goal_id=$1 AND i.plan_version=f.plan_version AND i.ordinal=f.ordinal
		WHERE i.topic_id=ANY($2::uuid[]) ORDER BY i.topic_id,f.accepted_at DESC,f.request_id`, goal, topics, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	hints := make(map[string]int)
	for rows.Next() {
		var id string
		var shift int
		if err := rows.Scan(&id, &shift); err != nil {
			return nil, err
		}
		hints[id] = shift
	}
	return hints, rows.Err()
}

func readCooldowns(ctx context.Context, tx pgx.Tx, goal string, candidates []planner.StudyResource, now time.Time) (map[resourceID]bool, error) {
	kinds, ids := make([]string, 0, len(candidates)), make([]string, 0, len(candidates))
	seen := make(map[resourceID]bool, len(candidates))
	for _, row := range candidates {
		key := resourceID{row.Kind, row.ID}
		if !seen[key] {
			seen[key] = true
			kinds, ids = append(kinds, string(row.Kind)), append(ids, row.ID)
		}
	}
	rows, err := tx.Query(ctx, `
		SELECT c.kind,c.resource_id::text
		FROM unnest($2::text[],$3::uuid[]) AS candidate(kind,id)
		JOIN study_plan_resource_cooldowns AS c
		  ON c.goal_id=$1 AND c.kind=candidate.kind AND c.resource_id=candidate.id
		WHERE c.expires_at>$4`, goal, kinds, ids, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	excluded := make(map[resourceID]bool)
	for rows.Next() {
		var key resourceID
		if err := rows.Scan(&key.kind, &key.id); err != nil {
			return nil, err
		}
		excluded[key] = true
	}
	return excluded, rows.Err()
}
