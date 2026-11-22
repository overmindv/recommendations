package resources

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
	"golang.org/x/sync/errgroup"
)

const studyPerTopic = 10

type StudySource struct {
	eligibility *Eligibility
	recent      time.Duration
}

func NewStudySource(eligibility *Eligibility, recent time.Duration) *StudySource {
	return &StudySource{eligibility: eligibility, recent: recent}
}

func (s *StudySource) StudyAlternativeAvailable(ctx context.Context, resource ranking.ResourceKey) bool {
	if s.eligibility == nil || !planner.ValidID(resource.ID) || !planner.ValidID(resource.RevisionID) {
		return false
	}
	var owner availabilitySource
	switch resource.Kind {
	case ranking.Theory:
		owner = s.eligibility.content
	case ranking.Practice:
		owner = s.eligibility.tasks
	default:
		return false
	}
	if owner == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	key := VersionKey{ID: resource.ID, RevisionID: resource.RevisionID}
	confirmed, err := s.eligibility.check(ctx, owner.Available, []VersionKey{key})

	return err == nil && confirmed[key]
}

type studyCandidate struct {
	planner.StudyResource
	origin Origin
	key    VersionKey
}

func (s *StudySource) StudyResources(ctx context.Context, actor, goal string, topics []string) ([]planner.StudyResource, error) {
	if s.eligibility == nil || s.eligibility.diagnostic == nil ||
		!planner.ValidID(actor) || !planner.ValidID(goal) || len(topics) < 1 || len(topics) > ranking.MaxTopics ||
		s.recent < time.Hour || s.recent > 30*24*time.Hour {
		return nil, ErrUnavailable
	}
	for _, topic := range topics {
		if !planner.ValidID(topic) {
			return nil, ErrUnavailable
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	e := s.eligibility
	choices, err := s.read(ctx, actor, goal, topics)
	if err != nil {
		return nil, fmt.Errorf("read study candidates: %w", err)
	}
	tasks, materials := studyKeys(choices)
	var taskVersions, materialVersions, unseen map[VersionKey]bool
	group, requestCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		var err error
		taskVersions, err = e.check(requestCtx, e.tasks.Available, tasks)

		return err
	})
	group.Go(func() error {
		var err error
		materialVersions, err = e.check(requestCtx, e.content.Available, materials)

		return err
	})
	group.Go(func() error {
		var err error
		unseen, err = e.check(requestCtx, func(ctx context.Context, keys []VersionKey) (map[VersionKey]bool, error) {
			return e.diagnosticCheck(ctx, actor, keys)
		}, tasks)

		return err
	})
	if err := group.Wait(); err != nil {
		return nil, err
	}
	result := make([]planner.StudyResource, 0, len(choices))
	for _, row := range choices {
		c := row.StudyResource
		if row.origin == Content {
			c.Available = materialVersions[row.key]
			result = append(result, c)

			continue
		}
		if c.DueReview {
			c.Available = taskVersions[row.key] && unseen[row.key]
			result = append(result, c)

			continue
		}
		c.Available = taskVersions[row.key]
		result = append(result, c)
		if c.Available && unseen[row.key] && !c.Recent && !c.AlreadySolved {
			c.IndependentCheck = true
			result = append(result, c)
		}
	}
	if len(result) > ranking.MaxCandidates {
		return nil, ErrUnavailable
	}

	return result, nil
}

func studyKeys(choices []studyCandidate) ([]VersionKey, []VersionKey) {
	tasks, materials := make([]VersionKey, 0, len(choices)/2), make([]VersionKey, 0, len(choices)/2)
	seen := make(map[struct {
		Origin
		VersionKey
	}]bool)
	for _, row := range choices {
		if row.AlreadySolved || row.Recent || row.Minutes == 0 {
			continue
		}
		key := struct {
			Origin
			VersionKey
		}{row.origin, row.key}
		if seen[key] {
			continue
		}
		seen[key] = true
		if row.origin == Tasks {
			tasks = append(tasks, row.key)
		} else {
			materials = append(materials, row.key)
		}
	}

	return tasks, materials
}

const studyCandidatesSQL = `
	WITH origins AS MATERIALIZED (
	    SELECT origin,active_generation FROM resource_projection_checkpoints
	    WHERE origin IN ('tasks','content') AND active_generation>0 LIMIT 2
	), selected AS MATERIALIZED (
	    SELECT w.topic_id, c.origin, resource.resource_id, resource.revision_id, resource.metadata
	    FROM unnest($1::uuid[]) AS w(topic_id) CROSS JOIN origins AS c
	    CROSS JOIN LATERAL (
	        SELECT t.resource_id FROM resource_projection_topics AS t
	        WHERE t.origin=c.origin AND t.generation=c.active_generation AND t.topic_id=w.topic_id
	        ORDER BY t.weight DESC,t.resource_id LIMIT $2
	    ) AS matched
	    JOIN LATERAL (
	        SELECT r.resource_id,r.revision_id,r.metadata FROM resource_projection_items AS r
	        WHERE r.origin=c.origin AND r.generation=c.active_generation AND r.resource_id=matched.resource_id
	          AND r.revision_id IS NOT NULL AND (r.metadata->>'EstimatedMinutes')::integer > 0
	        LIMIT 1
	    ) AS resource ON true
	)
	SELECT r.origin,r.topic_id::text,r.resource_id::text,r.revision_id::text,
	       (r.metadata->>'EstimatedMinutes')::integer,COALESCE(r.metadata->>'Difficulty',''),
	       ARRAY(SELECT runtime.value->>'ID' FROM jsonb_array_elements(
	           CASE WHEN jsonb_typeof(r.metadata->'Runtimes')='array' THEN r.metadata->'Runtimes' ELSE '[]'::jsonb END
	       ) AS runtime(value)),
	       COALESCE(solved.present,material_read.present,false),COALESCE(recent.present,false)
	FROM selected AS r
	LEFT JOIN LATERAL (
	    SELECT true AS present FROM learning_task_outcomes AS o
	    WHERE r.origin='tasks' AND o.learner_id=$3 AND o.task_id=r.resource_id
	      AND o.verdict='accepted' AND o.graded_at <= $5 LIMIT 1
	) AS solved ON true
	LEFT JOIN LATERAL (
	    SELECT true AS present FROM learning_material_reads AS m
	    WHERE r.origin='content' AND m.learner_id=$3 AND m.material_id=r.resource_id AND m.revision_id=r.revision_id
	    LIMIT 1
	) AS material_read ON true
	LEFT JOIN LATERAL (
	    SELECT true AS present FROM learning_task_outcomes AS o
	    WHERE r.origin='tasks' AND o.learner_id=$3 AND o.task_id=r.resource_id AND o.graded_at >= $4 AND o.graded_at <= $5
	    LIMIT 1
	) AS recent ON true
	ORDER BY r.topic_id,r.origin,r.resource_id`

const pinnedStudyChecksSQL = `
	SELECT p.topic_id::text,p.task_id::text,p.task_version_id::text,p.estimated_minutes,p.expires_at,
	       r.revision_id IS NOT NULL,COALESCE(r.metadata->>'Difficulty',''),
	       ARRAY(SELECT runtime.value->>'ID' FROM jsonb_array_elements(
	           CASE WHEN jsonb_typeof(r.metadata->'Runtimes')='array' THEN r.metadata->'Runtimes' ELSE '[]'::jsonb END
	       ) AS runtime(value))
	FROM learner_goals AS g
	JOIN retention_checks AS p ON p.goal_id=g.id AND p.goal_version=g.version
	JOIN resource_projection_checkpoints AS c ON c.origin='tasks'
	LEFT JOIN LATERAL (
	    SELECT item.revision_id,item.metadata FROM resource_projection_items AS item
	    JOIN resource_projection_topics AS t ON t.origin=item.origin AND t.generation=item.generation
	      AND t.resource_id=item.resource_id AND t.topic_id=p.topic_id
	    WHERE item.origin='tasks' AND item.generation=c.active_generation AND item.resource_id=p.task_id
	      AND item.revision_id=p.task_version_id LIMIT 1
	) AS r ON true
	LEFT JOIN LATERAL (
	    SELECT o.graded_at,o.task_version_id,o.topic_ids FROM learning_task_outcomes AS o
	    WHERE o.learner_id=g.user_id AND o.task_id=p.task_id AND o.evidence_kind='independent' AND o.graded_at <= $4
	    ORDER BY o.graded_at,o.submission_id LIMIT 1
	) AS first ON first.task_version_id=p.task_version_id AND p.topic_id=ANY(first.topic_ids)
	 AND first.graded_at >= p.assigned_at AND first.graded_at < p.expires_at
	WHERE g.id=$1 AND g.user_id=$2 AND g.active AND p.topic_id=ANY($3::uuid[]) AND p.expires_at>$4
	  AND first.graded_at IS NULL
	ORDER BY p.topic_id`

func (s *StudySource) read(ctx context.Context, actor, goal string, topics []string) ([]studyCandidate, error) {
	var result []studyCandidate
	err := pgx.BeginTxFunc(ctx, s.eligibility.diagnostic.pool,
		pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
			var now time.Time
			var ready, active bool
			err := tx.QueryRow(ctx, `
				SELECT statement_timestamp(),
				       (SELECT count(*)=2 FROM resource_projection_checkpoints WHERE active_generation>0),
				       (SELECT count(*)=cardinality($1::uuid[])
				        FROM catalog_projection_state AS state CROSS JOIN unnest($1::uuid[]) AS wanted(topic_id)
				        JOIN LATERAL (
				            SELECT 1 FROM catalog_projected_topics AS t
				            WHERE t.generation=state.active_generation AND t.topic_id=wanted.topic_id
				              AND NOT t.tombstone AND t.metadata->>'status'='active' LIMIT 1
				        ) AS active ON true WHERE state.singleton)`,
				topics).Scan(&now, &ready, &active)
			if err != nil {
				return err
			}
			if !ready || !active {
				return ErrUnavailable
			}
			rows, err := tx.Query(ctx, studyCandidatesSQL, topics, studyPerTopic, actor, now.Add(-s.recent), now)
			if err != nil {
				return err
			}
			result, err = pgx.CollectRows(rows, scanStudyCandidate)
			if err != nil {
				return err
			}
			pinned, err := readPinnedChecks(ctx, tx, actor, goal, topics, now)
			if err != nil {
				return err
			}
			pinnedByID := make(map[string]bool, len(pinned))
			for _, check := range pinned {
				pinnedByID[check.key.ID] = true
			}
			result = slices.DeleteFunc(result, func(row studyCandidate) bool {
				return row.origin == Tasks && pinnedByID[row.key.ID]
			})
			result = append(result, pinned...)

			return nil
		})

	return result, err
}

func scanStudyCandidate(row pgx.CollectableRow) (studyCandidate, error) {
	var c studyCandidate
	if err := row.Scan(&c.origin, &c.TopicID, &c.key.ID, &c.key.RevisionID, &c.Minutes,
		&c.Difficulty, &c.Languages, &c.AlreadySolved, &c.Recent); err != nil {
		return c, err
	}
	kind := ranking.Theory
	if c.origin == Tasks {
		kind = ranking.Practice
	}
	c.ResourceKey = ranking.ResourceKey{Kind: kind, ID: c.key.ID, RevisionID: c.key.RevisionID}

	return c, nil
}

func readPinnedChecks(ctx context.Context, tx pgx.Tx, actor, goal string, topics []string, now time.Time) ([]studyCandidate, error) {
	rows, err := tx.Query(ctx, pinnedStudyChecksSQL, goal, actor, topics, now)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (studyCandidate, error) {
		c := studyCandidate{origin: Tasks}
		var projected bool
		if err := row.Scan(&c.TopicID, &c.key.ID, &c.key.RevisionID, &c.Minutes, &c.ReviewExpiresAt,
			&projected, &c.Difficulty, &c.Languages); err != nil {
			return c, err
		}
		c.ResourceKey = ranking.ResourceKey{Kind: ranking.Practice, ID: c.key.ID, RevisionID: c.key.RevisionID}
		c.IndependentCheck, c.DueReview = true, true
		if !projected {
			c.Minutes = 0
		}

		return c, nil
	})
}
