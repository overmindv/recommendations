package completion

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/internal/ranking"
)

const maxVerificationPlans = 64

type basis struct {
	goalVersion int64
	planVersion int64
	topics      []string
}

type reviewPlan struct {
	basis
	goal     string
	current  bool
	checksOK bool
	allMet   bool
	guards   []string
	rank     ranking.Policy
	evidence mastery.Policy
}

const reviewPlansSQL = `
	WITH goals AS MATERIALIZED (
	    SELECT id,target_topics FROM learner_goals WHERE user_id=$1 AND active
	), wanted AS (
	    SELECT g.id,g.target_topics,h.version FROM goals AS g JOIN study_plan_heads AS h ON h.goal_id=g.id
	    UNION
	    SELECT g.id,g.target_topics,s.plan_version FROM goals AS g
	    JOIN goal_completion_state AS s ON s.goal_id=g.id AND s.learner_id=$1 AND s.verified
	)
	SELECT v.goal_id::text,v.goal_version,v.version,v.target_topics,v.version=h.version,
	       v.ranking_policy,v.evidence_policy,
	       NOT EXISTS (
	           SELECT 1 FROM jsonb_array_elements(
	               CASE WHEN jsonb_typeof(v.calendar->'unmet_outcomes')='array'
	                   THEN v.calendar->'unmet_outcomes' ELSE '[]'::jsonb END
	           ) AS outcome(value)
	           WHERE outcome.value->>'topic_id'=ANY(v.target_topics::text[])
	       ),
	       ARRAY(
	           SELECT DISTINCT t.id::text FROM study_plan_items AS i
	           CROSS JOIN unnest(i.requires_checks) AS t(id)
	           WHERE i.goal_id=v.goal_id AND i.plan_version=v.version AND NOT i.superseded
	           ORDER BY t.id::text LIMIT 65
	       ),
	       NOT EXISTS (
	           SELECT 1 FROM study_plan_items AS i
	           WHERE i.goal_id=v.goal_id AND i.plan_version=v.version AND i.stage='independent_check' AND NOT i.superseded
	             AND NOT COALESCE((
	                 SELECT o.verdict='accepted' AND o.task_version_id=i.resource_version_id
	                    AND i.topic_id=ANY(o.topic_ids) AND o.graded_at>=COALESCE(i.evidence_since,v.created_at)
	                 FROM learning_task_outcomes AS o
	                 WHERE o.learner_id=$1 AND o.task_id=i.resource_id AND o.evidence_kind='independent' AND o.graded_at<=$2
	                 ORDER BY o.graded_at,o.submission_id LIMIT 1
	             ),false)
	       )
	FROM wanted AS w
	JOIN study_plan_versions AS v ON v.goal_id=w.id AND v.version=w.version
	JOIN study_plan_heads AS h ON h.goal_id=v.goal_id
	WHERE v.target_topics @> w.target_topics AND v.target_topics <@ w.target_topics
	ORDER BY v.goal_id,v.version LIMIT 65`

func loadPlans(ctx context.Context, tx pgx.Tx, learner string, now time.Time) ([]reviewPlan, error) {
	rows, err := tx.Query(ctx, reviewPlansSQL, learner, now)
	if err != nil {
		return nil, err
	}
	plans, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (reviewPlan, error) {
		var plan reviewPlan
		var rank, evidence []byte
		if err := row.Scan(&plan.goal, &plan.goalVersion, &plan.planVersion, &plan.topics, &plan.current,
			&rank, &evidence, &plan.allMet, &plan.guards, &plan.checksOK); err != nil {
			return reviewPlan{}, err
		}
		if err := json.Unmarshal(rank, &plan.rank); err != nil {
			return reviewPlan{}, err
		}
		if err := json.Unmarshal(evidence, &plan.evidence); err != nil {
			return reviewPlan{}, err
		}
		slices.Sort(plan.topics)
		return plan, nil
	})
	if len(plans) > maxVerificationPlans {
		return nil, errors.New("too many plans to verify for a learner")
	}
	return plans, err
}

func (p reviewPlan) requiredScores() map[string]int {
	if !p.allMet || !p.checksOK || !p.rank.Valid() || !p.evidence.Valid() ||
		len(p.topics) == 0 || len(p.guards) > ranking.MaxTopics {
		return nil
	}
	required := make(map[string]int, len(p.topics)+len(p.guards))
	for _, id := range p.topics {
		required[id] = p.rank.TargetBPS
	}
	for _, id := range p.guards {
		required[id] = max(required[id], p.rank.PrerequisiteMinBPS)
	}
	if len(required) > ranking.MaxTopics {
		return nil
	}
	return required
}

func (p reviewPlan) verified(evidence mastery.VerificationEvidence, required map[string]int) bool {
	topics := slices.Sorted(maps.Keys(required))
	for _, score := range evidence.Scores(p.evidence, topics) {
		if score.Uncertainty != "sufficient_for_policy" || score.ScoreBPS == nil || *score.ScoreBPS < required[score.ID] {
			return false
		}
	}
	return len(topics) != 0
}
