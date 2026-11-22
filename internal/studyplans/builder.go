package studyplans

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/projection/catalog"
	"github.com/overmindv/recommendations/internal/ranking"
)

type ResourceSource interface {
	StudyResources(context.Context, string, string, []string) ([]planner.StudyResource, error)
}

type ProjectedBuilder struct {
	pool             *pgxpool.Pool
	resources        ResourceSource
	ranking          ranking.Policy
	evidence         mastery.Policy
	maxProjectionAge time.Duration
}

func NewProjectedBuilder(pool *pgxpool.Pool, resources ResourceSource, rankingPolicy ranking.Policy, evidencePolicy mastery.Policy) *ProjectedBuilder {
	return &ProjectedBuilder{pool: pool, resources: resources, ranking: rankingPolicy, evidence: evidencePolicy}
}

func (b *ProjectedBuilder) StudyAlternativeAvailable(ctx context.Context, key ranking.ResourceKey) bool {
	owner, ok := b.resources.(studyAlternativeSource)

	return ok && owner.StudyAlternativeAvailable(ctx, key)
}

func (b ProjectedBuilder) WithProjectionFreshness(maxAge time.Duration) *ProjectedBuilder {
	b.maxProjectionAge = maxAge

	return &b
}

func (b *ProjectedBuilder) Build(ctx context.Context, actor, goal string, source Sources) (Draft, error) {
	input, now, evidence, err := b.inputs(ctx, actor, goal, source, b.ranking, b.evidence)
	if err != nil {
		return Draft{}, err
	}
	plan, err := planner.BuildLearningPlan(ctx, now, input)
	if err != nil {
		return Draft{}, err
	}

	return Draft{Plan: plan, EvidenceWatermark: evidence.Watermark, EvidencePolicy: b.evidence,
		ScopeTopics: slices.Sorted(maps.Keys(input.Topics)), EvidenceTopics: evidence.Topics, EvidenceCaptured: now,
		Guidance: new(studyGuidance(source.TemplateVersion, evidence.Topics))}, nil
}

func (b *ProjectedBuilder) inputs(ctx context.Context, actor, goal string, source Sources, rank ranking.Policy,
	policy mastery.Policy) (planner.LearningRequest, time.Time, mastery.PlanningEvidence, error) {
	var topics map[string]ranking.Topic
	var ids []string
	var evidence mastery.PlanningEvidence
	var now time.Time
	err := pgx.BeginTxFunc(ctx, b.pool, readOnly, func(tx pgx.Tx) error {
		if b.maxProjectionAge > 0 {
			freshness, err := readFreshness(ctx, tx, b.maxProjectionAge)
			if err != nil {
				return err
			}
			if freshness.Status != freshCatalog {
				return ErrProjectionStale
			}
		}
		if err := tx.QueryRow(ctx, "SELECT statement_timestamp()").Scan(&now); err != nil {
			return err
		}
		var err error
		topics, ids, err = catalog.ReadStudyScope(ctx, tx, source.TargetTopics)
		if err != nil {
			return err
		}
		evidence, err = mastery.ReadPlanningEvidence(ctx, tx, actor, ids, policy, now)

		return err
	})
	if err != nil {
		return planner.LearningRequest{}, time.Time{}, mastery.PlanningEvidence{}, err
	}
	for _, score := range evidence.Topics {
		if score.ScoreBPS == nil {
			continue
		}
		topic := topics[score.ID]
		topic.Knowledge = &ranking.Knowledge{ScoreBPS: *score.ScoreBPS, Sufficient: score.Uncertainty == sufficientEvidence}
		topics[score.ID] = topic
	}
	resources, err := b.resources.StudyResources(ctx, actor, goal, ids)
	if err != nil {
		return planner.LearningRequest{}, time.Time{}, mastery.PlanningEvidence{}, err
	}
	input := planner.LearningRequest{
		TargetTopicIDs: source.TargetTopics, Topics: topics, Resources: resources, AllowedLanguages: source.AllowedLanguages,
		Budget: source.Budget, Ranking: rank,
	}
	if source.feedbackRevision > 0 {
		if err := applyResourceFeedback(ctx, b.pool, goal, ids, &input, now); err != nil {
			return planner.LearningRequest{}, time.Time{}, mastery.PlanningEvidence{}, err
		}
	}

	return input, now, evidence, nil
}
