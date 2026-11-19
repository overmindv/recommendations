package evaluation

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

type Report struct {
	Schema      int          `json:"schema_version"`
	Planner     string       `json:"planner"`
	Ranking     string       `json:"ranking_algorithm"`
	Policy      string       `json:"ranking_policy"`
	HoldoutFrom time.Time    `json:"holdout_from"`
	Passed      bool         `json:"passed"`
	ExpertRated int          `json:"expert_rated_cases"`
	Development split        `json:"development"`
	Holdout     split        `json:"holdout"`
	Cases       []caseReport `json:"cases"`
}

type split struct {
	Cases    int    `json:"cases"`
	Passed   int    `json:"passed"`
	Adaptive totals `json:"adaptive"`
	Static   totals `json:"static"`
}

type totals struct {
	Targets          int `json:"targets"`
	Covered          int `json:"covered"`
	AvailableMinutes int `json:"available_minutes"`
	PlannedMinutes   int `json:"planned_minutes"`
	Violations       int `json:"violations"`
}

type caseReport struct {
	ID           string        `json:"id"`
	AsOf         time.Time     `json:"as_of"`
	Split        string        `json:"split"`
	Adaptive     planStats     `json:"adaptive"`
	Static       planStats     `json:"static"`
	Regressions  []violation   `json:"regressions"`
	ExpertReview *expertReview `json:"expert_review,omitempty"`
}

func Evaluate(ctx context.Context, r io.Reader) (Report, error) {
	d, err := readDataset(r)
	if err != nil {
		return Report{}, err
	}
	c, err := d.validate()
	if err != nil {
		return Report{}, err
	}
	slices.SortFunc(d.Cases, func(a, b scenario) int { return cmp.Or(a.AsOf.Compare(b.AsOf), cmp.Compare(a.ID, b.ID)) })
	report := Report{Schema: 1, Planner: planner.LearningPlanVersion, Ranking: ranking.AlgorithmVersion,
		Policy: d.Policy.Version, HoldoutFrom: d.HoldoutFrom.UTC(), Passed: true, Cases: make([]caseReport, 0, len(d.Cases))}
	for _, row := range d.Cases {
		result, err := evaluateCase(ctx, d, c, row)
		if err != nil {
			return Report{}, fmt.Errorf("case %s: %w", row.ID, err)
		}
		group := &report.Holdout
		if result.Split == "development" {
			group = &report.Development
		}
		group.Cases++
		group.Adaptive.add(result.Adaptive)
		group.Static.add(result.Static)
		if len(result.Regressions)+len(result.Adaptive.Violations)+len(result.Static.Violations) == 0 {
			group.Passed++
		} else {
			report.Passed = false
		}
		if result.ExpertReview != nil {
			report.ExpertRated++
		}
		report.Cases = append(report.Cases, result)
	}
	return report, nil
}

func evaluateCase(ctx context.Context, d dataset, c catalog, row scenario) (caseReport, error) {
	s, err := asOf(d, c, row)
	if err != nil {
		return caseReport{}, err
	}
	adaptive, err := planner.BuildLearningPlan(ctx, row.AsOf, s.request)
	if err != nil {
		return caseReport{}, err
	}
	fixed, err := staticPlan(ctx, s, d.Curriculum)
	if err != nil {
		return caseReport{}, err
	}
	result := caseReport{
		ID: row.ID, AsOf: row.AsOf.UTC(), Split: "holdout",
		Adaptive: auditPlan(s, adaptive), Static: auditPlan(s, fixed), ExpertReview: row.ExpertReview,
	}
	if row.AsOf.Before(d.HoldoutFrom) {
		result.Split = "development"
	}
	result.Regressions = checkExpectations(result.Adaptive, row.Expect)
	return result, nil
}

func (t *totals) add(s planStats) {
	t.Targets += s.Targets
	t.Covered += s.Covered
	t.AvailableMinutes += s.AvailableMinutes
	t.PlannedMinutes += s.PlannedMinutes
	t.Violations += len(s.Violations)
}
