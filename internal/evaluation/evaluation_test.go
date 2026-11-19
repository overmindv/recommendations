package evaluation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

func fixture(t testing.TB) []byte {
	t.Helper()
	data, err := os.ReadFile("../../evaluation/algorithms.json")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func fixtureData(t testing.TB) (dataset, catalog) {
	t.Helper()
	d, err := readDataset(bytes.NewReader(fixture(t)))
	if err != nil {
		t.Fatal(err)
	}
	c, err := d.validate()
	if err != nil {
		t.Fatal(err)
	}
	return d, c
}

func (d dataset) caseNamed(t *testing.T, id string) scenario {
	t.Helper()
	for _, row := range d.Cases {
		if row.ID == id {
			return row
		}
	}
	t.Fatal("missing fixture", id)
	return scenario{}
}

func TestEvaluationRegressionFixtures(t *testing.T) {
	data := fixture(t)
	report, err := Evaluate(t.Context(), bytes.NewReader(data))
	if err != nil || !report.Passed || len(report.Cases) != 8 || report.ExpertRated != 0 {
		t.Fatal("evaluation expectations or human review status changed", report, err)
	}
	if report.Development.Cases != 3 || report.Holdout.Cases != 5 || report.Holdout.Passed != 5 ||
		report.Development.Adaptive.Covered != 2 || report.Development.Static.Covered != 2 ||
		report.Holdout.Adaptive.Covered != 3 || report.Holdout.Static.Covered != 1 {
		t.Fatal("split or coverage changed", report.Development, report.Holdout)
	}
	for _, row := range report.Cases {
		if len(row.Adaptive.Violations)+len(row.Static.Violations)+len(row.Regressions) > 0 {
			t.Fatal("unexpected plan failure", row)
		}
		if row.ID == "difficulty-feedback" && (row.Adaptive.DifficultyLevels != 2 || row.Static.DifficultyLevels != 1) {
			t.Fatal("difficulty feedback did not change practice variety", row)
		}
	}
	d, _ := fixtureData(t)
	slices.Reverse(d.Cases)
	slices.Reverse(d.Resources)
	encoded, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	shuffled, err := Evaluate(t.Context(), bytes.NewReader(encoded))
	if err != nil || !reflect.DeepEqual(report, shuffled) {
		t.Fatal("evaluation depends on file order", err)
	}
}

func TestEvaluationFiltersFutureAndLateFacts(t *testing.T) {
	d, c := fixtureData(t)
	row := d.caseNamed(t, "late-grades")
	before, err := evaluateCase(t.Context(), d, c, row)
	if err != nil {
		t.Fatal(err)
	}
	for i := range row.Observations {
		if row.Observations[i].AvailableAt.After(row.AsOf) {
			row.Observations[i].Score = 0
			row.Observations[i].Sufficient = false
		}
	}
	row.Activity = nil
	for i := range d.Resources {
		if d.Resources[i].PublishedAt.After(row.AsOf) {
			d.Resources[i].Minutes += 50
		}
	}
	after, err := evaluateCase(t.Context(), d, c, row)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("late evidence or future catalog changed the past", err)
	}
	old, err := asOf(d, c, d.caseNamed(t, "cold-start"))
	if err != nil || old.byName["arrays-note"].spec.Revision != "v1" {
		t.Fatal("earlier case lost the published revision", err)
	}
	holdout := d.caseNamed(t, "known-prerequisite")
	current, err := asOf(d, c, holdout)
	if err != nil || !current.known(scopedID("topic", "arrays"), d.Policy.PrerequisiteMinBPS) ||
		!holdout.AsOf.Equal(d.HoldoutFrom) {
		t.Fatal("evidence available at the cutoff was not read", err)
	}
	s, err := asOf(d, c, row)
	if err != nil || s.byName["arrays-note"].spec.Revision != "v2" ||
		s.byName["arrays-visual"].spec.ID != "" || s.byName["arrays-check-old"].spec.ID != "" {
		t.Fatal("incorrect resource version at the cutoff", err)
	}
}

func TestAuditRejectsForbiddenResourcesAndBrokenGraph(t *testing.T) {
	d, c := fixtureData(t)
	row := d.caseNamed(t, "cold-start")
	s, err := asOf(d, c, row)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := planner.BuildLearningPlan(t.Context(), row.AsOf, s.request)
	if err != nil || len(plan.Weeks) == 0 || len(plan.Weeks[0].Items) != 6 {
		t.Fatal("missing complete plan fixture", err)
	}
	items := plan.Weeks[0].Items
	items[0].RevisionID = scopedID("revision:arrays-note", "v2")
	items[1].Decision.Alternative = &planner.StudyAlternative{ResourceKey: items[2].ResourceKey, Minutes: items[2].Minutes}
	items[3].RequiresCheck = nil
	items[2], items[3] = items[3], items[2]
	stats := auditPlan(s, plan)
	for _, code := range []string{"prohibited_resource", "prohibited_alternative", "prerequisite_order", "prerequisite_gate", "stage_order"} {
		if !hasViolation(stats, code) {
			t.Fatal("audit missed", code, stats.Violations)
		}
	}
	deny := d.caseNamed(t, "no-permitted-check")
	blocked, err := asOf(d, c, deny)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"arrays-check", "arrays-check-go"} {
		entry := blocked.byName[name]
		bad := singleStepPlan(blocked, entry)
		if !hasViolation(auditPlan(blocked, bad), "prohibited_resource") {
			t.Fatal("audit accepted forbidden check", name)
		}
	}
}

func TestAuditUsesActualCalendarBudget(t *testing.T) {
	d, c := fixtureData(t)
	row := d.caseNamed(t, "tight-deadline")
	s, err := asOf(d, c, row)
	if err != nil {
		t.Fatal(err)
	}
	entry := s.byName["arrays-note"]
	plan := singleStepPlan(s, entry)
	plan.Weeks[0].CapacityMinutes = entry.item.Minutes
	stats := auditPlan(s, plan)
	if !hasViolation(stats, "budget_exceeded") || !hasViolation(stats, "calendar_mismatch") {
		t.Fatal("audit trusted the plan's inflated budget", stats.Violations)
	}
}

func singleStepPlan(s snapshot, entry candidate) planner.LearningPlan {
	plan := planner.LearningPlan{Timezone: s.calendar.Timezone, WeeklyMinutes: s.calendar.WeeklyMinutes}
	for _, week := range s.calendar.Weeks {
		plan.Weeks = append(plan.Weeks, planner.LearningWeek{StudyWeek: week})
	}
	plan.Weeks[0].PlannedMinutes = entry.item.Minutes
	plan.Weeks[0].Items = []planner.StudyItem{{
		Ranked: ranking.Ranked{ResourceKey: entry.item.ResourceKey, TopicID: entry.item.TopicID, Minutes: entry.item.Minutes},
		Stage:  entry.spec.Stage,
	}}
	return plan
}

func TestExpertReviewDoesNotChangePlans(t *testing.T) {
	d, c := fixtureData(t)
	row := d.caseNamed(t, "known-prerequisite")
	before, err := evaluateCase(t.Context(), d, c, row)
	if err != nil {
		t.Fatal(err)
	}
	row.ExpertReview = &expertReview{Rubric: "test", ReviewedAt: row.AsOf.Add(time.Hour), Adaptive: 1, Static: 5}
	if err := c.validateScenario(row); err != nil {
		t.Fatal(err)
	}
	after, err := evaluateCase(t.Context(), d, c, row)
	if err != nil || after.ExpertReview == nil || after.ExpertReview.Adaptive != 1 {
		t.Fatal("review was not reported separately", err)
	}
	after.ExpertReview = nil
	if !reflect.DeepEqual(before, after) {
		t.Fatal("review data reached the planner or mechanical gate")
	}
}

func TestEvaluationInvalidDatasetAndRegressions(t *testing.T) {
	for _, raw := range []string{
		strings.Repeat(" ", maxDatasetBytes+1),
		`{"schema_version":1,"learner_email":"not-allowed"}`,
		string(fixture(t)) + `{}`,
	} {
		if _, err := Evaluate(t.Context(), strings.NewReader(raw)); err == nil {
			t.Fatal("accepted oversized or malformed input")
		}
	}
	d, _ := fixtureData(t)
	d.Cases[0].Expect.Checks = nil
	d.Cases[0].Expect.Gaps = []gap{{Topic: "arrays", Reason: "missing_resource"}}
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Evaluate(t.Context(), bytes.NewReader(raw))
	if err != nil || report.Passed || len(report.Cases[0].Regressions) != 1 || report.Development.Passed != 2 {
		t.Fatal("an unexpected plan did not fail the gate", err)
	}
	d.Resources[0].WithdrawnAt = time.Time{}
	if _, err := d.validate(); err == nil {
		t.Fatal("overlapping resource revisions accepted")
	}
}

func TestEvaluationHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Evaluate(ctx, bytes.NewReader(fixture(t))); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled evaluation did not stop", err)
	}
}

func hasViolation(stats planStats, code string) bool {
	return slices.ContainsFunc(stats.Violations, func(row violation) bool { return row.Code == code })
}

func BenchmarkEvaluate(b *testing.B) {
	d, _ := fixtureData(b)
	for _, size := range []int{len(d.Cases), maxCases} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			data := d
			data.Cases = make([]scenario, size)
			for i := range data.Cases {
				row := d.Cases[i%len(d.Cases)]
				row.ID += "-" + strconv.Itoa(i)
				data.Cases[i] = row
			}
			raw, err := json.Marshal(data)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if report, err := Evaluate(b.Context(), bytes.NewReader(raw)); err != nil || !report.Passed {
					b.Fatal("benchmark evaluation failed", err)
				}
			}
		})
	}
}
