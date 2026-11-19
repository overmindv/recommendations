package planner

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/overmindv/recommendations/internal/ranking"
)

func studyID(n int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", n)
}

func learningPolicy() ranking.Policy {
	return ranking.Policy{
		Version: "test-v1", TargetBPS: 8000, UnknownGapBPS: 5000, PrerequisiteMinBPS: 7500,
		MediumDifficultyBPS: 5000, HardDifficultyBPS: 9000,
		Weights: ranking.Factors{Gap: 4, Difficulty: 3, Duration: 1, Variety: 2, DueReview: 6},
	}
}

func testStudyResource(id, topic int, stage StudyStage, minutes int) StudyResource {
	kind, difficulty := ranking.Practice, "easy"
	if stage == StudyTheory {
		kind, difficulty = ranking.Theory, ""
	}
	return StudyResource{Candidate: ranking.Candidate{
		ResourceKey: ranking.ResourceKey{Kind: kind, ID: studyID(id), RevisionID: studyID(10000 + id)},
		TopicID:     studyID(topic), Minutes: minutes, Available: true, Difficulty: difficulty,
	}, IndependentCheck: stage == StudyCheck}
}

func studyTriplet(topic, firstID, minutes int) []StudyResource {
	return []StudyResource{
		testStudyResource(firstID+2, topic, StudyCheck, minutes),
		testStudyResource(firstID, topic, StudyTheory, minutes),
		testStudyResource(firstID+1, topic, StudyPractice, minutes),
	}
}

func learningFixture() (time.Time, LearningRequest) {
	now := time.Date(2026, 11, 16, 8, 0, 0, 0, time.UTC)
	in := LearningRequest{
		TargetTopicIDs: []string{studyID(3)},
		Topics: map[string]ranking.Topic{
			studyID(1): {},
			studyID(2): {Prerequisites: []string{studyID(1)}},
			studyID(3): {Prerequisites: []string{studyID(2)}},
		},
		Budget:  StudyBudget{Timezone: "UTC", AvailableMinutes: 140, GoalMinutes: 100, ReservedMinutes: 70, MaxWeeks: 3},
		Ranking: learningPolicy(),
	}
	for n := 1; n <= 3; n++ {
		in.Resources = append(in.Resources, studyTriplet(n, n*10, 10)...)
	}
	return now, in
}

func TestLearningPlanOrdersStagesAndRequiresLiveEvidence(t *testing.T) {
	now, in := learningFixture()
	plan, err := BuildLearningPlan(t.Context(), now, in)
	if err != nil || len(plan.UnmetOutcomes) != 0 || len(plan.Weeks) != 3 {
		t.Fatal("full plan should fit:", plan, err)
	}
	var all []StudyItem
	for i, minutes := range []int{70, 20, 0} {
		w := plan.Weeks[i]
		if w.PlannedMinutes != minutes || w.PlannedMinutes > w.CapacityMinutes {
			t.Fatal("unexpected weekly schedule:", w)
		}
		all = append(all, w.Items...)
	}
	if len(all) != 9 || plan.RankingPolicy != in.Ranking || plan.RankingAlgorithm != ranking.AlgorithmVersion {
		t.Fatal("missing study stages or ranking policy:", plan)
	}
	for i, item := range all {
		topic := i/3 + 1
		if item.TopicID != studyID(topic) || item.Stage != []StudyStage{StudyTheory, StudyPractice, StudyCheck}[i%3] {
			t.Fatal("unexpected topic or stage order:", item)
		}
		var guards []string
		for pre := 1; pre < topic; pre++ {
			guards = append(guards, studyID(pre))
		}
		if !slices.Equal(item.RequiresCheck, guards) || item.Score == 0 {
			t.Fatal("unverified prerequisites were treated as known or explanation was discarded:", item)
		}
	}
	slices.Reverse(in.Resources)
	again, err := BuildLearningPlan(t.Context(), now, in)
	if err != nil || !reflect.DeepEqual(again, plan) {
		t.Fatal("resource order changed the plan:", again, err)
	}
}

func TestLearningPlanSuggestsOnlyVerifiedDeadline(t *testing.T) {
	now, in := learningFixture()
	in.Budget.Deadline = "2026-11-22"
	in.TargetTopicIDs = []string{studyID(3), studyID(1)}
	plan, err := BuildLearningPlan(t.Context(), now, in)
	if err != nil || len(plan.Weeks) != 1 || plan.Weeks[0].PlannedMinutes != 60 ||
		len(plan.UnmetOutcomes) != 1 || plan.UnmetOutcomes[0].TopicID != studyID(3) ||
		plan.UnmetOutcomes[0].Reason != "time_budget" {
		t.Fatal("partial topic was promised before deadline:", plan, err)
	}
	if len(plan.Suggestions) != 2 || plan.Suggestions[0].Kind != "reduce_scope" ||
		!slices.Equal(plan.Suggestions[0].TopicIDs, []string{studyID(1)}) ||
		plan.Suggestions[1].Kind != "extend_deadline" || plan.Suggestions[1].Deadline != "2026-11-29" {
		t.Fatal("expected a reduced scope and a later schedule:", plan.Suggestions)
	}
	in.Budget.Deadline = plan.Suggestions[1].Deadline
	extended, err := BuildLearningPlan(t.Context(), now, in)
	if err != nil || len(extended.UnmetOutcomes) != 0 {
		t.Fatal("the suggested deadline does not fit:", extended, err)
	}
}

func TestLearningPlanScarceContentDoesNotUnlockDependentTopics(t *testing.T) {
	now, in := learningFixture()
	in.Resources = slices.DeleteFunc(in.Resources, func(r StudyResource) bool {
		return r.TopicID == studyID(1) && r.IndependentCheck
	})
	plan, err := BuildLearningPlan(t.Context(), now, in)
	if err != nil || len(plan.UnmetOutcomes) != 3 || plan.Weeks[0].PlannedMinutes != 0 {
		t.Fatal("an incomplete prerequisite used time or unlocked the graph:", plan, err)
	}
	if missing := plan.UnmetOutcomes[0]; missing.Stage != StudyCheck || missing.Reason != "missing_resource" {
		t.Fatal("missing independent check was not reported:", missing)
	}
	for i, outcome := range plan.UnmetOutcomes[1:] {
		if outcome.Reason != "prerequisite" || !slices.Equal(outcome.BlockedBy, []string{studyID(i + 1)}) {
			t.Fatal("missing blocker:", outcome)
		}
	}
	if slices.ContainsFunc(plan.Suggestions, func(s ScopeSuggestion) bool { return s.Kind == "extend_deadline" }) {
		t.Fatal("more time was suggested for missing content")
	}
}

func TestLearningPlanDoesNotUsePlannedPracticeAsAnIndependentCheck(t *testing.T) {
	now, in := learningFixture()
	in.TargetTopicIDs = []string{studyID(2)}
	for i := range in.Resources {
		row := &in.Resources[i]
		if row.TopicID == studyID(2) && row.IndependentCheck {
			row.ID, row.RevisionID = studyID(11), studyID(10011)
		}
	}
	got, err := BuildLearningPlan(t.Context(), now, in)
	if err != nil || len(got.UnmetOutcomes) != 1 || got.UnmetOutcomes[0].Stage != StudyCheck ||
		got.UnmetOutcomes[0].TopicID != studyID(2) || len(got.Weeks[0].Items) != 3 {
		t.Fatal("plan reused a resource for an independent check or took time from a finished prerequisite:", got, err)
	}
}

func TestLearningPlanHonorsDelayedCheckExpiry(t *testing.T) {
	now, in := learningFixture()
	in.TargetTopicIDs = []string{studyID(1)}
	in.Topics[studyID(1)] = ranking.Topic{Knowledge: &ranking.Knowledge{ScoreBPS: 9000, Sufficient: true}}
	due := testStudyResource(90, 1, StudyCheck, 10)
	due.DueReview = true
	due.ReviewExpiresAt = now.AddDate(0, 0, 7)
	in.Resources = append(in.Resources, due)
	plan, err := BuildLearningPlan(t.Context(), now, in)
	if err != nil || len(plan.UnmetOutcomes) != 0 || len(plan.Weeks[0].Items) != 1 ||
		plan.Weeks[0].Items[0].ID != due.ID || plan.Weeks[0].Items[0].Stage != StudyCheck {
		t.Fatal("assigned retention was replaced or prior mastery was reset:", plan, err)
	}
	in.Resources[len(in.Resources)-1].ReviewExpiresAt = now.AddDate(0, 0, 3)
	expired, err := BuildLearningPlan(t.Context(), now, in)
	if err != nil || len(expired.UnmetOutcomes) != 1 || expired.Weeks[0].PlannedMinutes != 0 {
		t.Fatal("review was scheduled past its safe time range:", expired, err)
	}
}

func TestLearningPlanRejectsIncompleteOrCyclicGraph(t *testing.T) {
	now, in := learningFixture()
	for _, bad := range []string{studyID(3), studyID(100)} {
		in.Topics[studyID(1)] = ranking.Topic{Prerequisites: []string{bad}}
		if _, err := BuildLearningPlan(t.Context(), now, in); !errors.Is(err, ErrInvalidInput) {
			t.Fatal("unexpected validation of incomplete or cyclic graph:", err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := BuildLearningPlan(ctx, now, in); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled plan ran:", err)
	}
}

func BenchmarkBuildLearningPlan(b *testing.B) {
	now, request := learningFixture()
	request.TargetTopicIDs = make([]string, 0, 16)
	request.Topics = make(map[string]ranking.Topic)
	request.Resources = nil
	request.Budget.MaxWeeks = MaxLearningWeeks
	for root := 1; root <= 16; root++ {
		id := studyID(root)
		request.TargetTopicIDs = append(request.TargetTopicIDs, id)
		request.Topics[id] = ranking.Topic{Prerequisites: []string{studyID(16 + root*3 - 2), studyID(16 + root*3 - 1), studyID(16 + root*3)}}
	}
	for topic := 1; topic <= 64; topic++ {
		if _, ok := request.Topics[studyID(topic)]; !ok {
			request.Topics[studyID(topic)] = ranking.Topic{}
		}
		for i := range 10 {
			request.Resources = append(request.Resources, studyTriplet(topic, topic*100+i*3, 20)...)
		}
	}
	for _, tc := range []struct {
		name    string
		minutes int
		placed  bool
	}{{"weekly_fit", 480, true}, {"weekly_no_fit", 5, false}} {
		b.Run(tc.name, func(b *testing.B) {
			in := request
			in.Budget.AvailableMinutes, in.Budget.GoalMinutes, in.Budget.ReservedMinutes = tc.minutes, tc.minutes, 0
			b.ReportAllocs()
			for b.Loop() {
				plan, err := BuildLearningPlan(b.Context(), now, in)
				if err != nil || (plan.Weeks[0].PlannedMinutes > 0) != tc.placed {
					b.Fatal("invalid benchmark plan:", err)
				}
				if tc.placed && !slices.ContainsFunc(plan.UnmetOutcomes, func(outcome UnmetOutcome) bool {
					return outcome.Reason == "plan_limit"
				}) {
					b.Fatal("benchmark did not hit the item limit")
				}
			}
		})
	}
}
