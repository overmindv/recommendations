package explanation

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

func TestForStepDistinguishesGapFromMissingEvidence(t *testing.T) {
	const topic = "abcdef00-1234-4000-8000-000000000001"
	item := planner.StudyItem{Ranked: ranking.Ranked{TopicID: topic, Minutes: 20}, Stage: planner.StudyTheory}
	item.Decision = &planner.StudyDecision{RequiredBPS: 8000}
	evidence := &mastery.Topic{ID: topic, ScoreBPS: new(2400), SampleCount: 1,
		LastEvidenceAt: new(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)), Uncertainty: "limited_sample"}
	for _, tc := range []struct {
		name     string
		evidence *mastery.Topic
		reasons  []Reason
	}{
		{"missing", nil, []Reason{{Code: LimitedEvidence, TopicID: topic}, {Code: TimeFit, Minutes: 20}}},
		{"limited", evidence, []Reason{{Code: LimitedEvidence, TopicID: topic}, {Code: TimeFit, Minutes: 20}}},
		{"sufficient", &mastery.Topic{ID: topic, ScoreBPS: evidence.ScoreBPS, SampleCount: 4,
			LastEvidenceAt: evidence.LastEvidenceAt, Uncertainty: "sufficient_for_policy"},
			[]Reason{{Code: TopicGap, TopicID: topic}, {Code: TimeFit, Minutes: 20}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := ForStep(item, tc.evidence)
			if !slices.EqualFunc(result.Reasons, tc.reasons, sameReason) {
				t.Fatal("unexpected reasons:", result.Reasons)
			}
			if tc.evidence == nil && (result.Evidence.SampleCount != 0 || result.Evidence.ScoreBPS != nil) {
				t.Fatal("missing evidence produced a score")
			}
		})
	}
}

func TestForStepOnlyReturnsTypedReasonsAndAlternative(t *testing.T) {
	const (
		topic = "abcdef00-1234-4000-8000-000000000001"
		guard = "abcdef00-1234-4000-8000-000000000002"
		task  = "abcdef00-1234-4000-8000-000000000003"
		alt   = "abcdef00-1234-4000-8000-000000000004"
		rev   = "abcdef00-1234-4000-8000-000000000005"
	)
	item := planner.StudyItem{
		Ranked: ranking.Ranked{TopicID: topic, ResourceKey: ranking.ResourceKey{Kind: ranking.Practice, ID: task}},
		Stage:  planner.StudyPractice, RequiresCheck: []string{guard, "foreign data", guard},
	}
	input := planner.StudyAlternative{ResourceKey: ranking.ResourceKey{Kind: ranking.Practice, ID: alt, RevisionID: rev}, Minutes: 15}
	item.Decision = &planner.StudyDecision{RequiredBPS: 8000, ReviewDue: true, Alternative: &input}
	result := ForStep(item, &mastery.Topic{ID: guard, State: "foreign data"})
	want := []Reason{{Code: LimitedEvidence, TopicID: topic}, {Code: Prerequisite, TopicID: guard}}
	if !slices.EqualFunc(result.Reasons, want, sameReason) || result.Alternative == nil || *result.Alternative != input {
		t.Fatal("incorrect reasons or alternative:", result)
	}
	raw, err := json.Marshal(result)
	if err != nil || strings.Contains(string(raw), "foreign data") {
		t.Fatal("explanation did not stay within typed fields")
	}
	input.ID = task
	if got := ForStep(item, nil); got.Alternative != nil {
		t.Fatal("the recommended task cannot be its own alternative")
	}
	item.Stage = planner.StudyCheck
	if check := ForStep(item, nil); check.Alternative != nil || check.Reasons[1].Code != DueReview {
		t.Fatal("a due check exposed another task or omitted the review reason:", check)
	}
}

func sameReason(a, b Reason) bool {
	return a.Message != "" && a.Code == b.Code && a.TopicID == b.TopicID && a.Minutes == b.Minutes
}
