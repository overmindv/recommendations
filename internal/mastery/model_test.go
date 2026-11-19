package mastery

import (
	"reflect"
	"slices"
	"testing"
	"time"
)

func samplePolicy() Policy {
	return Policy{
		Revision: "test", Algorithm: AlgorithmVersion,
		GapMaxBPS: 3000, DemonstratedBPS: 8000, MinWeightedTasks: 1,
	}
}

func TestBaselineIgnoresUnverifiedSignals(t *testing.T) {
	at := time.Unix(10, 0).UTC()
	results, used := summarize(samplePolicy(), []string{"a", "b"}, []evidence{
		{TaskID: "1", GradedAt: at, Verdict: "infrastructure_error", Topics: []string{"a"}},
		{TaskID: "2", GradedAt: at, Verdict: "self_reported", Topics: []string{"a"}},
		{TaskID: "3", GradedAt: at, Verdict: "wrong_answer", Topics: []string{"b"}},
	}, false)
	unknown, wrong := results[0], results[1]
	if unknown.State != "unknown" || unknown.Uncertainty != "no_evidence" ||
		unknown.ScoreBPS != nil || unknown.LastEvidenceAt != nil || unknown.SampleCount != 0 {
		t.Fatal("unverified signals changed the baseline:", unknown)
	}
	if wrong.ScoreBPS == nil || *wrong.ScoreBPS != 0 || wrong.State != "gap" ||
		wrong.SampleCount != 1 || len(used) != 1 || used[0].TaskID != "3" {
		t.Fatal("graded zero must differ from missing evidence:", wrong, used)
	}
}

func TestBaselineWeightsAndFirstAttempt(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	end := start.Add(time.Second)
	rows := []evidence{
		{TaskID: "1", TaskVersionID: "v2", SubmissionID: "z", GradedAt: end, Verdict: "wrong_answer", Topics: []string{"b"}},
		{TaskID: "2", SubmissionID: "c", GradedAt: end, Verdict: "wrong_answer", Topics: []string{"a"}},
		{TaskID: "1", TaskVersionID: "v1", SubmissionID: "b", GradedAt: start, Verdict: "wrong_answer", Topics: []string{"a"}},
		{TaskID: "1", TaskVersionID: "v1", SubmissionID: "a", GradedAt: start, Verdict: "accepted", Topics: []string{"a", "b"}, Weights: []int{3, 1}},
	}
	topics := []string{"a", "b"}
	results, used := summarize(samplePolicy(), topics, rows, false)
	a, b := results[0], results[1]
	if a.ScoreBPS == nil || *a.ScoreBPS != 4286 || a.State != "developing" || a.WeightMicros != 1750000 ||
		a.SampleCount != 2 || a.LastEvidenceAt == nil || !a.LastEvidenceAt.Equal(end) {
		t.Fatal("unexpected weighted baseline:", a)
	}
	if b.ScoreBPS == nil || *b.ScoreBPS != 10000 || b.WeightMicros != 250000 || b.SampleCount != 1 ||
		b.State != "insufficient" || b.Uncertainty != "limited_sample" || len(used) != 2 || used[0].SubmissionID != "a" {
		t.Fatal("a retry replaced the first grade or a partial weight counted as a full task:", b, used)
	}
	slices.Reverse(rows)
	reversed, again := summarize(samplePolicy(), topics, rows, false)
	if !reflect.DeepEqual(results, reversed) || !reflect.DeepEqual(used, again) {
		t.Fatal("input order changed the result")
	}
	onlyA, _ := summarize(samplePolicy(), topics[:1], rows, false)
	if !reflect.DeepEqual(a, onlyA[0]) {
		t.Fatal("weight changed when a topic was removed from the goal")
	}
}

func TestBaselineTruncation(t *testing.T) {
	results, _ := summarize(samplePolicy(), []string{"a", "b"}, []evidence{
		{TaskID: "1", GradedAt: time.Unix(10, 0), Verdict: "accepted", Topics: []string{"a"}},
	}, true)
	if results[0].State != "insufficient" || results[1].State != "unknown" ||
		results[0].Uncertainty != "bounded_history" || results[1].Uncertainty != "bounded_history" {
		t.Fatal("truncated history must not be treated as complete:", results)
	}
}

func TestSharesConserveWeight(t *testing.T) {
	row := evidence{Topics: []string{"a", "b", "c"}}
	if got := shares(row); !slices.Equal(got, []int64{333334, 333333, 333333}) {
		t.Fatal("unexpected uniform shares:", got)
	}
	row.Weights = []int{1, 2, 10000}
	var sum int64
	for _, share := range shares(row) {
		sum += share
	}
	if sum != weightUnit {
		t.Fatal("combined weight is not one task:", sum)
	}
}
