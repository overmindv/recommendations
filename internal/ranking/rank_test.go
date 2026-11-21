package ranking

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
)

func samplePolicy(weights Factors) Policy {
	return Policy{
		Version: "test-r1", TargetBPS: 8000, UnknownGapBPS: 3000, PrerequisiteMinBPS: 7500,
		MediumDifficultyBPS: 5500, HardDifficultyBPS: 8500, Weights: weights,
	}
}

func sampleCandidate(kind Kind, id, topic, difficulty string, minutes int) Candidate {
	return Candidate{
		ResourceKey: ResourceKey{Kind: kind, ID: id, RevisionID: "v1"},
		TopicID:     topic, Minutes: minutes, Difficulty: difficulty, Available: true,
	}
}

func TestRankRecordsPolicyAndPoints(t *testing.T) {
	t.Parallel()

	policy := samplePolicy(Factors{Gap: 2, Difficulty: 3, Duration: 4, Variety: 5, DueReview: 6})
	practice := sampleCandidate(Practice, "task", "a", difficultyMedium, 5)
	practice.DueReview = true
	theory := sampleCandidate(Theory, "note", "b", "", 10)
	theory.DueReview = true
	request := Request{
		BudgetMinutes: 20, PreviousKind: Theory, Limit: 2,
		Candidates: []Candidate{theory, practice},
		Topics: map[string]Topic{
			"a": {Knowledge: &Knowledge{ScoreBPS: 6000, Sufficient: true}},
			"b": {Knowledge: &Knowledge{ScoreBPS: 10000}},
		},
	}
	got, err := Rank(request, policy)
	if err != nil || got.Policy != policy || got.Algorithm != AlgorithmVersion || len(got.Items) != 2 {
		t.Fatal("expected versioned ranking:", got, err)
	}
	want := Factors{Gap: 4000, Difficulty: 30000, Duration: 30000, Variety: 50000, DueReview: 60000}
	if got.Items[0].ResourceKey != practice.ResourceKey || got.Items[0].Components != want || got.Items[0].Score != 174000 {
		t.Fatal("unexpected weighted points:", got.Items[0])
	}
	note := got.Items[1]
	if note.ResourceKey != theory.ResourceKey || note.Components.Gap != 6000 ||
		note.Components.Difficulty != 15000 || note.Components.Variety != 0 || note.Components.DueReview != 0 {
		t.Fatal("an insufficient sample was treated as proven knowledge or theory got a review bonus:", note)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var replay Result
	if err := json.Unmarshal(raw, &replay); err != nil || !reflect.DeepEqual(replay, got) {
		t.Fatal("serialized ranking lost its explanation:", replay, err)
	}
}

func TestRankEqualScoresAndManyTopics(t *testing.T) {
	t.Parallel()

	policy := samplePolicy(Factors{Duration: 1})
	request := Request{
		Topics: map[string]Topic{"a": {}, "b": {}}, Limit: 3, BudgetMinutes: 5,
		Candidates: []Candidate{
			sampleCandidate(Theory, "a", "a", "", 5),
			sampleCandidate(Practice, "b", "a", "easy", 5),
			sampleCandidate(Practice, "a", "b", "easy", 5),
			sampleCandidate(Practice, "a", "a", "easy", 5),
			sampleCandidate(Theory, "z", "b", "", 5),
		},
	}
	first, err := Rank(request, policy)
	if err != nil || len(first.Items) != 3 {
		t.Fatal("expected three results:", first, err)
	}
	if first.Items[0].Kind != Practice || first.Items[0].ID != "a" || first.Items[0].TopicID != "a" ||
		first.Items[1].ID != "b" || first.Items[2].Kind != Theory || first.Items[2].ID != "a" {
		t.Fatal("unstable key order or duplicate resource:", first.Items)
	}
	for range len(request.Candidates) {
		request.Candidates = append(request.Candidates[1:], request.Candidates[0])
		again, rankErr := Rank(request, policy)
		if rankErr != nil || !reflect.DeepEqual(again, first) {
			t.Fatal("input order changed ranking:", again, rankErr)
		}
	}

	policy.Weights = Factors{Gap: 1}
	request.Topics["a"] = Topic{Knowledge: &Knowledge{ScoreBPS: 7500, Sufficient: true}}
	request.Topics["b"] = Topic{Knowledge: &Knowledge{ScoreBPS: 1500, Sufficient: true}}
	result, err := Rank(request, policy)
	if err != nil || result.Items[0].Kind != Practice || result.Items[0].ID != "a" || result.Items[0].TopicID != "b" {
		t.Fatal("did not keep the highest-scored topic for the same resource:", result, err)
	}
}

func TestRankExtremeDifficulty(t *testing.T) {
	t.Parallel()

	const mediumTask = "mid"
	policy := samplePolicy(Factors{Difficulty: 1})
	request := Request{
		Limit: 3, BudgetMinutes: 10, Topics: map[string]Topic{"a": {}},
		Candidates: []Candidate{
			sampleCandidate(Practice, "hard", "a", "hard", 10),
			sampleCandidate(Practice, "easy", "a", "easy", 10),
			sampleCandidate(Practice, mediumTask, "a", difficultyMedium, 10),
		},
	}
	for _, tc := range []struct {
		knowledge *Knowledge
		want      []string
	}{
		{nil, []string{"easy", mediumTask, "hard"}},
		{&Knowledge{ScoreBPS: 8500, Sufficient: true}, []string{"hard", mediumTask, "easy"}},
	} {
		request.Topics["a"] = Topic{Knowledge: tc.knowledge}
		got, err := Rank(request, policy)
		if err != nil || len(got.Items) != 3 {
			t.Fatal("expected all difficulty levels:", got, err)
		}
		for i, want := range tc.want {
			if got.Items[i].ID != want || got.Items[i].Components.Difficulty != (2-i)*5000 {
				t.Fatal("unexpected difficulty fit:", got.Items)
			}
		}
	}
}

func TestRankAllFiltered(t *testing.T) {
	t.Parallel()

	blocked := sampleCandidate(Practice, "task", "advanced", "hard", 1)
	blocked.DueReview = true
	result, err := Rank(Request{
		Candidates: []Candidate{blocked},
		Topics:     map[string]Topic{"advanced": {Prerequisites: []string{"unverified"}}},
		Limit:      1, BudgetMinutes: 30,
	}, samplePolicy(Factors{DueReview: 1}))
	if err != nil || result.Items == nil || len(result.Items) != 0 || result.Filters.Prerequisites != 1 {
		t.Fatal("a due review bypassed the prerequisites filter:", result, err)
	}
}

func TestRankRejectsConflictingCatalogOrPolicy(t *testing.T) {
	t.Parallel()

	candidate := sampleCandidate(Practice, "task", "a", "easy", 5)
	withRevision := candidate
	withRevision.RevisionID = "v2"
	withUnknownDifficulty := candidate
	withUnknownDifficulty.Difficulty = "extreme"
	policy := samplePolicy(Factors{Gap: 1})
	request := Request{Topics: map[string]Topic{"a": {}}, Limit: 1, BudgetMinutes: 5}
	for _, rows := range [][]Candidate{
		{candidate, candidate}, {candidate, withRevision}, {withUnknownDifficulty}, make([]Candidate, MaxCandidates+1),
	} {
		request.Candidates = rows
		if _, err := Rank(request, policy); !errors.Is(err, ErrInvalid) {
			t.Fatal("accepted invalid catalog input:", err)
		}
	}
	request.Candidates = []Candidate{candidate}
	for _, weights := range []Factors{{}, {Gap: -1}, {Gap: 1001}} {
		policy.Weights = weights
		if _, err := Rank(request, policy); !errors.Is(err, ErrInvalid) {
			t.Fatal("accepted invalid weights:", weights, err)
		}
	}
}

func TestRankRejectsUnboundedInputs(t *testing.T) {
	t.Parallel()

	policy := samplePolicy(Factors{Gap: 1000, Difficulty: 1000, Duration: 1000, Variety: 1000, DueReview: 1000})
	candidate := sampleCandidate(Practice, "task", "a", "easy", 1)
	candidate.DueReview = true
	request := Request{
		Candidates: []Candidate{candidate}, Topics: map[string]Topic{"a": {Knowledge: &Knowledge{Sufficient: true}}},
		Limit: 1, BudgetMinutes: MaxMinutes, PreviousKind: Theory,
	}
	got, err := Rank(request, policy)
	if err != nil || len(got.Items) != 1 || got.Items[0].Score != 47_999_000 {
		t.Fatal("unexpected score near the limits:", got, err)
	}
	for _, change := range []func(*Request){
		func(r *Request) { r.Limit = MaxResult + 1 },
		func(r *Request) { r.BudgetMinutes++ },
		func(r *Request) { r.PreviousKind = "unsupported" },
		func(r *Request) { r.AllowedLanguages = make([]string, 17) },
		func(r *Request) { r.Topics = map[string]Topic{"a": {Prerequisites: make([]string, MaxTopics+1)}} },
	} {
		invalid := request
		change(&invalid)
		if _, err := Rank(invalid, policy); !errors.Is(err, ErrInvalid) {
			t.Fatal("accepted invalid request:", err)
		}
	}
}

func BenchmarkRanking(b *testing.B) {
	request := Request{Topics: make(map[string]Topic, MaxTopics), BudgetMinutes: 20, PreviousKind: Theory, Limit: 20}
	topics := make([]string, MaxTopics)
	for i := range topics {
		id := fmt.Sprintf("topic-%02d", i)
		topics[i] = id
		request.Topics[id] = Topic{Knowledge: &Knowledge{ScoreBPS: i * 150, Sufficient: i%3 != 0}}
	}
	request.Candidates = make([]Candidate, 0, MaxCandidates)
	for i := range MaxCandidates {
		kind, difficulty := Theory, ""
		if i%2 != 0 {
			kind, difficulty = Practice, difficultyMedium
		}
		c := sampleCandidate(kind, fmt.Sprintf("resource-%04d", i/2), topics[i%len(topics)], difficulty, i%24+1)
		c.Recent, c.DueReview = i%11 == 0, i%17 == 0
		request.Candidates = append(request.Candidates, c)
	}
	policy := samplePolicy(Factors{Gap: 5, Difficulty: 3, Duration: 1, Variety: 2, DueReview: 4})
	b.Run("filters", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rows, _, err := filterCandidates(request, policy.PrerequisiteMinBPS)
			if err != nil || len(rows) < request.Limit {
				b.Fatal("unexpected candidate window:", len(rows), err)
			}
		}
	})
	b.Run("rank_top_20", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			out, err := Rank(request, policy)
			if err != nil || len(out.Items) != request.Limit ||
				!slices.IsSortedFunc(out.Items, func(a, b Ranked) int { return cmp.Compare(b.Score, a.Score) }) {
				b.Fatal("unexpected ranked window:", len(out.Items), err)
			}
		}
	})
}
