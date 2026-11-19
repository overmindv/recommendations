package ranking

import (
	"slices"
	"testing"
)

func TestFilterCandidatesRequiresVerifiedPrerequisites(t *testing.T) {
	topics := map[string]Topic{
		"intro":    {Knowledge: &Knowledge{ScoreBPS: 3000, Sufficient: true}},
		"advanced": {Prerequisites: []string{"intro"}},
		"unknown":  {Prerequisites: []string{"missing"}},
	}
	candidate := Candidate{ResourceKey: ResourceKey{Kind: Practice, ID: "task", RevisionID: "v1"},
		TopicID: "advanced", Minutes: 5, Available: true}
	input := Request{Topics: topics, Candidates: []Candidate{candidate}, BudgetMinutes: 10}
	for _, score := range []*Knowledge{{ScoreBPS: 3000, Sufficient: true}, {ScoreBPS: 10000}, nil} {
		input.Topics["intro"] = Topic{Knowledge: score}
		got, filters, err := filterCandidates(input, 8000)
		if err != nil || len(got) != 0 || filters.Prerequisites != 1 {
			t.Fatal("weak or uncertain prerequisite allowed a resource:", got, filters, err)
		}
	}
	input.Topics["intro"] = Topic{Knowledge: &Knowledge{ScoreBPS: 8000, Sufficient: true}}
	got, filters, err := filterCandidates(input, 8000)
	if err != nil || len(got) != 1 || input.Candidates[got[0]].ResourceKey != candidate.ResourceKey || filters != (Filters{}) {
		t.Fatal("sufficient evidence was rejected:", got, filters, err)
	}
}

func TestFilterCandidatesRejectsUnavailableOrUnusableResources(t *testing.T) {
	input := Request{BudgetMinutes: 10, Topics: map[string]Topic{"topic": {}}, AllowedLanguages: []string{"python"}}
	short := Candidate{ResourceKey: ResourceKey{Kind: Practice, ID: "task", RevisionID: "v1"},
		TopicID: "topic", Minutes: 3, Available: true}
	input.Candidates = []Candidate{
		{ResourceKey: short.ResourceKey, TopicID: "outside", Available: true, Minutes: 1},
		{ResourceKey: short.ResourceKey, TopicID: "topic", Available: false, Minutes: 1},
		{ResourceKey: short.ResourceKey, TopicID: "topic", Available: true, Languages: []string{"go"}, Minutes: 1},
		{ResourceKey: short.ResourceKey, TopicID: "topic", Available: true, Recent: true, Minutes: 1, DueReview: true},
		{ResourceKey: short.ResourceKey, TopicID: "topic", Available: true, AlreadySolved: true, Minutes: 1},
		{ResourceKey: short.ResourceKey, TopicID: "topic", Available: true, Minutes: 11},
		{ResourceKey: short.ResourceKey, TopicID: "topic", Available: true},
	}
	got, filters, err := filterCandidates(input, 8000)
	want := Filters{OutOfScope: 1, Unavailable: 1, Language: 1, Seen: 2, Budget: 2}
	if err != nil || len(got) != 0 || filters != want {
		t.Fatal("unexpected filtered candidates:", got, filters, err)
	}
	short.Languages = []string{"go", "python"}
	input.Candidates = append(input.Candidates, short)
	got, filters, err = filterCandidates(input, 8000)
	if err != nil || len(got) != 1 || !slices.Equal(input.Candidates[got[0]].Languages, short.Languages) || filters != want {
		t.Fatal("eligible candidate was not returned:", got, filters, err)
	}
}
