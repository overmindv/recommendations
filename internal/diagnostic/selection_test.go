package diagnostic

import (
	"fmt"
	"slices"
	"testing"
)

func testID(n int) string {
	return fmt.Sprintf("11111111-2222-4333-8444-%012x", n)
}

func item(task, version, topic, minutes int) Candidate {
	return Candidate{TaskID: testID(task), VersionID: testID(version), TopicID: testID(topic), Minutes: minutes}
}

func TestSelectCoversDistinctTopics(t *testing.T) {
	scope := Scope{
		Targets:       []string{testID(1), testID(2), testID(3)},
		Prerequisites: []string{testID(4), testID(5)},
	}
	a := item(11, 21, 1, 3)
	b := item(14, 24, 4, 2)
	c := item(12, 22, 2, 5)
	d := item(15, 25, 5, 4)
	candidates := []Candidate{
		item(19, 29, 3, 8), item(16, 26, 1, 6), item(11, 27, 4, 1),
		item(17, 28, 3, 0), c, d, a, b,
	}
	want := []Candidate{a, b, c, d}
	for range 2 {
		got, err := Select(scope, candidates, MaxMinutes)
		if err != nil || !slices.Equal(got.Tasks, want) || got.EstimatedMinutes != 14 ||
			got.StopRule != (StopRule{PolicyVersion: PolicyVersion, MaxTasks: 4, BudgetMinutes: 20}) {
			t.Fatal("unexpected selection:", got, err)
		}
		slices.Reverse(candidates)
	}
}

func TestSelectRespectsBudgetAndScarcity(t *testing.T) {
	scope := Scope{Targets: []string{testID(1)}, Prerequisites: []string{testID(2)}}
	targetTask := item(11, 21, 1, 7)
	prereqTask := item(12, 22, 2, 3)
	for _, tc := range []struct {
		name   string
		rows   []Candidate
		budget int
		want   []Candidate
	}{
		{"empty bank", nil, 10, nil},
		{"missing estimate", []Candidate{item(13, 23, 1, 0)}, 10, nil},
		{"exact fit", []Candidate{targetTask, prereqTask}, 10, []Candidate{targetTask, prereqTask}},
		{"no room for a second task", []Candidate{targetTask, prereqTask}, 8, []Candidate{targetTask}},
		{"prerequisite only", []Candidate{targetTask, prereqTask}, 5, []Candidate{prereqTask}},
		{"no task fits", []Candidate{targetTask, prereqTask}, 2, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Select(scope, tc.rows, tc.budget)
			if err != nil || got.Tasks == nil || !slices.Equal(got.Tasks, tc.want) ||
				got.EstimatedMinutes > tc.budget || got.StopRule.BudgetMinutes != tc.budget {
				t.Fatal("unexpected selection:", got, err)
			}
		})
	}
}

func TestSelectRejectsInvalidInput(t *testing.T) {
	valid := Scope{Targets: []string{testID(1)}}
	for _, tc := range []struct {
		scope  Scope
		budget int
		rows   []Candidate
	}{
		{Scope{}, 10, nil},
		{Scope{Targets: valid.Targets, Prerequisites: valid.Targets}, 10, nil},
		{Scope{Targets: slices.Repeat(valid.Targets, MaxTopics+1)}, 10, nil},
		{valid, 0, nil},
		{valid, 21, nil},
		{valid, 10, make([]Candidate, MaxCandidates+1)},
		{valid, 10, []Candidate{item(11, 21, 2, 3)}},
		{valid, 10, []Candidate{item(11, 21, 1, -1)}},
		{valid, 10, []Candidate{{TaskID: "invalid", VersionID: testID(21), TopicID: testID(1), Minutes: 1}}},
	} {
		if _, err := Select(tc.scope, tc.rows, tc.budget); err != ErrInvalidSelection {
			t.Fatal("invalid input accepted:", err)
		}
	}
}

func BenchmarkSelectMaxCatalog(b *testing.B) {
	var scope Scope
	for i := 1; i <= MaxTopics; i++ {
		if i <= MaxTopics/2 {
			scope.Targets = append(scope.Targets, testID(i))
		} else {
			scope.Prerequisites = append(scope.Prerequisites, testID(i))
		}
	}
	candidates := make([]Candidate, MaxCandidates)
	for i := range candidates {
		candidates[i] = item(100+i, 1000+i, i%MaxTopics+1, 1+i%MaxMinutes)
	}
	for b.Loop() {
		if got, err := Select(scope, candidates, MaxMinutes); err != nil || len(got.Tasks) != MaxTasks {
			b.Fatal("unexpected selection:", got, err)
		}
	}
}
