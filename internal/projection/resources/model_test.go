package resources

import (
	"slices"
	"testing"
)

const (
	resourceID = "11111111-1111-4111-8111-111111111111"
	revisionID = "22222222-2222-4222-8222-222222222222"
	topicID    = "33333333-3333-4333-8333-333333333333"
	otherTopic = "44444444-4444-4444-8444-444444444444"
)

func TestIncompleteResourceReplacesOldMetadata(t *testing.T) {
	program := Metadata{RevisionID: revisionID, Title: "Exercise", Type: "programming", Difficulty: "easy",
		Topics: []TopicWeight{{ID: topicID, Weight: 1}}, Runtimes: []Runtime{{ID: "python", Version: "3.12"}}}
	for _, mutate := range []func(*Metadata){
		func(m *Metadata) { m.Topics = nil },
		func(m *Metadata) { m.Runtimes = nil },
		func(m *Metadata) { m.Runtimes = []Runtime{{ID: "unknown", Version: "1.0"}} },
		func(m *Metadata) { m.RevisionID = "" },
		func(m *Metadata) { m.Topics = append(m.Topics, m.Topics[0]) },
	} {
		bad := program
		mutate(&bad)
		got, err := (Record{ID: resourceID, Version: 42, Metadata: &bad}).normalized(Tasks)
		if err != nil || got.Metadata != nil || got.ID != resourceID || got.Version != 42 {
			t.Fatal("incomplete row must keep the version without metadata:", got, err)
		}
	}
	got, err := (Record{ID: resourceID, Version: 43, Metadata: &program}).normalized(Tasks)
	if err != nil || got.Metadata == nil || got.Metadata.EstimatedMinutes != nil {
		t.Fatal("unknown duration must remain unknown:", got, err)
	}
}

func TestResourceMetadataNormalizationDoesNotMutateInput(t *testing.T) {
	task := Metadata{RevisionID: revisionID, Title: "Exercise", Type: "programming", Difficulty: "medium",
		Topics:   []TopicWeight{{ID: otherTopic, Weight: 2}, {ID: topicID, Weight: 1}},
		Runtimes: []Runtime{{ID: "python", Version: "3.12"}, {ID: "go", Version: "1.26"}, {ID: "python", Version: "3.12"}}}
	got, err := (Record{ID: resourceID, Version: 1, Metadata: &task}).normalized(Tasks)
	if err != nil || got.Metadata == nil || got.Metadata.Topics[0].ID != topicID || task.Topics[0].ID != otherTopic ||
		!slices.Equal(got.Metadata.Runtimes, []Runtime{{ID: "go", Version: "1.26"}, {ID: "python", Version: "3.12"}}) {
		t.Fatal("metadata was not independently normalized:", got, err)
	}
}

func TestResourceVersionZeroAllowedOnlyForLegacyMaterials(t *testing.T) {
	material := Metadata{RevisionID: revisionID, Title: "Reading", Type: "note", Format: "markdown",
		Topics: []TopicWeight{{ID: topicID, Weight: 1}}}
	row := Record{ID: resourceID, Version: 0, Metadata: &material}
	got, err := row.normalized(Content)
	if err != nil || got.Metadata == nil {
		t.Fatal("legacy material should be loaded:", got, err)
	}
	if _, err := row.normalized(Tasks); err == nil {
		t.Fatal("Tasks row accepted without a positive revision")
	}
}
