package resources

import (
	"context"
	"errors"
	"testing"

	"github.com/overmindv/recommendations/internal/ranking"
)

type accessLookup struct {
	origin Origin
	key    VersionKey
}

func (r accessLookup) lookup(_ context.Context, origin Origin, keys []lookupKey) (map[VersionKey]Candidate, error) {
	if origin != r.origin || len(keys) != 1 || keys[0].VersionKey != r.key || keys[0].TopicID != topicID {
		return nil, nil
	}
	return map[VersionKey]Candidate{r.key: {Key: r.key}}, nil
}

func (r accessLookup) materials(context.Context, []string) ([]Candidate, error) {
	return nil, nil
}

func TestStudyStepNeedsConfirmationOfTheSavedRevision(t *testing.T) {
	key := VersionKey{ID: resourceID, RevisionID: revisionID}
	for _, tc := range []struct {
		name   string
		kind   ranking.Kind
		origin Origin
	}{
		{"task", ranking.Practice, Tasks}, {"note", ranking.Theory, Content},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var allowed = true
			var fail bool
			var calls int
			owner := availabilityFunc(func(_ context.Context, keys []VersionKey) (map[VersionKey]bool, error) {
				calls++
				if fail {
					return nil, errors.New("owner unavailable")
				}
				return map[VersionKey]bool{keys[0]: allowed}, nil
			})
			source := &StudySource{eligibility: newEligibility(accessLookup{origin: tc.origin, key: key}, owner, owner)}
			step := ranking.ResourceKey{Kind: tc.kind, ID: key.ID, RevisionID: key.RevisionID}
			if got, err := source.StudyStepAvailable(t.Context(), step, topicID); !got || err != nil {
				t.Fatal("confirmed revision was not available:", got, err)
			}
			allowed = false
			if got, err := source.StudyStepAvailable(t.Context(), step, topicID); got || err != nil {
				t.Fatal("owner withdrawal was not respected:", got, err)
			}
			fail = true
			if got, err := source.StudyStepAvailable(t.Context(), step, topicID); got || !errors.Is(err, ErrUnavailable) {
				t.Fatal("owner outage left the step available:", got, err)
			}
			if got, err := source.StudyStepAvailable(t.Context(), step, otherTopic); got || err != nil || calls != 3 {
				t.Fatal("resource linked only to a different topic reached the owner check:", got, calls, err)
			}
			step.RevisionID = "abcdef00-1234-4000-8000-000000000001"
			if got, err := source.StudyStepAvailable(t.Context(), step, topicID); got || err != nil || calls != 3 {
				t.Fatal("an absent version was queried or substituted:", got, calls, err)
			}
		})
	}
}
