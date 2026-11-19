package resources

import (
	"context"

	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

func (s *StudySource) StudyStepAvailable(ctx context.Context, step ranking.ResourceKey, topic string) (bool, error) {
	if s.eligibility == nil || !planner.ValidID(step.ID) || !planner.ValidID(step.RevisionID) || !planner.ValidID(topic) {
		return false, ErrUnavailable
	}
	var origin Origin
	switch step.Kind {
	case ranking.Theory:
		origin = Content
	case ranking.Practice:
		origin = Tasks
	default:
		return false, ErrUnavailable
	}
	key := VersionKey{ID: step.ID, RevisionID: step.RevisionID}
	candidates, err := s.eligibility.verified(ctx, origin, []lookupKey{{VersionKey: key, TopicID: topic}}, false)
	if err != nil {
		return false, err
	}
	_, available := candidates[key]
	return available, nil
}
