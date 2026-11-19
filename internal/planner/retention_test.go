package planner

import (
	"slices"
	"testing"
	"time"
)

func TestDueReviewsAtInterval(t *testing.T) {
	const interval = 48 * time.Hour
	now := time.Date(2026, 11, 19, 14, 0, 0, 0, time.UTC)
	boundary := now.Add(-interval)
	older := boundary.Add(-time.Minute)
	recent := boundary.Add(time.Second)
	future := now.Add(time.Hour)
	got := DueReviews(now, interval, []RetentionEvidence{
		{TopicID: "unknown"},
		{TopicID: "b", LastEvidenceAt: &boundary},
		{TopicID: "future", LastEvidenceAt: &future},
		{TopicID: "recent", LastEvidenceAt: &recent},
		{TopicID: "a", LastEvidenceAt: &boundary},
		{TopicID: "older", LastEvidenceAt: &older},
	})
	want := []DueReview{
		{TopicID: "older", DueAt: now.Add(-time.Minute)},
		{TopicID: "a", DueAt: now},
		{TopicID: "b", DueAt: now},
	}
	if !slices.Equal(got, want) {
		t.Fatal("unexpected due reviews:", got)
	}
	if got := DueReviews(now, 0, []RetentionEvidence{{TopicID: "older", LastEvidenceAt: &older}}); len(got) != 0 {
		t.Fatal("disabled interval scheduled a review:", got)
	}
}
