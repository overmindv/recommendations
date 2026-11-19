package planner

import (
	"cmp"
	"slices"
	"time"
)

type RetentionEvidence struct {
	TopicID        string
	LastEvidenceAt *time.Time
}

type DueReview struct {
	TopicID string    `json:"topic_id"`
	DueAt   time.Time `json:"due_at"`
}

func DueReviews(now time.Time, interval time.Duration, evidence []RetentionEvidence) []DueReview {
	reviews := make([]DueReview, 0, len(evidence))
	if interval <= 0 || now.IsZero() {
		return reviews
	}
	for _, row := range evidence {
		if row.LastEvidenceAt == nil || row.LastEvidenceAt.IsZero() {
			continue
		}
		dueAt := row.LastEvidenceAt.UTC().Add(interval)
		if !now.Before(dueAt) {
			reviews = append(reviews, DueReview{TopicID: row.TopicID, DueAt: dueAt})
		}
	}
	slices.SortFunc(reviews, func(a, b DueReview) int {
		return cmp.Or(a.DueAt.Compare(b.DueAt), cmp.Compare(a.TopicID, b.TopicID))
	})
	return reviews
}
