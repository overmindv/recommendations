package mastery

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type VerificationEvidence struct {
	rows      []evidence
	truncated bool
}

func ReadVerificationEvidence(ctx context.Context, tx pgx.Tx, learner string, at time.Time) (VerificationEvidence, error) {
	rows, truncated, err := loadEvidence(ctx, tx, learner, at)
	return VerificationEvidence{rows: rows, truncated: truncated}, err
}

func (e VerificationEvidence) Scores(policy Policy, topics []string) []Topic {
	scores, _ := summarize(policy, topics, e.rows, e.truncated)
	return scores
}
