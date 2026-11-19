package mastery

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

type PlanningEvidence struct {
	Topics    []Topic
	Watermark [32]byte
}

func ReadPlanningEvidence(ctx context.Context, tx pgx.Tx, actor string, topics []string, policy Policy, until time.Time) (PlanningEvidence, error) {
	if !policy.Valid() || !planner.ValidID(actor) || len(topics) < 1 || len(topics) > ranking.MaxTopics || until.IsZero() {
		return PlanningEvidence{}, errors.New("invalid planning evidence scope")
	}
	for _, topic := range topics {
		if !planner.ValidID(topic) {
			return PlanningEvidence{}, errors.New("invalid planning evidence topic")
		}
	}
	rows, truncated, err := loadEvidence(ctx, tx, actor, until)
	if err != nil {
		return PlanningEvidence{}, err
	}
	scores, used := summarize(policy, topics, rows, truncated)
	for i := range used {
		used[i].GradedAt = used[i].GradedAt.UTC()
	}
	sorted := slices.SortedFunc(slices.Values(scores), func(a, b Topic) int { return cmp.Compare(a.ID, b.ID) })
	doc, err := json.Marshal(struct {
		Policy    Policy     `json:"policy"`
		Truncated bool       `json:"truncated"`
		Topics    []Topic    `json:"topics"`
		Used      []evidence `json:"used"`
	}{policy, truncated, sorted, used})
	if err != nil {
		return PlanningEvidence{}, err
	}
	return PlanningEvidence{Topics: scores, Watermark: sha256.Sum256(doc)}, nil
}
