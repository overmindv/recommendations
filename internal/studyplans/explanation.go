package studyplans

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/explanation"
	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/internal/planner"
)

type StepExplanation struct {
	Ordinal int `json:"ordinal"`
	explanation.Step
}

type Explanation struct {
	PlanVersion int64               `json:"plan_version"`
	Stale       bool                `json:"stale"`
	Steps       []StepExplanation   `json:"steps"`
	Limits      []explanation.Limit `json:"limits"`
}

func (p *Postgres) Explain(ctx context.Context, actor, goal string, version int64) (Explanation, error) {
	if !planner.ValidID(actor) || !planner.ValidID(goal) || version < 1 || version > MaxVersionsPerGoal {
		return Explanation{}, ErrInvalid
	}
	var result Explanation
	err := pgx.BeginTxFunc(ctx, p.pool, readOnly, func(tx pgx.Tx) error {
		source, err := readSources(ctx, tx, actor, goal)
		if err != nil {
			return err
		}
		if source.PlanVersion == 0 {
			return ErrNotFound
		}
		if version != source.PlanVersion {
			return ErrConflict
		}
		view, err := readView(ctx, tx, goal, version, source)
		if err != nil {
			return err
		}
		result, err = readExplanations(ctx, tx, view)
		return err
	})
	if err != nil {
		return Explanation{}, fmt.Errorf("read study explanations: %w", err)
	}
	return result, nil
}

func readExplanations(ctx context.Context, tx pgx.Tx, view View) (Explanation, error) {
	items := make([]*Item, 0, planner.MaxLearningItems)
	for _, week := range view.Weeks {
		for i := range week.Items {
			items = append(items, &week.Items[i])
		}
	}
	result := Explanation{PlanVersion: view.Version, Stale: view.Stale, Steps: make([]StepExplanation, len(items)),
		Limits: explanation.Limits(view.UnmetOutcomes)}
	if len(items) == 0 {
		return result, nil
	}
	rows, err := tx.Query(ctx, `
		SELECT ordinal,details FROM study_plan_explanations
		WHERE goal_id=$1 AND plan_version=$2 ORDER BY ordinal`, view.GoalID, view.Version)
	if err != nil {
		return Explanation{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var ordinal int
		var raw []byte
		if err := rows.Scan(&ordinal, &raw); err != nil {
			return Explanation{}, err
		}
		if ordinal < 1 || ordinal > len(items) || items[ordinal-1].Ordinal != ordinal {
			return Explanation{}, ErrInvalid
		}
		step := &result.Steps[ordinal-1]
		step.Ordinal = ordinal
		if err := json.Unmarshal(raw, &step.Step); err != nil {
			return Explanation{}, err
		}
	}
	if err := rows.Err(); err != nil {
		return Explanation{}, err
	}
	for i, item := range items {
		step := &result.Steps[i]
		if step.Ordinal == 0 {
			step.Ordinal = item.Ordinal
			step.Step = explanation.ForStep(item.StudyItem, nil)
		}
		if view.Stale || view.NextStep == nil || *view.NextStep != item.Ordinal ||
			item.ActivityStarted != nil || item.Superseded {
			step.Alternative = nil
		}
	}
	return result, nil
}

func saveExplanations(ctx context.Context, tx pgx.Tx, goal string, version int64, draft Draft, carried []int16) error {
	if draft.EvidenceCaptured.IsZero() {
		return nil
	}
	topics := make(map[string]*mastery.Topic, len(draft.EvidenceTopics))
	for i := range draft.EvidenceTopics {
		topic := &draft.EvidenceTopics[i]
		topics[topic.ID] = topic
	}
	ref := &explanation.EvidenceRef{Watermark: hex.EncodeToString(draft.EvidenceWatermark[:]),
		PolicyRevision: draft.EvidencePolicy.Revision, CapturedAt: draft.EvidenceCaptured.UTC()}
	var rows [][]any
	var ordinal int16
	for _, week := range draft.Plan.Weeks {
		for _, item := range week.Items {
			ordinal++
			if _, kept := slices.BinarySearch(carried, ordinal); kept {
				continue
			}
			step := explanation.ForStep(item, topics[item.TopicID])
			step.Evidence.Ref = ref
			raw, err := json.Marshal(step)
			if err != nil {
				return err
			}
			rows = append(rows, []any{goal, version, ordinal, raw})
		}
	}
	if len(rows) == 0 {
		return nil
	}
	count, err := tx.CopyFrom(ctx, pgx.Identifier{"study_plan_explanations"},
		[]string{"goal_id", "plan_version", "ordinal", "details"}, pgx.CopyFromRows(rows))
	if err != nil {
		return err
	}
	if count != int64(len(rows)) {
		return errors.New("incomplete study plan explanations insert")
	}
	return nil
}
