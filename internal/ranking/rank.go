package ranking

import (
	"cmp"
	"slices"
)

type Ranked struct {
	ResourceKey
	TopicID    string  `json:"topic_id"`
	Minutes    int     `json:"estimated_minutes"`
	Score      int     `json:"score"`
	Components Factors `json:"components"`
}

type Result struct {
	Algorithm string   `json:"algorithm"`
	Policy    Policy   `json:"policy"`
	Filters   Filters  `json:"filters"`
	Items     []Ranked `json:"items"`
}

type identity struct {
	kind Kind
	id   string
}

type placement struct {
	resource int
	topic    string
}

type scored struct {
	input      int
	total      int
	components Factors
}

func Rank(request Request, policy Policy) (Result, error) {
	if !policy.Valid() || request.Limit < 1 || request.Limit > MaxResult ||
		(request.PreviousKind != "" && request.PreviousKind != Theory && request.PreviousKind != Practice) {
		return Result{}, ErrInvalid
	}
	candidates, filters, err := filterCandidates(request, policy.PrerequisiteMinBPS)
	if err != nil {
		return Result{}, err
	}
	ranked := make([]scored, 0, len(candidates))
	placements := make(map[placement]struct{}, len(candidates))
	positions := make(map[identity]int, len(candidates))
	for _, input := range candidates {
		row := request.Candidates[input]
		logical := identity{row.Kind, row.ID}
		pos, exists := positions[logical]
		if !exists {
			pos = len(ranked)
		} else if request.Candidates[ranked[pos].input].RevisionID != row.RevisionID {
			return Result{}, ErrInvalid
		}
		place := placement{pos, row.TopicID}
		if _, duplicate := placements[place]; duplicate {
			return Result{}, ErrInvalid
		}
		placements[place] = struct{}{}

		points, err := score(row, request.Topics[row.TopicID].Knowledge, request.PreviousKind, request.BudgetMinutes, policy)
		if err != nil {
			return Result{}, err
		}
		current := scored{input: input, total: points.sum(), components: points}
		if !exists {
			ranked = append(ranked, current)
			positions[logical] = pos
		} else if current.total > ranked[pos].total ||
			(current.total == ranked[pos].total && row.TopicID < request.Candidates[ranked[pos].input].TopicID) {
			ranked[pos] = current
		}
	}
	slices.SortFunc(ranked, func(a, b scored) int {
		if comparison := cmp.Compare(b.total, a.total); comparison != 0 {
			return comparison
		}
		left, right := request.Candidates[a.input], request.Candidates[b.input]
		return cmp.Or(cmp.Compare(left.Kind, right.Kind), cmp.Compare(left.ID, right.ID),
			cmp.Compare(left.RevisionID, right.RevisionID), cmp.Compare(left.TopicID, right.TopicID))
	})

	count := min(len(ranked), request.Limit)
	result := Result{
		Algorithm: AlgorithmVersion, Policy: policy, Filters: filters,
		Items: make([]Ranked, count),
	}
	for i, item := range ranked[:count] {
		row := request.Candidates[item.input]
		result.Items[i] = Ranked{
			ResourceKey: row.ResourceKey, TopicID: row.TopicID, Minutes: row.Minutes, Score: item.total, Components: item.components,
		}
	}
	return result, nil
}

func score(row Candidate, knowledge *Knowledge, previous Kind, budget int, policy Policy) (Factors, error) {
	gap, desired := policy.UnknownGapBPS, 0
	if knowledge != nil && knowledge.Sufficient {
		gap = max(0, policy.TargetBPS-knowledge.ScoreBPS)
		switch {
		case knowledge.ScoreBPS >= policy.HardDifficultyBPS:
			desired = 2
		case knowledge.ScoreBPS >= policy.MediumDifficultyBPS:
			desired = 1
		}
	}
	fit := maxBPS / 2
	if row.Kind == Practice {
		difficulty, ok := practiceDifficulty(row.Difficulty)
		if !ok {
			return Factors{}, ErrInvalid
		}
		distance := max(desired-difficulty, difficulty-desired)
		fit = (2 - distance) * (maxBPS / 2)
	}
	points := Factors{
		Gap:        policy.Weights.Gap * gap,
		Difficulty: policy.Weights.Difficulty * fit,
		Duration:   policy.Weights.Duration * ((budget - row.Minutes) * maxBPS / budget),
	}
	if previous != "" && previous != row.Kind {
		points.Variety = policy.Weights.Variety * maxBPS
	}
	if row.DueReview && row.Kind == Practice {
		points.DueReview = policy.Weights.DueReview * maxBPS
	}
	return points, nil
}

func practiceDifficulty(value string) (int, bool) {
	switch value {
	case "easy":
		return 0, true
	case "medium":
		return 1, true
	case "hard":
		return 2, true
	default:
		return 0, false
	}
}
