package ranking

import "regexp"

const algorithmVersion = "resource-ranking-v1"

var versionPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

type Factors struct {
	Gap        int `json:"gap"`
	Difficulty int `json:"difficulty"`
	Duration   int `json:"duration"`
	Variety    int `json:"variety"`
	DueReview  int `json:"due_review"`
}

func (f Factors) sum() int {
	return f.Gap + f.Difficulty + f.Duration + f.Variety + f.DueReview
}

type Policy struct {
	Version             string  `json:"policy_version"`
	TargetBPS           int     `json:"target_bps"`
	UnknownGapBPS       int     `json:"unknown_gap_bps"`
	PrerequisiteMinBPS  int     `json:"prerequisite_min_bps"`
	MediumDifficultyBPS int     `json:"medium_difficulty_bps"`
	HardDifficultyBPS   int     `json:"hard_difficulty_bps"`
	Weights             Factors `json:"weights"`
}

func (p Policy) valid() bool {
	if !versionPattern.MatchString(p.Version) || p.TargetBPS < 1 || p.TargetBPS > maxBPS ||
		p.UnknownGapBPS < 0 || p.UnknownGapBPS > maxBPS || p.PrerequisiteMinBPS < 1 || p.PrerequisiteMinBPS > maxBPS ||
		p.MediumDifficultyBPS < 1 || p.MediumDifficultyBPS >= p.HardDifficultyBPS || p.HardDifficultyBPS > maxBPS {
		return false
	}
	for _, w := range [...]int{p.Weights.Gap, p.Weights.Difficulty, p.Weights.Duration, p.Weights.Variety, p.Weights.DueReview} {
		if w < 0 || w > 1000 {
			return false
		}
	}
	return p.Weights.sum() > 0
}
