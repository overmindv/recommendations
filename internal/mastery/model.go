package mastery

import (
	"cmp"
	"regexp"
	"slices"
	"time"
)

const (
	AlgorithmVersion = "weighted-v1"
	maxEvidence      = 512
	weightUnit       = 1_000_000
	maxScore         = 10_000
)

const (
	acceptedVerdict   = "accepted"
	unknownState      = "unknown"
	insufficientState = "insufficient"
	gapState          = "gap"
	developingState   = "developing"
	noEvidence        = "no_evidence"
	boundedHistory    = "bounded_history"
	limitedSample     = "limited_sample"
)

var revisionPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`)

type Policy struct {
	Revision         string `json:"revision"`
	Algorithm        string `json:"algorithm"`
	GapMaxBPS        int    `json:"gap_max_bps"`
	DemonstratedBPS  int    `json:"demonstrated_min_bps"`
	MinWeightedTasks int    `json:"min_weighted_tasks"`
}

func (p Policy) Valid() bool {
	return revisionPattern.MatchString(p.Revision) && p.Algorithm == AlgorithmVersion &&
		p.GapMaxBPS >= 0 && p.GapMaxBPS < p.DemonstratedBPS && p.DemonstratedBPS <= maxScore &&
		p.MinWeightedTasks >= 1 && p.MinWeightedTasks <= maxEvidence
}

type Topic struct {
	ID             string     `json:"topic_id"`
	ScoreBPS       *int       `json:"score_bps"`
	SampleCount    int        `json:"sample_count"`
	WeightMicros   int64      `json:"weight_micros"`
	LastEvidenceAt *time.Time `json:"last_evidence_at"`
	State          string     `json:"state"`
	Uncertainty    string     `json:"uncertainty"`
}

type Snapshot struct {
	GoalID      string    `json:"goal_id"`
	GoalVersion int64     `json:"goal_version"`
	Policy      Policy    `json:"policy"`
	ComputedAt  time.Time `json:"computed_at"`
	Truncated   bool      `json:"truncated"`
	Topics      []Topic   `json:"topics"`
}

type evidence struct {
	TaskID            string    `json:"task_id"`
	TaskVersionID     string    `json:"task_version_id"`
	SubmissionID      string    `json:"submission_id"`
	EventID           string    `json:"event_id"`
	CorrectionVersion int64     `json:"correction_version"`
	GradedAt          time.Time `json:"graded_at"`
	Verdict           string    `json:"verdict"`
	Topics            []string  `json:"topic_ids"`
	Weights           []int     `json:"topic_weights,omitempty"`
}

func isGraded(verdict string) bool {
	switch verdict {
	case acceptedVerdict, "wrong_answer", "compilation_error", "runtime_error",
		"time_limit_exceeded", "memory_limit_exceeded", "output_limit_exceeded":
		return true
	default:
		return false
	}
}

func summarize(policy Policy, topics []string, samples []evidence, truncated bool) ([]Topic, []evidence) {
	type statistic struct {
		Topic
		accepted int64
	}
	stats := make([]statistic, len(topics))
	index := make(map[string]int, len(topics))
	for i, id := range topics {
		stats[i].ID = id
		index[id] = i
	}
	canonical := slices.Clone(samples)
	slices.SortFunc(canonical, func(a, b evidence) int {
		return cmp.Or(a.GradedAt.Compare(b.GradedAt), cmp.Compare(a.SubmissionID, b.SubmissionID))
	})
	used := make([]evidence, 0, len(canonical))
	seen := make(map[string]bool, len(canonical))
	for _, row := range canonical {
		if !isGraded(row.Verdict) || row.GradedAt.IsZero() || seen[row.TaskID] {
			continue
		}
		seen[row.TaskID] = true
		weights := shares(row)
		contributed := false
		for i, topic := range row.Topics {
			n, ok := index[topic]
			if !ok {
				continue
			}
			s := &stats[n]
			s.SampleCount++
			s.WeightMicros += weights[i]
			s.LastEvidenceAt = new(row.GradedAt.UTC())
			if row.Verdict == acceptedVerdict {
				s.accepted += weights[i]
			}
			contributed = true
		}
		if contributed {
			used = append(used, row)
		}
	}
	results := make([]Topic, len(stats))
	for i, stat := range stats {
		topic := stat.Topic
		topic.State, topic.Uncertainty = unknownState, noEvidence
		if truncated {
			topic.Uncertainty = boundedHistory
		}
		if topic.WeightMicros > 0 {
			score := int((stat.accepted*maxScore + topic.WeightMicros/2) / topic.WeightMicros)
			topic.ScoreBPS = &score
			switch {
			case truncated:
				topic.State, topic.Uncertainty = insufficientState, boundedHistory
			case topic.WeightMicros < int64(policy.MinWeightedTasks)*weightUnit:
				topic.State, topic.Uncertainty = insufficientState, limitedSample
			default:
				topic.Uncertainty = "sufficient_for_policy"
				switch {
				case score <= policy.GapMaxBPS:
					topic.State = gapState
				case score >= policy.DemonstratedBPS:
					topic.State = "demonstrated_in_sample"
				default:
					topic.State = developingState
				}
			}
		}
		results[i] = topic
	}

	return results, used
}

func shares(row evidence) []int64 {
	result := make([]int64, len(row.Topics))
	if len(result) == 0 {
		return result
	}
	total := len(result)
	if row.Weights != nil {
		total = 0
		for _, w := range row.Weights {
			total += w
		}
	}
	var sum int64
	for i := range result {
		weight := 1
		if row.Weights != nil {
			weight = row.Weights[i]
		}
		result[i] = int64(weight) * weightUnit / int64(total)
		sum += result[i]
	}
	for i := range weightUnit - sum {
		result[i]++
	}

	return result
}
