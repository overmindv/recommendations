package ranking

import "errors"

const (
	MaxCandidates = 2000
	MaxTopics     = 64
	MaxResult     = 128
	MaxMinutes    = 7 * 24 * 60
	maxBPS        = 10_000
)

var ErrInvalid = errors.New("invalid ranking input or policy")

type Kind string

const (
	Theory   Kind = "theory"
	Practice Kind = "practice"
)

type ResourceKey struct {
	Kind       Kind   `json:"kind"`
	ID         string `json:"resource_id"`
	RevisionID string `json:"revision_id"`
}

type Candidate struct {
	ResourceKey
	TopicID       string
	Minutes       int
	Difficulty    string
	Languages     []string
	Available     bool
	AlreadySolved bool
	Recent        bool
	DueReview     bool
}

type Knowledge struct {
	ScoreBPS   int
	Sufficient bool
}

type Topic struct {
	Knowledge     *Knowledge
	Prerequisites []string
}

type Request struct {
	Candidates       []Candidate
	Topics           map[string]Topic
	AllowedLanguages []string
	BudgetMinutes    int
	PreviousKind     Kind
	DifficultyShift  int
	Limit            int
}

type Filters struct {
	OutOfScope    int `json:"out_of_scope"`
	Unavailable   int `json:"unavailable"`
	Language      int `json:"language"`
	Prerequisites int `json:"prerequisites"`
	Seen          int `json:"seen"`
	Budget        int `json:"budget"`
}
