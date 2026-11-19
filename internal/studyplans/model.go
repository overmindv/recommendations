package studyplans

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"

	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

const MaxVersionsPerGoal = 1024

var (
	ErrInvalid     = errors.New("invalid study plan request")
	ErrNotFound    = errors.New("study plan or goal not found")
	ErrConflict    = errors.New("study plan inputs changed")
	ErrRequestUsed = errors.New("study plan request key already used")
	ErrLimit       = errors.New("study plan version limit reached")
	ErrStepBlocked = errors.New("study plan step is not actionable")
	ErrPending     = errors.New("independent grade is not available")
)

type Expected struct {
	GoalVersion         int64 `json:"goal_version"`
	AvailabilityVersion int64 `json:"availability_version"`
	PlanVersion         int64 `json:"plan_version"`
}

type Generation struct {
	Expected
	RequestID string
}

func (g Generation) valid(actor, goal string) bool {
	return planner.ValidID(actor) && planner.ValidID(goal) && planner.ValidID(g.RequestID) &&
		g.GoalVersion > 0 && g.AvailabilityVersion > 0 && g.PlanVersion >= 0
}

func (g Generation) digest() [32]byte {
	var versions [24]byte
	binary.BigEndian.PutUint64(versions[:8], uint64(g.GoalVersion))
	binary.BigEndian.PutUint64(versions[8:16], uint64(g.AvailabilityVersion))
	binary.BigEndian.PutUint64(versions[16:], uint64(g.PlanVersion))
	return sha256.Sum256(versions[:])
}

type Sources struct {
	Expected
	TargetTopics     []string
	AllowedLanguages []string
	Budget           planner.StudyBudget
	Active           bool
	actor            string
}

type Draft struct {
	Plan              planner.LearningPlan
	EvidenceWatermark [32]byte
	EvidencePolicy    mastery.Policy
	ScopeTopics       []string
	EvidenceTopics    []mastery.Topic
	EvidenceCaptured  time.Time
}

type Assessment struct {
	Verdict  string    `json:"verdict"`
	GradedAt time.Time `json:"graded_at"`
}

type Item struct {
	planner.StudyItem
	Ordinal         int         `json:"ordinal"`
	ActivityStarted *time.Time  `json:"activity_started_at,omitempty"`
	ActivityDone    *time.Time  `json:"activity_completed_at,omitempty"`
	Assessment      *Assessment `json:"assessment,omitempty"`
	evidenceSince   time.Time
	hasReceipt      bool
	observedGrade   bool
}

type Week struct {
	WeekHeader
	Items []Item `json:"items"`
}

type WeekHeader struct {
	planner.StudyWeek
	PlannedMinutes int `json:"planned_minutes"`
}

type View struct {
	GoalID              string                    `json:"goal_id"`
	Version             int64                     `json:"version"`
	GoalVersion         int64                     `json:"goal_version"`
	AvailabilityVersion int64                     `json:"availability_version"`
	TargetTopics        []string                  `json:"target_topics"`
	PlannerVersion      string                    `json:"planner_version"`
	RankingAlgorithm    string                    `json:"ranking_algorithm"`
	RankingPolicy       ranking.Policy            `json:"ranking_policy"`
	EvidenceWatermark   string                    `json:"evidence_watermark"`
	EvidencePolicy      mastery.Policy            `json:"evidence_policy"`
	CreatedAt           time.Time                 `json:"created_at"`
	Timezone            string                    `json:"timezone"`
	WeeklyMinutes       int                       `json:"weekly_minutes"`
	HorizonLimited      bool                      `json:"horizon_limited"`
	Weeks               []Week                    `json:"weeks"`
	UnmetOutcomes       []planner.UnmetOutcome    `json:"unmet_outcomes"`
	Suggestions         []planner.ScopeSuggestion `json:"suggestions"`
	Current             bool                      `json:"current"`
	Stale               bool                      `json:"stale"`
	NextStep            *int                      `json:"next_step"`
	BlockedReason       string                    `json:"blocked_reason,omitempty"`
	GoalDemonstrated    bool                      `json:"goal_demonstrated"`
	scopeTopics         []string
}
