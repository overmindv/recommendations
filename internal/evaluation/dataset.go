package evaluation

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"time"

	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

const (
	maxDatasetBytes = 2 << 20
	maxCases        = 128
	maxHistory      = 1024
)

var slug = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)

type dataset struct {
	Schema      int            `json:"schema_version"`
	CatalogAt   time.Time      `json:"catalog_at"`
	HoldoutFrom time.Time      `json:"holdout_from"`
	RecentHours int            `json:"recent_hours"`
	Policy      ranking.Policy `json:"ranking"`
	Topics      []topicSpec    `json:"topics"`
	Resources   []resourceSpec `json:"resources"`
	Curriculum  []unit         `json:"curriculum"`
	Cases       []scenario     `json:"cases"`
}

type topicSpec struct {
	ID            string   `json:"id"`
	Prerequisites []string `json:"prerequisites"`
}

type resourceSpec struct {
	ID          string             `json:"id"`
	Revision    string             `json:"revision"`
	Topic       string             `json:"topic"`
	Stage       planner.StudyStage `json:"stage"`
	Minutes     int                `json:"minutes"`
	Difficulty  string             `json:"difficulty,omitempty"`
	Languages   []string           `json:"languages,omitempty"`
	PublishedAt time.Time          `json:"published_at"`
	WithdrawnAt time.Time          `json:"withdrawn_at,omitempty"`
}

type unit struct {
	Topic    string   `json:"topic"`
	Sequence []string `json:"resources"`
}

type scenario struct {
	ID              string         `json:"id"`
	AsOf            time.Time      `json:"as_of"`
	Targets         []string       `json:"targets"`
	Budget          budget         `json:"budget"`
	Languages       []string       `json:"languages"`
	Denied          []string       `json:"denied_resources,omitempty"`
	Observations    []observation  `json:"observations,omitempty"`
	Activity        []activity     `json:"activity,omitempty"`
	DifficultyHints map[string]int `json:"difficulty_hints,omitempty"`
	Expect          expectations   `json:"expect"`
	ExpertReview    *expertReview  `json:"expert_review,omitempty"`
}

type budget struct {
	Timezone  string `json:"timezone"`
	Deadline  string `json:"deadline,omitempty"`
	Available int    `json:"available_minutes"`
	Goal      int    `json:"goal_minutes"`
	Reserved  int    `json:"reserved_minutes"`
	Weeks     int    `json:"max_weeks"`
}

func (b budget) value() planner.StudyBudget {
	return planner.StudyBudget{Timezone: b.Timezone, Deadline: b.Deadline, AvailableMinutes: b.Available,
		GoalMinutes: b.Goal, ReservedMinutes: b.Reserved, MaxWeeks: b.Weeks}
}

type observation struct {
	Topic       string    `json:"topic"`
	ObservedAt  time.Time `json:"observed_at"`
	AvailableAt time.Time `json:"available_at"`
	Score       int       `json:"score_bps"`
	Sufficient  bool      `json:"sufficient"`
}

type activity struct {
	Resource    string    `json:"resource"`
	Revision    string    `json:"revision"`
	ObservedAt  time.Time `json:"observed_at"`
	AvailableAt time.Time `json:"available_at"`
	Completed   bool      `json:"completed"`
}

type expectations struct {
	Checks []string `json:"scheduled_checks,omitempty"`
	Gaps   []gap    `json:"gaps,omitempty"`
}

type gap struct {
	Topic  string `json:"topic"`
	Reason string `json:"reason"`
}

type expertReview struct {
	Rubric     string    `json:"rubric"`
	ReviewedAt time.Time `json:"reviewed_at"`
	Adaptive   int       `json:"adaptive"`
	Static     int       `json:"static"`
}

type catalog struct {
	topics    map[string]topicSpec
	resources map[string][]resourceSpec
}

func readDataset(r io.Reader) (dataset, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxDatasetBytes+1))
	if err != nil || len(data) > maxDatasetBytes {
		return dataset{}, errors.New("cannot read evaluation data within size limit")
	}
	var value dataset
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) {
		return dataset{}, errors.New("invalid evaluation JSON")
	}

	return value, nil
}

func (d dataset) validate() (catalog, error) {
	if d.Schema != 1 || d.CatalogAt.IsZero() || !d.HoldoutFrom.After(d.CatalogAt) ||
		d.RecentHours < 1 || d.RecentHours > 720 || !d.Policy.Valid() || len(d.Topics) == 0 || len(d.Topics) > ranking.MaxTopics ||
		len(d.Resources) > ranking.MaxCandidates || len(d.Cases) < 2 || len(d.Cases) > maxCases {
		return catalog{}, errors.New("invalid evaluation header")
	}
	c := catalog{topics: make(map[string]topicSpec, len(d.Topics)), resources: make(map[string][]resourceSpec)}
	for i, row := range d.Topics {
		if _, exists := c.topics[row.ID]; exists || !slug.MatchString(row.ID) || !uniqueSlugs(row.Prerequisites, ranking.MaxTopics-1) {
			return catalog{}, fmt.Errorf("invalid topic at row %d", i+1)
		}
		c.topics[row.ID] = row
	}
	for i, row := range d.Resources {
		if !c.validResource(row) {
			return catalog{}, fmt.Errorf("invalid resource at row %d", i+1)
		}
		c.resources[row.ID] = append(c.resources[row.ID], row)
	}
	if err := c.validateVersions(); err != nil {
		return catalog{}, err
	}
	if err := c.validateCurriculum(d.Curriculum); err != nil {
		return catalog{}, err
	}
	seen := make(map[string]bool, len(d.Cases))
	var development, holdout int
	for i, row := range d.Cases {
		if seen[row.ID] || !slug.MatchString(row.ID) || row.AsOf.Before(d.CatalogAt) {
			return catalog{}, fmt.Errorf("invalid case at row %d", i+1)
		}
		seen[row.ID] = true
		if err := c.validateScenario(row); err != nil {
			return catalog{}, fmt.Errorf("case %s: %w", row.ID, err)
		}
		if row.AsOf.Before(d.HoldoutFrom) {
			development++
		} else {
			holdout++
		}
	}
	if development == 0 || holdout == 0 {
		return catalog{}, errors.New("evaluation requires development and holdout cases")
	}

	return c, nil
}

func (c catalog) validResource(row resourceSpec) bool {
	if !slug.MatchString(row.ID) || !slug.MatchString(row.Revision) || c.topics[row.Topic].ID == "" ||
		row.Minutes < 1 || row.Minutes > ranking.MaxMinutes || !uniqueSlugs(row.Languages, 8) ||
		row.PublishedAt.IsZero() || (!row.WithdrawnAt.IsZero() && !row.WithdrawnAt.After(row.PublishedAt)) {
		return false
	}
	switch row.Stage {
	case planner.StudyTheory:
		if row.Difficulty != "" || len(row.Languages) > 0 {
			return false
		}
	case planner.StudyPractice, planner.StudyCheck:
		if !slices.Contains([]string{"easy", "medium", "hard"}, row.Difficulty) {
			return false
		}
	default:
		return false
	}

	return true
}

func (c catalog) validateVersions() error {
	for _, rows := range c.resources {
		slices.SortFunc(rows, func(a, b resourceSpec) int {
			return cmp.Or(a.PublishedAt.Compare(b.PublishedAt), cmp.Compare(a.Revision, b.Revision))
		})
		seen := make(map[string]bool, len(rows))
		for i, row := range rows {
			if seen[row.Revision] || row.Stage != rows[0].Stage || row.Topic != rows[0].Topic ||
				(i > 0 && beforeEnd(row.PublishedAt, rows[i-1].WithdrawnAt)) {
				return errors.New("resource revisions overlap, repeat or change topic or stage")
			}
			seen[row.Revision] = true
		}
	}

	return nil
}

func (c catalog) validateCurriculum(units []unit) error {
	if len(units) != len(c.topics) {
		return errors.New("fixed curriculum must list all topics")
	}
	seen := make(map[string]bool, len(units))
	stages := [...]planner.StudyStage{planner.StudyTheory, planner.StudyPractice, planner.StudyCheck}
	for i, row := range units {
		topic := c.topics[row.Topic]
		if topic.ID == "" || len(row.Sequence) != len(stages) || seen[row.Topic] ||
			slices.ContainsFunc(topic.Prerequisites, func(id string) bool { return !seen[id] }) {
			return fmt.Errorf("invalid curriculum order at row %d", i+1)
		}
		seen[row.Topic] = true
		for j, id := range row.Sequence {
			versions := c.resources[id]
			if len(versions) == 0 || versions[0].Stage != stages[j] || versions[0].Topic != row.Topic {
				return fmt.Errorf("invalid fixed resource at curriculum row %d", i+1)
			}
		}
	}

	return nil
}

func (c catalog) validateScenario(row scenario) error {
	if row.AsOf.IsZero() || len(row.Targets) == 0 || !uniqueSlugs(row.Targets, 16) || !uniqueSlugs(row.Languages, 16) ||
		!uniqueSlugs(row.Denied, ranking.MaxCandidates) || len(row.Observations) > maxHistory || len(row.Activity) > maxHistory ||
		len(row.DifficultyHints) > ranking.MaxTopics {
		return errors.New("invalid scenario input")
	}
	if _, err := planner.StudyWeeks(row.AsOf, row.Budget.value()); err != nil {
		return errors.New("invalid scenario budget")
	}
	for _, target := range row.Targets {
		if c.topics[target].ID == "" {
			return errors.New("unknown target")
		}
	}
	for _, id := range row.Denied {
		if len(c.resources[id]) == 0 {
			return errors.New("unknown denied resource")
		}
	}
	for id, shift := range row.DifficultyHints {
		if c.topics[id].ID == "" || (shift != -1 && shift != 1) {
			return errors.New("invalid difficulty hint")
		}
	}
	type observationKey struct {
		topic string
		at    time.Time
	}
	seen := make(map[observationKey]bool, len(row.Observations))
	for _, fact := range row.Observations {
		key := observationKey{fact.Topic, fact.ObservedAt.UTC()}
		if seen[key] || c.topics[fact.Topic].ID == "" || !validFact(fact.ObservedAt, fact.AvailableAt) || fact.Score < 0 || fact.Score > 10_000 {
			return errors.New("invalid or duplicate observation")
		}
		seen[key] = true
	}
	for _, fact := range row.Activity {
		versions := c.resources[fact.Resource]
		if !validFact(fact.ObservedAt, fact.AvailableAt) || !slices.ContainsFunc(versions, func(r resourceSpec) bool {
			return r.Revision == fact.Revision && !fact.ObservedAt.Before(r.PublishedAt) && beforeEnd(fact.ObservedAt, r.WithdrawnAt)
		}) {
			return errors.New("invalid activity")
		}
	}
	if !uniqueSlugs(row.Expect.Checks, ranking.MaxTopics) || len(row.Expect.Gaps) > ranking.MaxTopics ||
		(len(row.Expect.Checks) == 0 && len(row.Expect.Gaps) == 0) {
		return errors.New("missing or invalid expectations")
	}
	scope := c.scope(row.Targets)
	for _, check := range row.Expect.Checks {
		if !scope[check] {
			return errors.New("expected check is outside the goal")
		}
	}
	gapTopics := make(map[string]bool, len(row.Expect.Gaps))
	for _, gap := range row.Expect.Gaps {
		if !scope[gap.Topic] || gapTopics[gap.Topic] || slices.Contains(row.Expect.Checks, gap.Topic) ||
			!slices.Contains([]string{"missing_resource", "prerequisite", "time_budget", "plan_limit"}, gap.Reason) {
			return errors.New("invalid expected gap")
		}
		gapTopics[gap.Topic] = true
	}
	if r := row.ExpertReview; r != nil && (!slug.MatchString(r.Rubric) || r.ReviewedAt.Before(row.AsOf) ||
		r.Adaptive < 1 || r.Adaptive > 5 || r.Static < 1 || r.Static > 5) {
		return errors.New("invalid expert review")
	}

	return nil
}

func uniqueSlugs(values []string, limit int) bool {
	if len(values) > limit {
		return false
	}
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if seen[value] || !slug.MatchString(value) {
			return false
		}
		seen[value] = true
	}

	return true
}

func validFact(observed, available time.Time) bool {
	return !observed.IsZero() && !available.IsZero() && !available.Before(observed)
}

func beforeEnd(at, end time.Time) bool { return end.IsZero() || at.Before(end) }
