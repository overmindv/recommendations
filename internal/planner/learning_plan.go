package planner

import (
	"context"
	"slices"
	"time"

	"github.com/overmindv/recommendations/internal/ranking"
)

const (
	LearningPlanVersion     = "learning-plan-v1"
	MaxLearningItems        = 128
	maxLearningTargetTopics = 16
)

type StudyStage string

const (
	StudyTheory   StudyStage = "theory"
	StudyPractice StudyStage = "practice"
	StudyCheck    StudyStage = "independent_check"
)

type StudyResource struct {
	ranking.Candidate
	IndependentCheck bool
	ReviewExpiresAt  time.Time
}

type LearningRequest struct {
	TargetTopicIDs   []string
	Topics           map[string]ranking.Topic
	Resources        []StudyResource
	AllowedLanguages []string
	Budget           StudyBudget
	Ranking          ranking.Policy
	DifficultyHints  map[string]int
}

type StudyItem struct {
	ranking.Ranked
	Stage         StudyStage     `json:"stage"`
	RequiresCheck []string       `json:"requires_checks,omitempty"`
	Superseded    bool           `json:"superseded,omitempty"`
	Decision      *StudyDecision `json:"-"`
}

type StudyDecision struct {
	ReviewDue    bool
	RequiredBPS  int
	Alternative  *StudyAlternative
	alternatives []ranking.Ranked
}

type StudyAlternative struct {
	ranking.ResourceKey
	Minutes int `json:"estimated_minutes"`
}

type LearningContinuation struct {
	Weeks    []LearningWeek
	Remedial map[string]bool
	Rebudget bool
}

type LearningWeek struct {
	StudyWeek
	PlannedMinutes int         `json:"planned_minutes"`
	Items          []StudyItem `json:"items"`
}

type UnmetOutcome struct {
	TopicID   string     `json:"topic_id"`
	Stage     StudyStage `json:"stage,omitempty"`
	Reason    string     `json:"reason"`
	BlockedBy []string   `json:"blocked_by,omitempty"`
}

type ScopeSuggestion struct {
	Kind     string   `json:"kind"`
	TopicIDs []string `json:"topic_ids,omitempty"`
	Deadline string   `json:"deadline,omitempty"`
}

type LearningPlan struct {
	Version          string            `json:"version"`
	RankingAlgorithm string            `json:"ranking_algorithm"`
	RankingPolicy    ranking.Policy    `json:"ranking_policy"`
	Timezone         string            `json:"timezone"`
	WeeklyMinutes    int               `json:"weekly_minutes"`
	HorizonLimited   bool              `json:"horizon_limited"`
	Weeks            []LearningWeek    `json:"weeks"`
	UnmetOutcomes    []UnmetOutcome    `json:"unmet_outcomes"`
	Suggestions      []ScopeSuggestion `json:"suggestions"`
}

func BuildLearningPlan(ctx context.Context, now time.Time, request LearningRequest) (LearningPlan, error) {
	if err := ctx.Err(); err != nil {
		return LearningPlan{}, err
	}
	graph, err := learningGraph(request)
	if err != nil {
		return LearningPlan{}, err
	}
	calendar, err := StudyWeeks(now, request.Budget)
	if err != nil {
		return LearningPlan{}, err
	}
	book, err := studyResources(request, calendar.location)
	if err != nil {
		return LearningPlan{}, err
	}

	plan, remaining, err := allocateStudy(ctx, request, graph, book, calendar, nil)
	if err != nil {
		return LearningPlan{}, err
	}
	if len(plan.UnmetOutcomes) == 0 {
		return plan, nil
	}
	if len(remaining) < len(graph.targets) {
		plan.Suggestions = append(plan.Suggestions, ScopeSuggestion{Kind: "reduce_scope", TopicIDs: remaining})
	}
	if request.Budget.Deadline != "" && !plan.HorizonLimited && hasTimeShortage(plan) {
		budget := request.Budget
		budget.Deadline = ""
		extended, err := StudyWeeks(now, budget)
		if err != nil {
			return LearningPlan{}, err
		}
		replayed, _, err := allocateStudy(ctx, request, graph, book, extended, nil)
		if err != nil {
			return LearningPlan{}, err
		}
		if len(replayed.UnmetOutcomes) == 0 {
			for i := len(replayed.Weeks) - 1; i >= 0; i-- {
				week := replayed.Weeks[i]
				if week.PlannedMinutes > 0 && week.Through > request.Budget.Deadline {
					plan.Suggestions = append(plan.Suggestions, ScopeSuggestion{Kind: "extend_deadline", Deadline: week.Through})
					break
				}
			}
		}
	}
	return plan, nil
}

func ContinueLearningPlan(ctx context.Context, now time.Time, request LearningRequest, previous LearningContinuation) (LearningPlan, error) {
	graph, err := learningGraph(request)
	if err != nil {
		return LearningPlan{}, err
	}
	calendar, err := StudyWeeks(now, request.Budget)
	if err != nil {
		return LearningPlan{}, err
	}
	book, err := studyResources(request, calendar.location)
	if err != nil {
		return LearningPlan{}, err
	}
	plan, remaining, err := allocateStudy(ctx, request, graph, book, calendar, &previous)
	if err == nil && len(remaining) < len(graph.targets) && len(plan.UnmetOutcomes) != 0 {
		plan.Suggestions = append(plan.Suggestions, ScopeSuggestion{Kind: "reduce_scope", TopicIDs: remaining})
	}
	return plan, err
}

type studyGraph struct {
	targets      []string
	order        []string
	requirements map[string]int
}

func learningGraph(request LearningRequest) (studyGraph, error) {
	if !request.Ranking.Valid() || len(request.TargetTopicIDs) < 1 || len(request.TargetTopicIDs) > maxLearningTargetTopics ||
		len(request.Topics) > ranking.MaxTopics || len(request.Resources) > ranking.MaxCandidates ||
		len(request.AllowedLanguages) > 16 || len(request.DifficultyHints) > ranking.MaxTopics {
		return studyGraph{}, ErrInvalidInput
	}
	for _, language := range request.AllowedLanguages {
		if len(language) == 0 || len(language) > 32 {
			return studyGraph{}, ErrInvalidInput
		}
	}
	for id, hint := range request.DifficultyHints {
		if !ValidID(id) || (hint != -1 && hint != 1) {
			return studyGraph{}, ErrInvalidInput
		}
	}
	g := studyGraph{
		targets:      slices.Clone(request.TargetTopicIDs),
		requirements: make(map[string]int, len(request.Topics)),
	}
	slices.Sort(g.targets)
	for i, id := range g.targets {
		if !ValidID(id) || (i > 0 && g.targets[i-1] == id) {
			return studyGraph{}, ErrInvalidInput
		}
		g.requirements[id] = request.Ranking.TargetBPS
	}
	visiting := make(map[string]bool, len(request.Topics))
	visited := make(map[string]bool, len(request.Topics))
	var visit func(string) bool
	visit = func(id string) bool {
		if visiting[id] {
			return false
		}
		if visited[id] {
			return true
		}
		topic, exists := request.Topics[id]
		if !exists || !ValidID(id) || len(topic.Prerequisites) >= ranking.MaxTopics ||
			(topic.Knowledge != nil && (topic.Knowledge.ScoreBPS < 0 || topic.Knowledge.ScoreBPS > 10_000)) {
			return false
		}
		visiting[id] = true
		dependencies := slices.Clone(topic.Prerequisites)
		slices.Sort(dependencies)
		for i, pre := range dependencies {
			if (i > 0 && dependencies[i-1] == pre) || !visit(pre) {
				return false
			}
			g.requirements[pre] = max(g.requirements[pre], request.Ranking.PrerequisiteMinBPS)
		}
		delete(visiting, id)
		visited[id] = true
		g.order = append(g.order, id)
		return true
	}
	for _, id := range g.targets {
		if !visit(id) {
			return studyGraph{}, ErrInvalidInput
		}
	}
	return g, nil
}

func hasTimeShortage(plan LearningPlan) bool {
	return slices.ContainsFunc(plan.UnmetOutcomes, func(outcome UnmetOutcome) bool {
		return outcome.Reason == "time_budget"
	})
}
