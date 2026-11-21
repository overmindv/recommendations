package planner

import (
	"context"
	"slices"
	"time"

	"github.com/overmindv/recommendations/internal/ranking"
)

type studyResourceID struct {
	kind ranking.Kind
	id   string
}

type studyOption struct {
	input       int
	reviewUntil string
}

type studyChoices struct {
	theory   []studyOption
	practice []studyOption
	checks   []studyOption
	due      bool
}

func studyResources(request LearningRequest, location *time.Location) (map[string]studyChoices, error) {
	book := make(map[string]studyChoices, len(request.Topics))
	for i, row := range request.Resources {
		if !ValidID(row.ID) || !ValidID(row.RevisionID) || !ValidID(row.TopicID) ||
			row.Minutes < 0 || row.Minutes > ranking.MaxMinutes || (row.Kind != ranking.Theory && row.Kind != ranking.Practice) ||
			(row.IndependentCheck && row.Kind != ranking.Practice) ||
			(row.DueReview && (!row.IndependentCheck || row.ReviewExpiresAt.IsZero())) {
			return nil, ErrInvalidInput
		}
		group := book[row.TopicID]
		option := studyOption{input: i}
		if row.DueReview {
			// Назначение должно действовать до конца недели.
			option.reviewUntil = row.ReviewExpiresAt.In(location).AddDate(0, 0, -1).Format(time.DateOnly)
			group.due = true
		}
		switch {
		case row.Kind == ranking.Theory:
			group.theory = append(group.theory, option)
		case row.IndependentCheck:
			group.checks = append(group.checks, option)
		default:
			group.practice = append(group.practice, option)
		}
		book[row.TopicID] = group
	}

	return book, nil
}

type studyProgress struct {
	usable bool
	guards []string
	week   int
}

type studyAllocator struct {
	request LearningRequest
	weeks   []LearningWeek
	used    map[studyResourceID]bool
	count   int
}

func allocateStudy(ctx context.Context, request LearningRequest, graph studyGraph, book map[string]studyChoices,
	calendar StudyCalendar) (LearningPlan, []string, error) {
	plan := LearningPlan{
		Version: LearningPlanVersion, RankingAlgorithm: ranking.AlgorithmVersion, RankingPolicy: request.Ranking,
		Timezone: calendar.Timezone, WeeklyMinutes: calendar.WeeklyMinutes, HorizonLimited: calendar.HorizonLimited,
		Weeks: make([]LearningWeek, len(calendar.Weeks)), UnmetOutcomes: []UnmetOutcome{}, Suggestions: []ScopeSuggestion{},
	}
	for i, week := range calendar.Weeks {
		plan.Weeks[i] = LearningWeek{StudyWeek: week, Items: []StudyItem{}}
	}
	allocator := studyAllocator{request: request, weeks: plan.Weeks, used: make(map[studyResourceID]bool)}
	progress := make(map[string]studyProgress, len(graph.order))
	unmet := make(map[string]bool)
	for _, id := range graph.order {
		if err := ctx.Err(); err != nil {
			return LearningPlan{}, nil, err
		}
		topic := request.Topics[id]
		state, blocked := readiness(topic, progress)
		if len(blocked) != 0 {
			plan.UnmetOutcomes = append(plan.UnmetOutcomes, UnmetOutcome{TopicID: id, Reason: "prerequisite", BlockedBy: blocked})
			unmet[id] = true

			continue
		}
		state.usable = meetsThreshold(topic.Knowledge, request.Ranking.PrerequisiteMinBPS)
		needed := !meetsThreshold(topic.Knowledge, graph.requirements[id])
		resources := book[id]
		if !needed && !resources.due {
			progress[id] = state

			continue
		}
		last, stage, reason, err := allocator.placeTopic(ctx, id, topic, state, resources, needed)
		if err != nil {
			return LearningPlan{}, nil, err
		}
		if reason != "" {
			plan.UnmetOutcomes = append(plan.UnmetOutcomes, UnmetOutcome{TopicID: id, Stage: stage, Reason: reason})
			unmet[id] = true
		} else if !state.usable {
			state.usable = true
			state.guards = append(state.guards, id)
			state.week = last
		}
		progress[id] = state
	}
	remaining := make([]string, 0, len(graph.targets))
	for _, id := range graph.targets {
		if !unmet[id] {
			remaining = append(remaining, id)
		}
	}

	return plan, remaining, nil
}

func readiness(topic ranking.Topic, progress map[string]studyProgress) (studyProgress, []string) {
	var state studyProgress
	var blocked []string
	for _, id := range topic.Prerequisites {
		pre := progress[id]
		if !pre.usable {
			blocked = append(blocked, id)

			continue
		}
		state.week = max(state.week, pre.week)
		state.guards = append(state.guards, pre.guards...)
	}
	slices.Sort(blocked)
	slices.Sort(state.guards)
	state.guards = slices.Compact(state.guards)

	return state, blocked
}

func meetsThreshold(knowledge *ranking.Knowledge, threshold int) bool {
	return knowledge != nil && knowledge.Sufficient && knowledge.ScoreBPS >= threshold
}

func (a *studyAllocator) placeTopic(ctx context.Context, id string, topic ranking.Topic, state studyProgress,
	choices studyChoices, needed bool) (int, StudyStage, string, error) {
	stages := []struct {
		name    StudyStage
		options []studyOption
	}{{StudyTheory, choices.theory}, {StudyPractice, choices.practice}, {StudyCheck, choices.checks}}
	if !needed {
		stages = stages[2:]
	}
	if a.count+len(stages) > MaxLearningItems {
		return 0, "", unmetPlanLimit, nil
	}
	from := state.week
	var placed [3]int
	for i, stage := range stages {
		var reserved string
		if stage.name == StudyPractice {
			reserved = a.onlyCheck(choices.checks, from)
		}
		week, best, reason, err := a.pick(ctx, id, topic.Knowledge, from, stage.options, !needed, reserved)
		if err != nil || reason != "" {
			for j := i - 1; j >= 0; j-- {
				a.unplace(placed[j])
			}

			return 0, stage.name, reason, err
		}
		a.place(week, StudyItem{Ranked: best, Stage: stage.name, RequiresCheck: slices.Clone(state.guards)})
		placed[i], from = week, week
	}

	return from, "", "", nil
}

func (a *studyAllocator) onlyCheck(options []studyOption, from int) string {
	var reserved string
	for _, option := range options {
		row := a.request.Resources[option.input]
		if !row.Available || row.AlreadySolved || row.Recent || row.Minutes == 0 || a.used[studyResourceID{row.Kind, row.ID}] {
			continue
		}
		if len(row.Languages) != 0 && len(a.request.AllowedLanguages) != 0 &&
			!slices.ContainsFunc(row.Languages, func(language string) bool { return slices.Contains(a.request.AllowedLanguages, language) }) {
			continue
		}
		canFit := slices.ContainsFunc(a.weeks[from:], func(week LearningWeek) bool {
			return (option.reviewUntil == "" || week.Through <= option.reviewUntil) &&
				week.CapacityMinutes-week.PlannedMinutes >= row.Minutes
		})
		if !canFit {
			continue
		}
		if reserved != "" && reserved != row.ID {
			return ""
		}
		reserved = row.ID
	}

	return reserved
}

func (a *studyAllocator) pick(ctx context.Context, id string, knowledge *ranking.Knowledge, from int, options []studyOption,
	dueOnly bool, reserved string) (int, ranking.Ranked, string, error) {
	usable := make([]studyOption, 0, len(options))
	for _, option := range options {
		row := a.request.Resources[option.input]
		if row.ID != reserved && row.Minutes > 0 && (!dueOnly || row.DueReview) && !a.used[studyResourceID{row.Kind, row.ID}] {
			usable = append(usable, option)
		}
	}
	if len(usable) == 0 {
		return 0, ranking.Ranked{}, unmetMissingResource, nil
	}
	request := ranking.Request{
		Topics:           map[string]ranking.Topic{id: {Knowledge: knowledge}},
		AllowedLanguages: a.request.AllowedLanguages, Limit: 1,
		Candidates: make([]ranking.Candidate, 0, len(usable)),
	}
	largestRejected := -1
	for i := from; i < len(a.weeks); i++ {
		if err := ctx.Err(); err != nil {
			return 0, ranking.Ranked{}, "", err
		}
		slot := a.weeks[i]
		request.BudgetMinutes = slot.CapacityMinutes - slot.PlannedMinutes
		// Срок проверки не продлевается, поэтому после отказа ищем только с большим бюджетом.
		if request.BudgetMinutes <= largestRejected {
			continue
		}
		request.Candidates = request.Candidates[:0]
		request.PreviousKind = ""
		if len(slot.Items) != 0 {
			request.PreviousKind = slot.Items[len(slot.Items)-1].Kind
		}
		for _, row := range usable {
			if row.reviewUntil == "" || slot.Through <= row.reviewUntil {
				request.Candidates = append(request.Candidates, a.request.Resources[row.input].Candidate)
			}
		}
		if len(request.Candidates) == 0 {
			continue
		}
		result, err := ranking.Rank(request, a.request.Ranking)
		if err != nil {
			return 0, ranking.Ranked{}, "", err
		}
		if len(result.Items) != 0 {
			return i, result.Items[0], "", nil
		}
		if result.Filters.Budget == 0 {
			return 0, ranking.Ranked{}, unmetMissingResource, nil
		}
		largestRejected = request.BudgetMinutes
	}
	cannotFit := largestRejected >= 0
	if from >= len(a.weeks) {
		request.Candidates = request.Candidates[:0]
		request.BudgetMinutes = ranking.MaxMinutes
		request.PreviousKind = ""
		for _, row := range usable {
			request.Candidates = append(request.Candidates, a.request.Resources[row.input].Candidate)
		}
		result, err := ranking.Rank(request, a.request.Ranking)
		if err != nil {
			return 0, ranking.Ranked{}, "", err
		}
		cannotFit = len(result.Items) > 0
	}
	reason := unmetMissingResource
	if cannotFit {
		reason = unmetTimeBudget
	}

	return 0, ranking.Ranked{}, reason, nil
}

func (a *studyAllocator) place(week int, item StudyItem) {
	slot := &a.weeks[week]
	slot.Items = append(slot.Items, item)
	slot.PlannedMinutes += item.Minutes
	a.used[studyResourceID{item.Kind, item.ID}] = true
	a.count++
}

func (a *studyAllocator) unplace(week int) {
	slot := &a.weeks[week]
	last := len(slot.Items) - 1
	item := slot.Items[last]
	slot.Items[last] = StudyItem{}
	slot.Items = slot.Items[:last]
	slot.PlannedMinutes -= item.Minutes
	delete(a.used, studyResourceID{item.Kind, item.ID})
	a.count--
}
