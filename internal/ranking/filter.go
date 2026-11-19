package ranking

import "slices"

func filterCandidates(request Request, prerequisiteMinBPS int) ([]int, Filters, error) {
	if len(request.Candidates) > MaxCandidates || len(request.Topics) > MaxTopics ||
		len(request.AllowedLanguages) > 16 || request.BudgetMinutes < 0 || request.BudgetMinutes > MaxMinutes ||
		!validLanguages(request.AllowedLanguages) || prerequisiteMinBPS < 1 || prerequisiteMinBPS > maxBPS {
		return nil, Filters{}, ErrInvalid
	}
	for id, topic := range request.Topics {
		if !validID(id) || len(topic.Prerequisites) > MaxTopics ||
			(topic.Knowledge != nil && (topic.Knowledge.ScoreBPS < 0 || topic.Knowledge.ScoreBPS > maxBPS)) {
			return nil, Filters{}, ErrInvalid
		}
	}

	ready := make(map[string]bool, len(request.Topics))
	for id, topic := range request.Topics {
		ready[id] = true
		for _, prerequisiteID := range topic.Prerequisites {
			if !validID(prerequisiteID) || prerequisiteID == id {
				return nil, Filters{}, ErrInvalid
			}
			knowledge := request.Topics[prerequisiteID].Knowledge
			if knowledge == nil || !knowledge.Sufficient || knowledge.ScoreBPS < prerequisiteMinBPS {
				ready[id] = false
			}
		}
	}

	candidates := make([]int, 0, len(request.Candidates))
	var filters Filters
	for i, row := range request.Candidates {
		if !validID(row.ID) || !validID(row.RevisionID) || !validID(row.TopicID) ||
			len(row.Languages) > 8 || !validLanguages(row.Languages) || (row.Kind != Theory && row.Kind != Practice) {
			return nil, Filters{}, ErrInvalid
		}
		readyTopic, inScope := ready[row.TopicID]
		switch {
		case !inScope:
			filters.OutOfScope++
		case !row.Available:
			filters.Unavailable++
		case !matchesLanguages(row.Languages, request.AllowedLanguages):
			filters.Language++
		case !readyTopic:
			filters.Prerequisites++
		case row.AlreadySolved || row.Recent:
			filters.Seen++
		case row.Minutes < 1 || row.Minutes > request.BudgetMinutes:
			filters.Budget++
		default:
			candidates = append(candidates, i)
		}
	}
	return candidates, filters, nil
}

func validID(id string) bool {
	return len(id) > 0 && len(id) <= 128
}

func validLanguages(languages []string) bool {
	for _, language := range languages {
		if len(language) == 0 || len(language) > 32 {
			return false
		}
	}
	return true
}

func matchesLanguages(resource, allowed []string) bool {
	if len(resource) == 0 || len(allowed) == 0 {
		return true
	}
	for _, language := range resource {
		if slices.Contains(allowed, language) {
			return true
		}
	}
	return false
}
