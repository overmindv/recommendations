package evaluation

import (
	"crypto/sha256"
	"fmt"
	"slices"
	"time"

	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

type candidate struct {
	spec resourceSpec
	item planner.StudyResource
}

type snapshot struct {
	request  planner.LearningRequest
	calendar planner.StudyCalendar
	byKey    map[ranking.ResourceKey]candidate
	byName   map[string]candidate
	aliases  map[string]string
}

func (c catalog) scope(targets []string) map[string]bool {
	set := make(map[string]bool, len(c.topics))
	var visit func(string)
	visit = func(id string) {
		if set[id] {
			return
		}
		set[id] = true
		for _, pre := range c.topics[id].Prerequisites {
			visit(pre)
		}
	}
	for _, target := range targets {
		visit(target)
	}

	return set
}

func asOf(d dataset, c catalog, row scenario) (snapshot, error) {
	request := planner.LearningRequest{
		Topics: make(map[string]ranking.Topic), Budget: row.Budget.value(), Ranking: d.Policy,
		AllowedLanguages: row.Languages, DifficultyHints: make(map[string]int, len(row.DifficultyHints)),
	}
	calendar, err := planner.StudyWeeks(row.AsOf, request.Budget)
	if err != nil {
		return snapshot{}, err
	}
	s := snapshot{request: request, calendar: calendar, byKey: make(map[ranking.ResourceKey]candidate),
		byName: make(map[string]candidate), aliases: make(map[string]string)}
	scope := c.scope(row.Targets)
	latest := observationsAt(row.Observations, row.AsOf)
	for name := range scope {
		id := scopedID("topic", name)
		s.aliases[id] = name
		topic := ranking.Topic{}
		for _, pre := range c.topics[name].Prerequisites {
			topic.Prerequisites = append(topic.Prerequisites, scopedID("topic", pre))
		}
		if fact, ok := latest[name]; ok {
			topic.Knowledge = &ranking.Knowledge{ScoreBPS: fact.Score, Sufficient: fact.Sufficient}
		}
		s.request.Topics[id] = topic
		if hint, ok := row.DifficultyHints[name]; ok {
			s.request.DifficultyHints[id] = hint
		}
	}
	for _, name := range row.Targets {
		s.request.TargetTopicIDs = append(s.request.TargetTopicIDs, scopedID("topic", name))
	}
	denied := make(map[string]bool, len(row.Denied))
	for _, name := range row.Denied {
		denied[name] = true
	}
	history := activityAt(row.Activity, row.AsOf, time.Duration(d.RecentHours)*time.Hour)
	for _, r := range d.Resources {
		if !scope[r.Topic] || r.PublishedAt.After(row.AsOf) || !beforeEnd(row.AsOf, r.WithdrawnAt) {
			continue
		}
		entry := buildCandidate(r, denied[r.ID], history[r.ID])
		s.request.Resources = append(s.request.Resources, entry.item)
		s.byName[r.ID], s.byKey[entry.item.ResourceKey] = entry, entry
	}

	return s, nil
}

func observationsAt(rows []observation, cutoff time.Time) map[string]observation {
	latest := make(map[string]observation)
	for _, fact := range rows {
		if !fact.AvailableAt.After(cutoff) && !fact.ObservedAt.After(cutoff) && fact.ObservedAt.After(latest[fact.Topic].ObservedAt) {
			latest[fact.Topic] = fact
		}
	}

	return latest
}

type resourceHistory struct {
	completed bool
	recent    bool
	viewed    map[string]bool
	finished  map[string]bool
}

func activityAt(rows []activity, cutoff time.Time, recent time.Duration) map[string]resourceHistory {
	values := make(map[string]resourceHistory)
	start := cutoff.Add(-recent)
	for _, fact := range rows {
		if fact.ObservedAt.After(cutoff) || fact.AvailableAt.After(cutoff) {
			continue
		}
		value, used := values[fact.Resource]
		if !used {
			value.viewed, value.finished = make(map[string]bool), make(map[string]bool)
		}
		value.viewed[fact.Revision] = true
		if fact.Completed {
			value.finished[fact.Revision] = true
			value.completed = true
		}
		value.recent = value.recent || !fact.ObservedAt.Before(start)
		values[fact.Resource] = value
	}

	return values
}

func buildCandidate(r resourceSpec, denied bool, history resourceHistory) candidate {
	kind := ranking.Practice
	if r.Stage == planner.StudyTheory {
		kind = ranking.Theory
	}
	row := ranking.Candidate{
		ResourceKey: ranking.ResourceKey{Kind: kind, ID: scopedID("resource", r.ID), RevisionID: scopedID("revision:"+r.ID, r.Revision)},
		TopicID:     scopedID("topic", r.Topic), Minutes: r.Minutes, Difficulty: r.Difficulty, Languages: r.Languages,
		Available: !denied, AlreadySolved: history.completed, Recent: history.recent,
	}
	if kind == ranking.Theory {
		row.AlreadySolved, row.Recent = history.finished[r.Revision], false
	}
	if r.Stage == planner.StudyCheck && history.viewed[r.Revision] {
		row.Available = false
	}

	return candidate{spec: r, item: planner.StudyResource{Candidate: row, IndependentCheck: r.Stage == planner.StudyCheck}}
}

func scopedID(namespace, name string) string {
	digest := sha256.Sum256([]byte(namespace + ":" + name))
	digest[6] = digest[6]&0x0f | 0x80
	digest[8] = digest[8]&0x3f | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", digest[:4], digest[4:6], digest[6:8], digest[8:10], digest[10:16])
}

func (s snapshot) known(topic string, threshold int) bool {
	knowledge := s.request.Topics[topic].Knowledge

	return knowledge != nil && knowledge.Sufficient && knowledge.ScoreBPS >= threshold
}

func (s snapshot) guards(topic string) []string {
	seen := make(map[string]bool)
	var guards []string
	var visit func(string)
	visit = func(id string) {
		for _, pre := range s.request.Topics[id].Prerequisites {
			if seen[pre] {
				continue
			}
			seen[pre] = true
			if !s.known(pre, s.request.Ranking.PrerequisiteMinBPS) {
				guards = append(guards, pre)
			}
			visit(pre)
		}
	}
	visit(topic)
	slices.Sort(guards)

	return guards
}

func (s snapshot) admissible(entry candidate) bool {
	row := entry.item
	if !row.Available || row.AlreadySolved || row.Recent {
		return false
	}
	allowed := s.request.AllowedLanguages

	return len(allowed) == 0 || len(row.Languages) == 0 ||
		slices.ContainsFunc(row.Languages, func(lang string) bool { return slices.Contains(allowed, lang) })
}
