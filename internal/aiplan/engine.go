// Package aiplan добавляет ограниченную AI-приоритизацию к проверяемому планировщику.
package aiplan

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/overmindv/recommendations/internal/planner"
)

const promptVersion = "practice-rank-v1"

type Provider interface {
	Rank(context.Context, []byte) ([]int, error)
}
type entry struct {
	order   []int
	expires time.Time
}
type flight struct {
	done  chan struct{}
	order []int
	ok    bool
}
type quota struct {
	count   int
	expires time.Time
}
type Engine struct {
	providers []Provider
	mu        sync.Mutex
	cache     map[[32]byte]entry
	running   map[[32]byte]*flight
	users     map[string]quota
	slots     chan struct{}
	now       func() time.Time
}

func New(providers ...Provider) *Engine {
	return &Engine{providers: providers, cache: map[[32]byte]entry{}, running: map[[32]byte]*flight{}, users: map[string]quota{}, slots: make(chan struct{}, 2), now: time.Now}
}

type compactSkill struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Accepted int    `json:"accepted"`
	Errors   int    `json:"unresolved_errors"`
	Level    string `json:"level"`
	Target   bool   `json:"target"`
}
type compactTask struct {
	ID      int    `json:"id"`
	Skill   int    `json:"skill"`
	Title   string `json:"title"`
	Minutes int    `json:"minutes"`
	Kind    string `json:"kind"`
	Reason  string `json:"reason"`
}

type compactInput struct {
	Goal       string         `json:"goal"`
	Minutes    int            `json:"minutes_per_session"`
	Sessions   int            `json:"sessions"`
	Partial    bool           `json:"partial_history"`
	Skills     []compactSkill `json:"skills"`
	Candidates []compactTask  `json:"candidates"`
}

func compact(in planner.Request, pool planner.Plan) ([]byte, []planner.Item) {
	skills := map[string]int{}
	names := map[string]string{}
	for _, t := range in.Topics {
		names[t.ID] = t.Title
	}
	c := compactInput{Goal: in.Goal, Minutes: in.Minutes, Sessions: max(1, in.SessionCount), Partial: in.InputTruncated, Skills: []compactSkill{}, Candidates: []compactTask{}}
	for i, s := range pool.TopicInsights {
		skills[s.TopicID] = i + 1
		c.Skills = append(c.Skills, compactSkill{i + 1, short(names[s.TopicID], 80), s.AcceptedTasks, s.FailedTasks, s.RecommendedDifficulty, s.IsTarget})
	}
	items := []planner.Item{}
	for _, session := range pool.Sessions {
		for _, item := range session.Items {
			if item.EstimatedMinutes > in.Minutes {
				continue
			}
			items = append(items, item)
			if len(items) <= 40 {
				c.Candidates = append(c.Candidates, compactTask{len(items), skills[item.TopicID], short(item.Title, 100), item.EstimatedMinutes, item.TaskType, item.ReasonCode})
			}
		}
	}
	data, _ := json.Marshal(c)

	return data, items
}
func short(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}

	return s
}

// Build никогда не использует плановые задачи как evidence и не доверяет ID модели.
func (e *Engine) Build(ctx context.Context, in planner.Request, actor string) (planner.Plan, error) {
	base, err := planner.Build(ctx, in)
	if err != nil {
		return base, err
	}
	base.AIStatus = "NOT_REQUESTED"
	if in.Goal == "" {
		return base, nil
	}
	base.AIStatus = aiStatusUnavailable
	if len(e.providers) == 0 {
		return base, nil
	}
	poolInput := in
	poolInput.Minutes = in.Minutes
	poolInput.SessionCount = 8
	pool, err := planner.Build(ctx, poolInput)
	if err != nil {
		return base, err
	}
	data, items := compact(in, pool)
	if len(items) == 0 {
		base.AIStatus = "NO_CANDIDATES"

		return base, nil
	}
	// Полный snapshot в hash, но наружу уходят только агрегаты и публичные карточки.
	snapshot, _ := json.Marshal(in)
	key := sha256.Sum256(append(append([]byte(promptVersion+"\x00"+actor+"\x00"), snapshot...), data...))
	order, status := e.rank(ctx, key, actor, data, min(40, len(items)))
	base.AIStatus = status
	if order == nil {
		return base, nil
	}
	// Хвост каталога заполняет горизонт без дополнительных токенов.
	order = append([]int(nil), order...)
	for id := 41; id <= len(items); id++ {
		order = append(order, id)
	}
	base.PolicyVersion = planner.PolicyVersion + "+" + promptVersion
	base.Items = []planner.Item{}
	base.Sessions = []planner.Session{}
	base.TotalMinutes = 0
	base.TotalPlannedMinutes = 0
	counts := map[string]int{}
	used := map[int]bool{}
	for n := 1; n <= base.RequestedSessions; n++ {
		session := planner.Session{Number: n, Items: []planner.Item{}}
		for _, id := range order {
			item := items[id-1]
			if used[id] || session.TotalMinutes+item.EstimatedMinutes > in.Minutes || len(session.Items) >= planner.MaxItems {
				continue
			}
			used[id] = true
			session.Items = append(session.Items, item)
			session.TotalMinutes += item.EstimatedMinutes
			counts[item.TopicID]++
		}
		if len(session.Items) == 0 {
			break
		}
		base.Sessions = append(base.Sessions, session)
		base.TotalPlannedMinutes += session.TotalMinutes
	}
	if len(base.Sessions) > 0 {
		base.Items = base.Sessions[0].Items
		base.TotalMinutes = base.Sessions[0].TotalMinutes
	}
	warnings := []string{}
	for _, w := range base.Warnings {
		if w != "SHORT_HORIZON" && w != "NO_ELIGIBLE_TASKS" && w != "BUDGET_TOO_SMALL" {
			warnings = append(warnings, w)
		}
	}
	if len(base.Sessions) < base.RequestedSessions {
		warnings = append(warnings, "SHORT_HORIZON")
	}
	base.Warnings = warnings
	for i := range base.TopicInsights {
		base.TopicInsights[i].PlannedTasks = counts[base.TopicInsights[i].TopicID]
	}

	return base, nil
}

func (e *Engine) rank(ctx context.Context, key [32]byte, actor string, data []byte, n int) ([]int, string) {
	return e.cachedOrder(ctx, key, actor, data, func(order []int) bool { return validOrder(order, n) })
}

func (e *Engine) cachedOrder(ctx context.Context, key [32]byte, actor string, data []byte, validate func([]int) bool) ([]int, string) {
	e.mu.Lock()
	now := e.now()
	for k, v := range e.cache {
		if !now.Before(v.expires) {
			delete(e.cache, k)
		}
	}
	for k, v := range e.users {
		if !now.Before(v.expires) {
			delete(e.users, k)
		}
	}
	if v, ok := e.cache[key]; ok {
		e.mu.Unlock()

		return v.order, aiStatusCached
	}
	if f, ok := e.running[key]; ok {
		e.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, aiStatusUnavailable
		case <-f.done:
			if f.ok {
				return f.order, aiStatusCached
			}

			return nil, aiStatusUnavailable
		}
	}
	q := e.users[actor]
	if q.count >= 10 || (q.count == 0 && len(e.users) >= 10000) {
		e.mu.Unlock()

		return nil, aiStatusLimitReached
	}
	select {
	case e.slots <- struct{}{}:
	default:
		e.mu.Unlock()

		return nil, "BUSY"
	}
	if q.count == 0 {
		q.expires = now.Add(time.Hour)
	}
	q.count++
	e.users[actor] = q
	f := &flight{done: make(chan struct{})}
	e.running[key] = f
	e.mu.Unlock()
	defer func() { e.mu.Lock(); delete(e.running, key); close(f.done); e.mu.Unlock(); <-e.slots }()
	for i, p := range e.providers {
		if i >= 3 || ctx.Err() != nil {
			break
		}
		order, err := p.Rank(ctx, data)
		if err == nil && validate(order) {
			e.mu.Lock()
			if len(e.cache) >= 1000 {
				for k := range e.cache {
					delete(e.cache, k)

					break
				}
			}
			e.cache[key] = entry{order: order, expires: e.now().Add(15 * time.Minute)}
			f.order = order
			f.ok = true
			e.mu.Unlock()

			return order, aiStatusGenerated
		}
		if err == ErrRefused {
			return nil, aiStatusRefused
		}
	}

	return nil, aiStatusUnavailable
}
func validOrder(order []int, n int) bool {
	if len(order) != n {
		return false
	}
	copyIDs := append([]int{}, order...)
	sort.Ints(copyIDs)
	for i, id := range copyIDs {
		if id != i+1 {
			return false
		}
	}

	return true
}

var ErrRefused = fmt.Errorf("AI request refused")
