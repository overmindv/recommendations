package resources

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/overmindv/recommendations/internal/materialref"
	"github.com/overmindv/recommendations/internal/planner"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	maxMaterialTopics = 5
	maxPerTopic       = 16
	maxOutlineItems   = 8
)

var ErrUnavailable = errors.New("resource catalog unavailable")

type availabilitySource interface {
	Available(context.Context, []VersionKey) (map[VersionKey]bool, error)
}

type availabilityCheck func(context.Context, []VersionKey) (map[VersionKey]bool, error)

type resourceReader interface {
	lookup(context.Context, Origin, []lookupKey) (map[VersionKey]Candidate, error)
	materials(context.Context, []string) ([]Candidate, error)
}

type lookupKey struct {
	VersionKey
	TopicID string
}

type Candidate struct {
	Key              VersionKey
	Title            string
	Type             string
	Difficulty       string
	EstimatedMinutes *int
}

type Eligibility struct {
	store           resourceReader
	tasks           availabilitySource
	content         availabilitySource
	slots           *semaphore.Weighted
	diagnostic      *Store
	diagnosticCheck func(context.Context, string, []VersionKey) (map[VersionKey]bool, error)
}

func NewEligibility(store *Store, tasks *TaskClient, content *ContentClient) *Eligibility {
	e := newEligibility(store, tasks, content)
	e.diagnostic, e.diagnosticCheck = store, tasks.DiagnosticEligible

	return e
}

func newEligibility(store resourceReader, tasks, content availabilitySource) *Eligibility {
	return &Eligibility{store: store, tasks: tasks, content: content, slots: semaphore.NewWeighted(8)}
}

func (e *Eligibility) PracticeTasks(ctx context.Context, tasks []planner.Task) ([]planner.Task, error) {
	if len(tasks) > planner.MaxTasks {
		return nil, ErrUnavailable
	}
	keys := make([]lookupKey, 0, len(tasks))
	for _, task := range tasks {
		keys = append(keys, lookupKey{VersionKey: VersionKey{task.ID, task.TaskVersionID}, TopicID: task.TopicID})
	}
	available, err := e.verified(ctx, Tasks, keys, true)
	if err != nil {
		return nil, err
	}
	result := make([]planner.Task, 0, len(available))
	for i, key := range keys {
		candidate, ok := available[key.VersionKey]
		if !ok || candidate.EstimatedMinutes == nil {
			continue
		}
		result = append(result, planner.Task{
			ID: key.ID, TaskVersionID: key.RevisionID, TopicID: tasks[i].TopicID,
			Title: candidate.Title, TaskType: candidate.Type, Difficulty: candidate.Difficulty, CatalogMinutes: *candidate.EstimatedMinutes,
		})
	}

	return result, nil
}

func (e *Eligibility) VerifyVersions(ctx context.Context, origin Origin, keys []VersionKey) (map[VersionKey]Candidate, error) {
	lookups := make([]lookupKey, 0, len(keys))
	for _, key := range keys {
		lookups = append(lookups, lookupKey{VersionKey: key})
	}

	return e.verified(ctx, origin, lookups, false)
}

func (e *Eligibility) verified(ctx context.Context, origin Origin, keys []lookupKey, needsEstimate bool) (map[VersionKey]Candidate, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	if len(keys) > planner.MaxTasks || (origin != Tasks && origin != Content) {
		return nil, ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	candidates, err := e.store.lookup(ctx, origin, keys)
	if err != nil {
		return nil, ErrUnavailable
	}
	selected := make([]VersionKey, 0, len(candidates))
	seen := make(map[VersionKey]bool, len(candidates))
	for _, key := range keys {
		candidate, ok := candidates[key.VersionKey]
		if ok && (!needsEstimate || candidate.EstimatedMinutes != nil) && !seen[key.VersionKey] {
			selected = append(selected, key.VersionKey)
			seen[key.VersionKey] = true
		}
	}
	source := e.tasks
	if origin == Content {
		source = e.content
	}
	available, err := e.check(ctx, source.Available, selected)
	if err != nil {
		return nil, err
	}
	for key := range candidates {
		if !available[key] {
			delete(candidates, key)
		}
	}

	return candidates, nil
}

func (e *Eligibility) OutlineMaterials(ctx context.Context, topics, completed []string) ([]Candidate, error) {
	if len(topics) == 0 {
		return nil, nil
	}
	if len(topics) > maxMaterialTopics {
		return nil, ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	candidates, err := e.store.materials(ctx, topics)
	if err != nil {
		return nil, ErrUnavailable
	}
	done := make(map[string]bool, len(completed))
	for _, id := range completed {
		done[id] = true
	}
	keys := make([]VersionKey, 0, len(candidates))
	eligible := make([]Candidate, 0, len(candidates))
	seen := make(map[VersionKey]bool, len(candidates))
	for _, c := range candidates {
		if c.EstimatedMinutes != nil && !done[materialref.Encode(c.Key.ID, c.Key.RevisionID)] && !seen[c.Key] {
			keys, eligible = append(keys, c.Key), append(eligible, c)
			seen[c.Key] = true
		}
	}
	available, err := e.check(ctx, e.content.Available, keys)
	if err != nil {
		return nil, err
	}
	result := make([]Candidate, 0, maxOutlineItems)
	for _, c := range eligible {
		if available[c.Key] {
			result = append(result, c)
			if len(result) == maxOutlineItems {
				break
			}
		}
	}

	return result, nil
}

func (e *Eligibility) check(ctx context.Context, check availabilityCheck, keys []VersionKey) (map[VersionKey]bool, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	response := make([]map[VersionKey]bool, (len(keys)+pageLimit-1)/pageLimit)
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(4)
	index := 0
	for batch := range slices.Chunk(keys, pageLimit) {
		i := index
		index++
		group.Go(func() error {
			if err := e.slots.Acquire(ctx, 1); err != nil {
				return err
			}
			defer e.slots.Release(1)
			confirmed, err := check(ctx, batch)
			response[i] = confirmed

			return err
		})
	}
	if err := group.Wait(); err != nil {
		return nil, ErrUnavailable
	}
	result := make(map[VersionKey]bool, len(keys))
	for _, list := range response {
		for key, confirmed := range list {
			if confirmed {
				result[key] = true
			}
		}
	}

	return result, nil
}
