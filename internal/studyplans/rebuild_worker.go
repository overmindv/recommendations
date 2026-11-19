package studyplans

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/overmindv/recommendations/internal/observability"
)

const (
	rebuildPoll    = time.Second
	rebuildTimeout = 10 * time.Second
	rebuildLease   = 30 * time.Second
	rebuildRetry   = 5 * time.Second
)

type RebuildPlanner interface {
	Rebuild(context.Context, RebuildBase) (*Draft, error)
}

type rebuildStore interface {
	ClaimRebuild(context.Context, time.Duration) (RebuildJob, bool, error)
	LoadRebuild(context.Context, RebuildJob) (RebuildBase, error)
	CommitRebuild(context.Context, RebuildJob, RebuildBase, *Draft) error
	DiscardRebuild(context.Context, RebuildJob) error
	ReleaseRebuild(context.Context, RebuildJob, time.Duration) error
}

type RebuildWorker struct {
	store   rebuildStore
	planner RebuildPlanner
	log     *slog.Logger
	metrics *observability.Study
}

func NewRebuildWorker(store *Postgres, planner RebuildPlanner, log *slog.Logger) *RebuildWorker {
	return &RebuildWorker{store: store, planner: planner, log: log}
}

func (w RebuildWorker) WithMetrics(metrics *observability.Study) *RebuildWorker {
	w.metrics = metrics
	return &w
}

func (w *RebuildWorker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		worked, err := w.step(ctx)
		if err != nil && ctx.Err() == nil {
			w.log.Warn("study plan adaptation will retry")
		}
		if worked && err == nil {
			continue
		}
		timer := time.NewTimer(rebuildPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}

func (w *RebuildWorker) step(ctx context.Context) (bool, error) {
	work, cancel := context.WithTimeout(ctx, rebuildTimeout)
	defer cancel()
	job, found, err := w.store.ClaimRebuild(work, rebuildLease)
	if err != nil || !found {
		return false, err
	}
	err = w.rebuild(work, job)
	if err == nil {
		return true, nil
	}
	cleanup, done := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer done()
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrLimit) || errors.Is(err, ErrStaleRebuild) {
		err = w.store.DiscardRebuild(cleanup, job)
		if !errors.Is(err, ErrLeaseLost) {
			return true, err
		}
	}
	delay := rebuildRetry
	expected := errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrConflict) || ctx.Err() != nil
	if expected {
		delay = 0
	}
	release := w.store.ReleaseRebuild(cleanup, job, delay)
	if expected {
		return true, release
	}
	return true, errors.Join(err, release)
}

func (w *RebuildWorker) rebuild(ctx context.Context, job RebuildJob) (err error) {
	result := observability.Saved
	if w.metrics != nil {
		started := time.Now()
		defer func() { w.metrics.Plan(ctx, observability.Rebuild, planResult(result, err), time.Since(started)) }()
	}
	base, err := w.store.LoadRebuild(ctx, job)
	if err != nil {
		return err
	}
	draft, err := w.planner.Rebuild(ctx, base)
	if err != nil {
		return err
	}
	if draft == nil {
		result = observability.Unchanged
	}
	return w.store.CommitRebuild(ctx, job, base, draft)
}
