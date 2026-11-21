package catalog

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

type Worker struct {
	store  *Store
	source Source
	log    *slog.Logger
}

func NewWorker(store *Store, source Source, log *slog.Logger) *Worker {
	return &Worker{store: store, source: source, log: log}
}

func (w *Worker) Step(ctx context.Context) (bool, error) {
	st, err := w.store.State(ctx)
	if err != nil {
		return false, err
	}
	if st.Rebuild {
		var watermark *int64
		if st.PendingGeneration != 0 {
			watermark = &st.PendingWatermark
		}
		page, snapshotErr := w.source.Snapshot(ctx, watermark, st.PendingCursor)
		if snapshotErr != nil {
			return false, snapshotErr
		}

		return w.store.ApplySnapshot(ctx, st, page)
	}
	page, err := w.source.Changes(ctx, st.ReplayWatermark, st.Through)
	if err != nil {
		return false, err
	}

	return w.store.ApplyChanges(ctx, st, page)
}

func (w *Worker) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	nextPrune := time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		stepCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		moved, err := w.Step(stepCtx)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && !errors.Is(err, ErrConcurrent) {
			w.log.Warn("catalog projection did not advance", "error", err)
		}
		if time.Now().After(nextPrune) {
			pruneCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := w.store.Prune(pruneCtx)
			cancel()
			if err != nil && ctx.Err() == nil {
				w.log.Warn("could not prune retired catalog graph")
			}
			nextPrune = time.Now().Add(10 * time.Second)
		}
		if moved {
			timer.Reset(100 * time.Millisecond)
		} else {
			timer.Reset(2 * time.Second)
		}
	}
}
