package resources

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"time"
)

type snapshotSource interface {
	Snapshot(context.Context, *int64, string) (SnapshotPage, error)
}

type taskSource interface {
	snapshotSource
	Changes(context.Context, *int64, int64) (ChangePage, error)
}

type materialStream interface {
	Start(context.Context) (KafkaPosition, error)
	Read(context.Context, KafkaPosition) (KafkaPosition, []Event, error)
	VerifiedAt(KafkaPosition) time.Time
}

type Worker struct {
	store  *Store
	origin Origin
	source snapshotSource
	tasks  taskSource
	log    *slog.Logger
	stream materialStream
}

func NewTasksWorker(store *Store, source taskSource, logger *slog.Logger) *Worker {
	return &Worker{store: store, origin: Tasks, source: source, tasks: source, log: logger}
}

func NewContentWorker(store *Store, source snapshotSource, stream materialStream, logger *slog.Logger) *Worker {
	return &Worker{store: store, origin: Content, source: source, stream: stream, log: logger}
}

func (w *Worker) Run(ctx context.Context) error {
	failDelay := time.Second
	pruneAt := time.Now().Add(time.Minute)
	for ctx.Err() == nil {
		stepCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		progress, err := w.step(stepCtx)
		cancel()
		if ctx.Err() != nil {
			break
		}
		if time.Now().After(pruneAt) {
			pruneCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			pruneErr := w.store.prune(pruneCtx, w.origin)
			cancel()
			if pruneErr != nil && ctx.Err() == nil {
				w.log.Warn("could not prune resource generations", "source", w.origin)
			}
			pruneAt = time.Now().Add(time.Minute)
		}
		wait := time.Second
		switch {
		case errors.Is(err, errConcurrent):
			wait = 200 * time.Millisecond
		case err != nil:
			w.log.Warn("resource projection retry", "source", w.origin, "reason", resourceFailure(err))
			wait = failDelay
			failDelay = min(30*time.Second, failDelay*2)
		case progress:
			failDelay = time.Second

			continue
		default:
			failDelay = time.Second
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}

	return nil
}

func (w *Worker) step(ctx context.Context) (bool, error) {
	current, err := w.store.load(ctx, w.origin)
	if err != nil {
		return false, err
	}
	if current.Active == 0 && current.Pending == 0 {
		return true, w.start(ctx, current)
	}
	if current.Pending != 0 {
		page, snapshotErr := w.source.Snapshot(ctx, current.Watermark, current.Cursor)
		if errors.Is(snapshotErr, errSourcePosition) && current.Watermark != nil {
			return false, w.restart(ctx, current, "snapshot_position")
		}
		if snapshotErr != nil {
			return false, snapshotErr
		}

		return true, w.store.snapshotPage(ctx, current, page)
	}
	if w.origin == Content {
		return w.replayContent(ctx, current)
	}
	page, err := w.tasks.Changes(ctx, current.ReplayCeiling, current.Through)
	if errors.Is(err, errSourcePosition) {
		return false, w.restart(ctx, current, "journal_position")
	}
	if err != nil {
		return false, err
	}

	return page.Through > current.Through, w.store.applyTasks(ctx, current, page)
}

func (w *Worker) replayContent(ctx context.Context, current checkpoint) (bool, error) {
	if current.Kafka == nil {
		return false, w.restart(ctx, current, "missing_kafka_position")
	}
	next, events, err := w.stream.Read(ctx, *current.Kafka)
	if errors.Is(err, errResnapshot) || errors.Is(err, errInvalidEvent) {
		return false, w.restart(ctx, current, resourceFailure(err))
	}
	if err != nil {
		return false, err
	}
	err = w.store.applyContent(ctx, current, next, events, w.stream.VerifiedAt(next))
	if errors.Is(err, errEventConflict) {
		return false, w.restart(ctx, current, resourceFailure(err))
	}

	return !maps.Equal(next.Offsets, current.Kafka.Offsets), err
}

func (w *Worker) start(ctx context.Context, current checkpoint) error {
	if w.origin == Tasks {
		return w.store.beginSnapshot(ctx, current, nil)
	}
	position, err := w.stream.Start(ctx)
	if err != nil {
		return err
	}

	return w.store.beginSnapshot(ctx, current, &position)
}

func (w *Worker) restart(ctx context.Context, current checkpoint, reason string) error {
	if err := w.start(ctx, current); err != nil {
		return err
	}
	w.log.Warn("resource snapshot restarted", "source", w.origin, "reason", reason)

	return nil
}

func resourceFailure(err error) string {
	switch {
	case errors.Is(err, errResnapshot):
		return "kafka_position"
	case errors.Is(err, errEventConflict):
		return "event_conflict"
	case errors.Is(err, errInvalidEvent), errors.Is(err, errInvalidRecord):
		return "invalid_event"
	case errors.Is(err, errInvalidPage):
		return "invalid_page"
	case errors.Is(err, errKafkaUnavailable):
		return "kafka_unavailable"
	default:
		return "source_or_storage_unavailable"
	}
}
