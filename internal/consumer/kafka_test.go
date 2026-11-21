package consumer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/overmindv/content/pkg/api/activity"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type scriptedStore struct {
	calls int
	fail  bool
	saved bool
}

func (s *scriptedStore) apply(_ context.Context, rows []message) error {
	s.calls++
	if s.fail || s.calls == 1 {
		return errors.New("database unavailable")
	}
	s.saved = len(rows) == 2 && rows[0].offset == 12 && rows[1].offset == 13

	return nil
}

type scriptedKafka struct {
	t          *testing.T
	store      *scriptedStore
	commits    int
	rebalances int
	fetches    kgo.Fetches
}

func (k *scriptedKafka) PollRecords(context.Context, int) kgo.Fetches { return k.fetches }
func (k *scriptedKafka) AllowRebalance()                              { k.rebalances++ }

func (k *scriptedKafka) CommitRecords(_ context.Context, rows ...*kgo.Record) error {
	k.commits++
	if !k.store.saved || len(rows) != 2 || rows[0].Offset != 12 || rows[1].Offset != 13 {
		k.t.Error("Kafka offset committed before the matching database transaction")
	}
	if k.commits == 1 {
		return errors.New("broker unavailable")
	}

	return nil
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestBatchRetriesBeforeCommitting(t *testing.T) {
	t.Parallel()

	store := &scriptedStore{}
	client := &scriptedKafka{t: t, store: store}
	worker := Worker{store: store, log: quietLog(), contentTopic: activity.DefaultLearningTopic}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	rows := []*kgo.Record{{Topic: TasksTopic, Offset: 12}, {Topic: TasksTopic, Offset: 13}}
	if err := worker.processBatch(ctx, client, rows); err != nil {
		t.Fatal(err)
	}
	if client.commits != 2 || store.calls != 2 {
		t.Fatal("commit retry wrote the batch again or lost the offset:", client.commits, store.calls)
	}
	store.fail, store.saved, client.commits = true, false, 0
	short, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := worker.processBatch(short, client, rows); !errors.Is(err, context.DeadlineExceeded) || client.commits != 0 {
		t.Fatal("failed database transaction acknowledged:", err, client.commits)
	}
}

func TestWorkerDoesNotResetLostOffsets(t *testing.T) {
	t.Parallel()

	client := &scriptedKafka{fetches: kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: TasksTopic,
		Partitions: []kgo.FetchPartition{{Partition: 1, Err: kerr.OffsetOutOfRange}}}}}}}
	worker := Worker{log: quietLog(), contentTopic: activity.DefaultLearningTopic}
	if err := worker.consume(t.Context(), client, time.Second); !errors.Is(err, errOffsetLost) ||
		client.commits != 0 || client.rebalances != 1 {
		t.Fatal("offset loss did not stop the reader:", err, client.commits)
	}
}
