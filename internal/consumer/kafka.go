package consumer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/overmindv/recommendations/internal/observability"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const groupID = "recommendations-learning-evidence-v1"

var (
	errOffsetLost  = errors.New("learning evidence Kafka offset is no longer available")
	errKafkaAccess = errors.New("learning evidence Kafka access denied")
)

type batchStore interface {
	apply(context.Context, []message) error
}

type groupClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	AllowRebalance()
}

type Worker struct {
	store        batchStore
	contentTopic string
	opts         []kgo.Opt
	log          *slog.Logger
	metrics      *observability.Study
}

func NewWorker(store *Postgres, contentTopic string, log *slog.Logger, options ...kgo.Opt) *Worker {
	opts := append(append([]kgo.Opt(nil), options...),
		kgo.ConsumerGroup(groupID), kgo.ConsumeTopics(TasksTopic, contentTopic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()), kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.RebalanceTimeout(40*time.Second),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxBytes(1<<20), kgo.FetchMaxPartitionBytes(64<<10), kgo.MaxConcurrentFetches(2))
	return &Worker{store: store, contentTopic: contentTopic, opts: opts, log: log}
}

func (w *Worker) WithMetrics(metrics *observability.Study) *Worker {
	w.metrics = metrics
	return w
}

func (w *Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		client, err := kgo.NewClient(w.opts...)
		if err != nil {
			return errors.New("invalid learning evidence Kafka client configuration")
		}
		err = w.consume(ctx, client, 20*time.Second)
		client.CloseAllowingRebalance()
		if err == nil || errors.Is(err, errOffsetLost) || errors.Is(err, errKafkaAccess) {
			return err
		}
		w.log.WarnContext(ctx, "rejoining learning evidence group from committed offsets")
		if !wait(ctx, time.Second) {
			break
		}
	}
	return nil
}

func (w *Worker) consume(ctx context.Context, client groupClient, budget time.Duration) error {
	defer client.AllowRebalance()
	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, batchLimit)
		if ctx.Err() != nil {
			return nil
		}
		problems := fetches.Errors()
		for _, problem := range problems {
			var loss *kgo.ErrDataLoss
			if errors.Is(problem.Err, kerr.OffsetOutOfRange) || errors.As(problem.Err, &loss) {
				w.observeFailure(observability.EvidenceOffsetLost)
				return errOffsetLost
			}
			if errors.Is(problem.Err, kerr.TopicAuthorizationFailed) || errors.Is(problem.Err, kerr.GroupAuthorizationFailed) ||
				errors.Is(problem.Err, kerr.SaslAuthenticationFailed) {
				w.observeFailure(observability.EvidenceAccessDenied)
				return errKafkaAccess
			}
			w.observeFailure(observability.EvidencePollFailure)
		}
		records := fetches.Records()
		if len(records) == 0 {
			client.AllowRebalance()
			if len(problems) != 0 {
				w.log.WarnContext(ctx, "learning evidence poll failed")
				if !wait(ctx, time.Second) {
					return nil
				}
			}
			continue
		}
		batchCtx, cancel := context.WithTimeout(ctx, budget)
		err := w.processBatch(batchCtx, client, records)
		cancel()
		client.AllowRebalance()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			// Продолжать чтение этим клиентом нельзя: poll уже сдвинул курсор.
			return err
		}
	}
	return nil
}

func (w *Worker) processBatch(ctx context.Context, client groupClient, records []*kgo.Record) error {
	messages := make([]message, 0, len(records))
	for _, record := range records {
		messages = append(messages, decodeMessage(record, w.contentTopic))
	}
	saved := false
	delay := 200 * time.Millisecond
	for ctx.Err() == nil {
		stage := "store"
		attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
		var err error
		if !saved {
			err = w.store.apply(attempt, messages)
			saved = err == nil
		}
		if saved && err == nil {
			stage = "commit"
			err = client.CommitRecords(attempt, records...)
		}
		cancel()
		if err == nil {
			if w.metrics != nil {
				w.observeCommitted(records, time.Now())
			}
			return nil
		}
		failure := observability.EvidenceStoreFailure
		if saved {
			failure = observability.EvidenceCommitFailure
		}
		w.observeFailure(failure)
		w.log.WarnContext(ctx, "learning evidence batch retry", "stage", stage, "size", len(records), "delay", delay)
		if !wait(ctx, delay) {
			break
		}
		delay = min(2*delay, 3*time.Second)
	}
	return ctx.Err()
}

func (w *Worker) observeCommitted(records []*kgo.Record, committedAt time.Time) {
	for _, record := range records {
		switch record.Topic {
		case TasksTopic:
			w.metrics.EvidenceCommitted(observability.EvidenceTasks, record.Timestamp, committedAt)
		case w.contentTopic:
			w.metrics.EvidenceCommitted(observability.EvidenceContent, record.Timestamp, committedAt)
		}
	}
}

func (w *Worker) observeFailure(failure observability.EvidenceFailure) {
	if w.metrics != nil {
		w.metrics.EvidenceFailure(failure)
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
