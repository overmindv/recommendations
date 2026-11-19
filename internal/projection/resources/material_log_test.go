package resources

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/overmindv/content/pkg/api/activity"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type snapshotFunc func(context.Context, *int64, string) (SnapshotPage, error)

func (f snapshotFunc) Snapshot(ctx context.Context, watermark *int64, cursor string) (SnapshotPage, error) {
	return f(ctx, watermark, cursor)
}

func TestMaterialLogResumesFromCommittedPosition(t *testing.T) {
	brokers := os.Getenv("RESOURCE_KAFKA_TEST_BROKERS")
	if brokers == "" {
		t.Skip("RESOURCE_KAFKA_TEST_BROKERS is required")
	}
	ctx, store := resourceTestStore(t)
	topic := "material.projection.test." + strconv.FormatInt(time.Now().UnixNano(), 10)
	options := []kgo.Opt{kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.RequiredAcks(kgo.AllISRAcks())}
	broker, err := kgo.NewClient(append(options[:len(options):len(options)], kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()

	create := kmsg.NewPtrCreateTopicsRequest()
	create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 2, ReplicationFactor: 1}}
	created, err := create.RequestWith(ctx, broker)
	if err != nil || len(created.Topics) != 1 || created.Topics[0].ErrorCode != 0 {
		t.Fatal("could not create Kafka fixture topic:", created, err)
	}
	publish := func(partition int32, eventID, id string, version int64, removed bool) {
		t.Helper()
		event := activity.Event[activity.MaterialState]{
			EventID: eventID, EventType: activity.MaterialPublished, SchemaVersion: 1, OccurredAt: time.Now(),
			AggregateID: id, AggregateVersion: version, RevisionID: revisionID,
			Payload: activity.MaterialState{Metadata: &activity.ResourceMetadata{
				Title: "Published note", Type: "note", Format: "markdown", TopicIDs: []string{topicID},
			}},
		}
		if removed {
			event.EventType = activity.MaterialArchived
			event.Payload = activity.MaterialState{Tombstone: true}
		}
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if err := broker.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(id), Value: raw, Partition: partition}).FirstErr(); err != nil {
			t.Fatal("could not publish Kafka fixture:", err)
		}
	}
	publish(0, sourceEventID, resourceID, 5, false)

	stream, err := NewMaterialLog(topic, options...)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	source := snapshotFunc(func(_ context.Context, _ *int64, cursor string) (SnapshotPage, error) {
		if cursor != "" {
			return SnapshotPage{}, errInvalidPage
		}
		return SnapshotPage{Watermark: 5, Records: []Record{testProjectionRecord(resourceID, 5, topicID, Content)}}, nil
	})
	worker := NewContentWorker(store, source, stream, slog.Default())
	if err := worker.start(ctx, readProgress(t, ctx, store, Content)); err != nil {
		t.Fatal(err)
	}
	state := readProgress(t, ctx, store, Content)
	if state.Kafka == nil || len(state.Kafka.Offsets) != 2 || state.Kafka.Offsets[0] != 1 {
		t.Fatal("first snapshot must pin the current broker end:", state)
	}
	publish(0, secondEventID, resourceID, 6, true)
	publish(1, thirdEventID, otherTopic, 7, false)
	if _, err := worker.step(ctx); err != nil {
		t.Fatal(err)
	}
	state = readProgress(t, ctx, store, Content)

	read := func(pos KafkaPosition) (KafkaPosition, []Event) {
		t.Helper()
		for ctx.Err() == nil {
			next, events, err := stream.Read(ctx, pos)
			if err != nil {
				t.Fatal(err)
			}
			if len(events) != 0 {
				return next, events
			}
		}
		t.Fatal("material events did not arrive")
		return KafkaPosition{}, nil
	}
	_, uncommitted := read(*state.Kafka)
	next, replay := read(*state.Kafka)
	if replay[0].Record.Version == 5 || uncommitted[0].Record.Version == 5 {
		t.Fatal("event before the snapshot escaped the broker checkpoint")
	}
	// Первое чтение не зафиксировано в БД. Возврат к его offset не должен терять сообщения.
	if err := store.applyContent(ctx, state, next, replay, stream.VerifiedAt(next)); err != nil {
		t.Fatal(err)
	}
	for {
		state = readProgress(t, ctx, store, Content)
		if state.Kafka.Offsets[0] == 2 && state.Kafka.Offsets[1] == 1 {
			break
		}
		next, replay := read(*state.Kafka)
		if err := store.applyContent(ctx, state, next, replay, stream.VerifiedAt(next)); err != nil {
			t.Fatal(err)
		}
	}
	assertProjectedRow(t, ctx, store, state, resourceID, 6, false, 0)
	assertProjectedRow(t, ctx, store, state, otherTopic, 7, true, 1)
	var synced *time.Time
	if err := store.pool.QueryRow(ctx, "SELECT last_synced_at FROM resource_projection_checkpoints WHERE origin='content'").
		Scan(&synced); err != nil || synced == nil || state.Lagging {
		t.Fatal("caught-up material checkpoint was not confirmed:", synced, err)
	}

	expand := kmsg.NewPtrCreatePartitionsRequest()
	expand.Topics = []kmsg.CreatePartitionsRequestTopic{{Topic: topic, Count: 3}}
	changed, err := expand.RequestWith(ctx, broker)
	if err != nil || len(changed.Topics) != 1 || changed.Topics[0].ErrorCode != 0 {
		t.Fatal("could not expand fixture topic:", changed, err)
	}
	for ctx.Err() == nil {
		stream.boundsAt = time.Time{}
		_, _, err = stream.Read(ctx, *state.Kafka)
		if errors.Is(err, errResnapshot) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("old offsets accepted after partition growth")
}

func TestMaterialFreshnessNeedsCaughtUpBrokerOffsets(t *testing.T) {
	sample := time.Now()
	log := MaterialLog{boundsAt: sample, bounds: materialBounds{id: "known", end: map[int32]int64{0: 10, 1: 5}}}
	if stamp := log.VerifiedAt(KafkaPosition{TopicID: "known", Offsets: map[int32]int64{0: 10, 1: 4}}); !stamp.IsZero() {
		t.Fatal("lagging partition counted as synchronized")
	}
	if stamp := log.VerifiedAt(KafkaPosition{TopicID: "known", Offsets: map[int32]int64{0: 10, 1: 5}}); !stamp.Equal(sample) {
		t.Fatal("checkpoint changed the original broker observation time:", stamp)
	}
}

func TestMaterialLogRejectsExpiredAndReplacedPositions(t *testing.T) {
	bounds := materialBounds{id: "current", start: map[int32]int64{0: 5, 1: 0}, end: map[int32]int64{0: 10, 1: 1}}
	for _, pos := range []KafkaPosition{
		{TopicID: "old", Offsets: map[int32]int64{0: 5, 1: 0}},
		{TopicID: "current", Offsets: map[int32]int64{0: 4, 1: 0}},
		{TopicID: "current", Offsets: map[int32]int64{0: 11, 1: 0}},
		{TopicID: "current", Offsets: map[int32]int64{0: 5}},
	} {
		if bounds.contains(pos) {
			t.Error("old material position accepted:", pos)
		}
	}
	if !bounds.contains(KafkaPosition{TopicID: "current", Offsets: map[int32]int64{0: 5, 1: 1}}) {
		t.Error("committed bounds rejected")
	}
}
