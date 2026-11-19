package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/pkg/api/goalcompletion"
	"github.com/twmb/franz-go/pkg/kgo"
)

type testProducer struct {
	sent []*kgo.Record
	err  error
}

func (p *testProducer) ProduceSync(_ context.Context, rows ...*kgo.Record) kgo.ProduceResults {
	p.sent = append(p.sent, rows...)
	result := make(kgo.ProduceResults, 0, len(rows))
	for _, row := range rows {
		result = append(result, kgo.ProduceResult{Record: row, Err: p.err})
	}
	return result
}

func goalOutboxDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("GOAL_OUTBOX_TEST_DSN")
	if dsn == "" {
		t.Skip("GOAL_OUTBOX_TEST_DSN is required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("GOAL_OUTBOX_TEST_DSN must point to a dedicated _test database")
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func seedGoalEvents(t *testing.T, pool *pgxpool.Pool) (goal string, events [2]goalcompletion.Event) {
	t.Helper()
	ctx := t.Context()
	var actor, topic, first, second string
	if err := pool.QueryRow(ctx, `
		SELECT gen_random_uuid()::text,gen_random_uuid()::text,gen_random_uuid()::text,gen_random_uuid()::text,gen_random_uuid()::text`).
		Scan(&goal, &actor, &topic, &first, &second); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, "DELETE FROM goal_completion_outbox WHERE goal_id=$1", goal)
	})
	for i, kind := range []goalcompletion.Type{goalcompletion.Completed, goalcompletion.Reopened} {
		version := int64(i + 1)
		event := goalcompletion.Event{
			EventID: []string{first, second}[i], Type: kind, AggregateID: goal, AggregateVersion: version,
			SchemaVersion: goalcompletion.SchemaVersion, OccurredAt: time.Now().UTC(),
			Payload: goalcompletion.Payload{LearnerID: actor, BusinessAwardKey: goalcompletion.BusinessAwardKey(goal),
				CompletionVersion: version, GoalVersion: 1, PlanVersion: 1, TargetTopicIDs: []string{topic}},
		}
		if err := event.Validate(); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO goal_completion_outbox (event_id,goal_id,completion_version,payload) VALUES ($1,$2,$3,$4)`,
			event.EventID, goal, version, data); err != nil {
			t.Fatal(err)
		}
		events[i] = event
	}
	return goal, events
}

func TestPostgresGoalOutboxRetriesBeforePublishingNewVersion(t *testing.T) {
	ctx, pool := t.Context(), goalOutboxDB(t)
	goal, events := seedGoalEvents(t, pool)
	producer := &testProducer{err: errors.New("broker unavailable")}
	p := &GoalPublisher{pool: pool, producer: producer}
	if found, err := p.Step(ctx); !found || err == nil || len(producer.sent) != 1 {
		t.Fatal("temporary failure should leave the first event pending:", found, err)
	}
	if found, err := p.Step(ctx); found || err != nil {
		t.Fatal("second version passed the failed first version:", found, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE goal_completion_outbox SET ready_at=statement_timestamp() WHERE event_id=$1 AND goal_id=$2",
		events[0].EventID, goal); err != nil {
		t.Fatal(err)
	}
	producer.err = nil
	for range events {
		if found, err := p.Step(ctx); !found || err != nil {
			t.Fatal("publication was not retried in order:", found, err)
		}
	}
	if len(producer.sent) != 3 || !bytes.Equal(producer.sent[0].Value, producer.sent[1].Value) ||
		string(producer.sent[1].Key) != goal || producer.sent[1].Topic != goalcompletion.Topic {
		t.Fatal("retry changed the Kafka key, topic or event identity")
	}
	var last goalcompletion.Event
	if err := json.Unmarshal(producer.sent[2].Value, &last); err != nil || last.EventID != events[1].EventID {
		t.Fatal("second version did not follow the retry:", err)
	}
	var published int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM goal_completion_outbox WHERE goal_id=$1 AND published_at IS NOT NULL",
		goal).Scan(&published); err != nil || published != 2 {
		t.Fatal("outbox did not remember broker acknowledgments:", published, err)
	}
}

func TestPostgresGoalOutboxDeliversToKafka(t *testing.T) {
	broker := os.Getenv("GOAL_COMPLETION_KAFKA_TEST_BROKER")
	if broker == "" {
		t.Skip("GOAL_COMPLETION_KAFKA_TEST_BROKER is required")
	}
	host, _, err := net.SplitHostPort(broker)
	if err != nil || (host != "127.0.0.1" && host != "localhost") {
		t.Fatal("use a disposable local Kafka broker")
	}
	pool := goalOutboxDB(t)
	goal, events := seedGoalEvents(t, pool)
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordDeliveryTimeout(8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		goalcompletion.Topic: {0: kgo.NewOffset().AtStart()},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	publisher := NewGoalPublisher(pool, producer, slog.New(slog.DiscardHandler))
	for range events {
		if found, err := publisher.Step(ctx); !found || err != nil {
			t.Fatal("could not send the saved event to Kafka:", found, err)
		}
	}
	var got []goalcompletion.Event
	for len(got) < len(events) && ctx.Err() == nil {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		for _, record := range fetches.Records() {
			if string(record.Key) != goal {
				continue
			}
			var event goalcompletion.Event
			if err := json.Unmarshal(record.Value, &event); err != nil {
				t.Fatal(err)
			}
			if err := event.Validate(); err != nil {
				t.Fatal(err)
			}
			got = append(got, event)
		}
	}
	if len(got) != len(events) {
		t.Fatal("Kafka did not deliver both transitions:", len(got), ctx.Err())
	}
	for i, event := range got {
		if event.EventID != events[i].EventID || event.AggregateVersion != int64(i+1) ||
			event.Payload.BusinessAwardKey != events[0].Payload.BusinessAwardKey {
			t.Fatal("Kafka changed transition identity or order:", event)
		}
	}
}
