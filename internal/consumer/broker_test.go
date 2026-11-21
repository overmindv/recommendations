package consumer

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/overmindv/content/pkg/api/activity"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaEvidenceCommit(t *testing.T) {
	t.Parallel()

	brokers := os.Getenv("LEARNING_EVIDENCE_TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("LEARNING_EVIDENCE_TEST_KAFKA_BROKERS must point to a dedicated broker")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	pool := evidenceDB(t)
	options := kgo.SeedBrokers(strings.Split(brokers, ",")...)
	producer, err := kgo.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	ns := namespace(t)
	task := outcomeFixture(ns, 1, 2, 3, 4)
	read := readFixture(ns, 5, 3, 6, 7)
	records := []*kgo.Record{
		evidenceRecord(t, TasksTopic, task.AggregateID, 0, task),
		evidenceRecord(t, TasksTopic, task.AggregateID, 0, task),
		evidenceRecord(t, activity.DefaultLearningTopic, read.AggregateID, 0, read),
		{Topic: TasksTopic, Key: []byte(task.AggregateID), Value: []byte(`{"run_mode":"samples"}`)},
	}
	if produceErr := producer.ProduceSync(ctx, records...).FirstErr(); produceErr != nil {
		t.Fatal(produceErr)
	}
	worker := NewWorker(NewPostgres(pool), activity.DefaultLearningTopic, quietLog(), options)
	client, err := kgo.NewClient(worker.opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseAllowingRebalance()
	done := make(chan error, 1)
	go func() { done <- worker.consume(ctx, client, 10*time.Second) }()
	defer func() {
		cancel()
		if consumeErr := <-done; consumeErr != nil {
			t.Error(consumeErr)
		}
	}()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		offsets := client.CommittedOffsets()
		committed := true
		for _, row := range records {
			if offsets[row.Topic][row.Partition].Offset <= row.Offset {
				committed = false

				break
			}
		}
		if committed {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("learning evidence offsets not committed:", ctx.Err())
		}
	}
	var outcomes, reads, rejected int
	bad := records[len(records)-1]
	err = pool.QueryRow(ctx, `
		SELECT
		    (SELECT count(*) FROM learning_task_outcomes WHERE learner_id=$1 AND submission_id=$2),
		    (SELECT count(*) FROM learning_material_reads WHERE learner_id=$1 AND material_id=$3),
		    (SELECT count(*) FROM learning_evidence_rejections WHERE topic=$4 AND partition=$5 AND message_offset=$6)`,
		task.Payload.LearnerID, task.Payload.SubmissionID, read.AggregateID, bad.Topic, bad.Partition, bad.Offset).
		Scan(&outcomes, &reads, &rejected)
	if err != nil || outcomes != 1 || reads != 1 || rejected != 1 {
		t.Fatal("commit without matching database records:", outcomes, reads, rejected, err)
	}
}
