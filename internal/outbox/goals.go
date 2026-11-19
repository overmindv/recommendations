package outbox

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/pkg/api/goalcompletion"
	"github.com/twmb/franz-go/pkg/kgo"
)

const goalBatchSize = 16

type goalRecord struct {
	id      string
	goal    string
	token   string
	payload []byte
}

type goalProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

type GoalPublisher struct {
	pool     *pgxpool.Pool
	producer goalProducer
	log      *slog.Logger
}

func NewGoalPublisher(pool *pgxpool.Pool, producer *kgo.Client, log *slog.Logger) *GoalPublisher {
	return &GoalPublisher{pool: pool, producer: producer, log: log}
}

func (p *GoalPublisher) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		found, err := p.Step(ctx)
		if err == nil && found {
			continue
		}
		if err != nil && ctx.Err() == nil {
			p.log.Warn("goal completion publication will retry")
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
		}
	}
	return nil
}

const readyGoalEvent = "GREATEST(e.ready_at, COALESCE(e.leased_until, '-infinity'::timestamptz))"

func (p *GoalPublisher) claim(ctx context.Context) ([]goalRecord, error) {
	rows, err := p.pool.Query(ctx, `
		WITH ready AS (
		    SELECT e.event_id FROM goal_completion_outbox AS e
		    WHERE e.published_at IS NULL AND `+readyGoalEvent+`<=statement_timestamp()
		      AND NOT EXISTS (
		          SELECT 1 FROM goal_completion_outbox AS earlier
		          WHERE earlier.goal_id=e.goal_id AND earlier.completion_version<e.completion_version
		            AND earlier.published_at IS NULL
		      )
		    ORDER BY `+readyGoalEvent+`,e.goal_id LIMIT $1 FOR UPDATE OF e SKIP LOCKED
		), leased AS (
		    UPDATE goal_completion_outbox AS e
		    SET lease_token=gen_random_uuid(),leased_until=statement_timestamp()+interval '30 seconds'
		    FROM ready WHERE e.event_id=ready.event_id
		    RETURNING e.event_id,e.goal_id,e.lease_token,e.payload
		)
		SELECT event_id::text,goal_id::text,lease_token::text,payload FROM leased`, goalBatchSize)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (goalRecord, error) {
		var event goalRecord
		err := row.Scan(&event.id, &event.goal, &event.token, &event.payload)
		return event, err
	})
}

func (p *GoalPublisher) Step(ctx context.Context) (bool, error) {
	claim, done := context.WithTimeout(ctx, 3*time.Second)
	work, err := p.claim(claim)
	done()
	if err != nil || len(work) == 0 {
		return false, err
	}
	messages := make([]*kgo.Record, 0, len(work))
	byMessage := make(map[*kgo.Record]goalRecord, len(work))
	for _, event := range work {
		msg := &kgo.Record{Topic: goalcompletion.Topic, Key: []byte(event.goal), Value: event.payload}
		messages = append(messages, msg)
		byMessage[msg] = event
	}
	send, sent := context.WithTimeout(ctx, 8*time.Second)
	receipts := p.producer.ProduceSync(send, messages...)
	sent()
	batch := &pgx.Batch{}
	var failed bool
	for _, receipt := range receipts {
		event := byMessage[receipt.Record]
		failed = failed || receipt.Err != nil
		batch.Queue(`
			UPDATE goal_completion_outbox
			SET lease_token=NULL,leased_until=NULL,
			    published_at=CASE WHEN $3 THEN statement_timestamp() ELSE NULL END,
			    ready_at=CASE WHEN $3 THEN ready_at ELSE statement_timestamp()+interval '5 seconds' END
			WHERE event_id=$1 AND lease_token=$2 AND published_at IS NULL`, event.id, event.token, receipt.Err == nil)
	}
	finish, finished := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer finished()
	if err := p.pool.SendBatch(finish, batch).Close(); err != nil {
		return true, err
	}
	if failed {
		return true, errors.New("goal completion delivery failed")
	}
	return true, nil
}
