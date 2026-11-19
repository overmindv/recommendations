package completion

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/learnergoals"
)

type Worker struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewWorker(pool *pgxpool.Pool, log *slog.Logger) *Worker {
	return &Worker{pool: pool, log: log}
}

func (w *Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		found, err := w.Step(ctx)
		if err == nil && found {
			continue
		}
		if err != nil && ctx.Err() == nil {
			w.log.Warn("goal completion check will retry")
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

func (w *Worker) Step(ctx context.Context) (bool, error) {
	work, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var learner, token string
	var now time.Time
	err := pgx.BeginFunc(work, w.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(work, `
			SELECT learner_id::text,token::text,statement_timestamp() FROM goal_completion_checks
			WHERE ready_at<=statement_timestamp()
			ORDER BY ready_at,learner_id LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&learner, &token, &now)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		// Команды пользователя берут блокировку пользователя до записи в очередь.
		locked, err := learnergoals.TryLockBudget(work, tx, learner)
		if err != nil {
			return err
		}
		if !locked {
			_, err := tx.Exec(work, `
				UPDATE goal_completion_checks SET ready_at=statement_timestamp()+interval '250 milliseconds'
				WHERE learner_id=$1 AND token=$2`, learner, token)
			return err
		}
		if err := reconcile(work, tx, learner, now); err != nil {
			return err
		}
		_, err = tx.Exec(work, "DELETE FROM goal_completion_checks WHERE learner_id=$1 AND token=$2", learner, token)
		return err
	})
	if err != nil && learner != "" {
		retry, done := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer done()
		_, retryErr := w.pool.Exec(retry, `
			UPDATE goal_completion_checks SET ready_at=statement_timestamp()+interval '5 seconds'
			WHERE learner_id=$1 AND token=$2`, learner, token)
		err = errors.Join(err, retryErr)
	}
	return learner != "", err
}
