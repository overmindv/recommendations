package learnergoals

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	completionqueue "github.com/overmindv/recommendations/internal/completion/queue"
	"github.com/overmindv/recommendations/internal/planner"
)

type command struct {
	Operation string `json:"operation"`
	GoalID    string `json:"goal_id,omitempty"`
	Version   int64  `json:"version,omitempty"`
	Body      any    `json:"body,omitempty"`
}

func LockBudget(ctx context.Context, tx pgx.Tx, actor string) error {
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 198))", actor)

	return err
}

func TryLockBudget(ctx context.Context, tx pgx.Tx, actor string) (bool, error) {
	var locked bool
	err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 198))", actor).Scan(&locked)

	return locked, err
}

func executeWrite[T any](ctx context.Context, store *Postgres, actor, requestID string, cmd command,
	validate func(context.Context) error, write func(pgx.Tx, time.Time) (T, error)) (T, error) {
	var result, zero T
	if !planner.ValidID(actor) || !planner.ValidID(requestID) {
		return zero, ErrInvalid
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return zero, fmt.Errorf("encode learner goal request: %w", err)
	}
	digest := sha256.Sum256(payload)
	if validate != nil {
		// Проверка каталога не должна держать транзакцию или мешать возврату сохраненного ответа.
		replay := func() (bool, error) {
			return readReplay(ctx, store.pool, actor, requestID, store.now().UTC(), digest[:], &result)
		}
		if found, replayErr := replay(); found || replayErr != nil {
			return result, replayErr
		}
		if validateErr := validate(ctx); validateErr != nil {
			if found, replayErr := replay(); found || replayErr != nil {
				return result, replayErr
			}

			return zero, validateErr
		}
	}
	err = pgx.BeginTxFunc(ctx, store.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		// После ожидания блокировки запросы должны увидеть записи предыдущей транзакции.
		if lockErr := LockBudget(ctx, tx, actor); lockErr != nil {
			return lockErr
		}
		now := store.now().UTC()
		if found, replayErr := readReplay(ctx, tx, actor, requestID, now, digest[:], &result); found || replayErr != nil {
			return replayErr
		}
		var count int
		if countErr := tx.QueryRow(ctx, "SELECT count(*) FROM learner_goal_requests WHERE user_id = $1 AND expires_at > $2", actor, now).
			Scan(&count); countErr != nil {
			return countErr
		}
		if count >= maxRequests {
			return ErrRequestLimit
		}
		value, writeErr := write(tx, now)
		if writeErr != nil {
			return writeErr
		}
		if store.completionChecks && (cmd.Operation == "update" || cmd.Operation == "delete") {
			if scheduleErr := completionqueue.Request(ctx, tx, []string{actor}); scheduleErr != nil {
				return scheduleErr
			}
		}
		result = value
		response, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			return marshalErr
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO learner_goal_requests (user_id, request_id, fingerprint, response, expires_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (user_id, request_id) DO UPDATE
			SET fingerprint = EXCLUDED.fingerprint, response = EXCLUDED.response, expires_at = EXCLUDED.expires_at`,
			actor, requestID, digest[:], response, now.Add(idempotencyTTL))

		return err
	})
	if err != nil {
		return zero, fmt.Errorf("write learner goal: %w", err)
	}

	return result, nil
}

func readReplay(ctx context.Context, q queryer, actor, requestID string, now time.Time, digest []byte, result any) (bool, error) {
	var savedHash, savedResponse []byte
	err := q.QueryRow(ctx, `
		SELECT fingerprint, response FROM learner_goal_requests
		WHERE user_id = $1 AND request_id = $2 AND expires_at > $3`, actor, requestID, now).Scan(&savedHash, &savedResponse)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	case !bytes.Equal(savedHash, digest):
		return true, ErrRequestUsed
	default:
		return true, json.Unmarshal(savedResponse, result)
	}
}

// PruneExpiredRequests очищает одну ограниченную партию истекших ключей.
func (p *Postgres) PruneExpiredRequests(ctx context.Context, limit int) (int64, error) {
	if limit < 1 || limit > 1000 {
		return 0, ErrInvalid
	}
	tag, err := p.pool.Exec(ctx, `
		WITH expired AS (
		    SELECT user_id, request_id FROM learner_goal_requests
		    WHERE expires_at <= $1 ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED
		)
		DELETE FROM learner_goal_requests r USING expired e
		WHERE r.user_id = e.user_id AND r.request_id = e.request_id`, p.now().UTC(), limit)

	return tag.RowsAffected(), err
}
