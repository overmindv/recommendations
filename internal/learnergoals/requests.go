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
	"github.com/overmindv/recommendations/internal/planner"
)

type command struct {
	Operation string `json:"operation"`
	GoalID    string `json:"goal_id,omitempty"`
	Version   int64  `json:"version,omitempty"`
	Body      any    `json:"body,omitempty"`
}

func executeWrite[T any](ctx context.Context, store *Postgres, actor, requestID string, cmd command,
	write func(pgx.Tx, time.Time) (T, error)) (T, error) {
	var result, zero T
	if !planner.ValidID(actor) || !planner.ValidID(requestID) {
		return zero, ErrInvalid
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return zero, fmt.Errorf("encode learner goal request: %w", err)
	}
	digest := sha256.Sum256(payload)
	err = pgx.BeginTxFunc(ctx, store.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		// После ожидания блокировки запросы должны увидеть записи предыдущей транзакции.
		if _, lockErr := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 198))", actor); lockErr != nil {
			return lockErr
		}
		now := store.now().UTC()
		var savedHash, savedResponse []byte
		queryErr := tx.QueryRow(ctx, `
			SELECT fingerprint, response FROM learner_goal_requests
			WHERE user_id = $1 AND request_id = $2 AND expires_at > $3`, actor, requestID, now).Scan(&savedHash, &savedResponse)
		if queryErr == nil {
			if !bytes.Equal(savedHash, digest[:]) {
				return ErrRequestUsed
			}

			return json.Unmarshal(savedResponse, &result)
		}
		if !errors.Is(queryErr, pgx.ErrNoRows) {
			return queryErr
		}
		var count int
		if err = tx.QueryRow(ctx, "SELECT count(*) FROM learner_goal_requests WHERE user_id = $1 AND expires_at > $2", actor, now).
			Scan(&count); err != nil {
			return err
		}
		if count >= maxRequests {
			return ErrRequestLimit
		}
		result, err = write(tx, now)
		if err != nil {
			return err
		}
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
