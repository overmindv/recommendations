package goals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct{ pool *pgxpool.Pool }

func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

func (p *Postgres) Get(ctx context.Context, userID string) (Goal, error) {
	var goal Goal
	var raw []byte
	err := p.pool.QueryRow(ctx, `SELECT topic_ids, minutes, session_count, revision, updated_at FROM practice_goals WHERE user_id = $1`, userID).
		Scan(&raw, &goal.Minutes, &goal.SessionCount, &goal.Revision, &goal.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Goal{}, ErrNotFound
	}
	if err != nil {
		return Goal{}, fmt.Errorf("read practice goal: %w", err)
	}
	if err := json.Unmarshal(raw, &goal.TopicIDs); err != nil {
		return Goal{}, fmt.Errorf("decode practice goal: %w", err)
	}
	return goal, nil
}

func (p *Postgres) Save(ctx context.Context, userID string, input Input) (Goal, error) {
	if err := Validate(input); err != nil {
		return Goal{}, err
	}
	raw, err := json.Marshal(input.TopicIDs)
	if err != nil {
		return Goal{}, fmt.Errorf("encode practice goal: %w", err)
	}
	var goal Goal
	err = p.pool.QueryRow(ctx, `
		INSERT INTO practice_goals (user_id, topic_ids, minutes, session_count)
		VALUES ($1, $2::jsonb, $3, $4)
		ON CONFLICT (user_id) DO UPDATE SET topic_ids = EXCLUDED.topic_ids, minutes = EXCLUDED.minutes,
			session_count = EXCLUDED.session_count, revision = practice_goals.revision + 1, updated_at = now()
		RETURNING topic_ids, minutes, session_count, revision, updated_at`, userID, raw, input.Minutes, max(1, input.SessionCount)).
		Scan(&raw, &goal.Minutes, &goal.SessionCount, &goal.Revision, &goal.UpdatedAt)
	if err != nil {
		return Goal{}, fmt.Errorf("save practice goal: %w", err)
	}
	if err := json.Unmarshal(raw, &goal.TopicIDs); err != nil {
		return Goal{}, fmt.Errorf("decode saved practice goal: %w", err)
	}
	return goal, nil
}

func (p *Postgres) Delete(ctx context.Context, userID string) error {
	if _, err := p.pool.Exec(ctx, `DELETE FROM practice_goals WHERE user_id = $1`, userID); err != nil {
		return fmt.Errorf("delete practice goal: %w", err)
	}
	return nil
}
