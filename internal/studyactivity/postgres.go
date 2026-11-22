package studyactivity

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/projection/resources"
)

type Catalog interface {
	VerifyVersions(context.Context, resources.Origin, []resources.VersionKey) (map[resources.VersionKey]resources.Candidate, error)
}

type Postgres struct {
	pool    *pgxpool.Pool
	ttl     time.Duration
	catalog Catalog
}

func NewPostgres(pool *pgxpool.Pool, ttl time.Duration, catalog Catalog) (*Postgres, error) {
	if pool == nil || catalog == nil || ttl < time.Hour || ttl > 30*24*time.Hour {
		return nil, ErrInvalid
	}

	return &Postgres{pool: pool, ttl: ttl, catalog: catalog}, nil
}

func (p *Postgres) Capture(ctx context.Context, actor string, in Input) (Receipt, error) {
	// Проверка до SQL защищает от невалидных UUID и лишних запросов каталога.
	if err := in.validateShape(actor); err != nil {
		return Receipt{}, err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Один actor не может удвоить интервал через параллельные вкладки/реплики.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,205))", actor); err != nil {
		return Receipt{}, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return Receipt{}, err
	}
	// Нормализуем время до точности PostgreSQL, чтобы TTL и replay совпадали.
	in.OccurredAt = in.OccurredAt.UTC().Truncate(time.Microsecond)
	payload, err := json.Marshal(in)
	if err != nil {
		return Receipt{}, err
	}
	hash := sha256.Sum256(payload)
	result := Receipt{ClientEventID: in.ClientEventID, Trust: Trust}
	var oldHash []byte
	err = tx.QueryRow(ctx, `SELECT payload_hash, accepted_seconds, expires_at FROM study_activity
		WHERE actor_id=$1 AND client_event_id=$2`, actor, in.ClientEventID).Scan(&oldHash, &result.AcceptedSeconds, &result.ExpiresAt)
	if err == nil {
		result.ExpiresAt = result.ExpiresAt.UTC()
		if !bytes.Equal(oldHash, hash[:]) {
			return Receipt{}, ErrConflict
		}
		if !result.ExpiresAt.After(now) {
			return Receipt{}, ErrInvalid
		}

		return result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Receipt{}, err
	}
	if err = in.Validate(actor, now, p.ttl); err != nil {
		return Receipt{}, err
	}
	window := now.UTC().Truncate(time.Hour)
	var lastReceived, lastWindow time.Time
	var count int
	err = tx.QueryRow(ctx, `SELECT received_at, window_start, event_count
		FROM study_activity_rate WHERE actor_id=$1`, actor).Scan(&lastReceived, &lastWindow, &count)
	if err == nil {
		if now.Sub(lastReceived) < MinInterval || (lastWindow.Equal(window) && count >= MaxHourlyEvents) {
			return Receipt{}, ErrRate
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return Receipt{}, err
	}
	var previous Input
	var previousRaw []byte
	var receivedBefore time.Time
	err = tx.QueryRow(ctx, `SELECT payload, received_at FROM study_activity WHERE actor_id=$1
		ORDER BY received_at DESC LIMIT 1`, actor).Scan(&previousRaw, &receivedBefore)
	if err == nil {
		if err = json.Unmarshal(previousRaw, &previous); err != nil {
			return Receipt{}, err
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return Receipt{}, err
	}
	key := resources.VersionKey{ID: in.ResourceID, RevisionID: in.ResourceRevisionID}
	available, err := p.catalog.VerifyVersions(ctx, resources.Origin(in.ResourceKind), []resources.VersionKey{key})
	if err != nil {
		return Receipt{}, err
	}
	if _, ok := available[key]; !ok {
		return Receipt{}, ErrResource
	}
	result.AcceptedSeconds = AcceptedTime(previous, in, receivedBefore, now)
	// Та же граница, что у проверки возраста нового события: после очистки
	// future-skew событие нельзя вставить повторно до истечения client TTL.
	result.ExpiresAt = in.OccurredAt.Add(p.ttl).UTC().Truncate(time.Microsecond)
	_, err = tx.Exec(ctx, `INSERT INTO study_activity
		(actor_id,client_event_id,payload_hash,payload,accepted_seconds,received_at,expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, actor, in.ClientEventID, hash[:], payload, result.AcceptedSeconds, now, result.ExpiresAt)
	if err != nil {
		return Receipt{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO study_activity_rate(actor_id,received_at,window_start,event_count,expires_at)
		VALUES ($1,$2,$3,1,$4) ON CONFLICT (actor_id) DO UPDATE SET
		received_at=EXCLUDED.received_at, window_start=EXCLUDED.window_start,
		event_count=CASE WHEN study_activity_rate.window_start=EXCLUDED.window_start
		THEN study_activity_rate.event_count+1 ELSE 1 END, expires_at=EXCLUDED.expires_at`, actor, now, window, now.Add(p.ttl))
	if err != nil {
		return Receipt{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Receipt{}, err
	}

	return result, nil
}

// Prune удаляет только ограниченный пакет новой таблицы, без чужих evidence/планов.
func (p *Postgres) Prune(ctx context.Context) error {
	_, err := p.pool.Exec(ctx, `WITH expired AS (
		SELECT actor_id, client_event_id FROM study_activity WHERE expires_at<=clock_timestamp()
		ORDER BY expires_at LIMIT 1000 FOR UPDATE SKIP LOCKED
	) DELETE FROM study_activity s USING expired e
	WHERE s.actor_id=e.actor_id AND s.client_event_id=e.client_event_id`)
	if err != nil {
		return err
	}
	_, err = p.pool.Exec(ctx, `WITH expired AS (
		SELECT actor_id FROM study_activity_rate WHERE expires_at<=clock_timestamp()
		ORDER BY expires_at LIMIT 1000 FOR UPDATE SKIP LOCKED
	) DELETE FROM study_activity_rate s USING expired e WHERE s.actor_id=e.actor_id`)

	return err
}
