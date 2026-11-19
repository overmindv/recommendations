package studyplans

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrProjectionStale = errors.New("study catalog projection is stale")
	errFreshnessRead   = errors.New("cannot read study projection checkpoints")
)

type Freshness struct {
	Status     string     `json:"status"`
	CheckedAt  *time.Time `json:"checked_at,omitempty"`
	EntitiesAt *time.Time `json:"entities_synced_at,omitempty"`
	TasksAt    *time.Time `json:"tasks_synced_at,omitempty"`
	ContentAt  *time.Time `json:"content_synced_at,omitempty"`
}

const (
	freshCatalog       = "fresh"
	staleCatalog       = "stale"
	unavailableCatalog = "unavailable"
)

type freshnessQuery interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

const freshnessSQL = `
		SELECT statement_timestamp(),e.last_synced_at,t.last_synced_at,c.last_synced_at,
		       e.active_generation>0 AND NOT e.rebuild AND e.replay_watermark IS NULL
		       AND t.active_generation>0 AND c.active_generation>0
		       AND NOT (t.progress ? 'pending') AND NOT (t.progress ? 'replay_ceiling') AND NOT (t.progress ? 'lagging')
		       AND NOT (c.progress ? 'pending') AND NOT (c.progress ? 'lagging')
		FROM catalog_projection_state AS e
		CROSS JOIN resource_projection_checkpoints AS t CROSS JOIN resource_projection_checkpoints AS c
		WHERE e.singleton AND t.origin='tasks' AND c.origin='content'`

func readFreshness(ctx context.Context, q freshnessQuery, maxAge time.Duration) (Freshness, error) {
	return scanFreshness(q.QueryRow(ctx, freshnessSQL), maxAge)
}

func scanFreshness(row pgx.Row, maxAge time.Duration) (Freshness, error) {
	value := Freshness{Status: staleCatalog}
	var checked time.Time
	var ready bool
	err := row.Scan(&checked, &value.EntitiesAt, &value.TasksAt, &value.ContentAt, &ready)
	if err != nil {
		return Freshness{Status: unavailableCatalog}, err
	}
	checked = checked.UTC()
	value.CheckedAt = &checked
	for _, stamp := range []*time.Time{value.EntitiesAt, value.TasksAt, value.ContentAt} {
		if stamp != nil {
			*stamp = stamp.UTC()
		}
		if stamp == nil || stamp.After(checked) || checked.Sub(*stamp) > maxAge {
			ready = false
		}
	}
	if ready {
		value.Status = freshCatalog
	}
	return value, nil
}

func readSourcesWithFreshness(ctx context.Context, tx pgx.Tx, actor, goal string, maxAge time.Duration) (Sources, Freshness, error) {
	batch := &pgx.Batch{}
	batch.Queue(sourcesSQL, goal, actor)
	batch.Queue(freshnessSQL)
	rows := tx.SendBatch(ctx, batch)
	source, sourceErr := scanSources(rows.QueryRow(), actor)
	value, err := scanFreshness(rows.QueryRow(), maxAge)
	closeErr := rows.Close()
	if sourceErr != nil {
		return Sources{}, Freshness{}, sourceErr
	}
	if closeErr != nil || (err != nil && !errors.Is(err, pgx.ErrNoRows)) {
		return Sources{}, Freshness{}, errors.Join(errFreshnessRead, err, closeErr)
	}
	return source, value, nil
}
