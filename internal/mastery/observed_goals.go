package mastery

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type observedGoal struct {
	id      string
	actor   string
	version int64
	topics  []string
}

func (p *Postgres) RefreshObservedGoals(ctx context.Context, tx pgx.Tx, ids []string) ([]string, error) {
	if len(ids) == 0 || len(ids) > 1024 {
		return nil, errors.New("invalid affected goal count")
	}
	rows, err := tx.Query(ctx, `
		SELECT id::text,user_id::text,version,target_topics FROM learner_goals
		WHERE id=ANY($1::uuid[]) AND active ORDER BY user_id,id FOR UPDATE`, ids)
	if err != nil {
		return nil, err
	}
	goals, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (observedGoal, error) {
		var g observedGoal
		err := row.Scan(&g.id, &g.actor, &g.version, &g.topics)
		return g, err
	})
	if err != nil || len(goals) == 0 {
		return nil, err
	}
	var computed time.Time
	if err := tx.QueryRow(ctx, "SELECT statement_timestamp()").Scan(&computed); err != nil {
		return nil, err
	}
	batch := &pgx.Batch{}
	active := make([]string, 0, len(goals))
	var samples []evidence
	var actor string
	var truncated bool
	for _, g := range goals {
		if g.actor != actor {
			actor = g.actor
			samples, truncated, err = loadEvidence(ctx, tx, actor, computed)
			if err != nil {
				return nil, err
			}
		}
		scores, used := summarize(p.policy, g.topics, samples, truncated)
		args, err := snapshotArgs(Snapshot{GoalID: g.id, GoalVersion: g.version, Policy: p.policy,
			ComputedAt: computed, Topics: scores, Truncated: truncated}, used)
		if err != nil {
			return nil, err
		}
		batch.Queue(saveSnapshotSQL, args...)
		active = append(active, g.id)
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return nil, err
	}
	return active, nil
}
