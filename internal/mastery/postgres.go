package mastery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/planner"
)

var ErrPolicyConflict = errors.New("mastery policy revision already has different settings")

type Postgres struct {
	pool   *pgxpool.Pool
	policy Policy
}

func NewPostgres(ctx context.Context, pool *pgxpool.Pool, policy Policy) (*Postgres, error) {
	if !policy.Valid() {
		return nil, errors.New("invalid mastery policy")
	}
	raw, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO mastery_policies (revision, configuration) VALUES ($1,$2::jsonb) ON CONFLICT DO NOTHING`,
		policy.Revision, raw); err != nil {
		return nil, fmt.Errorf("register mastery policy: %w", err)
	}
	var same bool
	if err := pool.QueryRow(ctx, "SELECT configuration=$2::jsonb FROM mastery_policies WHERE revision=$1",
		policy.Revision, raw).Scan(&same); err != nil {
		return nil, fmt.Errorf("verify mastery policy: %w", err)
	}
	if !same {
		return nil, ErrPolicyConflict
	}
	return &Postgres{pool: pool, policy: policy}, nil
}

func (p *Postgres) Get(ctx context.Context, actor, goal string) (Snapshot, error) {
	if !planner.ValidID(actor) || !planner.ValidID(goal) {
		return Snapshot{}, learnergoals.ErrInvalid
	}
	result := Snapshot{GoalID: goal, Policy: p.policy}
	var raw []byte
	err := p.pool.QueryRow(ctx, `
		SELECT b.goal_version,b.computed_at,b.topics,b.truncated
		FROM learner_goals AS g JOIN mastery_baselines AS b ON b.goal_id=g.id AND b.goal_version=g.version
		WHERE g.user_id=$1 AND g.id=$2 AND b.policy_revision=$3`, actor, goal, p.policy.Revision).
		Scan(&result.GoalVersion, &result.ComputedAt, &raw, &result.Truncated)
	if errors.Is(err, pgx.ErrNoRows) {
		return Snapshot{}, learnergoals.ErrNotFound
	}
	if err != nil {
		return Snapshot{}, fmt.Errorf("read mastery baseline: %w", err)
	}
	if err := json.Unmarshal(raw, &result.Topics); err != nil {
		return Snapshot{}, fmt.Errorf("decode mastery baseline: %w", err)
	}
	return result, nil
}

func (p *Postgres) Refresh(ctx context.Context, actor, goal string, version int64) (Snapshot, error) {
	if version < 1 || !planner.ValidID(actor) || !planner.ValidID(goal) {
		return Snapshot{}, learnergoals.ErrInvalid
	}
	snapshot := Snapshot{GoalID: goal, Policy: p.policy}
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		var topics []string
		err := tx.QueryRow(ctx, `
			SELECT version,target_topics,clock_timestamp() FROM learner_goals WHERE id=$1 AND user_id=$2 FOR UPDATE`,
			goal, actor).Scan(&snapshot.GoalVersion, &topics, &snapshot.ComputedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return learnergoals.ErrNotFound
		}
		if err != nil {
			return err
		}
		if snapshot.GoalVersion != version {
			return learnergoals.ErrConflict
		}
		rows, truncated, err := loadEvidence(ctx, tx, actor, snapshot.ComputedAt)
		if err != nil {
			return err
		}
		result, used := summarize(p.policy, topics, rows, truncated)
		snapshot.Topics, snapshot.Truncated = result, truncated
		return saveSnapshot(ctx, tx, snapshot, used)
	})
	if err != nil {
		return Snapshot{}, fmt.Errorf("refresh mastery baseline: %w", err)
	}
	return snapshot, nil
}

const evidenceSQL = `
	WITH recent AS MATERIALIZED (
	    SELECT submission_id,graded_at
	    FROM learning_task_outcomes
	    WHERE learner_id=$1 AND evidence_kind='independent' AND graded_at <= $2
	    ORDER BY graded_at DESC, submission_id LIMIT $3
	)
	SELECT r.task_id::text,r.task_version_id::text,r.submission_id::text,r.event_id::text,r.correction_version,
	       r.graded_at,r.verdict,r.topic_ids,r.topic_weights,r.submission_id = first.submission_id
	FROM recent
	JOIN LATERAL (
	    SELECT o.*
	    FROM learning_task_outcomes AS o
	    WHERE o.submission_id=recent.submission_id
	    LIMIT 1
	) AS r ON true
	JOIN LATERAL (
	    SELECT o.submission_id
	    FROM learning_task_outcomes AS o
	    WHERE learner_id=$1 AND task_id=r.task_id AND evidence_kind='independent' AND graded_at <= $2
	    ORDER BY graded_at,submission_id LIMIT 1
	) AS first ON true
	ORDER BY recent.graded_at DESC,recent.submission_id`

func loadEvidence(ctx context.Context, tx pgx.Tx, actor string, until time.Time) ([]evidence, bool, error) {
	rows, err := tx.Query(ctx, evidenceSQL, actor, until, maxEvidence+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	result := make([]evidence, 0, 16)
	var count int
	for rows.Next() {
		count++
		if count > maxEvidence {
			break
		}
		var row evidence
		var first bool
		if err := rows.Scan(&row.TaskID, &row.TaskVersionID, &row.SubmissionID, &row.EventID, &row.CorrectionVersion,
			&row.GradedAt, &row.Verdict, &row.Topics, &row.Weights, &first); err != nil {
			return nil, false, err
		}
		if first {
			result = append(result, row)
		}
	}
	return result, count > maxEvidence, rows.Err()
}

func saveSnapshot(ctx context.Context, tx pgx.Tx, snapshot Snapshot, used []evidence) error {
	args, err := snapshotArgs(snapshot, used)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, saveSnapshotSQL, args...)
	return err
}

func snapshotArgs(snapshot Snapshot, used []evidence) ([]any, error) {
	topics, err := json.Marshal(snapshot.Topics)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(used)
	if err != nil {
		return nil, err
	}
	return []any{snapshot.GoalID, snapshot.GoalVersion, snapshot.Policy.Revision, snapshot.ComputedAt, topics, raw, snapshot.Truncated}, nil
}

const saveSnapshotSQL = `
		INSERT INTO mastery_baselines (goal_id,goal_version,policy_revision,computed_at,topics,evidence,truncated)
		VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7)
		ON CONFLICT (goal_id) DO UPDATE SET
		    goal_version=EXCLUDED.goal_version, policy_revision=EXCLUDED.policy_revision,
		    computed_at=EXCLUDED.computed_at, topics=EXCLUDED.topics, evidence=EXCLUDED.evidence, truncated=EXCLUDED.truncated`
