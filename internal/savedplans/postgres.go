package savedplans

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store interface {
	Get(context.Context, string, string) (Plan, error)
	List(context.Context, string) ([]Plan, error)
	Create(context.Context, string, Input) (Plan, error)
	Change(context.Context, string, string, string, Input) (Plan, error)
	Clone(context.Context, string, string, string) (Plan, error)
}
type Postgres struct{ Pool *pgxpool.Pool }

func scan(row pgx.Row) (Plan, error) {
	var p Plan
	var raw []byte
	err := row.Scan(&p.ID, &p.Owner, &raw, &p.Public, &p.Revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, ErrNotFound
	}
	if err != nil {
		return p, err
	}
	err = json.Unmarshal(raw, &p.Document)

	return p, err
}

const columns = "id, owner_id, document, is_public, revision"

func (p *Postgres) Get(ctx context.Context, id, actor string) (Plan, error) {
	plan, err := scan(p.Pool.QueryRow(ctx, "SELECT "+columns+" FROM saved_learning_plans WHERE id=$1 AND (owner_id::text=$2 OR is_public)", id, actor))

	return Expose(plan, actor), err
}
func (p *Postgres) List(ctx context.Context, actor string) ([]Plan, error) {
	rows, err := p.Pool.Query(ctx, "SELECT "+columns+" FROM saved_learning_plans WHERE owner_id::text=$1 ORDER BY updated_at DESC LIMIT 50", actor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Plan{}
	for rows.Next() {
		plan, err := scan(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, Summary(Expose(plan, actor)))
	}

	return result, rows.Err()
}
func (p *Postgres) Create(ctx context.Context, actor string, in Input) (Plan, error) {
	in.Document = Reset(in.Document)
	if len(in.Document.Materials)+len(in.Document.Tasks) == 0 {
		return Plan{}, ErrInvalid
	}
	if err := Validate(in.Document); err != nil {
		return Plan{}, err
	}
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return Plan{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	plan, err := create(ctx, tx, actor, in)
	if err != nil {
		return Plan{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Plan{}, err
	}

	return plan, nil
}
func create(ctx context.Context, tx pgx.Tx, actor string, in Input) (Plan, error) {
	// Лимит проверяется под общим для владельца lock, включая повторные запросы.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", actor); err != nil {
		return Plan{}, err
	}
	existing, err := scan(tx.QueryRow(ctx, "SELECT "+columns+" FROM saved_learning_plans WHERE id=$1", in.ID))
	if err == nil {
		if existing.Owner != actor {
			return Plan{}, ErrNotFound
		}

		return Expose(existing, actor), nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Plan{}, err
	}
	var count int
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM saved_learning_plans WHERE owner_id=$1", actor).Scan(&count); err != nil {
		return Plan{}, err
	}
	if count >= 50 {
		return Plan{}, ErrLimit
	}
	raw, _ := json.Marshal(in.Document)
	plan, err := scan(tx.QueryRow(ctx, "INSERT INTO saved_learning_plans(id,owner_id,document) VALUES($1,$2,$3) RETURNING "+columns, in.ID, actor, raw))
	if err != nil {
		return Plan{}, err
	}

	return Expose(plan, actor), nil
}
func (p *Postgres) Change(ctx context.Context, id, actor, op string, in Input) (Plan, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return Plan{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	plan, err := scan(tx.QueryRow(ctx, "SELECT "+columns+" FROM saved_learning_plans WHERE id=$1 AND owner_id::text=$2 FOR UPDATE", id, actor))
	if err != nil {
		return Plan{}, err
	}
	if in.Revision != plan.Revision {
		return Plan{}, ErrConflict
	}
	switch op {
	case "visibility":
		plan.Public = in.Public
	case "material":
		if !validStatus(in.Status) {
			return Plan{}, ErrInvalid
		}
		found := false
		for i := range plan.Document.Materials {
			if plan.Document.Materials[i].ID == in.MaterialID {
				plan.Document.Materials[i].Status = in.Status
				found = true
			}
		}
		if !found {
			return Plan{}, ErrNotFound
		}
	case "remove_task":
		found := false
		tasks := []Task{}
		for _, task := range plan.Document.Tasks {
			if task.ID == in.TaskID {
				found = true
			} else {
				tasks = append(tasks, task)
			}
		}
		if !found {
			return Plan{}, ErrNotFound
		}
		plan.Document.Tasks = tasks
		plan.Document.ExcludedTaskIDs = append(plan.Document.ExcludedTaskIDs, in.TaskID)
	case "refresh":
		if err = Validate(in.Document); err != nil {
			return Plan{}, err
		}
		plan.Document, err = Merge(plan.Document, in.Document)
		if err != nil {
			return Plan{}, err
		}
	default:
		return Plan{}, ErrInvalid
	}
	raw, _ := json.Marshal(plan.Document)
	plan, err = scan(tx.QueryRow(ctx, "UPDATE saved_learning_plans SET document=$3,is_public=$4,revision=revision+1,updated_at=now() WHERE id=$1 AND revision=$2 RETURNING "+columns, id, in.Revision, raw, plan.Public))
	if err != nil {
		return Plan{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Plan{}, err
	}

	return Expose(plan, actor), nil
}

func (p *Postgres) Clone(ctx context.Context, id, actor, newID string) (Plan, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return Plan{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	source, err := scan(tx.QueryRow(ctx, "SELECT "+columns+" FROM saved_learning_plans WHERE id=$1 AND (owner_id::text=$2 OR is_public) FOR SHARE", id, actor))
	if err != nil {
		return Plan{}, err
	}
	plan, err := create(ctx, tx, actor, Input{ID: newID, Document: Reset(source.Document)})
	if err != nil {
		return Plan{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Plan{}, err
	}

	return plan, nil
}
