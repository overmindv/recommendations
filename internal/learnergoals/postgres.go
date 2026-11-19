package learnergoals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/planner"
)

type Postgres struct {
	pool             *pgxpool.Pool
	now              func() time.Time
	validate         GoalValidator
	completionChecks bool
}

type GoalValidator func(context.Context, Input) error

var _ Store = (*Postgres)(nil)

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool, now: time.Now}
}

func NewValidatedPostgres(pool *pgxpool.Pool, validate GoalValidator) *Postgres {
	store := NewPostgres(pool)
	store.validate = validate
	return store
}

func (p Postgres) WithCompletionChecks() *Postgres {
	p.completionChecks = true
	return &p
}

func (p *Postgres) validateGoal(in Input) func(context.Context) error {
	if p.validate == nil {
		return nil
	}
	return func(ctx context.Context) error { return p.validate(ctx, in) }
}

type queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type scanner interface{ Scan(...any) error }

const availabilityColumns = "user_id::text, weekly_minutes, timezone, version, updated_at"

const goalColumns = `g.id::text, g.user_id::text, g.title, g.template_version, g.target_topics::text[],
	g.deadline, g.weekly_minutes, g.active, g.preferences, a.timezone, g.version, g.created_at, g.updated_at`

const goalRows = ` FROM learner_goals g JOIN learner_availability a ON a.user_id = g.user_id`

func scanAvailability(row scanner) (Availability, error) {
	var result Availability
	err := row.Scan(&result.UserID, &result.WeeklyMinutes, &result.Timezone, &result.Version, &result.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	return result, err
}

func scanGoal(row scanner) (Goal, error) {
	var goal Goal
	var deadline *time.Time
	var raw []byte
	err := row.Scan(&goal.ID, &goal.UserID, &goal.Title, &goal.TemplateVersion, &goal.TargetTopics, &deadline,
		&goal.WeeklyMinutes, &goal.Active, &raw, &goal.Timezone, &goal.Version, &goal.CreatedAt, &goal.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Goal{}, ErrNotFound
	}
	if err != nil {
		return Goal{}, err
	}
	if deadline != nil {
		goal.Deadline = deadline.Format(dateLayout)
	}
	if err := json.Unmarshal(raw, &goal.Preferences); err != nil {
		return Goal{}, fmt.Errorf("decode learner goal preferences: %w", err)
	}
	return goal, nil
}

func (p *Postgres) GetAvailability(ctx context.Context, actor string) (Availability, error) {
	if !planner.ValidID(actor) {
		return Availability{}, ErrInvalid
	}
	return readAvailability(ctx, p.pool, actor)
}

func readAvailability(ctx context.Context, q queryer, actor string) (Availability, error) {
	return scanAvailability(q.QueryRow(ctx, "SELECT "+availabilityColumns+" FROM learner_availability WHERE user_id = $1", actor))
}

func (p *Postgres) List(ctx context.Context, actor string) ([]Goal, error) {
	if !planner.ValidID(actor) {
		return nil, ErrInvalid
	}
	rows, err := p.pool.Query(ctx, "SELECT "+goalColumns+goalRows+" WHERE g.user_id = $1 ORDER BY g.updated_at DESC, g.id LIMIT $2", actor, MaxGoals)
	if err != nil {
		return nil, fmt.Errorf("list learner goals: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Goal, error) { return scanGoal(row) })
}

func (p *Postgres) Get(ctx context.Context, actor, id string) (Goal, error) {
	if !planner.ValidID(actor) || !planner.ValidID(id) {
		return Goal{}, ErrInvalid
	}
	return readGoal(ctx, p.pool, actor, id)
}

func readGoal(ctx context.Context, q queryer, actor, id string) (Goal, error) {
	return scanGoal(q.QueryRow(ctx, "SELECT "+goalColumns+goalRows+" WHERE g.user_id = $1 AND g.id = $2", actor, id))
}

func (p *Postgres) SaveAvailability(ctx context.Context, actor string, write Write, in AvailabilityInput) (Availability, error) {
	if in.validate() != nil || write.ExpectedVersion < 0 {
		return Availability{}, ErrInvalid
	}
	cmd := command{Operation: "availability", Version: write.ExpectedVersion, Body: in}
	return executeWrite(ctx, p, actor, write.RequestID, cmd, nil, func(tx pgx.Tx, _ time.Time) (Availability, error) {
		existing, err := readAvailability(ctx, tx, actor)
		switch {
		case errors.Is(err, ErrNotFound) && write.ExpectedVersion == 0:
			return scanAvailability(tx.QueryRow(ctx, `
				INSERT INTO learner_availability (user_id, weekly_minutes, timezone)
				VALUES ($1, $2, $3) RETURNING `+availabilityColumns, actor, in.WeeklyMinutes, in.Timezone))
		case err != nil:
			return Availability{}, err
		case existing.Version != write.ExpectedVersion:
			return Availability{}, ErrConflict
		}
		_, allocated, err := allocations(ctx, tx, actor)
		if err != nil {
			return Availability{}, err
		}
		if allocated > in.WeeklyMinutes {
			return Availability{}, ErrBudget
		}
		return scanAvailability(tx.QueryRow(ctx, `
			UPDATE learner_availability SET weekly_minutes = $2, timezone = $3, version = version + 1,
			                              updated_at = statement_timestamp()
			WHERE user_id = $1 AND version = $4 RETURNING `+availabilityColumns,
			actor, in.WeeklyMinutes, in.Timezone, write.ExpectedVersion))
	})
}

func (p *Postgres) Create(ctx context.Context, actor, requestID string, in Input) (Goal, error) {
	normalized, err := normalize(in)
	if err != nil {
		return Goal{}, err
	}
	cmd := command{Operation: "create", Body: normalized}
	return executeWrite(ctx, p, actor, requestID, cmd, p.validateGoal(normalized), func(tx pgx.Tx, now time.Time) (Goal, error) {
		availability, err := readAvailability(ctx, tx, actor)
		if errors.Is(err, ErrNotFound) {
			return Goal{}, ErrNoAvailability
		}
		if err != nil {
			return Goal{}, err
		}
		if err = checkDeadline(normalized, availability, now); err != nil {
			return Goal{}, err
		}
		count, allocated, err := allocations(ctx, tx, actor)
		if err != nil {
			return Goal{}, err
		}
		if count >= MaxGoals {
			return Goal{}, ErrGoalLimit
		}
		if allocated+allocation(normalized) > availability.WeeklyMinutes {
			return Goal{}, ErrBudget
		}
		raw, err := json.Marshal(normalized.Preferences)
		if err != nil {
			return Goal{}, err
		}
		var id string
		err = tx.QueryRow(ctx, `
			INSERT INTO learner_goals (user_id, title, template_version, target_topics, deadline, weekly_minutes, active, preferences)
			VALUES ($1, $2, $3, $4::text[]::uuid[], NULLIF($5::text, '')::date, $6, $7, $8) RETURNING id::text`,
			actor, normalized.Title, normalized.TemplateVersion, normalized.TargetTopics, normalized.Deadline,
			normalized.WeeklyMinutes, normalized.Active, raw).Scan(&id)
		if err != nil {
			return Goal{}, err
		}
		return readGoal(ctx, tx, actor, id)
	})
}

func (p *Postgres) Update(ctx context.Context, actor, id string, write Write, in Input) (Goal, error) {
	normalized, err := normalize(in)
	if err != nil || !planner.ValidID(id) || write.ExpectedVersion < 1 {
		return Goal{}, ErrInvalid
	}
	cmd := command{Operation: "update", GoalID: id, Version: write.ExpectedVersion, Body: normalized}
	return executeWrite(ctx, p, actor, write.RequestID, cmd, p.validateGoal(normalized), func(tx pgx.Tx, now time.Time) (Goal, error) {
		old, err := readGoal(ctx, tx, actor, id)
		if err != nil {
			return Goal{}, err
		}
		if old.Version != write.ExpectedVersion {
			return Goal{}, ErrConflict
		}
		availability, err := readAvailability(ctx, tx, actor)
		if err != nil {
			return Goal{}, err
		}
		if err = checkDeadline(normalized, availability, now); err != nil {
			return Goal{}, err
		}
		_, allocated, err := allocations(ctx, tx, actor)
		if err != nil {
			return Goal{}, err
		}
		if allocated-allocation(old.Input)+allocation(normalized) > availability.WeeklyMinutes {
			return Goal{}, ErrBudget
		}
		raw, err := json.Marshal(normalized.Preferences)
		if err != nil {
			return Goal{}, err
		}
		tag, err := tx.Exec(ctx, `
			UPDATE learner_goals
			SET title = $4, template_version = $5, target_topics = $6::text[]::uuid[], deadline = NULLIF($7::text, '')::date,
			    weekly_minutes = $8, active = $9, preferences = $10, version = version + 1, updated_at = statement_timestamp()
			WHERE user_id = $1 AND id = $2 AND version = $3`, actor, id, write.ExpectedVersion,
			normalized.Title, normalized.TemplateVersion, normalized.TargetTopics, normalized.Deadline,
			normalized.WeeklyMinutes, normalized.Active, raw)
		if err != nil {
			return Goal{}, err
		}
		if tag.RowsAffected() != 1 {
			return Goal{}, ErrConflict
		}
		return readGoal(ctx, tx, actor, id)
	})
}

func (p *Postgres) Delete(ctx context.Context, actor, id string, write Write) error {
	if !planner.ValidID(id) || write.ExpectedVersion < 1 {
		return ErrInvalid
	}
	cmd := command{Operation: "delete", GoalID: id, Version: write.ExpectedVersion}
	_, err := executeWrite(ctx, p, actor, write.RequestID, cmd, nil, func(tx pgx.Tx, _ time.Time) (struct{}, error) {
		tag, err := tx.Exec(ctx, "DELETE FROM learner_goals WHERE user_id = $1 AND id = $2 AND version = $3",
			actor, id, write.ExpectedVersion)
		if err == nil && tag.RowsAffected() == 0 {
			_, err = readGoal(ctx, tx, actor, id)
			if err == nil {
				err = ErrConflict
			}
		}
		return struct{}{}, err
	})
	return err
}

func allocations(ctx context.Context, tx pgx.Tx, actor string) (count, allocated int, err error) {
	err = tx.QueryRow(ctx, `
		SELECT count(*), COALESCE(sum(weekly_minutes) FILTER (WHERE active), 0)
		FROM learner_goals WHERE user_id = $1`, actor).Scan(&count, &allocated)
	return count, allocated, err
}

func allocation(in Input) int {
	if in.Active {
		return in.WeeklyMinutes
	}
	return 0
}
