package studyplans

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	completionqueue "github.com/overmindv/recommendations/internal/completion/queue"
	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/planner"
)

type Postgres struct {
	pool               *pgxpool.Pool
	completionChecks   bool
	manualRebuildsOnly bool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

func (p Postgres) WithCompletionChecks() *Postgres {
	p.completionChecks = true

	return &p
}

var readOnly = pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}

func (p *Postgres) Prepare(ctx context.Context, actor, goal string, input Generation) (Sources, *View, error) {
	if !input.valid(actor, goal) {
		return Sources{}, nil, ErrInvalid
	}
	var source Sources
	var replay *View
	err := pgx.BeginTxFunc(ctx, p.pool, readOnly, func(tx pgx.Tx) error {
		var err error
		source, err = readSources(ctx, tx, actor, goal)
		if err != nil {
			return err
		}
		replay, err = readReplay(ctx, tx, goal, input, source)
		if err != nil || replay != nil {
			return err
		}

		return checkExpected(source, input)
	})
	if err != nil {
		return Sources{}, nil, err
	}

	return source, replay, nil
}

func (p *Postgres) Save(ctx context.Context, actor, goal string, input Generation, source Sources, draft Draft) (View, error) {
	view, _, err := p.save(ctx, actor, goal, input, source, draft)

	return view, err
}

func (p *Postgres) save(ctx context.Context, actor, goal string, input Generation, source Sources, draft Draft) (View, bool, error) {
	if !input.valid(actor, goal) {
		return View{}, false, ErrInvalid
	}
	var saved View
	var replayed bool
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if err := learnergoals.LockBudget(ctx, tx, actor); err != nil {
			return err
		}
		current, err := readSources(ctx, tx, actor, goal)
		if err != nil {
			return err
		}
		replay, err := readReplay(ctx, tx, goal, input, current)
		if err != nil {
			return err
		}
		if replay != nil {
			saved, replayed = *replay, true

			return nil
		}
		if expectedErr := checkExpected(current, input); expectedErr != nil {
			return expectedErr
		}
		if current.Expected != source.Expected || current.Budget.ReservedMinutes != source.Budget.ReservedMinutes ||
			current.feedbackRevision != source.feedbackRevision {
			return ErrConflict
		}
		version := input.PlanVersion + 1
		if draftErr := saveDraft(ctx, tx, goal, version, current, draft, nil); draftErr != nil {
			return draftErr
		}
		digest := input.digest()
		if _, headErr := tx.Exec(ctx, `
			INSERT INTO study_plan_heads (goal_id,version) VALUES ($1,$2)
			ON CONFLICT (goal_id) DO UPDATE SET version=EXCLUDED.version`, goal, version); headErr != nil {
			return headErr
		}
		if _, requestErr := tx.Exec(ctx, `
			INSERT INTO study_plan_requests (goal_id,request_id,payload_hash,plan_version) VALUES ($1,$2,$3,$4)`,
			goal, input.RequestID, digest[:], version); requestErr != nil {
			return requestErr
		}
		current.PlanVersion = version
		if p.completionChecks {
			if scheduleErr := completionqueue.Request(ctx, tx, []string{actor}); scheduleErr != nil {
				return scheduleErr
			}
		}
		saved, err = readView(ctx, tx, goal, version, current)

		return err
	})
	if err != nil {
		return View{}, false, fmt.Errorf("save study plan: %w", err)
	}

	return saved, replayed, nil
}

func (p *Postgres) Current(ctx context.Context, actor, goal string) (View, error) {
	return p.current(ctx, actor, goal, 0)
}

func (p *Postgres) current(ctx context.Context, actor, goal string, maxAge time.Duration) (View, error) {
	if !planner.ValidID(actor) || !planner.ValidID(goal) {
		return View{}, ErrInvalid
	}
	var view View
	err := pgx.BeginTxFunc(ctx, p.pool, readOnly, func(tx pgx.Tx) error {
		var source Sources
		var value Freshness
		var err error
		if maxAge > 0 {
			source, value, err = readSourcesWithFreshness(ctx, tx, actor, goal, maxAge)
		} else {
			source, err = readSources(ctx, tx, actor, goal)
		}
		if err != nil {
			return err
		}
		if source.PlanVersion == 0 {
			return ErrNotFound
		}
		view, err = readView(ctx, tx, goal, source.PlanVersion, source)
		if err == nil && maxAge > 0 {
			view = restrictCatalog(view, value)
		}

		return err
	})
	if err != nil {
		return View{}, fmt.Errorf("read current study plan: %w", err)
	}

	return view, nil
}

func checkExpected(source Sources, input Generation) error {
	if !source.Active || source.Expected != input.Expected {
		return ErrConflict
	}
	if source.pausedAt != nil {
		return ErrStepBlocked
	}
	if source.PlanVersion >= MaxVersionsPerGoal {
		return ErrLimit
	}

	return nil
}

const sourcesSQL = `
		SELECT g.version,a.version,COALESCE(h.version,0),g.target_topics,g.preferences,g.active,
		       a.timezone,COALESCE(to_char(g.deadline,'YYYY-MM-DD'),''),a.weekly_minutes,g.weekly_minutes,
		       (SELECT COALESCE(sum(other.weekly_minutes),0) FROM learner_goals AS other
		        WHERE other.user_id=g.user_id AND other.active AND other.id<>g.id),
		       COALESCE(f.revision,0),f.paused_at,COALESCE(f.budget_from_plan,0),COALESCE(f.budget_goal_version,0),
		       g.template_version
		FROM learner_goals AS g JOIN learner_availability AS a ON a.user_id=g.user_id
		LEFT JOIN study_plan_heads AS h ON h.goal_id=g.id
		LEFT JOIN study_plan_feedback_state AS f ON f.goal_id=g.id
		WHERE g.id=$1 AND g.user_id=$2`

func readSources(ctx context.Context, tx pgx.Tx, actor, goal string) (Sources, error) {
	return scanSources(tx.QueryRow(ctx, sourcesSQL, goal, actor), actor)
}

func scanSources(row pgx.Row, actor string) (Sources, error) {
	s := Sources{actor: actor}
	var preferences []byte
	err := row.Scan(&s.GoalVersion, &s.AvailabilityVersion, &s.PlanVersion, &s.TargetTopics, &preferences, &s.Active,
		&s.Budget.Timezone, &s.Budget.Deadline, &s.Budget.AvailableMinutes, &s.Budget.GoalMinutes, &s.Budget.ReservedMinutes,
		&s.feedbackRevision, &s.pausedAt, &s.budgetFromPlan, &s.budgetGoalVersion, &s.TemplateVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return Sources{}, ErrNotFound
	}
	if err != nil {
		return Sources{}, fmt.Errorf("read study plan sources: %w", err)
	}
	var settings learnergoals.Preferences
	if err := json.Unmarshal(preferences, &settings); err != nil {
		return Sources{}, fmt.Errorf("decode study preferences: %w", err)
	}
	s.AllowedLanguages = settings.Languages
	s.Budget.MaxWeeks = planner.MaxLearningWeeks

	return s, nil
}

func lockPlanGoal(ctx context.Context, tx pgx.Tx, actor, goal string) error {
	var found bool
	err := tx.QueryRow(ctx, "SELECT true FROM learner_goals WHERE id=$1 AND user_id=$2 FOR UPDATE", goal, actor).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}

	return err
}

func readReplay(ctx context.Context, tx pgx.Tx, goal string, input Generation, source Sources) (*View, error) {
	var version int64
	var savedHash []byte
	err := tx.QueryRow(ctx, `SELECT plan_version,payload_hash FROM study_plan_requests WHERE goal_id=$1 AND request_id=$2`,
		goal, input.RequestID).Scan(&version, &savedHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	digest := input.digest()
	if !bytes.Equal(savedHash, digest[:]) {
		return nil, ErrRequestUsed
	}
	view, err := readView(ctx, tx, goal, version, source)

	return &view, err
}
