package completion

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/pkg/api/goalcompletion"
)

type savedState struct {
	State
	basis
	goal string
}

type transition struct {
	from  int64
	event goalcompletion.Event
}

func reconcile(ctx context.Context, tx pgx.Tx, learner string, now time.Time) error {
	states, err := loadStates(ctx, tx, learner)
	if err != nil {
		return err
	}
	plans, err := loadPlans(ctx, tx, learner, now)
	if err != nil {
		return err
	}
	type eligiblePlan struct {
		current bool
		basis
	}
	eligible := make(map[string][]eligiblePlan)
	var evidence *mastery.VerificationEvidence
	for _, plan := range plans {
		required := plan.requiredScores()
		if len(required) == 0 {
			continue
		}
		if evidence == nil {
			read, err := mastery.ReadVerificationEvidence(ctx, tx, learner, now)
			if err != nil {
				return err
			}
			evidence = &read
		}
		if plan.verified(*evidence, required) {
			eligible[plan.goal] = append(eligible[plan.goal], eligiblePlan{current: plan.current, basis: plan.basis})
		}
	}
	var changes []transition
	for _, state := range states {
		if !state.Verified || slices.ContainsFunc(eligible[state.goal], func(p eligiblePlan) bool {
			return state.planVersion == p.planVersion && slices.Equal(state.topics, p.topics)
		}) {
			continue
		}
		change, err := makeTransition(state, learner, false, now)
		if err != nil {
			return err
		}
		changes = append(changes, change)
		states[state.goal] = savedState{State: State{Version: change.event.AggregateVersion}, goal: state.goal}
	}
	for _, plan := range plans {
		if !plan.current || states[plan.goal].Verified || !slices.ContainsFunc(eligible[plan.goal], func(p eligiblePlan) bool { return p.current }) {
			continue
		}
		state := savedState{State: states[plan.goal].State, goal: plan.goal, basis: plan.basis}
		change, err := makeTransition(state, learner, true, now)
		if err != nil {
			return err
		}
		changes = append(changes, change)
	}

	return storeTransitions(ctx, tx, changes)
}

func loadStates(ctx context.Context, tx pgx.Tx, learner string) (map[string]savedState, error) {
	rows, err := tx.Query(ctx, `
		SELECT goal_id::text,completion_version,verified,goal_version,plan_version,topics
		FROM goal_completion_state WHERE learner_id=$1 AND verified
		UNION ALL
		SELECT s.goal_id::text,s.completion_version,s.verified,s.goal_version,s.plan_version,s.topics
		FROM learner_goals AS g JOIN goal_completion_state AS s ON s.goal_id=g.id
		WHERE g.user_id=$1 AND g.active AND s.learner_id=$1 AND NOT s.verified
		LIMIT 65`, learner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	states := make(map[string]savedState)
	for rows.Next() {
		var state savedState
		if err := rows.Scan(&state.goal, &state.Version, &state.Verified, &state.goalVersion, &state.planVersion, &state.topics); err != nil {
			return nil, err
		}
		states[state.goal] = state
	}
	if len(states) > maxVerificationPlans {
		return nil, errors.New("too many goal completion states for a learner")
	}

	return states, rows.Err()
}

func makeTransition(state savedState, learner string, verified bool, now time.Time) (transition, error) {
	next, kind, err := state.Advance(verified)
	if err != nil || kind == "" {
		return transition{}, goalcompletion.ErrInvalid
	}
	id := pgtype.UUID{Valid: true}
	if _, err := rand.Read(id.Bytes[:]); err != nil {
		return transition{}, err
	}
	id.Bytes[6] = id.Bytes[6]&0x0f | 0x40
	id.Bytes[8] = id.Bytes[8]&0x3f | 0x80
	event := goalcompletion.Event{
		EventID: id.String(), Type: kind, SchemaVersion: goalcompletion.SchemaVersion,
		AggregateID: state.goal, AggregateVersion: next.Version, OccurredAt: now.UTC(),
		Payload: goalcompletion.Payload{
			LearnerID: learner, BusinessAwardKey: goalcompletion.BusinessAwardKey(state.goal),
			CompletionVersion: next.Version, GoalVersion: state.goalVersion, PlanVersion: state.planVersion,
			TargetTopicIDs: state.topics,
		},
	}

	return transition{from: state.Version, event: event}, event.Validate()
}

func storeTransitions(ctx context.Context, tx pgx.Tx, changes []transition) error {
	if len(changes) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, change := range changes {
		event := change.event
		wire, err := json.Marshal(event)
		if err != nil {
			return err
		}
		p := event.Payload
		batch.Queue(`
			WITH saved AS (
			    INSERT INTO goal_completion_state AS s
			        (goal_id,learner_id,completion_version,verified,goal_version,plan_version,topics,changed_at)
			    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			    ON CONFLICT (goal_id) DO UPDATE SET completion_version=EXCLUDED.completion_version,
			        verified=EXCLUDED.verified,goal_version=EXCLUDED.goal_version,plan_version=EXCLUDED.plan_version,
			        topics=EXCLUDED.topics,changed_at=EXCLUDED.changed_at
			    WHERE s.completion_version=$9 AND s.learner_id=$2 RETURNING goal_id
			)
			INSERT INTO goal_completion_outbox (event_id,goal_id,completion_version,payload)
			SELECT $10,goal_id,$3,$11 FROM saved`, event.AggregateID, p.LearnerID, event.AggregateVersion,
			event.Type == goalcompletion.Completed, p.GoalVersion, p.PlanVersion, p.TargetTopicIDs,
			event.OccurredAt, change.from, event.EventID, wire)
	}
	results := tx.SendBatch(ctx, batch)
	defer func() { _ = results.Close() }()
	for range changes {
		tag, err := results.Exec()
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return errors.New("goal completion changed before saving")
		}
	}

	return results.Close()
}
