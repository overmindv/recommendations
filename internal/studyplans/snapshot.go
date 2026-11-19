package studyplans

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

type calendarDocument struct {
	Timezone       string                    `json:"timezone"`
	WeeklyMinutes  int                       `json:"weekly_minutes"`
	HorizonLimited bool                      `json:"horizon_limited"`
	Weeks          []WeekHeader              `json:"weeks"`
	UnmetOutcomes  []planner.UnmetOutcome    `json:"unmet_outcomes"`
	Suggestions    []planner.ScopeSuggestion `json:"suggestions"`
}

func saveDraft(ctx context.Context, tx pgx.Tx, goal string, version int64, source Sources, draft Draft) error {
	doc, rows, err := prepareDraft(goal, version, source, draft)
	if err != nil {
		return err
	}
	policy, err := json.Marshal(draft.Plan.RankingPolicy)
	if err != nil {
		return err
	}
	cal, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	evidencePolicy, err := json.Marshal(draft.EvidencePolicy)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO study_plan_versions (goal_id,version,goal_version,availability_version,reserved_minutes,target_topics,
		    planner_version,ranking_algorithm,policy_version,ranking_policy,evidence_watermark,evidence_policy,calendar,scope_topics)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,COALESCE($14::uuid[],'{}'))`,
		goal, version, source.GoalVersion, source.AvailabilityVersion, source.Budget.ReservedMinutes, source.TargetTopics,
		draft.Plan.Version, draft.Plan.RankingAlgorithm, draft.Plan.RankingPolicy.Version, policy, draft.EvidenceWatermark[:], evidencePolicy, cal,
		draft.ScopeTopics)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	count, err := tx.CopyFrom(ctx, pgx.Identifier{"study_plan_items"}, []string{
		"goal_id", "plan_version", "ordinal", "week_number", "topic_id", "resource_id", "resource_version_id",
		"kind", "stage", "estimated_minutes", "score", "score_components", "requires_checks", "superseded",
	}, pgx.CopyFromRows(rows))
	if err != nil {
		return err
	}
	if count != int64(len(rows)) {
		return errors.New("incomplete study plan items insert")
	}
	return nil
}

func prepareDraft(goal string, version int64, source Sources, draft Draft) (calendarDocument, [][]any, error) {
	plan := draft.Plan
	budget := source.Budget
	maxMinutes := min(budget.GoalMinutes, max(0, budget.AvailableMinutes-budget.ReservedMinutes))
	if plan.Version != planner.LearningPlanVersion || plan.RankingAlgorithm != ranking.AlgorithmVersion ||
		!plan.RankingPolicy.Valid() || !draft.EvidencePolicy.Valid() || draft.EvidenceWatermark == [32]byte{} || plan.Timezone != budget.Timezone ||
		plan.WeeklyMinutes != maxMinutes || len(plan.Weeks) > planner.MaxLearningWeeks ||
		len(plan.UnmetOutcomes) > ranking.MaxTopics || len(plan.Suggestions) > 2 {
		return calendarDocument{}, nil, ErrInvalid
	}
	doc := calendarDocument{
		Timezone: plan.Timezone, WeeklyMinutes: plan.WeeklyMinutes, HorizonLimited: plan.HorizonLimited,
		Weeks: make([]WeekHeader, 0, len(plan.Weeks)), UnmetOutcomes: plan.UnmetOutcomes, Suggestions: plan.Suggestions,
	}
	rows := make([][]any, 0, min(planner.MaxLearningItems, len(plan.Weeks)*3))
	var previous string
	for i, week := range plan.Weeks {
		from, fromErr := time.Parse(time.DateOnly, week.From)
		through, throughErr := time.Parse(time.DateOnly, week.Through)
		if fromErr != nil || throughErr != nil || through.Before(from) || through.After(from.AddDate(0, 0, 6)) ||
			week.From <= previous || (budget.Deadline != "" && week.Through > budget.Deadline) ||
			week.CapacityMinutes < 0 || week.CapacityMinutes > maxMinutes ||
			week.PlannedMinutes < 0 || week.PlannedMinutes > week.CapacityMinutes ||
			len(week.Items) > planner.MaxLearningItems-len(rows) {
			return calendarDocument{}, nil, ErrInvalid
		}
		previous = week.Through
		doc.Weeks = append(doc.Weeks, WeekHeader{StudyWeek: week.StudyWeek, PlannedMinutes: week.PlannedMinutes})
		minutes := 0
		for _, item := range week.Items {
			if !validItem(item) {
				return calendarDocument{}, nil, ErrInvalid
			}
			minutes += item.Minutes
			components, err := json.Marshal(item.Components)
			if err != nil {
				return calendarDocument{}, nil, err
			}
			guards := item.RequiresCheck
			if guards == nil {
				guards = []string{}
			}
			rows = append(rows, []any{
				goal, version, int16(len(rows) + 1), int16(i + 1), item.TopicID, item.ID, item.RevisionID,
				string(item.Kind), string(item.Stage), int16(item.Minutes), int32(item.Score), components, guards, item.Superseded,
			})
		}
		if minutes != week.PlannedMinutes {
			return calendarDocument{}, nil, ErrInvalid
		}
	}
	return doc, rows, nil
}

func validItem(item planner.StudyItem) bool {
	if !planner.ValidID(item.TopicID) || !planner.ValidID(item.ID) || !planner.ValidID(item.RevisionID) ||
		item.Minutes < 1 || item.Minutes > ranking.MaxMinutes || item.Score < 0 || item.Score > 50_000_000 ||
		len(item.RequiresCheck) > ranking.MaxTopics || (item.Superseded && item.Stage != planner.StudyCheck) {
		return false
	}
	for _, topic := range item.RequiresCheck {
		if !planner.ValidID(topic) || topic == item.TopicID {
			return false
		}
	}
	switch item.Stage {
	case planner.StudyTheory:
		return item.Kind == ranking.Theory
	case planner.StudyPractice, planner.StudyCheck:
		return item.Kind == ranking.Practice
	default:
		return false
	}
}

func readView(ctx context.Context, tx pgx.Tx, goal string, version int64, source Sources) (View, error) {
	view := View{GoalID: goal, Version: version, Current: version == source.PlanVersion}
	var rawPolicy, rawEvidencePolicy, rawCal, watermark []byte
	var reserved int
	var now time.Time
	err := tx.QueryRow(ctx, `
		SELECT goal_version,availability_version,reserved_minutes,target_topics,planner_version,ranking_algorithm,
		       ranking_policy,evidence_watermark,evidence_policy,calendar,created_at,statement_timestamp(),scope_topics
		FROM study_plan_versions WHERE goal_id=$1 AND version=$2`, goal, version).
		Scan(&view.GoalVersion, &view.AvailabilityVersion, &reserved, &view.TargetTopics, &view.PlannerVersion, &view.RankingAlgorithm,
			&rawPolicy, &watermark, &rawEvidencePolicy, &rawCal, &view.CreatedAt, &now, &view.scopeTopics)
	if errors.Is(err, pgx.ErrNoRows) {
		return View{}, ErrNotFound
	}
	if err != nil {
		return View{}, err
	}
	var cal calendarDocument
	if err := json.Unmarshal(rawPolicy, &view.RankingPolicy); err != nil {
		return View{}, fmt.Errorf("decode ranking policy: %w", err)
	}
	if err := json.Unmarshal(rawEvidencePolicy, &view.EvidencePolicy); err != nil {
		return View{}, fmt.Errorf("decode evidence policy: %w", err)
	}
	if err := json.Unmarshal(rawCal, &cal); err != nil {
		return View{}, fmt.Errorf("decode study calendar: %w", err)
	}
	view.Stale = !source.Active || view.GoalVersion != source.GoalVersion || view.AvailabilityVersion != source.AvailabilityVersion ||
		reserved != source.Budget.ReservedMinutes
	view.EvidenceWatermark = hex.EncodeToString(watermark)
	view.Timezone, view.WeeklyMinutes, view.HorizonLimited = cal.Timezone, cal.WeeklyMinutes, cal.HorizonLimited
	view.UnmetOutcomes, view.Suggestions = cal.UnmetOutcomes, cal.Suggestions
	view.Weeks = make([]Week, len(cal.Weeks))
	for i, week := range cal.Weeks {
		view.Weeks[i] = Week{WeekHeader: week, Items: []Item{}}
	}
	if err := readItems(ctx, tx, source.actor, &view); err != nil {
		return View{}, err
	}
	if err := describeProgress(ctx, tx, source.actor, &view, now); err != nil {
		return View{}, err
	}
	return view, nil
}

func readItems(ctx context.Context, tx pgx.Tx, actor string, view *View) error {
	rows, err := tx.Query(ctx, `
		SELECT i.ordinal,i.week_number,i.topic_id::text,i.resource_id::text,i.resource_version_id::text,
		       i.kind,i.stage,i.estimated_minutes,i.score,i.score_components,i.requires_checks,i.superseded,
		       COALESCE(a.started_at,a.completed_at),a.completed_at,COALESCE(i.evidence_since,$4::timestamptz),p.ordinal IS NOT NULL,
		       CASE WHEN valid.current THEN proof.verdict END,
		       CASE WHEN valid.current THEN proof.graded_at END
		FROM study_plan_items AS i
		LEFT JOIN study_plan_activity AS a USING (goal_id,plan_version,ordinal)
		LEFT JOIN study_plan_assessments AS p USING (goal_id,plan_version,ordinal)
		LEFT JOIN LATERAL (
		    SELECT submission_id,event_id,correction_version,graded_at,verdict,task_version_id,topic_ids
		    FROM learning_task_outcomes
		    WHERE p.submission_id IS NOT NULL AND learner_id=$3 AND task_id=i.resource_id
		      AND evidence_kind='independent' AND graded_at <= statement_timestamp()
		    ORDER BY graded_at,submission_id LIMIT 1
		) AS proof ON proof.submission_id=p.submission_id AND proof.task_version_id=i.resource_version_id
		    AND i.topic_id=ANY(proof.topic_ids) AND proof.graded_at >= COALESCE(i.evidence_since,$4::timestamptz)
		CROSS JOIN LATERAL (
		    SELECT i.superseded OR (p.event_id=proof.event_id AND p.correction_version=proof.correction_version) AS current
		) AS valid
		WHERE i.goal_id=$1 AND i.plan_version=$2 ORDER BY i.ordinal`, view.GoalID, view.Version, actor, view.CreatedAt)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var item Item
		var week int
		var rawScore []byte
		var verdict *string
		var gradedAt *time.Time
		if err := rows.Scan(&item.Ordinal, &week, &item.TopicID, &item.ID, &item.RevisionID, &item.Kind, &item.Stage,
			&item.Minutes, &item.Score, &rawScore, &item.RequiresCheck, &item.Superseded, &item.ActivityStarted, &item.ActivityDone,
			&item.evidenceSince, &item.hasReceipt, &verdict, &gradedAt); err != nil {
			return err
		}
		if week < 1 || week > len(view.Weeks) {
			return errors.New("study item has no week")
		}
		if err := json.Unmarshal(rawScore, &item.Components); err != nil {
			return err
		}
		if verdict != nil && gradedAt != nil {
			item.Assessment = &Assessment{Verdict: *verdict, GradedAt: *gradedAt}
		}
		view.Weeks[week-1].Items = append(view.Weeks[week-1].Items, item)
	}
	return rows.Err()
}
