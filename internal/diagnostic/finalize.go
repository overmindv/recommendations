package diagnostic

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/learnergoals"
)

const assessmentSQL = `
	SELECT p.task_id::text,p.version_id::text,p.topic_id::text,
	       o.submission_id::text,o.event_id::text,o.correction_version,o.verdict,o.graded_at
	FROM unnest($2::uuid[], $3::uuid[], $4::uuid[]) WITH ORDINALITY AS p(task_id, version_id, topic_id, position)
	JOIN LATERAL (
	    SELECT submission_id, event_id, correction_version, verdict, graded_at
	    FROM learning_task_outcomes
	    WHERE learner_id=$1 AND task_id=p.task_id AND task_version_id=p.version_id
	      AND evidence_kind='independent' AND p.topic_id=ANY(topic_ids)
	      AND graded_at >= $5 AND graded_at < $6 AND graded_at <= statement_timestamp()
	    ORDER BY graded_at, submission_id LIMIT 1
	) AS o ON true
	ORDER BY p.position`

func assessments(ctx context.Context, tx pgx.Tx, actor string, session Session) ([]AssessedTask, error) {
	if len(session.Tasks) == 0 {
		return nil, nil
	}
	tasks, versions, topics := make([]string, 0, len(session.Tasks)), make([]string, 0, len(session.Tasks)), make([]string, 0, len(session.Tasks))
	for _, task := range session.Tasks {
		tasks, versions, topics = append(tasks, task.TaskID), append(versions, task.VersionID), append(topics, task.TopicID)
	}
	rows, err := tx.Query(ctx, assessmentSQL, actor, tasks, versions, topics, session.StartedAt, session.ExpiresAt)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (AssessedTask, error) {
		var result AssessedTask
		err := row.Scan(&result.TaskID, &result.VersionID, &result.TopicID, &result.SubmissionID, &result.EventID, &result.CorrectionVersion, &result.Verdict, &result.GradedAt)
		return result, err
	})
}

func (p *Postgres) finalize(ctx context.Context, goal learnergoals.Goal, sessionID string, allowPartial bool) (Session, error) {
	var session Session
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if err := lockGoalVersion(ctx, tx, goal); err != nil {
			return err
		}
		var err error
		session, err = readSession(tx.QueryRow(ctx, sessionSQL+" FOR UPDATE OF s", goal.UserID, goal.ID, goal.Version))
		if err != nil {
			return err
		}
		if session.ID != sessionID {
			return ErrSessionChanged
		}
		if session.Status == Finalized || session.Status == Skipped {
			return nil
		}
		outcomes, err := assessments(ctx, tx, goal.UserID, session)
		if err != nil {
			return fmt.Errorf("read diagnostic outcomes: %w", err)
		}
		report := buildReport(goal.TargetTopics, session.Tasks, outcomes)
		session.Report = &report
		if report.Assessed < report.Assigned && !allowPartial {
			return nil
		}
		payload, err := json.Marshal(report)
		if err != nil {
			return fmt.Errorf("encode diagnostic report: %w", err)
		}
		if err := tx.QueryRow(ctx, `
			UPDATE diagnostic_sessions SET status='finalized', report=$2, finalized_at=statement_timestamp()
			WHERE id=$1 RETURNING finalized_at`, session.ID, payload).Scan(&session.FinalizedAt); err != nil {
			return err
		}
		session.Status = Finalized
		return nil
	})
	if err != nil {
		return Session{}, fmt.Errorf("finalize diagnostic session: %w", err)
	}
	return session, nil
}
