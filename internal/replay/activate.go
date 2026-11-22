package replay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/jackc/pgx/v5"
	completionqueue "github.com/overmindv/recommendations/internal/completion/queue"
	"github.com/overmindv/recommendations/internal/mastery"
)

func sameManifest(a, b Manifest) bool {
	return a.Watermark == b.Watermark && a.PrivacyEpoch == b.PrivacyEpoch && a.Count == b.Count && a.SHA256 == b.SHA256 && a.LegacyMissing == b.LegacyMissing && a.CompleteSince.Equal(b.CompleteSince)
}

type ActivationOptions struct {
	Mastery          *mastery.Policy
	CompletionChecks bool
}

func (s *Store) Activate(ctx context.Context, id string, options ActivationOptions) error {
	status, err := s.Status(ctx, id)
	if err != nil {
		return err
	}
	if status.State == "active" {
		return nil
	}
	if status.State != stateBuilding || status.Manifest == nil || status.Through != status.Manifest.Watermark {
		return ErrSnapshot
	}
	expected := *status.Manifest
	current, err := s.source.Manifest(ctx, expected.Watermark, expected.PrivacyEpoch)
	if err != nil {
		return err
	}
	if !sameManifest(expected, current) {
		return ErrSnapshot
	}
	var baselines *mastery.Postgres
	if options.Mastery != nil {
		baselines, err = mastery.NewPostgres(ctx, s.pool, *options.Mastery)
		if err != nil {
			return err
		}
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockRun(ctx, tx, id); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT digest FROM evidence_replay_entries WHERE version_id=$1 ORDER BY position`, id)
		if err != nil {
			return err
		}
		hash := sha256.New()
		var count int64
		for rows.Next() {
			var digest []byte
			if err = rows.Scan(&digest); err != nil {
				rows.Close()

				return err
			}
			_, _ = hash.Write(digest)
			count++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if count != expected.Count || hex.EncodeToString(hash.Sum(nil)) != expected.SHA256 {
			return ErrSnapshot
		}
		// Ограничиваем атомарную замену и пересчёт заранее; переполнение не даёт partial switch.
		var oldCount int64
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM learning_task_outcomes`).Scan(&oldCount); err != nil {
			return err
		}
		if oldCount > MaxEvents {
			return ErrSnapshot
		}
		rows, err = tx.Query(ctx, `SELECT g.id::text FROM learner_goals g WHERE g.active AND (
 EXISTS(SELECT 1 FROM learning_task_outcomes o WHERE o.learner_id=g.user_id)
 OR EXISTS(SELECT 1 FROM evidence_replay_outcomes o WHERE o.version_id=$1 AND o.learner_id=g.user_id)) ORDER BY g.user_id,g.id LIMIT 1025 FOR UPDATE`, id)
		if err != nil {
			return err
		}
		goals, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		if len(goals) > 1024 {
			return ErrSnapshot
		}
		var hasBaselines bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM mastery_baselines WHERE goal_id=ANY($1::uuid[]))`, goals).Scan(&hasBaselines); err != nil {
			return err
		}
		if hasBaselines && baselines == nil {
			return ErrSnapshot
		}
		if _, err = tx.Exec(ctx, `DELETE FROM learning_task_outcomes`); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO learning_task_outcomes(submission_id,learner_id,task_id,task_version_id,topic_ids,verdict,evidence_kind,graded_at,event_id,correction_version,topic_weights)
 SELECT submission_id,learner_id,task_id,task_version_id,topic_ids,verdict,evidence_kind,graded_at,event_id,correction_version,topic_weights FROM evidence_replay_outcomes WHERE version_id=$1`, id); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO learning_outcome_history(event_id,submission_id,learner_id,correction_version,proof) SELECT event_id,submission_id,learner_id,correction_version,proof FROM evidence_replay_history WHERE version_id=$1 ON CONFLICT DO NOTHING`, id); err != nil {
			return err
		}
		if baselines != nil && len(goals) > 0 {
			if _, err = baselines.RefreshObservedGoals(ctx, tx, goals); err != nil {
				return err
			}
		}
		if _, err = tx.Exec(ctx, `INSERT INTO study_plan_rebuilds AS q(goal_id,first_requested_at,requested_at,available_at)
 SELECT h.goal_id,statement_timestamp(),statement_timestamp(),statement_timestamp() FROM study_plan_heads h
 LEFT JOIN study_plan_feedback_state f ON f.goal_id=h.goal_id
 WHERE h.goal_id=ANY($1::uuid[]) AND f.paused_at IS NULL ORDER BY h.goal_id
 ON CONFLICT(goal_id) DO UPDATE SET revision=q.revision+1,requested_at=EXCLUDED.requested_at,available_at=EXCLUDED.available_at`, goals); err != nil {
			return err
		}
		if err = completionqueue.RequestReplay(ctx, tx, goals, options.CompletionChecks); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE evidence_replay_versions SET state='superseded' WHERE state='active'`); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE evidence_replay_versions SET state='active' WHERE id=$1`, id); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE evidence_replay_clock SET active_version=$1,building_version=NULL,paused_until=NULL WHERE singleton`, id)

		return err
	})
}
