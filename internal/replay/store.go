package replay

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/consumer"
)

type Store struct {
	pool   *pgxpool.Pool
	source Source
}

const stateBuilding = "building"

func NewStore(pool *pgxpool.Pool, source Source) *Store { return &Store{pool: pool, source: source} }

type Status struct {
	ID       string    `json:"id"`
	State    string    `json:"state"`
	Through  int64     `json:"through"`
	Manifest *Manifest `json:"manifest,omitempty"`
}

func (s *Store) Status(ctx context.Context, id string) (Status, error) {
	result := Status{ID: id}
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT state,through,manifest FROM evidence_replay_versions WHERE id=$1`, id).Scan(&result.State, &result.Through, &raw)
	if err == nil && raw != nil {
		err = json.Unmarshal(raw, &result.Manifest)
	}

	return result, err
}
func (s *Store) Start(ctx context.Context) (string, error) {
	id := uuid.NewString()
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var available bool
		var revision int64
		if err := tx.QueryRow(ctx, `SELECT building_version IS NULL,live_revision FROM evidence_replay_clock WHERE singleton FOR UPDATE`).Scan(&available, &revision); err != nil {
			return err
		}
		if !available {
			return ErrSnapshot
		}
		if _, err := tx.Exec(ctx, `INSERT INTO evidence_replay_versions(id,state,live_revision)VALUES($1,'preparing',$2)`, id, revision); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE evidence_replay_clock SET building_version=$1,paused_until=clock_timestamp()+interval '15 minutes' WHERE singleton`, id)

		return err
	})
	if err != nil {
		return "", err
	}

	return id, s.Prepare(ctx, id)
}
func (s *Store) Prepare(ctx context.Context, id string) error {
	manifest, err := s.source.Manifest(ctx, 0, 0)
	if err != nil {
		return err
	}
	if !manifest.Valid() {
		return ErrSnapshot
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return err
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockRun(ctx, tx, id); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE evidence_replay_versions SET manifest=$2,state='building' WHERE id=$1 AND state='preparing'`, id, raw)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrSnapshot
		}

		return nil
	})
}
func lockRun(ctx context.Context, tx pgx.Tx, id string) error {
	var valid bool
	err := tx.QueryRow(ctx, `SELECT c.building_version=$1 AND c.paused_until>clock_timestamp() AND c.live_revision=v.live_revision FROM evidence_replay_clock c JOIN evidence_replay_versions v ON v.id=$1 WHERE c.singleton FOR UPDATE OF c,v`, id).Scan(&valid)
	if err != nil {
		return err
	}
	if !valid {
		return ErrSnapshot
	}

	return nil
}
func (s *Store) Step(ctx context.Context, id string) error {
	status, err := s.Status(ctx, id)
	if err != nil {
		return err
	}
	if status.State == "preparing" {
		return s.Prepare(ctx, id)
	}
	if status.State != stateBuilding || status.Manifest == nil {
		return ErrSnapshot
	}
	if status.Through == status.Manifest.Watermark {
		return nil
	}
	page, err := s.source.Page(ctx, *status.Manifest, status.Through)
	if err != nil {
		return err
	}

	return s.ApplyPage(ctx, id, status.Through, *status.Manifest, page)
}
func (s *Store) ApplyPage(ctx context.Context, id string, after int64, m Manifest, page Page) error {
	if !m.Valid() || page.Watermark != m.Watermark || page.PrivacyEpoch != m.PrivacyEpoch || page.Events == nil || len(page.Events) > 100 || page.Through < after || page.Through > m.Watermark || (page.More && (len(page.Events) == 0 || page.Through <= after)) || (!page.More && page.Through != m.Watermark) {
		return ErrSnapshot
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockRun(ctx, tx, id); err != nil {
			return err
		}
		var current int64
		var raw []byte
		var state string
		if err := tx.QueryRow(ctx, `SELECT through,manifest,state FROM evidence_replay_versions WHERE id=$1`, id).Scan(&current, &raw, &state); err != nil {
			return err
		}
		var persisted Manifest
		if json.Unmarshal(raw, &persisted) != nil || !sameManifest(persisted, m) || current != after || state != stateBuilding {
			return ErrSnapshot
		}
		position := after
		for _, entry := range page.Events {
			if entry.Position <= position || entry.Position > page.Through {
				return ErrSnapshot
			}
			position = entry.Position
			digest, err := Digest(entry.Event)
			if err != nil {
				return err
			}
			expected, err := hex.DecodeString(entry.SHA256)
			if err != nil || !bytes.Equal(expected, digest) {
				return ErrSnapshot
			}
			eventID, outcome, err := consumer.DecodeReplayOutcome(entry.Event)
			if err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, `INSERT INTO evidence_replay_entries(version_id,position,event_id,digest)VALUES($1,$2,$3,$4)`, id, position, eventID, digest); err != nil {
				return err
			}
			if err = stageOutcome(ctx, tx, id, eventID, outcome); err != nil {
				return err
			}
		}
		if page.More && position != page.Through {
			return ErrSnapshot
		}
		var count int64
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM evidence_replay_entries WHERE version_id=$1`, id).Scan(&count); err != nil {
			return err
		}
		if count > m.Count {
			return ErrSnapshot
		}
		if _, err := tx.Exec(ctx, `UPDATE evidence_replay_versions SET through=$2 WHERE id=$1`, id, page.Through); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE evidence_replay_clock SET paused_until=clock_timestamp()+interval '15 minutes' WHERE singleton`)

		return err
	})
}

func stageOutcome(ctx context.Context, tx pgx.Tx, version, eventID string, o consumer.ReplayOutcome) error {
	if len(o.Topics) > 1 {
		var known bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM learning_task_outcomes WHERE submission_id=$1 AND learner_id=$2 AND task_id=$3 AND task_version_id=$4 AND topic_ids=$5)`, o.SubmissionID, o.LearnerID, o.TaskID, o.VersionID, o.Topics).Scan(&known); err != nil {
			return err
		}
		if !known {
			return ErrSnapshot
		}
	}
	kind := "non_independent"
	if *o.Independent {
		kind = "independent"
	}
	// Если прежняя попытка есть, сохраняем её зафиксированные веса, включая unknown(NULL).
	_, err := tx.Exec(ctx, `INSERT INTO evidence_replay_outcomes AS o
 (version_id,event_id,submission_id,learner_id,task_id,task_version_id,topic_ids,verdict,evidence_kind,graded_at,correction_version,topic_weights)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,(SELECT topic_weights FROM learning_task_outcomes WHERE submission_id=$3))
 ON CONFLICT(version_id,submission_id) DO UPDATE SET event_id=EXCLUDED.event_id,verdict=EXCLUDED.verdict,evidence_kind=EXCLUDED.evidence_kind,graded_at=EXCLUDED.graded_at,correction_version=EXCLUDED.correction_version
 WHERE o.correction_version<EXCLUDED.correction_version AND (o.learner_id,o.task_id,o.task_version_id,o.topic_ids)=(EXCLUDED.learner_id,EXCLUDED.task_id,EXCLUDED.task_version_id,EXCLUDED.topic_ids)`, version, eventID, o.SubmissionID, o.LearnerID, o.TaskID, o.VersionID, o.Topics, o.Verdict, kind, o.GradedAt, o.Correction)
	if err != nil {
		return err
	}
	var consistent bool
	if err = tx.QueryRow(ctx, `SELECT (learner_id,task_id,task_version_id,topic_ids)=($3::uuid,$4::uuid,$5::uuid,$6::uuid[])
 AND (correction_version>$7 OR (correction_version=$7 AND (verdict,evidence_kind,graded_at)=($8,$9,$10)))
 FROM evidence_replay_outcomes WHERE version_id=$1 AND submission_id=$2`, version, o.SubmissionID, o.LearnerID, o.TaskID, o.VersionID, o.Topics, o.Correction, o.Verdict, kind, o.GradedAt).Scan(&consistent); err != nil {
		return err
	}
	if !consistent {
		return ErrSnapshot
	}
	// Сохраняем также более старую коррекцию, пришедшую после новой; она не меняет attempt.
	_, err = tx.Exec(ctx, `INSERT INTO evidence_replay_history(version_id,event_id,submission_id,learner_id,correction_version,proof)
 VALUES($11,$1,$2,$3,$4,jsonb_build_object('event_id',$1::uuid,'submission_id',$2::uuid,'learner_id',$3::uuid,'correction_version',$4::bigint,
 'task_id',$5::uuid,'task_version_id',$6::uuid,'topic_ids',$7::uuid[],'verdict',$8::text,'evidence_kind',$9::text,'graded_at',$10::timestamptz,
 'topic_weights',(SELECT topic_weights FROM learning_task_outcomes WHERE submission_id=$2)))`, eventID, o.SubmissionID, o.LearnerID, o.Correction, o.TaskID, o.VersionID, o.Topics, o.Verdict, kind, o.GradedAt, version)

	return err
}

func (s *Store) Cancel(ctx context.Context, id string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var current *string
		if err := tx.QueryRow(ctx, `SELECT building_version::text FROM evidence_replay_clock WHERE singleton FOR UPDATE`).Scan(&current); err != nil {
			return err
		}
		if current == nil || *current != id {
			return ErrSnapshot
		}
		for _, query := range []string{`DELETE FROM evidence_replay_entries WHERE version_id=$1`, `DELETE FROM evidence_replay_outcomes WHERE version_id=$1`, `DELETE FROM evidence_replay_history WHERE version_id=$1`, `UPDATE evidence_replay_versions SET state='cancelled' WHERE id=$1`, `UPDATE evidence_replay_clock SET building_version=NULL,paused_until=NULL WHERE singleton AND building_version=$1`} {
			if _, err := tx.Exec(ctx, query, id); err != nil {
				return err
			}
		}

		return nil
	})
}
