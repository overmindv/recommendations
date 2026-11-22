package consumer

import (
	"context"
	"errors"
	"slices"

	"github.com/jackc/pgx/v5"
)

// ReplayOutcome использует те же проверки границ, что и Kafka-приёмник.
type ReplayOutcome = taskResult

func DecodeReplayOutcome(raw []byte) (string, ReplayOutcome, error) {
	var wire taskEnvelope
	if len(raw) == 0 || len(raw) > maxEventBytes || !decodeJSON(raw, &wire) || !wire.valid(wire.AggregateID) {
		return "", ReplayOutcome{}, errors.New("invalid replay outcome")
	}
	slices.Sort(wire.Payload.Topics)

	return wire.ID, wire.Payload, nil
}

// Сохраняем согласованную старую коррекцию даже при доставке после новой версии.
func archiveOutcome(ctx context.Context, tx pgx.Tx, msg message) error {
	o := msg.outcome
	_, err := tx.Exec(ctx, `INSERT INTO learning_outcome_history(event_id,submission_id,learner_id,correction_version,proof)
 SELECT $1,$2,$3,$4,jsonb_build_object('event_id',$1::uuid,'submission_id',$2::uuid,'learner_id',$3::uuid,'correction_version',$4::bigint,
 'task_id',$5::uuid,'task_version_id',$6::uuid,'topic_ids',$7::uuid[],'verdict',$8::text,'evidence_kind',$9::text,'graded_at',$10::timestamptz,
 'topic_weights',(SELECT topic_weights FROM learning_task_outcomes WHERE submission_id=$2))
 WHERE NOT EXISTS(SELECT 1 FROM learning_evidence_rejections WHERE topic=$11 AND partition=$12 AND message_offset=$13)
	 AND EXISTS(SELECT 1 FROM learning_evidence_inbox WHERE source='tasks' AND event_id=$1 AND NOT rejected)
	 AND EXISTS(SELECT 1 FROM learning_task_outcomes WHERE submission_id=$2 AND learner_id=$3 AND task_id=$5 AND task_version_id=$6 AND topic_ids=$7)
 ON CONFLICT DO NOTHING`, msg.id, o.SubmissionID, o.LearnerID, o.Correction, o.TaskID, o.VersionID, o.Topics, o.Verdict, o.kind(), o.GradedAt, msg.topic, msg.partition, msg.offset)

	return err
}
