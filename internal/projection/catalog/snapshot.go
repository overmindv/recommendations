package catalog

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

func (s *Store) ApplySnapshot(ctx context.Context, expected State, page SnapshotPage) (bool, error) {
	return s.apply(ctx, expected, "snapshot", func(tx pgx.Tx, st *State) (bool, error) {
		if !st.Rebuild {
			return false, ErrConcurrent
		}
		if page.Watermark == nil || *page.Watermark < 0 {
			return false, errors.New("missing or invalid snapshot watermark")
		}
		w := *page.Watermark
		if st.PendingGeneration == 0 {
			if w < st.Through || w <= st.RejectedWatermark {
				return false, nil
			}
			st.PendingGeneration, st.PendingWatermark = st.Revision+1, w
		}
		if w != st.PendingWatermark || page.Records == nil || len(page.Records) > PageLimit ||
			len(page.NextCursor) > 128 ||
			(page.NextCursor != "" && (page.NextCursor == st.PendingCursor || len(page.Records) == 0)) {
			return false, &rejected{revision: st.PendingWatermark, reason: "invalid_snapshot_page"}
		}
		st.PendingRows += int64(len(page.Records))
		if st.PendingRows > maxSnapshotRows {
			return false, &rejected{revision: w, reason: "too_many_snapshot_rows"}
		}
		err := insertSnapshotPage(ctx, tx, st.PendingGeneration, w, page.Records)
		if err != nil {
			return false, markRejected(err, w, "")
		}
		if page.NextCursor != "" {
			st.PendingCursor = page.NextCursor

			return true, nil
		}
		if err := validateGraph(ctx, tx, st.PendingGeneration); err != nil {
			return false, markRejected(err, w, "")
		}
		st.ActiveGeneration, st.Through = st.PendingGeneration, w
		st.Rebuild = false
		st.ReplayWatermark = nil
		st.clearPending()

		return true, nil
	})
}

func insertSnapshotPage(ctx context.Context, tx pgx.Tx, generation, watermark int64, records []Row) error {
	if len(records) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range records {
		if row.Revision < 0 || row.Revision > watermark {
			return invalid("invalid_snapshot_record_revision")
		}
		if err := row.Record.validate(); err != nil {
			return err
		}
		query, args, err := recordQuery(generation, row, false)
		if err != nil {
			return err
		}
		batch.Queue(query, args...)
	}
	results := tx.SendBatch(ctx, batch)
	defer func() { _ = results.Close() }()
	for range records {
		tag, err := results.Exec()
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return invalid("duplicate_snapshot_record")
		}
	}

	return results.Close()
}

func markRejected(err error, revision int64, eventID string) error {
	var problem *rejected
	if errors.As(err, &problem) {
		return &rejected{revision: revision, eventID: eventID, reason: problem.reason}
	}

	return err
}
