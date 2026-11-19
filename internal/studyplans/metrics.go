package studyplans

import (
	"context"
	"errors"
	"time"

	"github.com/overmindv/recommendations/internal/observability"
)

func planResult(success observability.PlanResult, err error) observability.PlanResult {
	switch {
	case err == nil:
		return success
	case errors.Is(err, context.Canceled):
		return observability.Canceled
	case errors.Is(err, ErrConflict), errors.Is(err, ErrRequestUsed), errors.Is(err, ErrLeaseLost):
		return observability.Conflict
	case errors.Is(err, ErrInvalid), errors.Is(err, ErrNotFound), errors.Is(err, ErrLimit),
		errors.Is(err, ErrBudget), errors.Is(err, ErrStaleRebuild):
		return observability.Rejected
	case errors.Is(err, ErrUnavailable), errors.Is(err, ErrProjectionStale), errors.Is(err, context.DeadlineExceeded):
		return observability.Unavailable
	default:
		return observability.Failed
	}
}

func (s *Service) WatchCatalog(ctx context.Context) error {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		s.sampleCatalog(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
	return nil
}

func (s *Service) sampleCatalog(ctx context.Context) {
	query, cancel := context.WithTimeout(ctx, 2*time.Second)
	freshness := s.freshness(query)
	cancel()
	state := observability.CatalogUnavailable
	switch freshness.Status {
	case freshCatalog:
		state = observability.CatalogFresh
	case staleCatalog:
		state = observability.CatalogStale
	}
	s.metrics.Catalog(state, [3]*time.Time{freshness.EntitiesAt, freshness.TasksAt, freshness.ContentAt})
}
