package studyplans

import (
	"context"
	"errors"
	"time"

	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

var ErrUnavailable = errors.New("study plan sources unavailable")

type DraftBuilder interface {
	Build(context.Context, string, string, Sources) (Draft, error)
}

type studyAlternativeSource interface {
	StudyAlternativeAvailable(context.Context, ranking.ResourceKey) bool
}

type StepAvailability interface {
	StudyStepAvailable(context.Context, ranking.ResourceKey, string) (bool, error)
}

type Service struct {
	store            *Postgres
	builder          DraftBuilder
	slots            chan struct{}
	access           StepAvailability
	maxProjectionAge time.Duration
}

func NewService(store *Postgres, builder DraftBuilder) *Service {
	return &Service{store: store, builder: builder, slots: make(chan struct{}, 4)}
}

func (s Service) WithLiveCatalog(access StepAvailability, maxAge time.Duration) *Service {
	s.access, s.maxProjectionAge = access, maxAge

	return &s
}

func (s *Service) Generate(ctx context.Context, actor, goal string, input Generation) (View, error) {
	source, replay, err := s.store.Prepare(ctx, actor, goal, input)
	if err != nil {
		return View{}, err
	}
	if replay != nil {
		return s.catalogView(ctx, *replay, nil)
	}
	select {
	case <-ctx.Done():
		return View{}, ctx.Err()
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return s.savedPlanFallback(ctx, actor, goal, source.PlanVersion, ErrUnavailable)
	}
	draft, err := s.builder.Build(ctx, actor, goal, source)
	if err != nil {
		_, replay, retryErr := s.store.Prepare(ctx, actor, goal, input)
		if retryErr != nil {
			return View{}, retryErr
		}
		if replay != nil {
			return s.catalogView(ctx, *replay, nil)
		}

		return s.savedPlanFallback(ctx, actor, goal, source.PlanVersion, err)
	}
	view, err := s.store.Save(ctx, actor, goal, input, source, draft)

	return s.catalogView(ctx, view, err)
}

func (s *Service) savedPlanFallback(ctx context.Context, actor, goal string, version int64, reason error) (View, error) {
	if s.maxProjectionAge <= 0 || version <= 0 {
		return View{}, errors.Join(ErrUnavailable, reason)
	}
	saved, err := s.Current(ctx, actor, goal)
	if err != nil {
		return View{}, err
	}
	if saved.Version != version {
		return View{}, ErrConflict
	}
	freshness := *saved.Freshness
	if !errors.Is(reason, ErrProjectionStale) || freshness.Status == freshCatalog {
		freshness.Status = unavailableCatalog
	}

	return restrictCatalog(saved, freshness), nil
}

func (s *Service) Current(ctx context.Context, actor, goal string) (View, error) {
	view, err := s.store.current(ctx, actor, goal, s.maxProjectionAge)
	if !errors.Is(err, errFreshnessRead) {
		return view, err
	}
	view, err = s.store.Current(ctx, actor, goal)
	if err != nil {
		return View{}, err
	}

	return restrictCatalog(view, Freshness{Status: unavailableCatalog}), nil
}

func (s *Service) Explain(ctx context.Context, actor, goal string, version int64) (Explanation, error) {
	result, err := s.store.Explain(ctx, actor, goal, version)
	if err != nil {
		return Explanation{}, err
	}
	owner, ok := s.builder.(studyAlternativeSource)
	var freshness *Freshness
	if s.maxProjectionAge > 0 {
		freshness = new(s.freshness(ctx))
	}
	for i := range result.Steps {
		step := &result.Steps[i]
		if step.Alternative == nil {
			continue
		}
		if freshness == nil {
			if !ok || !owner.StudyAlternativeAvailable(ctx, step.Alternative.ResourceKey) {
				step.Alternative = nil
			}

			continue
		}
		if freshness.Status != freshCatalog || s.access == nil {
			step.Alternative = nil

			continue
		}
		available, accessErr := s.access.StudyStepAvailable(ctx, step.Alternative.ResourceKey, step.Evidence.TopicID)
		if accessErr != nil {
			freshness.Status = unavailableCatalog
		}
		if accessErr != nil || !available {
			step.Alternative = nil
		}
	}
	result.Freshness = freshness

	return result, nil
}

func (s *Service) SubmitFeedback(ctx context.Context, actor, goal string, input Feedback) (FeedbackReceipt, error) {
	return s.store.SubmitFeedback(ctx, actor, goal, input)
}

func (s *Service) CompleteActivity(ctx context.Context, actor, goal string, version int64, ordinal int) (View, error) {
	view, err := s.store.CompleteActivity(ctx, actor, goal, version, ordinal)

	return s.catalogView(ctx, view, err)
}

func (s *Service) StartActivity(ctx context.Context, actor, goal string, version int64, ordinal int) (View, error) {
	if s.maxProjectionAge > 0 {
		if version < 1 || ordinal < 1 || ordinal > planner.MaxLearningItems {
			return View{}, ErrInvalid
		}
		view, err := s.Current(ctx, actor, goal)
		if err != nil {
			return View{}, err
		}
		if version != view.Version || view.Stale {
			return View{}, ErrConflict
		}
		if view.Freshness.Status != freshCatalog || s.access == nil {
			return View{}, ErrUnavailable
		}
		if view.NextStep == nil || *view.NextStep != ordinal {
			return View{}, ErrStepBlocked
		}
		item := findItem(view, ordinal)
		if item == nil {
			return View{}, ErrNotFound
		}
		// Проверка владельца не держит блокировку плана. Запись повторно сверяет версию и шаг.
		ok, err := s.access.StudyStepAvailable(ctx, item.ResourceKey, item.TopicID)
		if err != nil {
			return View{}, ErrUnavailable
		}
		if !ok {
			return View{}, ErrResourceUnavailable
		}
	}
	view, err := s.store.StartActivity(ctx, actor, goal, version, ordinal)

	return s.catalogView(ctx, view, err)
}

func (s *Service) Assess(ctx context.Context, actor, goal string, version int64, ordinal int) (View, error) {
	view, err := s.store.Assess(ctx, actor, goal, version, ordinal)

	return s.catalogView(ctx, view, err)
}

func (s *Service) freshness(ctx context.Context) Freshness {
	value, _ := readFreshness(ctx, s.store.pool, s.maxProjectionAge)

	return value
}

func (s *Service) catalogView(ctx context.Context, view View, err error) (View, error) {
	if err != nil || s.maxProjectionAge <= 0 {
		return view, err
	}

	return restrictCatalog(view, s.freshness(ctx)), nil
}

func restrictCatalog(view View, freshness Freshness) View {
	view.Freshness = &freshness
	if freshness.Status != freshCatalog {
		view.NextStep = nil
		if view.BlockedReason == "" || view.BlockedReason == "catalog_stale" || view.BlockedReason == "catalog_unavailable" {
			view.BlockedReason = "catalog_" + freshness.Status
		}
	}

	return view
}
