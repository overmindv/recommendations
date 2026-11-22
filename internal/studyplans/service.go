package studyplans

import (
	"context"
	"errors"

	"github.com/overmindv/recommendations/internal/ranking"
)

var ErrUnavailable = errors.New("study plan sources unavailable")

type DraftBuilder interface {
	Build(context.Context, string, string, Sources) (Draft, error)
}

type studyAlternativeSource interface {
	StudyAlternativeAvailable(context.Context, ranking.ResourceKey) bool
}

type Service struct {
	store   *Postgres
	builder DraftBuilder
	slots   chan struct{}
}

func NewService(store *Postgres, builder DraftBuilder) *Service {
	return &Service{store: store, builder: builder, slots: make(chan struct{}, 4)}
}

func (s *Service) Generate(ctx context.Context, actor, goal string, input Generation) (View, error) {
	source, replay, err := s.store.Prepare(ctx, actor, goal, input)
	if err != nil {
		return View{}, err
	}
	if replay != nil {
		return *replay, nil
	}
	select {
	case <-ctx.Done():
		return View{}, ctx.Err()
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return View{}, ErrUnavailable
	}
	draft, err := s.builder.Build(ctx, actor, goal, source)
	if err != nil {
		_, replay, retryErr := s.store.Prepare(ctx, actor, goal, input)
		if retryErr != nil {
			return View{}, retryErr
		}
		if replay != nil {
			return *replay, nil
		}

		return View{}, errors.Join(ErrUnavailable, err)
	}

	return s.store.Save(ctx, actor, goal, input, source, draft)
}

func (s *Service) Current(ctx context.Context, actor, goal string) (View, error) {
	return s.store.Current(ctx, actor, goal)
}

func (s *Service) Explain(ctx context.Context, actor, goal string, version int64) (Explanation, error) {
	result, err := s.store.Explain(ctx, actor, goal, version)
	if err != nil {
		return Explanation{}, err
	}
	owner, ok := s.builder.(studyAlternativeSource)
	for i := range result.Steps {
		step := &result.Steps[i]
		if step.Alternative != nil && (!ok || !owner.StudyAlternativeAvailable(ctx, step.Alternative.ResourceKey)) {
			step.Alternative = nil
		}
	}

	return result, nil
}

func (s *Service) SubmitFeedback(ctx context.Context, actor, goal string, input Feedback) (FeedbackReceipt, error) {
	return s.store.SubmitFeedback(ctx, actor, goal, input)
}

func (s *Service) CompleteActivity(ctx context.Context, actor, goal string, version int64, ordinal int) (View, error) {
	return s.store.CompleteActivity(ctx, actor, goal, version, ordinal)
}

func (s *Service) StartActivity(ctx context.Context, actor, goal string, version int64, ordinal int) (View, error) {
	return s.store.StartActivity(ctx, actor, goal, version, ordinal)
}

func (s *Service) Assess(ctx context.Context, actor, goal string, version int64, ordinal int) (View, error) {
	return s.store.Assess(ctx, actor, goal, version, ordinal)
}
