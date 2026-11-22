package config

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/overmindv/recommendations/internal/ranking"
	"github.com/overmindv/recommendations/pkg/api/goalcompletion"
)

type StudyPlans struct {
	Ranking          ranking.Policy
	Recent           time.Duration
	AdaptationWindow time.Duration
	CompletionEvents bool
	ProjectionMaxAge time.Duration
}

func loadStudyPlans(cfg Config) (*StudyPlans, error) {
	completion, err := envSwitch("RECOMMENDATIONS_GOAL_COMPLETION_ENABLED")
	if err != nil {
		return nil, err
	}
	adaptive, err := envSwitch("RECOMMENDATIONS_STUDY_ADAPTATION_ENABLED")
	if err != nil {
		return nil, err
	}
	enabled, err := envSwitch("RECOMMENDATIONS_STUDY_PLANS_ENABLED")
	if err != nil {
		return nil, err
	}
	if !enabled {
		if completion {
			return nil, errors.New("goal completion events require study plans")
		}
		if adaptive {
			return nil, errors.New("study adaptation requires study plans")
		}

		return nil, nil
	}
	if !cfg.Resources.Enabled || !cfg.Evidence.Enabled || cfg.Mastery == nil {
		return nil, errors.New("study plans require resource projection, learning evidence and mastery")
	}
	if completion && (cfg.Evidence.ContentTopic == goalcompletion.Topic || cfg.Resources.MaterialTopic == goalcompletion.Topic) {
		return nil, errors.New("goal completion topic must be separate from input topics")
	}
	raw := strings.TrimSpace(os.Getenv("RECOMMENDATIONS_STUDY_RANKING_POLICY"))
	if raw == "" || len(raw) > 4096 {
		return nil, errors.New("missing or oversized study ranking policy")
	}
	var policy ranking.Policy
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decodeErr := decoder.Decode(&policy); decodeErr != nil || !policy.Valid() {
		return nil, errors.New("invalid study ranking policy")
	}
	var trailing any
	if !errors.Is(decoder.Decode(&trailing), io.EOF) {
		return nil, errors.New("invalid study ranking policy")
	}
	recent, err := time.ParseDuration(strings.TrimSpace(os.Getenv("RECOMMENDATIONS_STUDY_RECENT_WINDOW")))
	if err != nil || recent < time.Hour || recent > 30*24*time.Hour || recent%time.Second != 0 {
		return nil, errors.New("study recent window must be a duration from 1h to 720h in whole seconds")
	}
	maxAge := 90 * time.Second
	if raw := strings.TrimSpace(os.Getenv("RECOMMENDATIONS_STUDY_PROJECTION_MAX_AGE")); raw != "" {
		maxAge, err = time.ParseDuration(raw)
		if err != nil || maxAge < time.Minute || maxAge > 15*time.Minute || maxAge%time.Second != 0 {
			return nil, errors.New("study projection age must be a duration from 1m to 15m in whole seconds")
		}
	}
	study := &StudyPlans{Ranking: policy, Recent: recent, CompletionEvents: completion, ProjectionMaxAge: maxAge}
	if adaptive {
		window, windowErr := time.ParseDuration(strings.TrimSpace(os.Getenv("RECOMMENDATIONS_STUDY_ADAPTATION_WINDOW")))
		if windowErr != nil || window < time.Second || window > 5*time.Minute || window%time.Second != 0 {
			return nil, errors.New("study adaptation window must be a duration from 1s to 5m in whole seconds")
		}
		study.AdaptationWindow = window
	}

	return study, nil
}
