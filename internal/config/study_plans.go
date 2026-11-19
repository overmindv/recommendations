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
	PolicySlot       string
	MetricsToken     string
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
	policy, slot, err := studyRanking()
	if err != nil {
		return nil, err
	}
	metricsToken := os.Getenv("RECOMMENDATIONS_STUDY_METRICS_TOKEN")
	if metricsToken != "" {
		if len(metricsToken) < 32 || len(metricsToken) > 256 ||
			strings.IndexFunc(metricsToken, func(c rune) bool { return c < '!' || c > '~' }) >= 0 {
			return nil, errors.New("study metrics token must contain 32 to 256 visible ASCII bytes")
		}
		for _, token := range [...]string{cfg.ServiceToken, cfg.CatalogSnapshotToken, cfg.Resources.TasksToken, cfg.Resources.ContentToken} {
			if metricsToken == token {
				return nil, errors.New("study metrics require a separate token")
			}
		}
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
	study := &StudyPlans{Ranking: policy, PolicySlot: slot, MetricsToken: metricsToken,
		Recent: recent, CompletionEvents: completion, ProjectionMaxAge: maxAge}
	if adaptive {
		window, err := time.ParseDuration(strings.TrimSpace(os.Getenv("RECOMMENDATIONS_STUDY_ADAPTATION_WINDOW")))
		if err != nil || window < time.Second || window > 5*time.Minute || window%time.Second != 0 {
			return nil, errors.New("study adaptation window must be a duration from 1s to 5m in whole seconds")
		}
		study.AdaptationWindow = window
	}
	return study, nil
}

func studyRanking() (ranking.Policy, string, error) {
	slot := strings.TrimSpace(os.Getenv("RECOMMENDATIONS_STUDY_RANKING_POLICY_SLOT"))
	key := "RECOMMENDATIONS_STUDY_RANKING_POLICY"
	switch slot {
	case "", "current":
		slot = "current"
	case "previous":
		key = "RECOMMENDATIONS_STUDY_RANKING_PREVIOUS_POLICY"
	default:
		return ranking.Policy{}, "", errors.New("study ranking policy slot must be current or previous")
	}
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" || len(raw) > 4096 {
		return ranking.Policy{}, "", errors.New("missing or oversized study ranking policy in " + key)
	}
	var policy ranking.Policy
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil || !policy.Valid() {
		return ranking.Policy{}, "", errors.New("invalid study ranking policy in " + key)
	}
	var trailing any
	if !errors.Is(decoder.Decode(&trailing), io.EOF) {
		return ranking.Policy{}, "", errors.New("invalid study ranking policy in " + key)
	}
	return policy, slot, nil
}
