package config

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/overmindv/recommendations/internal/ranking"
)

type StudyPlans struct {
	Ranking ranking.Policy
	Recent  time.Duration
}

func loadStudyPlans(cfg Config) (*StudyPlans, error) {
	enabled, err := envSwitch("RECOMMENDATIONS_STUDY_PLANS_ENABLED")
	if err != nil || !enabled {
		return nil, err
	}
	if !cfg.Resources.Enabled || !cfg.Evidence.Enabled || cfg.Mastery == nil {
		return nil, errors.New("study plans require resource projection, learning evidence and mastery")
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

	return &StudyPlans{Ranking: policy, Recent: recent}, nil
}
