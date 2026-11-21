package config

import (
	"testing"
	"time"

	"github.com/overmindv/recommendations/internal/mastery"
)

func TestStudyPlansRequireExplicitPolicy(t *testing.T) {
	t.Setenv("RECOMMENDATIONS_STUDY_PLANS_ENABLED", "")
	if cfg, err := loadStudyPlans(Config{}); err != nil || cfg != nil {
		t.Fatal("study plans should be disabled by default:", cfg, err)
	}
	t.Setenv("RECOMMENDATIONS_STUDY_PLANS_ENABLED", "true")
	if _, err := loadStudyPlans(Config{}); err == nil {
		t.Fatal("study plans enabled without live projections")
	}
	prerequisites := Config{Resources: Resources{Enabled: true}, Evidence: Evidence{Enabled: true}, Mastery: &mastery.Policy{}}
	const policy = `{"policy_version":"test","target_bps":8000,"unknown_gap_bps":3000,"prerequisite_min_bps":7500,"medium_difficulty_bps":5000,"hard_difficulty_bps":9000,"weights":{"gap":1}}`
	t.Setenv("RECOMMENDATIONS_STUDY_RANKING_POLICY", policy)
	t.Setenv("RECOMMENDATIONS_STUDY_RECENT_WINDOW", "24h")
	if cfg, err := loadStudyPlans(prerequisites); err != nil || cfg == nil || cfg.Recent != 24*time.Hour || cfg.Ranking.Version != "test" {
		t.Fatal("valid explicit policy rejected:", cfg, err)
	}
	for _, bad := range []string{"", "5m", "1h5ms", "721h"} {
		t.Setenv("RECOMMENDATIONS_STUDY_RECENT_WINDOW", bad)
		if _, err := loadStudyPlans(prerequisites); err == nil {
			t.Fatal("invalid recent window accepted:", bad)
		}
	}
	t.Setenv("RECOMMENDATIONS_STUDY_RECENT_WINDOW", "24h")
	t.Setenv("RECOMMENDATIONS_STUDY_RANKING_POLICY", policy+" {}")
	if _, err := loadStudyPlans(prerequisites); err == nil {
		t.Fatal("trailing data accepted")
	}
}
