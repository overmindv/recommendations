package config

import (
	"strings"
	"testing"
	"time"

	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/pkg/api/goalcompletion"
)

const (
	testStudyVersion = "test"
	testStudyRanking = `{"policy_version":"` + testStudyVersion + `","target_bps":8000,"unknown_gap_bps":3000,"prerequisite_min_bps":7500,"medium_difficulty_bps":5000,"hard_difficulty_bps":9000,"weights":{"gap":1}}`
)

func TestStudyPlansRequireExplicitPolicy(t *testing.T) {
	t.Setenv("RECOMMENDATIONS_GOAL_COMPLETION_ENABLED", "")
	t.Setenv("RECOMMENDATIONS_STUDY_PROJECTION_MAX_AGE", "")
	t.Setenv("RECOMMENDATIONS_STUDY_ADAPTATION_ENABLED", "")
	t.Setenv("RECOMMENDATIONS_STUDY_PLANS_ENABLED", "")
	if cfg, err := loadStudyPlans(Config{}); err != nil || cfg != nil {
		t.Fatal("study plans should be disabled by default:", cfg, err)
	}
	t.Setenv("RECOMMENDATIONS_STUDY_PLANS_ENABLED", "true")
	if _, err := loadStudyPlans(Config{}); err == nil {
		t.Fatal("study plans enabled without live projections")
	}
	prerequisites := Config{Resources: Resources{Enabled: true}, Evidence: Evidence{Enabled: true}, Mastery: &mastery.Policy{}}
	const policy = testStudyRanking
	t.Setenv("RECOMMENDATIONS_STUDY_RANKING_POLICY", policy)
	t.Setenv("RECOMMENDATIONS_STUDY_RECENT_WINDOW", "24h")
	if cfg, err := loadStudyPlans(prerequisites); err != nil || cfg == nil || cfg.Recent != 24*time.Hour ||
		cfg.Ranking.Version != testStudyVersion || cfg.AdaptationWindow != 0 || cfg.ProjectionMaxAge != 90*time.Second {
		t.Fatal("valid explicit policy rejected:", cfg, err)
	}
	for _, bad := range []string{"59s", "75s2ms", "16m"} {
		t.Setenv("RECOMMENDATIONS_STUDY_PROJECTION_MAX_AGE", bad)
		if _, err := loadStudyPlans(prerequisites); err == nil {
			t.Fatal("invalid projection age accepted:", bad)
		}
	}
	t.Setenv("RECOMMENDATIONS_STUDY_PROJECTION_MAX_AGE", "2m")
	if cfg, err := loadStudyPlans(prerequisites); err != nil || cfg.ProjectionMaxAge != 2*time.Minute {
		t.Fatal("could not configure the maximum projection age:", cfg, err)
	}
	t.Setenv("RECOMMENDATIONS_GOAL_COMPLETION_ENABLED", "true")
	if cfg, err := loadStudyPlans(prerequisites); err != nil || !cfg.CompletionEvents || cfg.AdaptationWindow != 0 {
		t.Fatal("goal completion should not depend on automatic replanning:", cfg, err)
	}
	collision := prerequisites
	collision.Evidence.ContentTopic = goalcompletion.Topic
	if _, err := loadStudyPlans(collision); err == nil {
		t.Fatal("goal completion accepted an input topic collision")
	}
	t.Setenv("RECOMMENDATIONS_STUDY_PLANS_ENABLED", "")
	if _, err := loadStudyPlans(prerequisites); err == nil {
		t.Fatal("goal completion allowed without study plans")
	}
	t.Setenv("RECOMMENDATIONS_GOAL_COMPLETION_ENABLED", "")
	t.Setenv("RECOMMENDATIONS_STUDY_PLANS_ENABLED", "true")
	for _, bad := range []string{"", "5m", "1h5ms", "721h"} {
		t.Setenv("RECOMMENDATIONS_STUDY_RECENT_WINDOW", bad)
		if _, err := loadStudyPlans(prerequisites); err == nil {
			t.Fatal("invalid recent window accepted:", bad)
		}
	}
	t.Setenv("RECOMMENDATIONS_STUDY_RECENT_WINDOW", "24h")
	t.Setenv("RECOMMENDATIONS_STUDY_ADAPTATION_ENABLED", "true")
	for _, bad := range []string{"", "500ms", "1s1ms", "6m"} {
		t.Setenv("RECOMMENDATIONS_STUDY_ADAPTATION_WINDOW", bad)
		if _, err := loadStudyPlans(prerequisites); err == nil {
			t.Fatal("invalid adaptation window accepted:", bad)
		}
	}
	t.Setenv("RECOMMENDATIONS_STUDY_ADAPTATION_WINDOW", "20s")
	if cfg, err := loadStudyPlans(prerequisites); err != nil || cfg.AdaptationWindow != 20*time.Second {
		t.Fatal("explicit adaptation window rejected:", cfg, err)
	}
	t.Setenv("RECOMMENDATIONS_STUDY_PLANS_ENABLED", "")
	if _, err := loadStudyPlans(prerequisites); err == nil {
		t.Fatal("adaptation allowed without study plans")
	}
	t.Setenv("RECOMMENDATIONS_STUDY_PLANS_ENABLED", "true")
	t.Setenv("RECOMMENDATIONS_STUDY_RANKING_POLICY", policy+" {}")
	if _, err := loadStudyPlans(prerequisites); err == nil {
		t.Fatal("trailing data accepted")
	}
}

func TestStudyRankingRollbackRequiresExplicitValidPolicy(t *testing.T) {
	t.Setenv("RECOMMENDATIONS_STUDY_RANKING_POLICY_SLOT", "")
	t.Setenv("RECOMMENDATIONS_STUDY_RANKING_POLICY", testStudyRanking)
	t.Setenv("RECOMMENDATIONS_STUDY_RANKING_PREVIOUS_POLICY", strings.Replace(testStudyRanking, `"`+testStudyVersion+`"`, `"stable"`, 1))
	if p, slot, err := studyRanking(); err != nil || p.Version != testStudyVersion || slot != studyPolicyCurrent {
		t.Fatal("current policy not selected:", slot, err)
	}
	t.Setenv("RECOMMENDATIONS_STUDY_RANKING_POLICY", `{"bad_field":"private-fixture"}`)
	if _, _, err := studyRanking(); err == nil || strings.Contains(err.Error(), "private-fixture") {
		t.Fatal("broken current policy accepted or echoed")
	}
	t.Setenv("RECOMMENDATIONS_STUDY_RANKING_POLICY_SLOT", studyPolicyPrevious)
	if p, slot, err := studyRanking(); err != nil || p.Version != "stable" || slot != studyPolicyPrevious {
		t.Fatal("rollback did not select the previous policy:", slot, err)
	}
	for _, raw := range []string{"", "{}", testStudyRanking + " {}", strings.Repeat("x", 4097)} {
		t.Setenv("RECOMMENDATIONS_STUDY_RANKING_PREVIOUS_POLICY", raw)
		if _, _, err := studyRanking(); err == nil {
			t.Fatal("invalid previous policy accepted")
		}
	}
	t.Setenv("RECOMMENDATIONS_STUDY_RANKING_POLICY_SLOT", "other")
	if _, _, err := studyRanking(); err == nil {
		t.Fatal("unknown policy slot accepted")
	}
}

func TestStudyMetricsRequireSeparateScrapeToken(t *testing.T) {
	t.Setenv("RECOMMENDATIONS_STUDY_PLANS_ENABLED", "true")
	t.Setenv("RECOMMENDATIONS_GOAL_COMPLETION_ENABLED", "")
	t.Setenv("RECOMMENDATIONS_STUDY_ADAPTATION_ENABLED", "")
	t.Setenv("RECOMMENDATIONS_STUDY_RECENT_WINDOW", "24h")
	t.Setenv("RECOMMENDATIONS_STUDY_RANKING_POLICY_SLOT", "")
	t.Setenv("RECOMMENDATIONS_STUDY_RANKING_POLICY", testStudyRanking)
	apiToken := strings.Repeat("a", 32)
	ownerToken := strings.Repeat("t", 32)
	prerequisites := Config{ServiceToken: apiToken, Resources: Resources{Enabled: true, TasksToken: ownerToken},
		Evidence: Evidence{Enabled: true}, Mastery: &mastery.Policy{}}
	for _, token := range []string{apiToken, ownerToken, "short", strings.Repeat("s", 32) + "\n", strings.Repeat("s", 257)} {
		t.Setenv("RECOMMENDATIONS_STUDY_METRICS_TOKEN", token)
		if _, err := loadStudyPlans(prerequisites); err == nil {
			t.Fatal("invalid or reused scrape token accepted")
		}
	}
	t.Setenv("RECOMMENDATIONS_STUDY_METRICS_TOKEN", strings.Repeat("s", 32))
	if cfg, err := loadStudyPlans(prerequisites); err != nil || len(cfg.MetricsToken) != 32 {
		t.Fatal("valid scrape token rejected:", err)
	}
}
