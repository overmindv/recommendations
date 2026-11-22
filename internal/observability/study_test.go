package observability

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStudyScrapeRequiresDedicatedAuthorization(t *testing.T) {
	t.Parallel()

	m := NewStudy(nil, "pilot-v1", "current", false)
	token := strings.Repeat("s", 32)
	for _, headers := range [][]string{nil, {token}, {"Bearer " + strings.Repeat("a", 32)}, {"Bearer " + token, "Bearer " + token}} {
		r := httptest.NewRequest(http.MethodGet, StudyMetricsPath, nil)
		r.Header["Authorization"] = headers
		w := httptest.NewRecorder()
		m.Handler(token).ServeHTTP(w, r)
		if w.Code != http.StatusForbidden || strings.Contains(w.Body.String(), "recommendations_") {
			t.Fatal("unauthorized scrape returned monitoring data")
		}
	}
	r := httptest.NewRequest(http.MethodGet, StudyMetricsPath, nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	m.Handler(token).ServeHTTP(w, r)
	if w.Code != http.StatusOK || w.Header().Get("Cache-Control") != "no-store" ||
		!strings.Contains(w.Body.String(), `recommendations_study_ranking_policy_info{slot="current",version="pilot-v1"} 1`) ||
		strings.Contains(w.Body.String(), token) {
		t.Fatal("authenticated scrape did not expose only the expected metrics")
	}
}

func TestStudyMetricCardinalityAndPrivateError(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	m := NewStudy(log, "v1", "previous", false)
	count := func() int {
		t.Helper()
		families, err := m.registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		var total int
		for _, family := range families {
			total += len(family.GetMetric())
		}

		return total
	}
	before := count()
	if before != 41 {
		t.Fatal("unexpected number of initialized label combinations:", before)
	}
	for range 200 {
		m.Plan(t.Context(), Generate, FallbackUnavailable, time.Millisecond)
		m.Candidates(Generate, false)
		m.EvidenceFailure(EvidenceStoreFailure)
	}
	if count() != before || !strings.Contains(logs.String(), `"reason":"fallback_unavailable"`) ||
		strings.Contains(logs.String(), `"error":`) || strings.Contains(logs.String(), `"goal_id":`) {
		t.Fatal("dynamic series or missing reason code")
	}
}
