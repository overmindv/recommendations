package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/topiccatalog"
)

const (
	onboardingTopic  = "00000000-0000-0000-0000-000000001001"
	learnerGoalsPath = "/v1/learner/goals"
	learnerProfile   = "/v1/learner/profile"
)

func onboardingRequest(method, path, userID, body string, key int, version string) *http.Request {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-User-ID", userID)
	if key > 0 {
		r.Header.Set("Idempotency-Key", fmt.Sprintf("00000000-0000-0000-0000-%012d", key))
	}
	if version != "" {
		r.Header.Set("X-Expected-Version", version)
	}
	if len(body) != 0 {
		r.Header.Set("Content-Type", "application/json")
	}

	return r
}

func performOnboarding(t *testing.T, mux *http.ServeMux, r *http.Request, status int, contains string) string {
	t.Helper()
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	body := w.Body.String()
	if w.Code != status || (contains != "" && !strings.Contains(body, contains)) {
		t.Fatal(r.Method, r.URL.Path, "wanted", status, contains, "got", w.Code, body)
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("response can be cached", w.Header())
	}

	return body
}

func TestOnboardingGuards(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	type unusedStore struct{ learnergoals.Store }
	RegisterOnboarding(mux, token, unusedStore{})
	r := onboardingRequest(http.MethodGet, learnerProfile, actor, "", 0, "")
	r.Header.Add("X-User-ID", actor)
	performOnboarding(t, mux, r, http.StatusForbidden, "PERMISSION_DENIED")
	r = onboardingRequest(http.MethodGet, "/v1/learner/goal-templates", actor, "", 0, "")
	r.Header.Del("Authorization")
	performOnboarding(t, mux, r, http.StatusForbidden, "PERMISSION_DENIED")
	r = onboardingRequest(http.MethodPut, learnerProfile, actor, `{"weekly_minutes":30,"timezone":"UTC"}`, 1, "0")
	r.Header.Add("Idempotency-Key", r.Header.Get("Idempotency-Key"))
	performOnboarding(t, mux, r, http.StatusBadRequest, "INVALID_ARGUMENT")
	for _, body := range []string{
		`{"timezone":"UTC","weekly_minutes":30,"user_id":"another actor"}`,
		`{"timezone":"UTC","weekly_minutes":30}{}`,
	} {
		performOnboarding(t, mux, onboardingRequest(http.MethodPut, learnerProfile, actor, body, 1, "0"), 400, "INVALID_ARGUMENT")
	}
	body := strings.Repeat(" ", maxOnboardingBody) + "{}"
	performOnboarding(t, mux, onboardingRequest(http.MethodPut, learnerProfile, actor, body, 1, "0"), 413, "Request too large")
	performOnboarding(t, mux, onboardingRequest(http.MethodPut, learnerProfile, actor, "{}", 1, "+0"), 400, "INVALID_ARGUMENT")
	performOnboarding(t, mux, onboardingRequest(http.MethodPost, learnerGoalsPath, actor, "{}", 1, "0"), 400, "INVALID_ARGUMENT")
	performOnboarding(t, mux, onboardingRequest(http.MethodPut, learnerProfile, actor, "", 1, "0"), 415, "Expected application/json")
	performOnboarding(t, mux, onboardingRequest(http.MethodGet, "/v1/learner/goal-templates", actor, "", 0, ""), 200, "exam:1")
}

func TestOnboardingDeadlineAndSkills(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 19, 2, 0, 0, 0, time.UTC)
	goal := learnergoals.Goal{
		Timezone: "America/Los_Angeles",
		Input: learnergoals.Input{
			Deadline: "2026-11-18", TargetTopics: []string{onboardingTopic},
			Preferences: learnergoals.Preferences{
				SelfAssessments: []learnergoals.SelfAssessment{{TopicID: onboardingTopic, Level: "advanced"}},
			},
		},
	}
	view := presentGoal(goal, now)
	if view.Feasibility != feasibilityNotEvaluated || len(view.Skills) != 1 || view.Skills[0].State != "unknown" {
		t.Fatal("goal was evaluated without evidence", view)
	}
	view = presentGoal(goal, now.Add(12*time.Hour))
	if view.Feasibility != feasibilityInfeasible || view.FeasibilityReason != reasonDeadlinePassed {
		t.Fatal("expired goal was not flagged", view)
	}
}

func TestPostgresOnboarding(t *testing.T) {
	t.Parallel()

	dsn := os.Getenv("GOALS_TEST_DSN")
	if dsn == "" {
		t.Skip("GOALS_TEST_DSN is not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || (!strings.HasSuffix(cfg.ConnConfig.Database, "_test") && !strings.HasSuffix(cfg.ConnConfig.Database, "_component")) {
		t.Fatal("GOALS_TEST_DSN must point to a dedicated test or component database")
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var owner, other string
	if scanErr := pool.QueryRow(t.Context(), "SELECT gen_random_uuid()::text, gen_random_uuid()::text").Scan(&owner, &other); scanErr != nil {
		t.Fatal(scanErr)
	}
	var status, calls atomic.Int32
	status.Store(http.StatusOK)
	entities := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		result := status.Load()
		w.WriteHeader(int(result))
		if result == http.StatusOK {
			_ = json.NewEncoder(w).Encode(map[string]string{"id": onboardingTopic, "status": "active"})
		}
	}))
	defer entities.Close()
	catalog, err := topiccatalog.New(entities.URL)
	if err != nil {
		t.Fatal(err)
	}
	store := learnergoals.NewValidatedPostgres(pool, func(ctx context.Context, input learnergoals.Input) error {
		if err := learnergoals.ValidateTemplate(input.TemplateVersion); err != nil {
			return err
		}

		return catalog.Validate(ctx, input.TargetTopics)
	})
	mux := http.NewServeMux()
	RegisterOnboarding(mux, token, store)
	request := func(method, path, body string, key int, version string, want int, contains string) string {
		t.Helper()

		return performOnboarding(t, mux, onboardingRequest(method, path, owner, body, key, version), want, contains)
	}
	request("GET", learnerProfile, "", 0, "", 200, `"availability":null`)
	request("GET", learnerGoalsPath, "", 0, "", 200, "[]")
	goal := `{"title":"Lists","template_version":"practice:1","target_topics":["` + onboardingTopic + `"],"active":true,"weekly_minutes":60}`
	request("POST", learnerGoalsPath, goal, 1, "", 409, "PROFILE_REQUIRED")
	request("PUT", learnerProfile, `{"weekly_minutes":90,"timezone":"UTC"}`, 2, "0", 200, `"version":1`)
	status.Store(http.StatusNotFound)
	request("POST", learnerGoalsPath, goal, 1, "", 422, "UNKNOWN_TOPIC")
	status.Store(http.StatusOK)
	request("POST", learnerGoalsPath, strings.Replace(goal, "practice:1", "missing:1", 1), 1, "", 422, "UNKNOWN_TEMPLATE")
	expired := strings.Replace(goal, `"active":true`, `"active":true,"deadline":"2000-01-01"`, 1)
	request("POST", learnerGoalsPath, expired, 1, "", 422, "Goal deadline has passed")
	created := request("POST", learnerGoalsPath, goal, 1, "", 201, `"state":"unknown"`)
	var view goalView
	if err := json.Unmarshal([]byte(created), &view); err != nil || view.ID == "" || view.Feasibility != feasibilityNotEvaluated {
		t.Fatal("invalid goal response", created, err)
	}
	path := learnerGoalsPath + "/" + view.ID
	status.Store(http.StatusServiceUnavailable)
	callCount := calls.Load()
	if replay := request("POST", learnerGoalsPath, goal, 1, "", 201, `"state":"unknown"`); replay != created || calls.Load() != callCount {
		t.Fatal("saved response changed or required the catalog on replay")
	}
	request("POST", learnerGoalsPath, strings.Replace(goal, "Lists", "Queues", 1), 1, "", 409, "IDEMPOTENCY_CONFLICT")
	request("POST", learnerGoalsPath, goal, 3, "", 503, "SERVICE_UNAVAILABLE")
	performOnboarding(t, mux, onboardingRequest("GET", path, other, "", 0, ""), 404, "NOT_FOUND")
	status.Store(http.StatusOK)
	request("POST", learnerGoalsPath, goal, 3, "", 422, "Goals exceed the weekly budget")
	request("PUT", path, goal, 4, "2", 409, "VERSION_CONFLICT")
	request("PUT", path, goal, 4, "1", 200, `"version":2`)
	request("DELETE", path, "", 5, "2", 204, "")
	request("DELETE", path, "", 5, "2", 204, "")
	request("GET", path, "", 0, "", 404, "NOT_FOUND")
}
