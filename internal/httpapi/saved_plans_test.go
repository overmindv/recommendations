package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/overmindv/recommendations/internal/savedplans"
)

type plansStub struct {
	calls int
	actor string
}

func (s *plansStub) Get(_ context.Context, _, actor string) (savedplans.Plan, error) {
	s.calls++
	s.actor = actor

	return savedplans.Plan{}, savedplans.ErrNotFound
}
func (s *plansStub) List(_ context.Context, actor string) ([]savedplans.Plan, error) {
	s.calls++
	s.actor = actor

	return []savedplans.Plan{}, nil
}
func (s *plansStub) Create(context.Context, string, savedplans.Input) (savedplans.Plan, error) {
	s.calls++

	return savedplans.Plan{}, nil
}
func (s *plansStub) Change(context.Context, string, string, string, savedplans.Input) (savedplans.Plan, error) {
	s.calls++

	return savedplans.Plan{}, savedplans.ErrConflict
}
func (s *plansStub) Clone(context.Context, string, string, string) (savedplans.Plan, error) {
	s.calls++

	return savedplans.Plan{}, savedplans.ErrNotFound
}
func TestSavedPlanTransportAuthAndRevisionCodes(t *testing.T) {
	t.Parallel()

	id := "00000000-0000-4000-8000-000000000081"
	s := &plansStub{}
	mux := http.NewServeMux()
	RegisterSavedPlans(mux, "token", s)
	cases := []struct {
		method, path, token, actor, body string
		status                           int
	}{
		{http.MethodGet, "/v1/learning-plans/" + id, "", "", "", 403},
		{http.MethodGet, "/v1/learning-plans/" + id, "token", "", "", 404},
		{http.MethodGet, "/v1/learning-plans", "token", "", "", 403},
		{"POST", "/v1/learning-plans/" + id + "/material", "token", "", `{}`, 403},
		{"POST", "/v1/learning-plans/" + id + "/material", "token", id, `{"revision":1,"material_id":"python","status":"DONE"}`, 409},
		{"POST", "/v1/learning-plans", "token", id, `{"id":"invalid"}`, 400},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		if tc.token != "" {
			r.Header.Set("Authorization", "Bearer "+tc.token)
		}
		if tc.actor != "" {
			r.Header.Set("X-User-ID", tc.actor)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != tc.status || w.Header().Get("Cache-Control") != noStoreCache {
			t.Fatal(tc, w.Code, w.Body.String())
		}
	}
	if s.calls != 2 {
		t.Fatal("unauthorized access reached store", s.calls)
	}
}
