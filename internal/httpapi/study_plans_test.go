package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/overmindv/recommendations/internal/studyplans"
)

const (
	studyCallGenerate   = "generate"
	studyCallActivity   = "activity"
	studyCallAssessment = "assessment"
)

type studyRouteStub struct {
	actor, goal string
	generation  studyplans.Generation
	version     int64
	last        string
	step        int
	err         error
}

func (s *studyRouteStub) Current(_ context.Context, actor, goal string) (studyplans.View, error) {
	s.actor, s.goal, s.last = actor, goal, "current"

	return studyplans.View{GoalID: goal, Version: 1, NextStep: new(2)}, s.err
}

func (s *studyRouteStub) Generate(_ context.Context, actor, goal string, generation studyplans.Generation) (studyplans.View, error) {
	s.actor, s.goal, s.last, s.generation = actor, goal, studyCallGenerate, generation

	return studyplans.View{GoalID: goal, Version: 1}, s.err
}

func (s *studyRouteStub) CompleteActivity(_ context.Context, actor, goal string, version int64, step int) (studyplans.View, error) {
	s.actor, s.goal, s.last, s.version, s.step = actor, goal, studyCallActivity, version, step

	return studyplans.View{GoalID: goal, Version: version}, s.err
}

func (s *studyRouteStub) Assess(_ context.Context, actor, goal string, version int64, step int) (studyplans.View, error) {
	s.actor, s.goal, s.last, s.version, s.step = actor, goal, studyCallAssessment, version, step

	return studyplans.View{GoalID: goal, Version: version}, s.err
}

func TestStudyPlanRoutesAcceptOnlyVersionAndTrustedActor(t *testing.T) {
	t.Parallel()

	token := strings.Repeat("s", 32)
	const actor = "00000000-0000-4000-8000-000000000001"
	const goal = "00000000-0000-4000-8000-000000000002"
	const key = "00000000-0000-4000-8000-000000000003"
	const body = `{"goal_version":1,"availability_version":2,"plan_version":0}`
	service := &studyRouteStub{}
	router := http.NewServeMux()
	RegisterStudyPlans(router, token, service)
	send := func(method, path, bearer, input string, keys ...string) *httptest.ResponseRecorder {
		r := httptest.NewRequestWithContext(t.Context(), method, "/v1/learner/goals/"+goal+"/study-plan"+path, strings.NewReader(input))
		r.Header.Set("Authorization", "Bearer "+bearer)
		r.Header.Set("X-User-ID", actor)
		r.Header.Set("Content-Type", "application/json")
		for _, k := range keys {
			r.Header.Add("Idempotency-Key", k)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)

		return w
	}
	for _, tc := range []struct {
		response *httptest.ResponseRecorder
		status   int
	}{
		{send("GET", "", "wrong", ""), http.StatusForbidden},
		{send("PUT", "", token, body), http.StatusBadRequest},
		{send("PUT", "", token, body, key, key), http.StatusBadRequest},
		{send("PUT", "", token, `{"goal_version":1,"availability_version":2}`, key), http.StatusBadRequest},
		{send("PUT", "", token, `{"goal_version":1,"availability_version":2,"plan_version":0,"items":[]}`, key), http.StatusBadRequest},
		{send("PUT", "/steps/129/activity", token, `{"plan_version":1}`), http.StatusBadRequest},
		{send("PUT", "/steps/2/assessment", token, `{"plan_version":1,"verdict":"accepted"}`), http.StatusBadRequest},
	} {
		if tc.response.Code != tc.status || tc.response.Header().Get("Cache-Control") != noStoreCache {
			t.Fatal("unexpected response to invalid plan request:", tc.response.Code, tc.response.Body.String())
		}
	}
	if service.last != "" {
		t.Fatal("invalid request reached service:", service)
	}
	if res := send("PUT", "", token, body, key); res.Code != http.StatusOK || service.actor != actor ||
		service.goal != goal || service.last != studyCallGenerate || service.generation.RequestID != key ||
		service.generation.Expected != (studyplans.Expected{GoalVersion: 1, AvailabilityVersion: 2, PlanVersion: 0}) {
		t.Fatal("valid generation was routed incorrectly:", res.Code, service)
	}
	if res := send("GET", "", token, ""); res.Code != http.StatusOK || !strings.Contains(res.Body.String(), `"next_step":2`) {
		t.Fatal("current plan did not include next step:", res.Code, res.Body.String())
	}
	if res := send("PUT", "/steps/2/activity", token, `{"plan_version":1}`); res.Code != http.StatusOK ||
		service.step != 2 || service.version != 1 || service.last != studyCallActivity {
		t.Fatal("activity was routed incorrectly:", res.Code, service)
	}
	service.err = studyplans.ErrPending
	if res := send("PUT", "/steps/3/assessment", token, `{"plan_version":1}`); res.Code != http.StatusConflict ||
		!strings.Contains(res.Body.String(), "GRADE_PENDING") || service.last != studyCallAssessment || service.step != 3 {
		t.Fatal("pending grade was not reported separately:", res.Code, res.Body.String(), service)
	}
}
