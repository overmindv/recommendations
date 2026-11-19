package httpapi

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/studyplans"
)

type StudyPlans interface {
	Current(context.Context, string, string) (studyplans.View, error)
	Explain(context.Context, string, string, int64) (studyplans.Explanation, error)
	SubmitFeedback(context.Context, string, string, studyplans.Feedback) (studyplans.FeedbackReceipt, error)
	Generate(context.Context, string, string, studyplans.Generation) (studyplans.View, error)
	StartActivity(context.Context, string, string, int64, int) (studyplans.View, error)
	CompleteActivity(context.Context, string, string, int64, int) (studyplans.View, error)
	Assess(context.Context, string, string, int64, int) (studyplans.View, error)
}

func RegisterStudyPlans(router Router, token string, service StudyPlans) {
	const route = "/v1/learner/goals/{id}/study-plan"
	expected := sha256.Sum256([]byte(token))
	handle := func(pattern string, fn http.HandlerFunc) {
		timed := http.TimeoutHandler(fn, 8*time.Second, `{"code":"SERVICE_UNAVAILABLE","message":"Service unavailable"}`)
		router.Handle(pattern, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "application/json")
			if !authorized(r, token, expected) {
				writeJSON(w, http.StatusForbidden, publicError{"PERMISSION_DENIED", "Permission denied"})
				return
			}
			if service == nil {
				writeJSON(w, http.StatusServiceUnavailable, publicError{"SERVICE_UNAVAILABLE", "Service unavailable"})
				return
			}
			timed.ServeHTTP(w, r)
		}))
	}
	handle("GET "+route, func(w http.ResponseWriter, r *http.Request) {
		view, err := service.Current(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"))
		respondStudyPlan(w, view, err)
	})
	handle("GET "+route+"/explanation", func(w http.ResponseWriter, r *http.Request) {
		query, err := url.ParseQuery(r.URL.RawQuery)
		versions := query["plan_version"]
		if err != nil || len(query) != 1 || len(versions) != 1 {
			respondStudyPlan(w, nil, studyplans.ErrInvalid)
			return
		}
		version, err := strconv.ParseInt(versions[0], 10, 64)
		if err != nil || version < 1 || version > studyplans.MaxVersionsPerGoal || strconv.FormatInt(version, 10) != versions[0] {
			respondStudyPlan(w, nil, studyplans.ErrInvalid)
			return
		}
		value, err := service.Explain(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"), version)
		respondStudyPlan(w, value, err)
	})
	handle("POST "+route+"/feedback", func(w http.ResponseWriter, r *http.Request) {
		keys := r.Header.Values("Idempotency-Key")
		if len(keys) != 1 || !planner.ValidID(keys[0]) {
			respondStudyPlan(w, nil, studyplans.ErrInvalid)
			return
		}
		var input struct {
			PlanVersion   int64                     `json:"plan_version"`
			Action        studyplans.FeedbackAction `json:"action"`
			Ordinal       *int                      `json:"ordinal"`
			WeeklyMinutes *int                      `json:"weekly_minutes"`
		}
		if !decodeOnboarding(w, r, &input) {
			return
		}
		receipt, err := service.SubmitFeedback(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"), studyplans.Feedback{
			RequestID: keys[0], PlanVersion: input.PlanVersion, Action: input.Action, Ordinal: input.Ordinal, WeeklyMinutes: input.WeeklyMinutes,
		})
		if err != nil {
			respondStudyPlan(w, nil, err)
			return
		}
		writeJSON(w, http.StatusAccepted, receipt)
	})
	handle("PUT "+route, func(w http.ResponseWriter, r *http.Request) {
		keys := r.Header.Values("Idempotency-Key")
		if len(keys) != 1 || !planner.ValidID(keys[0]) {
			respondStudyPlan(w, studyplans.View{}, studyplans.ErrInvalid)
			return
		}
		var input struct {
			GoalVersion         int64  `json:"goal_version"`
			AvailabilityVersion int64  `json:"availability_version"`
			PlanVersion         *int64 `json:"plan_version"`
		}
		if !decodeOnboarding(w, r, &input) {
			return
		}
		if input.PlanVersion == nil {
			respondStudyPlan(w, studyplans.View{}, studyplans.ErrInvalid)
			return
		}
		view, err := service.Generate(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"),
			studyplans.Generation{
				RequestID: keys[0],
				Expected: studyplans.Expected{
					GoalVersion: input.GoalVersion, AvailabilityVersion: input.AvailabilityVersion, PlanVersion: *input.PlanVersion,
				},
			})
		respondStudyPlan(w, view, err)
	})
	type progressWrite func(StudyPlans, context.Context, string, string, int64, int) (studyplans.View, error)
	progress := func(write progressWrite) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ordinal, err := strconv.Atoi(r.PathValue("ordinal"))
			if err != nil || ordinal < 1 || ordinal > planner.MaxLearningItems || strconv.Itoa(ordinal) != r.PathValue("ordinal") {
				respondStudyPlan(w, studyplans.View{}, studyplans.ErrInvalid)
				return
			}
			var input struct {
				PlanVersion int64 `json:"plan_version"`
			}
			if !decodeOnboarding(w, r, &input) {
				return
			}
			actor, goal := r.Header.Get("X-User-ID"), r.PathValue("id")
			view, err := write(service, r.Context(), actor, goal, input.PlanVersion, ordinal)
			respondStudyPlan(w, view, err)
		}
	}
	handle("PUT "+route+"/steps/{ordinal}/start", progress(StudyPlans.StartActivity))
	handle("PUT "+route+"/steps/{ordinal}/activity", progress(StudyPlans.CompleteActivity))
	handle("PUT "+route+"/steps/{ordinal}/assessment", progress(StudyPlans.Assess))
}

func respondStudyPlan(w http.ResponseWriter, view any, err error) {
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, view)
	case errors.Is(err, studyplans.ErrInvalid):
		writeJSON(w, http.StatusBadRequest, publicError{"INVALID_ARGUMENT", "Invalid study plan request"})
	case errors.Is(err, studyplans.ErrNotFound):
		writeJSON(w, http.StatusNotFound, publicError{"NOT_FOUND", "Not found"})
	case errors.Is(err, studyplans.ErrConflict):
		writeJSON(w, http.StatusConflict, publicError{"VERSION_CONFLICT", "Plan inputs changed"})
	case errors.Is(err, studyplans.ErrRequestUsed):
		writeJSON(w, http.StatusConflict, publicError{"IDEMPOTENCY_CONFLICT", "Key already used"})
	case errors.Is(err, studyplans.ErrStepBlocked):
		writeJSON(w, http.StatusConflict, publicError{"STEP_BLOCKED", "Step is not actionable"})
	case errors.Is(err, studyplans.ErrResourceUnavailable):
		writeJSON(w, http.StatusConflict, publicError{"RESOURCE_UNAVAILABLE", "Step resource is not available"})
	case errors.Is(err, studyplans.ErrPending):
		writeJSON(w, http.StatusConflict, publicError{"GRADE_PENDING", "Independent grade is not available"})
	case errors.Is(err, studyplans.ErrLimit):
		writeJSON(w, http.StatusConflict, publicError{"PLAN_LIMIT", "Plan version limit reached"})
	case errors.Is(err, studyplans.ErrBudget):
		writeJSON(w, http.StatusConflict, publicError{"BUDGET_EXCEEDED", "Weekly allocations exceed available minutes"})
	case errors.Is(err, studyplans.ErrFeedbackLimit):
		writeJSON(w, http.StatusTooManyRequests, publicError{"WRITE_LIMIT", "Write limit reached"})
	default:
		writeJSON(w, http.StatusServiceUnavailable, publicError{"SERVICE_UNAVAILABLE", "Service unavailable"})
	}
}
