package httpapi

import (
	"crypto/sha256"
	"errors"
	"net/http"
	"time"

	"github.com/overmindv/recommendations/internal/diagnostic"
)

func RegisterDiagnostic(router Router, token string, service *diagnostic.Service) {
	const route = "/v1/learner/goals/{id}/diagnostic"
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
		session, err := service.Get(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"))
		respondDiagnostic(w, session, err)
	})
	handle("PUT "+route, func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			GoalVersion   int64 `json:"goal_version"`
			BudgetMinutes int   `json:"budget_minutes"`
		}
		if !decodeOnboarding(w, r, &input) {
			return
		}
		session, err := service.Start(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"), input.GoalVersion, input.BudgetMinutes)
		respondDiagnostic(w, session, err)
	})
	handle("PUT "+route+"/skip", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			GoalVersion int64 `json:"goal_version"`
		}
		if !decodeOnboarding(w, r, &input) {
			return
		}
		session, err := service.Skip(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"), input.GoalVersion)
		respondDiagnostic(w, session, err)
	})
	handle("PUT "+route+"/finalize", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			GoalVersion  int64  `json:"goal_version"`
			SessionID    string `json:"session_id"`
			AllowPartial bool   `json:"allow_partial"`
		}
		if !decodeOnboarding(w, r, &input) {
			return
		}
		session, err := service.Finalize(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"),
			input.SessionID, input.GoalVersion, input.AllowPartial)
		respondDiagnostic(w, session, err)
	})
}

func respondDiagnostic(w http.ResponseWriter, session diagnostic.Session, err error) {
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, session)
	case errors.Is(err, diagnostic.ErrNotStarted):
		writeJSON(w, http.StatusNotFound, publicError{"DIAGNOSTIC_NOT_STARTED", "Diagnostic not started"})
	case errors.Is(err, diagnostic.ErrTopicUnavailable):
		writeJSON(w, http.StatusUnprocessableEntity, publicError{"UNKNOWN_TOPIC", "A target topic is unavailable"})
	case errors.Is(err, diagnostic.ErrBudget):
		writeJSON(w, http.StatusUnprocessableEntity, publicError{"DIAGNOSTIC_BUDGET", "Diagnostic exceeds goal time"})
	case errors.Is(err, diagnostic.ErrTaskUnavailable):
		writeJSON(w, http.StatusConflict, publicError{"DIAGNOSTIC_TASK_UNAVAILABLE", "A diagnostic task is unavailable"})
	case errors.Is(err, diagnostic.ErrSessionChanged):
		writeJSON(w, http.StatusConflict, publicError{"DIAGNOSTIC_SESSION_CHANGED", "Diagnostic session changed"})
	case errors.Is(err, diagnostic.ErrInvalidSelection):
		writeJSON(w, http.StatusBadRequest, publicError{"INVALID_ARGUMENT", "Invalid diagnostic request"})
	default:
		writeOnboardingError(w, err)
	}
}
