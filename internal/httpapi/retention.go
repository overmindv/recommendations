package httpapi

import (
	"crypto/sha256"
	"errors"
	"net/http"
	"time"

	"github.com/overmindv/recommendations/internal/mastery"
)

func RegisterRetention(router Router, token string, service *mastery.Retention) {
	const route = "/v1/learner/goals/{id}/retention"
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
		plan, err := service.Get(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"))
		respondRetention(w, plan, err)
	})
	handle("PUT "+route+"/{topic}", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			GoalVersion int64     `json:"goal_version"`
			DueAt       time.Time `json:"due_at"`
		}
		if !decodeOnboarding(w, r, &input) {
			return
		}
		review, err := service.Assign(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"), r.PathValue("topic"),
			input.GoalVersion, input.DueAt)
		respondRetention(w, review, err)
	})
}

func respondRetention(w http.ResponseWriter, result any, err error) {
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, result)
	case errors.Is(err, mastery.ErrReviewNotDue):
		writeJSON(w, http.StatusConflict, publicError{"REVIEW_NOT_DUE", "Review is not due"})
	case errors.Is(err, mastery.ErrReviewCatalog):
		writeJSON(w, http.StatusServiceUnavailable, publicError{"SERVICE_UNAVAILABLE", "Service unavailable"})
	default:
		writeOnboardingError(w, err)
	}
}
