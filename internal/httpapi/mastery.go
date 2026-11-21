package httpapi

import (
	"crypto/sha256"
	"net/http"
	"time"

	"github.com/overmindv/recommendations/internal/mastery"
)

func RegisterMastery(router Router, token string, store *mastery.Postgres) {
	const route = "/v1/learner/goals/{id}/mastery"
	expected := sha256.Sum256([]byte(token))
	handle := func(pattern string, fn http.HandlerFunc) {
		timed := http.TimeoutHandler(fn, 4*time.Second, `{"code":"SERVICE_UNAVAILABLE","message":"Service unavailable"}`)
		router.Handle(pattern, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", noStoreCache)
			w.Header().Set("Content-Type", jsonContentType)
			if !authorized(r, token, expected) {
				writeJSON(w, http.StatusForbidden, publicError{"PERMISSION_DENIED", "Permission denied"})

				return
			}
			if store == nil {
				writeJSON(w, http.StatusServiceUnavailable, publicError{"SERVICE_UNAVAILABLE", "Service unavailable"})

				return
			}
			timed.ServeHTTP(w, r)
		}))
	}
	handle("GET "+route, func(w http.ResponseWriter, r *http.Request) {
		result, err := store.Get(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"))
		respondMastery(w, result, err)
	})
	handle("PUT "+route, func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			GoalVersion int64 `json:"goal_version"`
		}
		if !decodeOnboarding(w, r, &input) {
			return
		}
		result, err := store.Refresh(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"), input.GoalVersion)
		respondMastery(w, result, err)
	})
}

func respondMastery(w http.ResponseWriter, result mastery.Snapshot, err error) {
	if err != nil {
		writeOnboardingError(w, err)

		return
	}
	writeJSON(w, http.StatusOK, result)
}
