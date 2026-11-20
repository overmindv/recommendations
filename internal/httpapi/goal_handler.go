package httpapi

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/overmindv/recommendations/internal/goals"
)

const MaxGoalBodyBytes int64 = 4096

func RegisterGoals(router Router, token string, store goals.Store) {
	handler := NewGoalHandler(token, store)
	router.Handle("GET /v1/practice-goal", handler)
	router.Handle("PUT /v1/practice-goal", handler)
	router.Handle("DELETE /v1/practice-goal", handler)
}

// NewGoalHandler доверяет только actor из проверенного gateway header.
func NewGoalHandler(token string, store goals.Store) http.Handler {
	expected := sha256.Sum256([]byte(token))
	endpoint := http.TimeoutHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID := r.Header.Get("X-User-ID")
		switch r.Method {
		case http.MethodGet:
			goal, err := store.Get(r.Context(), userID)
			if errors.Is(err, goals.ErrNotFound) {
				writeJSON(w, http.StatusNotFound, publicError{"NOT_FOUND", "Not found"})
			} else if err != nil {
				writeJSON(w, http.StatusServiceUnavailable, publicError{"SERVICE_UNAVAILABLE", "Service unavailable"})
			} else {
				writeJSON(w, http.StatusOK, goal)
			}
		case http.MethodPut:
			mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil || mediaType != jsonContentType {
				writeJSON(w, http.StatusUnsupportedMediaType, publicError{codeInvalidArgument, "Expected application/json"})

				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, MaxGoalBodyBytes)
			decoder := json.NewDecoder(r.Body)
			decoder.DisallowUnknownFields()
			var input goals.Input
			if err = decoder.Decode(&input); err == nil {
				var extra any
				err = decoder.Decode(&extra)
				if errors.Is(err, io.EOF) {
					err = nil
				} else if err == nil {
					err = goals.ErrInvalid
				}
			}
			if err == nil {
				err = goals.Validate(input)
			}
			if err != nil {
				writeJSON(w, http.StatusBadRequest, publicError{codeInvalidArgument, "Invalid practice goal"})

				return
			}
			goal, err := store.Save(r.Context(), userID, input)
			if err != nil {
				writeJSON(w, http.StatusServiceUnavailable, publicError{"SERVICE_UNAVAILABLE", "Service unavailable"})

				return
			}
			writeJSON(w, http.StatusOK, goal)
		case http.MethodDelete:
			if err := store.Delete(r.Context(), userID); err != nil {
				writeJSON(w, http.StatusServiceUnavailable, publicError{"SERVICE_UNAVAILABLE", "Service unavailable"})

				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			writeJSON(w, http.StatusMethodNotAllowed, publicError{codeInvalidArgument, "Method not allowed"})
		}
	}), 3*time.Second, `{"code":"SERVICE_UNAVAILABLE","message":"Service unavailable"}`)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		endpoint.ServeHTTP(w, r)
	})
}
