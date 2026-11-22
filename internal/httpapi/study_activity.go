package httpapi

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"time"

	"github.com/overmindv/recommendations/internal/studyactivity"
)

type ActivityStore interface {
	Capture(context.Context, string, studyactivity.Input) (studyactivity.Receipt, error)
}

func RegisterStudyActivity(router Router, token string, store ActivityStore) {
	expected := sha256.Sum256([]byte(token))
	endpoint := http.TimeoutHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input studyactivity.Input
		if !decodeOnboarding(w, r, &input) {
			return
		}
		result, err := store.Capture(r.Context(), r.Header.Get("X-User-ID"), input)
		if err != nil {
			writeActivityError(w, err)

			return
		}
		writeJSON(w, http.StatusOK, result)
	}), 4*time.Second, `{"code":"SERVICE_UNAVAILABLE","message":"Service unavailable"}`)
	router.Handle("POST /v1/learner/study-activity", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", noStoreCache)
		w.Header().Set("Content-Type", jsonContentType)
		if !authorized(r, token, expected) {
			writeJSON(w, http.StatusForbidden, publicError{"PERMISSION_DENIED", "Permission denied"})

			return
		}
		if store == nil {
			writeActivityError(w, errors.New("activity disabled"))

			return
		}
		endpoint.ServeHTTP(w, r)
	}))
}

func writeActivityError(w http.ResponseWriter, err error) {
	status, response := http.StatusServiceUnavailable, publicError{"SERVICE_UNAVAILABLE", "Service unavailable"}
	switch {
	case errors.Is(err, studyactivity.ErrInvalid):
		status, response = http.StatusBadRequest, publicError{codeInvalidArgument, "Invalid study activity"}
	case errors.Is(err, studyactivity.ErrConflict):
		status, response = http.StatusConflict, publicError{"IDEMPOTENCY_CONFLICT", "Key already used"}
	case errors.Is(err, studyactivity.ErrRate):
		w.Header().Set("Retry-After", "5")
		status, response = http.StatusTooManyRequests, publicError{"WRITE_LIMIT", "Write limit reached"}
	case errors.Is(err, studyactivity.ErrResource):
		status, response = http.StatusUnprocessableEntity, publicError{"RESOURCE_UNAVAILABLE", "Resource unavailable"}
	}
	writeJSON(w, status, response)
}
