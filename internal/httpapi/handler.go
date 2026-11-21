package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/overmindv/recommendations/internal/aiplan"
	"github.com/overmindv/recommendations/internal/planner"
)

const (
	jsonContentType     = "application/json"
	cloneOperation      = "clone"
	codeInvalidArgument = "INVALID_ARGUMENT"
	noStoreCache        = "no-store"
)

const MaxBodyBytes int64 = 4 << 20

type Router interface{ Handle(string, http.Handler) }

type publicError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func Register(router Router, token string) { RegisterAI(router, token, aiplan.New()) }
func RegisterAI(router Router, token string, engine *aiplan.Engine) {
	RegisterVerifiedAI(router, token, engine, nil)
}

func RegisterVerifiedAI(router Router, token string, engine *aiplan.Engine, catalog ResourceCatalog) {
	router.Handle("POST /v1/practice-plan", newHandler(token, engine, catalog))
	router.Handle("POST /v1/learning-outline", outlineHandler(token, engine, catalog))
}

// NewHandler принимает только доверенный actor из аутентифицированного gateway.
func NewHandler(token string) http.Handler { return newHandler(token, aiplan.New(), nil) }
func newHandler(token string, engine *aiplan.Engine, catalog ResourceCatalog) http.Handler {
	expected := sha256.Sum256([]byte(token))
	endpoint := http.TimeoutHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { preview(w, r, engine, catalog) }), 27*time.Second, `{"code":"SERVICE_UNAVAILABLE","message":"Service unavailable"}`)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", noStoreCache)
		w.Header().Set("Content-Type", jsonContentType)
		if !authorized(r, token, expected) {
			writeJSON(w, http.StatusForbidden, publicError{"PERMISSION_DENIED", "Permission denied"})

			return
		}
		endpoint.ServeHTTP(w, r)
	})
}

func authorized(r *http.Request, token string, expected [32]byte) bool {
	auth := r.Header.Get("Authorization")
	provided := sha256.Sum256([]byte(strings.TrimPrefix(auth, "Bearer ")))

	return token != "" && len(r.Header.Values("Authorization")) == 1 && strings.HasPrefix(auth, "Bearer ") &&
		subtle.ConstantTimeCompare(expected[:], provided[:]) == 1 && len(r.Header.Values("X-User-ID")) == 1 &&
		planner.ValidID(r.Header.Get("X-User-ID"))
}

func preview(w http.ResponseWriter, r *http.Request, engine *aiplan.Engine, catalog ResourceCatalog) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != jsonContentType {
		writeJSON(w, http.StatusUnsupportedMediaType, publicError{codeInvalidArgument, "Expected application/json"})

		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input planner.Request
	if err = decoder.Decode(&input); err == nil {
		var extra any
		err = decoder.Decode(&extra)
		if errors.Is(err, io.EOF) {
			err = nil
		} else if err == nil {
			err = planner.ErrInvalidInput
		}
	}
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, publicError{codeInvalidArgument, "Request too large"})
		} else {
			writeJSON(w, http.StatusBadRequest, publicError{codeInvalidArgument, "Invalid practice plan request"})
		}

		return
	}
	if catalog != nil {
		if validationErr := planner.Validate(input); validationErr != nil {
			writeJSON(w, http.StatusBadRequest, publicError{codeInvalidArgument, "Invalid practice plan request"})

			return
		}
		input.Tasks, err = catalog.PracticeTasks(r.Context(), input.Tasks)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, publicError{"SERVICE_UNAVAILABLE", "Service unavailable"})

			return
		}
	}
	plan, err := engine.Build(r.Context(), input, r.Header.Get("X-User-ID"))
	if errors.Is(err, planner.ErrInvalidInput) {
		writeJSON(w, http.StatusBadRequest, publicError{codeInvalidArgument, "Invalid practice plan request"})

		return
	}
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, publicError{"SERVICE_UNAVAILABLE", "Service unavailable"})

		return
	}
	if catalog != nil {
		if err := confirmPlan(r.Context(), catalog, plan); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, publicError{"SERVICE_UNAVAILABLE", "Service unavailable"})

			return
		}
		plan.PolicyVersion = resourcePolicy
	}
	writeJSON(w, http.StatusOK, plan)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", jsonContentType)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
