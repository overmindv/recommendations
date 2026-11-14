package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/savedplans"
)

func RegisterSavedPlans(router Router, token string, store savedplans.Store) {
	handle := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		want := sha256.Sum256([]byte(token))
		got := sha256.Sum256([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")))
		if token == "" || len(r.Header.Values("Authorization")) != 1 || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
			writeJSON(w, 403, publicError{"PERMISSION_DENIED", "Permission denied"})
			return
		}
		actor := r.Header.Get("X-User-ID")
		id := r.PathValue("id")
		op := r.PathValue("op")
		if len(r.Header.Values("X-User-ID")) > 1 || (actor != "" && !planner.ValidID(actor)) || (id != "" && !planner.ValidID(id)) {
			writeJSON(w, 400, publicError{"INVALID_ARGUMENT", "Invalid input"})
			return
		}
		if actor == "" && (r.Method != "GET" || id == "") {
			writeJSON(w, 403, publicError{"PERMISSION_DENIED", "Permission denied"})
			return
		}
		if store == nil {
			writeJSON(w, 503, publicError{"SERVICE_UNAVAILABLE", "Service unavailable"})
			return
		}
		var result any
		var err error
		if r.Method == "GET" {
			if id == "" {
				result, err = store.List(r.Context(), actor)
			} else {
				result, err = store.Get(r.Context(), id, actor)
			}
		} else {
			r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
			d := json.NewDecoder(r.Body)
			d.DisallowUnknownFields()
			var in savedplans.Input
			var extra any
			if d.Decode(&in) != nil || d.Decode(&extra) != io.EOF {
				writeJSON(w, 400, publicError{"INVALID_ARGUMENT", "Invalid input"})
				return
			}
			if id == "" || op == "clone" {
				if !planner.ValidID(in.ID) {
					writeJSON(w, 400, publicError{"INVALID_ARGUMENT", "Invalid input"})
					return
				}
			}
			if id == "" {
				result, err = store.Create(r.Context(), actor, in)
			} else if op == "clone" {
				result, err = store.Clone(r.Context(), id, actor, in.ID)
			} else {
				result, err = store.Change(r.Context(), id, actor, op, in)
			}
		}
		if err != nil {
			code, status := "SERVICE_UNAVAILABLE", 503
			switch {
			case errors.Is(err, savedplans.ErrNotFound):
				code, status = "NOT_FOUND", 404
			case errors.Is(err, savedplans.ErrConflict):
				code, status = "CONFLICT", 409
			case errors.Is(err, savedplans.ErrInvalid):
				code, status = "INVALID_ARGUMENT", 400
			case errors.Is(err, savedplans.ErrLimit):
				code, status = "LIMIT_REACHED", 429
			}
			writeJSON(w, status, publicError{code, "Unable to update plan"})
			return
		}
		writeJSON(w, 200, result)
	}
	router.Handle("GET /v1/learning-plans", http.HandlerFunc(handle))
	router.Handle("GET /v1/learning-plans/{id}", http.HandlerFunc(handle))
	router.Handle("POST /v1/learning-plans", http.HandlerFunc(handle))
	router.Handle("POST /v1/learning-plans/{id}/{op}", http.HandlerFunc(handle))
}
