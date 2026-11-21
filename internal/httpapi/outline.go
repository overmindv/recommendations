package httpapi

import (
	"crypto/sha256"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/overmindv/recommendations/internal/aiplan"
)

func outlineHandler(token string, engine *aiplan.Engine, catalog ResourceCatalog) http.Handler {
	expected := sha256.Sum256([]byte(token))
	endpoint := http.TimeoutHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || kind != jsonContentType {
			writeJSON(w, 415, publicError{codeInvalidArgument, "Expected JSON"})

			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 512<<10)
		d := json.NewDecoder(r.Body)
		d.DisallowUnknownFields()
		var in aiplan.OutlineRequest
		var extra any
		if d.Decode(&in) != nil || d.Decode(&extra) != io.EOF {
			writeJSON(w, 400, publicError{codeInvalidArgument, "Invalid input"})

			return
		}
		out, err := engine.Outline(r.Context(), in, r.Header.Get("X-User-ID"))
		if err != nil {
			writeJSON(w, 400, publicError{codeInvalidArgument, "Invalid input"})

			return
		}
		if catalog != nil && len(out.TopicIDs) > 0 {
			selected, err := catalog.OutlineMaterials(r.Context(), out.TopicIDs, in.CompletedMaterialIDs)
			if err == nil && len(selected) > 0 {
				materials := make([]aiplan.Material, 0, len(selected)+len(out.Materials))
				for _, c := range selected {
					materials = append(materials, materialForOutline(c))
				}
				materials = append(materials, out.Materials...)
				out.Materials = materials[:min(8, len(materials))]
			}
		}
		writeJSON(w, 200, out)
	}), 27*time.Second, `{"code":"SERVICE_UNAVAILABLE","message":"Service unavailable"}`)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", noStoreCache)
		w.Header().Set("Content-Type", jsonContentType)
		if !authorized(r, token, expected) {
			writeJSON(w, 403, publicError{"PERMISSION_DENIED", "Permission denied"})

			return
		}
		endpoint.ServeHTTP(w, r)
	})
}
