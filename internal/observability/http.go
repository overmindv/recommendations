package observability

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const StudyMetricsPath = "/internal/metrics/study"

func (m *Study) Handler(token string) http.Handler {
	expected := sha256.Sum256([]byte(token))
	scrape := promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{MaxRequestsInFlight: 2})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		values := r.Header.Values("Authorization")
		if len(token) < 32 || len(values) != 1 {
			http.Error(w, "Forbidden", http.StatusForbidden)

			return
		}
		provided, ok := strings.CutPrefix(values[0], "Bearer ")
		if len(provided) != len(token) || !ok {
			http.Error(w, "Forbidden", http.StatusForbidden)

			return
		}
		digest := sha256.Sum256([]byte(provided))
		if subtle.ConstantTimeCompare(expected[:], digest[:]) != 1 {
			http.Error(w, "Forbidden", http.StatusForbidden)

			return
		}
		scrape.ServeHTTP(w, r)
	})
}
