package replay

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestReplayHTTPContract(t *testing.T) {
	t.Parallel()
	token := strings.Repeat("r", 32)
	source := sourceFor(t, eventFixture(t, uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), 1, "accepted"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("missing dedicated token")
		}
		if r.URL.Path == "/v1/internal/outcomes/manifest" {
			_ = json.NewEncoder(w).Encode(source.manifest)

			return
		}
		if r.URL.Query().Get("privacy_epoch") != "1" || r.URL.Query().Get("watermark") != "1" || r.URL.Query().Get("limit") != "100" {
			t.Error("missing snapshot fence")
		}
		page, _ := source.Page(r.Context(), source.manifest, 0)
		_ = json.NewEncoder(w).Encode(page)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, token)
	if err != nil {
		t.Fatal(err)
	}
	m, err := client.Manifest(t.Context(), 0, 0)
	if err != nil || !sameManifest(m, source.manifest) {
		t.Fatal(m, err)
	}
	page, err := client.Page(t.Context(), m, 0)
	if err != nil || len(page.Events) != 1 || page.Events[0].SHA256 != source.events[0].SHA256 {
		t.Fatal(page, err)
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, server.URL, http.StatusFound) }))
	defer redirect.Close()
	redirected, err := NewClient(redirect.URL, token)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = redirected.Manifest(t.Context(), 0, 0); err == nil {
		t.Fatal("redirect followed")
	}
}

func TestManifestRefusesLegacyGaps(t *testing.T) {
	t.Parallel()
	m := sourceFor(t).manifest
	m.LegacyMissing = 1
	if m.Valid() {
		t.Fatal("legacy gap accepted")
	}
	if _, err := NewClient("http://tasks:8080", strings.Repeat("r", 32)); err == nil {
		t.Fatal("remote cleartext accepted")
	}
}
