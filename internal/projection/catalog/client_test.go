package catalog

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestClientDoesNotFollowRedirects(t *testing.T) {
	token := strings.Repeat("x", 32)
	var reached atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer other.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusFound)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, token)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Snapshot(t.Context(), nil, ""); err == nil || reached.Load() || strings.Contains(err.Error(), token) {
		t.Fatal("unexpected behavior after redirect")
	}
}

func TestClientRejectsInvalidResponses(t *testing.T) {
	for name, body := range map[string]string{
		"oversize":  strings.Repeat(" ", maxPageBytes) + `{"watermark":0,"records":[]}`,
		"trailing":  `{"watermark":0,"records":[]} {}`,
		"truncated": `{"watermark":0`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			client, err := NewClient(server.URL, strings.Repeat("x", 32))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.Snapshot(t.Context(), nil, ""); err == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}

func TestClientRequiresPlainBaseAddressAndToken(t *testing.T) {
	for _, address := range []string{"", "ftp://127.0.0.1", "http://name:pass@localhost", "https://localhost/?q=x", "http://localhost/#fragment"} {
		if _, err := NewClient(address, strings.Repeat("x", 32)); err == nil {
			t.Fatal("invalid base address accepted")
		}
	}
	for _, token := range []string{"", "short", strings.Repeat("x", 513), strings.Repeat("x", 32) + "\n"} {
		if _, err := NewClient("http://localhost", token); err == nil {
			t.Fatal("invalid token accepted")
		}
	}
}
