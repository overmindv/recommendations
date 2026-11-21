package topiccatalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func topicIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1)
	}

	return ids
}

func activeTopic(w http.ResponseWriter, r *http.Request) {
	_ = json.NewEncoder(w).Encode(map[string]string{
		"id": strings.TrimPrefix(r.URL.Path, "/v1/topics/"), "status": "active",
	})
}

func TestCatalogRequiresPublishedTopic(t *testing.T) {
	t.Parallel()
	const published = "published"
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   error
	}{
		{published, 200, `{"id":"$id","status":"active"}`, nil},
		{"hidden", 200, `{"id":"$id","status":"hidden"}`, ErrTopicUnavailable},
		{"not found", 404, "", ErrTopicUnavailable},
		{"unavailable", 503, "internal error", nil},
		{"wrong id", 200, `{"id":"wrong","status":"active"}`, nil},
		{"oversize", 200, strings.Repeat(" ", maxTopicResponse) + `{"id":"$id","status":"active"}`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			id := topicIDs(1)[0]
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/topics/"+id || r.Method != http.MethodGet ||
					r.Header.Get("Authorization") != "" || r.Header.Get("X-User-ID") != "" {
					t.Error("unexpected catalog request")
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(strings.ReplaceAll(tc.body, "$id", id)))
			}))
			defer server.Close()
			client, err := New(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			err = client.Validate(t.Context(), []string{id})
			switch {
			case tc.name == published && err != nil:
				t.Fatal(err)
			case tc.want != nil && !errors.Is(err, tc.want):
				t.Fatal("wrong error", err)
			case tc.want == nil && tc.name != published && (err == nil || errors.Is(err, ErrTopicUnavailable)):
				t.Fatal("dependency error treated as a topic result", err)
			}
		})
	}
}

func TestCatalogDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	var redirected atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected.Store(true)
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	client, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Validate(t.Context(), topicIDs(1)); err == nil || redirected.Load() {
		t.Fatal("redirect was accepted", err)
	}
	for _, address := range []string{"//example.test", "http://user@example.test", "https://example.test/?q=1", "file:///etc/hosts"} {
		if _, err := New(address); err == nil {
			t.Fatal("invalid catalog endpoint accepted", address)
		}
	}
}

func TestCatalogCapsParallelRequests(t *testing.T) {
	t.Parallel()
	var active, peak atomic.Int32
	started := make(chan struct{}, 16)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		for previous := peak.Load(); n > previous && !peak.CompareAndSwap(previous, n); previous = peak.Load() {
		}
		defer active.Add(-1)
		started <- struct{}{}
		select {
		case <-release:
			activeTopic(w, r)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	client, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Validate(ctx, topicIDs(16)) }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for range maxParallelTopics {
		select {
		case <-started:
		case <-deadline.C:
			t.Fatal("catalog calls did not start in parallel")
		}
	}
	close(release)
	if err := <-done; err != nil || peak.Load() != maxParallelTopics {
		t.Fatal("parallel catalog limit not held", peak.Load(), err)
	}
}

func BenchmarkCatalogValidate16(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(activeTopic))
	defer server.Close()
	client, err := New(server.URL)
	if err != nil {
		b.Fatal(err)
	}
	ids := topicIDs(16)
	b.ReportAllocs()
	for b.Loop() {
		if err := client.Validate(b.Context(), ids); err != nil {
			b.Fatal(err)
		}
	}
}
