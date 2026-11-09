package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/overmindv/recommendations/internal/planner"
)

const actor = "00000000-0000-0000-0000-000000000001"
const token = "test-service-token-with-at-least-32-bytes"
const validBody = `{"topic_ids":["00000000-0000-0000-0000-000000000001"],"minutes":60,"topics":[{"id":"00000000-0000-0000-0000-000000000001","prerequisite_ids":[]}],"tasks":[{"id":"00000000-0000-0000-0000-000000000002","task_version_id":"00000000-0000-0000-0000-000000000003","topic_id":"00000000-0000-0000-0000-000000000001","title":"First task","difficulty":"easy","task_type":"single_choice"}],"evidence":[],"input_truncated":false}`

func request(body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/practice-plan", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-User-ID", actor)
	r.Header.Set("Content-Type", "application/json")
	return r
}

func TestHTTPContract(t *testing.T) {
	mux := http.NewServeMux()
	Register(mux, token)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, request(validBody))
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("unexpected headers: %v", w.Header())
	}
	var got planner.Plan
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.PolicyVersion != "practice-v2" || got.TotalMinutes != 10 || len(got.Items) != 1 || got.Items[0].TaskID != "00000000-0000-0000-0000-000000000002" || got.Items[0].TaskVersionID != "00000000-0000-0000-0000-000000000003" || got.Items[0].ReasonCode != "START_TOPIC" || got.Warnings == nil {
		t.Fatalf("contract mismatch: %s", w.Body)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &wire); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"policy_version", "total_minutes", "warnings", "items"} {
		if _, ok := wire[key]; !ok {
			t.Fatalf("missing field %s", key)
		}
	}
	if len(wire) != 4 {
		t.Fatalf("unexpected response fields: %s", w.Body)
	}
	get := httptest.NewRequest(http.MethodGet, "/v1/practice-plan", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, get)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET accepted: %d", w.Code)
	}
}

func TestAuthentication(t *testing.T) {
	cases := map[string]func(*http.Request){
		"missing token":   func(r *http.Request) { r.Header.Del("Authorization") },
		"wrong token":     func(r *http.Request) { r.Header.Set("Authorization", "Bearer wrong") },
		"missing bearer":  func(r *http.Request) { r.Header.Set("Authorization", token) },
		"duplicate token": func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+token) },
		"missing actor":   func(r *http.Request) { r.Header.Del("X-User-ID") },
		"invalid actor":   func(r *http.Request) { r.Header.Set("X-User-ID", "attacker") },
		"nil actor":       func(r *http.Request) { r.Header.Set("X-User-ID", "00000000-0000-0000-0000-000000000000") },
		"duplicate actor": func(r *http.Request) { r.Header.Add("X-User-ID", actor) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := request("secret SQL password")
			mutate(r)
			w := httptest.NewRecorder()
			NewHandler(token).ServeHTTP(w, r)
			if w.Code != 403 || strings.Contains(w.Body.String(), "secret") {
				t.Fatalf("authentication leaked or failed: %d %s", w.Code, w.Body)
			}
			var got publicError
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.Code != "PERMISSION_DENIED" {
				t.Fatalf("unexpected error: %s", w.Body)
			}
		})
	}
	t.Run("empty configured token", func(t *testing.T) {
		w := httptest.NewRecorder()
		NewHandler("").ServeHTTP(w, request(validBody))
		if w.Code != 403 {
			t.Fatalf("empty configuration accepted: %d", w.Code)
		}
	})
}

func TestStrictInputAndSafeErrors(t *testing.T) {
	cases := map[string]struct {
		body   string
		status int
	}{
		"malformed":            {`{"secret":"password"`, 400},
		"trailing JSON":        {validBody + `{}`, 400},
		"trailing junk":        {validBody + `password`, 400},
		"unknown field":        {strings.Replace(validBody, `"minutes":60`, `"minutes":60,"user_id":"secret"`, 1), 400},
		"nested unknown field": {strings.Replace(validBody, `"title":"First task"`, `"title":"First task","solution":"secret"`, 1), 400},
		"null":                 {`null`, 400},
		"invalid budget":       {strings.Replace(validBody, `"minutes":60`, `"minutes":0`, 1), 400},
		"empty body":           {"", 400},
		"oversized":            {validBody + strings.Repeat(" ", int(MaxBodyBytes)), 413},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			w := httptest.NewRecorder()
			NewHandler(token).ServeHTTP(w, request(tc.body))
			if w.Code != tc.status {
				t.Fatalf("want %d got %d: %s", tc.status, w.Code, w.Body)
			}
			if strings.Contains(w.Body.String(), "secret") || strings.Contains(w.Body.String(), "password") {
				t.Fatalf("unsafe error: %s", w.Body)
			}
			var got publicError
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.Code != "INVALID_ARGUMENT" {
				t.Fatalf("unexpected error: %s", w.Body)
			}
		})
	}
	r := request(validBody)
	r.Header.Set("Content-Type", "text/plain")
	w := httptest.NewRecorder()
	NewHandler(token).ServeHTTP(w, r)
	if w.Code != 415 {
		t.Fatalf("wrong content type accepted: %d", w.Code)
	}
}

func TestConcurrentActorIsolation(t *testing.T) {
	h := NewHandler(token)
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(solved bool) {
			defer wg.Done()
			body := validBody
			if solved {
				body = strings.Replace(body, `"evidence":[]`, `"evidence":[{"task_id":"00000000-0000-0000-0000-000000000002","task_version_id":"00000000-0000-0000-0000-000000000003","verdict":"accepted","occurred_at":"2026-11-01T12:00:00Z"}]`, 1)
			}
			r := request(body)
			if solved {
				r.Header.Set("X-User-ID", "00000000-0000-0000-0000-000000000009")
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			var plan planner.Plan
			if err := json.NewDecoder(bytes.NewReader(w.Body.Bytes())).Decode(&plan); err != nil {
				t.Error(err)
				return
			}
			if w.Code != 200 || (len(plan.Items) == 0) != solved {
				t.Errorf("request state crossed actors: %d %s", w.Code, w.Body)
			}
		}(i%2 == 0)
	}
	wg.Wait()
}
