package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/overmindv/recommendations/internal/goals"
)

type memoryGoals struct {
	mu    sync.Mutex
	items map[string]goals.Goal
}

func (m *memoryGoals) Get(_ context.Context, userID string) (goals.Goal, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	goal, ok := m.items[userID]
	if !ok {
		return goals.Goal{}, goals.ErrNotFound
	}
	return goal, nil
}

func (m *memoryGoals) Save(_ context.Context, userID string, input goals.Input) (goals.Goal, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	goal := m.items[userID]
	goal.TopicIDs = append([]string{}, input.TopicIDs...)
	goal.Minutes = input.Minutes
	goal.Revision++
	goal.UpdatedAt = time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	m.items[userID] = goal
	return goal, nil
}

func (m *memoryGoals) Delete(_ context.Context, userID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, userID)
	return nil
}

func goalRequest(method, body, userID string) *http.Request {
	r := httptest.NewRequest(method, "/v1/practice-goal", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-User-ID", userID)
	if method == http.MethodPut {
		r.Header.Set("Content-Type", "application/json")
	}
	return r
}

func TestGoalLifecycleAndActorIsolation(t *testing.T) {
	store := &memoryGoals{items: map[string]goals.Goal{}}
	h := NewGoalHandler(token, store)
	other := "00000000-0000-0000-0000-000000000002"
	request := func(method, body, userID string) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, goalRequest(method, body, userID))
		return w
	}
	if w := request(http.MethodGet, "", actor); w.Code != 404 {
		t.Fatalf("missing goal: %d %s", w.Code, w.Body)
	}
	body := `{"topic_ids":["00000000-0000-0000-0000-000000000003"],"minutes":60}`
	for wantRevision := int64(1); wantRevision <= 2; wantRevision++ {
		w := request(http.MethodPut, body, actor)
		var goal goals.Goal
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &goal) != nil || goal.Revision != wantRevision || goal.Minutes != 60 {
			t.Fatalf("save goal: %d %s", w.Code, w.Body)
		}
	}
	if w := request(http.MethodGet, "", other); w.Code != 404 {
		t.Fatalf("other actor saw goal: %d %s", w.Code, w.Body)
	}
	if w := request(http.MethodGet, "", actor); w.Code != 200 || !strings.Contains(w.Body.String(), `"revision":2`) {
		t.Fatalf("goal not reloaded: %d %s", w.Code, w.Body)
	}
	for i := 0; i < 2; i++ {
		if w := request(http.MethodDelete, "", actor); w.Code != 204 {
			t.Fatalf("delete goal: %d %s", w.Code, w.Body)
		}
	}
	if w := request(http.MethodGet, "", actor); w.Code != 404 {
		t.Fatalf("deleted goal remained: %d %s", w.Code, w.Body)
	}
}

func TestGoalInputAndAuthentication(t *testing.T) {
	h := NewGoalHandler(token, &memoryGoals{items: map[string]goals.Goal{}})
	for name, body := range map[string]string{
		"null":            "null",
		"unknown field":   `{"topic_ids":["00000000-0000-0000-0000-000000000003"],"minutes":60,"user_id":"secret"}`,
		"duplicate topic": `{"topic_ids":["00000000-0000-0000-0000-000000000003","00000000-0000-0000-0000-000000000003"],"minutes":60}`,
		"trailing JSON":   `{"topic_ids":["00000000-0000-0000-0000-000000000003"],"minutes":60}{}`,
	} {
		t.Run(name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, goalRequest(http.MethodPut, body, actor))
			if w.Code != 400 || strings.Contains(w.Body.String(), "secret") {
				t.Fatalf("invalid body accepted or leaked: %d %s", w.Code, w.Body)
			}
		})
	}
	for name, mutate := range map[string]func(*http.Request){
		"missing actor":   func(r *http.Request) { r.Header.Del("X-User-ID") },
		"duplicate actor": func(r *http.Request) { r.Header.Add("X-User-ID", actor) },
		"wrong token":     func(r *http.Request) { r.Header.Set("Authorization", "Bearer wrong") },
	} {
		t.Run(name, func(t *testing.T) {
			r := goalRequest(http.MethodGet, "", actor)
			mutate(r)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 403 {
				t.Fatalf("unauthorized request accepted: %d %s", w.Code, w.Body)
			}
		})
	}
}

type failingGoals struct{ memoryGoals }

func (f *failingGoals) Get(context.Context, string) (goals.Goal, error) {
	return goals.Goal{}, errors.New("private database secret")
}

func TestGoalDatabaseErrorIsGeneric(t *testing.T) {
	w := httptest.NewRecorder()
	NewGoalHandler(token, &failingGoals{}).ServeHTTP(w, goalRequest(http.MethodGet, "", actor))
	if w.Code != 503 || strings.Contains(w.Body.String(), "secret") {
		t.Fatalf("database error leaked: %d %s", w.Code, w.Body)
	}
}

func TestGoalWithoutDatabaseIsUnavailable(t *testing.T) {
	w := httptest.NewRecorder()
	NewGoalHandler(token, nil).ServeHTTP(w, goalRequest(http.MethodGet, "", actor))
	if w.Code != 503 || !strings.Contains(w.Body.String(), "SERVICE_UNAVAILABLE") {
		t.Fatalf("missing database was treated as empty goal: %d %s", w.Code, w.Body)
	}
}
