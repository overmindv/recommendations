package resources

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/overmindv/content/pkg/api/activity"
)

const sourceEventID = "55555555-5555-4555-8555-555555555555"

func TestTaskSourceReadsPinnedSnapshotAndAvailability(t *testing.T) {
	token := strings.Repeat("a", 32)
	event := taskEnvelope{EventID: sourceEventID, EventType: "task.resource.updated", Schema: 1,
		ID: resourceID, Version: 7, Time: time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC),
		Metadata: &taskMetadata{VersionID: revisionID, Title: "An exercise", Type: "single_choice", Level: "easy",
			Topics: []taskTopic{{ID: topicID, Weight: 1}}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("resource credential missing")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch r.URL.Path {
		case "/v1/internal/resources/snapshot":
			if r.URL.Query().Get("limit") != "100" || r.URL.Query().Get("watermark") != "7" {
				t.Error("snapshot bounds not forwarded", r.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"watermark": 7, "resources": []taskEnvelope{event}})
		case "/v1/internal/resources/changes":
			if r.URL.Query().Get("after") != "6" {
				t.Error("wrong replay position", r.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"watermark": 7, "through": 7, "more": false, "changes": []taskEnvelope{event}})
		case "/v1/internal/resources/availability":
			if r.Header.Get("X-User-ID") != "" {
				t.Error("general availability received a learner ID")
			}
			input, _ := io.ReadAll(r.Body)
			if r.Method != http.MethodPost || !strings.Contains(string(input), revisionID) {
				t.Error("resource check did not use the exact revision")
			}
			_ = json.NewEncoder(w).Encode(taskAvailability{Resources: []taskVersionKey{{ID: resourceID, VersionID: revisionID}}})
		default:
			t.Error("unexpected path:", r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := NewTaskClient(server.URL, token)
	if err != nil {
		t.Fatal(err)
	}
	limit := int64(7)
	page, err := client.Snapshot(t.Context(), &limit, "")
	if err != nil || len(page.Records) != 1 || page.Records[0].Metadata == nil || page.Records[0].Metadata.RevisionID != revisionID {
		t.Fatal(page, err)
	}
	changes, err := client.Changes(t.Context(), &limit, 6)
	if err != nil || len(changes.Events) != 1 || changes.Through != 7 || changes.Events[0].ID != sourceEventID {
		t.Fatal(changes, err)
	}
	key := VersionKey{resourceID, revisionID}
	other := VersionKey{topicID, revisionID}
	available, err := client.Available(t.Context(), []VersionKey{key, other})
	if err != nil || !available[key] || available[other] {
		t.Fatal(available, err)
	}
}

func TestTaskSourceChecksDiagnosticHistoryForExactVersions(t *testing.T) {
	key := VersionKey{resourceID, revisionID}
	other := VersionKey{topicID, revisionID}
	token := strings.Repeat("a", 32)
	for _, tc := range []struct {
		name string
		code int
		keys []taskVersionKey
	}{
		{"eligible subset", http.StatusOK, []taskVersionKey{{ID: key.ID, VersionID: key.RevisionID}}},
		{"unknown revision", http.StatusOK, []taskVersionKey{{ID: key.ID, VersionID: sourceEventID}}},
		{"owner unavailable", http.StatusServiceUnavailable, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/internal/resources/diagnostic-eligibility" ||
					r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("X-User-ID") != sourceEventID {
					t.Error("incorrect diagnostic owner request")
				}
				var got taskAvailability
				decoder := json.NewDecoder(r.Body)
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&got); err != nil || len(got.Resources) != 2 ||
					got.Resources[0] != (taskVersionKey{ID: key.ID, VersionID: key.RevisionID}) ||
					got.Resources[1] != (taskVersionKey{ID: other.ID, VersionID: other.RevisionID}) {
					t.Error("incorrect diagnostic versions:", got, err)
				}
				w.WriteHeader(tc.code)
				_ = json.NewEncoder(w).Encode(taskAvailability{Resources: tc.keys})
			}))
			defer server.Close()
			client, err := NewTaskClient(server.URL, token)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.DiagnosticEligible(t.Context(), "", []VersionKey{key}); err == nil {
				t.Fatal("unchecked learner accepted")
			}
			got, err := client.DiagnosticEligible(t.Context(), sourceEventID, []VersionKey{key, other})
			if tc.name == "eligible subset" {
				if err != nil || !got[key] || got[other] {
					t.Fatal("incorrect eligible subset:", got, err)
				}
			} else if err == nil || got != nil {
				t.Fatal("unchecked task marked eligible:", got, err)
			}
		})
	}
}

func TestTaskSourceRejectsInvalidPagesAndForeignAvailability(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"missing", `{"watermark":0}`},
		{"trailing", `{"watermark":0,"resources":[]} {}`},
		{"unknown", `{"watermark":0,"resources":[],"private_payload":"not accepted"}`},
		{"large", `{"watermark":0,"resources":[]}` + strings.Repeat(" ", maxPageBytes)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			client, err := NewTaskClient(server.URL, strings.Repeat("a", 32))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.Snapshot(t.Context(), nil, ""); err == nil {
				t.Fatal("accepted invalid resource page")
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(taskAvailability{Resources: []taskVersionKey{{ID: topicID, VersionID: revisionID}}})
	}))
	defer server.Close()
	client, err := NewTaskClient(server.URL, strings.Repeat("a", 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Available(t.Context(), []VersionKey{{resourceID, revisionID}}); err == nil {
		t.Fatal("availability accepted a foreign resource")
	}
}

func TestTaskSourceDoesNotFollowRedirects(t *testing.T) {
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("redirect was followed")
		w.WriteHeader(http.StatusOK)
	}))
	defer other.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	client, err := NewTaskClient(server.URL, strings.Repeat("a", 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Snapshot(t.Context(), nil, ""); err == nil {
		t.Fatal("redirect was accepted as a valid response")
	}
}

func TestMaterialEventModerationAndVersioning(t *testing.T) {
	wire := activity.Event[activity.MaterialState]{EventID: sourceEventID, EventType: activity.MaterialCorrected,
		SchemaVersion: activity.SchemaVersion, OccurredAt: time.Now(), AggregateID: resourceID, AggregateVersion: 12,
		RevisionID: revisionID, Payload: activity.MaterialState{InvalidatedRevisionID: otherTopic,
			Metadata: &activity.ResourceMetadata{Title: "Note", Type: "note", Format: "markdown", TopicIDs: []string{topicID}}}}
	encode := func() []byte {
		t.Helper()
		raw, err := json.Marshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	got, err := materialEvent([]byte(resourceID), encode())
	if err != nil || got.Record.Metadata == nil || got.Record.Metadata.EstimatedMinutes != nil || got.Record.Version != 12 {
		t.Fatal(got, err)
	}
	if _, err := materialEvent([]byte(topicID), encode()); err == nil {
		t.Fatal("event with wrong partition key accepted")
	}
	wire.EventType = activity.MaterialWithdrawn
	wire.Payload = activity.MaterialState{Tombstone: true, InvalidatedRevisionID: revisionID}
	got, err = materialEvent([]byte(resourceID), encode())
	if err != nil || got.Record.Metadata != nil || got.Record.Version != 12 {
		t.Fatal(got, err)
	}
	for _, kind := range []string{activity.MaterialReadConfirmed, activity.ReviewAccepted, activity.MaterialPublished + ".v2"} {
		wire.EventType = kind
		if _, err := materialEvent([]byte(resourceID), encode()); err == nil {
			t.Fatal("non-catalog event accepted:", kind)
		}
	}
}
