package resources

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/overmindv/recommendations/internal/planner"
)

const maxPageBytes = 2 << 20

type TaskClient struct {
	base  *url.URL
	token string
	http  *http.Client
}

func (c *TaskClient) CloseIdleConnections() { c.http.CloseIdleConnections() }

func NewTaskClient(address, token string) (*TaskClient, error) {
	base, err := url.Parse(address)
	if err != nil || base.Hostname() == "" || (base.Scheme != "http" && base.Scheme != "https") ||
		base.User != nil || base.Opaque != "" || base.RawQuery != "" || base.ForceQuery || base.Fragment != "" {
		return nil, errors.New("invalid TASKS_RESOURCE_URL")
	}
	if !validToken(token, 512) {
		return nil, errors.New("invalid TASKS_RESOURCE_TOKEN")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxConnsPerHost = 8
	transport.MaxIdleConnsPerHost = 8
	transport.MaxResponseHeaderBytes = 16 << 10
	return &TaskClient{
		base: base.JoinPath("v1", "internal", "resources"), token: token,
		http: &http.Client{
			Transport: transport, Timeout: 6 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

type taskEnvelope struct {
	EventID   string        `json:"event_id"`
	EventType string        `json:"event_type"`
	Schema    int           `json:"schema_version"`
	ID        string        `json:"aggregate_id"`
	Version   int64         `json:"aggregate_version"`
	Time      time.Time     `json:"occurred_at"`
	Removed   bool          `json:"tombstone"`
	Metadata  *taskMetadata `json:"metadata,omitempty"`
}

type taskMetadata struct {
	VersionID string        `json:"task_version_id"`
	Title     string        `json:"title"`
	Type      string        `json:"task_type"`
	Level     string        `json:"difficulty"`
	Minutes   *int          `json:"estimated_minutes,omitempty"`
	Topics    []taskTopic   `json:"topic_weights"`
	Languages []taskRuntime `json:"languages"`
}

type taskTopic struct {
	ID     string `json:"topic_id"`
	Weight int    `json:"weight"`
}

type taskRuntime struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

func (e taskEnvelope) record() (Record, error) {
	if !planner.ValidID(e.EventID) || e.Schema != 1 || e.Time.IsZero() ||
		(e.Removed && (e.EventType != "task.resource.removed" || e.Metadata != nil)) ||
		(!e.Removed && e.EventType != "task.resource.updated") {
		return Record{}, errInvalidEvent
	}
	record := Record{ID: e.ID, Version: e.Version}
	if m := e.Metadata; m != nil {
		record.Metadata = &Metadata{RevisionID: m.VersionID, Title: m.Title, Type: m.Type, Difficulty: m.Level, EstimatedMinutes: m.Minutes}
		for _, topic := range m.Topics {
			record.Metadata.Topics = append(record.Metadata.Topics, TopicWeight(topic))
		}
		for _, language := range m.Languages {
			record.Metadata.Runtimes = append(record.Metadata.Runtimes, Runtime(language))
		}
	}
	return record.normalized(Tasks)
}

func (c *TaskClient) Snapshot(ctx context.Context, watermark *int64, cursor string) (SnapshotPage, error) {
	query := resourcePageParams(watermark)
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	var wire struct {
		Watermark *int64         `json:"watermark"`
		Resources []taskEnvelope `json:"resources"`
		Cursor    string         `json:"next_cursor"`
	}
	if err := c.request(ctx, http.MethodGet, "snapshot", query, nil, "", &wire); err != nil {
		return SnapshotPage{}, err
	}
	if wire.Watermark == nil || wire.Resources == nil || len(wire.Resources) > pageLimit {
		return SnapshotPage{}, errInvalidPage
	}
	page := SnapshotPage{Watermark: *wire.Watermark, NextCursor: wire.Cursor, Records: make([]Record, 0, len(wire.Resources))}
	for _, e := range wire.Resources {
		record, err := e.record()
		if err != nil {
			return SnapshotPage{}, err
		}
		page.Records = append(page.Records, record)
	}
	if err := validateSnapshot(page, watermark, cursor); err != nil {
		return SnapshotPage{}, err
	}
	return page, nil
}

func (c *TaskClient) Changes(ctx context.Context, watermark *int64, after int64) (ChangePage, error) {
	query := resourcePageParams(watermark)
	query.Set("after", strconv.FormatInt(after, 10))
	var wire struct {
		Watermark *int64         `json:"watermark"`
		Through   *int64         `json:"through"`
		More      bool           `json:"more"`
		Changes   []taskEnvelope `json:"changes"`
	}
	if err := c.request(ctx, http.MethodGet, "changes", query, nil, "", &wire); err != nil {
		return ChangePage{}, err
	}
	if wire.Watermark == nil || wire.Through == nil || wire.Changes == nil || len(wire.Changes) > pageLimit ||
		(watermark != nil && *watermark != *wire.Watermark) || *wire.Through < after ||
		(wire.More && *wire.Through >= *wire.Watermark) || (!wire.More && *wire.Through != *wire.Watermark) {
		return ChangePage{}, errInvalidPage
	}
	page := ChangePage{Watermark: *wire.Watermark, Through: *wire.Through, More: wire.More, Events: make([]Event, 0, len(wire.Changes))}
	last := after
	for _, e := range wire.Changes {
		record, err := e.record()
		if err != nil || record.Version <= last || record.Version > page.Through {
			return ChangePage{}, errInvalidPage
		}
		payload, err := json.Marshal(e)
		if err != nil {
			return ChangePage{}, errInvalidEvent
		}
		page.Events = append(page.Events, Event{ID: e.EventID, Fingerprint: sha256.Sum256(payload), Record: record})
		last = record.Version
	}
	if page.More && (len(page.Events) == 0 || last != page.Through) {
		return ChangePage{}, errInvalidPage
	}
	return page, nil
}

func resourcePageParams(watermark *int64) url.Values {
	params := url.Values{"limit": {strconv.Itoa(pageLimit)}}
	if watermark != nil {
		params.Set("watermark", strconv.FormatInt(*watermark, 10))
	}
	return params
}

type taskVersionKey struct {
	ID        string `json:"task_id"`
	VersionID string `json:"task_version_id"`
}

type taskAvailability struct {
	Resources []taskVersionKey `json:"resources"`
}

func (c *TaskClient) Available(ctx context.Context, keys []VersionKey) (map[VersionKey]bool, error) {
	return c.available(ctx, keys, "")
}

func (c *TaskClient) DiagnosticEligible(ctx context.Context, learner string, keys []VersionKey) (map[VersionKey]bool, error) {
	if !planner.ValidID(learner) {
		return nil, errInvalidPage
	}
	return c.available(ctx, keys, learner)
}

func (c *TaskClient) available(ctx context.Context, keys []VersionKey, learner string) (map[VersionKey]bool, error) {
	requested, err := validateVersionKeys(keys)
	if err != nil {
		return nil, err
	}
	input := taskAvailability{Resources: make([]taskVersionKey, 0, len(keys))}
	for _, key := range keys {
		input.Resources = append(input.Resources, taskVersionKey{ID: key.ID, VersionID: key.RevisionID})
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, errInvalidPage
	}
	var output taskAvailability
	path := "availability"
	if learner != "" {
		path = "diagnostic-eligibility"
	}
	if err := c.request(ctx, http.MethodPost, path, nil, raw, learner, &output); err != nil {
		return nil, err
	}
	if output.Resources == nil || len(output.Resources) > len(keys) {
		return nil, errInvalidPage
	}
	for _, key := range output.Resources {
		if err := confirmedVersion(requested, VersionKey{ID: key.ID, RevisionID: key.VersionID}); err != nil {
			return nil, err
		}
	}
	return requested, nil
}

func (c *TaskClient) request(ctx context.Context, method, path string, query url.Values, raw []byte, learner string, target any) error {
	address := c.base.JoinPath(path)
	address.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, method, address.String(), bytes.NewReader(raw))
	if err != nil {
		return errors.New("invalid tasks resource request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if learner != "" {
		req.Header.Set("X-User-ID", learner)
	}
	if len(raw) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return errors.New("tasks resource request failed")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusBadRequest && (path == "snapshot" || path == "changes") {
		return errSourcePosition
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("tasks resource returned HTTP %d", resp.StatusCode)
	}
	return decodeResourceJSON(resp.Body, target, maxPageBytes)
}

func decodeResourceJSON(in io.Reader, target any, max int64) error {
	reader := &io.LimitedReader{R: in, N: max + 1}
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errInvalidPage
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) || reader.N == 0 {
		return errInvalidPage
	}
	return nil
}
