package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/content/pkg/api/activity"
	contentpb "github.com/overmindv/content/pkg/api/content"
	"github.com/overmindv/recommendations/internal/materialref"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/savedplans"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/grpc"
)

const (
	smokeActor    = "11111111-1111-4111-8111-111111111111"
	smokeTopic    = "22222222-2222-4222-8222-222222222222"
	smokeTask     = "33333333-3333-4333-8333-333333333333"
	smokeMaterial = "44444444-4444-4444-8444-444444444444"
	smokeRevision = "55555555-5555-4555-8555-555555555555"
	smokePlan     = "66666666-6666-4666-8666-666666666666"
	smokeEvent    = "77777777-7777-4777-8777-777777777777"
)

type smokeContent struct {
	contentpb.UnimplementedContentServiceServer
	contentpb.UnimplementedMaterialCatalogServiceServer
	archived atomic.Bool
}

func (m *smokeContent) SnapshotMaterials(context.Context, *contentpb.SnapshotMaterialsRequest) (*contentpb.SnapshotMaterialsResponse, error) {
	return &contentpb.SnapshotMaterialsResponse{Watermark: 1, Entries: []*contentpb.MaterialSnapshotEntry{
		{MaterialId: smokeMaterial, RevisionId: smokeRevision, Version: 1, Metadata: &contentpb.MaterialSnapshotMetadata{
			Title: "Published fixture", Type: "note", Format: "markdown", TopicIds: []string{smokeTopic}, EstimatedMinutes: 15,
		}},
	}}, nil
}

func (m *smokeContent) CheckMaterialAvailability(_ context.Context, req *contentpb.CheckMaterialAvailabilityRequest) (*contentpb.CheckMaterialAvailabilityResponse, error) {
	out := &contentpb.CheckMaterialAvailabilityResponse{}
	if !m.archived.Load() && len(req.Resources) == 1 && req.Resources[0].GetMaterialId() == smokeMaterial && req.Resources[0].GetRevisionId() == smokeRevision {
		out.Resources = req.Resources
	}
	return out, nil
}

func TestResourceRuntimeSmoke(t *testing.T) {
	binary, dsn, brokers := os.Getenv("RESOURCE_RUNTIME_BINARY"), os.Getenv("RESOURCE_PROJECTION_TEST_DSN"), os.Getenv("RESOURCE_KAFKA_TEST_BROKERS")
	if binary == "" || dsn == "" || brokers == "" {
		t.Skip("runtime binary, isolated Postgres and isolated Kafka are required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("runtime smoke requires a dedicated _test database")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	broker, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	kafkaTopic := "resource.runtime.test." + strconv.FormatInt(time.Now().UnixNano(), 10)
	create := kmsg.NewPtrCreateTopicsRequest()
	create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: kafkaTopic, NumPartitions: 1, ReplicationFactor: 1}}
	created, err := create.RequestWith(ctx, broker)
	if err != nil || len(created.Topics) != 1 || created.Topics[0].ErrorCode != 0 {
		t.Fatal("could not create smoke topic:", err)
	}

	entities := smokeEntities(t)
	tasks, taskArchived := smokeTasks(t)
	content := &smokeContent{}
	listener := smokeListener(t)
	grpcServer := grpc.NewServer()
	contentpb.RegisterContentServiceServer(grpcServer, content)
	contentpb.RegisterMaterialCatalogServiceServer(grpcServer, content)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	bind := smokeListener(t)
	httpAddress := bind.Addr().String()
	if err := bind.Close(); err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("r", 32)
	cmd := exec.CommandContext(ctx, binary, "serve")
	cmd.Env = append(os.Environ(),
		"ENV=test", "DATABASE_URL="+dsn, "HTTP_ADDRESS="+httpAddress, "AI_ENABLED=false",
		"RECOMMENDATIONS_RESOURCE_PROJECTION_ENABLED=true", "RECOMMENDATIONS_SERVICE_TOKEN="+token,
		"ENTITIES_URL="+entities.URL, "ENTITIES_SNAPSHOT_TOKEN="+strings.Repeat("e", 32),
		"TASKS_RESOURCE_URL="+tasks.URL, "TASKS_RESOURCE_TOKEN="+strings.Repeat("t", 32),
		"CONTENT_CATALOG_ADDRESS="+listener.Addr().String(), "CONTENT_GRPC_CATALOG_TOKEN="+strings.Repeat("c", 32),
		"KAFKA_BOOTSTRAP_SERVERS="+brokers, "KAFKA_CONTENT_MATERIAL_TOPIC="+kafkaTopic,
		"KAFKA_TLS_ENABLED=false", "KAFKA_TLS_CA_FILE=", "KAFKA_TLS_CERT_FILE=", "KAFKA_TLS_KEY_FILE=",
		"KAFKA_TLS_SERVER_NAME=", "KAFKA_SASL_MECHANISM=", "KAFKA_SASL_USERNAME=", "KAFKA_SASL_PASSWORD=",
	)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	defer func() {
		_ = cmd.Process.Kill()
		<-exited
	}()
	client := &http.Client{Timeout: 5 * time.Second}
	call := func(method, path string, input any) (int, []byte, error) {
		var body []byte
		if input != nil {
			body, _ = json.Marshal(input)
		}
		req, err := http.NewRequestWithContext(ctx, method, "http://"+httpAddress+path, bytes.NewReader(body))
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-User-ID", smokeActor)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		result, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return resp.StatusCode, result, err
	}
	smokeWait(t, ctx, func() bool {
		select {
		case err := <-exited:
			exited <- err
			t.Fatal("recommendations stopped before ready:", err, output.String())
		default:
		}
		code, _, _ := call(http.MethodGet, "/ready", nil)
		return code == http.StatusOK
	})
	practice := planner.Request{TopicIDs: []string{smokeTopic}, Minutes: 60, Topics: []planner.Topic{{ID: smokeTopic}},
		Tasks: []planner.Task{{ID: smokeTask, TaskVersionID: smokeRevision, TopicID: smokeTopic, Title: "Gateway title", TaskType: "single_choice", Difficulty: "easy"}}}
	checkPractice := func(want int) {
		t.Helper()
		code, body, err := call(http.MethodPost, "/v1/practice-plan", practice)
		var plan planner.Plan
		if decode := json.Unmarshal(body, &plan); code != http.StatusOK || err != nil || decode != nil || len(plan.Items) != want ||
			(want != 0 && (plan.TotalMinutes != 17 || plan.Items[0].Title != "Owner task")) {
			t.Fatal("unexpected practice response:", code, string(body), err)
		}
	}
	checkPractice(1)
	input := savedplans.Input{ID: smokePlan, Document: savedplans.Document{
		Goal: "Fixture", Minutes: 60, SessionCount: 1, TopicIDs: []string{smokeTopic},
		Tasks:     []savedplans.Task{{ID: smokeTask, VersionID: smokeRevision, Title: "Input title", Minutes: 17}},
		Materials: []savedplans.Material{{ID: materialref.Encode(smokeMaterial, smokeRevision), URL: materialref.Path(smokeMaterial), Minutes: 15, Status: "TODO"}},
	}}
	if code, body, err := call(http.MethodPost, "/v1/learning-plans", input); code != http.StatusOK || err != nil || !bytes.Contains(body, []byte("Published fixture")) {
		t.Fatal("initial published plan not served:", code, string(body), err)
	}
	taskArchived.Store(true)
	content.archived.Store(true)
	checkPractice(0)
	code, body, err := call(http.MethodGet, "/v1/learning-plans/"+smokePlan, nil)
	var plan savedplans.Plan
	if decode := json.Unmarshal(body, &plan); code != http.StatusOK || err != nil || decode != nil ||
		len(plan.Document.Tasks) != 0 || len(plan.Document.Materials) != 0 {
		t.Fatal("stale saved resources escaped the live check:", code, string(body), err)
	}
	raw, err := json.Marshal(activity.Event[activity.MaterialState]{
		EventID: smokeEvent, EventType: activity.MaterialArchived, SchemaVersion: 1, OccurredAt: time.Now(),
		AggregateID: smokeMaterial, AggregateVersion: 2, RevisionID: smokeRevision, Payload: activity.MaterialState{Tombstone: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.ProduceSync(ctx, &kgo.Record{Topic: kafkaTopic, Key: []byte(smokeMaterial), Value: raw}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	smokeWait(t, ctx, func() bool {
		var archived bool
		err := pool.QueryRow(ctx, `SELECT i.version = 2 AND i.metadata IS NULL
		    FROM resource_projection_items AS i JOIN resource_projection_checkpoints AS c
		      ON c.origin=i.origin AND i.generation=c.active_generation WHERE i.origin='content' AND i.resource_id=$1`, smokeMaterial).Scan(&archived)
		return err == nil && archived
	})
}

func smokeListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func smokeWait(t *testing.T, ctx context.Context, predicate func() bool) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for !predicate() {
		select {
		case <-ctx.Done():
			t.Fatal("runtime test deadline:", ctx.Err())
		case <-ticker.C:
		}
	}
}

func smokeEntities(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/catalog/snapshot":
			_, _ = io.WriteString(w, `{"watermark":1,"records":[{"catalog_revision":1,"record":{"kind":"topic","topic_id":"`+smokeTopic+`","topic":{"title":"Fixture topic","order_index":0,"difficulty":"basic","status":"active"}}}]}`)
		case "/v1/internal/catalog/changes":
			_, _ = io.WriteString(w, `{"watermark":1,"through":1,"more":false,"changes":[]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func smokeTasks(t *testing.T) (*httptest.Server, *atomic.Bool) {
	t.Helper()
	archived := &atomic.Bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/resources/snapshot":
			_, _ = io.WriteString(w, `{"watermark":1,"resources":[{"event_id":"`+smokeEvent+`","event_type":"task.resource.updated","schema_version":1,"aggregate_id":"`+smokeTask+`","aggregate_version":1,"occurred_at":"2026-11-19T00:00:00Z","tombstone":false,"metadata":{"task_version_id":"`+smokeRevision+`","title":"Owner task","task_type":"single_choice","difficulty":"easy","estimated_minutes":17,"topic_weights":[{"topic_id":"`+smokeTopic+`","weight":1}],"languages":[]}}]}`)
		case "/v1/internal/resources/changes":
			_, _ = io.WriteString(w, `{"watermark":1,"through":1,"more":false,"changes":[]}`)
		case "/v1/internal/resources/availability":
			if archived.Load() {
				_, _ = io.WriteString(w, `{"resources":[]}`)
			} else {
				_, _ = io.WriteString(w, `{"resources":[{"task_id":"`+smokeTask+`","task_version_id":"`+smokeRevision+`"}]}`)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server, archived
}
