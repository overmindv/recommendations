package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/content/pkg/api/activity"
	"github.com/overmindv/recommendations/internal/consumer"
	"github.com/overmindv/recommendations/internal/diagnostic"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDiagnosticRuntimeFinalization(t *testing.T) {
	t.Parallel()

	binary, dsn, brokers := os.Getenv("DIAGNOSTIC_RUNTIME_BINARY"), os.Getenv("DIAGNOSTIC_TEST_DSN"),
		os.Getenv("DIAGNOSTIC_KAFKA_TEST_BROKERS")
	if binary == "" || dsn == "" || brokers == "" {
		t.Skip("diagnostic runtime binary, isolated PostgreSQL and isolated Kafka are required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("diagnostic smoke requires a dedicated _test database")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	const (
		sessionID = "99999999-9999-4999-8999-999999999999"
		actorID   = "11111111-1111-4111-8111-111111111112"
		goalID    = "88888888-8888-4888-8888-888888888889"
	)
	tasks, err := json.Marshal([]diagnostic.Candidate{{TaskID: smokeTask, VersionID: smokeRevision, TopicID: smokeTopic, Minutes: 4}})
	if err != nil {
		t.Fatal(err)
	}
	var startedAt time.Time
	if fixtureErr := pool.QueryRow(ctx, `
		WITH availability AS (
		    INSERT INTO learner_availability (user_id,weekly_minutes,timezone) VALUES ($1,60,'UTC') RETURNING user_id
		), goal AS (
		    INSERT INTO learner_goals (id,user_id,title,target_topics,weekly_minutes,active,preferences)
		    SELECT $2,user_id,'Diagnostic',ARRAY[$3::uuid],60,true,'{}' FROM availability RETURNING id
		)
		INSERT INTO diagnostic_sessions
		    (id,goal_id,goal_version,status,policy_version,max_tasks,budget_minutes,estimated_minutes,tasks,started_at,expires_at)
		SELECT $4,id,1,'ready','diagnostic-v1',4,20,4,$5,now()-interval '2 minute',now()+interval '18 minute' FROM goal
		RETURNING started_at`, actorID, goalID, smokeTopic, sessionID, tasks).Scan(&startedAt); fixtureErr != nil {
		t.Fatal(fixtureErr)
	}
	listener := smokeListener(t)
	address := listener.Addr().String()
	if closeErr := listener.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	token := strings.Repeat("d", 32)
	cmd := exec.CommandContext(ctx, binary, "serve") //nolint:gosec // Путь к собранному бинарнику задает оператор теста.
	cmd.Env = []string{"ENV=test", "DATABASE_URL=" + dsn, "HTTP_ADDRESS=" + address, "AI_ENABLED=false",
		"RECOMMENDATIONS_SERVICE_TOKEN=" + token, "RECOMMENDATIONS_RESOURCE_PROJECTION_ENABLED=false",
		"ENTITIES_URL=", "ENTITIES_SNAPSHOT_TOKEN=", "RECOMMENDATIONS_LEARNING_EVIDENCE_ENABLED=true",
		"KAFKA_BOOTSTRAP_SERVERS=" + brokers, "KAFKA_CONTENT_LEARNING_TOPIC=" + activity.DefaultLearningTopic,
		"KAFKA_TLS_ENABLED=false", "KAFKA_TLS_CA_FILE=", "KAFKA_TLS_CERT_FILE=", "KAFKA_TLS_KEY_FILE=", "KAFKA_TLS_SERVER_NAME=",
		"KAFKA_SASL_MECHANISM=", "KAFKA_SASL_USERNAME=", "KAFKA_SASL_PASSWORD="}
	if startErr := cmd.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	client := &http.Client{Timeout: 2 * time.Second}
	path := "http://" + address + "/v1/learner/goals/" + goalID + "/diagnostic"
	call := func(method, endpoint string) (diagnostic.Session, error) {
		var status diagnostic.Session
		var body io.Reader
		if method == http.MethodPut {
			body = strings.NewReader(`{"goal_version":1,"session_id":"` + sessionID + `"}`)
		}
		req, requestErr := http.NewRequestWithContext(ctx, method, endpoint, body)
		if requestErr != nil {
			return status, requestErr
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-User-ID", actorID)
		req.Header.Set("Content-Type", "application/json")
		resp, responseErr := client.Do(req)
		if responseErr != nil {
			return status, responseErr
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return status, fmt.Errorf("diagnostic returned HTTP %d", resp.StatusCode)
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&status)

		return status, decodeErr
	}
	smokeWait(t, ctx, func() bool {
		preview, previewErr := call(http.MethodPut, path+"/finalize")

		return previewErr == nil && preview.Status == diagnostic.Ready && preview.Report != nil && preview.Report.Assessed == 0
	})

	producer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	gradedAt := startedAt.Add(time.Minute).UTC()
	value, err := json.Marshal(map[string]any{
		"event_id": smokeEvent, "event_type": consumer.TasksTopic, "schema_version": 1,
		"aggregate_id": smokePlan, "aggregate_version": 1, "occurred_at": gradedAt,
		"payload": map[string]any{
			"submission_id": smokePlan, "learner_id": actorID, "task_id": smokeTask, "task_version_id": smokeRevision,
			"topic_ids": []string{smokeTopic}, "verdict": "accepted", "graded_at": gradedAt,
			"first_independent_attempt": true, "correction_version": 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if publishErr := producer.ProduceSync(ctx, &kgo.Record{Topic: consumer.TasksTopic, Key: []byte(smokePlan), Value: value}).FirstErr(); publishErr != nil {
			t.Fatal(publishErr)
		}
	}
	smokeWait(t, ctx, func() bool {
		closed, finalizeErr := call(http.MethodPut, path+"/finalize")

		return finalizeErr == nil && closed.Status == diagnostic.Finalized && closed.Report != nil && closed.Report.Assessed == 1
	})
	report, err := call(http.MethodGet, path)
	if err != nil || report.Status != diagnostic.Finalized || report.Report == nil || len(report.Report.Outcomes) != 1 ||
		report.Report.Outcomes[0].SubmissionID != smokePlan || report.Report.Outcomes[0].EventID != smokeEvent {
		t.Fatal("runtime report lost the Kafka outcome:", report, err)
	}
}
