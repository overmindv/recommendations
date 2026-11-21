package httpapi

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/diagnostic"
	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/projection/resources"
)

const diagnosticEligibilityPath = "/v1/internal/resources/diagnostic-eligibility"

func diagnosticTestID(n int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", n)
}

func diagnosticPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DIAGNOSTIC_TEST_DSN")
	if dsn == "" {
		t.Skip("DIAGNOSTIC_TEST_DSN is required")
	}
	cfg, parseErr := pgxpool.ParseConfig(dsn)
	if parseErr != nil || !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("DIAGNOSTIC_TEST_DSN must point to a dedicated _test database")
	}
	ctx := t.Context()
	pool, poolErr := pgxpool.NewWithConfig(ctx, cfg)
	if poolErr != nil {
		t.Fatal(poolErr)
	}
	t.Cleanup(pool.Close)

	return pool
}

func diagnosticIDs(t *testing.T) func(int) string {
	t.Helper()
	var namespace [8]byte
	if _, err := rand.Read(namespace[:]); err != nil {
		t.Fatal(err)
	}
	namespace[6] = namespace[6]&0x3f | 0x80

	return func(n int) string {
		return fmt.Sprintf("%x-%x-4000-%x-%012x", namespace[:4], namespace[4:6], namespace[6:], n)
	}
}

//nolint:paralleltest // Тест меняет состояние проекции в выделенной БД.
func TestPostgresDiagnosticSessions(t *testing.T) {
	ctx, pool, id := t.Context(), diagnosticPool(t), diagnosticIDs(t)
	actor, peer := id(1), id(2)
	target, prerequisite, empty, inactive := id(3), id(4), id(5), id(6)
	goal, scarce, expired, skipFirst, fresh := id(7), id(8), id(9), id(10), id(14)
	seen, selected, supporting := id(11), id(12), id(13)
	batch := &pgx.Batch{}
	batch.Queue("INSERT INTO learner_availability (user_id, weekly_minutes, timezone) VALUES ($1, 60, 'UTC'), ($2, 60, 'UTC')", actor, peer)
	for _, entry := range []struct{ id, topic, owner string }{
		{goal, target, actor}, {scarce, empty, actor}, {expired, target, actor}, {skipFirst, target, actor}, {fresh, target, peer},
	} {
		batch.Queue(`INSERT INTO learner_goals (id, user_id, title, target_topics, weekly_minutes, active, preferences)
			VALUES ($1, $2, 'Goal', ARRAY[$3::uuid], 20, true, '{}')`, entry.id, entry.owner, entry.topic)
	}
	batch.Queue("UPDATE catalog_projection_state SET active_generation=1 WHERE singleton")
	batch.Queue("UPDATE resource_projection_checkpoints SET active_generation=1 WHERE origin='tasks'")
	for _, id := range []string{target, prerequisite, empty} {
		batch.Queue(`INSERT INTO catalog_projected_topics (generation, topic_id, revision, tombstone, metadata)
			VALUES (1, $1, 1, false, '{"status":"active"}')`, id)
	}
	batch.Queue(`INSERT INTO catalog_projected_topics (generation, topic_id, revision, tombstone, metadata)
		VALUES (1, $1, 1, false, '{"status":"archived"}')`, inactive)
	batch.Queue(`INSERT INTO catalog_projected_prerequisites (generation, topic_id, prerequisite_id, revision, tombstone)
		VALUES (1, $1, $2, 1, false), (1, $1, $3, 1, false)`, target, prerequisite, inactive)
	for _, entry := range []struct {
		id, topic string
		minutes   int
	}{
		{seen, target, 2}, {selected, target, 4}, {supporting, prerequisite, 3}, {id(15), inactive, 1},
	} {
		batch.Queue(`INSERT INTO resource_projection_items (origin, generation, resource_id, version, revision_id, metadata)
			VALUES ('tasks',1,$1,1,$1,jsonb_build_object('Title','Question','Type','single_choice','Difficulty','easy','EstimatedMinutes',$2::integer))`,
			entry.id, entry.minutes)
		batch.Queue(`INSERT INTO resource_projection_topics (origin, generation, resource_id, topic_id, weight, diagnostic_minutes)
			VALUES ('tasks',1,$1,$2,1,$3)`, entry.id, entry.topic, entry.minutes)
	}
	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		t.Fatal(err)
	}

	type versionKey struct {
		ID      string `json:"task_id"`
		Version string `json:"task_version_id"`
	}
	var revoked, ownerDown atomic.Bool
	var historyChecks atomic.Int32
	ownerToken, workerToken := strings.Repeat("t", 32), strings.Repeat("w", 32)
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ownerDown.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}
		var wire struct {
			Resources []versionKey `json:"resources"`
		}
		if r.Header.Get("Authorization") != "Bearer "+ownerToken || json.NewDecoder(r.Body).Decode(&wire) != nil {
			w.WriteHeader(http.StatusForbidden)

			return
		}
		switch r.URL.Path {
		case diagnosticEligibilityPath:
			historyChecks.Add(1)
			switch r.Header.Get("X-User-ID") {
			case actor:
				wire.Resources = slices.DeleteFunc(wire.Resources, func(key versionKey) bool { return key.ID == seen })
			case peer:
			default:
				w.WriteHeader(http.StatusBadRequest)

				return
			}
		case "/v1/internal/resources/availability":
			if revoked.Load() {
				wire.Resources = slices.DeleteFunc(wire.Resources, func(key versionKey) bool { return key.ID == selected })
			}
		default:
			w.WriteHeader(http.StatusNotFound)

			return
		}
		_ = json.NewEncoder(w).Encode(wire)
	}))
	defer owner.Close()
	tasks, err := resources.NewTaskClient(owner.URL, ownerToken)
	if err != nil {
		t.Fatal(err)
	}
	defer tasks.CloseIdleConnections()
	catalog := resources.NewEligibility(resources.NewStore(pool), tasks, nil)
	service := diagnostic.NewService(learnergoals.NewPostgres(pool), diagnostic.NewPostgres(pool), catalog)
	router := http.NewServeMux()
	RegisterDiagnostic(router, workerToken, service)
	request := func(method, path, browserActor, token, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequestWithContext(ctx, method, "/v1/learner/goals/"+path, strings.NewReader(body))
		r.Header.Set("X-User-ID", browserActor)
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		res := httptest.NewRecorder()
		router.ServeHTTP(res, r)

		return res
	}
	at := func(method, path, browserActor, body string, status int) diagnostic.Session {
		t.Helper()
		res := request(method, path, browserActor, workerToken, body)
		if res.Code != status || res.Header().Get("Cache-Control") != noStoreCache {
			t.Fatal("unexpected response:", res.Code, res.Body.String())
		}
		var session diagnostic.Session
		if status == http.StatusOK {
			if err := json.NewDecoder(res.Body).Decode(&session); err != nil {
				t.Fatal(err)
			}
		}

		return session
	}

	path := goal + "/diagnostic"
	if got := request(http.MethodGet, path, actor, "wrong", ""); got.Code != http.StatusForbidden {
		t.Fatal("service token not enforced")
	}
	at(http.MethodGet, path, actor, "", http.StatusNotFound)
	at(http.MethodPut, path, peer, `{"goal_version":1}`, http.StatusNotFound)
	at(http.MethodPut, path, actor, `{"goal_version":1,"user_id":"other"}`, http.StatusBadRequest)
	at(http.MethodPut, path, actor, `{"goal_version":1,"tasks":[]}`, http.StatusBadRequest)
	at(http.MethodPut, path, actor, `{"goal_version":1,"budget_minutes":21}`, http.StatusBadRequest)

	const parallel = 6
	var group sync.WaitGroup
	var responses [parallel]*httptest.ResponseRecorder
	for i := range responses {
		group.Go(func() {
			responses[i] = request(http.MethodPut, path, actor, workerToken, `{"goal_version":1,"budget_minutes":7}`)
		})
	}
	group.Wait()
	var pinned string
	for _, res := range responses {
		var session diagnostic.Session
		err := json.NewDecoder(res.Body).Decode(&session)
		if res.Code != http.StatusOK || err != nil || session.Status != diagnostic.Ready || session.Baseline != diagnostic.BaselineUnknown ||
			len(session.Tasks) != 2 || session.Tasks[0].TaskID != selected || session.Tasks[1].TaskID != supporting ||
			session.Tasks[0].VersionID != selected || session.GoalVersion != 1 || session.EstimatedMinutes != 7 ||
			session.ExpiresAt.Sub(session.StartedAt) != 7*time.Minute {
			t.Fatal("incorrect diagnostic selection:", res.Code, session, err)
		}
		if pinned != "" && pinned != session.ID {
			t.Fatal("concurrent start replaced pinned tasks")
		}
		pinned = session.ID
	}
	checks := historyChecks.Load()
	replayed := at(http.MethodPut, path, actor, `{"goal_version":1,"budget_minutes":10}`, http.StatusOK)
	if replayed.ID != pinned || replayed.StopRule.BudgetMinutes != 7 || historyChecks.Load() != checks {
		t.Fatal("start retry should not reselect tasks:", replayed)
	}
	unknown := at(http.MethodPut, fresh+"/diagnostic", peer, `{"goal_version":1,"budget_minutes":7}`, http.StatusOK)
	if len(unknown.Tasks) != 2 || unknown.Tasks[0].TaskID != seen || unknown.Baseline != diagnostic.BaselineUnknown {
		t.Fatal("no-history diagnostic:", unknown)
	}
	at(http.MethodGet, path, peer, "", http.StatusNotFound)
	none := at(http.MethodPut, scarce+"/diagnostic", actor, `{"goal_version":1}`, http.StatusOK)
	if none.Status != diagnostic.NoTasks || none.Tasks == nil || len(none.Tasks) != 0 || none.Baseline != diagnostic.BaselineUnknown {
		t.Fatal("scarce bank:", none)
	}
	at(http.MethodPut, expired+"/diagnostic", actor, `{"goal_version":1}`, http.StatusOK)
	if _, err := pool.Exec(ctx, `UPDATE diagnostic_sessions
		SET expires_at=expires_at-interval '1 day', started_at=started_at-interval '1 day' WHERE goal_id=$1`, expired); err != nil {
		t.Fatal(err)
	}
	past := at(http.MethodGet, expired+"/diagnostic", actor, "", http.StatusOK)
	if past.Status != diagnostic.Expired || len(past.Tasks) != 0 {
		t.Fatal("expired session still offers tasks:", past)
	}

	revoked.Store(true)
	at(http.MethodGet, path, actor, "", http.StatusConflict)
	ownerDown.Store(true)
	at(http.MethodPut, skipFirst+"/diagnostic", actor, `{"goal_version":1}`, http.StatusServiceUnavailable)
	at(http.MethodGet, skipFirst+"/diagnostic", actor, "", http.StatusNotFound)
	at(http.MethodPut, skipFirst+"/diagnostic/skip", actor, `{"goal_version":1}`, http.StatusOK)
	firstSkip := at(http.MethodPut, path+"/skip", actor, `{"goal_version":1}`, http.StatusOK)
	secondSkip := at(http.MethodPut, path+"/skip", actor, `{"goal_version":1}`, http.StatusOK)
	if firstSkip.Status != diagnostic.Skipped || firstSkip.Baseline != diagnostic.BaselineUnknown || firstSkip.ID != pinned ||
		len(firstSkip.Tasks) != 0 || firstSkip.SkippedAt == nil || secondSkip.SkippedAt == nil || !firstSkip.SkippedAt.Equal(*secondSkip.SkippedAt) {
		t.Fatal("skip changed session identity or baseline:", firstSkip, secondSkip)
	}
	at(http.MethodPut, path, actor, `{"goal_version":1}`, http.StatusOK)
	var storedTasks int
	if err := pool.QueryRow(ctx, "SELECT jsonb_array_length(tasks) FROM diagnostic_sessions WHERE goal_id=$1", goal).Scan(&storedTasks); err != nil || storedTasks != 2 {
		t.Fatal("skip did not preserve pinned tasks:", storedTasks, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE learner_goals SET version=2 WHERE id=$1", goal); err != nil {
		t.Fatal(err)
	}
	at(http.MethodPut, path+"/skip", actor, `{"goal_version":1}`, http.StatusConflict)
	at(http.MethodGet, path, actor, "", http.StatusNotFound)
	ownerDown.Store(false)
	revoked.Store(false)
	revised := at(http.MethodPut, path, actor, `{"goal_version":2}`, http.StatusOK)
	if revised.ID == pinned || revised.GoalVersion != 2 {
		t.Fatal("stale session reused after goal edit:", revised)
	}
}

func TestDiagnosticRejectsDuplicateActor(t *testing.T) {
	t.Parallel()

	token := strings.Repeat("t", 32)
	router := http.NewServeMux()
	RegisterDiagnostic(router, token, nil)
	req := httptest.NewRequest(http.MethodGet, "/v1/learner/goals/"+diagnosticTestID(1)+"/diagnostic", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-User-ID", diagnosticTestID(1))
	req.Header.Add("X-User-ID", diagnosticTestID(2))
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		body, _ := io.ReadAll(res.Body)
		t.Fatal("ambiguous actor accepted:", res.Code, string(body))
	}
}
