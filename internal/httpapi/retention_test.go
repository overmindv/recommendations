package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/internal/projection/resources"
)

//nolint:paralleltest // Тест меняет состояние проекции в выделенной БД.
func TestPostgresRetentionAPI(t *testing.T) {
	const (
		dueStatus      = "due"
		assignedStatus = "assigned"
		checkedStatus  = "checked"
	)
	ctx, pool, id := t.Context(), diagnosticPool(t), diagnosticIDs(t)
	actor, peer, goal, topic, unknown := id(1), id(2), id(3), id(4), id(5)
	seen, candidate, revision := id(6), id(7), id(8)
	now := time.Now().UTC().Truncate(time.Microsecond)
	const interval = 24 * time.Hour
	prior := now.Add(-2 * interval)
	batch := &pgx.Batch{}
	batch.Queue("INSERT INTO learner_availability (user_id,weekly_minutes,timezone) VALUES ($1,60,'UTC'),($2,60,'UTC')", actor, peer)
	batch.Queue(`INSERT INTO learner_goals (id,user_id,title,target_topics,weekly_minutes,active,preferences)
		VALUES ($1,$2,'Goal',ARRAY[$3::uuid,$4::uuid],5,true,'{}')`, goal, actor, topic, unknown)
	batch.Queue("UPDATE catalog_projection_state SET active_generation=1 WHERE singleton")
	batch.Queue("UPDATE resource_projection_checkpoints SET active_generation=1 WHERE origin='tasks'")
	batch.Queue(`INSERT INTO catalog_projected_topics (generation,topic_id,revision,tombstone,metadata)
		VALUES (1,$1,1,false,'{"status":"active"}'),(1,$2,1,false,'{"status":"active"}')`, topic, unknown)
	batch.Queue(`INSERT INTO catalog_projected_prerequisites (generation,topic_id,prerequisite_id,revision,tombstone)
		VALUES (1,$1,$2,1,false)`, topic, unknown)
	for _, key := range []struct{ task, version, topic string }{
		{seen, id(9), topic}, {candidate, revision, topic}, {id(10), id(11), unknown},
	} {
		batch.Queue(`INSERT INTO resource_projection_items (origin,generation,resource_id,version,revision_id,metadata)
			VALUES ('tasks',1,$1,1,$2,'{"EstimatedMinutes":2}')`, key.task, key.version)
		batch.Queue(`INSERT INTO resource_projection_topics (origin,generation,resource_id,topic_id,weight,diagnostic_minutes)
			VALUES ('tasks',1,$1,$2,1,2)`, key.task, key.topic)
	}
	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		t.Fatal(err)
	}
	grade := func(n int, learner, task, version, verdict, kind string, at time.Time, topics []string, weights []int) {
		t.Helper()
		_, err := pool.Exec(ctx, `INSERT INTO learning_task_outcomes
		    (submission_id,learner_id,task_id,task_version_id,topic_ids,verdict,evidence_kind,graded_at,event_id,correction_version,topic_weights)
		    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,1,$10)`,
			id(100+n), learner, task, version, topics, verdict, kind, at, id(200+n), weights)
		if err != nil {
			t.Fatal(err)
		}
	}
	grade(1, actor, seen, id(9), "accepted", "independent", prior, []string{topic}, nil)
	policy := mastery.Policy{Revision: "test-retention", Algorithm: mastery.AlgorithmVersion, GapMaxBPS: 3000, DemonstratedBPS: 8000, MinWeightedTasks: 1}
	baseline, err := mastery.NewPostgres(ctx, pool, policy)
	if err != nil {
		t.Fatal(err)
	}
	before, err := baseline.Refresh(ctx, actor, goal, 1)
	if err != nil {
		t.Fatal(err)
	}

	ownerToken := strings.Repeat("t", 32)
	var calls atomic.Int32
	var allow, ownerFails atomic.Bool
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if ownerFails.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}
		if r.URL.Path != diagnosticEligibilityPath ||
			r.Header.Get("Authorization") != "Bearer "+ownerToken || r.Header.Get("X-User-ID") != actor {
			w.WriteHeader(http.StatusForbidden)

			return
		}
		type key struct {
			Task    string `json:"task_id"`
			Version string `json:"task_version_id"`
		}
		var wire struct {
			Resources []key `json:"resources"`
		}
		if json.NewDecoder(r.Body).Decode(&wire) != nil || len(wire.Resources) != 2 {
			w.WriteHeader(http.StatusBadRequest)

			return
		}
		confirmed := make([]key, 0, 1)
		for _, k := range wire.Resources {
			if allow.Load() && k.Task == candidate && k.Version == revision {
				confirmed = append(confirmed, k)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string][]key{"resources": confirmed})
	}))
	defer owner.Close()
	tasks, err := resources.NewTaskClient(owner.URL, ownerToken)
	if err != nil {
		t.Fatal(err)
	}
	defer tasks.CloseIdleConnections()
	catalog := resources.NewEligibility(resources.NewStore(pool), tasks, nil)
	router := http.NewServeMux()
	token := strings.Repeat("w", 32)
	RegisterRetention(router, token, mastery.NewRetention(baseline, catalog, interval))
	base := "/v1/learner/goals/" + goal + "/retention"
	send := func(method, route, learner, bearer, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequestWithContext(ctx, method, route, strings.NewReader(body))
		r.Header.Set("X-User-ID", learner)
		r.Header.Set("Authorization", "Bearer "+bearer)
		r.Header.Set("Content-Type", "application/json")
		res := httptest.NewRecorder()
		router.ServeHTTP(res, r)

		return res
	}
	read := func(res *httptest.ResponseRecorder, want int, target any) {
		t.Helper()
		if res.Code != want || res.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("unexpected retention response:", res.Code, res.Body.String())
		}
		if target != nil {
			if strings.Contains(res.Body.String(), "submission_id") || strings.Contains(res.Body.String(), "event_id") {
				t.Fatal("public response exposed raw evidence")
			}
			if decodeErr := json.NewDecoder(res.Body).Decode(target); decodeErr != nil {
				t.Fatal(decodeErr)
			}
		}
	}
	body := `{"goal_version":1,"due_at":"` + prior.Add(interval).Format(time.RFC3339Nano) + `"}`
	route := base + "/" + topic
	get := func() mastery.ReviewPlan {
		t.Helper()
		var plan mastery.ReviewPlan
		read(send("GET", base, actor, token, ""), http.StatusOK, &plan)

		return plan
	}
	start := get()
	if start.GoalVersion != 1 || len(start.Topics) != 2 || start.Topics[0].Status != dueStatus || start.Topics[1].Status != unknownState {
		t.Fatal("overdue and unknown topics not separated:", start)
	}
	read(send("PUT", route, actor, "wrong", body), http.StatusForbidden, nil)
	read(send("PUT", route, peer, token, body), http.StatusNotFound, nil)
	read(send("PUT", route, actor, token, strings.Replace(body, ":1,", ":2,", 1)), http.StatusConflict, nil)
	read(send("PUT", route, actor, token, strings.TrimSuffix(body, "}")+`,"task_id":"`+candidate+`"}`), http.StatusBadRequest, nil)
	read(send("PUT", base+"/"+unknown, actor, token, body), http.StatusConflict, nil)
	if calls.Load() != 0 {
		t.Fatal("invalid assignment reached Tasks")
	}

	var review mastery.TopicReview
	read(send("PUT", route, actor, token, body), http.StatusOK, &review)
	if review.Status != "no_new_task" || review.Check != nil || get().Topics[0].Status != dueStatus {
		t.Fatal("missing candidate cleared an overdue topic:", review)
	}
	ownerFails.Store(true)
	read(send("PUT", route, actor, token, body), http.StatusServiceUnavailable, nil)
	ownerFails.Store(false)
	allow.Store(true)

	responses := make([]*httptest.ResponseRecorder, 2)
	var concurrent sync.WaitGroup
	for i := range responses {
		concurrent.Go(func() { responses[i] = send("PUT", route, actor, token, body) })
	}
	concurrent.Wait()
	var assigned *mastery.ReviewCheck
	for _, res := range responses {
		var current mastery.TopicReview
		read(res, http.StatusOK, &current)
		if current.Status != assignedStatus || current.Check == nil || current.Check.TaskID != candidate || current.Check.VersionID != revision {
			t.Fatal("expected unseen task at exact version:", current)
		}
		if assigned != nil && !assigned.AssignedAt.Equal(current.Check.AssignedAt) {
			t.Fatal("concurrent requests created separate assignments")
		}
		assigned = current.Check
	}
	graded := assigned.AssignedAt
	grade(2, peer, candidate, revision, "accepted", "independent", graded, []string{topic}, nil)
	grade(3, actor, candidate, revision, "accepted", "non_independent", graded, []string{topic}, nil)
	grade(6, actor, candidate, id(47), "accepted", "independent", graded.Add(time.Microsecond), []string{topic}, nil)
	if got := get().Topics[0]; got.Status != assignedStatus || got.Check.Result != nil {
		t.Fatal("wrong version, foreign or non-independent grade checked the topic:", got)
	}
	grade(4, actor, candidate, revision, wrongAnswerVerdict, "independent", graded, []string{topic, unknown}, []int{3, 1})
	grade(5, actor, candidate, revision, "accepted", "independent", graded.Add(time.Microsecond), []string{topic}, nil)
	checked := get().Topics[0]
	if checked.Status != checkedStatus || checked.Check.Result == nil || checked.Check.Result.Verdict != wrongAnswerVerdict ||
		checked.Check.Result.WeightMicros != 750000 || !checked.Check.Result.GradedAt.Equal(graded) {
		t.Fatal("first independent review grade or its weight was lost:", checked)
	}
	count := calls.Load()
	read(send("PUT", route, actor, token, body), http.StatusOK, &review)
	if calls.Load() != count || review.Status != checkedStatus {
		t.Fatal("assignment replay did not return the completed check")
	}
	stored, err := baseline.Get(ctx, actor, goal)
	if err != nil || !stored.ComputedAt.Equal(before.ComputedAt) || stored.Topics[0].SampleCount != 1 {
		t.Fatal("retention overwrote the baseline:", stored, err)
	}
	after, err := baseline.Refresh(ctx, actor, goal, 1)
	if err != nil || after.Topics[0].ScoreBPS == nil || *after.Topics[0].ScoreBPS != 5714 ||
		after.Topics[0].WeightMicros != 1750000 || after.Topics[0].SampleCount != 2 {
		t.Fatal("failed review did not count once with its normal weight:", after, err)
	}
}

func TestRetentionDisabled(t *testing.T) {
	t.Parallel()

	router := http.NewServeMux()
	token := strings.Repeat("s", 32)
	RegisterRetention(router, token, nil)
	r := httptest.NewRequestWithContext(t.Context(), "GET", "/v1/learner/goals/00000000-0000-4000-8000-000000000001/retention", nil)
	r.Header.Set("X-User-ID", "00000000-0000-4000-8000-000000000002")
	r.Header.Set("Authorization", "Bearer "+token)
	res := httptest.NewRecorder()
	router.ServeHTTP(res, r)
	if res.Code != http.StatusServiceUnavailable {
		t.Fatal("disabled retention endpoint should return 503:", res.Code)
	}
}
