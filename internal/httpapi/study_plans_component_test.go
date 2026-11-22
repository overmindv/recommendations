package httpapi

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	contentpb "github.com/overmindv/content/pkg/api/content"
	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/projection/resources"
	"github.com/overmindv/recommendations/internal/ranking"
	"github.com/overmindv/recommendations/internal/studyplans"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type studyContentOwner struct {
	contentpb.UnimplementedMaterialCatalogServiceServer
	token string
	key   string
	rev   string
}

func (s *studyContentOwner) CheckMaterialAvailability(ctx context.Context, req *contentpb.CheckMaterialAvailabilityRequest) (*contentpb.CheckMaterialAvailabilityResponse, error) {
	auth := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(auth) != 1 || auth[0] != "Bearer "+s.token || len(metadata.ValueFromIncomingContext(ctx, "x-user-id")) != 0 {
		return nil, status.Error(codes.PermissionDenied, "wrong catalog metadata")
	}
	out := &contentpb.CheckMaterialAvailabilityResponse{}
	for _, resource := range req.Resources {
		if resource.GetMaterialId() == s.key && resource.GetRevisionId() == s.rev {
			out.Resources = append(out.Resources, &contentpb.MaterialVersionKey{MaterialId: s.key, RevisionId: s.rev})
		}
	}

	return out, nil
}

//nolint:paralleltest // Тест меняет поколение проекции в выделенной БД.
func TestPostgresStudyPlanThroughOwners(t *testing.T) {
	ctx, pool, id := t.Context(), diagnosticPool(t), diagnosticIDs(t)
	actor, peer, goal, topic := id(1), id(2), id(3), id(4)
	theory, practice, check := id(10), id(21), id(22)
	batch := &pgx.Batch{}
	batch.Queue("INSERT INTO learner_availability (user_id,weekly_minutes,timezone) VALUES ($1,240,'UTC'),($2,60,'UTC')", actor, peer)
	batch.Queue(`INSERT INTO learner_goals (id,user_id,title,target_topics,weekly_minutes,active,preferences)
		VALUES ($1,$2,'Goal',ARRAY[$3::uuid],240,true,'{"languages":["python"]}')`, goal, actor, topic)
	batch.Queue("UPDATE catalog_projection_state SET active_generation=11 WHERE singleton")
	batch.Queue("UPDATE resource_projection_checkpoints SET active_generation=11")
	batch.Queue(`INSERT INTO catalog_projected_topics (generation,topic_id,revision,tombstone,metadata)
		VALUES (11,$1,1,false,'{"status":"active"}')`, topic)
	for _, material := range []int{10, 11, 12} {
		batch.Queue(`INSERT INTO resource_projection_items (origin,generation,resource_id,version,revision_id,metadata)
			VALUES ('content',11,$1,1,$2,'{"Type":"note","EstimatedMinutes":5}')`, id(material), id(100+material))
		batch.Queue(`INSERT INTO resource_projection_topics (origin,generation,resource_id,topic_id,weight)
			VALUES ('content',11,$1,$2,1)`, id(material), topic)
	}
	for _, task := range []struct {
		id         int
		difficulty string
		language   string
	}{
		{21, "medium", "python"}, {22, "easy", "python"}, {23, "easy", "go"},
		{24, "easy", "python"}, {25, "easy", "python"}, {26, "easy", "python"},
	} {
		batch.Queue(`INSERT INTO resource_projection_items (origin,generation,resource_id,version,revision_id,metadata)
			VALUES ('tasks',11,$1,1,$2,jsonb_build_object('Type','programming','EstimatedMinutes',5,
				'Difficulty',$3::text,'Runtimes',jsonb_build_array(jsonb_build_object('ID',$4::text,'Version','3.12'))))`,
			id(task.id), id(100+task.id), task.difficulty, task.language)
		batch.Queue(`INSERT INTO resource_projection_topics (origin,generation,resource_id,topic_id,weight)
			VALUES ('tasks',11,$1,$2,1)`, id(task.id), topic)
	}
	batch.Queue(`INSERT INTO learning_material_reads (learner_id,material_id,revision_id,confirmed_at,event_id)
		VALUES($1,$2,$3,now(),$4)`, actor, id(12), id(112), id(50))
	for _, graded := range []struct {
		task, hours   int
		verdict, kind string
	}{
		{21, 60, "wrong_answer", "independent"}, {24, 48, "accepted", "independent"}, {25, 1, "wrong_answer", "non_independent"},
	} {
		batch.Queue(`INSERT INTO learning_task_outcomes (submission_id,learner_id,task_id,task_version_id,topic_ids,
			 verdict,evidence_kind,graded_at,event_id,correction_version)
			 VALUES($1,$2,$3,$4,ARRAY[$5::uuid],$6,$7,now()-$8::integer*interval '1 hour',$9,1)`,
			id(200+graded.task), actor, id(graded.task), id(100+graded.task), topic, graded.verdict, graded.kind, graded.hours, id(300+graded.task))
	}
	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		t.Fatal(err)
	}

	taskToken := strings.Repeat("t", 32)
	pinned := id(90)
	var tasksDown atomic.Bool
	var ownerCalls atomic.Int32
	tasksOwner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ownerCalls.Add(1)
		if tasksDown.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}
		if r.Header.Get("Authorization") != "Bearer "+taskToken {
			w.WriteHeader(http.StatusForbidden)

			return
		}
		type key struct {
			Task string `json:"task_id"`
			Rev  string `json:"task_version_id"`
		}
		var wire struct {
			Resources []key `json:"resources"`
		}
		if json.NewDecoder(r.Body).Decode(&wire) != nil || len(wire.Resources) == 0 || len(wire.Resources) > 100 {
			w.WriteHeader(http.StatusBadRequest)

			return
		}
		result := make([]key, 0, len(wire.Resources))
		for _, resource := range wire.Resources {
			if resource.Task == id(24) || resource.Task == id(25) {
				w.WriteHeader(http.StatusBadRequest)

				return
			}
			switch r.URL.Path {
			case resourceAvailabilityPath:
				if r.Header.Get("X-User-ID") != "" {
					w.WriteHeader(http.StatusForbidden)

					return
				}
				if resource.Task != id(26) {
					result = append(result, resource)
				}
			case "/v1/internal/resources/diagnostic-eligibility":
				if r.Header.Get("X-User-ID") != actor {
					w.WriteHeader(http.StatusForbidden)

					return
				}
				if resource.Task == check || resource.Task == id(23) || resource.Task == pinned {
					result = append(result, resource)
				}
			default:
				w.WriteHeader(http.StatusNotFound)

				return
			}
		}
		_ = json.NewEncoder(w).Encode(map[string][]key{"resources": result})
	}))
	t.Cleanup(tasksOwner.Close)
	tasks, err := resources.NewTaskClient(tasksOwner.URL, taskToken)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tasks.CloseIdleConnections)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	contentToken := strings.Repeat("c", 32)
	grpcServer := grpc.NewServer()
	contentpb.RegisterMaterialCatalogServiceServer(grpcServer, &studyContentOwner{token: contentToken, key: theory, rev: id(110)})
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	content, err := resources.NewContentClient(listener.Addr().String(), contentToken)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = content.Close() })

	source := resources.NewStudySource(resources.NewEligibility(resources.NewStore(pool), tasks, content), 24*time.Hour)
	policy := ranking.Policy{
		Version: "test-study-rank", TargetBPS: 6000, UnknownGapBPS: 3500, PrerequisiteMinBPS: 5500,
		MediumDifficultyBPS: 7000, HardDifficultyBPS: 8500,
		Weights: ranking.Factors{Difficulty: 3, Gap: 1},
	}
	evidence := mastery.Policy{Revision: "test-study-evidence", Algorithm: mastery.AlgorithmVersion, GapMaxBPS: 2500,
		DemonstratedBPS: 6000, MinWeightedTasks: 1}
	service := studyplans.NewService(studyplans.NewPostgres(pool), studyplans.NewProjectedBuilder(pool, source, policy, evidence))
	router := http.NewServeMux()
	serviceToken := strings.Repeat("w", 32)
	RegisterStudyPlans(router, serviceToken, service)
	path := "/v1/learner/goals/" + goal + "/study-plan"
	request := func(method, suffix, user, body, key string, want int) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequestWithContext(ctx, method, path+suffix, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+serviceToken)
		r.Header.Set("X-User-ID", user)
		r.Header.Set("Content-Type", "application/json")
		if key != "" {
			r.Header.Set("Idempotency-Key", key)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatal("unexpected study plan response:", w.Code, w.Body.String())
		}

		return w
	}
	send := func(method, suffix, user, body, key string, want int) studyplans.View {
		t.Helper()
		w := request(method, suffix, user, body, key, want)
		var view studyplans.View
		if want == http.StatusOK {
			if err := json.NewDecoder(w.Body).Decode(&view); err != nil {
				t.Fatal(err)
			}
		}

		return view
	}
	const initial = `{"goal_version":1,"availability_version":1,"plan_version":0}`
	send("PUT", "", peer, initial, id(60), http.StatusNotFound)
	if ownerCalls.Load() != 0 {
		t.Fatal("a foreign goal read the owner catalog")
	}
	plan := send("PUT", "", actor, initial, id(60), http.StatusOK)
	request("GET", "/explanation?plan_version=1", peer, "", "", http.StatusNotFound)
	explained := request("GET", "/explanation?plan_version=1", actor, "", "", http.StatusOK)
	var explanation studyplans.Explanation
	if explained.Header().Get("Cache-Control") != noStoreCache ||
		json.NewDecoder(explained.Body).Decode(&explanation) != nil || len(explanation.Steps) != 3 ||
		explanation.Steps[0].Reasons[0].Code != "topic_gap" || explanation.Steps[0].Evidence.Ref == nil ||
		explanation.Steps[0].Evidence.Ref.Watermark != plan.EvidenceWatermark {
		t.Fatal("initial explanation did not include the saved evidence reference")
	}
	var items []studyplans.Item
	for _, week := range plan.Weeks {
		items = append(items, week.Items...)
	}
	if plan.Version != 1 || plan.NextStep == nil || *plan.NextStep != 1 || len(items) != 3 ||
		items[0].ID != theory || items[1].ID != practice || items[2].ID != check || items[2].RevisionID != id(122) ||
		items[2].Stage != planner.StudyCheck {
		t.Fatal("expected an available note, allowed practice and the reserved unseen task:", plan)
	}
	tasksDown.Store(true)
	const next = `{"goal_version":1,"availability_version":1,"plan_version":1}`
	send("PUT", "", actor, next, id(61), http.StatusServiceUnavailable)
	callsAfterFailure := ownerCalls.Load()
	replay := send("PUT", "", actor, initial, id(60), http.StatusOK)
	reload := send("GET", "", actor, "", "", http.StatusOK)
	if replay.Version != 1 || reload.Version != 1 || reload.EvidenceWatermark != plan.EvidenceWatermark ||
		ownerCalls.Load() != callsAfterFailure {
		t.Fatal("catalog outage overwrote or recalculated the saved plan")
	}
	const progress = `{"plan_version":1}`
	send("PUT", "/steps/1/activity", actor, progress, "", http.StatusOK)
	ready := send("PUT", "/steps/2/activity", actor, progress, "", http.StatusOK)
	if ready.NextStep == nil || *ready.NextStep != 3 || ready.GoalDemonstrated {
		t.Fatal("activity did not stay separate from mastery:", ready)
	}
	send("PUT", "/steps/3/assessment", actor, progress, "", http.StatusConflict)
	if _, err := pool.Exec(ctx, `INSERT INTO learning_task_outcomes (submission_id,learner_id,task_id,task_version_id,topic_ids,
		verdict,evidence_kind,graded_at,event_id,correction_version)
		VALUES ($1,$2,$3,$4,ARRAY[$5::uuid],'accepted','independent',clock_timestamp(),$6,1)`,
		id(201), actor, check, id(122), topic, id(301)); err != nil {
		t.Fatal(err)
	}
	done := send("PUT", "/steps/3/assessment", actor, progress, "", http.StatusOK)
	if !done.GoalDemonstrated || done.NextStep != nil {
		t.Fatal("verified assessment did not complete the goal:", done)
	}
	tasksDown.Store(false)
	batch = &pgx.Batch{}
	batch.Queue(`INSERT INTO resource_projection_items (origin,generation,resource_id,version,revision_id,metadata)
		VALUES ('tasks',11,$1,1,$2,'{"Type":"programming","Difficulty":"easy","EstimatedMinutes":5,"Runtimes":[{"ID":"python","Version":"3.12"}]}')`,
		pinned, id(190))
	batch.Queue(`INSERT INTO resource_projection_topics (origin,generation,resource_id,topic_id,weight)
		VALUES ('tasks',11,$1,$2,1)`, pinned, topic)
	batch.Queue(`INSERT INTO retention_checks (goal_id,topic_id,goal_version,due_at,assigned_at,expires_at,task_id,task_version_id,estimated_minutes)
		VALUES ($1,$2,1,now()-interval '2 days',now()-interval '1 day',now()+interval '14 days',$3,$4,5)`,
		goal, topic, pinned, id(190))
	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		t.Fatal(err)
	}
	followup := send("PUT", "", actor, next, id(61), http.StatusOK)
	request("GET", "/explanation?plan_version=1", actor, "", "", http.StatusConflict)
	due := request("GET", "/explanation?plan_version=2", actor, "", "", http.StatusOK)
	if !strings.Contains(due.Body.String(), `"code":"due_review"`) || strings.Contains(due.Body.String(), `"alternative"`) {
		t.Fatal("the pinned check was not explained or an alternate check was exposed:", due.Body.String())
	}
	items = items[:0]
	for _, week := range followup.Weeks {
		items = append(items, week.Items...)
	}
	if followup.Version != 2 || len(items) != 1 || items[0].ID != pinned || items[0].RevisionID != id(190) ||
		items[0].Stage != planner.StudyCheck {
		t.Fatal("existing retention check was replaced or reused as practice:", followup)
	}
	if _, err := pool.Exec(ctx, `UPDATE resource_projection_items SET revision_id=$2,version=2
		WHERE origin='tasks' AND generation=11 AND resource_id=$1`, pinned, id(191)); err != nil {
		t.Fatal(err)
	}
	const third = `{"goal_version":1,"availability_version":1,"plan_version":2}`
	withdrawn := send("PUT", "", actor, third, id(62), http.StatusOK)
	reasons := request("GET", "/explanation?plan_version=3", actor, "", "", http.StatusOK)
	if !strings.Contains(reasons.Body.String(), `"code":"missing_resource"`) {
		t.Fatal("withdrawn version did not report its limitation:", reasons.Body.String())
	}
	if withdrawn.Version != 3 || withdrawn.NextStep != nil || withdrawn.GoalDemonstrated || len(withdrawn.UnmetOutcomes) != 1 ||
		withdrawn.UnmetOutcomes[0].Stage != planner.StudyCheck || withdrawn.UnmetOutcomes[0].Reason != "missing_resource" {
		t.Fatal("withdrawn retention version was silently replaced:", withdrawn)
	}
	const pause = `{"plan_version":3,"action":"pause"}`
	request("POST", "/feedback", peer, pause, id(70), http.StatusNotFound)
	request("POST", "/feedback", actor, `{"plan_version":2,"action":"pause"}`, id(70), http.StatusConflict)
	accepted := request("POST", "/feedback", actor, pause, id(70), http.StatusAccepted)
	repeated := request("POST", "/feedback", actor, pause, id(70), http.StatusAccepted)
	if accepted.Body.String() != repeated.Body.String() || accepted.Header().Get("Cache-Control") != noStoreCache {
		t.Fatal("feedback replay changed the receipt")
	}
	paused := send("GET", "", actor, "", "", http.StatusOK)
	if paused.PausedAt == nil || paused.BlockedReason != "paused" {
		t.Fatal("pause was not reflected in the current plan")
	}
	request("POST", "/feedback", actor, `{"plan_version":3,"action":"resume"}`, id(71), http.StatusAccepted)
}
