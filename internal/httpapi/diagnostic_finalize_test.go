package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/recommendations/internal/diagnostic"
	"github.com/overmindv/recommendations/internal/learnergoals"
)

func TestPostgresDiagnosticFinalization(t *testing.T) {
	t.Parallel()

	ctx, pool, id := t.Context(), diagnosticPool(t), diagnosticIDs(t)
	owner, peer, target, unassigned, prerequisite := id(1), id(2), id(3), id(4), id(5)
	first := diagnostic.Candidate{TaskID: id(6), VersionID: id(7), TopicID: target, Minutes: 5}
	second := diagnostic.Candidate{TaskID: id(8), VersionID: id(9), TopicID: prerequisite, Minutes: 5}
	partialFirst, partialSecond := first, second
	partialFirst.TaskID, partialFirst.VersionID = id(40), id(41)
	partialSecond.TaskID, partialSecond.VersionID = id(42), id(43)
	goal, session, partialGoal, partialSession := id(10), id(11), id(12), id(13)
	skipGoal, skippedSession := id(14), id(15)
	tasks, err := json.Marshal([]diagnostic.Candidate{first, second})
	if err != nil {
		t.Fatal(err)
	}
	batch := &pgx.Batch{}
	batch.Queue("INSERT INTO learner_availability (user_id, weekly_minutes, timezone) VALUES ($1, 60, 'UTC'), ($2, 60, 'UTC')", owner, peer)
	for _, entry := range []struct {
		goal, session string
		minutesAgo    int
		partial       bool
	}{
		{goal, session, 8, false}, {partialGoal, partialSession, 25, true}, {skipGoal, skippedSession, 2, false},
	} {
		batch.Queue(`INSERT INTO learner_goals (id, user_id, title, target_topics, weekly_minutes, active, preferences)
			VALUES ($1,$2,'Diagnostic',ARRAY[$3::uuid,$4::uuid],20,true,'{}')`, entry.goal, owner, target, unassigned)
		payload := tasks
		if entry.partial {
			payload, err = json.Marshal([]diagnostic.Candidate{partialFirst, partialSecond})
			if err != nil {
				t.Fatal(err)
			}
		}
		batch.Queue(`INSERT INTO diagnostic_sessions
			(id,goal_id,goal_version,status,policy_version,max_tasks,budget_minutes,estimated_minutes,tasks,started_at,expires_at)
			VALUES ($1,$2,1,'ready','diagnostic-v1',4,20,10,$3,
			        now()-$4::int*interval '1 minute',now()+(20-$4::int)*interval '1 minute')`,
			entry.session, entry.goal, payload, entry.minutesAgo)
	}
	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		t.Fatal(err)
	}
	var startedAt, partialAt time.Time
	if err := pool.QueryRow(ctx, "SELECT started_at FROM diagnostic_sessions WHERE id=$1", session).Scan(&startedAt); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT started_at FROM diagnostic_sessions WHERE id=$1", partialSession).Scan(&partialAt); err != nil {
		t.Fatal(err)
	}
	outcome := func(n int, actor string, task diagnostic.Candidate, kind, verdict string, stamp time.Time) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO learning_task_outcomes (submission_id,learner_id,task_id,task_version_id,topic_ids,verdict,evidence_kind,
			                                     graded_at,event_id,correction_version)
			VALUES ($1,$2,$3,$4,ARRAY[$5::uuid],$6,$7,$8,$9,1)`,
			id(n), actor, task.TaskID, task.VersionID, task.TopicID, verdict, kind, stamp, id(n+50)); err != nil {
			t.Fatal(err)
		}
	}
	outcome(20, owner, first, "independent", "wrong_answer", startedAt.Add(time.Minute))
	outcome(21, owner, first, "independent", "accepted", startedAt.Add(2*time.Minute))
	outcome(22, peer, second, "independent", "accepted", startedAt.Add(time.Minute))
	outcome(23, owner, second, "non_independent", "accepted", startedAt.Add(time.Minute))
	outcome(24, owner, second, "independent", "accepted", startedAt.Add(-time.Second))
	outcome(25, owner, second, "independent", "accepted", startedAt.Add(21*time.Minute))
	outcome(26, owner, second, "independent", "accepted", startedAt.Add(12*time.Minute))
	previous := second
	previous.VersionID = id(16)
	outcome(27, owner, previous, "independent", "accepted", startedAt.Add(time.Minute))
	outside := second
	outside.TopicID = unassigned
	outcome(28, owner, outside, "independent", "accepted", startedAt.Add(time.Minute))
	outside = second
	outside.TaskID = id(17)
	outcome(29, owner, outside, "independent", "accepted", startedAt.Add(time.Minute))

	const goalsPath = "/v1/learner/goals/"
	token := strings.Repeat("r", 32)
	router := http.NewServeMux()
	RegisterDiagnostic(router, token, diagnostic.NewService(learnergoals.NewPostgres(pool), diagnostic.NewPostgres(pool), nil))
	request := func(method, path, actor, supplied, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequestWithContext(ctx, method, goalsPath+path, strings.NewReader(body))
		r.Header.Set("X-User-ID", actor)
		r.Header.Set("Authorization", "Bearer "+supplied)
		r.Header.Set("Content-Type", "application/json")
		res := httptest.NewRecorder()
		router.ServeHTTP(res, r)

		return res
	}
	check := func(res *httptest.ResponseRecorder, status int) diagnostic.Session {
		t.Helper()
		if res.Code != status || res.Header().Get("Cache-Control") != noStoreCache {
			t.Fatal("unexpected finalize response:", res.Code, res.Body.String())
		}
		var got diagnostic.Session
		if status == http.StatusOK {
			if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
				t.Fatal(err)
			}
		}

		return got
	}
	body := func(id string, partial bool) string {
		return fmt.Sprintf(`{"goal_version":1,"session_id":%q,"allow_partial":%t}`, id, partial)
	}
	path := goal + "/diagnostic"
	closePath := path + "/finalize"
	check(request(http.MethodPut, closePath, owner, "wrong", body(session, false)), http.StatusForbidden)
	check(request(http.MethodPut, closePath, peer, token, body(session, false)), http.StatusNotFound)
	check(request(http.MethodPut, closePath, owner, token, body(id(18), false)), http.StatusConflict)
	check(request(http.MethodPut, closePath, owner, token, strings.Replace(body(session, false), "1,", "2,", 1)), http.StatusConflict)
	for _, field := range []string{`"verdict":"accepted"`, `"score":100`, `"submission_id":"fake"`, `"learner_id":"fake"`} {
		input := strings.TrimSuffix(body(session, false), "}") + "," + field + "}"
		check(request(http.MethodPut, closePath, owner, token, input), http.StatusBadRequest)
	}

	preview := check(request(http.MethodPut, closePath, owner, token, body(session, false)), http.StatusOK)
	if preview.Status != diagnostic.Ready || preview.FinalizedAt != nil || len(preview.Tasks) != 0 ||
		preview.Baseline != diagnostic.BaselineUnknown || preview.Report == nil || preview.Report.Assessed != 1 || preview.Report.Assigned != 2 ||
		len(preview.Report.Outcomes) != 1 || preview.Report.Outcomes[0].SubmissionID != id(20) {
		t.Fatal("untrusted results entered the preview or an incomplete session closed:", preview)
	}
	wantTopics := []diagnostic.TopicCoverage{
		{TopicID: target, Assigned: 1, Assessed: 1, Observation: "not_demonstrated", Uncertainty: "limited"},
		{TopicID: unassigned, Observation: diagnostic.BaselineUnknown, Uncertainty: "no_evidence"},
		{TopicID: prerequisite, Assigned: 1, Observation: diagnostic.BaselineUnknown, Uncertainty: "no_evidence"},
	}
	if !reflect.DeepEqual(preview.Report.Topics, wantTopics) {
		t.Fatal("incorrect topic coverage:", preview.Report.Topics)
	}
	var pending bool
	if err := pool.QueryRow(ctx, "SELECT report IS NULL AND finalized_at IS NULL FROM diagnostic_sessions WHERE id=$1", session).
		Scan(&pending); err != nil || !pending {
		t.Fatal("an incomplete preview persisted a final report:", pending, err)
	}

	outcome(30, owner, second, "independent", "accepted", startedAt.Add(3*time.Minute))
	var requests [8]*httptest.ResponseRecorder
	var group sync.WaitGroup
	for i := range requests {
		group.Go(func() {
			requests[i] = request(http.MethodPut, closePath, owner, token, body(session, false))
		})
	}
	group.Wait()
	var closed diagnostic.Session
	for _, res := range requests {
		got := check(res, http.StatusOK)
		if got.Status != diagnostic.Finalized || got.FinalizedAt == nil || got.Report == nil || got.Report.Assessed != 2 ||
			got.Report.Topics[2].Observation != "demonstrated_on_task" {
			t.Fatal("retry after evaluation did not finalize:", got)
		}
		if closed.ID != "" && !reflect.DeepEqual(got, closed) {
			t.Fatal("concurrent finalization changed the snapshot:", got, closed)
		}
		closed = got
	}
	afterSkip := check(request(http.MethodPut, path+"/skip", owner, token, `{"goal_version":1}`), http.StatusOK)
	if !reflect.DeepEqual(afterSkip, closed) {
		t.Fatal("skip changed a finalized session:", afterSkip)
	}

	outcome(31, owner, partialFirst, "independent", "accepted", partialAt.Add(time.Minute))
	outcome(33, owner, partialSecond, "independent", "accepted", partialAt.Add(20*time.Minute))
	partialPath := partialGoal + "/diagnostic"
	incomplete := check(request(http.MethodPut, partialPath+"/finalize", owner, token, body(partialSession, false)), http.StatusOK)
	if incomplete.Status != diagnostic.Expired || incomplete.FinalizedAt != nil || incomplete.Report == nil ||
		incomplete.Report.Assessed != 1 {
		t.Fatal("an expired session did not wait for explicit partial finalization:", incomplete)
	}
	partial := check(request(http.MethodPut, partialPath+"/finalize", owner, token, body(partialSession, true)), http.StatusOK)
	if partial.Status != diagnostic.Finalized || partial.Report == nil || partial.Report.Assessed != 1 ||
		partial.Report.Topics[2].Observation != diagnostic.BaselineUnknown {
		t.Fatal("missing assessment treated as a failure:", partial)
	}
	outcome(32, owner, partialSecond, "independent", "accepted", partialAt.Add(time.Minute))
	reloaded := check(request(http.MethodGet, partialPath, owner, token, ""), http.StatusOK)
	if !reflect.DeepEqual(partial, reloaded) {
		t.Fatal("late event changed a finalized report:", reloaded)
	}
	skipPath := skipGoal + "/diagnostic"
	check(request(http.MethodPut, skipPath+"/skip", owner, token, `{"goal_version":1}`), http.StatusOK)
	skipped := check(request(http.MethodPut, skipPath+"/finalize", owner, token, body(skippedSession, true)), http.StatusOK)
	if skipped.Status != diagnostic.Skipped || skipped.Report != nil || skipped.Baseline != diagnostic.BaselineUnknown {
		t.Fatal("skipped session acquired evidence:", skipped)
	}
}
