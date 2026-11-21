package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/mastery"
)

const (
	unknownState       = "unknown"
	wrongAnswerVerdict = "wrong_answer"
)

func TestPostgresMasteryAPI(t *testing.T) {
	t.Parallel()

	dsn := os.Getenv("MASTERY_TEST_DSN")
	if dsn == "" {
		t.Skip("MASTERY_TEST_DSN is required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("MASTERY_TEST_DSN must point to a dedicated _test database")
	}
	ctx, id := t.Context(), diagnosticIDs(t)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	actor, peer, goal, topic, absent := id(1), id(2), id(3), id(4), id(5)
	batch := &pgx.Batch{}
	batch.Queue("INSERT INTO learner_availability (user_id,weekly_minutes,timezone) VALUES ($1,60,'UTC'),($2,60,'UTC')", actor, peer)
	batch.Queue(`INSERT INTO learner_goals (id,user_id,title,target_topics,weekly_minutes,active,preferences)
		VALUES ($1,$2,'Goal',ARRAY[$3::uuid,$4::uuid],20,true,
		        jsonb_build_object('self_assessments',jsonb_build_object($3::text,'confident')))`,
		goal, actor, topic, absent)
	if seedErr := pool.SendBatch(ctx, batch).Close(); seedErr != nil {
		t.Fatal(seedErr)
	}

	policy := mastery.Policy{Revision: "test-" + goal, Algorithm: mastery.AlgorithmVersion, GapMaxBPS: 3000, DemonstratedBPS: 8000, MinWeightedTasks: 1}
	store, err := mastery.NewPostgres(ctx, pool, policy)
	if err != nil {
		t.Fatal(err)
	}
	other := policy
	other.GapMaxBPS++
	if _, err := mastery.NewPostgres(ctx, pool, other); !errors.Is(err, mastery.ErrPolicyConflict) {
		t.Fatal("registered revision accepted different settings:", err)
	}

	mux := http.NewServeMux()
	token := strings.Repeat("w", 32)
	RegisterMastery(mux, token, store)
	request := func(method, owner, bearer, body string, status int) mastery.Snapshot {
		t.Helper()
		r := httptest.NewRequestWithContext(ctx, method, "/v1/learner/goals/"+goal+"/mastery", strings.NewReader(body))
		r.Header.Set("X-User-ID", owner)
		r.Header.Set("Authorization", "Bearer "+bearer)
		r.Header.Set("Content-Type", "application/json")
		res := httptest.NewRecorder()
		mux.ServeHTTP(res, r)
		if res.Code != status || res.Header().Get("Cache-Control") != noStoreCache {
			t.Fatal(method, "unexpected response:", res.Code, res.Body.String())
		}
		var snapshot mastery.Snapshot
		if status == http.StatusOK {
			if strings.Contains(res.Body.String(), "submission_id") || strings.Contains(res.Body.String(), "event_id") ||
				strings.Contains(res.Body.String(), "learner_id") {
				t.Fatal("response exposes internal evidence")
			}
			if err := json.NewDecoder(res.Body).Decode(&snapshot); err != nil {
				t.Fatal(err)
			}
		}

		return snapshot
	}
	const refresh = `{"goal_version":1}`
	request("GET", actor, token, "", http.StatusNotFound)
	request("GET", actor, "wrong", "", http.StatusForbidden)
	request("PUT", peer, token, refresh, http.StatusNotFound)
	request("PUT", actor, token, `{"goal_version":1,"score":10000}`, http.StatusBadRequest)
	request("PUT", actor, token, `{"goal_version":2}`, http.StatusConflict)
	empty := request("PUT", actor, token, refresh, http.StatusOK)
	if len(empty.Topics) != 2 || empty.Topics[0].ScoreBPS != nil || empty.Topics[0].State != unknownState {
		t.Fatal("self-assessment was counted in the baseline:", empty)
	}

	batch = &pgx.Batch{}
	for i, kind := range []string{"independent", "independent", "non_independent"} {
		verdict := "accepted"
		if i == 0 {
			verdict = wrongAnswerVerdict
		}
		batch.Queue(`INSERT INTO learning_task_outcomes (submission_id,learner_id,task_id,task_version_id,topic_ids,
			verdict,evidence_kind,graded_at,event_id,correction_version)
			VALUES ($1,$2,$3,$4,ARRAY[$5::uuid],$6,$7,now() - interval '1 day' + $8 * interval '1 minute',$9,1)`,
			id(10+i), actor, id(6), id(7), topic, verdict, kind, int32(i), id(20+i))
	}
	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		t.Fatal(err)
	}
	wrong := request("PUT", actor, token, refresh, http.StatusOK)
	first := wrong.Topics[0]
	if first.ScoreBPS == nil || *first.ScoreBPS != 0 || first.State != "gap" || first.SampleCount != 1 ||
		wrong.Topics[1].State != unknownState || wrong.Policy != policy {
		t.Fatal("wrong grade or duplicate accounting:", wrong)
	}
	if _, err := pool.Exec(ctx, `UPDATE learning_task_outcomes SET verdict='accepted',event_id=$2,correction_version=2 WHERE submission_id=$1`,
		id(10), id(23)); err != nil {
		t.Fatal(err)
	}
	stored := request("GET", actor, token, "", http.StatusOK)
	if !reflect.DeepEqual(stored, wrong) {
		t.Fatal("GET changed the stored snapshot")
	}
	fixed := request("PUT", actor, token, refresh, http.StatusOK)
	if fixed.Topics[0].ScoreBPS == nil || *fixed.Topics[0].ScoreBPS != 10000 || fixed.Topics[0].State != "demonstrated_in_sample" {
		t.Fatal("refresh did not pick up the correction:", fixed)
	}
	var version int
	var event string
	if err := pool.QueryRow(ctx, `SELECT (evidence->0->>'correction_version')::integer,evidence->0->>'event_id'
		FROM mastery_baselines WHERE goal_id=$1 AND jsonb_array_length(evidence)=1`, goal).Scan(&version, &event); err != nil ||
		version != 2 || event != id(23) {
		t.Fatal("could not trace the score to its corrected event:", version, event, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE learner_goals SET version=2 WHERE id=$1", goal); err != nil {
		t.Fatal(err)
	}
	request("GET", actor, token, "", http.StatusNotFound)
	request("PUT", actor, token, refresh, http.StatusConflict)
}
