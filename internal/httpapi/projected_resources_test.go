package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/overmindv/recommendations/internal/aiplan"
	"github.com/overmindv/recommendations/internal/materialref"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/projection/resources"
	"github.com/overmindv/recommendations/internal/savedplans"
)

const taskFixtureID = "00000000-0000-0000-0000-000000000002"
const revisionFixtureID = "00000000-0000-0000-0000-000000000003"
const materialFixtureID = "00000000-0000-0000-0000-000000000004"

type resourceCatalogStub struct {
	tasks     func([]planner.Task) ([]planner.Task, error)
	materials []resources.Candidate
	verify    func(resources.Origin, []resources.VersionKey) (map[resources.VersionKey]resources.Candidate, error)
}

func (s resourceCatalogStub) PracticeTasks(_ context.Context, tasks []planner.Task) ([]planner.Task, error) {
	return s.tasks(tasks)
}
func (s resourceCatalogStub) OutlineMaterials(context.Context, []string, []string) ([]resources.Candidate, error) {
	return s.materials, nil
}
func (s resourceCatalogStub) VerifyVersions(_ context.Context, origin resources.Origin, keys []resources.VersionKey) (map[resources.VersionKey]resources.Candidate, error) {
	return s.verify(origin, keys)
}

func TestProjectedPracticeUsesOwnerEstimateAndChecksBeforeReturning(t *testing.T) {
	archived := false
	catalog := resourceCatalogStub{
		tasks: func(tasks []planner.Task) ([]planner.Task, error) {
			tasks[0].Title, tasks[0].CatalogMinutes = "Verified task", 17
			return tasks, nil
		},
		verify: func(origin resources.Origin, keys []resources.VersionKey) (map[resources.VersionKey]resources.Candidate, error) {
			key := resources.VersionKey{ID: taskFixtureID, RevisionID: revisionFixtureID}
			if origin != resources.Tasks || len(keys) != 1 || keys[0] != key {
				t.Error("wrong task versions checked:", origin, keys)
			}
			if archived {
				return nil, nil
			}
			return map[resources.VersionKey]resources.Candidate{key: {Key: key}}, nil
		},
	}
	mux := http.NewServeMux()
	RegisterVerifiedAI(mux, token, aiplan.New(), catalog)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, request(validBody))
	var got planner.Plan
	err := json.Unmarshal(w.Body.Bytes(), &got)
	if w.Code != http.StatusOK || err != nil || got.PolicyVersion != resourcePolicy ||
		len(got.Items) != 1 || got.Items[0].Title != "Verified task" || got.TotalMinutes != 17 {
		t.Fatal(w.Code, w.Body.String(), err)
	}
	archived = true
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, request(validBody))
	if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), "Verified task") {
		t.Fatal("task changed before response but was returned:", w.Code, w.Body.String())
	}
}

type outlineProvider func(context.Context, []byte) ([]int, error)

func (p outlineProvider) Rank(ctx context.Context, prompt []byte) ([]int, error) {
	return p(ctx, prompt)
}

func TestOutlineAppendsCheckedContentOutsideThePrompt(t *testing.T) {
	const title = "Project note from Content"
	minutes := 13
	catalog := resourceCatalogStub{materials: []resources.Candidate{
		{Key: resources.VersionKey{ID: materialFixtureID, RevisionID: revisionFixtureID}, Title: title, EstimatedMinutes: &minutes},
	}}
	engine := aiplan.New(outlineProvider(func(_ context.Context, prompt []byte) ([]int, error) {
		if strings.Contains(string(prompt), title) {
			t.Error("local material sent to the external provider")
		}
		return []int{1, 2}, nil
	}))
	mux := http.NewServeMux()
	RegisterVerifiedAI(mux, token, engine, catalog)
	req := request(`{"goal":"Учебная цель","topics":[{"id":"` + actor + `","prerequisite_ids":[]}]}`)
	req.URL.Path = "/v1/learning-outline"
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	var out aiplan.Outline
	err := json.Unmarshal(w.Body.Bytes(), &out)
	if w.Code != http.StatusOK || err != nil || len(out.Materials) != 2 ||
		out.Materials[0].ID != materialref.Encode(materialFixtureID, revisionFixtureID) ||
		out.Materials[0].URL != materialref.Path(materialFixtureID) {
		t.Fatal(w.Code, w.Body.String(), err)
	}
}

type planReadStub struct {
	plansStub
	savedplans.Plan
}

func (s *planReadStub) Get(context.Context, string, string) (savedplans.Plan, error) {
	return s.Plan, nil
}

func TestSavedPlanHidesUnavailableResourcesWithoutMutatingStoredHistory(t *testing.T) {
	minutes := 13
	plan := savedplans.Plan{ID: actor, Revision: 1, Document: savedplans.Document{
		Tasks: []savedplans.Task{{ID: taskFixtureID, VersionID: revisionFixtureID, Title: "Archived task"}},
		Materials: []savedplans.Material{
			{ID: materialref.Encode(materialFixtureID, revisionFixtureID), Title: "Stored draft-like title", Status: "DONE"},
			{ID: "python", Title: "External reference", URL: "https://docs.python.org/", Status: "TODO"},
		},
	}}
	store := &planReadStub{Plan: plan}
	catalog := resourceCatalogStub{verify: func(origin resources.Origin, keys []resources.VersionKey) (map[resources.VersionKey]resources.Candidate, error) {
		if origin == resources.Tasks {
			return nil, resources.ErrUnavailable
		}
		return map[resources.VersionKey]resources.Candidate{keys[0]: {
			Title: "Confirmed material", EstimatedMinutes: &minutes,
		}}, nil
	}}
	mux := http.NewServeMux()
	RegisterVerifiedSavedPlans(mux, token, store, catalog)
	req := httptest.NewRequest(http.MethodGet, "/v1/learning-plans/"+plan.ID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	var got savedplans.Plan
	err := json.Unmarshal(w.Body.Bytes(), &got)
	if w.Code != http.StatusOK || err != nil || len(got.Document.Tasks) != 0 || len(got.Document.Materials) != 2 ||
		got.Document.Materials[0].Title != "Confirmed material" || got.Document.Materials[0].Status != "DONE" ||
		strings.Contains(w.Body.String(), "Archived task") || strings.Contains(w.Body.String(), "Stored draft-like title") ||
		len(store.Document.Tasks) != 1 || store.Document.Materials[0].Title != "Stored draft-like title" {
		t.Fatal(w.Code, w.Body.String(), err)
	}
}
