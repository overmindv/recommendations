package httpapi

import (
	"context"

	"github.com/overmindv/recommendations/internal/aiplan"
	"github.com/overmindv/recommendations/internal/materialref"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/projection/resources"
	"github.com/overmindv/recommendations/internal/savedplans"
)

type ResourceCatalog interface {
	PracticeTasks(context.Context, []planner.Task) ([]planner.Task, error)
	OutlineMaterials(context.Context, []string, []string) ([]resources.Candidate, error)
	VerifyVersions(context.Context, resources.Origin, []resources.VersionKey) (map[resources.VersionKey]resources.Candidate, error)
}

const resourcePolicy = "practice-v3-resource-catalog"
const materialAction = "Прочитайте опубликованный материал по теме."

func confirmPlan(ctx context.Context, catalog ResourceCatalog, plan planner.Plan) error {
	keys := make([]resources.VersionKey, 0, len(plan.Items))
	seen := make(map[resources.VersionKey]bool)
	appendItems := func(items []planner.Item) {
		for _, item := range items {
			key := resources.VersionKey{ID: item.TaskID, RevisionID: item.TaskVersionID}
			if !seen[key] {
				keys = append(keys, key)
				seen[key] = true
			}
		}
	}
	appendItems(plan.Items)
	for _, session := range plan.Sessions {
		appendItems(session.Items)
	}
	got, err := catalog.VerifyVersions(ctx, resources.Tasks, keys)
	if err != nil || len(got) != len(keys) {
		return resources.ErrUnavailable
	}
	for _, key := range keys {
		if _, ok := got[key]; !ok {
			return resources.ErrUnavailable
		}
	}

	return nil
}

func materialForOutline(c resources.Candidate) aiplan.Material {
	return aiplan.Material{
		ID: materialref.Encode(c.Key.ID, c.Key.RevisionID), Title: c.Title,
		URL: materialref.Path(c.Key.ID), Action: materialAction, Minutes: *c.EstimatedMinutes,
	}
}

func filterSavedDocument(ctx context.Context, catalog ResourceCatalog, document savedplans.Document, strict bool) (savedplans.Document, error) {
	tasks := make([]resources.VersionKey, 0, len(document.Tasks))
	for _, t := range document.Tasks {
		tasks = append(tasks, resources.VersionKey{ID: t.ID, RevisionID: t.VersionID})
	}
	materials := make([]resources.VersionKey, 0, len(document.Materials))
	for _, m := range document.Materials {
		if id, revision, ok := materialref.Decode(m.ID); ok {
			materials = append(materials, resources.VersionKey{ID: id, RevisionID: revision})
		}
	}
	availableTasks, taskErr := catalog.VerifyVersions(ctx, resources.Tasks, tasks)
	availableMaterials, materialErr := catalog.VerifyVersions(ctx, resources.Content, materials)
	if strict && (taskErr != nil || materialErr != nil) {
		return savedplans.Document{}, resources.ErrUnavailable
	}
	result := document
	result.Tasks = make([]savedplans.Task, 0, len(availableTasks))
	result.Materials = make([]savedplans.Material, 0, len(document.Materials))
	for _, t := range document.Tasks {
		c, ok := availableTasks[resources.VersionKey{ID: t.ID, RevisionID: t.VersionID}]
		if taskErr == nil && ok {
			t.Title = c.Title
			if c.EstimatedMinutes != nil {
				t.Minutes = *c.EstimatedMinutes
			}
			result.Tasks = append(result.Tasks, t)
		}
	}
	for _, m := range document.Materials {
		if !materialref.Owned(m.ID) {
			result.Materials = append(result.Materials, m)

			continue
		}
		id, revision, ok := materialref.Decode(m.ID)
		c, confirmed := availableMaterials[resources.VersionKey{ID: id, RevisionID: revision}]
		if materialErr == nil && ok && confirmed && c.EstimatedMinutes != nil {
			m.Title, m.URL, m.Action, m.Minutes = c.Title, materialref.Path(id), materialAction, *c.EstimatedMinutes
			result.Materials = append(result.Materials, m)
		}
	}

	return result, nil
}
