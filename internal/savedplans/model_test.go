package savedplans

import (
	"encoding/json"
	"strings"
	"testing"
)

func sample() Document {
	return Document{Goal: "Junior AI", Minutes: 60, SessionCount: 3, TopicIDs: []string{}, Materials: []Material{{ID: "python", Title: "Python", URL: "https://docs.python.org/3/tutorial/", Minutes: 25, Status: statusDone}}, Tasks: []Task{{ID: "00000000-0000-4000-8000-000000000001", VersionID: "00000000-0000-4000-8000-000000000002", Title: "Task", Minutes: 10, Completed: true}}}
}
func TestPublicRedactionAndCopyDoNotMutateOwner(t *testing.T) {
	t.Parallel()

	p := Plan{Owner: "owner", Document: sample()}
	public := Expose(p, "visitor")
	if public.IsOwner || public.Document.Materials[0].Status != statusTODO || public.Document.Tasks[0].Completed {
		t.Fatal("progress leaked")
	}
	if p.Document.Materials[0].Status != statusDone || !p.Document.Tasks[0].Completed {
		t.Fatal("owner mutated")
	}
	own := Expose(p, "owner")
	if !own.IsOwner || own.Document.Materials[0].Status != statusDone {
		t.Fatal("owner progress lost")
	}
}
func TestRefreshPreservesWorkAndDeduplicates(t *testing.T) {
	t.Parallel()

	old := sample()
	next := sample()
	next.Materials[0].Status = statusTODO
	next.Materials = append(next.Materials, Material{ID: "numpy", URL: "https://numpy.org/", Minutes: 25, Status: statusTODO})
	next.Tasks = nil
	got, err := Merge(old, next)
	if err != nil || len(got.Materials) != 2 || got.Materials[0].Status != statusDone || len(got.Tasks) != 1 {
		t.Fatal(got, err)
	}
	again, err := Merge(got, next)
	if err != nil || len(again.Materials) != 2 {
		t.Fatal("duplicate refresh")
	}
}
func TestPlanValidationRejectsUnsafeOrUnboundedInput(t *testing.T) {
	t.Parallel()

	for _, edit := range []func(*Document){func(d *Document) { d.Materials[0].URL = "javascript:alert(1)" }, func(d *Document) { d.Materials[0].Status = "MASTERED" }, func(d *Document) { d.Materials = append(d.Materials, d.Materials[0]) }, func(d *Document) { d.Tasks[0].VersionID = "bad" }} {
		d := sample()
		edit(&d)
		if Validate(d) == nil {
			t.Fatal("invalid plan accepted")
		}
	}
}

func TestSummaryBoundsListAndPreservesCounts(t *testing.T) {
	t.Parallel()

	d := sample()
	d.Materials[0].Action = strings.Repeat("x", 4000)
	d.Materials[0].Title = strings.Repeat("x", 2000)
	d.Tasks[0].Title = strings.Repeat("x", 2000)
	for len(d.Tasks) < 300 {
		d.Tasks = append(d.Tasks, d.Tasks[0])
	}
	for len(d.Materials) < 40 {
		d.Materials = append(d.Materials, d.Materials[0])
	}
	list := []Plan{}
	for i := 0; i < 50; i++ {
		list = append(list, Summary(Plan{Document: d}))
	}
	raw, err := json.Marshal(list)
	if err != nil || len(raw) > 8<<20 {
		t.Fatal("list exceeds client limit", len(raw), err)
	}
	if len(list[0].Document.Tasks) != 300 || list[0].Document.Materials[0].Status != statusDone || list[0].Document.Materials[0].Action != "" {
		t.Fatal("summary invalid")
	}
}
func TestRefreshDoesNotDuplicateTaskVersions(t *testing.T) {
	t.Parallel()

	old := sample()
	next := sample()
	next.Tasks[0].VersionID = "00000000-0000-4000-8000-000000000003"
	got, err := Merge(old, next)
	if err != nil || len(got.Tasks) != 1 || got.Tasks[0].VersionID != old.Tasks[0].VersionID {
		t.Fatal("task version duplicated", got, err)
	}
}

func TestEmptySavedPlanCanRefreshAndRemovedTasksStayRemoved(t *testing.T) {
	t.Parallel()

	old := sample()
	id := old.Tasks[0].ID
	old.Tasks = nil
	old.Materials = nil
	old.ExcludedTaskIDs = []string{id}
	next := sample()
	next.Materials = nil
	got, err := Merge(old, next)
	if err != nil || len(got.Tasks) != 0 {
		t.Fatal("removed task reappeared or empty update rejected", got, err)
	}
}
