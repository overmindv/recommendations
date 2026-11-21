// Package savedplans хранит учебные планы отдельно от истории решений.
package savedplans

import (
	"errors"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/overmindv/recommendations/internal/materialref"
	"github.com/overmindv/recommendations/internal/planner"
)

const (
	statusTODO = "TODO"
	statusDone = "DONE"
)

var ErrNotFound = errors.New("plan not found")
var ErrConflict = errors.New("plan revision conflict")
var ErrInvalid = errors.New("invalid plan")
var ErrLimit = errors.New("plan limit reached")

type Material struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	URL     string `json:"url"`
	Action  string `json:"action"`
	Minutes int    `json:"minutes"`
	Status  string `json:"status"`
}
type Task struct {
	ID        string `json:"id"`
	VersionID string `json:"version_id"`
	Title     string `json:"title"`
	Minutes   int    `json:"minutes"`
	Completed bool   `json:"completed"`
}
type Document struct {
	ExcludedTaskIDs []string   `json:"excluded_task_ids,omitempty"`
	Goal            string     `json:"goal"`
	Minutes         int        `json:"minutes"`
	SessionCount    int        `json:"session_count"`
	TopicIDs        []string   `json:"topic_ids"`
	Materials       []Material `json:"materials"`
	Tasks           []Task     `json:"tasks"`
}
type Plan struct {
	ID       string   `json:"id"`
	Owner    string   `json:"-"`
	IsOwner  bool     `json:"is_owner"`
	Public   bool     `json:"public"`
	Revision int64    `json:"revision"`
	Document Document `json:"document"`
}
type Input struct {
	TaskID     string   `json:"task_id"`
	ID         string   `json:"id"`
	Revision   int64    `json:"revision"`
	Document   Document `json:"document"`
	Public     bool     `json:"public"`
	MaterialID string   `json:"material_id"`
	Status     string   `json:"status"`
}

func Validate(d Document) error {
	if strings.TrimSpace(d.Goal) == "" || !utf8.ValidString(d.Goal) || utf8.RuneCountInString(d.Goal) > 1000 || d.Minutes < 10 || d.Minutes > 240 || d.SessionCount < 1 || d.SessionCount > 8 || len(d.Materials) > 40 || len(d.Tasks) > 300 || len(d.TopicIDs) > 32 || len(d.ExcludedTaskIDs) > 300 {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for _, m := range d.Materials {
		if !validMaterialURL(m) || len(m.URL) > 2000 || m.ID == "" || len(m.ID) > 100 || seen[m.ID] || len(m.Title) > 2000 || len(m.Action) > 4000 || m.Minutes < 1 || !validStatus(m.Status) {
			return ErrInvalid
		}
		seen[m.ID] = true
	}
	seen = map[string]bool{}
	for _, t := range d.Tasks {
		key := t.ID
		if !planner.ValidID(t.ID) || !planner.ValidID(t.VersionID) || seen[key] || len(t.Title) > 2000 || t.Minutes < 1 {
			return ErrInvalid
		}
		seen[key] = true
	}
	for _, id := range append(append([]string{}, d.TopicIDs...), d.ExcludedTaskIDs...) {
		if !planner.ValidID(id) {
			return ErrInvalid
		}
	}

	return nil
}
func validMaterialURL(m Material) bool {
	if materialref.Owned(m.ID) {
		id, _, ok := materialref.Decode(m.ID)

		return ok && m.URL == materialref.Path(id)
	}
	u, err := url.Parse(m.URL)

	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil
}

func validStatus(s string) bool { return s == statusTODO || s == "IN_PROGRESS" || s == statusDone }

// Reset не переносит отметки автора в публичный ответ или копию.
func Reset(d Document) Document {
	d.Materials = append([]Material{}, d.Materials...)
	d.Tasks = append([]Task{}, d.Tasks...)
	d.TopicIDs = append([]string{}, d.TopicIDs...)
	d.ExcludedTaskIDs = append([]string{}, d.ExcludedTaskIDs...)
	for i := range d.Materials {
		d.Materials[i].Status = statusTODO
	}
	for i := range d.Tasks {
		d.Tasks[i].Completed = false
	}

	return d
}
func Expose(p Plan, actor string) Plan {
	p.IsOwner = p.Owner == actor && actor != ""
	if !p.IsOwner {
		p.Document = Reset(p.Document)
	}

	return p
}

// Merge сохраняет старые шаги и отметки. Обновление не стирает учебный путь.
func Merge(old, next Document) (Document, error) {
	next = Reset(next)
	seen := map[string]bool{}
	for _, m := range old.Materials {
		seen[m.ID] = true
	}
	materials := append([]Material{}, old.Materials...)
	for _, m := range next.Materials {
		if !seen[m.ID] {
			materials = append(materials, m)
			seen[m.ID] = true
		}
	}
	seen = map[string]bool{}
	tasks := append([]Task{}, old.Tasks...)
	for _, t := range tasks {
		seen[t.ID] = true
	}
	for _, id := range old.ExcludedTaskIDs {
		seen[id] = true
	}
	for _, t := range next.Tasks {
		key := t.ID
		if !seen[key] {
			tasks = append(tasks, t)
			seen[key] = true
		}
	}
	old.Materials = materials
	old.Tasks = tasks
	old.TopicIDs = next.TopicIDs

	return old, Validate(old)
}

// Summary ограничивает размер списка: описания и ссылки загружаются только в деталях.
func Summary(p Plan) Plan {
	d := p.Document
	d.Materials = append([]Material{}, d.Materials...)
	d.Tasks = append([]Task{}, d.Tasks...)
	d.TopicIDs = []string{}
	d.ExcludedTaskIDs = nil
	for i := range d.Materials {
		m := d.Materials[i]
		d.Materials[i] = Material{ID: m.ID, Status: m.Status}
	}
	for i := range d.Tasks {
		d.Tasks[i] = Task{ID: d.Tasks[i].ID}
	}
	p.Document = d

	return p
}
