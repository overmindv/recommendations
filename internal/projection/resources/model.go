package resources

import (
	"errors"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/overmindv/recommendations/internal/planner"
)

type Origin string

const (
	Tasks   Origin = "tasks"
	Content Origin = "content"
)

var errInvalidRecord = errors.New("invalid resource identity or version")

var runtimeVersion = regexp.MustCompile(`^[0-9]+\.[0-9]+(\.[0-9]+)?$`)

type TopicWeight struct {
	ID     string
	Weight int
}

type Runtime struct {
	ID      string
	Version string
}

type Metadata struct {
	RevisionID       string
	Title            string
	Type             string
	Format           string
	Difficulty       string
	EstimatedMinutes *int
	Topics           []TopicWeight
	Runtimes         []Runtime
}

type Record struct {
	ID       string
	Version  int64
	Metadata *Metadata
}

// Неполное обновление сохраняет номер версии, но убирает ресурс из выдачи.
func (r Record) normalized(origin Origin) (Record, error) {
	if !planner.ValidID(r.ID) || r.Version < 0 ||
		(origin != Tasks && origin != Content) || (origin == Tasks && r.Version == 0) {
		return Record{}, errInvalidRecord
	}
	if r.Metadata == nil {
		return r, nil
	}
	metadata, ok := r.Metadata.normalized(origin)
	if !ok {
		r.Metadata = nil
	} else {
		r.Metadata = &metadata
	}
	return r, nil
}

func (m Metadata) normalized(origin Origin) (Metadata, bool) {
	if !planner.ValidID(m.RevisionID) || strings.TrimSpace(m.Title) == "" ||
		!utf8.ValidString(m.Title) || utf8.RuneCountInString(m.Title) > 256 ||
		strings.ContainsFunc(m.Title, unicode.IsControl) || len(m.Topics) == 0 || len(m.Topics) > 8 {
		return Metadata{}, false
	}
	maxMinutes := 1440
	if origin == Content {
		maxMinutes = 480
	}
	if m.EstimatedMinutes != nil {
		if *m.EstimatedMinutes < 1 || *m.EstimatedMinutes > maxMinutes {
			return Metadata{}, false
		}
		minutes := *m.EstimatedMinutes
		m.EstimatedMinutes = &minutes
	}
	m.Topics = slices.Clone(m.Topics)
	slices.SortFunc(m.Topics, func(a, b TopicWeight) int { return strings.Compare(a.ID, b.ID) })
	for i, topic := range m.Topics {
		if !planner.ValidID(topic.ID) || topic.Weight < 1 || topic.Weight > 10000 ||
			(i > 0 && m.Topics[i-1].ID == topic.ID) {
			return Metadata{}, false
		}
	}

	if origin == Content {
		switch m.Type {
		case "article", "note", "summary", "theory":
		default:
			return Metadata{}, false
		}
		if (m.Format != "markdown" && m.Format != "typst") || len(m.Runtimes) != 0 || m.Difficulty != "" {
			return Metadata{}, false
		}
		return m, true
	}

	switch m.Difficulty {
	case "easy", "medium", "hard":
	default:
		return Metadata{}, false
	}
	if m.Format != "" {
		return Metadata{}, false
	}
	switch m.Type {
	case "single_choice", "multiple_choice":
		return m, len(m.Runtimes) == 0
	case "programming":
		m.Runtimes = supportedRuntimes(m.Runtimes)
		return m, len(m.Runtimes) > 0
	default:
		return Metadata{}, false
	}
}

func supportedRuntimes(runtimes []Runtime) []Runtime {
	if len(runtimes) == 0 || len(runtimes) > 8 {
		return nil
	}
	supported := make([]Runtime, 0, len(runtimes))
	for _, runtime := range runtimes {
		if (runtime.ID == "python" || runtime.ID == "go") &&
			len(runtime.Version) <= 32 && runtimeVersion.MatchString(runtime.Version) {
			supported = append(supported, runtime)
		}
	}
	slices.SortFunc(supported, func(a, b Runtime) int {
		if byID := strings.Compare(a.ID, b.ID); byID != 0 {
			return byID
		}
		return strings.Compare(a.Version, b.Version)
	})
	return slices.Compact(supported)
}
