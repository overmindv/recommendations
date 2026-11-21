package learnergoals

import (
	"errors"
	"slices"
)

var ErrUnknownTemplate = errors.New("unknown goal template")

type Template struct {
	Version          string `json:"version"`
	Title            string `json:"title"`
	SuggestsDeadline bool   `json:"suggests_deadline"`
}

var templates = [...]Template{
	{Version: "exam:1", Title: "Подготовиться к экзамену", SuggestsDeadline: true},
	{Version: "practice:1", Title: "Закрепить темы на практике"},
}

func Templates() []Template { return slices.Clone(templates[:]) }

func ValidateTemplate(version string) error {
	if version == "" {
		return nil
	}
	for _, item := range templates {
		if item.Version == version {
			return nil
		}
	}

	return ErrUnknownTemplate
}
