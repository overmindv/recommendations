package materialref

import (
	"strings"

	"github.com/overmindv/recommendations/internal/planner"
)

const prefix = "content:"

func Encode(id, revision string) string {
	return prefix + id + ":" + revision
}

func Owned(value string) bool {
	return strings.HasPrefix(value, prefix)
}

func Decode(value string) (id, revision string, ok bool) {
	ref, managed := strings.CutPrefix(value, prefix)
	id, revision, separated := strings.Cut(ref, ":")
	return id, revision, managed && separated && planner.ValidID(id) && planner.ValidID(revision)
}

func Path(id string) string {
	return "/materials/" + id
}
