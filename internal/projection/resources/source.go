package resources

import (
	"errors"
	"strings"

	"github.com/overmindv/recommendations/internal/planner"
)

const pageLimit = 100

var (
	errInvalidPage    = errors.New("invalid resource source page")
	errInvalidEvent   = errors.New("invalid resource source event")
	errSourcePosition = errors.New("resource source no longer has that position")
)

type SnapshotPage struct {
	Watermark  int64
	NextCursor string
	Records    []Record
}

type ChangePage struct {
	Watermark int64
	Through   int64
	More      bool
	Events    []Event
}

type Event struct {
	ID          string
	Fingerprint [32]byte
	Record      Record
}

type VersionKey struct {
	ID         string
	RevisionID string
}

func validateSnapshot(page SnapshotPage, watermark *int64, cursor string) error {
	if page.Watermark < 0 || (watermark != nil && page.Watermark != *watermark) || len(page.Records) > pageLimit {
		return errInvalidPage
	}
	last := cursor
	for _, record := range page.Records {
		if record.ID <= last || record.Version > page.Watermark {
			return errInvalidPage
		}
		last = record.ID
	}
	if page.NextCursor != "" && (len(page.Records) == 0 || page.NextCursor != last) {
		return errInvalidPage
	}
	return nil
}

func validateVersionKeys(keys []VersionKey) (map[VersionKey]bool, error) {
	if len(keys) == 0 || len(keys) > pageLimit {
		return nil, errInvalidPage
	}
	requested := make(map[VersionKey]bool, len(keys))
	for _, key := range keys {
		if _, duplicate := requested[key]; duplicate || !planner.ValidID(key.ID) || !planner.ValidID(key.RevisionID) {
			return nil, errInvalidPage
		}
		requested[key] = false
	}
	return requested, nil
}

func confirmedVersion(requested map[VersionKey]bool, key VersionKey) error {
	if confirmed, exists := requested[key]; !exists || confirmed {
		return errInvalidPage
	}
	requested[key] = true
	return nil
}

func validToken(token string, max int) bool {
	return len(token) >= 32 && len(token) <= max &&
		!strings.ContainsFunc(token, func(r rune) bool { return r < 0x21 || r > 0x7e })
}
