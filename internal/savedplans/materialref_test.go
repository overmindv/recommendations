package savedplans

import (
	"testing"

	"github.com/overmindv/recommendations/internal/materialref"
)

func TestPublishedMaterialLinksAreCanonical(t *testing.T) {
	id := "11111111-1111-4111-8111-111111111111"
	revision := "22222222-2222-4222-8222-222222222222"
	right := Material{ID: materialref.Encode(id, revision), URL: materialref.Path(id)}
	if !validMaterialURL(right) {
		t.Fatal("exact material path was rejected")
	}
	for _, path := range []string{"//other.test/materials/" + id, "/materials/../" + id, right.URL + "?draft=true", "https://other.test/"} {
		wrong := right
		wrong.URL = path
		if validMaterialURL(wrong) {
			t.Error("managed material allowed a different URL:", path)
		}
	}
	if validMaterialURL(Material{ID: "content:broken", URL: right.URL}) ||
		validMaterialURL(Material{ID: "external", URL: right.URL}) {
		t.Fatal("relative URL accepted without exact published revision")
	}
}
