package aiplan

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/overmindv/recommendations/internal/planner"
)

func TestOutlineUsesOnlyCatalogIDsAndSharedCache(t *testing.T) {
	in := OutlineRequest{Goal: "Junior AI", Topics: []planner.Topic{{ID: testID(1), Title: "Python"}}}
	calls := 0
	e := New(rankFunc(func(_ context.Context, b []byte) ([]int, error) {
		calls++
		if strings.Contains(string(b), testID(1)) {
			t.Fatal("UUID leak")
		}
		return []int{1, 2, 5}, nil
	}))
	out, err := e.Outline(context.Background(), in, "user-a")
	if err != nil || out.AIStatus != "GENERATED" || len(out.TopicIDs) != 1 || len(out.Materials) != 2 || out.Materials[0].ID != "python" {
		t.Fatalf("bad outline: %+v %v", out, err)
	}
	out, err = e.Outline(context.Background(), in, "user-a")
	if err != nil || out.AIStatus != "CACHED" || calls != 1 {
		t.Fatal("cache miss")
	}
	_, _ = e.Outline(context.Background(), in, "user-b")
	if calls != 2 {
		t.Fatal("actor isolation")
	}
	in.Topics[0].Title = "SQL"
	_, _ = e.Outline(context.Background(), in, "user-a")
	if calls != 3 {
		t.Fatal("catalog invalidation")
	}
}
func TestOutlineRejectsInvalidSelections(t *testing.T) {
	for _, ids := range [][]int{nil, {0}, {99}, {1, 1}, {1, 2, 3, 4, 5, 6, 7, 8, 9}} {
		e := New(rankFunc(func(context.Context, []byte) ([]int, error) { return ids, nil }))
		out, err := e.Outline(context.Background(), OutlineRequest{Goal: "ML"}, "a")
		if err != nil || out.AIStatus != "UNAVAILABLE" || len(out.Materials) != 0 {
			t.Fatalf("accepted invalid IDs: %+v", out)
		}
	}
	e := New(rankFunc(func(context.Context, []byte) ([]int, error) { return []int{}, nil }))
	out, err := e.Outline(context.Background(), OutlineRequest{Goal: "unrelated"}, "a")
	if err != nil || out.AIStatus != "NO_MATCH" {
		t.Fatal(out, err)
	}
	for _, goal := range []string{"", strings.Repeat("я", 1001)} {
		if _, err = e.Outline(context.Background(), OutlineRequest{Goal: goal}, "a"); err == nil {
			t.Fatal("invalid goal")
		}
	}
}

// TestLiveOutline запускается вручную после настройки бесплатных аккаунтов; ключи не печатает.
func TestLiveOutline(t *testing.T) {
	if os.Getenv("AI_LIVE_TEST") != "true" {
		t.Skip("manual free API smoke")
	}
	e, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	in := OutlineRequest{Goal: "Подготовь меня к собеседованию на Junior AI-инженера", Topics: []planner.Topic{{ID: testID(1), Title: "Python"}, {ID: testID(2), Title: "Машинное обучение"}, {ID: testID(3), Title: "NLP"}}}
	out, err := e.Outline(context.Background(), in, "manual-smoke")
	if err != nil || out.AIStatus != "GENERATED" || len(out.Materials) == 0 {
		t.Fatalf("live outline status=%s error=%v", out.AIStatus, err)
	}
	safe, _ := json.Marshal(struct {
		Status    string
		Topics    int
		Materials []Material
	}{out.AIStatus, len(out.TopicIDs), out.Materials})
	t.Log(string(safe))
}

func TestOutlineSkipsCompletedMaterialsAndInvalidatesCache(t *testing.T) {
	calls := 0
	e := New(rankFunc(func(_ context.Context, b []byte) ([]int, error) {
		calls++
		if strings.Contains(string(b), `"completed":true`) {
			return []int{2}, nil
		}
		return []int{1}, nil
	}))
	in := OutlineRequest{Goal: "Python NumPy"}
	first, err := e.Outline(context.Background(), in, "a")
	if err != nil || first.Materials[0].ID != "python" {
		t.Fatal(first, err)
	}
	in.CompletedMaterialIDs = []string{"python"}
	next, err := e.Outline(context.Background(), in, "a")
	if err != nil || next.Materials[0].ID != "numpy" || calls != 2 {
		t.Fatal(next, err)
	}
	bad := New(rankFunc(func(context.Context, []byte) ([]int, error) { return []int{1}, nil }))
	out, err := bad.Outline(context.Background(), in, "a")
	if err != nil || out.AIStatus != "UNAVAILABLE" {
		t.Fatal("completed recommendation accepted")
	}
}
