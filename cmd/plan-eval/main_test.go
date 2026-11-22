package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRunExitCodes(t *testing.T) {
	t.Parallel()

	const fixture = "../../evaluation/algorithms.json"
	var stdout, stderr bytes.Buffer
	if code := run(t.Context(), []string{"-cases", fixture}, &stdout, &stderr); code != 0 || stderr.Len() != 0 {
		t.Fatal("fixture did not pass:", code, stderr.String())
	}
	var report struct {
		Passed bool `json:"passed"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil || !report.Passed {
		t.Fatal("missing JSON report:", err)
	}
	dir := t.TempDir()
	broken, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	broken = bytes.Replace(broken, []byte(`"scheduled_checks": ["arrays", "binary-search"]`),
		[]byte(`"gaps": [{"topic": "arrays", "reason": "missing_resource"}]`), 1)
	path := filepath.Join(dir, "regression.json")
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	if writeErr := root.WriteFile("regression.json", broken, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	stdout.Reset()
	if code := run(t.Context(), []string{"-cases", path}, &stdout, &stderr); code != 1 || stdout.Len() == 0 {
		t.Fatal("regression did not produce a failing report:", code, stderr.String())
	}
	stdout.Reset()
	if code := run(t.Context(), []string{"-cases", dir}, &stdout, &stderr); code != 2 || stdout.Len() != 0 {
		t.Fatal("invalid input returned a report:", code)
	}
}
