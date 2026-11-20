package config

import (
	"strings"
	"testing"
)

func cleanEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"HTTP_ADDRESS", "HTTP_ADDR", "PORT"} {
		t.Setenv(key, "")
	}
	t.Setenv("RECOMMENDATIONS_SERVICE_TOKEN", strings.Repeat("s", 32))
}

//nolint:paralleltest // t.Setenv изменяет process-wide environment
func TestAddressPrecedence(t *testing.T) {

	cleanEnv(t)
	check := func(want string) {
		t.Helper()
		cfg, err := Load()
		if err != nil || cfg.HTTPAddress != want {
			t.Fatalf("want %s, got %+v %v", want, cfg, err)
		}
	}
	check(":8080")
	t.Setenv("PORT", "8088")
	check(":8088")
	t.Setenv("HTTP_ADDR", ":8089")
	check(":8089")
	t.Setenv("HTTP_ADDRESS", "127.0.0.1:8090")
	check("127.0.0.1:8090")
}

//nolint:paralleltest // t.Setenv изменяет process-wide environment и subtests выполняются последовательно
func TestInvalidConfig(t *testing.T) {

	for _, value := range []string{"", "short", strings.Repeat("s", 4097), strings.Repeat("s", 32) + "\n"} {
		t.Run("token", func(t *testing.T) {

			cleanEnv(t)
			t.Setenv("RECOMMENDATIONS_SERVICE_TOKEN", value)
			if _, err := Load(); err == nil {
				t.Fatal("invalid token accepted")
			}
		})
	}
	for _, value := range []string{"0", "-1", "65536", "bad", "8080/path"} {
		t.Run("port_"+value, func(t *testing.T) {

			cleanEnv(t)
			t.Setenv("PORT", value)
			if _, err := Load(); err == nil {
				t.Fatal("invalid port accepted")
			}
		})
	}
	cleanEnv(t)
	t.Setenv("HTTP_ADDRESS", "http://localhost:8080")
	if _, err := Load(); err == nil {
		t.Fatal("URL accepted as address")
	}
}
