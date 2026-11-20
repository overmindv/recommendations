package aiplan

import (
	"errors"
	"os"
	"strings"
)

const (
	enabledValue         = "true"
	providerOpenRouter   = "openrouter"
	materialPython       = "python"
	aiStatusUnavailable  = "UNAVAILABLE"
	aiStatusCached       = "CACHED"
	aiStatusLimitReached = "LIMIT_REACHED"
	aiStatusGenerated    = "GENERATED"
	aiStatusRefused      = "REFUSED"
	aiStatusNoMatch      = "NO_MATCH"
)

// FromEnv не подключает платный fallback и не позволяет произвольный endpoint.
func FromEnv() (*Engine, error) {
	if os.Getenv("AI_ENABLED") != enabledValue {
		return New(), nil
	}
	if os.Getenv("AI_FREE_TIER_CONFIRMED") != enabledValue {
		return nil, errors.New("AI_FREE_TIER_CONFIRMED must be true for accounts without paid billing")
	}
	providers := []Provider{}
	for _, name := range []string{"gemini", "groq", providerOpenRouter} {
		prefix := strings.ToUpper(name)
		key := strings.TrimSpace(os.Getenv(prefix + "_API_KEY"))
		model := strings.TrimSpace(os.Getenv(prefix + "_MODEL"))
		if key == "" && model == "" {
			continue
		}
		if key == "" || model == "" || len(model) > 200 || strings.ContainsAny(model, " \t\r\n") || strings.ContainsAny(key, " \t\r\n") {
			return nil, errors.New("AI provider requires a valid key and explicit model")
		}
		if name == providerOpenRouter && !strings.HasSuffix(model, ":free") {
			return nil, errors.New("OPENROUTER_MODEL must be an explicit :free model")
		}
		providers = append(providers, NewHTTPProvider(name, model, key))
	}
	if len(providers) == 0 {
		return nil, errors.New("AI_ENABLED requires at least one configured free provider")
	}

	return New(providers...), nil
}
