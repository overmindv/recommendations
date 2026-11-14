package aiplan

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
}
func TestHTTPProviderFreePriceAndPayload(t *testing.T) {
	p := NewHTTPProvider("openrouter", "example/model:free", "secret")
	calls := 0
	p.client.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method == "GET" {
			return response(200, `{"data":[{"id":"example/model:free","pricing":{"prompt":"0","completion":"0"}}]}`), nil
		}
		b, _ := io.ReadAll(r.Body)
		s := string(b)
		if r.Header.Get("Authorization") != "Bearer secret" || !strings.Contains(s, `"max_price":{"completion":0,"prompt":0}`) || !strings.Contains(s, `"allow_fallbacks":false`) || strings.Contains(s, `"tools"`) {
			t.Error("unsafe request")
		}
		return response(200, `{"choices":[{"finish_reason":"stop","message":{"content":"{\"order\":[2,1]}"}}]}`), nil
	})
	ids, err := p.Rank(context.Background(), []byte(`{}`))
	if err != nil || len(ids) != 2 || ids[0] != 2 || calls != 2 {
		t.Fatalf("bad rank %v %v %d", ids, err, calls)
	}
}
func TestPaidPriceFailsClosed(t *testing.T) {
	p := NewHTTPProvider("openrouter", "example/model:free", "secret")
	p.client.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.Method != "GET" {
			t.Fatal("paid call made")
		}
		return response(200, `{"data":[{"id":"example/model:free","pricing":{"prompt":"0.01","completion":"0"}}]}`), nil
	})
	if _, err := p.Rank(context.Background(), nil); err == nil {
		t.Fatal("paid model accepted")
	}
}
func TestProviderCooldownAuthAndInvalidResponses(t *testing.T) {
	for _, status := range []int{401, 403, 429, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			p := NewHTTPProvider("groq", "test", "secret")
			calls := 0
			p.client.Transport = roundTrip(func(*http.Request) (*http.Response, error) {
				calls++
				r := response(status, "private error")
				r.Header.Set("Retry-After", "120")
				return r, nil
			})
			_, _ = p.Rank(context.Background(), nil)
			_, _ = p.Rank(context.Background(), nil)
			if calls != 1 {
				t.Fatal("cooldown ignored")
			}
		})
	}
	for _, body := range []string{`{"choices":[]}`, `{"choices":[{"finish_reason":"length","message":{"content":"{}"}}]}`, `{"choices":[{"finish_reason":"stop","message":{"content":"{\"order\":[1]} extra"}}]}`} {
		p := NewHTTPProvider("groq", "test", "secret")
		p.client.Transport = roundTrip(func(*http.Request) (*http.Response, error) { return response(200, body), nil })
		if _, err := p.Rank(context.Background(), nil); err == nil {
			t.Fatal("invalid output accepted")
		}
	}
	if retryDelay("120") != 2*time.Minute {
		t.Fatal("retry-after ignored")
	}
}
func TestConfigurationFailsClosed(t *testing.T) {
	for _, key := range []string{"AI_ENABLED", "AI_FREE_TIER_CONFIRMED", "GEMINI_API_KEY", "GEMINI_MODEL", "GROQ_API_KEY", "GROQ_MODEL", "OPENROUTER_API_KEY", "OPENROUTER_MODEL"} {
		t.Setenv(key, "")
	}
	if e, err := FromEnv(); err != nil || len(e.providers) != 0 {
		t.Fatal("disabled config")
	}
	t.Setenv("AI_ENABLED", "true")
	if _, err := FromEnv(); err == nil {
		t.Fatal("free account unconfirmed")
	}
	t.Setenv("AI_FREE_TIER_CONFIRMED", "true")
	t.Setenv("OPENROUTER_API_KEY", "secret")
	t.Setenv("OPENROUTER_MODEL", "paid/model")
	if _, err := FromEnv(); err == nil {
		t.Fatal("paid config accepted")
	}
	t.Setenv("OPENROUTER_MODEL", "example/model:free")
	if e, err := FromEnv(); err != nil || len(e.providers) != 1 {
		t.Fatal("free config rejected")
	}
}
