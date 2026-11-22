package replay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var ErrSnapshot = errors.New("invalid or stale evidence replay")

const MaxEvents = 100000

type Manifest struct {
	Watermark     int64     `json:"watermark"`
	PrivacyEpoch  int64     `json:"privacy_epoch"`
	Count         int64     `json:"count"`
	SHA256        string    `json:"sha256"`
	LegacyMissing int64     `json:"legacy_missing"`
	CompleteSince time.Time `json:"complete_since"`
}

func (m Manifest) Valid() bool {
	hash, err := hex.DecodeString(m.SHA256)

	return err == nil && len(hash) == 32 && m.Watermark >= 0 && m.PrivacyEpoch > 0 && m.Count >= 0 && m.Count <= MaxEvents && m.LegacyMissing == 0 && !m.CompleteSince.IsZero()
}

type Entry struct {
	Position int64           `json:"position"`
	SHA256   string          `json:"sha256"`
	Event    json.RawMessage `json:"event"`
}
type Page struct {
	Watermark    int64   `json:"watermark"`
	PrivacyEpoch int64   `json:"privacy_epoch"`
	Through      int64   `json:"through"`
	More         bool    `json:"more"`
	Events       []Entry `json:"events"`
}
type Source interface {
	Manifest(context.Context, int64, int64) (Manifest, error)
	Page(context.Context, Manifest, int64) (Page, error)
}
type Client struct {
	url, token string
	http       *http.Client
}

func NewClient(address, token string) (*Client, error) {
	u, err := url.Parse(address)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, ErrSnapshot
	}
	local := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if u.Scheme != "https" && (u.Scheme != "http" || !local) || len(token) < 32 || len(token) > 512 || strings.ContainsFunc(token, func(r rune) bool { return r < 33 || r > 126 }) {
		return nil, ErrSnapshot
	}

	return &Client{url: strings.TrimRight(address, "/"), token: token, http: &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) get(ctx context.Context, path string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	response, err := c.http.Do(req)
	if err != nil {
		return errors.New("replay source unavailable")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return ErrSnapshot
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		return ErrSnapshot
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil || d.Decode(new(any)) != io.EOF {
		return ErrSnapshot
	}

	return nil
}
func (c *Client) Manifest(ctx context.Context, wm, epoch int64) (Manifest, error) {
	var m Manifest
	err := c.get(ctx, "/v1/internal/outcomes/manifest?watermark="+strconv.FormatInt(wm, 10)+"&privacy_epoch="+strconv.FormatInt(epoch, 10), &m)
	if err == nil && !m.Valid() {
		err = ErrSnapshot
	}

	return m, err
}
func (c *Client) Page(ctx context.Context, m Manifest, after int64) (Page, error) {
	var p Page
	err := c.get(ctx, "/v1/internal/outcomes/replay?limit=100&watermark="+strconv.FormatInt(m.Watermark, 10)+"&privacy_epoch="+strconv.FormatInt(m.PrivacyEpoch, 10)+"&after="+strconv.FormatInt(after, 10), &p)

	return p, err
}
func Digest(raw []byte) ([]byte, error) {
	var value any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&value) != nil || d.Decode(new(any)) != io.EOF {
		return nil, ErrSnapshot
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(canonical)

	return sum[:], nil
}
