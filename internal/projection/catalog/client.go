package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxPageBytes = 2 << 20

type Source interface {
	Snapshot(context.Context, *int64, string) (SnapshotPage, error)
	Changes(context.Context, *int64, int64) (ChangePage, error)
}

type Client struct {
	base  string
	token string
	http  *http.Client
}

func NewClient(base, token string) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") ||
		u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, errors.New("catalog projection requires an absolute ENTITIES_URL without credentials, query or fragment")
	}
	if len(token) < 32 || len(token) > 512 || strings.ContainsFunc(token, func(r rune) bool { return r < 0x21 || r > 0x7e }) {
		return nil, errors.New("ENTITIES_SNAPSHOT_TOKEN must contain 32-512 printable ASCII characters without spaces")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxConnsPerHost = 2
	transport.MaxIdleConnsPerHost = 2
	transport.MaxResponseHeaderBytes = 16 << 10
	return &Client{
		base:  strings.TrimRight(base, "/") + "/v1/internal/catalog",
		token: token,
		http: &http.Client{
			Timeout:   8 * time.Second,
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (c *Client) Snapshot(ctx context.Context, watermark *int64, cursor string) (SnapshotPage, error) {
	params := pageParams(watermark)
	if cursor != "" {
		params.Set("cursor", cursor)
	}
	var page SnapshotPage
	err := c.read(ctx, "/snapshot", params, &page)
	return page, err
}

func (c *Client) Changes(ctx context.Context, watermark *int64, after int64) (ChangePage, error) {
	params := pageParams(watermark)
	params.Set("after", strconv.FormatInt(after, 10))
	var page ChangePage
	err := c.read(ctx, "/changes", params, &page)
	return page, err
}

func pageParams(watermark *int64) url.Values {
	params := url.Values{"limit": {strconv.Itoa(PageLimit)}}
	if watermark != nil {
		params.Set("watermark", strconv.FormatInt(*watermark, 10))
	}
	return params
}

func (c *Client) read(ctx context.Context, path string, params url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path+"?"+params.Encode(), nil)
	if err != nil {
		return errors.New("invalid catalog page request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return errors.New("catalog page request failed")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("catalog page returned HTTP %d", resp.StatusCode)
	}
	limited := &io.LimitedReader{R: resp.Body, N: maxPageBytes + 1}
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(out); err != nil {
		return errors.New("invalid source catalog JSON")
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) || limited.N == 0 {
		return errors.New("invalid source catalog JSON")
	}
	return nil
}
