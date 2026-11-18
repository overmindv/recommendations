package topiccatalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/planner"
	"golang.org/x/sync/errgroup"
)

const (
	maxParallelTopics = 4
	maxTopicResponse  = 64 << 10
	catalogTimeout    = 2 * time.Second
)

var ErrTopicUnavailable = errors.New("topic is not published")

type Client struct {
	base  string
	http  *http.Client
	slots chan struct{}
}

func New(base string) (*Client, error) {
	if base != "" {
		u, err := url.Parse(base)
		if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") ||
			u.User != nil || u.Opaque != "" || u.ForceQuery || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("ENTITIES_URL must be an absolute HTTP or HTTPS URL without credentials, query or fragment")
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxConnsPerHost = 32
	transport.MaxIdleConnsPerHost = 32
	transport.MaxResponseHeaderBytes = 16 << 10
	return &Client{
		base:  strings.TrimRight(base, "/"),
		slots: make(chan struct{}, 32),
		http: &http.Client{
			Timeout:   catalogTimeout,
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (c *Client) Validate(ctx context.Context, ids []string) error {
	if len(ids) == 0 || len(ids) > learnergoals.MaxTargetTopics {
		return learnergoals.ErrInvalid
	}
	for _, id := range ids {
		if !planner.ValidID(id) {
			return learnergoals.ErrInvalid
		}
	}
	if c.base == "" {
		return errors.New("topic catalog is not configured")
	}
	deadline, cancel := context.WithTimeout(ctx, catalogTimeout)
	defer cancel()
	group, ctx := errgroup.WithContext(deadline)
	group.SetLimit(maxParallelTopics)
	for _, id := range ids {
		if ctx.Err() != nil {
			break
		}
		group.Go(func() error { return c.check(ctx, id) })
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return deadline.Err()
}

func (c *Client) check(ctx context.Context, id string) error {
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/topics/"+id, nil)
	if err != nil {
		return errors.New("invalid topic catalog request")
	}
	res, err := c.http.Do(req)
	if err != nil {
		return errors.New("topic catalog request failed")
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode == http.StatusNotFound {
		return ErrTopicUnavailable
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("topic catalog returned HTTP %d", res.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, maxTopicResponse+1))
	if err != nil || len(body) > maxTopicResponse {
		return errors.New("invalid topic catalog response")
	}
	var topic struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err = json.Unmarshal(body, &topic); err != nil || topic.ID != id || topic.Status == "" {
		return errors.New("invalid topic catalog response")
	}
	if topic.Status != "active" {
		return ErrTopicUnavailable
	}
	return nil
}
