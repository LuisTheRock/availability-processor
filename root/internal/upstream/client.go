// Package upstream is the HTTP client for the Product Context API, the
// Availability API Gateway and the output-event sink.
package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	callTimeout     = 5 * time.Second  // per attempt, single calls
	snapshotTimeout = 60 * time.Second // per attempt, a full country snapshot
	maxAttempts     = 3
	maxRetryAfter   = 10 * time.Second
	baseBackoff     = 200 * time.Millisecond
)

// ErrNotFound is returned when the Product Context API answers 404.
var ErrNotFound = errors.New("not found")

// RateLimitedError is returned for a 429 when the caller asked to fail fast.
type RateLimitedError struct{ RetryAfter time.Duration }

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("rate limited, retry after %s", e.RetryAfter)
}

type failFastKey struct{}

// WithFailFast429 makes calls made with ctx return a *RateLimitedError on 429
// instead of sleeping through Retry-After: for callers that can push the
// retry back to their own caller (the synchronous event endpoint).
// Publish ignores it: once the stock write is recorded, a caller retry would
// be skipped as a duplicate and the event would be lost.
func WithFailFast429(ctx context.Context) context.Context {
	return context.WithValue(ctx, failFastKey{}, true)
}

func failFast(ctx context.Context) bool {
	v, _ := ctx.Value(failFastKey{}).(bool)
	return v
}

type Named struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type ProductContext struct {
	ProductID      string `json:"productId"`
	ProductName    string `json:"productName"`
	Brand          Named  `json:"brand"`
	Category       Named  `json:"category"`
	Color          Named  `json:"color"`
	ProductStatus  string `json:"productStatus"`
	SizeID         string `json:"sizeId"`
	SizeLabel      string `json:"sizeLabel"`
	SizeSystem     string `json:"sizeSystem"`
	CountryCode    string `json:"countryCode"`
	CountryEnabled bool   `json:"countryEnabled"`
}

type Client struct {
	baseURL string
	http    *http.Client
}

func New(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{}} // timeouts are per attempt, set in do
}

func (c *Client) ProductContext(ctx context.Context, productID, sizeID, country string) (ProductContext, error) {
	path := fmt.Sprintf("/v1/products/%s/sizes/%s?countryCode=%s",
		url.PathEscape(productID), url.PathEscape(sizeID), url.QueryEscape(country))

	var pc ProductContext
	if err := c.do(ctx, http.MethodGet, path, nil, &pc, callTimeout); err != nil {
		return ProductContext{}, fmt.Errorf("looking up context %s/%s/%s: %w", productID, sizeID, country, err)
	}
	return pc, nil
}

func (c *Client) Snapshot(ctx context.Context, country string) ([]ProductContext, error) {
	var out []ProductContext
	if err := c.do(ctx, http.MethodGet, "/v1/product-contexts/snapshot?countryCode="+url.QueryEscape(country), nil, &out, snapshotTimeout); err != nil {
		return nil, fmt.Errorf("fetching snapshot for %s: %w", country, err)
	}
	return out, nil
}

func (c *Client) PutAvailability(ctx context.Context, seller, productID, sizeID, country string, body any) error {
	path := fmt.Sprintf("/v1/availability/%s/%s/%s?countryCode=%s",
		url.PathEscape(seller), url.PathEscape(productID), url.PathEscape(sizeID), url.QueryEscape(country))

	if err := c.do(ctx, http.MethodPut, path, body, nil, callTimeout); err != nil {
		return fmt.Errorf("putting availability %s/%s/%s/%s: %w", seller, productID, sizeID, country, err)
	}
	return nil
}

func (c *Client) Publish(ctx context.Context, event any) error {
	ctx = context.WithValue(ctx, failFastKey{}, false) // always retry in-process
	if err := c.do(ctx, http.MethodPost, "/mock-broker/v1/events", event, nil, callTimeout); err != nil {
		return fmt.Errorf("publishing event: %w", err)
	}
	return nil
}

// do sends the request, retrying 429/503/network errors up to maxAttempts.
func (c *Client) do(ctx context.Context, method, path string, in, out any, timeout time.Duration) error {
	var payload []byte
	if in != nil {
		var err error
		if payload, err = json.Marshal(in); err != nil {
			return fmt.Errorf("encoding body: %w", err)
		}
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		wait, err := c.once(ctx, method, path, payload, out, timeout)
		if err == nil {
			return nil
		}
		lastErr = err
		if wait < 0 || attempt == maxAttempts { // permanent error, or out of attempts
			break
		}

		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return lastErr
}

// once performs one attempt. A negative wait means the error is permanent.
// The timeout covers sending and reading the whole body; a timed-out attempt
// is retryable, a cancelled caller context is not.
func (c *Client) once(ctx context.Context, method, path string, payload []byte, out any, timeout time.Duration) (time.Duration, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(callCtx, method, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return -1, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return -1, ctx.Err()
		}
		return backoff(0), err
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if out == nil {
			return 0, nil
		}
		return -1, json.NewDecoder(resp.Body).Decode(out)
	case resp.StatusCode == http.StatusNotFound:
		return -1, ErrNotFound
	case resp.StatusCode == http.StatusTooManyRequests && failFast(ctx):
		return -1, &RateLimitedError{RetryAfter: retryAfter(resp)}
	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode == http.StatusServiceUnavailable:
		_, _ = io.Copy(io.Discard, resp.Body)
		return backoff(retryAfter(resp)), fmt.Errorf("transient status %d", resp.StatusCode)
	default:
		return -1, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
}

func retryAfter(resp *http.Response) time.Duration {
	secs, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err != nil || secs < 0 {
		return 0
	}
	return min(time.Duration(secs)*time.Second, maxRetryAfter)
}

// backoff honors Retry-After when given, otherwise jittered exponential
// (the jitter alone is enough here: attempts are capped at 3).
func backoff(retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return retryAfter
	}
	return baseBackoff + rand.N(baseBackoff)
}
