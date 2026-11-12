package upstream

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func resp(status int, body string, headers ...string) *http.Response {
	h := http.Header{}
	for i := 0; i+1 < len(headers); i += 2 {
		h.Set(headers[i], headers[i+1])
	}
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(body))}
}

// clientWith returns a client whose transport replays the given responses
// and counts attempts. No sockets: the retry waits run on synctest's fake clock.
func clientWith(attempts *int, responses ...*http.Response) *Client {
	c := New("http://dep")
	c.http.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		r := responses[min(*attempts, len(responses)-1)]
		*attempts++
		return r, nil
	})
	return c
}

func TestRetries(t *testing.T) {
	tests := []struct {
		name         string
		responses    []*http.Response
		wantErr      error
		wantAttempts int
		minElapsed   time.Duration
	}{
		{"503 then success", []*http.Response{resp(503, ""), resp(200, `{"productId":"p"}`)}, nil, 2, 0},
		{
			"429 honours Retry-After then gives up",
			[]*http.Response{resp(429, "", "Retry-After", "5")}, nil, 3, 10 * time.Second,
		},
		{"404 is permanent", []*http.Response{resp(404, "")}, ErrNotFound, 1, 0},
		{"400 is permanent", []*http.Response{resp(400, "")}, nil, 1, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var attempts int
				c := clientWith(&attempts, tt.responses...)

				start := time.Now()
				_, err := c.ProductContext(t.Context(), "p", "s", "PT")

				if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
					t.Errorf("err = %v, want %v", err, tt.wantErr)
				}
				// "no error expected" only for the success case; the others fail with some error
				if tt.wantAttempts == 2 && err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if tt.wantAttempts == 3 && err == nil {
					t.Error("expected an error after exhausting attempts")
				}
				if attempts != tt.wantAttempts {
					t.Errorf("attempts = %d, want %d", attempts, tt.wantAttempts)
				}
				if elapsed := time.Since(start); elapsed < tt.minElapsed {
					t.Errorf("waited %v, want >= %v", elapsed, tt.minElapsed)
				}
			})
		})
	}
}

func TestRetryStopsWhenContextIsCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts int
		c := clientWith(&attempts, resp(503, ""))

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { _, err := c.Snapshot(ctx, "PT"); done <- err }()

		synctest.Wait() // first attempt failed; the client is now waiting to retry
		cancel()

		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
		if attempts != 1 {
			t.Errorf("attempts = %d, want 1", attempts)
		}
	})
}

func TestFailFast429(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts int
		c := clientWith(&attempts, resp(429, "", "Retry-After", "5"))

		start := time.Now()
		_, err := c.ProductContext(WithFailFast429(t.Context()), "p", "s", "PT")

		var rl *RateLimitedError
		if !errors.As(err, &rl) || rl.RetryAfter != 5*time.Second {
			t.Fatalf("err = %v, want *RateLimitedError{5s}", err)
		}
		if attempts != 1 || time.Since(start) != 0 {
			t.Errorf("attempts = %d, waited %v; want 1 attempt, no wait", attempts, time.Since(start))
		}
	})
}

// Publish must retry a 429 in-process even when the caller asked to fail fast:
// the stock write is already recorded, so a caller retry would be skipped as a
// duplicate and the event lost.
func TestPublishIgnoresFailFast(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts int
		c := clientWith(&attempts, resp(429, "", "Retry-After", "5"), resp(202, ""))

		start := time.Now()
		err := c.Publish(WithFailFast429(t.Context()), map[string]string{"eventId": "e"})

		if err != nil {
			t.Fatalf("Publish = %v, want success after the retry", err)
		}
		if attempts != 2 || time.Since(start) != 5*time.Second {
			t.Errorf("attempts = %d, waited %v; want 2 attempts and the 5s Retry-After", attempts, time.Since(start))
		}
	})
}

// The 5s per-attempt limit is for single calls; a country snapshot download
// gets a longer one, or a slow network would fail the whole import.
func TestSnapshotHasALongerTimeoutThanSingleCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts int
		c := New("http://dep")
		c.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			attempts++
			select { // a response that takes 10s to arrive
			case <-time.After(10 * time.Second):
				return resp(200, "[]"), nil
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		})

		if _, err := c.Snapshot(t.Context(), "PT"); err != nil {
			t.Errorf("Snapshot = %v, want success: 10s is within its timeout", err)
		}
		if attempts != 1 {
			t.Errorf("snapshot attempts = %d, want 1", attempts)
		}

		attempts = 0
		if _, err := c.ProductContext(t.Context(), "p", "s", "PT"); err == nil {
			t.Error("ProductContext succeeded, want the 5s per-attempt timeout")
		}
		if attempts != maxAttempts {
			t.Errorf("timed-out single call attempts = %d, want %d (a timeout is retried)", attempts, maxAttempts)
		}
	})
}
