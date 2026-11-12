package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"availability-processor/internal/startup"
	"availability-processor/internal/web"

	"github.com/redis/go-redis/v9"
)

type fakeChecker struct{ err error }

func (f fakeChecker) Check(context.Context) error { return f.err }

func TestReadiness(t *testing.T) {
	tests := []struct {
		name     string
		checkers map[string]Checker
		want     int
	}{
		{"all dependencies up", map[string]Checker{"redis": fakeChecker{}}, http.StatusOK},
		{"a dependency is down", map[string]Checker{"redis": fakeChecker{errors.New("boom")}}, http.StatusServiceUnavailable},
		{"one of two is down", map[string]Checker{"a": fakeChecker{}, "b": fakeChecker{errors.New("boom")}}, http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := startup.NewState()
			state.MarkReady()
			h := NewHandlers(state, tt.checkers)

			rec := httptest.NewRecorder()
			web.Make(h.HandleReadiness)(rec, httptest.NewRequest(http.MethodGet, "/health/readiness", nil))

			if rec.Code != tt.want {
				t.Errorf("status = %d (%s), want %d", rec.Code, rec.Body, tt.want)
			}
		})
	}
}

// The checker wired in production must report an unreachable Redis as an error.
func TestRedisCheckerReportsUnreachableRedis(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: time.Second})
	t.Cleanup(func() { _ = rdb.Close() })

	if err := (redisChecker{rdb}).Check(t.Context()); err == nil {
		t.Error("Check = nil for an unreachable Redis, want an error")
	}
}
