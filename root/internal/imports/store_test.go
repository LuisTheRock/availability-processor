package imports

import (
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisStore(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set (docker compose up -d provides Redis)")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	s := newRedisStore(rdb)

	id := "test-" + time.Now().Format(time.RFC3339Nano)
	t.Cleanup(func() { rdb.Del(t.Context(), jobKey(id), jobKey(id)+":errors") })
	now := time.Now().UTC()

	save := func(sn Snapshot, errs ...RowError) {
		t.Helper()
		if err := s.Save(t.Context(), sn, errs); err != nil {
			t.Fatal(err)
		}
	}

	save(Snapshot{ID: id, State: StateRunning, Total: 10, Processed: 2, Failed: 2, CreatedAt: now, UpdatedAt: now},
		RowError{1, "INVALID", "a"}, RowError{2, "UNKNOWN_PRODUCT", "b"})
	save(Snapshot{ID: id, State: StateCompletedWithErrors, Total: 10, Processed: 10, Succeeded: 7, Failed: 3, CreatedAt: now, UpdatedAt: now},
		RowError{9, "PUBLISH_FAILED", "c"})

	sn, errs, err := s.Load(t.Context(), id, 100)
	if err != nil {
		t.Fatal(err)
	}
	if sn.State != StateCompletedWithErrors || sn.Processed != 10 || sn.Succeeded != 7 || len(errs) != 3 {
		t.Errorf("loaded %+v with %d errors", sn, len(errs))
	}
	if _, errs, _ := s.Load(t.Context(), id, 0); len(errs) != 0 {
		t.Errorf("errorsLimit 0 returned %d errors, want none (LRANGE 0 -1 is the whole list)", len(errs))
	}
	if _, errs, _ := s.Load(t.Context(), id, 2); len(errs) != 2 {
		t.Errorf("errorsLimit 2 returned %d errors", len(errs))
	}
	if _, _, err := s.Load(t.Context(), "does-not-exist", 10); err != ErrJobNotFound {
		t.Errorf("err = %v, want ErrJobNotFound", err)
	}
}

func TestRunningJobWithOldUpdateLoadsAsInterrupted(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set (docker compose up -d provides Redis)")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	s := newRedisStore(rdb)

	id := "stale-" + time.Now().Format(time.RFC3339Nano)
	t.Cleanup(func() { rdb.Del(t.Context(), jobKey(id)) })
	old := time.Now().UTC().Add(-2 * staleAfter)
	if err := s.Save(t.Context(), Snapshot{ID: id, State: StateRunning, Total: 5, Processed: 3, CreatedAt: old, UpdatedAt: old}, nil); err != nil {
		t.Fatal(err)
	}

	sn, _, err := s.Load(t.Context(), id, 10)
	if err != nil || sn.State != StateInterrupted || sn.Processed != 3 {
		t.Errorf("loaded %+v, %v; want INTERRUPTED with partial counts", sn, err)
	}
}
