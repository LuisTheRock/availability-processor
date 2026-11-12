package availability

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func redisVersions(t *testing.T) *RedisVersions {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set (docker compose up -d provides Redis)")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewRedisVersions(rdb)
}

func uniqueKey(t *testing.T) Key {
	return Key{Seller: t.Name() + time.Now().Format(time.RFC3339Nano), Product: "p", Size: "s", Country: "PT"}
}

func TestRedisRecordIsCompareAndSet(t *testing.T) {
	r, k := redisVersions(t), uniqueKey(t)

	for _, step := range []struct {
		version int64
		want    bool
	}{{8, true}, {7, false}, {8, false}, {9, true}} {
		got, err := r.Record(t.Context(), k, step.version)
		if err != nil || got != step.want {
			t.Fatalf("Record(%d) = %v, %v; want %v", step.version, got, err, step.want)
		}
	}
	if v, ok, _ := r.Applied(t.Context(), k); !ok || v != 9 {
		t.Errorf("Applied = %d, %v; want 9", v, ok)
	}
}

func TestRedisLockExcludesSameKeyOnly(t *testing.T) {
	r, k := redisVersions(t), uniqueKey(t)

	var inside, overlaps atomic.Int32
	done := make(chan struct{})
	for range 2 {
		go func() {
			defer func() { done <- struct{}{} }()
			unlock, err := r.Lock(t.Context(), k)
			if err != nil {
				t.Error(err)
				return
			}
			if inside.Add(1) > 1 {
				overlaps.Add(1)
			}
			time.Sleep(50 * time.Millisecond) // real Redis: the critical section needs real time
			inside.Add(-1)
			unlock()
		}()
	}
	<-done
	<-done
	if overlaps.Load() != 0 {
		t.Error("two holders were inside the lock at once")
	}

	other := Key{Seller: k.Seller + "-other", Product: "p", Size: "s", Country: "PT"}
	unlock, err := r.Lock(t.Context(), k)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	unlockOther, err := r.Lock(t.Context(), other) // must not block on k
	if err != nil {
		t.Fatal(err)
	}
	unlockOther()
}
