package availability

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// The lease outlives the worst case of one Process call (3 attempts x
	// 5s timeouts + capped Retry-After waits, for lookup, PUT and publish).
	lockLease = 60 * time.Second
	lockWait  = 10 * time.Second
)

// recordScript is a compare-and-set: a lower version never overwrites a higher one.
var recordScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur and tonumber(cur) >= tonumber(ARGV[1]) then return 0 end
redis.call('SET', KEYS[1], ARGV[1])
return 1`)

// unlockScript deletes the lock only if this caller still owns it.
var unlockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0`)

type RedisVersions struct {
	rdb *redis.Client
}

func NewRedisVersions(rdb *redis.Client) *RedisVersions {
	return &RedisVersions{rdb: rdb}
}

func (r *RedisVersions) Lock(ctx context.Context, k Key) (func(), error) {
	key, token := "lock:"+k.String(), cryptorand.Text()
	deadline := time.Now().Add(lockWait)

	for {
		ok, err := r.rdb.SetNX(ctx, key, token, lockLease).Result()
		if err != nil {
			return nil, fmt.Errorf("acquiring lock: %w", err)
		}
		if ok {
			return func() {
				ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
				defer cancel()
				_ = unlockScript.Run(ctx, r.rdb, []string{key}, token).Err() // lease expiry is the fallback
			}, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("%w: %s", ErrBusy, k)
		}

		select {
		case <-time.After(25*time.Millisecond + rand.N(25*time.Millisecond)):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (r *RedisVersions) Applied(ctx context.Context, k Key) (int64, bool, error) {
	s, err := r.rdb.Get(ctx, versionKey(k)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	v, err := strconv.ParseInt(s, 10, 64)
	return v, err == nil, err
}

func (r *RedisVersions) Record(ctx context.Context, k Key, v int64) (bool, error) {
	n, err := recordScript.Run(ctx, r.rdb, []string{versionKey(k)}, v).Int()
	return n == 1, err
}

func versionKey(k Key) string { return "stockver:" + k.String() }

// MemoryVersions is a single-process VersionStore for tests. Like the Redis
// lock, Lock is per key: different keys never block each other.
type MemoryVersions struct {
	mu      sync.Mutex
	applied map[Key]int64
	locks   map[Key]*sync.Mutex
}

func (m *MemoryVersions) Lock(_ context.Context, k Key) (func(), error) {
	m.mu.Lock()
	if m.locks == nil {
		m.locks = map[Key]*sync.Mutex{}
	}
	l, ok := m.locks[k]
	if !ok {
		l = &sync.Mutex{}
		m.locks[k] = l
	}
	m.mu.Unlock()

	l.Lock()
	return l.Unlock, nil
}

func (m *MemoryVersions) Applied(_ context.Context, k Key) (int64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.applied[k]
	return v, ok, nil
}

func (m *MemoryVersions) Record(_ context.Context, k Key, v int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.applied[k]; ok && cur >= v {
		return false, nil
	}
	if m.applied == nil {
		m.applied = map[Key]int64{}
	}
	m.applied[k] = v
	return true, nil
}
