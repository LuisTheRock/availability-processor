package imports

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const jobTTL = 7 * 24 * time.Hour

type redisStore struct {
	rdb *redis.Client
}

func newRedisStore(rdb *redis.Client) *redisStore { return &redisStore{rdb: rdb} }

func jobKey(id string) string { return "job:" + id }

func (s *redisStore) Save(ctx context.Context, sn Snapshot, newErrs []RowError) error {
	_, err := s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		key := jobKey(sn.ID)
		p.HSet(ctx, key, map[string]any{
			"state": sn.State, "total": sn.Total, "processed": sn.Processed,
			"succeeded": sn.Succeeded, "failed": sn.Failed, "skipped": sn.Skipped,
			"createdAt": sn.CreatedAt.Format(time.RFC3339Nano), "updatedAt": sn.UpdatedAt.Format(time.RFC3339Nano),
		})
		p.Expire(ctx, key, jobTTL)

		if len(newErrs) > 0 {
			vals := make([]any, len(newErrs))
			for i, e := range newErrs {
				b, _ := json.Marshal(e) // plain struct: cannot fail
				vals[i] = b
			}
			p.RPush(ctx, key+":errors", vals...)
			p.Expire(ctx, key+":errors", jobTTL)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("saving job %s: %w", sn.ID, err)
	}
	return nil
}

func (s *redisStore) Load(ctx context.Context, id string, errorsLimit int) (Snapshot, []RowError, error) {
	key := jobKey(id)
	h, err := s.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return Snapshot{}, nil, fmt.Errorf("loading job %s: %w", id, err)
	}
	if len(h) == 0 {
		return Snapshot{}, nil, ErrJobNotFound
	}

	sn := Snapshot{ID: id, State: h["state"]}
	for field, dst := range map[string]*int64{
		"total": &sn.Total, "processed": &sn.Processed, "succeeded": &sn.Succeeded,
		"failed": &sn.Failed, "skipped": &sn.Skipped,
	} {
		*dst, _ = strconv.ParseInt(h[field], 10, 64)
	}
	sn.CreatedAt, _ = time.Parse(time.RFC3339Nano, h["createdAt"])
	sn.UpdatedAt, _ = time.Parse(time.RFC3339Nano, h["updatedAt"])

	// A job that is still "running" but stopped updating lost its process.
	if (sn.State == StateRunning || sn.State == StatePending) && time.Since(sn.UpdatedAt) > staleAfter {
		sn.State = StateInterrupted
		_ = s.rdb.HSet(ctx, key, "state", StateInterrupted).Err() // best effort write-back
	}

	var raw []string
	if errorsLimit > 0 { // LRANGE 0 -1 would return the whole list
		if raw, err = s.rdb.LRange(ctx, key+":errors", 0, int64(errorsLimit)-1).Result(); err != nil {
			return Snapshot{}, nil, fmt.Errorf("loading job %s errors: %w", id, err)
		}
	}
	errs := make([]RowError, 0, len(raw))
	for _, r := range raw {
		var e RowError
		if json.Unmarshal([]byte(r), &e) == nil {
			errs = append(errs, e)
		}
	}
	return sn, errs, nil
}
