package availability

import (
	"availability-processor/internal/upstream"

	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
)

var Module = fx.Provide(
	func(rdb *redis.Client) VersionStore { return NewRedisVersions(rdb) },
	func(c *upstream.Client, v VersionStore) *Processor { return NewProcessor(c, v) },
	NewHandler,
)
