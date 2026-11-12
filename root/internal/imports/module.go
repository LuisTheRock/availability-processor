package imports

import (
	"context"

	"availability-processor/internal/availability"
	"availability-processor/internal/upstream"

	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var Module = fx.Options(
	fx.Provide(
		func(proc *availability.Processor, c *upstream.Client, rdb *redis.Client, l *zap.Logger, lc fx.Lifecycle) *Service {
			s := newService(proc, c, newRedisStore(rdb), l)

			ctx, cancel := context.WithCancel(context.Background())
			lc.Append(fx.Hook{
				OnStart: func(context.Context) error {
					s.ctx = ctx // owns every worker goroutine
					return nil
				},
				OnStop: func(context.Context) error {
					cancel()
					s.Wait() // workers save their final state before Redis closes
					return nil
				},
			})
			return s
		},
		NewHandler,
	),
)
