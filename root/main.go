package main

import (
	"availability-processor/cfg"
	"availability-processor/internal/availability"
	"availability-processor/internal/health"
	"availability-processor/internal/imports"
	"availability-processor/internal/server"
	"availability-processor/internal/startup"
	"availability-processor/internal/upstream"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const shutdownTimeout = 30 * time.Second

// @title		Service Availability Processor
// @version     1.0
// @basePath	/
func main() {
	app := fx.New(
		fx.StopTimeout(shutdownTimeout),
		fx.Provide(newZapLogger),

		fx.Provide(cfg.NewApiConfig),

		fx.Provide(newRedis, newUpstream),

		startup.Module,
		health.Module,
		availability.Module,
		imports.Module,

		server.Module,
	)

	if app.Err() != nil {
		log.Fatalf("failed to initialize app: %v", app.Err())
	}

	app.Run()
}

func newUpstream(c *cfg.ApiConfig) *upstream.Client {
	return upstream.New(c.Dependencies.BaseURL)
}

// newRedis fails startup fast when Redis is down; versions are never applied unchecked.
func newRedis(lc fx.Lifecycle, c *cfg.ApiConfig) *redis.Client {
	rdb := redis.NewClient(&redis.Options{Addr: c.Redis.Addr})
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := rdb.Ping(ctx).Err(); err != nil {
				return fmt.Errorf("redis unreachable at %s (run `docker compose up -d`, or set REDIS_ADDR): %w", c.Redis.Addr, err)
			}
			return nil
		},
		OnStop: func(context.Context) error { return rdb.Close() },
	})
	return rdb
}

func newZapLogger() (*zap.Logger, error) {
	return zap.NewProduction()
}
