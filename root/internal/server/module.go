package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

var Module = fx.Options(
	fx.Provide(
		fx.Annotate(WebRouter, fx.As(new(http.Handler))),
		NewHttpServer,
	),
	fx.Invoke(
		func(lc fx.Lifecycle, srv *http.Server, l *zap.Logger, sd fx.Shutdowner) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
							l.Error("http server stopped unexpectedly", zap.Error(err))
							if shutdownErr := sd.Shutdown(fx.ExitCode(1)); shutdownErr != nil {
								l.Error("Failed to signal app shutdown", zap.Error(shutdownErr))
							}
						}
					}()

					l.Info("http server started", zap.String("addr", srv.Addr))

					return nil
				},
				OnStop: func(ctx context.Context) error {
					shutDownCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
					defer cancel()

					if err := srv.Shutdown(shutDownCtx); err != nil {
						return fmt.Errorf("shutting down http server: %w", err)
					}

					l.Info("Successfully shutdown http server")

					return nil
				},
			})
		},
	),
)
