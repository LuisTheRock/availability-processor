package startup

import (
	"context"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

var Module = fx.Options(
	fx.Provide(NewState),
	fx.Invoke(func(lc fx.Lifecycle, state *State, l *zap.Logger) {
		lc.Append(fx.Hook{
			OnStart: func(context.Context) error {
				state.MarkReady()
				l.Info("Application startup completed")
				return nil
			},
		})
	}),
)
