package health

import (
	"availability-processor/internal/startup"
	"availability-processor/internal/web"
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
)

const dependencyCheckTimeout = 5 * time.Second

type Checker interface {
	Check(ctx context.Context) error
}

type Handlers struct {
	startUp  *startup.State
	checkers map[string]Checker
}

func NewHandlers(startUp *startup.State, checkers map[string]Checker) *Handlers {
	return &Handlers{startUp: startUp, checkers: checkers}
}

type params struct {
	fx.In

	StartUp *startup.State
	Redis   *redis.Client
}

// redisChecker reports Redis as a dependency: without it every change fails,
// so a pod that cannot reach it must leave the load balancer.
type redisChecker struct{ rdb *redis.Client }

func (c redisChecker) Check(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }

func NewHealthHandlers(p params) *Handlers {
	return NewHandlers(p.StartUp, map[string]Checker{"redis": redisChecker{p.Redis}})
}

// HandleLiveness reports whether the process has finished starting up.
//
//	@Summary		Liveness probe
//	@Description	Returns UP once startup has completed, otherwise DOWN with 503.
//	@Tags			health
//	@Produce		json
//	@Success		200	{object}	Response
//	@Failure		503	{object}	Response
//	@Router			/health/liveness [get]
func (h *Handlers) HandleLiveness(w http.ResponseWriter, _ *http.Request) error {
	status := Up().Status
	code := http.StatusOK

	if !h.startUp.IsReady() {
		status = Down().Status
		code = http.StatusServiceUnavailable
	}

	return web.WriteJSON(w, code, NewHealthResponse(status, nil))
}

// HandleReadiness reports whether all dependencies are reachable.
//
//	@Summary		Readiness probe
//	@Description	Checks every registered dependency; any failure returns DOWN with 503.
//	@Tags			health
//	@Produce		json
//	@Success		200	{object}	Response
//	@Failure		503	{object}	Response
//	@Router			/health/readiness [get]
func (h *Handlers) HandleReadiness(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), dependencyCheckTimeout)
	defer cancel()

	components := h.checkDependencies(ctx)
	status, code := overallStatus(components)

	return web.WriteJSON(w, code, NewHealthResponse(status, components))
}

// HandleHealth combines liveness and dependency checks.
//
//	@Summary		Health dashboard
//	@Description	Returns liveness state, readiness state and per-component status.
//	@Tags			health
//	@Produce		json
//	@Success		200	{object}	DashboardResponse
//	@Failure		503	{object}	DashboardResponse
//	@Router			/health [get]
func (h *Handlers) HandleHealth(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), dependencyCheckTimeout)
	defer cancel()

	livenessStatus := Up().Status
	if !h.startUp.IsReady() {
		livenessStatus = Down().Status
	}

	components := h.checkDependencies(ctx)
	status, code := overallStatus(components)

	return web.WriteJSON(w, code, NewHealthDashboardResponse(status, livenessStatus, components))
}

func (h *Handlers) checkDependencies(ctx context.Context) map[string]ComponentStatus {
	components := make(map[string]ComponentStatus, len(h.checkers))

	var wg sync.WaitGroup
	var mu sync.Mutex

	for name, checker := range h.checkers {
		wg.Go(func() {
			status := Up()
			if err := checker.Check(ctx); err != nil {
				status = Down()
			}

			mu.Lock()
			components[name] = status
			mu.Unlock()
		})
	}

	wg.Wait()

	return components
}

func overallStatus(components map[string]ComponentStatus) (status string, httpCode int) {
	for _, c := range components {
		if c.Status != "UP" {
			return "DOWN", http.StatusServiceUnavailable
		}
	}
	return "UP", http.StatusOK
}
