package server

import (
	"availability-processor/cfg"
	_ "availability-processor/docs"
	"availability-processor/internal/availability"
	"availability-processor/internal/health"
	"availability-processor/internal/imports"
	"availability-processor/internal/web"
	"net/http"
	"time"

	httpSwagger "github.com/swaggo/http-swagger/v2"
	"go.uber.org/zap"
)

const (
	shutdownTimeout   = 5 * time.Second
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 60 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 120 * time.Second
)

func NewHttpServer(config *cfg.ApiConfig, router http.Handler) *http.Server {
	return &http.Server{
		Addr:              config.Server.Address,
		Handler:           router,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}
}

func WebRouter(l *zap.Logger, h *health.Handlers, a *availability.Handler, i *imports.Handler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", web.Make(h.HandleHealth))
	mux.HandleFunc("GET /health/liveness", web.Make(h.HandleLiveness))
	mux.HandleFunc("GET /health/readiness", web.Make(h.HandleReadiness))

	mux.HandleFunc("POST /_dev/messages", web.Make(a.HandleMessage))

	v1 := http.NewServeMux()
	v1.HandleFunc("POST /availability/imports", web.Make(i.HandleCreate))
	v1.HandleFunc("GET /availability/imports/{jobId}", web.Make(i.HandleGet))

	mux.Handle("/v1/", http.StripPrefix("/v1", v1))

	mux.Handle("GET /swagger/", httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"),
	))

	return web.EnableCorsMiddleware()(web.LoggerMiddleware(l)(mux))
}
