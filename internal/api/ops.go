package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/dipen1510/gpu-control-plane/internal/metrics"
)

// Operational routes: not part of the /v1 API and not counted in
// gpucp_api_requests_total (a scrape every few seconds would drown the API
// traffic it is meant to show).
const (
	routeHealthz = "GET /healthz"
	routeReadyz  = "GET /readyz"
	routeMetrics = "GET /metrics"

	// routeUnmatched is the route label for requests that matched no
	// pattern (404 or 405 from the mux). Without it, every random path a
	// scanner tries would become its own series.
	routeUnmatched = "unmatched"

	// readyTimeout bounds the /readyz database ping. A load balancer probes
	// often; a hung database must fail the probe quickly, not hold it open.
	readyTimeout = time.Second

	codeNotReady = "not_ready"
)

// Pinger checks that the database is reachable. *pgxpool.Pool satisfies it.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Option configures a Handler.
type Option func(*Handler)

// WithMetrics makes the handler record its metrics on m and serve m on
// GET /metrics. Without it the handler records on a private Metrics that
// nothing exports (tests that don't care about metrics need not build one).
func WithMetrics(m *metrics.Metrics) Option {
	return func(h *Handler) { h.metrics, h.serveMetrics = m, true }
}

// WithReadiness makes GET /readyz ping db. Without it, /readyz answers 503:
// a server that cannot check its database must not claim to be ready.
func WithReadiness(db Pinger) Option {
	return func(h *Handler) { h.ready = db }
}

// healthz reports that the process is up and serving HTTP. It checks
// nothing else on purpose: a liveness probe that fails when Postgres is down
// would get every replica restarted, which cannot fix Postgres.
func (h *Handler) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz reports whether this replica can serve requests, i.e. reach
// Postgres within readyTimeout. 503 tells the load balancer to route around
// it (ADR-0001 §Failure modes: "Postgres down or slow").
func (h *Handler) readyz(w http.ResponseWriter, r *http.Request) {
	if h.ready == nil {
		writeError(w, http.StatusServiceUnavailable, codeNotReady, "no readiness check configured")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
	defer cancel()
	if err := h.ready.Ping(ctx); err != nil {
		h.log.Warn("readiness check failed: database unreachable", "err", err)
		writeError(w, http.StatusServiceUnavailable, codeNotReady, "database unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ServeHTTP routes the request and records gpucp_api_requests_total and
// gpucp_api_request_duration_seconds for /v1 routes. The route label is the
// mux pattern that matched (e.g. "GET /v1/instances/{id}"), which ServeMux
// stores in r.Pattern, never the raw path: label values must come from a
// small fixed set.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rec := &statusRecorder{ResponseWriter: w}
	h.mux.ServeHTTP(rec, r)

	route := r.Pattern
	switch route {
	case routeHealthz, routeReadyz, routeMetrics:
		return
	case "":
		route = routeUnmatched
	}
	h.metrics.ObserveRequest(route, rec.code(), time.Since(start).Seconds())
}

// statusRecorder remembers the status code a handler wrote.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// code is the status sent; a handler that wrote nothing sent 200.
func (s *statusRecorder) code() int {
	if s.status == 0 {
		return http.StatusOK
	}
	return s.status
}

// promLogger adapts slog to promhttp's error logger.
type promLogger struct{ log *slog.Logger }

// Println logs v formatted as fmt.Println would (operands separated by
// spaces), without the newline. promhttp passes a message and an error
// value; logged as-is, a JSON handler would render the error as {} and the
// cause would be lost.
func (p promLogger) Println(v ...any) {
	p.log.Warn("metrics: scrape error", "detail", strings.TrimSuffix(fmt.Sprintln(v...), "\n"))
}
