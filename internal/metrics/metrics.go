// Package metrics holds the apiserver's Prometheus metrics (ADR-0001
// §Observability). Every metric lives on the registry owned by a Metrics
// value, never on the global default registry, so each test can build its
// own Metrics and read exact values without seeing other tests' counts.
//
// Two kinds of metrics live here:
//
//   - Event metrics (counters, histograms) that code increments as things
//     happen: API requests, create results, reserve conflicts, Place
//     duration, state transitions.
//   - Database-derived gauges (node GPUs, fragmentation, allocation drift,
//     unpublished outbox rows, stuck TERMINATING instances), computed by
//     DBCollector at scrape time. They read Postgres and never write it: a
//     drift is reported, not repaired (Discussion D15).
package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
)

// Results of a create, the values of gpucp_create_results_total{result}.
// The first five are the ADR list; the last three cover the remaining
// outcomes so every create request lands in exactly one bucket.
const (
	CreateScheduled            = "scheduled"
	CreateReplayed             = "replayed"
	CreateQuotaExceeded        = "quota_exceeded"
	CreateInsufficientCapacity = "insufficient_capacity"
	CreateKeyReused            = "key_reused"
	// CreateInvalid is any 4xx found before the store is called: bad
	// headers, malformed or oversized body, unknown gpu_type, bad gpu_count.
	CreateInvalid = "invalid"
	// CreateConflictRetry is a transaction aborted by Postgres to break a
	// deadlock (SQLSTATE 40P01), answered 503. It should stay at 0: a
	// non-zero value means some code path broke the lock order.
	CreateConflictRetry = "conflict_retry"
	// CreateError is any other failure, answered 500.
	CreateError = "error"
)

// createResults lists every result so each series exists (at 0) from the
// start: a rate() over a series that appears only on the first event misses
// that event.
var createResults = []string{
	CreateScheduled, CreateReplayed, CreateQuotaExceeded, CreateInsufficientCapacity,
	CreateKeyReused, CreateInvalid, CreateConflictRetry, CreateError,
}

// Metrics is the set of apiserver metrics and the registry that exports
// them. All methods are safe for concurrent use.
type Metrics struct {
	registry *prometheus.Registry

	apiRequests   *prometheus.CounterVec
	apiDuration   *prometheus.HistogramVec
	createResults *prometheus.CounterVec
	transitions   *prometheus.CounterVec

	// ReserveConflicts counts reserve attempts that lost a node race and
	// fell back to the next candidate. Pass it to scheduler.New.
	ReserveConflicts prometheus.Counter
	// PlaceDuration observes how long scheduler.Place takes, in seconds,
	// whatever its outcome. Each reserve attempt is 3 round trips
	// (savepoint, update, release/rollback; ADR-0001 Amendment 2), so this
	// grows with contention. Pass it to scheduler.WithPlaceDuration.
	PlaceDuration prometheus.Histogram
}

// New returns Metrics registered on a fresh registry, which also carries the
// standard Go runtime and process collectors.
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		apiRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gpucp_api_requests_total",
			Help: "API requests by route pattern (method and path template, never the raw path) and HTTP status code.",
		}, []string{"route", "code"}),
		apiDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gpucp_api_request_duration_seconds",
			Help:    "API request latency by route pattern, including the M1 lifecycle walk.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route"}),
		createResults: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gpucp_create_results_total",
			Help: "Instance create requests by outcome.",
		}, []string{"result"}),
		transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gpucp_instance_transitions_total",
			Help: "Committed instance state transitions (store.Transition) by from and to state.",
		}, []string{"from", "to"}),
		ReserveConflicts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gpucp_scheduler_reserve_conflicts_total",
			Help: "Reserve attempts that lost a node race (the node filled or was drained after the snapshot) and fell back.",
		}),
		PlaceDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "gpucp_scheduler_place_duration_seconds",
			Help: "Duration of scheduler.Place (snapshot, rank and reserve attempts), any outcome.",
			// Place is a few round trips to a local Postgres: most calls take
			// about a millisecond, contention adds lock waits.
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}),
	}
	m.registry.MustRegister(
		m.apiRequests, m.apiDuration, m.createResults, m.transitions,
		m.ReserveConflicts, m.PlaceDuration,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	for _, r := range createResults {
		m.createResults.WithLabelValues(r)
	}
	return m
}

// Registry returns the registry, for registering more collectors (see
// DBCollector) and for reading values in tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves the registry in the Prometheus text format. A collector
// that fails (e.g. the database is down) is left out of that scrape and
// logged; the other metrics are still served, so an outage doesn't also
// blind the API metrics.
func (m *Metrics) Handler(errLog promhttp.Logger) http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		ErrorLog:      errLog,
		ErrorHandling: promhttp.ContinueOnError,
	})
}

// ObserveRequest records one API request. route must be a route pattern,
// not the raw path, or every instance id becomes its own series.
func (m *Metrics) ObserveRequest(route string, code int, seconds float64) {
	m.apiRequests.WithLabelValues(route, strconv.Itoa(code)).Inc()
	m.apiDuration.WithLabelValues(route).Observe(seconds)
}

// CreateResult counts one create outcome; result is one of the Create*
// constants.
func (m *Metrics) CreateResult(result string) {
	m.createResults.WithLabelValues(result).Inc()
}

// Transitioned counts one committed state transition. It satisfies
// store.TransitionObserver.
func (m *Metrics) Transitioned(from, to statemachine.State) {
	m.transitions.WithLabelValues(string(from), string(to)).Inc()
}
