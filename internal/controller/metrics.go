package controller

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the gpucp_controller_* collectors owned by the instance
// reconciler (ADR-0002 §Observability). Poller metrics (S2-5) and node sync
// metrics (S2-18) are registered by their owners.
type Metrics struct {
	// Transitions counts state changes the controller committed via
	// store.Transition, labelled from, to and reason.
	Transitions *prometheus.CounterVec
	// Orphans counts orphaned objects cleaned up, labelled kind (pod, cr).
	Orphans *prometheus.CounterVec
}

// NewMetrics creates the collectors and registers them on reg (normally
// controller-runtime's metrics.Registry, served on the controller's :8081).
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		Transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gpucp_controller_transitions_total",
			Help: "Instance state transitions committed by the controller.",
		}, []string{"from", "to", "reason"}),
		Orphans: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gpucp_controller_orphans_total",
			Help: "Orphaned objects (Pods, GPUInstances) found and cleaned up.",
		}, []string{"kind"}),
	}
	var err error
	if m.Transitions, err = register(reg, m.Transitions); err != nil {
		return nil, err
	}
	if m.Orphans, err = register(reg, m.Orphans); err != nil {
		return nil, err
	}
	// Pre-create the known orphan kinds so the series exist at 0 from start.
	for _, kind := range []string{"pod", "gpuinstance"} {
		m.Orphans.WithLabelValues(kind)
	}
	return m, nil
}

// register adds c to reg. If an identical collector is already registered
// (a second Manager in the same process, e.g. in tests), it reuses that one,
// because controller-runtime's metrics.Registry is process-wide.
func register(reg prometheus.Registerer, c *prometheus.CounterVec) (*prometheus.CounterVec, error) {
	err := reg.Register(c)
	if err == nil {
		return c, nil
	}
	var are prometheus.AlreadyRegisteredError
	if errors.As(err, &are) {
		if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
			return existing, nil
		}
	}
	return nil, fmt.Errorf("register controller metric: %w", err)
}
