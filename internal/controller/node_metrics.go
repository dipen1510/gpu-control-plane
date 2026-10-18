package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// nodeCountsTimeout bounds the gpucp_nodes scrape query.
const nodeCountsTimeout = 2 * time.Second

// NodeMetrics holds the node sync gauges (ADR-0002 §Observability):
// gpucp_nodes_capacity_conflicts and gpucp_nodes_type_conflicts count the GPU
// Nodes currently in that conflict, and gpucp_nodes{gpu_type,status} is read
// from the nodes table at scrape time. Methods are safe on a nil receiver.
type NodeMetrics struct {
	capacityConflicts prometheus.Gauge
	typeConflicts     prometheus.Gauge

	mu      sync.Mutex // guards the two sets; never held across I/O
	capSet  map[string]struct{}
	typeSet map[string]struct{}
}

// NewNodeMetrics creates the node collectors and registers them on reg. A
// collector already registered under the same name (an earlier Manager in
// the same process, e.g. in tests) is replaced: the newest one wins.
func NewNodeMetrics(reg prometheus.Registerer, nodes NodeSyncer) (*NodeMetrics, error) {
	m := &NodeMetrics{
		capacityConflicts: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gpucp_nodes_capacity_conflicts",
			Help: "GPU Nodes reporting a capacity below nodes.allocated; the stored capacity is kept.",
		}),
		typeConflicts: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gpucp_nodes_type_conflicts",
			Help: "GPU Nodes whose gpu.example.com/type label differs from the stored gpu_type; the stored type is kept.",
		}),
		capSet:  map[string]struct{}{},
		typeSet: map[string]struct{}{},
	}
	for _, c := range []prometheus.Collector{m.capacityConflicts, m.typeConflicts, &nodeCountCollector{nodes: nodes}} {
		if err := replaceRegister(reg, c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func replaceRegister(reg prometheus.Registerer, c prometheus.Collector) error {
	err := reg.Register(c)
	var are prometheus.AlreadyRegisteredError
	if errors.As(err, &are) {
		reg.Unregister(are.ExistingCollector)
		err = reg.Register(c)
	}
	if err != nil {
		return fmt.Errorf("register node metric: %w", err)
	}
	return nil
}

// record sets node's conflict state and reports which conflicts are new
// (so the caller logs each once, not on every reconcile).
func (m *NodeMetrics) record(node string, capConflict, typeConflict bool) (newCap, newType bool) {
	if m == nil {
		return capConflict, typeConflict
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	newCap = setMember(m.capSet, node, capConflict)
	newType = setMember(m.typeSet, node, typeConflict)
	m.capacityConflicts.Set(float64(len(m.capSet)))
	m.typeConflicts.Set(float64(len(m.typeSet)))
	return newCap, newType
}

// forget clears node's conflicts (the Node is gone or no longer a GPU Node).
func (m *NodeMetrics) forget(node string) {
	m.record(node, false, false)
}

// setMember adds or removes node and reports whether it was newly added.
func setMember(set map[string]struct{}, node string, in bool) bool {
	_, had := set[node]
	if !in {
		delete(set, node)
		return false
	}
	set[node] = struct{}{}
	return !had
}

var nodesDesc = prometheus.NewDesc("gpucp_nodes",
	"Nodes rows per gpu_type and status.", []string{"gpu_type", "status"}, nil)

// nodeCountCollector reads gpucp_nodes from Postgres at scrape time.
type nodeCountCollector struct {
	nodes NodeSyncer
}

func (c *nodeCountCollector) Describe(ch chan<- *prometheus.Desc) { ch <- nodesDesc }

// Collect implements prometheus.Collector. Prometheus passes no context, so
// it bounds its own query.
func (c *nodeCountCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), nodeCountsTimeout)
	defer cancel()
	counts, err := c.nodes.NodeCounts(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(nodesDesc, err)
		return
	}
	for _, n := range counts {
		ch <- prometheus.MustNewConstMetric(nodesDesc, prometheus.GaugeValue, float64(n.Count), n.GPUType, n.Status)
	}
}
