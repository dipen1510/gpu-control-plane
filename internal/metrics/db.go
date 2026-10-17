package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
)

const (
	// collectTimeout bounds the collector's queries, so a slow database
	// slows a scrape by at most this much instead of hanging it.
	collectTimeout = 2 * time.Second

	// StuckTerminatingAfter is how long an instance may sit in TERMINATING
	// before gpucp_store_stuck_terminating_instances counts it. In M1 the
	// lifecycle driver finishes a delete within one request (a few
	// milliseconds, at most api.lifecycleTimeout = 3s), so an instance still
	// TERMINATING a minute later was left by a crash or a failed release
	// step and needs another DELETE (ADR-0001 Amendment 1, accepted
	// weakness). M2 adds real shutdown delays and must revisit this.
	StuckTerminatingAfter = time.Minute
)

// Querier is what the collector needs from Postgres. *pgxpool.Pool
// satisfies it.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var (
	nodesGPUsDesc = prometheus.NewDesc("gpucp_nodes_gpus",
		"GPUs per GPU type across all nodes: kind=capacity (installed) or kind=allocated (reserved by instances).",
		[]string{"gpu_type", "kind"}, nil)
	fragmentedDesc = prometheus.NewDesc("gpucp_scheduler_fragmented_gpus",
		"Free GPUs on schedulable nodes that cannot serve an 8-GPU request (free % 8), per GPU type (Discussion D7).",
		[]string{"gpu_type"}, nil)
	driftDesc = prometheus.NewDesc("gpucp_store_allocation_drift_nodes",
		"Nodes whose allocated differs from the sum of gpu_count of the capacity-holding instances on them (invariant 7). Should be 0; reported, never repaired.",
		nil, nil)
	outboxDesc = prometheus.NewDesc("gpucp_outbox_unpublished_rows",
		"Outbox rows not yet published. Grows in M1 by design; the M3 relay drains it.",
		nil, nil)
	// stuckTerminatingDesc surfaces instances that a DELETE accepted but
	// whose release step never committed (S1-10 decision: the DELETE body
	// carries no "re-DELETE" hint, so the signal lives here). Their GPUs and
	// quota stay held until something finishes the walk.
	stuckTerminatingDesc = prometheus.NewDesc("gpucp_store_stuck_terminating_instances",
		fmt.Sprintf("Instances in TERMINATING for longer than %s (GPUs still held; a crash or failed release left them).", StuckTerminatingAfter),
		nil, nil)
)

// DBCollector computes the database-derived gauges at scrape time with two
// read-only queries. It never writes: a drift is reported, not repaired
// (Discussion D15), because "fixing" allocated to match the instances would
// hide the bug that caused it.
type DBCollector struct {
	db  Querier
	log *slog.Logger
}

// NewDBCollector returns a collector reading db. Register it on
// Metrics.Registry(). Query errors are logged to log and reported to the
// registry, which leaves these gauges out of that scrape.
func NewDBCollector(db Querier, log *slog.Logger) *DBCollector {
	return &DBCollector{db: db, log: log}
}

// Describe implements prometheus.Collector.
func (c *DBCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nodesGPUsDesc
	ch <- fragmentedDesc
	ch <- driftDesc
	ch <- outboxDesc
	ch <- stuckTerminatingDesc
}

// Collect implements prometheus.Collector. Prometheus gives it no context,
// so it bounds its own queries with collectTimeout.
func (c *DBCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), collectTimeout)
	defer cancel()

	nodes, drift, err := c.readNodes(ctx)
	if err != nil {
		c.fail(ch, err, nodesGPUsDesc, fragmentedDesc, driftDesc)
	} else {
		for t, s := range summarize(nodes) {
			ch <- prometheus.MustNewConstMetric(nodesGPUsDesc, prometheus.GaugeValue, float64(s.Capacity), t, "capacity")
			ch <- prometheus.MustNewConstMetric(nodesGPUsDesc, prometheus.GaugeValue, float64(s.Allocated), t, "allocated")
			ch <- prometheus.MustNewConstMetric(fragmentedDesc, prometheus.GaugeValue, float64(s.Fragmented), t)
		}
		ch <- prometheus.MustNewConstMetric(driftDesc, prometheus.GaugeValue, float64(drift))
	}

	var unpublished, stuck int64
	if err := c.db.QueryRow(ctx, countsSQL, StuckTerminatingAfter.Seconds()).Scan(&unpublished, &stuck); err != nil {
		c.fail(ch, fmt.Errorf("read outbox and stuck counts: %w", err), outboxDesc, stuckTerminatingDesc)
		return
	}
	ch <- prometheus.MustNewConstMetric(outboxDesc, prometheus.GaugeValue, float64(unpublished))
	ch <- prometheus.MustNewConstMetric(stuckTerminatingDesc, prometheus.GaugeValue, float64(stuck))
}

func (c *DBCollector) fail(ch chan<- prometheus.Metric, err error, descs ...*prometheus.Desc) {
	c.log.Warn("metrics: database collector failed", "err", err)
	for _, d := range descs {
		ch <- prometheus.NewInvalidMetric(d, err)
	}
}

// nodesSQL returns every node with whether its allocated matches the sum of
// gpu_count of the instances holding capacity on it ($1 = holding states).
// One row per node; fine for an M1-sized fleet.
const nodesSQL = `
SELECT n.id, n.gpu_type, n.capacity, n.allocated, n.schedulable,
       n.allocated <> COALESCE(h.held, 0) AS drifted
FROM nodes n
LEFT JOIN (
    SELECT node_id, SUM(gpu_count) AS held
    FROM instances
    WHERE state = ANY($1::text[]) AND node_id IS NOT NULL
    GROUP BY node_id
) h ON h.node_id = n.id
`

// countsSQL counts unpublished outbox rows (uses the partial index) and
// instances in TERMINATING since more than $1 seconds.
const countsSQL = `
SELECT (SELECT count(*) FROM outbox WHERE published_at IS NULL),
       (SELECT count(*) FROM instances
         WHERE state = 'TERMINATING' AND updated_at < now() - make_interval(secs => $1))`

// typeSummary is the per-GPU-type part of the node gauges.
type typeSummary struct {
	Capacity   int
	Allocated  int
	Fragmented int
}

// summarize groups nodes by GPU type. Fragmentation is computed per type:
// free GPUs of different types can never combine into one request, so a
// fleet-wide FragmentedGPUs would hide where the fragmentation is (S1-4
// review). It is pure, so it is unit tested without a database.
func summarize(nodes []scheduler.Node) map[string]typeSummary {
	byType := map[string][]scheduler.Node{}
	for _, n := range nodes {
		byType[n.GPUType] = append(byType[n.GPUType], n)
	}
	out := make(map[string]typeSummary, len(byType))
	for t, ns := range byType {
		var s typeSummary
		for _, n := range ns {
			s.Capacity += n.Capacity
			s.Allocated += n.Allocated
		}
		s.Fragmented = scheduler.FragmentedGPUs(ns)
		out[t] = s
	}
	return out
}

// readNodes returns every node and the number of nodes whose allocated
// differs from what their instances hold.
func (c *DBCollector) readNodes(ctx context.Context) ([]scheduler.Node, int, error) {
	rows, err := c.db.Query(ctx, nodesSQL, statemachine.HoldingStates())
	if err != nil {
		return nil, 0, fmt.Errorf("read nodes: %w", err)
	}
	defer rows.Close()
	var nodes []scheduler.Node
	drift := 0
	for rows.Next() {
		var n scheduler.Node
		var drifted bool
		if err := rows.Scan(&n.ID, &n.GPUType, &n.Capacity, &n.Allocated, &n.Schedulable, &drifted); err != nil {
			return nil, 0, fmt.Errorf("scan node: %w", err)
		}
		nodes = append(nodes, n)
		if drifted {
			drift++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("read nodes: %w", err)
	}
	return nodes, drift, nil
}
