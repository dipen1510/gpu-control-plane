//go:build integration

package metrics

import (
	"crypto/sha256"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	dto "github.com/prometheus/client_model/go"

	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

// newCollected returns a fresh database with the given nodes and a Metrics
// whose registry carries a DBCollector on it.
func newCollected(t *testing.T, nodes ...string) (*pgxpool.Pool, *Metrics) {
	t.Helper()
	db := testutil.NewDB(t)
	for _, sql := range nodes {
		if _, err := db.Exec(t.Context(), sql); err != nil {
			t.Fatalf("insert node: %v", err)
		}
	}
	m := New()
	m.Registry().MustRegister(NewDBCollector(db, slog.New(slog.NewTextHandler(io.Discard, nil))))
	return db, m
}

func gather(t *testing.T, m *Metrics) []*dto.MetricFamily {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	return families
}

func create(t *testing.T, st *store.Store, id, gpuType string, n int) store.Instance {
	t.Helper()
	sum := sha256.Sum256([]byte(id))
	res, err := st.Create(t.Context(), store.CreateRequest{
		ID: id, TenantID: "t1", GPUType: gpuType, GPUCount: n, IdempotencyKey: id, RequestHash: sum[:],
	})
	if err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	return res.Instance
}

func allocated(t *testing.T, db *pgxpool.Pool, node string) int {
	t.Helper()
	var a int
	if err := db.QueryRow(t.Context(), `SELECT allocated FROM nodes WHERE id = $1`, node).Scan(&a); err != nil {
		t.Fatalf("read allocated: %v", err)
	}
	return a
}

// TestAllocationDriftGauge: a node whose allocated no longer matches its
// instances makes gpucp_store_allocation_drift_nodes report 1, and reading
// the gauge never "fixes" allocated (alert, never auto-fix: Discussion D15).
func TestAllocationDriftGauge(t *testing.T) {
	db, m := newCollected(t,
		`INSERT INTO nodes (id, gpu_type, capacity, failure_domain) VALUES ('n1', 'H100', 8, 'fd-a')`,
		`INSERT INTO nodes (id, gpu_type, capacity, failure_domain) VALUES ('n2', 'H100', 8, 'fd-b')`)
	st := store.New(db, scheduler.New(nil))

	// Real instances keep the accounting right: 2 on one node, and one that
	// went all the way to TERMINATED (released, so it must not count).
	live := create(t, st, "inst_live", "H100", 2)
	gone := create(t, st, "inst_gone", "H100", 4)
	inst := gone
	for _, to := range []statemachine.State{statemachine.Terminating, statemachine.Terminated} {
		next, err := st.Transition(t.Context(), inst.ID, to, inst.ResourceVersion, "")
		if err != nil {
			t.Fatalf("transition to %s: %v", to, err)
		}
		inst = next
	}
	if got := count(gather(t, m), "gpucp_store_allocation_drift_nodes", ""); got != 0 {
		t.Fatalf("drift before corruption = %v, want 0", got)
	}

	// Corrupt one node behind the store's back.
	before := allocated(t, db, live.NodeID)
	if _, err := db.Exec(t.Context(), `UPDATE nodes SET allocated = allocated + 1 WHERE id = $1`, live.NodeID); err != nil {
		t.Fatalf("corrupt allocated: %v", err)
	}
	for range 2 { // two scrapes: the first must not repair it for the second
		if got := count(gather(t, m), "gpucp_store_allocation_drift_nodes", ""); got != 1 {
			t.Errorf("drift after corrupting %s = %v, want 1", live.NodeID, got)
		}
	}
	if got := allocated(t, db, live.NodeID); got != before+1 {
		t.Errorf("allocated on %s = %d after scraping, want the corrupted %d (never auto-fix)", live.NodeID, got, before+1)
	}
}

// TestNodeGaugesPerType: capacity, allocated and fragmentation are reported
// per GPU type from the database.
func TestNodeGaugesPerType(t *testing.T) {
	db, m := newCollected(t,
		`INSERT INTO nodes (id, gpu_type, capacity) VALUES ('h1', 'H100', 8), ('h2', 'H100', 8), ('a1', 'A100', 8)`)
	st := store.New(db, scheduler.New(nil))
	create(t, st, "inst_h", "H100", 4) // best fit: one H100 node at 4 free
	create(t, st, "inst_a", "A100", 2) // the A100 node at 6 free

	f := gather(t, m)
	checks := []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"gpucp_nodes_gpus", map[string]string{"gpu_type": "H100", "kind": "capacity"}, 16},
		{"gpucp_nodes_gpus", map[string]string{"gpu_type": "H100", "kind": "allocated"}, 4},
		{"gpucp_nodes_gpus", map[string]string{"gpu_type": "A100", "kind": "capacity"}, 8},
		{"gpucp_nodes_gpus", map[string]string{"gpu_type": "A100", "kind": "allocated"}, 2},
		{"gpucp_scheduler_fragmented_gpus", map[string]string{"gpu_type": "H100"}, 4},
		{"gpucp_scheduler_fragmented_gpus", map[string]string{"gpu_type": "A100"}, 6},
	}
	for _, c := range checks {
		if got := gauge(f, c.name, c.labels); got != c.want {
			t.Errorf("%s%v = %v, want %v", c.name, c.labels, got, c.want)
		}
	}
	if got := count(f, "gpucp_store_allocation_drift_nodes", ""); got != 0 {
		t.Errorf("drift = %v, want 0", got)
	}
	// Two creates, each one instance.scheduled event, none published in M1.
	if got := count(f, "gpucp_outbox_unpublished_rows", ""); got != 2 {
		t.Errorf("outbox unpublished = %v, want 2", got)
	}
}

// TestStuckTerminatingGauge: only instances in TERMINATING for longer than
// StuckTerminatingAfter count; a delete in progress does not.
func TestStuckTerminatingGauge(t *testing.T) {
	db, m := newCollected(t, `INSERT INTO nodes (id, gpu_type, capacity) VALUES ('h1', 'H100', 8)`)
	st := store.New(db, scheduler.New(nil))
	for _, id := range []string{"inst_old", "inst_new", "inst_old_running"} {
		inst := create(t, st, id, "H100", 1)
		if id == "inst_old_running" {
			continue
		}
		if _, err := st.Transition(t.Context(), inst.ID, statemachine.Terminating, inst.ResourceVersion, ""); err != nil {
			t.Fatalf("to TERMINATING: %v", err)
		}
	}
	if got := count(gather(t, m), "gpucp_store_stuck_terminating_instances", ""); got != 0 {
		t.Fatalf("stuck right after delete = %v, want 0", got)
	}
	if _, err := db.Exec(t.Context(),
		`UPDATE instances SET updated_at = now() - interval '2 minutes' WHERE id IN ('inst_old', 'inst_old_running')`); err != nil {
		t.Fatalf("age instances: %v", err)
	}
	if got := count(gather(t, m), "gpucp_store_stuck_terminating_instances", ""); got != 1 {
		t.Errorf("stuck = %v, want 1 (only inst_old)", got)
	}
}

// TestDBCollectorDBDown: with the database gone the DB gauges are left out
// and an error is reported, but nothing panics or hangs.
func TestDBCollectorDBDown(t *testing.T) {
	db, m := newCollected(t)
	db.Close()
	if _, err := m.Registry().Gather(); err == nil {
		t.Errorf("gather with a closed pool: no error, want the collector's error")
	}
}

// gauge returns the gauge of family name whose labels equal want, or -1.
func gauge(families []*dto.MetricFamily, name string, want map[string]string) float64 {
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	metrics:
		for _, m := range f.GetMetric() {
			if len(m.GetLabel()) != len(want) {
				continue
			}
			for _, l := range m.GetLabel() {
				if want[l.GetName()] != l.GetValue() {
					continue metrics
				}
			}
			return m.GetGauge().GetValue()
		}
	}
	return -1
}
