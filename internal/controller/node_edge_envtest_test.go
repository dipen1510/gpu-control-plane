package controller

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/dipen1510/gpu-control-plane/internal/store"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

// nodeEnv is one envtest API server + Postgres + Manager running node sync.
type nodeEnv struct {
	db *pgxpool.Pool
	c  client.Client
}

// startNodeEnv starts envtest and Postgres; before runs against the DB
// before the Manager starts (to seed rows the sweep should catch).
func startNodeEnv(t *testing.T, before func(db *pgxpool.Pool)) *nodeEnv {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run `make test-envtest`")
	}
	db := testutil.NewDB(t)
	if before != nil {
		before(db)
	}
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})
	mgr, err := NewManager(cfg, Options{MetricsAddr: "0", ProbeAddr: "0", SkipNameValidation: true,
		Nodes: store.New(db, nil)})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("manager stopped with error: %v", err)
		}
	})
	c, err := client.New(cfg, client.Options{Scheme: mgr.GetScheme()})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return &nodeEnv{db: db, c: c}
}

func (e *nodeEnv) createNode(t *testing.T, name, gpuType string, capacity string) {
	t.Helper()
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{LabelGPUType: gpuType}}}
	if err := e.c.Create(t.Context(), n); err != nil {
		t.Fatalf("create node: %v", err)
	}
	e.updateStatus(t, name, corev1.ConditionTrue, capacity)
}

// updateStatus sets Ready and the GPU capacity ("" removes the resource).
func (e *nodeEnv) updateStatus(t *testing.T, name string, ready corev1.ConditionStatus, capacity string) {
	t.Helper()
	var n corev1.Node
	if err := e.c.Get(t.Context(), types.NamespacedName{Name: name}, &n); err != nil {
		t.Fatalf("get node: %v", err)
	}
	n.Status.Capacity = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")}
	if capacity != "" {
		n.Status.Capacity[ResourceGPU] = resource.MustParse(capacity)
	}
	n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready,
		LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now()}}
	if err := e.c.Status().Update(t.Context(), &n); err != nil {
		t.Fatalf("update node status: %v", err)
	}
}

func (e *nodeEnv) updateLabels(t *testing.T, name string, mutate func(map[string]string)) {
	t.Helper()
	var n corev1.Node
	if err := e.c.Get(t.Context(), types.NamespacedName{Name: name}, &n); err != nil {
		t.Fatalf("get node: %v", err)
	}
	mutate(n.Labels)
	if err := e.c.Update(t.Context(), &n); err != nil {
		t.Fatalf("update node labels: %v", err)
	}
}

type envRow struct {
	gpuType, status     string
	capacity, allocated int
}

// waitRow polls until ok(row) or fails after 15s.
func (e *nodeEnv) waitRow(t *testing.T, name string, ok func(envRow) bool) envRow {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var r envRow
	for time.Now().Before(deadline) {
		err := e.db.QueryRow(t.Context(), `SELECT gpu_type, status, capacity, allocated FROM nodes WHERE id = $1`, name).
			Scan(&r.gpuType, &r.status, &r.capacity, &r.allocated)
		if err == nil && ok(r) {
			return r
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node %s row %+v never matched", name, r)
	return r
}

func gaugeValue(t *testing.T, name string) float64 {
	t.Helper()
	mfs, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name && len(mf.GetMetric()) == 1 {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

func TestNodeDeletedMarksNotReadyEnvtest(t *testing.T) {
	e := startNodeEnv(t, nil)
	e.createNode(t, "gpu-del", "H100", "8")
	e.waitRow(t, "gpu-del", func(r envRow) bool { return r.status == store.NodeReady && r.capacity == 8 })
	if err := e.c.Delete(t.Context(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-del"}}); err != nil {
		t.Fatalf("delete node: %v", err)
	}
	r := e.waitRow(t, "gpu-del", func(r envRow) bool { return r.status == store.NodeNotReady })
	if r.capacity != 8 || r.gpuType != "H100" {
		t.Fatalf("row after delete = %+v, want kept with capacity 8", r)
	}
}

func TestNodeCapacityDisappearsKeepsStored(t *testing.T) {
	e := startNodeEnv(t, nil)
	e.createNode(t, "gpu-cap", "H100", "8")
	e.waitRow(t, "gpu-cap", func(r envRow) bool { return r.status == store.NodeReady && r.capacity == 8 })
	// Kubelet restart: the GPU resource is gone for a while. Ready=False
	// lets us see that the reconcile ran.
	e.updateStatus(t, "gpu-cap", corev1.ConditionFalse, "")
	r := e.waitRow(t, "gpu-cap", func(r envRow) bool { return r.status == store.NodeNotReady })
	if r.capacity != 8 {
		t.Fatalf("capacity = %d after resource disappeared, want 8", r.capacity)
	}
	// A brand-new Node that is Ready but reports no capacity yet is inserted
	// NOT_READY / 0. The first event the reconciler sees must already carry
	// Ready=True, or the insert is trivially NOT_READY (no conditions) and a
	// later update takes the conflict branch, which keeps capacity 0 and
	// syncs status READY. So: create unlabeled (filtered out), set status,
	// then add the label; that label update is the first event that passes.
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-new"}}
	if err := e.c.Create(t.Context(), n); err != nil {
		t.Fatalf("create node: %v", err)
	}
	e.updateStatus(t, "gpu-new", corev1.ConditionTrue, "")
	if err := e.c.Get(t.Context(), types.NamespacedName{Name: "gpu-new"}, n); err != nil {
		t.Fatalf("get node: %v", err)
	}
	n.Labels = map[string]string{LabelGPUType: "A100"}
	if err := e.c.Update(t.Context(), n); err != nil {
		t.Fatalf("label node: %v", err)
	}
	e.waitRow(t, "gpu-new", func(r envRow) bool { return r.status == store.NodeNotReady && r.capacity == 0 })
}

func TestNodeLabelRemovedMarksNotReady(t *testing.T) {
	e := startNodeEnv(t, nil)
	e.createNode(t, "gpu-lbl", "H100", "8")
	e.waitRow(t, "gpu-lbl", func(r envRow) bool { return r.status == store.NodeReady })
	e.updateLabels(t, "gpu-lbl", func(l map[string]string) { delete(l, LabelGPUType) })
	e.waitRow(t, "gpu-lbl", func(r envRow) bool { return r.status == store.NodeNotReady })
}

func TestNodeSweepCatchesMissedDelete(t *testing.T) {
	e := startNodeEnv(t, func(db *pgxpool.Pool) {
		// Rows for Nodes deleted while the controller was down.
		if _, err := db.Exec(t.Context(), `INSERT INTO nodes (id, gpu_type, capacity, status)
			VALUES ('ghost', 'H100', 8, 'READY'), ('gpu-live', 'H100', 8, 'READY')`); err != nil {
			t.Fatalf("seed: %v", err)
		}
	})
	e.createNode(t, "gpu-live", "H100", "8")
	e.waitRow(t, "ghost", func(r envRow) bool { return r.status == store.NodeNotReady })
	// The live Node stays READY (the sweep only touches missing names).
	e.waitRow(t, "gpu-live", func(r envRow) bool { return r.status == store.NodeReady })
}

func TestNodeConflictsEnvtest(t *testing.T) {
	e := startNodeEnv(t, nil)
	e.createNode(t, "gpu-cf", "H100", "8")
	e.waitRow(t, "gpu-cf", func(r envRow) bool { return r.status == store.NodeReady && r.capacity == 8 })
	if _, err := e.db.Exec(t.Context(), `UPDATE nodes SET allocated = 6 WHERE id = 'gpu-cf'`); err != nil {
		t.Fatalf("set allocated: %v", err)
	}
	// Capacity below allocated: kept, other columns (status) still synced.
	e.updateStatus(t, "gpu-cf", corev1.ConditionFalse, "4")
	r := e.waitRow(t, "gpu-cf", func(r envRow) bool { return r.status == store.NodeNotReady })
	if r.capacity != 8 || r.allocated != 6 {
		t.Fatalf("row = %+v, want capacity 8 allocated 6", r)
	}
	if v := gaugeValue(t, "gpucp_nodes_capacity_conflicts"); v != 1 {
		t.Fatalf("gpucp_nodes_capacity_conflicts = %v, want 1", v)
	}
	// Type label changed: stored type kept.
	e.updateLabels(t, "gpu-cf", func(l map[string]string) { l[LabelGPUType] = "A100" })
	deadline := time.Now().Add(15 * time.Second)
	for gaugeValue(t, "gpucp_nodes_type_conflicts") != 1 {
		if time.Now().After(deadline) {
			t.Fatal("gpucp_nodes_type_conflicts never reached 1")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if r := e.waitRow(t, "gpu-cf", func(envRow) bool { return true }); r.gpuType != "H100" {
		t.Fatalf("gpu_type = %s, want H100 kept", r.gpuType)
	}
}
