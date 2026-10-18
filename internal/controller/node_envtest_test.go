package controller

import (
	"context"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/dipen1510/gpu-control-plane/internal/store"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

// TestNodeSyncStatus runs the NodeReconciler against a real API server
// (envtest) and Postgres, and walks one GPU Node through Ready → cordon →
// Ready=False → back to Ready, checking nodes.status after each step. Needs
// KUBEBUILDER_ASSETS and DATABASE_URL: run `make test-envtest` with Postgres up.
func TestNodeSyncStatus(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run `make test-envtest`")
	}
	db := testutil.NewDB(t) // skips if DATABASE_URL is unset

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

	// A non-GPU Node must never get a row.
	if err := c.Create(t.Context(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "control-plane"}}); err != nil {
		t.Fatalf("create plain node: %v", err)
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-w1", Labels: map[string]string{
		LabelGPUType: "H100", LabelZone: "zone-a",
	}}}
	if err := c.Create(t.Context(), node); err != nil {
		t.Fatalf("create gpu node: %v", err)
	}

	setReady := func(status corev1.ConditionStatus) {
		t.Helper()
		var n corev1.Node
		if err := c.Get(t.Context(), types.NamespacedName{Name: "gpu-w1"}, &n); err != nil {
			t.Fatalf("get node: %v", err)
		}
		n.Status.Capacity = corev1.ResourceList{ResourceGPU: resource.MustParse("8")}
		n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: status,
			LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now()}}
		if err := c.Status().Update(t.Context(), &n); err != nil {
			t.Fatalf("update node status: %v", err)
		}
	}
	setCordon := func(on bool) {
		t.Helper()
		var n corev1.Node
		if err := c.Get(t.Context(), types.NamespacedName{Name: "gpu-w1"}, &n); err != nil {
			t.Fatalf("get node: %v", err)
		}
		n.Spec.Unschedulable = on
		if err := c.Update(t.Context(), &n); err != nil {
			t.Fatalf("update node spec: %v", err)
		}
	}
	waitStatus := func(want string) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		var got string
		var capacity int
		for time.Now().Before(deadline) {
			err := db.QueryRow(t.Context(), `SELECT status, capacity FROM nodes WHERE id = 'gpu-w1'`).Scan(&got, &capacity)
			if err == nil && got == want && capacity == 8 {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("nodes.status = %q capacity %d, want %q / 8", got, capacity, want)
	}

	setReady(corev1.ConditionTrue)
	waitStatus(store.NodeReady)
	setCordon(true)
	waitStatus(store.NodeCordoned)
	setCordon(false)
	setReady(corev1.ConditionFalse)
	waitStatus(store.NodeNotReady)
	setReady(corev1.ConditionTrue)
	waitStatus(store.NodeReady)

	var gpuType, domain string
	if err := db.QueryRow(t.Context(), `SELECT gpu_type, failure_domain FROM nodes WHERE id = 'gpu-w1'`).Scan(&gpuType, &domain); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if gpuType != "H100" || domain != "zone-a" {
		t.Fatalf("row = %s/%s, want H100/zone-a", gpuType, domain)
	}
	var plain int
	if err := db.QueryRow(t.Context(), `SELECT count(*) FROM nodes WHERE id = 'control-plane'`).Scan(&plain); err != nil {
		t.Fatalf("count: %v", err)
	}
	if plain != 0 {
		t.Fatal("non-GPU node got a row")
	}
}
