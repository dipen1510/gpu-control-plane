package controller

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/dipen1510/gpu-control-plane/internal/store"
)

func gpuNode(name string, ready corev1.ConditionStatus, cordoned bool) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{
			LabelGPUType: "H100", LabelZone: "zone-a",
		}},
		Spec: corev1.NodeSpec{Unschedulable: cordoned},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{ResourceGPU: resource.MustParse("8")},
		},
	}
	if ready != "" {
		n.Status.Conditions = []corev1.NodeCondition{
			{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse},
			{Type: corev1.NodeReady, Status: ready},
		}
	}
	return n
}

func TestNodeStatus(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ready    corev1.ConditionStatus
		cordoned bool
		want     string
	}{
		{name: "ready", ready: corev1.ConditionTrue, want: store.NodeReady},
		{name: "ready and cordoned", ready: corev1.ConditionTrue, cordoned: true, want: store.NodeCordoned},
		{name: "not ready", ready: corev1.ConditionFalse, want: store.NodeNotReady},
		{name: "unknown", ready: corev1.ConditionUnknown, want: store.NodeNotReady},
		{name: "no condition", want: store.NodeNotReady},
		{name: "not ready and cordoned", ready: corev1.ConditionFalse, cordoned: true, want: store.NodeNotReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := nodeStatus(gpuNode("w", tc.ready, tc.cordoned)); got != tc.want {
				t.Fatalf("nodeStatus = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestObserveNode(t *testing.T) {
	obs, ok := observeNode(gpuNode("w1", corev1.ConditionTrue, false))
	want := store.NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, CapacityKnown: true, FailureDomain: "zone-a", Status: store.NodeReady}
	if !ok || obs != want {
		t.Fatalf("observeNode = %+v, %v; want %+v, true", obs, ok, want)
	}
	plain := gpuNode("cp", corev1.ConditionTrue, false)
	delete(plain.Labels, LabelGPUType)
	if _, ok := observeNode(plain); ok {
		t.Fatal("node without gpu type label observed")
	}
	starting := gpuNode("w2", corev1.ConditionFalse, false)
	starting.Status.Capacity = nil // kubelet has not reported the GPU resource yet
	obs, ok = observeNode(starting)
	if !ok || obs.CapacityKnown || obs.Capacity != 0 {
		t.Fatalf("observeNode(no capacity) = %+v, %v; want CapacityKnown false", obs, ok)
	}
}

type fakeSyncer struct {
	mu       sync.Mutex
	got      []store.NodeObservation
	marked   []string
	present  [][]string
	res      store.NodeSyncResult
	err      error
	markErr  error
	sweepErr error
}

func (f *fakeSyncer) SyncNode(_ context.Context, obs store.NodeObservation) (store.NodeSyncResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, obs)
	if f.err != nil {
		return store.NodeSyncResult{}, f.err
	}
	res := f.res
	res.Wrote = true
	if res.StoredGPUType == "" {
		res.StoredGPUType = obs.GPUType
	}
	return res, nil
}

func (f *fakeSyncer) MarkNodeNotReady(_ context.Context, name string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marked = append(f.marked, name)
	return f.markErr == nil, f.markErr
}

func (f *fakeSyncer) SweepNodes(_ context.Context, present []string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.present = append(f.present, present)
	return nil, f.sweepErr
}

func (f *fakeSyncer) NodeCounts(context.Context) ([]store.NodeCount, error) {
	return []store.NodeCount{{GPUType: "H100", Status: store.NodeReady, Count: 2}}, nil
}

func TestNodeReconcile(t *testing.T) {
	s, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	plain := gpuNode("cp", corev1.ConditionTrue, false)
	delete(plain.Labels, LabelGPUType)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(gpuNode("w1", corev1.ConditionTrue, true), plain).Build()

	for _, tc := range []struct {
		name       string
		node       string
		syncErr    error
		wantSyncs  int
		wantErr    bool
		wantMarked bool
	}{
		{name: "gpu node synced", node: "w1", wantSyncs: 1},
		{name: "missing node marked not ready", node: "gone", wantMarked: true},
		{name: "unlabeled node marked not ready (label removed)", node: "cp", wantMarked: true},
		{name: "db error retried", node: "w1", syncErr: errors.New("db down"), wantSyncs: 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeSyncer{err: tc.syncErr}
			r := &NodeReconciler{Reader: c, Nodes: f}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: tc.node}})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if len(f.got) != tc.wantSyncs {
				t.Fatalf("syncs = %d, want %d", len(f.got), tc.wantSyncs)
			}
			if wantMarked := []string{tc.node}; tc.wantMarked != (len(f.marked) == 1) || (tc.wantMarked && f.marked[0] != wantMarked[0]) {
				t.Fatalf("marked = %v, wantMarked %v", f.marked, tc.wantMarked)
			}
			if tc.wantSyncs == 1 && f.got[0].Status != store.NodeCordoned {
				t.Fatalf("status = %s, want CORDONED", f.got[0].Status)
			}
		})
	}
}

func TestNodeReconcileMarkError(t *testing.T) {
	s, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).Build()
	r := &NodeReconciler{Reader: c, Nodes: &fakeSyncer{markErr: errors.New("db down")}}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "gone"}}); err == nil {
		t.Fatal("mark error not returned for retry")
	}
}

func TestNodeReconcileConflictMetrics(t *testing.T) {
	s, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(gpuNode("w1", corev1.ConditionTrue, false)).Build()
	reg := prometheus.NewPedanticRegistry()
	f := &fakeSyncer{res: store.NodeSyncResult{CapacityConflict: true, TypeConflict: true, StoredGPUType: "A100"}}
	m, err := NewNodeMetrics(reg, f)
	if err != nil {
		t.Fatal(err)
	}
	r := &NodeReconciler{Reader: c, Nodes: f, Metrics: m}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "w1"}}
	for range 3 { // a persisting conflict is one node, not three
		if _, err := r.Reconcile(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
	if got := promtestutil.ToFloat64(m.capacityConflicts); got != 1 {
		t.Fatalf("capacity conflicts = %v, want 1", got)
	}
	if got := promtestutil.ToFloat64(m.typeConflicts); got != 1 {
		t.Fatalf("type conflicts = %v, want 1", got)
	}
	f.res = store.NodeSyncResult{}
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if promtestutil.ToFloat64(m.capacityConflicts) != 0 || promtestutil.ToFloat64(m.typeConflicts) != 0 {
		t.Fatal("resolved conflicts still counted")
	}
	if n, err := promtestutil.GatherAndCount(reg, "gpucp_nodes"); err != nil || n != 1 {
		t.Fatalf("gpucp_nodes series = %d, %v; want 1", n, err)
	}
	// A second registration (another Manager in the process) replaces the first.
	if _, err := NewNodeMetrics(reg, f); err != nil {
		t.Fatalf("re-register: %v", err)
	}
}

func TestNodePredicateSeesLabelRemoval(t *testing.T) {
	labeled := gpuNode("w1", corev1.ConditionTrue, false)
	unlabeled := labeled.DeepCopy()
	delete(unlabeled.Labels, LabelGPUType)
	pred := gpuNodePredicate()
	if !pred.Update(event.UpdateEvent{ObjectOld: labeled, ObjectNew: unlabeled}) {
		t.Fatal("label removal filtered out")
	}
	if pred.Update(event.UpdateEvent{ObjectOld: unlabeled, ObjectNew: unlabeled}) {
		t.Fatal("non-gpu node update passed")
	}
	if !pred.Delete(event.DeleteEvent{Object: labeled}) {
		t.Fatal("gpu node delete filtered out")
	}
}

type fakeCache struct{ synced bool }

func (c fakeCache) WaitForCacheSync(context.Context) bool { return c.synced }

func TestNodeSweeperWaitsForCacheSync(t *testing.T) {
	s, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	plain := gpuNode("cp", corev1.ConditionTrue, false)
	delete(plain.Labels, LabelGPUType)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(gpuNode("w1", corev1.ConditionTrue, false), plain).Build()

	f := &fakeSyncer{}
	if err := (&NodeSweeper{Cache: fakeCache{synced: false}, Reader: c, Nodes: f}).Start(t.Context()); err == nil {
		t.Fatal("unsynced cache: want error")
	}
	if len(f.present) != 0 {
		t.Fatal("swept before cache sync")
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Start sweeps once, then returns on the done ctx
	if err := (&NodeSweeper{Cache: fakeCache{synced: true}, Reader: c, Nodes: f}).Start(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.present) != 1 || len(f.present[0]) != 1 || f.present[0][0] != "w1" {
		t.Fatalf("swept with present = %v, want [[w1]]", f.present)
	}
}
