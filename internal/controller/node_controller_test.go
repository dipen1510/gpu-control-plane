package controller

import (
	"context"
	"errors"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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
	want := store.NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, FailureDomain: "zone-a", Status: store.NodeReady}
	if !ok || obs != want {
		t.Fatalf("observeNode = %+v, %v; want %+v, true", obs, ok, want)
	}
	plain := gpuNode("cp", corev1.ConditionTrue, false)
	delete(plain.Labels, LabelGPUType)
	if _, ok := observeNode(plain); ok {
		t.Fatal("node without gpu type label observed")
	}
}

type fakeSyncer struct {
	mu  sync.Mutex
	got []store.NodeObservation
	err error
}

func (f *fakeSyncer) SyncNode(_ context.Context, obs store.NodeObservation) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, obs)
	return true, f.err
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
		name      string
		node      string
		syncErr   error
		wantSyncs int
		wantErr   bool
	}{
		{name: "gpu node synced", node: "w1", wantSyncs: 1},
		{name: "non-gpu node ignored", node: "cp"},
		{name: "missing node ignored", node: "gone"},
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
			if tc.wantSyncs == 1 && f.got[0].Status != store.NodeCordoned {
				t.Fatalf("status = %s, want CORDONED", f.got[0].Status)
			}
		})
	}
}
