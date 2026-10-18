package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// fakeStore returns row from GetByK8sName and transitionErr from Transition.
type fakeStore struct {
	row           store.Instance
	getErr        error
	transitionErr error
	transitions   int
}

func (f *fakeStore) GetByK8sName(_ context.Context, name string) (store.Instance, error) {
	if f.getErr != nil {
		return store.Instance{}, f.getErr
	}
	if gpuv1alpha1.K8sName(f.row.ID) != name {
		return store.Instance{}, store.ErrNotFound
	}
	return f.row, nil
}

func (f *fakeStore) Transition(_ context.Context, _ string, to statemachine.State, rv int64, _ string) (store.Instance, error) {
	f.transitions++
	if f.transitionErr != nil {
		return store.Instance{}, f.transitionErr
	}
	next := f.row
	next.State, next.ResourceVersion = to, rv+1
	return next, nil
}

func newFakeReconciler(t *testing.T, st InstanceStore) *InstanceReconciler {
	t.Helper()
	s, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&gpuv1alpha1.GPUInstance{}).Build()
	return &InstanceReconciler{Client: c, APIReader: c, Store: st, Pod: DefaultPodConfig(), Namespace: "gpucp"}
}

func scheduledRow() store.Instance {
	return store.Instance{ID: "inst_u1", TenantID: "t1", GPUType: "H100", GPUCount: 2, NodeID: "w1",
		State: statemachine.Scheduled, ResourceVersion: 1}
}

func TestReconcileResults(t *testing.T) {
	stale := fmt.Errorf("transition: %w", store.ErrStaleVersion)
	for _, tc := range []struct {
		name       string
		st         *fakeStore
		ns         string
		wantErr    bool
		wantResult ctrl.Result
		wantTrans  int
	}{
		{name: "stale version requeues without error", st: &fakeStore{row: scheduledRow(), transitionErr: stale},
			ns: "gpucp", wantResult: ctrl.Result{RequeueAfter: staleRequeueDelay}, wantTrans: 1},
		{name: "other transition error is returned", st: &fakeStore{row: scheduledRow(), transitionErr: errors.New("db down")},
			ns: "gpucp", wantErr: true, wantTrans: 1},
		{name: "scheduled moves to provisioning", st: &fakeStore{row: scheduledRow()}, ns: "gpucp", wantTrans: 1},
		{name: "row not found is a no-op", st: &fakeStore{getErr: store.ErrNotFound}, ns: "gpucp"},
		{name: "db read error is returned", st: &fakeStore{getErr: errors.New("db down")}, ns: "gpucp", wantErr: true},
		{name: "other namespace is ignored", st: &fakeStore{row: scheduledRow()}, ns: "default"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newFakeReconciler(t, tc.st)
			res, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: tc.ns, Name: "inst-u1"}})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if res != tc.wantResult {
				t.Errorf("result = %+v, want %+v", res, tc.wantResult)
			}
			if tc.st.transitions != tc.wantTrans {
				t.Errorf("transitions = %d, want %d", tc.st.transitions, tc.wantTrans)
			}
		})
	}
}

// TestReconcileProvisioningWaitsForReady: PROVISIONING with a Pod that is not
// Ready makes no transition; once Ready, it moves to RUNNING.
func TestReconcileProvisioningWaitsForReady(t *testing.T) {
	row := scheduledRow()
	row.State, row.ResourceVersion = statemachine.Provisioning, 2
	st := &fakeStore{row: row}
	r := newFakeReconciler(t, st)
	cr := newCR("gpucp", row)
	if err := r.Client.Create(t.Context(), cr); err != nil {
		t.Fatal(err)
	}
	pod := BuildPod(cr, r.Pod)
	if err := r.Client.Create(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "gpucp", Name: "inst-u1"}}
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if st.transitions != 0 {
		t.Fatalf("transitions with Pod not Ready = %d, want 0", st.transitions)
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := r.Client.Status().Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if st.transitions != 1 {
		t.Fatalf("transitions with Pod Ready = %d, want 1", st.transitions)
	}
	var got gpuv1alpha1.GPUInstance
	if err := r.Client.Get(t.Context(), req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.State != gpuv1alpha1.StateRunning || got.Status.ResourceVersion != 3 {
		t.Errorf("CR status = %s rv %d, want RUNNING rv 3", got.Status.State, got.Status.ResourceVersion)
	}
}

// TestReconcileTerminatingPodNeverDrivesProgress: a Pod being deleted keeps
// Ready=True through preStop. It must not move PROVISIONING → RUNNING (a
// wrong instance.running event would start billing), and in SCHEDULED it
// must not count as "Pod exists": no transition, requeue until it is gone.
func TestReconcileTerminatingPodNeverDrivesProgress(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state statemachine.State
	}{
		{name: "provisioning", state: statemachine.Provisioning},
		{name: "scheduled", state: statemachine.Scheduled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := scheduledRow()
			row.State = tc.state
			st := &fakeStore{row: row}
			r := newFakeReconciler(t, st)
			cr := newCR("gpucp", row)
			if err := r.Client.Create(t.Context(), cr); err != nil {
				t.Fatal(err)
			}
			pod := BuildPod(cr, r.Pod)
			pod.Finalizers = []string{"test.example.com/hold"} // keeps it terminating after Delete
			if err := r.Client.Create(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			if err := r.Client.Status().Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			if err := r.Client.Delete(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			var cur corev1.Pod
			if err := r.Client.Get(t.Context(), types.NamespacedName{Namespace: "gpucp", Name: pod.Name}, &cur); err != nil || cur.DeletionTimestamp == nil {
				t.Fatalf("setup: pod not terminating (err %v)", err)
			}

			req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "gpucp", Name: "inst-u1"}}
			res, err := r.Reconcile(t.Context(), req)
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if st.transitions != 0 {
				t.Errorf("transitions = %d, want 0 with a terminating Pod", st.transitions)
			}
			if tc.state == statemachine.Scheduled && res.RequeueAfter <= 0 {
				t.Errorf("result = %+v, want a requeue while the Pod terminates", res)
			}
		})
	}
}
