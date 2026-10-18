package controller

// S2-8 acceptance tests: the delete path against a real API server (envtest)
// and Postgres. envtest has no kubelet: a deleted Pod bound to a node keeps
// its deletionTimestamp (preStop "running") until the test finishes it with
// kubeletFinish.

import (
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// capacity returns nodes.allocated for inst's node and tenant_quota.used for
// its tenant and type.
func (e *lifecycleEnv) capacity(t *testing.T, inst store.Instance) (allocated, used int) {
	t.Helper()
	if err := e.db.QueryRow(t.Context(), `SELECT allocated FROM nodes WHERE id = $1`, inst.NodeID).Scan(&allocated); err != nil {
		t.Fatalf("read node: %v", err)
	}
	if err := e.db.QueryRow(t.Context(), `SELECT used FROM tenant_quota WHERE tenant_id = $1 AND gpu_type = $2`,
		inst.TenantID, inst.GPUType).Scan(&used); err != nil {
		t.Fatalf("read quota: %v", err)
	}
	return allocated, used
}

// waitPodDeleting polls until the controller has deleted the instance's Pod
// (deletionTimestamp set; envtest keeps it since there is no kubelet).
func (e *lifecycleEnv) waitPodDeleting(t *testing.T, id string) *corev1.Pod {
	t.Helper()
	key := types.NamespacedName{Namespace: DefaultInstanceNamespace, Name: gpuv1alpha1.K8sName(id)}
	deadline := time.Now().Add(20 * time.Second)
	for {
		var pod corev1.Pod
		err := e.c.Get(t.Context(), key, &pod)
		if err == nil && pod.DeletionTimestamp != nil {
			return &pod
		}
		if time.Now().After(deadline) {
			t.Fatalf("Pod %s not deleting (err %v)", key, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// kubeletFinish plays the kubelet after preStop: removes the Pod for good.
func (e *lifecycleEnv) kubeletFinish(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	if err := e.c.Delete(t.Context(), pod, client.GracePeriodSeconds(0)); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("finish Pod deletion: %v", err)
	}
}

// waitCRGone polls until the instance's GPUInstance no longer exists.
func (e *lifecycleEnv) waitCRGone(t *testing.T, id string) {
	t.Helper()
	key := types.NamespacedName{Namespace: DefaultInstanceNamespace, Name: gpuv1alpha1.K8sName(id)}
	deadline := time.Now().Add(20 * time.Second)
	for {
		var cr gpuv1alpha1.GPUInstance
		err := e.c.Get(t.Context(), key, &cr)
		if apierrors.IsNotFound(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("GPUInstance %s still present (finalizers %v, err %v)", key, cr.Finalizers, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// driveToRunning starts from a fresh SCHEDULED instance and returns once it
// is RUNNING (the test marks the Pod Ready).
func (e *lifecycleEnv) driveToRunning(t *testing.T, id string) store.Instance {
	t.Helper()
	inst := e.create(t, id)
	e.waitState(t, inst.ID, statemachine.Provisioning)
	e.setPodReady(t, e.waitPod(t, inst.ID))
	inst.ResourceVersion = e.waitState(t, inst.ID, statemachine.Running)
	return inst
}

// holdWhileTerminating checks, over a settle window, that the row stays
// TERMINATING, capacity is still held and the CR keeps its finalizer.
func (e *lifecycleEnv) holdWhileTerminating(t *testing.T, inst store.Instance, wantAlloc, wantUsed int) {
	t.Helper()
	settle := time.After(2 * time.Second)
	for {
		if st, _ := e.row(t, inst.ID); st != statemachine.Terminating {
			t.Fatalf("state %s while the Pod exists, want TERMINATING", st)
		}
		if a, u := e.capacity(t, inst); a != wantAlloc || u != wantUsed {
			t.Fatalf("allocated/used = %d/%d while the Pod exists, want %d/%d unchanged", a, u, wantAlloc, wantUsed)
		}
		if cr := e.cr(t, inst.ID); !slices.Contains(cr.Finalizers, gpuv1alpha1.FinalizerReleaseCapacity) {
			t.Fatalf("finalizer removed while the row is TERMINATING: %v", cr.Finalizers)
		}
		select {
		case <-settle:
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// TestDeleteReleasesOnlyAfterPodGone: API DELETE (→ TERMINATING) on a RUNNING
// instance; the controller deletes the Pod, but while it still exists
// nodes.allocated and tenant_quota.used are unchanged. Once it is gone the
// row is TERMINATED (released once) and the CR is deleted (invariant 5).
func TestDeleteReleasesOnlyAfterPodGone(t *testing.T) {
	e := newLifecycleEnv(t)
	e.startManager(t, true)
	inst := e.driveToRunning(t, "inst_del01")
	alloc, used := e.capacity(t, inst)

	// What the API's DELETE does (M2-10).
	if _, err := e.st.Transition(t.Context(), inst.ID, statemachine.Terminating, inst.ResourceVersion, ""); err != nil {
		t.Fatalf("API delete: %v", err)
	}
	pod := e.waitPodDeleting(t, inst.ID)
	e.waitCRStatus(t, inst.ID, statemachine.Terminating, inst.ResourceVersion+1)
	e.holdWhileTerminating(t, inst, alloc, used)

	e.kubeletFinish(t, pod)
	e.waitState(t, inst.ID, statemachine.Terminated)
	e.waitCRGone(t, inst.ID)
	if a, u := e.capacity(t, inst); a != alloc-inst.GPUCount || u != used-inst.GPUCount {
		t.Errorf("allocated/used = %d/%d after TERMINATED, want %d/%d", a, u, alloc-inst.GPUCount, used-inst.GPUCount)
	}
	want := []string{"instance.scheduled", "instance.provisioning", "instance.running", "instance.terminating", "instance.terminated"}
	if got := e.outbox(t, inst.ID); !slices.Equal(got, want) {
		t.Errorf("outbox = %v, want %v", got, want)
	}
}

// TestCRDeleteHonorsFinalizer: `kubectl delete gpuinstance` on a RUNNING
// instance. The finalizer holds the CR; the DB goes TERMINATING, and only
// after the Pod is gone TERMINATED; the finalizer is removed only then.
func TestCRDeleteHonorsFinalizer(t *testing.T) {
	e := newLifecycleEnv(t)
	e.startManager(t, true)
	inst := e.driveToRunning(t, "inst_crdel01")
	alloc, used := e.capacity(t, inst)

	if err := e.c.Delete(t.Context(), e.cr(t, inst.ID)); err != nil {
		t.Fatalf("delete CR: %v", err)
	}
	e.waitState(t, inst.ID, statemachine.Terminating)
	pod := e.waitPodDeleting(t, inst.ID)
	e.holdWhileTerminating(t, inst, alloc, used)
	if cr := e.cr(t, inst.ID); cr.DeletionTimestamp == nil {
		t.Fatal("CR not marked for deletion")
	}

	e.kubeletFinish(t, pod)
	e.waitState(t, inst.ID, statemachine.Terminated)
	e.waitCRGone(t, inst.ID)
	if a, u := e.capacity(t, inst); a != alloc-inst.GPUCount || u != used-inst.GPUCount {
		t.Errorf("allocated/used = %d/%d after TERMINATED, want %d/%d", a, u, alloc-inst.GPUCount, used-inst.GPUCount)
	}
	want := []string{"instance.scheduled", "instance.provisioning", "instance.running", "instance.terminating", "instance.terminated"}
	if got := e.outbox(t, inst.ID); !slices.Equal(got, want) {
		t.Errorf("outbox = %v, want %v", got, want)
	}
}

// TestTerminalRowCleansUpCR: a crash after → TERMINATED / FAILED leaves a CR
// (and maybe a Pod). The next controller deletes the Pod, then the CR, and
// never writes the row.
func TestTerminalRowCleansUpCR(t *testing.T) {
	for _, tc := range []struct {
		name    string
		path    []statemachine.State
		withPod bool
	}{
		{name: "TERMINATED, no Pod", path: []statemachine.State{statemachine.Terminating, statemachine.Terminated}},
		{name: "FAILED, Pod left", path: []statemachine.State{statemachine.Failed}, withPod: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newLifecycleEnv(t)
			inst := e.create(t, "inst_term01")
			cr := newCR(DefaultInstanceNamespace, inst)
			if err := e.c.Create(t.Context(), cr); err != nil {
				t.Fatalf("pre-create CR: %v", err)
			}
			if tc.withPod {
				if err := e.c.Create(t.Context(), BuildPod(cr, DefaultPodConfig())); err != nil {
					t.Fatalf("pre-create Pod: %v", err)
				}
			}
			rv := inst.ResourceVersion
			for _, to := range tc.path {
				next, err := e.st.Transition(t.Context(), inst.ID, to, rv, "boot_failed")
				if err != nil {
					t.Fatalf("→ %s: %v", to, err)
				}
				rv = next.ResourceVersion
			}
			events := e.outbox(t, inst.ID)

			e.startManager(t, false) // the lingering CR's informer event is the trigger
			if tc.withPod {
				e.kubeletFinish(t, e.waitPodDeleting(t, inst.ID))
			}
			e.waitCRGone(t, inst.ID)
			if st, gotRV := e.row(t, inst.ID); st != tc.path[len(tc.path)-1] || gotRV != rv {
				t.Errorf("row = %s rv %d, want %s rv %d untouched", st, gotRV, tc.path[len(tc.path)-1], rv)
			}
			if got := e.outbox(t, inst.ID); !slices.Equal(got, events) {
				t.Errorf("outbox = %v, want %v unchanged", got, events)
			}
		})
	}
}
