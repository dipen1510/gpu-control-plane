package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// reasonStore is a fakeStore that also records each transition's target
// state and failure_reason.
type reasonStore struct {
	row  store.Instance
	tos  []statemachine.State
	why  []string
	rvIn []int64
	// staleFirst makes the first Transition lose its CAS (ErrStaleVersion)
	// without changing the row, as when another writer bumped the rv.
	staleFirst bool
}

func (s *reasonStore) GetByK8sName(_ context.Context, name string) (store.Instance, error) {
	if gpuv1alpha1.K8sName(s.row.ID) != name {
		return store.Instance{}, store.ErrNotFound
	}
	return s.row, nil
}

func (s *reasonStore) Transition(_ context.Context, _ string, to statemachine.State, rv int64, reason string) (store.Instance, error) {
	s.tos, s.why, s.rvIn = append(s.tos, to), append(s.why, reason), append(s.rvIn, rv)
	if s.staleFirst && len(s.tos) == 1 {
		return store.Instance{}, fmt.Errorf("transition: %w", store.ErrStaleVersion)
	}
	next := s.row
	next.State, next.ResourceVersion, next.FailureReason = to, rv+1, reason
	s.row = next
	return next, nil
}

func runningRow() store.Instance {
	row := scheduledRow()
	row.State, row.ResourceVersion = statemachine.Running, 3
	return row
}

// newSplitReconciler returns a reconciler whose cached Client and live
// APIReader are separate fakes, so a test can make the cache disagree with
// the API server. The CR (with finalizer) exists in both.
func newSplitReconciler(t *testing.T, st InstanceStore) (r *InstanceReconciler, cache, live client.Client) {
	t.Helper()
	s, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	build := func() client.Client {
		return fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&gpuv1alpha1.GPUInstance{}).Build()
	}
	cache, live = build(), build()
	cr := newCR("gpucp", runningRow())
	if err := cache.Create(t.Context(), cr); err != nil {
		t.Fatal(err)
	}
	// The same object (same UID) in both, as cache and API server would have.
	same := cr.DeepCopy()
	same.ResourceVersion = ""
	if err := live.Create(t.Context(), same); err != nil {
		t.Fatal(err)
	}
	return &InstanceReconciler{Client: cache, APIReader: live, Store: st, Pod: DefaultPodConfig(), Namespace: "gpucp"}, cache, live
}

// storedCR returns the instance's GPUInstance as stored in c (with its UID).
func storedCR(t *testing.T, c client.Client) *gpuv1alpha1.GPUInstance {
	t.Helper()
	var cr gpuv1alpha1.GPUInstance
	if err := c.Get(t.Context(), runningReq.NamespacedName, &cr); err != nil {
		t.Fatal(err)
	}
	return &cr
}

// podIn creates pod in c (with finalizers, if any) and sets its status.
func podIn(t *testing.T, c client.Client, pod *corev1.Pod, status corev1.PodStatus) *corev1.Pod {
	t.Helper()
	if err := c.Create(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	pod.Status = status
	if err := c.Status().Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	return pod
}

var readyStatus = corev1.PodStatus{
	Phase:      corev1.PodRunning,
	Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
}

// readyPod creates the instance's Pod, controlled by its CR, in c with phase
// Running and Ready=True.
func readyPod(t *testing.T, c client.Client, r *InstanceReconciler) *corev1.Pod {
	t.Helper()
	return podIn(t, c, BuildPod(storedCR(t, c), r.Pod), readyStatus)
}

// foreignPod is a Ready Pod with the instance's name whose controller is not
// its GPUInstance: another owner UID, or no owner at all.
func foreignPod(t *testing.T, c client.Client, r *InstanceReconciler, noOwner bool) *corev1.Pod {
	t.Helper()
	pod := BuildPod(storedCR(t, c), r.Pod)
	if noOwner {
		pod.OwnerReferences = nil
	} else {
		// Another GPUInstance by name and UID: foreign whether ownership is
		// checked by UID or by name + label (D42).
		for i := range pod.OwnerReferences {
			pod.OwnerReferences[i].Name = "some-other-gpuinstance"
			pod.OwnerReferences[i].UID = "some-other-gpuinstance-uid"
		}
	}
	return podIn(t, c, pod, readyStatus)
}

var runningReq = ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "gpucp", Name: "inst-u1"}}

// TestPodLostUsesLiveRead (S2-9): the cache says the Pod is missing but the
// API server has it (a cold or lagging cache) → no transition, Pod kept.
// When the API server agrees it is missing → FAILED pod_lost, once.
func TestPodLostUsesLiveRead(t *testing.T) {
	t.Run("cache missing, API present: not lost", func(t *testing.T) {
		st := &reasonStore{row: runningRow()}
		r, _, live := newSplitReconciler(t, st)
		pod := readyPod(t, live, r)

		if _, err := r.Reconcile(t.Context(), runningReq); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if len(st.tos) != 0 {
			t.Fatalf("transitions = %v %v, want none while the API server has the Pod", st.tos, st.why)
		}
		var got corev1.Pod
		if err := live.Get(t.Context(), client.ObjectKeyFromObject(pod), &got); err != nil || got.DeletionTimestamp != nil {
			t.Errorf("live Pod deleted or deleting (err %v)", err)
		}
	})
	t.Run("cache missing, API missing: FAILED pod_lost", func(t *testing.T) {
		st := &reasonStore{row: runningRow()}
		r, cache, _ := newSplitReconciler(t, st)

		if _, err := r.Reconcile(t.Context(), runningReq); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if len(st.tos) != 1 || st.tos[0] != statemachine.Failed || st.why[0] != ReasonPodLost || st.rvIn[0] != 3 {
			t.Fatalf("transitions = %v reasons %v rv %v, want [FAILED] [pod_lost] at rv 3", st.tos, st.why, st.rvIn)
		}
		var cr gpuv1alpha1.GPUInstance
		if err := cache.Get(t.Context(), runningReq.NamespacedName, &cr); err != nil {
			t.Fatal(err)
		}
		if cr.Status.State != gpuv1alpha1.StateFailed || cr.Status.Reason != ReasonPodLost || cr.Status.ResourceVersion != 4 {
			t.Errorf("CR status = %s %q rv %d, want FAILED pod_lost rv 4", cr.Status.State, cr.Status.Reason, cr.Status.ResourceVersion)
		}
	})
}

// TestReconcileRunningPodStates: the RUNNING row of ADR-0002 §Reconcile for
// each Pod state the cache and API server agree on.
func TestReconcileRunningPodStates(t *testing.T) {
	t.Run("ready: no write", func(t *testing.T) {
		st := &reasonStore{row: runningRow()}
		r, cache, live := newSplitReconciler(t, st)
		readyPod(t, cache, r)
		readyPod(t, live, r)
		res, err := r.Reconcile(t.Context(), runningReq)
		if err != nil || res != (ctrl.Result{}) || len(st.tos) != 0 {
			t.Fatalf("res %+v err %v transitions %v, want nothing", res, err, st.tos)
		}
	})

	// Failed, and Succeeded too: the main container is `sleep infinity`, so
	// any exit means the machine is gone.
	for _, phase := range []corev1.PodPhase{corev1.PodFailed, corev1.PodSucceeded} {
		t.Run(string(phase)+": delete Pod, FAILED only once it is gone", func(t *testing.T) {
			testExitedPod(t, phase)
		})
	}

	t.Run("deleting (ready, preStop): capacity held, requeue", func(t *testing.T) {
		st := &reasonStore{row: runningRow()}
		r := newFakeReconciler(t, st)
		if err := r.Client.Create(t.Context(), newCR("gpucp", runningRow())); err != nil {
			t.Fatal(err)
		}
		pod := readyPod(t, r.Client, r)
		pod.Finalizers = []string{"test.example.com/hold"}
		if err := r.Client.Update(t.Context(), pod); err != nil {
			t.Fatal(err)
		}
		if err := r.Client.Delete(t.Context(), pod); err != nil {
			t.Fatal(err)
		}
		res, err := r.Reconcile(t.Context(), runningReq)
		if err != nil || res.RequeueAfter <= 0 || len(st.tos) != 0 {
			t.Fatalf("res %+v err %v transitions %v, want requeue and no transition", res, err, st.tos)
		}
	})

	t.Run("CR being deleted by hand, Pod gone: not pod_lost", func(t *testing.T) {
		st := &reasonStore{row: runningRow()}
		r := newFakeReconciler(t, st)
		cr := newCR("gpucp", runningRow()) // carries the release-capacity finalizer
		if err := r.Client.Create(t.Context(), cr); err != nil {
			t.Fatal(err)
		}
		if err := r.Client.Delete(t.Context(), cr); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Reconcile(t.Context(), runningReq); err != nil {
			t.Fatal(err)
		}
		for i, to := range st.tos {
			if to == statemachine.Failed {
				t.Errorf("transition %d = FAILED %q on a user delete, want it recorded as a delete (D29)", i, st.why[i])
			}
		}
	})
}

// testExitedPod: RUNNING with our Pod in phase: the Pod is deleted, capacity
// is held while it still exists, and FAILED pod_lost follows once it is gone.
func testExitedPod(t *testing.T, phase corev1.PodPhase) {
	st := &reasonStore{row: runningRow()}
	r := newFakeReconciler(t, st)
	if err := r.Client.Create(t.Context(), newCR("gpucp", runningRow())); err != nil {
		t.Fatal(err)
	}
	pod := BuildPod(storedCR(t, r.Client), r.Pod)
	pod.Finalizers = []string{"test.example.com/hold"} // a kubelet still tearing it down
	podIn(t, r.Client, pod, corev1.PodStatus{Phase: phase})

	res, err := r.Reconcile(t.Context(), runningReq)
	if err != nil || res.RequeueAfter <= 0 {
		t.Fatalf("res %+v err %v, want a requeue while the deleted Pod goes away", res, err)
	}
	if len(st.tos) != 0 {
		t.Fatalf("transitions = %v while the Pod exists, want none (capacity held)", st.tos)
	}
	var got corev1.Pod
	if err := r.Client.Get(t.Context(), client.ObjectKeyFromObject(pod), &got); err != nil || got.DeletionTimestamp == nil {
		t.Fatalf("%s Pod not deleted (err %v)", phase, err)
	}
	// Still terminating: a second reconcile must not fail it either.
	if _, err := r.Reconcile(t.Context(), runningReq); err != nil || len(st.tos) != 0 {
		t.Fatalf("err %v transitions %v while the Pod is terminating, want none", err, st.tos)
	}

	got.Finalizers = nil // kubelet done: the Pod is removed
	if err := r.Client.Update(t.Context(), &got); err != nil {
		t.Fatal(err)
	}
	if err := r.Client.Get(t.Context(), client.ObjectKeyFromObject(pod), &got); !apierrors.IsNotFound(err) {
		t.Fatalf("setup: Pod still present (err %v)", err)
	}
	if _, err := r.Reconcile(t.Context(), runningReq); err != nil {
		t.Fatal(err)
	}
	if len(st.tos) != 1 || st.tos[0] != statemachine.Failed || st.why[0] != ReasonPodLost {
		t.Fatalf("transitions = %v %v, want [FAILED] [pod_lost]", st.tos, st.why)
	}
}

// TestRunningForeignPodIsLost: a Pod with the instance's name whose controller
// is not this GPUInstance is not ours, so our Pod is gone. Once a live read
// agrees: FAILED pod_lost, the foreign Pod is left in place (never deleted by
// us), and gpucp_controller_orphans_total{kind="pod"} counts it once. If only
// the cache shows a foreign Pod and the API server has ours, nothing happens.
func TestRunningForeignPodIsLost(t *testing.T) {
	for _, tc := range []struct {
		name    string
		noOwner bool
	}{
		{name: "other owner"},
		{name: "no owner", noOwner: true},
	} {
		t.Run(tc.name+": cache and API agree, FAILED pod_lost, Pod kept", func(t *testing.T) {
			st := &reasonStore{row: runningRow()}
			r, cache, live := newSplitReconciler(t, st)
			m, err := NewMetrics(prometheus.NewRegistry())
			if err != nil {
				t.Fatal(err)
			}
			r.Metrics = m
			foreignPod(t, cache, r, tc.noOwner)
			pod := foreignPod(t, live, r, tc.noOwner)

			if _, err := r.Reconcile(t.Context(), runningReq); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if len(st.tos) != 1 || st.tos[0] != statemachine.Failed || st.why[0] != ReasonPodLost {
				t.Fatalf("transitions = %v %v, want [FAILED] [pod_lost]", st.tos, st.why)
			}
			for name, c := range map[string]client.Client{"cache": cache, "live": live} {
				var got corev1.Pod
				if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), &got); err != nil || got.DeletionTimestamp != nil {
					t.Errorf("%s: foreign Pod deleted or deleting (err %v), want it left alone", name, err)
				}
			}
			if got := promtestutil.ToFloat64(m.Orphans.WithLabelValues("pod")); got != 1 {
				t.Errorf("orphans_total{kind=pod} = %v, want 1", got)
			}
		})
	}

	t.Run("cache shows foreign Pod, API has ours: not lost", func(t *testing.T) {
		st := &reasonStore{row: runningRow()}
		r, cache, live := newSplitReconciler(t, st)
		foreignPod(t, cache, r, false)
		readyPod(t, live, r)
		if _, err := r.Reconcile(t.Context(), runningReq); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if len(st.tos) != 0 {
			t.Fatalf("transitions = %v %v, want none while the API server has our Pod", st.tos, st.why)
		}
	})
}

// TestRunningForeignPodOrphanCountedOnce: gpucp_controller_orphans_total
// {kind="pod"} counts a foreign Pod exactly once per committed FAILED
// pod_lost: not skipped when the CR status patch after the commit fails, and
// not counted for a CAS that lost (ErrStaleVersion) before the retry won.
func TestRunningForeignPodOrphanCountedOnce(t *testing.T) {
	setup := func(t *testing.T, st *reasonStore) (*InstanceReconciler, client.Client, *Metrics) {
		t.Helper()
		r, cache, live := newSplitReconciler(t, st)
		m, err := NewMetrics(prometheus.NewRegistry())
		if err != nil {
			t.Fatal(err)
		}
		r.Metrics = m
		foreignPod(t, cache, r, true)
		foreignPod(t, live, r, true)
		return r, cache, m
	}
	orphans := func(m *Metrics) float64 { return promtestutil.ToFloat64(m.Orphans.WithLabelValues("pod")) }

	t.Run("status patch fails after the commit: still counted once", func(t *testing.T) {
		st := &reasonStore{row: runningRow()}
		r, cache, m := setup(t, st)
		errPatch := errors.New("apiserver unavailable")
		r.Client = interceptor.NewClient(cache.(client.WithWatch), interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
				if sub == "status" {
					if cr, ok := obj.(*gpuv1alpha1.GPUInstance); ok && cr.Status.State == gpuv1alpha1.StateFailed {
						return errPatch
					}
				}
				return c.SubResource(sub).Patch(ctx, obj, p, opts...)
			},
		})
		if _, err := r.Reconcile(t.Context(), runningReq); !errors.Is(err, errPatch) {
			t.Fatalf("reconcile err = %v, want the status patch error", err)
		}
		if len(st.tos) != 1 || st.tos[0] != statemachine.Failed {
			t.Fatalf("transitions = %v, want one committed FAILED", st.tos)
		}
		if got := orphans(m); got != 1 {
			t.Errorf("orphans_total{kind=pod} = %v after commit + failed patch, want 1", got)
		}
	})

	t.Run("stale then success: counted once", func(t *testing.T) {
		st := &reasonStore{row: runningRow(), staleFirst: true}
		r, _, m := setup(t, st)
		res, err := r.Reconcile(t.Context(), runningReq)
		if err != nil || res.RequeueAfter != staleRequeueDelay {
			t.Fatalf("first reconcile: res %+v err %v, want stale requeue", res, err)
		}
		if got := orphans(m); got != 0 {
			t.Errorf("orphans_total{kind=pod} = %v after a lost CAS, want 0", got)
		}
		if _, err := r.Reconcile(t.Context(), runningReq); err != nil {
			t.Fatalf("second reconcile: %v", err)
		}
		if len(st.tos) != 2 || st.row.State != statemachine.Failed {
			t.Fatalf("transitions = %v, row %s; want a lost CAS then FAILED", st.tos, st.row.State)
		}
		if got := orphans(m); got != 1 {
			t.Errorf("orphans_total{kind=pod} = %v after stale then success, want 1", got)
		}
	})
}
