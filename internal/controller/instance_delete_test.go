package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// recStore is an in-memory InstanceStore that applies transitions (with the
// rv CAS) and records their targets, so a reconcile that continues from a
// returned row sees the same thing the next GetByK8sName would.
type recStore struct {
	mu  sync.Mutex
	row store.Instance
	to  []statemachine.State
	why []string // reason passed with each transition, aligned with to
}

func (s *recStore) GetByK8sName(_ context.Context, name string) (store.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if gpuv1alpha1.K8sName(s.row.ID) != name {
		return store.Instance{}, store.ErrNotFound
	}
	return s.row, nil
}

func (s *recStore) Transition(_ context.Context, _ string, to statemachine.State, rv int64, reason string) (store.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rv != s.row.ResourceVersion {
		return store.Instance{}, store.ErrStaleVersion
	}
	if !statemachine.CanTransition(s.row.State, to) {
		return store.Instance{}, store.ErrInvalidTransition
	}
	s.row.State, s.row.ResourceVersion = to, rv+1
	if to == statemachine.Failed {
		s.row.FailureReason = reason
	}
	s.to = append(s.to, to)
	s.why = append(s.why, reason)
	return s.row, nil
}

func (s *recStore) transitions() []statemachine.State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.to)
}

const holdFinalizer = "test.example.com/hold" // keeps a deleted Pod "terminating"

// deleteFixture creates the CR (uid "uid-cr", with finalizer) and, if
// withPod, a Pod it controls carrying podFinalizers.
func deleteFixture(t *testing.T, r *InstanceReconciler, row store.Instance, withPod bool, podFinalizers ...string) *gpuv1alpha1.GPUInstance {
	t.Helper()
	cr := newCR("gpucp", row)
	cr.UID = "uid-cr"
	if err := r.Client.Create(t.Context(), cr); err != nil {
		t.Fatal(err)
	}
	if withPod {
		pod := BuildPod(cr, r.Pod)
		pod.Finalizers = podFinalizers
		if err := r.Client.Create(t.Context(), pod); err != nil {
			t.Fatal(err)
		}
	}
	return cr
}

func (r *InstanceReconciler) testGetPod(t *testing.T) (*corev1.Pod, bool) {
	t.Helper()
	var p corev1.Pod
	err := r.Client.Get(t.Context(), types.NamespacedName{Namespace: "gpucp", Name: "inst-u1"}, &p)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return &p, true
}

func (r *InstanceReconciler) testGetCR(t *testing.T) (*gpuv1alpha1.GPUInstance, bool) {
	t.Helper()
	var cr gpuv1alpha1.GPUInstance
	err := r.Client.Get(t.Context(), types.NamespacedName{Namespace: "gpucp", Name: "inst-u1"}, &cr)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return &cr, true
}

var deleteReq = ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "gpucp", Name: "inst-u1"}}

// TestReconcileTerminatingReleasesOnlyAfterPodGone: TERMINATING deletes the
// Pod, mirrors the row into the CR status and does not transition while the
// Pod still exists (preStop). Once it is gone: → TERMINATED, CR removed.
func TestReconcileTerminatingReleasesOnlyAfterPodGone(t *testing.T) {
	row := scheduledRow()
	row.State, row.ResourceVersion = statemachine.Terminating, 4
	st := &recStore{row: row}
	r := newFakeReconciler(t, st)
	deleteFixture(t, r, row, true, holdFinalizer)

	res, err := r.Reconcile(t.Context(), deleteReq)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("result = %+v, want a backstop requeue while the Pod terminates", res)
	}
	if got := st.transitions(); len(got) != 0 {
		t.Fatalf("transitions = %v while the Pod exists, want none", got)
	}
	pod, ok := r.testGetPod(t)
	if !ok || pod.DeletionTimestamp == nil {
		t.Fatalf("Pod = %v (exists %v), want it deleting", pod, ok)
	}
	cr, ok := r.testGetCR(t)
	if !ok || !slices.Contains(cr.Finalizers, gpuv1alpha1.FinalizerReleaseCapacity) {
		t.Fatalf("CR gone or lost its finalizer before the row is terminal")
	}
	if cr.Status.State != string(statemachine.Terminating) || cr.Status.ResourceVersion != 4 {
		t.Errorf("CR status = %s rv %d, want TERMINATING rv 4", cr.Status.State, cr.Status.ResourceVersion)
	}

	// Second pass with the Pod still terminating: still nothing released.
	if _, err := r.Reconcile(t.Context(), deleteReq); err != nil {
		t.Fatal(err)
	}
	if got := st.transitions(); len(got) != 0 {
		t.Fatalf("transitions = %v while the Pod exists, want none", got)
	}

	// The kubelet finishes preStop.
	pod.Finalizers = nil
	if err := r.Client.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.testGetPod(t); ok {
		t.Fatal("setup: Pod still present after removing its last finalizer")
	}
	if _, err := r.Reconcile(t.Context(), deleteReq); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := st.transitions(); !slices.Equal(got, []statemachine.State{statemachine.Terminated}) {
		t.Fatalf("transitions = %v, want [TERMINATED]", got)
	}
	if _, ok := r.testGetCR(t); ok {
		t.Error("CR still present after TERMINATED")
	}
}

// TestReconcileTerminatingWithoutCR: an instance deleted through the API
// before the controller ever created its CR still reaches TERMINATED, and no
// CR is created for it.
func TestReconcileTerminatingWithoutCR(t *testing.T) {
	row := scheduledRow()
	row.State = statemachine.Terminating
	st := &recStore{row: row}
	r := newFakeReconciler(t, st)
	if _, err := r.Reconcile(t.Context(), deleteReq); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := st.transitions(); !slices.Equal(got, []statemachine.State{statemachine.Terminated}) {
		t.Errorf("transitions = %v, want [TERMINATED]", got)
	}
	if _, ok := r.testGetCR(t); ok {
		t.Error("CR created for a TERMINATING instance")
	}
}

// TestReconcileTerminalRowCleansUp: TERMINATED / FAILED rows are never
// written; the Pod is deleted first, and only once it is gone is the CR
// (finalizer and object) removed.
func TestReconcileTerminalRowCleansUp(t *testing.T) {
	for _, state := range []statemachine.State{statemachine.Terminated, statemachine.Failed} {
		t.Run(string(state), func(t *testing.T) {
			row := scheduledRow()
			row.State = state
			st := &recStore{row: row}
			r := newFakeReconciler(t, st)
			deleteFixture(t, r, row, true, holdFinalizer)

			if _, err := r.Reconcile(t.Context(), deleteReq); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			pod, ok := r.testGetPod(t)
			if !ok || pod.DeletionTimestamp == nil {
				t.Fatalf("Pod = %v (exists %v), want it deleting", pod, ok)
			}
			if cr, ok := r.testGetCR(t); !ok || !slices.Contains(cr.Finalizers, gpuv1alpha1.FinalizerReleaseCapacity) {
				t.Fatal("finalizer removed while the Pod still exists")
			}

			pod.Finalizers = nil
			if err := r.Client.Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Reconcile(t.Context(), deleteReq); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if _, ok := r.testGetCR(t); ok {
				t.Error("CR still present on a terminal row with no Pod")
			}
			if got := st.transitions(); len(got) != 0 {
				t.Errorf("transitions = %v on a terminal row, want none", got)
			}
			// Idempotent: once clean, another pass does nothing and fails nothing.
			if res, err := r.Reconcile(t.Context(), deleteReq); err != nil || res != (ctrl.Result{}) {
				t.Errorf("reconcile when clean: res %+v err %v", res, err)
			}
		})
	}
}

// TestReconcileCRDeletedByHand: a deleting CR on a live row never gets a
// Pod; the row goes TERMINATING (and, with no Pod, on to TERMINATED), and
// the finalizer is removed only after that.
func TestReconcileCRDeletedByHand(t *testing.T) {
	for _, state := range []statemachine.State{statemachine.Scheduled, statemachine.Provisioning, statemachine.Running} {
		t.Run(string(state), func(t *testing.T) {
			row := scheduledRow()
			row.State = state
			st := &recStore{row: row}
			r := newFakeReconciler(t, st)
			cr := deleteFixture(t, r, row, false)
			if err := r.Client.Delete(t.Context(), cr); err != nil {
				t.Fatal(err)
			}
			if cur, ok := r.testGetCR(t); !ok || cur.DeletionTimestamp == nil {
				t.Fatal("setup: CR not held by its finalizer")
			}

			if _, err := r.Reconcile(t.Context(), deleteReq); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if _, ok := r.testGetPod(t); ok {
				t.Error("a Pod was created for a GPUInstance being deleted")
			}
			want := []statemachine.State{statemachine.Terminating, statemachine.Terminated}
			if got := st.transitions(); !slices.Equal(got, want) {
				t.Errorf("transitions = %v, want %v", got, want)
			}
			if _, ok := r.testGetCR(t); ok {
				t.Error("CR still present after the row is terminal")
			}
		})
	}
}

// TestReconcileCRDeletedByHandHoldsFinalizer: with a Pod still terminating,
// the CR keeps its finalizer and the row stays TERMINATING.
func TestReconcileCRDeletedByHandHoldsFinalizer(t *testing.T) {
	row := scheduledRow()
	row.State = statemachine.Running
	st := &recStore{row: row}
	r := newFakeReconciler(t, st)
	cr := deleteFixture(t, r, row, true, holdFinalizer)
	if err := r.Client.Delete(t.Context(), cr); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), deleteReq); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := st.transitions(); !slices.Equal(got, []statemachine.State{statemachine.Terminating}) {
		t.Errorf("transitions = %v, want [TERMINATING]", got)
	}
	cur, ok := r.testGetCR(t)
	if !ok || !slices.Contains(cur.Finalizers, gpuv1alpha1.FinalizerReleaseCapacity) {
		t.Fatal("finalizer removed before the row is terminal")
	}
	if cur.Status.State != string(statemachine.Terminating) {
		t.Errorf("CR status = %s, want TERMINATING", cur.Status.State)
	}
}

// ownerCase is one D42 ownership variant, applied to a BuildPod of the
// instance's CR (uid "uid-cr").
type ownerCase struct {
	name string
	set  func(*corev1.Pod)
	ours bool
}

// ownerCases: ownership is scoped to the instance, not to a CR's UID (D42).
// A Pod belongs to the instance iff its controller is a gpu.example.com
// GPUInstance named K8sName(id) and it carries instance-id = id.
func ownerCases() []ownerCase {
	return []ownerCase{
		{name: "this CR", set: func(*corev1.Pod) {}, ours: true},
		{name: "older CR incarnation (same name, other uid)", ours: true,
			set: func(p *corev1.Pod) { p.OwnerReferences[0].UID = "uid-old" }},
		{name: "other GPUInstance name", set: func(p *corev1.Pod) { p.OwnerReferences[0].Name = "inst-other" }},
		{name: "other group", set: func(p *corev1.Pod) { p.OwnerReferences[0].APIVersion = "example.org/v1alpha1" }},
		{name: "other kind", set: func(p *corev1.Pod) { p.OwnerReferences[0].Kind = "ReplicaSet" }},
		{name: "instance-id label missing", set: func(p *corev1.Pod) { delete(p.Labels, gpuv1alpha1.LabelInstanceID) }},
		{name: "instance-id label wrong", set: func(p *corev1.Pod) { p.Labels[gpuv1alpha1.LabelInstanceID] = "inst_other" }},
		{name: "no controller", set: func(p *corev1.Pod) { p.OwnerReferences = nil }},
	}
}

// TestPodBelongsTo: the rule itself, including the namespace.
func TestPodBelongsTo(t *testing.T) {
	row := scheduledRow()
	cr := newCR("gpucp", row)
	cr.UID = "uid-cr"
	for _, tc := range ownerCases() {
		t.Run(tc.name, func(t *testing.T) {
			pod := BuildPod(cr, DefaultPodConfig())
			tc.set(pod)
			if got := podBelongsTo(pod, "gpucp", row.ID); got != tc.ours {
				t.Errorf("podBelongsTo = %v, want %v", got, tc.ours)
			}
		})
	}
	t.Run("other namespace", func(t *testing.T) {
		pod := BuildPod(cr, DefaultPodConfig())
		pod.Namespace = "default"
		if podBelongsTo(pod, "gpucp", row.ID) {
			t.Error("Pod in another namespace counted as the instance's")
		}
	})
	t.Run("other instance id", func(t *testing.T) {
		if podBelongsTo(BuildPod(cr, DefaultPodConfig()), "gpucp", "inst_other") {
			t.Error("Pod counted for another instance")
		}
	})
}

// TestEnsurePodOwnership: SCHEDULED with an existing Pod. One that belongs to
// the instance (even from an older CR) is AlreadyExists = ok → PROVISIONING;
// any other is not "our Pod": no transition, left alone, errPodNotOwned.
func TestEnsurePodOwnership(t *testing.T) {
	for _, tc := range ownerCases() {
		t.Run(tc.name, func(t *testing.T) {
			row := scheduledRow()
			st := &recStore{row: row}
			r := newFakeReconciler(t, st)
			cr := deleteFixture(t, r, row, false)
			pod := BuildPod(cr, r.Pod)
			tc.set(pod)
			if err := r.Client.Create(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			_, err := r.Reconcile(t.Context(), deleteReq)
			if tc.ours {
				if err != nil {
					t.Fatalf("reconcile: %v", err)
				}
				if got := st.transitions(); !slices.Equal(got, []statemachine.State{statemachine.Provisioning}) {
					t.Errorf("transitions = %v, want [PROVISIONING]", got)
				}
				return
			}
			if !errors.Is(err, errPodNotOwned) {
				t.Fatalf("err = %v, want errPodNotOwned", err)
			}
			if got := st.transitions(); len(got) != 0 {
				t.Errorf("transitions = %v, want none", got)
			}
			assertNotDeleted(t, r)
		})
	}
}

// TestPodReadyOwnership: PROVISIONING with a Ready Pod. Only a Pod that
// belongs to the instance moves it to RUNNING.
func TestPodReadyOwnership(t *testing.T) {
	for _, tc := range ownerCases() {
		t.Run(tc.name, func(t *testing.T) {
			row := scheduledRow()
			row.State = statemachine.Provisioning
			st := &recStore{row: row}
			r := newFakeReconciler(t, st)
			cr := deleteFixture(t, r, row, false)
			pod := BuildPod(cr, r.Pod)
			tc.set(pod)
			if err := r.Client.Create(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			if err := r.Client.Status().Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}

			_, err := r.Reconcile(t.Context(), deleteReq)
			if tc.ours {
				if err != nil {
					t.Fatalf("reconcile: %v", err)
				}
				if got := st.transitions(); !slices.Equal(got, []statemachine.State{statemachine.Running}) {
					t.Errorf("transitions = %v, want [RUNNING]", got)
				}
				return
			}
			if !errors.Is(err, errPodNotOwned) {
				t.Fatalf("err = %v, want errPodNotOwned", err)
			}
			if got := st.transitions(); len(got) != 0 {
				t.Errorf("transitions = %v with a foreign Ready Pod, want none", got)
			}
		})
	}
}

// TestEnsurePodRefusesDeletingCR: the guard inside ensurePod itself.
func TestEnsurePodRefusesDeletingCR(t *testing.T) {
	r := newFakeReconciler(t, &recStore{row: scheduledRow()})
	cr := newCR("gpucp", scheduledRow())
	now := metav1.Now()
	cr.DeletionTimestamp = &now
	if err := r.ensurePod(t.Context(), cr); !errors.Is(err, errCRDeleting) {
		t.Fatalf("err = %v, want errCRDeleting", err)
	}
	if _, ok := r.testGetPod(t); ok {
		t.Error("Pod created for a deleting CR")
	}
}

// createPodOwnedBy creates a Pod with the instance's name and labels whose
// owners are set by owners (applied to a BuildPod of a CR with uid "uid-cr").
// It carries holdFinalizer, so a delete leaves it visibly terminating.
func createPodOwnedBy(t *testing.T, r *InstanceReconciler, row store.Instance, owners func(*corev1.Pod)) {
	t.Helper()
	cr := newCR("gpucp", row)
	cr.UID = "uid-cr"
	pod := BuildPod(cr, r.Pod)
	pod.Finalizers = []string{holdFinalizer}
	owners(pod)
	if err := r.Client.Create(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
}

// assertNotDeleted: the Pod is still there and was never deleted.
func assertNotDeleted(t *testing.T, r *InstanceReconciler) {
	t.Helper()
	p, ok := r.testGetPod(t)
	if !ok {
		t.Fatal("Pod vanished")
	}
	if p.DeletionTimestamp != nil {
		t.Error("Pod that does not belong to the instance was deleted; we never delete a Pod we do not own")
	}
}

// finishPod plays the kubelet: drops holdFinalizer so the deleted Pod goes.
func finishPod(t *testing.T, r *InstanceReconciler) {
	t.Helper()
	p, ok := r.testGetPod(t)
	if !ok {
		t.Fatal("Pod vanished before the kubelet finished it")
	}
	p.Finalizers = nil
	if err := r.Client.Update(t.Context(), p); err != nil {
		t.Fatal(err)
	}
}

// TestDeletePathOwnership: TERMINATING, TERMINATED and FAILED rows, with and
// without a CR, over every D42 variant. A Pod that belongs to the instance
// (even from an older CR) is deleted and nothing is released or cleaned up
// until it is gone. Any other Pod is never deleted, and the instance's own
// Pod counts as gone: TERMINATED is reached and the CR is cleaned up.
func TestDeletePathOwnership(t *testing.T) {
	for _, state := range []statemachine.State{statemachine.Terminating, statemachine.Terminated, statemachine.Failed} {
		for _, withCR := range []bool{true, false} {
			for _, tc := range ownerCases() {
				name := fmt.Sprintf("%s/cr=%v/%s", state, withCR, tc.name)
				t.Run(name, func(t *testing.T) {
					row := scheduledRow()
					row.State = state
					st := &recStore{row: row}
					r := newFakeReconciler(t, st)
					if withCR {
						deleteFixture(t, r, row, false)
					}
					createPodOwnedBy(t, r, row, tc.set)
					var wantEnd []statemachine.State
					if state == statemachine.Terminating {
						wantEnd = []statemachine.State{statemachine.Terminated}
					}

					if _, err := r.Reconcile(t.Context(), deleteReq); err != nil {
						t.Fatalf("reconcile: %v", err)
					}
					if !tc.ours {
						assertNotDeleted(t, r)
					} else {
						p, ok := r.testGetPod(t)
						if !ok || p.DeletionTimestamp == nil {
							t.Fatalf("instance's Pod not deleted (exists %v)", ok)
						}
						if got := st.transitions(); len(got) != 0 {
							t.Fatalf("transitions = %v while the instance's Pod exists, want none", got)
						}
						if cr, ok := r.testGetCR(t); withCR && (!ok || !slices.Contains(cr.Finalizers, gpuv1alpha1.FinalizerReleaseCapacity)) {
							t.Fatal("CR or its finalizer removed while the instance's Pod exists")
						}
						finishPod(t, r)
						if _, err := r.Reconcile(t.Context(), deleteReq); err != nil {
							t.Fatalf("reconcile: %v", err)
						}
					}
					if got := st.transitions(); !slices.Equal(got, wantEnd) {
						t.Errorf("transitions = %v, want %v", got, wantEnd)
					}
					if _, ok := r.testGetCR(t); ok {
						t.Error("CR still present; cleanup should complete")
					}
					if !tc.ours {
						assertNotDeleted(t, r)
					}
				})
			}
		}
	}
}

// TestDeletePathConfirmsGoneLive: the cache says the Pod is missing, or that
// it does not belong to the instance, but the API server has the instance's
// Pod. Neither cached hint counts: nothing is released.
func TestDeletePathConfirmsGoneLive(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cache func(*testing.T, *InstanceReconciler, store.Instance)
	}{
		{name: "cache missing", cache: func(*testing.T, *InstanceReconciler, store.Instance) {}},
		{name: "cache foreign", cache: func(t *testing.T, r *InstanceReconciler, row store.Instance) {
			createPodOwnedBy(t, r, row, func(p *corev1.Pod) { p.OwnerReferences = nil })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := scheduledRow()
			row.State = statemachine.Terminating
			st := &recStore{row: row}
			r := newFakeReconciler(t, st) // r.Client plays the cache
			live := newFakeReconciler(t, st)
			r.APIReader = live.Client // the API server
			tc.cache(t, r, row)
			createPodOwnedBy(t, live, row, func(*corev1.Pod) {})

			if _, err := r.Reconcile(t.Context(), deleteReq); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if got := st.transitions(); len(got) != 0 {
				t.Errorf("transitions = %v while the API server has the instance's Pod, want none", got)
			}
		})
	}
}

// TestRunningForeignPodLostThenCleanedUp: S2-9 and S2-8 agree on one Pod
// across reconciles (D42). RUNNING with a Ready Pod that does not belong to
// the instance → FAILED pod_lost (S2-9) → terminal cleanup removes the CR
// (S2-8) → later passes are no-ops. The foreign Pod is never deleted.
func TestRunningForeignPodLostThenCleanedUp(t *testing.T) {
	for _, tc := range ownerCases() {
		if tc.ours {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			row := scheduledRow()
			row.State = statemachine.Running
			st := &recStore{row: row}
			r := newFakeReconciler(t, st)
			deleteFixture(t, r, row, false)
			createPodOwnedBy(t, r, row, tc.set)
			p, _ := r.testGetPod(t)
			p.Status = corev1.PodStatus{Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
			if err := r.Client.Status().Update(t.Context(), p); err != nil {
				t.Fatal(err)
			}

			const passes = 4
			for i := range passes {
				if _, err := r.Reconcile(t.Context(), deleteReq); err != nil {
					t.Fatalf("reconcile pass %d: %v", i+1, err)
				}
				assertNotDeleted(t, r)
			}
			if got := st.transitions(); !slices.Equal(got, []statemachine.State{statemachine.Failed}) {
				t.Fatalf("transitions = %v, want exactly [FAILED]", got)
			}
			if st.why[0] != ReasonPodLost {
				t.Errorf("reason = %q, want %q", st.why[0], ReasonPodLost)
			}
			if _, ok := r.testGetCR(t); ok {
				t.Error("CR still present after FAILED")
			}
		})
	}
}
