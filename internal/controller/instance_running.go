package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// ReasonPodLost is the failure_reason of RUNNING → FAILED when the instance's
// Pod (or its node) is gone (ADR-0002 D25, D29). Metering can tell it apart
// from a user delete, which ends TERMINATING → TERMINATED.
const ReasonPodLost = "pod_lost"

// reconcileRunning is the RUNNING row of ADR-0002 §Reconcile, by DB state:
// Pod missing or not ours (confirmed by a live read) or exited (Failed or
// Succeeded) → delete our Pod (never a foreign one); once the Pod is
// confirmed gone → FAILED pod_lost, which releases GPUs and quota in the
// same transaction (store.Transition: HoldsCapacity(from) && IsTerminal(to)).
// A healthy Pod writes nothing.
func (r *InstanceReconciler) reconcileRunning(ctx context.Context, inst store.Instance) (ctrl.Result, error) {
	cr, err := r.ensureCR(ctx, inst)
	if err != nil {
		return ctrl.Result{}, err
	}
	// A crash between the transition and the status patch is healed here.
	if err := r.syncStatus(ctx, cr, inst); err != nil {
		return ctrl.Result{}, err
	}
	if cr.DeletionTimestamp != nil {
		// The CR is being deleted by hand: that is a delete (the finalizer
		// path moves the row to TERMINATING, S2-8), so its Pod going away
		// must not be recorded as pod_lost (D29).
		return ctrl.Result{}, nil
	}
	gone, foreign, err := r.lostPodGone(ctx, cr)
	if err != nil || !gone {
		return ctrl.Result{}, err
	}
	next, err := r.commitWithReason(ctx, inst, statemachine.Failed, ReasonPodLost)
	if err != nil {
		return ctrl.Result{}, err
	}
	if foreign {
		// Counted exactly once per committed FAILED: after the commit (a lost
		// CAS and retry does not count twice) and before the status patch (a
		// failed patch does not skip it; the retry sees a FAILED row and never
		// comes back here).
		logf.FromContext(ctx).Info("pod with the instance's name is not controlled by its GPUInstance; left in place",
			"pod", cr.Name, "gpuinstance_uid", cr.UID)
		if r.Metrics != nil {
			r.Metrics.Orphans.WithLabelValues("pod").Inc()
		}
	}
	return ctrl.Result{}, r.syncStatus(ctx, cr, next)
}

// lostPodGone reports whether a RUNNING instance's Pod is confirmed gone, so
// the instance may be failed with pod_lost. It is false for a healthy Pod.
// foreign is true when the only Pod with the instance's name is controlled by
// something else: it is not ours, so our Pod is gone, and it is never deleted.
//
//   - Missing from the cache (or present but not ours) is only a hint: the
//     cache may be behind the API server (D25), so it is confirmed with a live
//     read before it counts.
//   - A Pod of ours being deleted (preStop, a delete racing us, or our own
//     earlier delete) still holds its GPUs: errPodTerminating, wait.
//   - A Pod of ours that exited (Failed, or Succeeded: the main container is
//     `sleep infinity`, so any exit means the machine is gone) is deleted
//     first; the reconcile after it disappears confirms it gone (D21:
//     capacity is released only after the Pod is gone).
func (r *InstanceReconciler) lostPodGone(ctx context.Context, cr *gpuv1alpha1.GPUInstance) (gone, foreign bool, err error) {
	key := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	var pod corev1.Pod
	err = r.Client.Get(ctx, key, &pod)
	if apierrors.IsNotFound(err) || err == nil && !podBelongsTo(&pod, cr.Namespace, cr.Spec.InstanceID) {
		pod = corev1.Pod{}
		err = r.APIReader.Get(ctx, key, &pod)
		if apierrors.IsNotFound(err) {
			return true, false, nil
		}
		if err == nil && !podBelongsTo(&pod, cr.Namespace, cr.Spec.InstanceID) {
			return true, true, nil
		}
		if err == nil {
			logf.FromContext(ctx).V(1).Info("cache disagrees with API server about the Pod; using the live read", "pod", key)
		}
	}
	if err != nil {
		return false, false, fmt.Errorf("get Pod %s: %w", key, err)
	}
	if pod.DeletionTimestamp != nil {
		return false, false, fmt.Errorf("running Pod %s: %w", key, errPodTerminating)
	}
	if pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded {
		return false, false, nil
	}
	// The UID precondition makes sure we delete the Pod we saw exit.
	uid := pod.UID
	err = r.Client.Delete(ctx, &pod, client.Preconditions{UID: &uid})
	switch {
	case err == nil:
		logf.FromContext(ctx).Info("deleting exited Pod of running instance", "pod", key.Name, "phase", pod.Status.Phase)
	case apierrors.IsNotFound(err), apierrors.IsConflict(err):
		// Already gone or replaced: the next reconcile re-checks with a live read.
	default:
		return false, false, fmt.Errorf("delete exited Pod %s: %w", key, err)
	}
	// Not confirmed gone yet: its delete event (or the backstop requeue)
	// brings us back to confirm with a live read.
	return false, false, fmt.Errorf("running Pod %s: %w", key, errPodTerminating)
}
