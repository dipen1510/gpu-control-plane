package controller

// S2-8: the delete path (ADR-0002 §Reconcile, by DB state, rows TERMINATING
// and TERMINATED / FAILED; "A CR deleted by hand"; Discussion D19, D21, D38).
// Capacity is released only by store.Transition → TERMINATED, and only after
// a live read confirms the Pod is gone (invariant 5). The CR's finalizer is
// removed only once the row is terminal.

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// errPodNotOwned means a Pod with the instance's name exists but does not
// belong to the instance (podBelongsTo, D42). It must not count as "our Pod
// exists"; the error is returned so the key backs off and the condition is
// visible.
var errPodNotOwned = errors.New("pod does not belong to this instance")

// errCRDeleting guards ensurePod: a GPUInstance being deleted never gets a
// new Pod.
var errCRDeleting = errors.New("GPUInstance is being deleted")

// podBelongsTo is the one ownership rule for instance Pods (D42), used by
// every "is this our Pod" check: ensurePod, podReady, the delete path and
// the RUNNING branch. pod belongs to instance id if and only if
//
//   - it is in namespace (the instance namespace), and
//   - its controller ownerReference is a gpu.example.com GPUInstance named
//     K8sName(id), and
//   - it carries the instance-id label with value id.
//
// The owner UID is deliberately not compared: names are unique per instance
// (D37), so any GPUInstance with that name, including an older copy of the
// CR, was made for this instance, and its Pod is this instance's machine.
// That keeps the delete path from releasing capacity while such a Pod runs,
// and makes the rule the same with or without a CR present.
func podBelongsTo(pod *corev1.Pod, namespace, id string) bool {
	if pod.Namespace != namespace || pod.Labels[gpuv1alpha1.LabelInstanceID] != id {
		return false
	}
	ref := metav1.GetControllerOf(pod)
	if ref == nil || ref.Kind != "GPUInstance" || ref.Name != gpuv1alpha1.K8sName(id) {
		return false
	}
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	return err == nil && gv.Group == gpuv1alpha1.GroupVersion.Group
}

// describeController names pod's controller for logs and errors.
func describeController(pod *corev1.Pod) string {
	ref := metav1.GetControllerOf(pod)
	if ref == nil {
		return "none"
	}
	return ref.APIVersion + "/" + ref.Kind + "/" + ref.Name
}

// existingPodUsable decides whether an existing Pod satisfies ensurePod: it
// must belong to cr's instance and not be being deleted.
func existingPodUsable(key types.NamespacedName, pod *corev1.Pod, cr *gpuv1alpha1.GPUInstance) error {
	if !podBelongsTo(pod, cr.Namespace, cr.Spec.InstanceID) {
		return fmt.Errorf("ensure Pod %s: controller %s, instance-id label %q: %w",
			key, describeController(pod), pod.Labels[gpuv1alpha1.LabelInstanceID], errPodNotOwned)
	}
	if pod.DeletionTimestamp != nil {
		return fmt.Errorf("ensure Pod %s: %w", key, errPodTerminating)
	}
	return nil
}

// instanceKey is the CR and Pod key of an instance (same name for both).
func (r *InstanceReconciler) instanceKey(id string) types.NamespacedName {
	return types.NamespacedName{Namespace: r.Namespace, Name: gpuv1alpha1.K8sName(id)}
}

// cachedCR returns the instance's GPUInstance from the cache, or nil if the
// cache has none. Never creates one: the delete path must not resurrect a CR.
func (r *InstanceReconciler) cachedCR(ctx context.Context, id string) (*gpuv1alpha1.GPUInstance, error) {
	cr := &gpuv1alpha1.GPUInstance{}
	err := r.Client.Get(ctx, r.instanceKey(id), cr)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get GPUInstance %s: %w", r.instanceKey(id), err)
	}
	return cr, nil
}

// commit runs store.Transition (one tx: state, outbox row, rv CAS, and the
// capacity release on → terminal), counts it and logs it. It does not touch
// the CR; callers mirror the returned row when a CR exists.
func (r *InstanceReconciler) commit(ctx context.Context, inst store.Instance, to statemachine.State) (store.Instance, error) {
	next, err := r.Store.Transition(ctx, inst.ID, to, inst.ResourceVersion, "")
	if err != nil {
		return store.Instance{}, fmt.Errorf("reconcile %s: %w", inst.ID, err)
	}
	if r.Metrics != nil {
		r.Metrics.Transitions.WithLabelValues(string(inst.State), string(to), "").Inc()
	}
	logf.FromContext(ctx).Info("transitioned", "from", inst.State, "to", to, "new_rv", next.ResourceVersion)
	return next, nil
}

// terminateIfCRDeleted handles `kubectl delete gpuinstance` on a live
// instance (SCHEDULED, PROVISIONING, RUNNING): the finalizer holds the CR,
// and the row is moved to TERMINATING so the normal delete path runs. It
// returns the row to continue with (unchanged if the CR is not being
// deleted). A deleting CR therefore never reaches ensurePod.
func (r *InstanceReconciler) terminateIfCRDeleted(ctx context.Context, inst store.Instance) (store.Instance, error) {
	switch inst.State {
	case statemachine.Scheduled, statemachine.Provisioning, statemachine.Running:
	default:
		return inst, nil
	}
	cr, err := r.cachedCR(ctx, inst.ID)
	if err != nil || cr == nil || cr.DeletionTimestamp == nil {
		return inst, err
	}
	logf.FromContext(ctx).Info("GPUInstance deleted while instance is live; terminating")
	next, err := r.commit(ctx, inst, statemachine.Terminating)
	if err != nil {
		return store.Instance{}, err
	}
	return next, r.syncStatus(ctx, cr, next)
}

// reconcileTerminating: delete the Pod (its preStop runs); once a live read
// confirms it is gone → TERMINATED, which releases GPUs and quota in the same
// transaction; then remove the finalizer and delete the CR. While the Pod
// still exists nothing is released; its delete event (or the backstop
// requeue) brings us back. Never sleeps.
func (r *InstanceReconciler) reconcileTerminating(ctx context.Context, inst store.Instance) (ctrl.Result, error) {
	cr, err := r.cachedCR(ctx, inst.ID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if cr != nil {
		// The API's DELETE committed TERMINATING; mirror it (DB wins).
		if err := r.syncStatus(ctx, cr, inst); err != nil {
			return ctrl.Result{}, err
		}
	}
	gone, err := r.ensurePodGone(ctx, inst.ID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !gone {
		return ctrl.Result{RequeueAfter: terminatingRequeueDelay}, nil
	}
	if _, err := r.commit(ctx, inst, statemachine.Terminated); err != nil {
		return ctrl.Result{}, err
	}
	// A crash here leaves a CR on a terminal row; the next reconcile
	// (reconcileTerminal) removes it.
	return ctrl.Result{}, r.removeCR(ctx, cr)
}

// reconcileTerminal: TERMINATED / FAILED rows are never written. Make sure
// the Pod is gone, then remove the finalizer and delete the CR (D38). The
// finalizer stays while a Pod still exists.
func (r *InstanceReconciler) reconcileTerminal(ctx context.Context, inst store.Instance) (ctrl.Result, error) {
	cr, err := r.cachedCR(ctx, inst.ID)
	if err != nil {
		return ctrl.Result{}, err
	}
	gone, err := r.ensurePodGone(ctx, inst.ID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !gone {
		return ctrl.Result{RequeueAfter: terminatingRequeueDelay}, nil
	}
	return ctrl.Result{}, r.removeCR(ctx, cr)
}

// ensurePodGone deletes the instance's Pod (graceful: preStop runs) and
// reports true only when a live read from the API server shows no Pod that
// belongs to the instance. The cache alone never confirms "gone": an empty or
// lagging cache must not release capacity while a Pod still runs
// (invariant 5).
//
// We never delete a Pod that does not belong to the instance (podBelongsTo,
// D42). Such a Pod is not this instance's machine, so for this instance its
// Pod is gone: it is logged and left in place, and the delete path continues.
func (r *InstanceReconciler) ensurePodGone(ctx context.Context, id string) (bool, error) {
	key := r.instanceKey(id)
	var pod corev1.Pod
	err := r.Client.Get(ctx, key, &pod)
	if apierrors.IsNotFound(err) || err == nil && !podBelongsTo(&pod, r.Namespace, id) {
		// A cached "missing" or "not ours" is only a hint: confirm it live.
		pod = corev1.Pod{}
		err = r.APIReader.Get(ctx, key, &pod)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err == nil && !podBelongsTo(&pod, r.Namespace, id) {
			logf.FromContext(ctx).Info("Pod with the instance's name does not belong to it; leaving it in place",
				"pod", key.Name, "pod_uid", pod.UID, "controller", describeController(&pod))
			return true, nil
		}
	}
	if err != nil {
		return false, fmt.Errorf("get Pod %s: %w", key, err)
	}
	if pod.DeletionTimestamp != nil {
		return false, nil // preStop running; its delete event wakes us
	}
	// The UID precondition makes sure we delete exactly the Pod we looked at.
	err = r.Client.Delete(ctx, &pod, client.Preconditions{UID: &pod.UID})
	switch {
	case err == nil:
		logf.FromContext(ctx).Info("deleting Pod", "pod", key.Name)
	case apierrors.IsNotFound(err), apierrors.IsConflict(err):
		// Already gone or replaced: the next reconcile confirms with a live read.
	default:
		return false, fmt.Errorf("delete Pod %s: %w", key, err)
	}
	return false, nil
}

// removeCR removes the release-capacity finalizer and deletes cr. Callers
// guarantee the row is terminal and the Pod is gone. NotFound is success; nil
// cr is a no-op.
func (r *InstanceReconciler) removeCR(ctx context.Context, cr *gpuv1alpha1.GPUInstance) error {
	if cr == nil {
		return nil
	}
	key := client.ObjectKeyFromObject(cr)
	if controllerutil.ContainsFinalizer(cr, gpuv1alpha1.FinalizerReleaseCapacity) {
		base := cr.DeepCopy()
		controllerutil.RemoveFinalizer(cr, gpuv1alpha1.FinalizerReleaseCapacity)
		// Optimistic lock: the merge patch replaces the whole finalizer list,
		// so never write it over a version we have not seen.
		err := r.Client.Patch(ctx, cr, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("remove finalizer from GPUInstance %s: %w", key, err)
		}
	}
	if cr.DeletionTimestamp != nil {
		return nil // already deleting; removing the finalizer lets it go
	}
	err := r.Client.Delete(ctx, cr, client.Preconditions{UID: &cr.UID})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete GPUInstance %s: %w", key, err)
	}
	logf.FromContext(ctx).Info("deleted GPUInstance", "cr", key.Name)
	return nil
}
