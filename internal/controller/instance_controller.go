// Package controller holds the Kubernetes reconcilers (ADR-0002). In S2-4 the
// InstanceReconciler is a no-op frame registered on the Manager; the
// lifecycle logic arrives in S2-7 onward.
package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	crcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
	"github.com/dipen1510/gpu-control-plane/internal/controller/poller"
)

// RBAC for the whole controller binary (ADR-0002). Leases and events are for
// leader election (off by default in M2a, on in M2b).
//
// +kubebuilder:rbac:groups=gpu.example.com,resources=gpuinstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gpu.example.com,resources=gpuinstances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gpu.example.com,resources=gpuinstances/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// InstanceReconciler drives a GPUInstance through its lifecycle. It is a
// no-op until S2-7.
type InstanceReconciler struct {
	// Workers is MaxConcurrentReconciles; zero means DefaultInstanceWorkers.
	Workers int
	// Metrics receives gpucp_controller_* observations; may be nil in tests.
	Metrics *Metrics
	// PollerEvents carries instance ids from the DB poller (S2-5) into the
	// same workqueue as the informers (D31). Nil: no poller source.
	PollerEvents <-chan poller.Event
	// Namespace of GPUInstance CRs; poller ids are keyed into it.
	Namespace string
}

// pollerRequest maps a poller id to the same queue key the GPUInstance
// informer produces for that instance's CR: {namespace, K8sName(id)}. One key
// per instance is what makes the workqueue's one-worker-per-key rule hold.
func pollerRequest(namespace, id string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{
		Namespace: namespace, Name: gpuv1alpha1.K8sName(id),
	}}
}

// pollerHandler enqueues pollerRequest(namespace, id) for every poller event.
func pollerHandler(namespace string) handler.TypedEventHandler[string, reconcile.Request] {
	return handler.TypedFuncs[string, reconcile.Request]{
		GenericFunc: func(_ context.Context, e poller.Event, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			q.Add(pollerRequest(namespace, e.Object))
		},
	}
}

// instanceIDFromCR returns the original instance id of a GPUInstance. The
// queue key holds only K8sName(id), which is never reversed (D37).
//
// TODO(S2-7): when the CR does not exist yet (a poller key for a new
// instance), Reconcile must find the id in the DB instead. K8sName is not
// reversible, so that lookup needs a decision (e.g. match on
// replace(id, '_', '-') or a stored k8s name column).
func instanceIDFromCR(cr *gpuv1alpha1.GPUInstance) (string, bool) {
	id, ok := cr.Labels[gpuv1alpha1.LabelInstanceID]
	return id, ok && id != ""
}

// Reconcile does nothing yet and never requeues.
func (r *InstanceReconciler) Reconcile(_ context.Context, _ ctrl.Request) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

// SetupWithManager registers the reconciler on mgr, watching GPUInstance.
func (r *InstanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&gpuv1alpha1.GPUInstance{}).
		Named("gpuinstance")
	if r.PollerEvents != nil {
		b = b.WatchesRawSource(source.Channel(r.PollerEvents, pollerHandler(r.Namespace)))
	}
	return b.
		WithOptions(crcontroller.Options{
			MaxConcurrentReconciles: workersOrDefault(r.Workers, DefaultInstanceWorkers),
			RateLimiter:             NewRateLimiter(),
		}).
		Complete(r)
}
