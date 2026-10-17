// Package controller holds the Kubernetes reconcilers (ADR-0002). In S2-4 the
// InstanceReconciler is a no-op frame registered on the Manager; the
// lifecycle logic arrives in S2-7 onward.
package controller

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
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
type InstanceReconciler struct{}

// Reconcile does nothing yet and never requeues.
func (r *InstanceReconciler) Reconcile(_ context.Context, _ ctrl.Request) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

// SetupWithManager registers the reconciler on mgr, watching GPUInstance.
func (r *InstanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gpuv1alpha1.GPUInstance{}).
		Named("gpuinstance").
		Complete(r)
}
