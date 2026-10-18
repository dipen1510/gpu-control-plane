package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// Node labels and resources node sync reads (S2-1, ADR-0002 §Node sync).
const (
	LabelGPUType                     = "gpu.example.com/type"
	LabelZone                        = "topology.kubernetes.io/zone"
	ResourceGPU  corev1.ResourceName = "gpu.example.com/gpu"
)

// NodeSyncer writes one node's inventory row. *store.Store satisfies it.
type NodeSyncer interface {
	SyncNode(ctx context.Context, obs store.NodeObservation) (bool, error)
}

// NodeReconciler mirrors Kubernetes GPU Nodes into the nodes table (ADR-0002
// §Node sync, D39). It has its own queue (key = node name) and reads Nodes
// from the Manager's shared informer. It never writes nodes.allocated.
// Deleted Nodes and conflicts are S2-19; here a missing Node is a no-op.
type NodeReconciler struct {
	Reader  client.Reader // the Manager's cache-backed client
	Nodes   NodeSyncer
	Workers int // <= 0 means DefaultNodeWorkers
}

// Reconcile upserts the row for one GPU Node.
func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var node corev1.Node
	if err := r.Reader.Get(ctx, req.NamespacedName, &node); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil // deleted Node: S2-19
		}
		return ctrl.Result{}, fmt.Errorf("get node %s: %w", req.Name, err)
	}
	obs, ok := observeNode(&node)
	if !ok {
		return ctrl.Result{}, nil
	}
	wrote, err := r.Nodes.SyncNode(ctx, obs)
	if err != nil {
		return ctrl.Result{}, err // the queue retries with backoff
	}
	if wrote {
		log.FromContext(ctx).Info("node synced", "node", obs.Name, "status", obs.Status,
			"capacity", obs.Capacity, "gpu_type", obs.GPUType)
	}
	return ctrl.Result{}, nil
}

// SetupWithManager registers the reconciler on mgr, watching only Nodes that
// carry the GPU type label.
func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	workers := r.Workers
	if workers <= 0 {
		workers = DefaultNodeWorkers
	}
	isGPU := predicate.NewPredicateFuncs(func(o client.Object) bool {
		_, ok := o.GetLabels()[LabelGPUType]
		return ok
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}, builder.WithPredicates(isGPU)).
		Named("node").
		WithOptions(crcontroller.Options{MaxConcurrentReconciles: workers}).
		Complete(r)
}

// observeNode maps a Node to its row. ok is false for non-GPU Nodes.
func observeNode(n *corev1.Node) (store.NodeObservation, bool) {
	gpuType, ok := n.Labels[LabelGPUType]
	if !ok || gpuType == "" {
		return store.NodeObservation{}, false
	}
	var capacity int
	if q, ok := n.Status.Capacity[ResourceGPU]; ok {
		capacity = int(q.Value())
	}
	return store.NodeObservation{
		Name:          n.Name,
		GPUType:       gpuType,
		Capacity:      capacity,
		FailureDomain: n.Labels[LabelZone],
		Status:        nodeStatus(n),
	}, true
}

// nodeStatus applies the ADR-0002 table: Ready not True → NOT_READY (a
// missing condition counts as not ready); Ready and cordoned → CORDONED;
// otherwise READY.
func nodeStatus(n *corev1.Node) string {
	ready := false
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			ready = c.Status == corev1.ConditionTrue
			break
		}
	}
	switch {
	case !ready:
		return store.NodeNotReady
	case n.Spec.Unschedulable:
		return store.NodeCordoned
	default:
		return store.NodeReady
	}
}
