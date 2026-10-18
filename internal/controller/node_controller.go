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
	"sigs.k8s.io/controller-runtime/pkg/event"
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

// NodeSyncer writes node inventory rows. *store.Store satisfies it. None of
// its methods writes nodes.allocated (D15).
type NodeSyncer interface {
	SyncNode(ctx context.Context, obs store.NodeObservation) (store.NodeSyncResult, error)
	MarkNodeNotReady(ctx context.Context, name string) (bool, error)
	SweepNodes(ctx context.Context, present []string) ([]string, error)
	NodeCounts(ctx context.Context) ([]store.NodeCount, error)
}

// NodeReconciler mirrors Kubernetes GPU Nodes into the nodes table (ADR-0002
// §Node sync, D39). It has its own queue (key = node name) and reads Nodes
// from the Manager's shared informer. It never writes nodes.allocated.
// A deleted Node, or one that lost its GPU type label, keeps its row and is
// marked NOT_READY (S2-19).
type NodeReconciler struct {
	Reader  client.Reader // the Manager's cache-backed client
	Nodes   NodeSyncer
	Metrics *NodeMetrics // nil: conflicts are only logged
	Workers int          // <= 0 means DefaultNodeWorkers
}

// Reconcile upserts the row for one GPU Node, or marks it NOT_READY when the
// Node is gone or no longer carries the GPU type label.
func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var node corev1.Node
	if err := r.Reader.Get(ctx, req.NamespacedName, &node); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.markNotReady(ctx, req.Name, "node deleted")
		}
		return ctrl.Result{}, fmt.Errorf("get node %s: %w", req.Name, err)
	}
	obs, ok := observeNode(&node)
	if !ok {
		// Only reaches here if the label was just removed (the predicate
		// passes updates where the old object had it); no row → no-op.
		return ctrl.Result{}, r.markNotReady(ctx, req.Name, "gpu type label removed")
	}
	res, err := r.Nodes.SyncNode(ctx, obs)
	if err != nil {
		return ctrl.Result{}, err // the queue retries with backoff
	}
	logger := log.FromContext(ctx)
	newCap, newType := r.Metrics.record(obs.Name, res.CapacityConflict, res.TypeConflict)
	if newCap {
		logger.Info("node capacity below allocated; keeping stored capacity", "node", obs.Name,
			"reported_capacity", obs.Capacity, "conflict", "capacity")
	}
	if newType {
		logger.Info("node gpu type label differs from stored gpu_type; keeping stored type", "node", obs.Name,
			"label_gpu_type", obs.GPUType, "stored_gpu_type", res.StoredGPUType, "conflict", "gpu_type")
	}
	if res.Wrote {
		logger.Info("node synced", "node", obs.Name, "status", obs.Status,
			"capacity", obs.Capacity, "capacity_known", obs.CapacityKnown, "gpu_type", res.StoredGPUType)
	}
	return ctrl.Result{}, nil
}

func (r *NodeReconciler) markNotReady(ctx context.Context, name, why string) error {
	r.Metrics.forget(name)
	changed, err := r.Nodes.MarkNodeNotReady(ctx, name)
	if err != nil {
		return err
	}
	if changed {
		log.FromContext(ctx).Info("node marked NOT_READY", "node", name, "reason", why)
	}
	return nil
}

// SetupWithManager registers the reconciler on mgr, watching only Nodes that
// carry (or, for updates, carried) the GPU type label, so removing the label
// still reaches Reconcile.
func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	workers := r.Workers
	if workers <= 0 {
		workers = DefaultNodeWorkers
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}, builder.WithPredicates(gpuNodePredicate())).
		Named("node").
		WithOptions(crcontroller.Options{MaxConcurrentReconciles: workers}).
		Complete(r)
}

// gpuNodePredicate passes events for GPU Nodes; an update passes if either
// the old or the new object has the label, so label removal is seen.
func gpuNodePredicate() predicate.Funcs {
	p := predicate.NewPredicateFuncs(hasGPULabel)
	p.UpdateFunc = func(e event.UpdateEvent) bool {
		return hasGPULabel(e.ObjectOld) || hasGPULabel(e.ObjectNew)
	}
	return p
}

func hasGPULabel(o client.Object) bool {
	if o == nil {
		return false
	}
	_, ok := o.GetLabels()[LabelGPUType]
	return ok
}

// observeNode maps a Node to its row. ok is false for non-GPU Nodes.
func observeNode(n *corev1.Node) (store.NodeObservation, bool) {
	gpuType, ok := n.Labels[LabelGPUType]
	if !ok || gpuType == "" {
		return store.NodeObservation{}, false
	}
	// No GPU resource reported (kubelet starting) is "unknown", not 0.
	var capacity int
	q, known := n.Status.Capacity[ResourceGPU]
	if known {
		capacity = int(q.Value())
	}
	return store.NodeObservation{
		Name:          n.Name,
		GPUType:       gpuType,
		Capacity:      capacity,
		CapacityKnown: known,
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
