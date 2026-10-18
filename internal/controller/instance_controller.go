// Package controller holds the Kubernetes reconcilers (ADR-0002).
package controller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
	"github.com/dipen1510/gpu-control-plane/internal/controller/poller"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
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

// InstanceStore is what the InstanceReconciler needs from Postgres.
// *store.Store satisfies it.
type InstanceStore interface {
	// GetByK8sName resolves a queue key name to its row (D40); wraps
	// store.ErrNotFound when no row has that name.
	GetByK8sName(ctx context.Context, name string) (store.Instance, error)
	// Transition is the only way the controller changes state: one
	// transaction with the outbox row and a resource_version CAS.
	Transition(ctx context.Context, id string, to statemachine.State, expectedRV int64, reason string) (store.Instance, error)
}

// staleRequeueDelay is how soon a key is retried after ErrStaleVersion. Losing
// a CAS is normal (someone else moved the row first), not an error: re-read
// soon, without error-log noise or growing backoff.
const staleRequeueDelay = 100 * time.Millisecond

// terminatingRequeueDelay is the backstop recheck while an old Pod is still
// being deleted; normally its delete event wakes us first.
const terminatingRequeueDelay = time.Second

// errPodTerminating means the instance's Pod is being deleted. A terminating
// Pod (the kubelet keeps Ready=True through preStop) must never drive
// progress: wait until it is gone, then the next reconcile recreates it.
var errPodTerminating = errors.New("pod is terminating")

// InstanceReconciler drives a GPUInstance through its lifecycle (ADR-0002
// §Reconcile, by DB state). S2-7 implements SCHEDULED → PROVISIONING →
// RUNNING; other states are left alone until S2-8 … S2-11.
type InstanceReconciler struct {
	// Client reads from the Manager's cache and writes to the API server.
	Client client.Client
	// APIReader reads the API server directly; used after AlreadyExists,
	// when the cache has not seen our own create yet.
	APIReader client.Reader
	// Store is the source of truth. Nil makes Reconcile a no-op (tests and
	// tools that run no Postgres).
	Store InstanceStore
	// Pod shapes the fake GPU Pod; validated at startup by the caller.
	Pod PodConfig
	// Workers is MaxConcurrentReconciles; zero means DefaultInstanceWorkers.
	Workers int
	// Metrics receives gpucp_controller_* observations; may be nil in tests.
	Metrics *Metrics
	// PollerEvents carries instance ids from the DB poller (S2-5) into the
	// same workqueue as the informers (D31). Nil: no poller source.
	PollerEvents <-chan poller.Event
	// Namespace of GPUInstance CRs and their Pods; poller ids are keyed into it.
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

// instanceIDFromCR returns the original instance id of a GPUInstance from its
// label. The queue key holds only K8sName(id), which is never parsed back
// (D37); Reconcile resolves the key with InstanceStore.GetByK8sName (D40),
// which works before any CR exists.
func instanceIDFromCR(cr *gpuv1alpha1.GPUInstance) (string, bool) {
	id, ok := cr.Labels[gpuv1alpha1.LabelInstanceID]
	return id, ok && id != ""
}

// Reconcile takes one idempotent step for the instance behind req (key
// {Namespace, K8sName(id)}). Kubernetes is written first, then the DB
// (D21), so a crash leaves the DB behind reality and the next call
// converges. When nothing needs doing, nothing is written.
func (r *InstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.Store == nil || req.Namespace != r.Namespace {
		// A CR outside our namespace is not ours: its key would differ from
		// the poller's key for the same id and break one-worker-per-key.
		return ctrl.Result{}, nil
	}
	inst, err := r.Store.GetByK8sName(ctx, req.Name)
	if errors.Is(err, store.ErrNotFound) {
		// Orphan handling (row gone, CR left) is M2b; nothing to drive.
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile %s: %w", req.Name, err)
	}
	log := logf.FromContext(ctx).WithValues("instance_id", inst.ID, "state", inst.State, "rv", inst.ResourceVersion)
	ctx = logf.IntoContext(ctx, log)

	res, err := r.reconcileInstance(ctx, inst)
	if errors.Is(err, store.ErrStaleVersion) {
		log.V(1).Info("lost resource_version race; re-reading", "err", err)
		return ctrl.Result{RequeueAfter: staleRequeueDelay}, nil
	}
	if errors.Is(err, errPodTerminating) {
		log.V(1).Info("pod is terminating; waiting for it to be gone")
		return ctrl.Result{RequeueAfter: terminatingRequeueDelay}, nil
	}
	return res, err
}

// reconcileInstance dispatches on the DB state (the DB always wins).
func (r *InstanceReconciler) reconcileInstance(ctx context.Context, inst store.Instance) (ctrl.Result, error) {
	switch inst.State {
	case statemachine.Scheduled:
		cr, err := r.ensureCR(ctx, inst)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := r.ensurePod(ctx, cr); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.transition(ctx, cr, inst, statemachine.Provisioning)
	case statemachine.Provisioning:
		cr, err := r.ensureCR(ctx, inst)
		if err != nil {
			return ctrl.Result{}, err
		}
		// A crash between the transition and the status patch is healed here.
		if err := r.syncStatus(ctx, cr, inst); err != nil {
			return ctrl.Result{}, err
		}
		ready, err := r.podReady(ctx, cr)
		if err != nil || !ready {
			// Not Ready yet (or not in the cache yet): the Pod event wakes us.
			// Boot failure and timeout are S2-10.
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.transition(ctx, cr, inst, statemachine.Running)
	case statemachine.Running:
		cr, err := r.ensureCR(ctx, inst)
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.syncStatus(ctx, cr, inst)
	default:
		// TERMINATING, TERMINATED, FAILED: S2-8 onward. PENDING: v2 queue.
		return ctrl.Result{}, nil
	}
}

// transition commits inst → to through store.Transition (outbox + rv CAS),
// counts it, then mirrors the returned row into the CR status.
func (r *InstanceReconciler) transition(ctx context.Context, cr *gpuv1alpha1.GPUInstance, inst store.Instance, to statemachine.State) error {
	next, err := r.Store.Transition(ctx, inst.ID, to, inst.ResourceVersion, "")
	if err != nil {
		return fmt.Errorf("reconcile %s: %w", inst.ID, err)
	}
	if r.Metrics != nil {
		r.Metrics.Transitions.WithLabelValues(string(inst.State), string(to), "").Inc()
	}
	logf.FromContext(ctx).Info("transitioned", "from", inst.State, "to", to, "new_rv", next.ResourceVersion)
	return r.syncStatus(ctx, cr, next)
}

// ensureCR returns the instance's GPUInstance, creating it (with finalizer)
// if missing and adding the finalizer if absent. AlreadyExists is success.
func (r *InstanceReconciler) ensureCR(ctx context.Context, inst store.Instance) (*gpuv1alpha1.GPUInstance, error) {
	key := types.NamespacedName{Namespace: r.Namespace, Name: gpuv1alpha1.K8sName(inst.ID)}
	cr := &gpuv1alpha1.GPUInstance{}
	err := r.Client.Get(ctx, key, cr)
	if apierrors.IsNotFound(err) {
		cr = newCR(r.Namespace, inst)
		err = r.Client.Create(ctx, cr)
		if err == nil {
			return cr, nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create GPUInstance %s: %w", key, err)
		}
		// Our cache is behind the API server (e.g. created just before a crash).
		cr = &gpuv1alpha1.GPUInstance{}
		err = r.APIReader.Get(ctx, key, cr)
	}
	if err != nil {
		return nil, fmt.Errorf("get GPUInstance %s: %w", key, err)
	}
	if !controllerutil.ContainsFinalizer(cr, gpuv1alpha1.FinalizerReleaseCapacity) {
		base := cr.DeepCopy()
		controllerutil.AddFinalizer(cr, gpuv1alpha1.FinalizerReleaseCapacity)
		if err := r.Client.Patch(ctx, cr, client.MergeFrom(base)); err != nil {
			return nil, fmt.Errorf("add finalizer to GPUInstance %s: %w", key, err)
		}
	}
	return cr, nil
}

// newCR builds the GPUInstance for inst; spec is copied once and immutable.
func newCR(namespace string, inst store.Instance) *gpuv1alpha1.GPUInstance {
	return &gpuv1alpha1.GPUInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:       gpuv1alpha1.K8sName(inst.ID),
			Namespace:  namespace,
			Labels:     map[string]string{gpuv1alpha1.LabelInstanceID: inst.ID},
			Finalizers: []string{gpuv1alpha1.FinalizerReleaseCapacity},
		},
		Spec: gpuv1alpha1.GPUInstanceSpec{
			InstanceID: inst.ID,
			TenantID:   inst.TenantID,
			GPUType:    inst.GPUType,
			GPUCount:   int32(min(max(inst.GPUCount, 0), math.MaxInt32)),
			NodeName:   inst.NodeID,
		},
	}
}

// ensurePod creates the fake GPU Pod for cr unless it exists. AlreadyExists
// is success, which is what makes a crash after Pod create converge with
// exactly one Pod. An existing Pod that is being deleted is errPodTerminating.
func (r *InstanceReconciler) ensurePod(ctx context.Context, cr *gpuv1alpha1.GPUInstance) error {
	key := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	var pod corev1.Pod
	err := r.Client.Get(ctx, key, &pod)
	if err == nil {
		if pod.DeletionTimestamp != nil {
			return fmt.Errorf("ensure Pod %s: %w", key, errPodTerminating)
		}
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get Pod %s: %w", key, err)
	}
	err = r.Client.Create(ctx, BuildPod(cr, r.Pod))
	if apierrors.IsAlreadyExists(err) {
		// Our cache is behind; check the live Pod is not one being deleted.
		if err := r.APIReader.Get(ctx, key, &pod); err != nil {
			return fmt.Errorf("get Pod %s: %w", key, err)
		}
		if pod.DeletionTimestamp != nil {
			return fmt.Errorf("ensure Pod %s: %w", key, errPodTerminating)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("create Pod %s: %w", key, err)
	}
	return nil
}

// podReady reports whether cr's Pod is in the cache with Ready=True and not
// being deleted (Ready stays True during preStop; it must not count).
func (r *InstanceReconciler) podReady(ctx context.Context, cr *gpuv1alpha1.GPUInstance) (bool, error) {
	key := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	var pod corev1.Pod
	if err := r.Client.Get(ctx, key, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get Pod %s: %w", key, err)
	}
	return pod.DeletionTimestamp == nil && isPodReady(&pod), nil
}

// isPodReady is the Pod's Ready condition.
func isPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// syncStatus mirrors inst into cr.status (DB wins). It writes only when the
// mirrored fields differ, so a converged instance causes no Kubernetes write.
func (r *InstanceReconciler) syncStatus(ctx context.Context, cr *gpuv1alpha1.GPUInstance, inst store.Instance) error {
	want := gpuv1alpha1.GPUInstanceStatus{
		State:           string(inst.State),
		ResourceVersion: inst.ResourceVersion,
		Reason:          inst.FailureReason,
		PodName:         cr.Name,
	}
	got := cr.Status
	if got.State == want.State && got.ResourceVersion == want.ResourceVersion &&
		got.Reason == want.Reason && got.PodName == want.PodName {
		return nil
	}
	base := cr.DeepCopy()
	now := metav1.Now()
	want.LastSyncTime = &now
	cr.Status = want
	if err := r.Client.Status().Patch(ctx, cr, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("patch GPUInstance %s/%s status: %w", cr.Namespace, cr.Name, err)
	}
	return nil
}

// isGPUInstancePod limits the Pod watch to our fake GPU Pods.
func isGPUInstancePod(o client.Object) bool {
	return o.GetLabels()[PodAppLabel] == PodAppValue
}

// SetupWithManager registers the reconciler on mgr. GPUInstance events, Pod
// events (mapped through the Pod's controller ownerReference to its CR's
// name) and poller ids all produce the same key {Namespace, K8sName(id)}, so
// one queue key per instance holds (D31).
func (r *InstanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&gpuv1alpha1.GPUInstance{}).
		Owns(&corev1.Pod{}, builder.WithPredicates(predicate.NewPredicateFuncs(isGPUInstancePod))).
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
