package controller

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/seed"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

// lifecycleEnv is a real API server (envtest, no kubelet) with the CRD
// installed, a fresh Postgres database with seeded nodes, and a direct
// (uncached) client for the test's own reads and writes.
type lifecycleEnv struct {
	env *envtest.Environment
	db  *pgxpool.Pool
	st  *store.Store
	c   client.WithWatch
}

// newLifecycleEnv skips unless both KUBEBUILDER_ASSETS and DATABASE_URL are
// set (`make test-envtest` with Postgres up).
func newLifecycleEnv(t *testing.T) *lifecycleEnv {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run `make test-envtest`")
	}
	db := testutil.NewDB(t) // skips if DATABASE_URL is unset
	nodes, err := seed.Nodes(4)
	if err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	if err := seed.Upsert(t.Context(), db, nodes); err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "deploy", "crds")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})
	s, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.NewWithWatch(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	if err := c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: DefaultInstanceNamespace}}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	return &lifecycleEnv{env: env, db: db, st: store.New(db, scheduler.New(nil)), c: c}
}

// startManager runs the Manager (informers + reconciler), with the DB poller
// if withPoller.
func (e *lifecycleEnv) startManager(t *testing.T, withPoller bool) {
	t.Helper()
	opts := Options{MetricsAddr: "0", ProbeAddr: "0", SkipNameValidation: true, Instances: e.st}
	if withPoller {
		opts.PollDB, opts.PollInterval = e.db, 100*time.Millisecond
	}
	mgr, err := NewManager(e.env.Config, opts)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("manager stopped with error: %v", err)
		}
	})
}

// create makes a SCHEDULED instance through store.Create (outbox row
// instance.scheduled), exactly as the API does.
func (e *lifecycleEnv) create(t *testing.T, id string) store.Instance {
	t.Helper()
	h := sha256.Sum256([]byte(id))
	res, err := e.st.Create(t.Context(), store.CreateRequest{
		ID: id, TenantID: "t1", GPUType: "H100", GPUCount: 2, IdempotencyKey: "k-" + id, RequestHash: h[:],
	})
	if err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	return res.Instance
}

func (e *lifecycleEnv) row(t *testing.T, id string) (statemachine.State, int64) {
	t.Helper()
	var state string
	var rv int64
	if err := e.db.QueryRow(t.Context(), `SELECT state, resource_version FROM instances WHERE id = $1`, id).Scan(&state, &rv); err != nil {
		t.Fatalf("read row %s: %v", id, err)
	}
	return statemachine.State(state), rv
}

func (e *lifecycleEnv) outbox(t *testing.T, id string) []string {
	t.Helper()
	rows, err := e.db.Query(t.Context(), `SELECT event_type FROM outbox WHERE aggregate_id = $1 ORDER BY id`, id)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var et string
		if err := rows.Scan(&et); err != nil {
			t.Fatalf("scan outbox: %v", err)
		}
		out = append(out, et)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	return out
}

// waitState polls until the row is in want (the reconciler runs async).
func (e *lifecycleEnv) waitState(t *testing.T, id string, want statemachine.State) int64 {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		got, rv := e.row(t, id)
		if got == want {
			return rv
		}
		if time.Now().After(deadline) {
			t.Fatalf("instance %s state = %s, want %s", id, got, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitPod polls until the instance's Pod exists and returns it.
func (e *lifecycleEnv) waitPod(t *testing.T, id string) *corev1.Pod {
	t.Helper()
	key := types.NamespacedName{Namespace: DefaultInstanceNamespace, Name: gpuv1alpha1.K8sName(id)}
	deadline := time.Now().Add(20 * time.Second)
	for {
		var pod corev1.Pod
		err := e.c.Get(t.Context(), key, &pod)
		if err == nil {
			return &pod
		}
		if time.Now().After(deadline) {
			t.Fatalf("Pod %s not created: %v", key, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// setPodReady plays the kubelet: envtest has none, so the test marks the Pod
// Ready itself.
func (e *lifecycleEnv) setPodReady(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
	if err := e.c.Status().Update(t.Context(), pod); err != nil {
		t.Fatalf("set Pod Ready: %v", err)
	}
}

func (e *lifecycleEnv) cr(t *testing.T, id string) *gpuv1alpha1.GPUInstance {
	t.Helper()
	var cr gpuv1alpha1.GPUInstance
	key := types.NamespacedName{Namespace: DefaultInstanceNamespace, Name: gpuv1alpha1.K8sName(id)}
	if err := e.c.Get(t.Context(), key, &cr); err != nil {
		t.Fatalf("get GPUInstance %s: %v", key, err)
	}
	return &cr
}

// waitCRStatus polls until the CR status mirrors state at rv.
func (e *lifecycleEnv) waitCRStatus(t *testing.T, id string, state statemachine.State, rv int64) *gpuv1alpha1.GPUInstance {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		cr := e.cr(t, id)
		if cr.Status.State == string(state) && cr.Status.ResourceVersion == rv {
			return cr
		}
		if time.Now().After(deadline) {
			t.Fatalf("CR status = %s rv %d, want %s rv %d", cr.Status.State, cr.Status.ResourceVersion, state, rv)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestReconcileHappyPath: the poller finds a new SCHEDULED row, the
// reconciler creates the CR (with finalizer) and the Pod, moves the row to
// PROVISIONING, and on Pod Ready to RUNNING; the CR status mirrors each
// committed row and the outbox has exactly one event per state, in order.
func TestReconcileHappyPath(t *testing.T) {
	e := newLifecycleEnv(t)
	e.startManager(t, true) // no CR yet: only the poller can find the row
	inst := e.create(t, "inst_happy01")

	e.waitState(t, inst.ID, statemachine.Provisioning)
	pod := e.waitPod(t, inst.ID)
	cr := e.waitCRStatus(t, inst.ID, statemachine.Provisioning, 2)

	if !slices.Contains(cr.Finalizers, gpuv1alpha1.FinalizerReleaseCapacity) {
		t.Errorf("CR finalizers = %v, want %s", cr.Finalizers, gpuv1alpha1.FinalizerReleaseCapacity)
	}
	if cr.Labels[gpuv1alpha1.LabelInstanceID] != inst.ID || cr.Spec.InstanceID != inst.ID ||
		cr.Spec.NodeName != inst.NodeID || cr.Spec.GPUCount != 2 || cr.Spec.GPUType != "H100" || cr.Spec.TenantID != "t1" {
		t.Errorf("CR = labels %v spec %+v, want copied from row %+v", cr.Labels, cr.Spec, inst)
	}
	if pod.Spec.NodeName != inst.NodeID || len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].UID != cr.UID {
		t.Errorf("Pod node %q owners %+v, want node %q owned by CR uid %s", pod.Spec.NodeName, pod.OwnerReferences, inst.NodeID, cr.UID)
	}
	if cr.Status.PodName != pod.Name {
		t.Errorf("CR status.podName = %q, want %q", cr.Status.PodName, pod.Name)
	}

	e.setPodReady(t, pod)
	rv := e.waitState(t, inst.ID, statemachine.Running)
	if rv != 3 {
		t.Errorf("RUNNING rv = %d, want 3", rv)
	}
	e.waitCRStatus(t, inst.ID, statemachine.Running, 3)

	want := []string{"instance.scheduled", "instance.provisioning", "instance.running"}
	if got := e.outbox(t, inst.ID); !slices.Equal(got, want) {
		t.Errorf("outbox = %v, want %v", got, want)
	}
}

// TestReconcileCrashAfterPodCreate: a previous controller created the CR and
// the Pod and crashed before the DB transition. The new one must converge to
// PROVISIONING with the same single Pod (AlreadyExists is success).
func TestReconcileCrashAfterPodCreate(t *testing.T) {
	e := newLifecycleEnv(t)
	inst := e.create(t, "inst_crash01")

	// What the crashed controller left behind.
	cr := newCR(DefaultInstanceNamespace, inst)
	if err := e.c.Create(t.Context(), cr); err != nil {
		t.Fatalf("pre-create CR: %v", err)
	}
	pre := BuildPod(cr, DefaultPodConfig())
	if err := e.c.Create(t.Context(), pre); err != nil {
		t.Fatalf("pre-create Pod: %v", err)
	}

	e.startManager(t, false) // the pre-created CR's informer event is the trigger
	e.waitState(t, inst.ID, statemachine.Provisioning)
	e.waitCRStatus(t, inst.ID, statemachine.Provisioning, 2)

	var pods corev1.PodList
	if err := e.c.List(t.Context(), &pods, client.InNamespace(DefaultInstanceNamespace),
		client.MatchingLabels{gpuv1alpha1.LabelInstanceID: inst.ID}); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 || pods.Items[0].UID != pre.UID {
		t.Fatalf("pods = %d (first uid %v), want exactly the pre-created Pod %s", len(pods.Items), podUIDs(pods.Items), pre.UID)
	}
	want := []string{"instance.scheduled", "instance.provisioning"}
	if got := e.outbox(t, inst.ID); !slices.Equal(got, want) {
		t.Errorf("outbox = %v, want %v", got, want)
	}
}

func podUIDs(pods []corev1.Pod) []types.UID {
	out := make([]types.UID, 0, len(pods))
	for _, p := range pods {
		out = append(out, p.UID)
	}
	return out
}

// countingStore counts Transition calls on top of the real store.
type countingStore struct {
	*store.Store
	transitions atomic.Int64
}

func (s *countingStore) Transition(ctx context.Context, id string, to statemachine.State, rv int64, reason string) (store.Instance, error) {
	s.transitions.Add(1)
	return s.Store.Transition(ctx, id, to, rv, reason)
}

// TestReconcileIsIdempotent: once an instance is RUNNING and its CR status
// is in sync, reconciling it again (twice) writes nothing to Postgres and
// nothing to Kubernetes. The reconciler under test runs without a Manager,
// with every Kubernetes write intercepted and counted.
func TestReconcileIsIdempotent(t *testing.T) {
	e := newLifecycleEnv(t)
	inst := e.create(t, "inst_idem01")

	var writes atomic.Int64
	count := func() { writes.Add(1) }
	wc := interceptor.NewClient(e.c, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.CreateOption) error {
			count()
			return c.Create(ctx, o, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.UpdateOption) error {
			count()
			return c.Update(ctx, o, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, o client.Object, p client.Patch, opts ...client.PatchOption) error {
			count()
			return c.Patch(ctx, o, p, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.DeleteOption) error {
			count()
			return c.Delete(ctx, o, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, o client.Object, opts ...client.SubResourceUpdateOption) error {
			count()
			return c.SubResource(sub).Update(ctx, o, opts...)
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, o client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
			count()
			return c.SubResource(sub).Patch(ctx, o, p, opts...)
		},
	})
	cs := &countingStore{Store: e.st}
	r := &InstanceReconciler{Client: wc, APIReader: e.c, Store: cs, Pod: DefaultPodConfig(), Namespace: DefaultInstanceNamespace}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: DefaultInstanceNamespace, Name: gpuv1alpha1.K8sName(inst.ID)}}

	// Drive to RUNNING with the same reconciler: SCHEDULED → PROVISIONING,
	// then Pod Ready → RUNNING.
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("reconcile SCHEDULED: %v", err)
	}
	e.setPodReady(t, e.waitPod(t, inst.ID))
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("reconcile PROVISIONING: %v", err)
	}
	if st, rv := e.row(t, inst.ID); st != statemachine.Running || rv != 3 {
		t.Fatalf("after drive: %s rv %d, want RUNNING rv 3", st, rv)
	}
	if writes.Load() == 0 || cs.transitions.Load() != 2 {
		t.Fatalf("drive made %d k8s writes and %d transitions; the counters are not wired", writes.Load(), cs.transitions.Load())
	}

	k8sBefore, outboxBefore := writes.Load(), len(e.outbox(t, inst.ID))
	for i := range 2 {
		res, err := r.Reconcile(t.Context(), req)
		if err != nil || res != (ctrl.Result{}) {
			t.Fatalf("reconcile RUNNING #%d: res %+v err %v", i+1, res, err)
		}
	}
	if got := writes.Load() - k8sBefore; got != 0 {
		t.Errorf("Kubernetes writes on RUNNING = %d, want 0", got)
	}
	if got := cs.transitions.Load(); got != 2 {
		t.Errorf("Transition calls = %d, want 2 (none on RUNNING)", got)
	}
	if st, rv := e.row(t, inst.ID); st != statemachine.Running || rv != 3 {
		t.Errorf("row = %s rv %d, want RUNNING rv 3 unchanged", st, rv)
	}
	if got := len(e.outbox(t, inst.ID)); got != outboxBefore {
		t.Errorf("outbox rows = %d, want %d unchanged", got, outboxBefore)
	}
}
