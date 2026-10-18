package controller

// Helpers for the S2-15 reconciler spec tests (spec_envtest_test.go). They
// drive the controller only from the outside: rows in Postgres, objects in a
// real kube-apiserver (envtest), and assertions on what the controller did
// to both. Nothing here knows how the reconciler is built.

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

const (
	// specHoldFinalizer is put on a Pod by the test to keep it in the
	// "deleting" state: deletionTimestamp set, object still present. It
	// stands in for a kubelet that is still running preStop.
	specHoldFinalizer = "spec.test.gpu.example.com/hold"
	// specSettle is how long a "nothing should happen" case waits. The
	// poller runs every 200ms and resyncs every 1s, so 3s gives the
	// controller many chances to do the wrong thing.
	specSettle = 3 * time.Second
	// specTimeout bounds every "eventually" wait.
	specTimeout = 30 * time.Second
	// specGPUs is the gpu_count of every seeded instance.
	specGPUs = 2
	// specSeedRV is the resource_version every seeded row starts at.
	specSeedRV = 3
)

// podState is the Kubernetes side of one table row (S2-15 acceptance).
type podState string

const (
	podAbsent     podState = "absent"
	podPending    podState = "pending"
	podReady      podState = "ready"
	podInitFailed podState = "init-failed"
	podDeleting   podState = "deleting"
)

// specEnv is one envtest API server + one Postgres database.
type specEnv struct {
	cfg *rest.Config
	c   client.Client // direct client: reads go to the API server, never a cache
	db  *pgxpool.Pool
	st  *store.Store
	ns  string
	// instances, if set, is what the controller uses instead of st (a test
	// wrapper that observes or gates its transitions).
	instances InstanceStore
}

// newSpecEnv starts envtest with the GPUInstance CRD, creates a fresh
// database, the instance namespace, and a fake kubelet. It registers an
// invariant 7 check that runs when the test ends. Skips without
// KUBEBUILDER_ASSETS or DATABASE_URL.
func newSpecEnv(t *testing.T) *specEnv {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run `make test-envtest`")
	}
	db := testutil.NewDB(t) // skips if DATABASE_URL is unset

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
		t.Fatalf("scheme: %v", err)
	}
	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	e := &specEnv{cfg: cfg, c: c, db: db, st: store.New(db, nil), ns: DefaultInstanceNamespace}
	if err := c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: e.ns}}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	e.startFakeKubelet(t)

	// Registered after NewDB, so it runs before the database is dropped.
	t.Cleanup(func() { e.assertInvariant7(t) })
	return e
}

// startFakeKubelet finishes graceful Pod deletion the way a kubelet would
// after preStop: envtest has no kubelet, so a deleted Pod bound to a node
// would otherwise keep its deletionTimestamp forever. Pods carrying
// specHoldFinalizer are left alone (the "deleting" state).
func (e *specEnv) startFakeKubelet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			var pods corev1.PodList
			if err := e.c.List(ctx, &pods, client.InNamespace(e.ns)); err != nil {
				continue // API server stopping, or transient; next tick retries
			}
			for i := range pods.Items {
				p := &pods.Items[i]
				if p.DeletionTimestamp == nil || slices.Contains(p.Finalizers, specHoldFinalizer) {
					continue
				}
				// NotFound/transient errors: the next tick looks again.
				_ = e.c.Delete(ctx, p, client.GracePeriodSeconds(0))
			}
		}
	}()
	t.Cleanup(func() { cancel(); wg.Wait() })
}

// startController starts a controller Manager wired to this env's API
// server and database and returns a function that stops it and waits.
//
// This is the ONE place that knows how the controller is assembled.
func (e *specEnv) startController(t *testing.T) (stop func()) {
	t.Helper()
	// poller.New registers its counters on crmetrics.Registry and fails on
	// a duplicate, so a second Manager with PollDB in one process cannot be
	// built. Give each Manager a fresh registry (top-level tests here do not
	// run in parallel). Reported as an open question on S2-15.
	prev := crmetrics.Registry
	crmetrics.Registry = prometheus.NewRegistry()

	mgr, err := NewManager(e.cfg, Options{
		MetricsAddr: "0", ProbeAddr: "0", SkipNameValidation: true,
		PollDB:             e.db,
		Instances:          e.instanceStore(),
		PollInterval:       200 * time.Millisecond,
		PollResyncInterval: time.Second,
		InstanceNamespace:  e.ns,
	})
	if err != nil {
		crmetrics.Registry = prev
		t.Fatalf("new manager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("manager stopped with error: %v", err)
			}
			crmetrics.Registry = prev
		})
	}
	t.Cleanup(stop)
	return stop
}

// instanceStore is the store the controller is given.
func (e *specEnv) instanceStore() InstanceStore {
	if e.instances != nil {
		return e.instances
	}
	return e.st
}

// fixture is one seeded instance.
type fixture struct {
	id, node, tenant string
	podUID           types.UID // "" if no Pod was created
}

// specSuffix returns a random lower-case base32 string (no "-" or "_"), so
// ids keep the shape D37 relies on.
func specSuffix(t *testing.T) string {
	t.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b))
}

// seedRow inserts, in one transaction, a node, a quota row and an instance
// in state st at resource_version specSeedRV, with nodes.allocated and
// tenant_quota.used matching (invariant 7 holds from the start). extraSet
// is an optional SQL fragment applied to the instance afterwards (e.g.
// "updated_at = now() - interval '6 minutes'"). No outbox rows are written,
// so every outbox row seen later was written by the controller.
func (e *specEnv) seedRow(t *testing.T, st statemachine.State, reason, extraSet string) fixture {
	t.Helper()
	sfx := specSuffix(t)
	f := fixture{id: "inst_" + sfx, node: "spec-node-" + sfx, tenant: "tenant-" + sfx}
	held := 0
	if statemachine.HoldsCapacity(st) {
		held = specGPUs
	}
	ctx := t.Context()
	err := pgx.BeginFunc(ctx, e.db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO nodes (id, gpu_type, capacity, allocated, status)
			VALUES ($1, 'H100', 8, $2, 'READY')`, f.node, held); err != nil {
			return fmt.Errorf("node: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO tenant_quota (tenant_id, gpu_type, quota_limit, used)
			VALUES ($1, 'H100', 16, $2)`, f.tenant, held); err != nil {
			return fmt.Errorf("quota: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO instances
			(id, tenant_id, gpu_type, gpu_count, node_id, state, resource_version, failure_reason)
			VALUES ($1, $2, 'H100', $3, $4, $5, $6, $7)`,
			f.id, f.tenant, specGPUs, f.node, string(st), specSeedRV, reason); err != nil {
			return fmt.Errorf("instance: %w", err)
		}
		if extraSet != "" {
			if _, err := tx.Exec(ctx, `UPDATE instances SET `+extraSet+` WHERE id = $1`, f.id); err != nil {
				return fmt.Errorf("extra: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed %s %s: %v", st, f.id, err)
	}
	return f
}

// createCR creates the GPUInstance for f with the release-capacity finalizer,
// as the controller would have before a crash.
func (e *specEnv) createCR(t *testing.T, f fixture) *gpuv1alpha1.GPUInstance {
	t.Helper()
	cr := &gpuv1alpha1.GPUInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:       gpuv1alpha1.K8sName(f.id),
			Namespace:  e.ns,
			Labels:     map[string]string{gpuv1alpha1.LabelInstanceID: f.id},
			Finalizers: []string{gpuv1alpha1.FinalizerReleaseCapacity},
		},
		Spec: gpuv1alpha1.GPUInstanceSpec{
			InstanceID: f.id, TenantID: f.tenant, GPUType: "H100", GPUCount: specGPUs, NodeName: f.node,
		},
	}
	if err := e.c.Create(t.Context(), cr); err != nil {
		t.Fatalf("create CR %s: %v", cr.Name, err)
	}
	return cr
}

// createPod creates f's Pod owned by cr and puts it in state ps. It is built
// here from ADR-0002 §GPUInstance CRD (name, labels, ownerRef, node), not
// with BuildPod, so the fixture does not depend on the code under test.
func (e *specEnv) createPod(t *testing.T, f fixture, cr *gpuv1alpha1.GPUInstance, ps podState) types.UID {
	t.Helper()
	if ps == podAbsent {
		return ""
	}
	ctx := t.Context()
	gpus := resource.MustParse(fmt.Sprint(specGPUs))
	isController := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gpuv1alpha1.K8sName(f.id),
			Namespace: e.ns,
			Labels:    map[string]string{PodAppLabel: PodAppValue, gpuv1alpha1.LabelInstanceID: f.id},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: gpuv1alpha1.GroupVersion.String(), Kind: "GPUInstance",
				Name: cr.Name, UID: cr.UID, Controller: &isController,
			}},
		},
		Spec: corev1.PodSpec{
			NodeName:       f.node,
			RestartPolicy:  corev1.RestartPolicyNever,
			InitContainers: []corev1.Container{{Name: "boot", Image: "busybox:1.36", Command: []string{"true"}}},
			Containers: []corev1.Container{{
				Name: "gpu", Image: "busybox:1.36", Command: []string{"sleep", "infinity"},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{GPUResourceName: gpus},
					Limits:   corev1.ResourceList{GPUResourceName: gpus},
				},
			}},
		},
	}
	if ps == podDeleting {
		pod.Finalizers = []string{specHoldFinalizer}
	}
	if err := e.c.Create(ctx, pod); err != nil {
		t.Fatalf("create pod %s: %v", pod.Name, err)
	}

	now := metav1.Now()
	switch ps {
	case podPending, podDeleting:
		pod.Status = corev1.PodStatus{
			Phase: corev1.PodPending,
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name: "boot", Image: "busybox:1.36",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}},
			}},
		}
	case podReady:
		pod.Status = corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: now},
			},
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name: "boot", Image: "busybox:1.36",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"}},
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "gpu", Image: "busybox:1.36", Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}},
			}},
		}
	case podInitFailed:
		// restartPolicy Never + init exit 1: the kubelet marks the Pod Failed.
		pod.Status = corev1.PodStatus{
			Phase: corev1.PodFailed,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodInitialized, Status: corev1.ConditionFalse, LastTransitionTime: now},
				{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: now},
			},
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name: "boot", Image: "busybox:1.36",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}},
			}},
		}
	}
	if err := e.c.Status().Update(ctx, pod); err != nil {
		t.Fatalf("set pod %s status %s: %v", pod.Name, ps, err)
	}
	if ps == podDeleting {
		if err := e.c.Delete(ctx, pod); err != nil {
			t.Fatalf("delete pod %s: %v", pod.Name, err)
		}
	}
	return pod.UID
}

// row is what the spec tests read back from instances.
type row struct {
	state  statemachine.State
	rv     int64
	reason string
}

func (e *specEnv) readRow(t *testing.T, id string) row {
	t.Helper()
	var r row
	var st string
	err := e.db.QueryRow(context.Background(),
		`SELECT state, resource_version, failure_reason FROM instances WHERE id = $1`, id).Scan(&st, &r.rv, &r.reason)
	if err != nil {
		t.Fatalf("read row %s: %v", id, err)
	}
	r.state = statemachine.State(st)
	return r
}

// events returns the outbox event types for id in insertion order.
func (e *specEnv) events(t *testing.T, id string) []string {
	t.Helper()
	rows, err := e.db.Query(context.Background(),
		`SELECT event_type FROM outbox WHERE aggregate_id = $1 ORDER BY id`, id)
	if err != nil {
		t.Fatalf("read outbox %s: %v", id, err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("read outbox %s: %v", id, err)
	}
	return out
}

// nodeAllocated returns nodes.allocated for node.
func (e *specEnv) nodeAllocated(t *testing.T, node string) int {
	t.Helper()
	var n int
	if err := e.db.QueryRow(context.Background(), `SELECT allocated FROM nodes WHERE id = $1`, node).Scan(&n); err != nil {
		t.Fatalf("read node %s: %v", node, err)
	}
	return n
}

// getPod reads f's Pod from the API server; nil if it does not exist.
func (e *specEnv) getPod(t *testing.T, id string) *corev1.Pod {
	t.Helper()
	var p corev1.Pod
	err := e.c.Get(context.Background(), types.NamespacedName{Namespace: e.ns, Name: gpuv1alpha1.K8sName(id)}, &p)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("get pod %s: %v", id, err)
	}
	return &p
}

// getCR reads f's GPUInstance from the API server; nil if it does not exist.
func (e *specEnv) getCR(t *testing.T, id string) *gpuv1alpha1.GPUInstance {
	t.Helper()
	var cr gpuv1alpha1.GPUInstance
	err := e.c.Get(context.Background(), types.NamespacedName{Namespace: e.ns, Name: gpuv1alpha1.K8sName(id)}, &cr)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("get CR %s: %v", id, err)
	}
	return &cr
}

// podsFor counts Pods carrying id's instance-id label (the duplicate check).
func (e *specEnv) podsFor(t *testing.T, id string) int {
	t.Helper()
	var pods corev1.PodList
	if err := e.c.List(context.Background(), &pods, client.InNamespace(e.ns),
		client.MatchingLabels{gpuv1alpha1.LabelInstanceID: id}); err != nil {
		t.Fatalf("list pods %s: %v", id, err)
	}
	return len(pods.Items)
}

// assertInvariant7 checks nodes.allocated and tenant_quota.used against the
// SUM of gpu_count over capacity-holding instances (CLAUDE.md invariant 7).
func (e *specEnv) assertInvariant7(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var holding []string
	for _, s := range statemachine.HoldingStates() {
		holding = append(holding, string(s))
	}
	check := func(what, sql string) {
		rows, err := e.db.Query(ctx, sql, holding)
		if err != nil {
			t.Errorf("invariant 7 (%s): query: %v", what, err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var key string
			var got, want int
			if err := rows.Scan(&key, &got, &want); err != nil {
				t.Errorf("invariant 7 (%s): scan: %v", what, err)
				return
			}
			t.Errorf("invariant 7 violated: %s %s = %d, SUM of holding instances = %d", what, key, got, want)
		}
		if err := rows.Err(); err != nil {
			t.Errorf("invariant 7 (%s): %v", what, err)
		}
	}
	check("nodes.allocated", `
		SELECT n.id, n.allocated, COALESCE(SUM(i.gpu_count) FILTER (WHERE i.state = ANY($1)), 0)::int
		FROM nodes n LEFT JOIN instances i ON i.node_id = n.id
		GROUP BY n.id, n.allocated
		HAVING n.allocated <> COALESCE(SUM(i.gpu_count) FILTER (WHERE i.state = ANY($1)), 0)`)
	check("tenant_quota.used", `
		SELECT q.tenant_id || '/' || q.gpu_type, q.used,
		       COALESCE(SUM(i.gpu_count) FILTER (WHERE i.state = ANY($1)), 0)::int
		FROM tenant_quota q LEFT JOIN instances i ON i.tenant_id = q.tenant_id AND i.gpu_type = q.gpu_type
		GROUP BY q.tenant_id, q.gpu_type, q.used
		HAVING q.used <> COALESCE(SUM(i.gpu_count) FILTER (WHERE i.state = ANY($1)), 0)`)
}

// specEventually polls cond every 100ms until it reports true, failing the
// test with cond's last description after timeout.
func specEventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(timeout)
	for {
		ok, last := cond()
		if ok {
			return
		}
		select {
		case <-tick.C:
		case <-deadline:
			t.Fatalf("%s: not reached within %s; last seen: %s", what, timeout, last)
		case <-t.Context().Done():
			t.Fatalf("%s: test context done", what)
		}
	}
}

// specWait blocks for d (or until the test ends) without time.Sleep.
func specWait(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case <-time.After(d):
	case <-t.Context().Done():
	}
}

// eventType mirrors the outbox naming in ADR-0001: "instance.<state>".
func eventOf(s statemachine.State) string { return "instance." + strings.ToLower(string(s)) }

// isStale reports whether err is the store's optimistic-concurrency loss.
func isStale(err error) bool { return errors.Is(err, store.ErrStaleVersion) }
