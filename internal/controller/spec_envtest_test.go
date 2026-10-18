package controller

// S2-15: reconciler spec tests written from ADR-0002 §Reconcile, by DB state
// and §Failure modes, not from the implementation. They run the whole
// controller (Manager, informers, DB poller) against envtest + Postgres and
// look only at outcomes: DB state, outbox rows, Pods, CRs, capacity.
//
// envtest has no kubelet, so the tests play its part: they set Pod status
// (pending / Ready / init failed) and a fake kubelet finishes graceful Pod
// deletion (spec_helpers_envtest_test.go).
//
// Cases for behaviour owned by later stories carry `pending: "S2-x"` and are
// skipped (and not seeded) until that story lands; delete the field to turn
// one on. Run with `make test-envtest` and Postgres up (DATABASE_URL).

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// podOutcome is what must have happened to the Pod.
type podOutcome int

const (
	podUnchanged podOutcome = iota // same Pod (same UID), or still absent
	podCreated                     // a Pod now exists (any UID)
	podGone                        // no Pod
)

// specCase is one row of the DB state × Pod state table.
type specCase struct {
	name    string
	db      statemachine.State // "" = no DB row (orphan CR)
	reason  string             // failure_reason to seed (FAILED rows)
	extra   string             // extra SET fragment for the seeded row
	pod     podState
	pending string // story that implements this row; non-empty = skipped

	// Exactly one of the three expectation kinds is used:
	//   want  != "": the row eventually reaches want, writing exactly events.
	//   allow != nil: after specSettle the row is in one of allow (ADR is
	//                 silent on the action; only the safety rule is checked).
	//   neither: after specSettle nothing changed (no write anywhere).
	want       statemachine.State
	wantReason string
	events     []statemachine.State
	allow      []statemachine.State

	podWant podOutcome
	crGone  bool // CR deleted and finalizer removed (terminal rows)
}

// specTable is ADR-0002 §Reconcile, by DB state, expanded over every Pod
// state named in S2-15. A comment names the ADR cell each row comes from.
func specTable() []specCase {
	S, P, R := statemachine.Scheduled, statemachine.Provisioning, statemachine.Running
	T, D, F := statemachine.Terminating, statemachine.Terminated, statemachine.Failed
	cases := []specCase{
		// SCHEDULED: ensure CR (+finalizer); ensure Pod (AlreadyExists = ok) → PROVISIONING.
		{name: "SCHEDULED/absent: create CR+Pod, PROVISIONING", db: S, pod: podAbsent,
			want: P, events: []statemachine.State{P}, podWant: podCreated},
		{name: "SCHEDULED/pending: crash after Pod create, AlreadyExists ok", db: S, pod: podPending,
			want: P, events: []statemachine.State{P}, podWant: podUnchanged},
		{name: "SCHEDULED/ready: PROVISIONING then RUNNING", db: S, pod: podReady,
			want: R, events: []statemachine.State{P, R}, podWant: podUnchanged},
		{name: "SCHEDULED/init-failed: PROVISIONING then boot_failed", db: S, pod: podInitFailed, pending: "S2-10",
			want: F, wantReason: "boot_failed", events: []statemachine.State{P, F}, podWant: podGone},
		// ADR silent on a Pod already being deleted; safety: never RUNNING
		// (not Ready) and never terminal while the Pod exists; Pod not replaced.
		{name: "SCHEDULED/deleting: never RUNNING or terminal while Pod exists", db: S, pod: podDeleting,
			allow: []statemachine.State{S, P}, podWant: podUnchanged},

		// PROVISIONING: Pod Ready → RUNNING; init failed or timeout → delete Pod →
		// FAILED once gone; else RequeueAfter(deadline).
		// ADR silent on a missing Pod; safety: not RUNNING.
		{name: "PROVISIONING/absent: never RUNNING without a Ready Pod", db: P, pod: podAbsent,
			allow: []statemachine.State{P, F}},
		{name: "PROVISIONING/pending: wait, no write", db: P, pod: podPending, podWant: podUnchanged},
		{name: "PROVISIONING/ready: RUNNING", db: P, pod: podReady,
			want: R, events: []statemachine.State{R}, podWant: podUnchanged},
		{name: "PROVISIONING/init-failed: delete Pod, FAILED boot_failed", db: P, pod: podInitFailed, pending: "S2-10",
			want: F, wantReason: "boot_failed", events: []statemachine.State{F}, podWant: podGone},
		{name: "PROVISIONING/deleting: no FAILED until Pod is gone", db: P, pod: podDeleting,
			allow: []statemachine.State{P}, podWant: podUnchanged},
		{name: "PROVISIONING/pending past 5m on DB time: provisioning_timeout", db: P, pod: podPending, pending: "S2-10",
			extra: "updated_at = now() - interval '6 minutes'",
			want:  F, wantReason: "provisioning_timeout", events: []statemachine.State{F}, podWant: podGone},
		{name: "PROVISIONING/pending, 1m old: still waiting", db: P, pod: podPending,
			extra: "updated_at = now() - interval '1 minute'", podWant: podUnchanged},

		// RUNNING: Pod missing (live read) or Failed → delete Pod → FAILED pod_lost.
		{name: "RUNNING/absent: FAILED pod_lost", db: R, pod: podAbsent,
			want: F, wantReason: "pod_lost", events: []statemachine.State{F}, podWant: podGone},
		{name: "RUNNING/pending: no write", db: R, pod: podPending, podWant: podUnchanged},
		{name: "RUNNING/ready: idempotent, no write", db: R, pod: podReady, podWant: podUnchanged},
		{name: "RUNNING/failed: delete Pod, FAILED pod_lost", db: R, pod: podInitFailed,
			want: F, wantReason: "pod_lost", events: []statemachine.State{F}, podWant: podGone},
		{name: "RUNNING/deleting: capacity held until Pod is gone", db: R, pod: podDeleting, podWant: podUnchanged},

		// TERMINATING: delete Pod (NotFound ok); Pod confirmed gone → TERMINATED.
		{name: "TERMINATING/absent: TERMINATED, CR removed", db: T, pod: podAbsent,
			want: D, events: []statemachine.State{D}, podWant: podGone, crGone: true},
		{name: "TERMINATING/pending: delete Pod, TERMINATED", db: T, pod: podPending,
			want: D, events: []statemachine.State{D}, podWant: podGone, crGone: true},
		{name: "TERMINATING/ready: delete Pod, TERMINATED", db: T, pod: podReady,
			want: D, events: []statemachine.State{D}, podWant: podGone, crGone: true},
		{name: "TERMINATING/failed: delete Pod, TERMINATED", db: T, pod: podInitFailed,
			want: D, events: []statemachine.State{D}, podWant: podGone, crGone: true},
		{name: "TERMINATING/deleting: no release while Pod exists", db: T, pod: podDeleting, podWant: podUnchanged},

		// row not found: CR exists → log + orphans metric; nothing deleted (M2b repairs).
		{name: "no row/ready: orphan CR and Pod left alone", db: "", pod: podReady, podWant: podUnchanged},
	}
	// TERMINATED / FAILED: ensure Pod gone; delete CR; remove finalizer. No DB write.
	for _, term := range []statemachine.State{D, F} {
		reason := ""
		if term == F {
			reason = "boot_failed"
		}
		for _, ps := range []podState{podAbsent, podPending, podReady, podInitFailed} {
			cases = append(cases, specCase{
				name: fmt.Sprintf("%s/%s: Pod and CR removed, row untouched", term, ps),
				db:   term, reason: reason, pod: ps, podWant: podGone, crGone: true,
			})
		}
		// ADR does not order "Pod gone" vs "remove finalizer" for a Pod stuck
		// deleting; only check the row is untouched.
		cases = append(cases, specCase{
			name: fmt.Sprintf("%s/deleting: row untouched", term),
			db:   term, reason: reason, pod: podDeleting, podWant: podUnchanged,
		})
	}
	// SCHEDULED/PROVISIONING/RUNNING with expires_at <= db_now → TERMINATING.
	for _, st := range []statemachine.State{S, P, R} {
		cases = append(cases, specCase{
			name: fmt.Sprintf("%s/ready, TTL expired: TERMINATING", st),
			db:   st, pod: podReady, pending: "S2-11",
			extra: "ttl_seconds = 60, expires_at = now() - interval '1 second'",
			// TERMINATING then (S2-8) TERMINATED; events checked as a prefix.
			want: D, events: []statemachine.State{T, D}, podWant: podGone, crGone: true,
		})
	}
	cases = append(cases, specCase{
		name: "RUNNING/ready, TTL in 1h: no write", db: R, pod: podReady, pending: "S2-11",
		extra: "ttl_seconds = 3600, expires_at = now() + interval '1 hour'", podWant: podUnchanged,
	})
	return cases
}

// TestSpecReconcileTable is S2-15's table test: for every DB state × Pod
// state, the controller takes the next action ADR-0002's table prescribes,
// and nothing else. All fixtures are created before the controller starts,
// so it sees each case in its intended initial state.
func TestSpecReconcileTable(t *testing.T) {
	e := newSpecEnv(t)
	cases := specTable()

	fixtures := make([]fixture, len(cases))
	for i, tc := range cases {
		if tc.pending != "" {
			continue // not seeded: its columns or behaviour may not exist yet
		}
		var f fixture
		if tc.db == "" {
			f = fixture{id: "inst_" + specSuffix(t)}
			f.node, f.tenant = "spec-orphan-node", "spec-orphan-tenant"
		} else {
			f = e.seedRow(t, tc.db, tc.reason, tc.extra)
		}
		// A brand-new SCHEDULED instance has no CR yet; every other state
		// had one created by the controller earlier.
		if !(tc.db == statemachine.Scheduled && tc.pod == podAbsent) {
			cr := e.createCR(t, f)
			f.podUID = e.createPod(t, f, cr, tc.pod)
		}
		fixtures[i] = f
	}

	e.startController(t)

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.pending != "" {
				t.Skip("pending " + tc.pending)
			}
			t.Parallel()
			e.checkCase(t, tc, fixtures[i])
		})
	}
}

// checkCase asserts one table row's expected outcome.
func (e *specEnv) checkCase(t *testing.T, tc specCase, f fixture) {
	t.Helper()
	switch {
	case tc.want != "":
		specEventually(t, specTimeout, "DB state "+string(tc.want), func() (bool, string) {
			r := e.readRow(t, f.id)
			return r.state == tc.want, fmt.Sprintf("state %s rv %d", r.state, r.rv)
		})
		r := e.readRow(t, f.id)
		if tc.wantReason != "" && r.reason != tc.wantReason {
			t.Errorf("failure_reason = %q, want %q", r.reason, tc.wantReason)
		}
		// Invariant 3: one outbox row per committed transition, in order;
		// and each transition bumps resource_version by exactly one.
		wantEv := make([]string, len(tc.events))
		for i, s := range tc.events {
			wantEv[i] = eventOf(s)
		}
		if got := e.events(t, f.id); !slices.Equal(got, wantEv) {
			t.Errorf("outbox = %v, want exactly %v", got, wantEv)
		}
		if r.rv != specSeedRV+int64(len(tc.events)) {
			t.Errorf("resource_version = %d, want %d (seed %d + %d transitions)",
				r.rv, specSeedRV+int64(len(tc.events)), specSeedRV, len(tc.events))
		}
		// Capacity released exactly when the row is terminal (invariant 5).
		wantAlloc := specGPUs
		if statemachine.IsTerminal(tc.want) {
			wantAlloc = 0
		}
		if got := e.nodeAllocated(t, f.node); got != wantAlloc {
			t.Errorf("nodes.allocated = %d, want %d", got, wantAlloc)
		}
		if !statemachine.IsTerminal(tc.want) {
			// After each committed transition the CR status is patched from the row.
			specEventually(t, specTimeout, "CR status from row", func() (bool, string) {
				cr := e.getCR(t, f.id)
				if cr == nil {
					return false, "CR missing"
				}
				return cr.Status.State == string(r.state) && cr.Status.ResourceVersion == r.rv,
					fmt.Sprintf("CR status %s rv %d, row %s rv %d", cr.Status.State, cr.Status.ResourceVersion, r.state, r.rv)
			})
		}

	case tc.allow != nil:
		specWait(t, specSettle)
		r := e.readRow(t, f.id)
		if !slices.Contains(tc.allow, r.state) {
			t.Errorf("state %s after %s, want one of %v", r.state, specSettle, tc.allow)
		}
		if r.state == tc.db && len(e.events(t, f.id)) != 0 {
			t.Errorf("state unchanged but outbox has %v", e.events(t, f.id))
		}
		if r.state == statemachine.Failed {
			// A failure must say why, write exactly one event and give the
			// GPUs back once (invariants 3, 5).
			switch r.reason {
			case "boot_failed", "provisioning_timeout", "pod_lost":
			default:
				t.Errorf("FAILED with failure_reason %q, want an ADR-0002 reason", r.reason)
			}
			if ev := e.events(t, f.id); !slices.Equal(ev, []string{eventOf(statemachine.Failed)}) {
				t.Errorf("outbox = %v, want exactly [%s]", ev, eventOf(statemachine.Failed))
			}
			if got := e.nodeAllocated(t, f.node); got != 0 {
				t.Errorf("nodes.allocated = %d after FAILED, want 0", got)
			}
		}

	default: // nothing may change
		specWait(t, specSettle)
		if tc.db != "" {
			r := e.readRow(t, f.id)
			if r.state != tc.db || r.rv != specSeedRV {
				t.Errorf("row = %s rv %d, want unchanged %s rv %d", r.state, r.rv, tc.db, specSeedRV)
			}
			if ev := e.events(t, f.id); len(ev) != 0 {
				t.Errorf("outbox = %v, want no rows", ev)
			}
		}
	}

	switch tc.podWant {
	case podUnchanged:
		p := e.getPod(t, f.id)
		switch {
		case f.podUID == "" && p != nil:
			t.Errorf("Pod created, want none")
		case f.podUID != "" && p == nil:
			t.Errorf("Pod deleted, want it kept")
		case p != nil && p.UID != f.podUID:
			t.Errorf("Pod replaced (uid %s → %s), want the same Pod", f.podUID, p.UID)
		}
	case podCreated:
		specEventually(t, specTimeout, "Pod created", func() (bool, string) {
			return e.getPod(t, f.id) != nil, "no Pod"
		})
		e.assertCreatedObjects(t, f)
	case podGone:
		specEventually(t, specTimeout, "Pod gone", func() (bool, string) {
			return e.getPod(t, f.id) == nil, "Pod exists"
		})
	}
	if n := e.podsFor(t, f.id); n > 1 {
		t.Errorf("%d Pods for %s, want at most 1", n, f.id)
	}

	if tc.crGone {
		specEventually(t, specTimeout, "CR deleted", func() (bool, string) {
			cr := e.getCR(t, f.id)
			if cr == nil {
				return true, ""
			}
			return false, fmt.Sprintf("CR present, finalizers %v deleting %v", cr.Finalizers, cr.DeletionTimestamp != nil)
		})
	} else if tc.db == "" || tc.podWant == podUnchanged && tc.pod != podAbsent {
		// The finalizer is removed only once the row is terminal.
		cr := e.getCR(t, f.id)
		if cr == nil || !slices.Contains(cr.Finalizers, gpuv1alpha1.FinalizerReleaseCapacity) {
			t.Errorf("CR missing or lost its finalizer: %+v", cr)
		}
	}
}

// assertCreatedObjects checks the CR and Pod a SCHEDULED reconcile creates
// (ADR-0002 §GPUInstance CRD, D37).
func (e *specEnv) assertCreatedObjects(t *testing.T, f fixture) {
	t.Helper()
	cr := e.getCR(t, f.id)
	if cr == nil {
		t.Fatalf("Pod created but no CR %s", gpuv1alpha1.K8sName(f.id))
	}
	if !slices.Contains(cr.Finalizers, gpuv1alpha1.FinalizerReleaseCapacity) {
		t.Errorf("CR finalizers = %v, want %s", cr.Finalizers, gpuv1alpha1.FinalizerReleaseCapacity)
	}
	if cr.Labels[gpuv1alpha1.LabelInstanceID] != f.id || cr.Spec.InstanceID != f.id {
		t.Errorf("CR label/spec id = %q/%q, want %q", cr.Labels[gpuv1alpha1.LabelInstanceID], cr.Spec.InstanceID, f.id)
	}
	if cr.Spec.NodeName != f.node || cr.Spec.TenantID != f.tenant || cr.Spec.GPUCount != specGPUs || cr.Spec.GPUType != "H100" {
		t.Errorf("CR spec = %+v, want node %s tenant %s H100×%d from the row", cr.Spec, f.node, f.tenant, specGPUs)
	}
	p := e.getPod(t, f.id)
	if p == nil {
		t.Fatal("Pod vanished")
	}
	if p.Spec.NodeName != f.node {
		t.Errorf("Pod nodeName = %q, want %q", p.Spec.NodeName, f.node)
	}
	if p.Labels[PodAppLabel] != PodAppValue || p.Labels[gpuv1alpha1.LabelInstanceID] != f.id {
		t.Errorf("Pod labels = %v", p.Labels)
	}
	owned := false
	for _, o := range p.OwnerReferences {
		owned = owned || (o.Kind == "GPUInstance" && o.UID == cr.UID)
	}
	if !owned {
		t.Errorf("Pod ownerReferences = %+v, want the CR (uid %s)", p.OwnerReferences, cr.UID)
	}
}

// TestSpecRestartOverExistingObjects: a new Manager over Pods and rows that
// already exist (a crash or upgrade) creates no duplicate Pods and fails no
// healthy instance (ADR-0002 §Under the hood 4, §Failure modes).
//
// It guards against a cold cache: if workers ran before WaitForCacheSync, an
// empty cache would show "no Pod" for every RUNNING instance and S2-9's
// pod_lost path would fail them all. It does NOT guard against a cache-only
// read (skipping the live read before "Pod missing"): controller-runtime's
// cache Get blocks until the Pod informer has synced, so after a restart the
// cache already holds every Pod and such a mutant passes here (S2-9 verified
// it, 3/3 runs). The live read is covered by TestPodLostUsesLiveRead, which
// makes the cache and the API server disagree.
func TestSpecRestartOverExistingObjects(t *testing.T) {
	e := newSpecEnv(t)
	const nRunning, nProvisioning, nScheduled = 20, 10, 10

	type inst struct {
		f     fixture
		start statemachine.State
	}
	var all []inst
	add := func(n int, st statemachine.State, ps podState) {
		for range n {
			f := e.seedRow(t, st, "", "")
			cr := e.createCR(t, f)
			f.podUID = e.createPod(t, f, cr, ps)
			all = append(all, inst{f: f, start: st})
		}
	}
	add(nRunning, statemachine.Running, podReady)
	add(nProvisioning, statemachine.Provisioning, podReady)
	add(nScheduled, statemachine.Scheduled, podPending) // crashed after Pod create

	noneFailed := func(when string) {
		t.Helper()
		for _, in := range all {
			r := e.readRow(t, in.f.id)
			if r.state == statemachine.Failed || r.state == statemachine.Terminating {
				t.Fatalf("%s: %s (%s with a live Pod) became %s %q", when, in.f.id, in.start, r.state, r.reason)
			}
		}
	}
	samePods := func(when string) {
		t.Helper()
		for _, in := range all {
			p := e.getPod(t, in.f.id)
			if p == nil || p.UID != in.f.podUID || e.podsFor(t, in.f.id) != 1 {
				t.Fatalf("%s: %s Pod replaced, deleted or duplicated (want exactly uid %s)", when, in.f.id, in.f.podUID)
			}
		}
	}

	// First controller: everything converges to where the ADR says.
	stopA := e.startController(t)
	specEventually(t, specTimeout, "first controller converges", func() (bool, string) {
		for _, in := range all {
			r := e.readRow(t, in.f.id)
			want := statemachine.Running
			if in.start == statemachine.Scheduled {
				want = statemachine.Provisioning // Pod still pending
			}
			if r.state != want {
				return false, fmt.Sprintf("%s (%s) is %s, want %s", in.f.id, in.start, r.state, want)
			}
		}
		return true, ""
	})
	noneFailed("after first controller")
	samePods("after first controller")
	stopA()

	before := make(map[string]row, len(all))
	for _, in := range all {
		before[in.f.id] = e.readRow(t, in.f.id)
	}

	// Second controller, fresh empty cache, same objects.
	e.startController(t)
	specWait(t, specSettle)
	noneFailed("after restart")
	samePods("after restart")
	for _, in := range all {
		if r := e.readRow(t, in.f.id); r != before[in.f.id] {
			t.Errorf("%s changed across restart: %+v → %+v (want no write)", in.f.id, before[in.f.id], r)
		}
		ev := e.events(t, in.f.id)
		seen := map[string]bool{}
		for _, x := range ev {
			if seen[x] {
				t.Errorf("%s: duplicate outbox event %s in %v", in.f.id, x, ev)
			}
			seen[x] = true
		}
		if seen[eventOf(statemachine.Failed)] {
			t.Errorf("%s: outbox has %s", in.f.id, eventOf(statemachine.Failed))
		}
	}
}

// raceGate wraps the controller's store. The first time the controller
// tries to CAS a gated instance to RUNNING, it announces that (with the rv
// it read) and waits for the test to say go, then reports the outcome. That
// pins both CASes on the same resource_version and lets the test choose who
// commits first, so both outcomes happen on every run instead of whenever
// the scheduler feels like it. The real store does the CAS either way.
type raceGate struct {
	inner *store.Store
	gates map[string]*gate // fixed before the controller starts; read-only after

	ctrlStale atomic.Int64 // controller CASes that lost with ErrStaleVersion
}

type gate struct {
	once    sync.Once
	arrived chan int64 // controller's expectedRV; buffered 1, sent once
	release chan struct{}
	result  chan error // controller's CAS outcome; buffered 1, sent once
}

func (g *raceGate) GetByK8sName(ctx context.Context, name string) (store.Instance, error) {
	return g.inner.GetByK8sName(ctx, name)
}

func (g *raceGate) Transition(ctx context.Context, id string, to statemachine.State, rv int64, reason string) (store.Instance, error) {
	gt, ok := g.gates[id]
	if !ok || to != statemachine.Running {
		return g.inner.Transition(ctx, id, to, rv, reason)
	}
	first := false
	gt.once.Do(func() { first = true })
	if !first {
		inst, err := g.inner.Transition(ctx, id, to, rv, reason)
		if isStale(err) {
			g.ctrlStale.Add(1)
		}
		return inst, err
	}
	gt.arrived <- rv
	select {
	case <-gt.release:
	case <-ctx.Done():
		gt.result <- ctx.Err()
		return store.Instance{}, ctx.Err()
	}
	inst, err := g.inner.Transition(ctx, id, to, rv, reason)
	if isStale(err) {
		g.ctrlStale.Add(1)
	}
	gt.result <- err
	return inst, err
}

// TestSpecDeleteRacesRunning: the API's DELETE (→ TERMINATING) and the
// controller's → RUNNING compare-and-set the same resource_version. One wins;
// the loser gets ErrStaleVersion and re-reads; never both, never RUNNING
// after TERMINATING, one outbox row per state (ADR-0002 §Failure modes,
// invariants 3 and 4).
//
// Each of n instances is PROVISIONING with a Ready Pod, so the controller
// wants → RUNNING. A gate holds the controller at its CAS until the API
// goroutine for that instance has read the same rv. Even instances: the API
// commits first, the controller must lose (stale) and then leave the row
// alone. Odd instances: the controller commits first, the API loses (stale),
// re-reads and deletes from RUNNING. Both sides run concurrently across all
// instances; the gate only fixes the order within one instance.
func TestSpecDeleteRacesRunning(t *testing.T) {
	e := newSpecEnv(t)
	const n = 100
	const gateTimeout = 20 * time.Second

	rg := &raceGate{inner: e.st, gates: make(map[string]*gate, n)}
	fs := make([]fixture, n)
	for i := range fs {
		fs[i] = e.seedRow(t, statemachine.Provisioning, "", "")
		cr := e.createCR(t, fs[i])
		fs[i].podUID = e.createPod(t, fs[i], cr, podReady)
		rg.gates[fs[i].id] = &gate{arrived: make(chan int64, 1), release: make(chan struct{}), result: make(chan error, 1)}
	}
	e.instances = rg

	var apiFirst, apiStale atomic.Int64
	errs := make(chan error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, f := range fs {
		apiWinsFirst := i%2 == 0
		gt := rg.gates[f.id]
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Always let the controller go, even if we bail out early, so no
			// worker stays blocked.
			released := false
			release := func() {
				if !released {
					released = true
					close(gt.release)
				}
			}
			defer release()
			<-start
			// The API read the row: PROVISIONING at the seed rv.
			rv := int64(specSeedRV)
			var ctrlRV int64
			select {
			case ctrlRV = <-gt.arrived:
			case <-time.After(gateTimeout):
				errs <- fmt.Errorf("%s: controller never tried → RUNNING", f.id)
				return
			}
			if ctrlRV != rv {
				errs <- fmt.Errorf("%s: controller CAS at rv %d, API read rv %d: not the same version", f.id, ctrlRV, rv)
				return
			}
			if !apiWinsFirst {
				release()
				select {
				case err := <-gt.result:
					if err != nil {
						errs <- fmt.Errorf("%s: controller should have won its CAS: %w", f.id, err)
						return
					}
				case <-time.After(gateTimeout):
					errs <- fmt.Errorf("%s: controller CAS did not return", f.id)
					return
				}
			}
			// What the API DELETE does: CAS from the version it read; on
			// ErrStaleVersion re-read and decide again.
			for attempt := 0; ; attempt++ {
				_, err := e.st.Transition(t.Context(), f.id, statemachine.Terminating, rv, "")
				if err == nil {
					if attempt == 0 {
						apiFirst.Add(1)
					}
					break
				}
				if !isStale(err) {
					errs <- fmt.Errorf("%s: %w", f.id, err)
					return
				}
				apiStale.Add(1)
				cur, err := e.st.Get(t.Context(), f.tenant, f.id)
				if err != nil {
					errs <- fmt.Errorf("%s: re-read: %w", f.id, err)
					return
				}
				if cur.State == statemachine.Terminating || statemachine.IsTerminal(cur.State) {
					break // already on its way out: DELETE answers 202
				}
				rv = cur.ResourceVersion
			}
			if apiWinsFirst {
				release()
				select {
				case err := <-gt.result:
					if !isStale(err) {
						errs <- fmt.Errorf("%s: controller CAS after the API's commit = %v, want ErrStaleVersion", f.id, err)
					}
				case <-time.After(gateTimeout):
					errs <- fmt.Errorf("%s: controller CAS did not return", f.id)
				}
			}
		}()
	}

	e.startController(t)
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("race: %v", err)
	}
	t.Logf("API won first CAS %d/%d, controller won %d/%d; API stale %d, controller stale %d",
		apiFirst.Load(), n, int64(n)-apiFirst.Load(), n, apiStale.Load(), rg.ctrlStale.Load())

	// Floors: both outcomes really happened, and both sides really lost a CAS.
	if a := apiFirst.Load(); a == 0 || a == n {
		t.Errorf("API won %d/%d first CASes: the race did not produce both outcomes", a, n)
	}
	if apiStale.Load() == 0 || rg.ctrlStale.Load() == 0 {
		t.Errorf("stale losses: API %d, controller %d; want both > 0", apiStale.Load(), rg.ctrlStale.Load())
	}

	// Let the controller requeue and re-read; it must never move a
	// TERMINATING instance back to RUNNING.
	specWait(t, specSettle)
	run, term, done := eventOf(statemachine.Running), eventOf(statemachine.Terminating), eventOf(statemachine.Terminated)
	for i, f := range fs {
		r := e.readRow(t, f.id)
		if r.state != statemachine.Terminating && r.state != statemachine.Terminated {
			t.Errorf("%s: state %s, want TERMINATING or TERMINATED", f.id, r.state)
		}
		ev := e.events(t, f.id)
		body := ev
		if len(body) > 0 && body[len(body)-1] == done {
			body = body[:len(body)-1] // TERMINATED once S2-8 lands
		}
		want := []string{term} // API won: the controller must not have gone RUNNING
		if i%2 == 1 {
			want = []string{run, term} // controller won, then the API deleted
		}
		if !slices.Equal(body, want) {
			t.Errorf("%s: outbox %v, want %v (+ optional %s)", f.id, ev, want, done)
		}
		if r.rv != specSeedRV+int64(len(ev)) {
			t.Errorf("%s: rv %d with %d outbox rows, want seed+%d", f.id, r.rv, len(ev), len(ev))
		}
		// DB wins (ADR-0002 §GPUInstance CRD): CR status is copied from the
		// row, so it may lag (the API's → TERMINATING is not mirrored until
		// the controller reconciles TERMINATING, S2-8) but it must never show
		// a (state, rv) the row never committed. Outbox event k was written by
		// the transition that produced rv seed+k, so the committed history is
		// {(PROVISIONING, seed), (event k state, seed+k)}. A controller that
		// treats ErrStaleVersion as success and mirrors the RUNNING it wanted
		// shows (RUNNING, seed+1) on an API-won instance, where seed+1 is
		// TERMINATING: caught here.
		cr := e.getCR(t, f.id)
		if cr == nil || cr.Status.State == "" {
			continue // never mirrored (or removed after TERMINATED): nothing claimed
		}
		committed := map[int64]string{specSeedRV: string(statemachine.Provisioning)}
		for k, x := range ev {
			committed[specSeedRV+int64(k+1)] = strings.ToUpper(strings.TrimPrefix(x, "instance."))
		}
		if got, ok := committed[cr.Status.ResourceVersion]; !ok || got != cr.Status.State {
			t.Errorf("%s: CR status (%s, rv %d) was never committed; row history %v from rv %d",
				f.id, cr.Status.State, cr.Status.ResourceVersion, ev, specSeedRV)
		}
		if i%2 == 0 && cr.Status.State == gpuv1alpha1.StateRunning {
			t.Errorf("%s: API won, row was never RUNNING, but CR status is RUNNING (rv %d)",
				f.id, cr.Status.ResourceVersion)
		}
	}
}

// TestSpecCRDeletedByHand: `kubectl delete gpuinstance` on a RUNNING
// instance. The finalizer holds the CR; the row goes TERMINATING →
// TERMINATED; the finalizer is removed only once the row is terminal
// (ADR-0002 §Reconcile, "A CR deleted by hand"; invariant 5).
func TestSpecCRDeletedByHand(t *testing.T) {
	e := newSpecEnv(t)
	f := e.seedRow(t, statemachine.Running, "", "")
	cr := e.createCR(t, f)
	f.podUID = e.createPod(t, f, cr, podDeleting) // preStop still running
	e.startController(t)

	if err := e.c.Delete(t.Context(), cr); err != nil {
		t.Fatalf("delete CR: %v", err)
	}
	specEventually(t, specTimeout, "row TERMINATING", func() (bool, string) {
		r := e.readRow(t, f.id)
		return r.state == statemachine.Terminating, string(r.state)
	})
	specWait(t, specSettle)
	// Pod still present: capacity held, CR held by the finalizer.
	if r := e.readRow(t, f.id); r.state != statemachine.Terminating {
		t.Fatalf("state %s while Pod exists, want TERMINATING", r.state)
	}
	if got := e.nodeAllocated(t, f.node); got != specGPUs {
		t.Fatalf("allocated %d while Pod exists, want %d", got, specGPUs)
	}
	if c := e.getCR(t, f.id); c == nil || !slices.Contains(c.Finalizers, gpuv1alpha1.FinalizerReleaseCapacity) {
		t.Fatalf("CR gone or finalizer removed before the row is terminal: %+v", c)
	}

	// Kubelet finishes preStop.
	p := e.getPod(t, f.id)
	p.Finalizers = nil
	if err := e.c.Update(t.Context(), p); err != nil {
		t.Fatalf("release pod: %v", err)
	}
	specEventually(t, specTimeout, "TERMINATED and CR gone", func() (bool, string) {
		r := e.readRow(t, f.id)
		return r.state == statemachine.Terminated && e.getCR(t, f.id) == nil, string(r.state)
	})
	want := []string{eventOf(statemachine.Terminating), eventOf(statemachine.Terminated)}
	if got := e.events(t, f.id); !slices.Equal(got, want) {
		t.Errorf("outbox %v, want %v", got, want)
	}
}

// TestSpecPodCreateRejected: SCHEDULED whose Pod the API server rejects as
// invalid → FAILED pod_create_failed, capacity released (S2-10). The node id
// contains "_" and upper case, which is not a valid spec.nodeName.
func TestSpecPodCreateRejected(t *testing.T) {
	t.Skip("pending S2-10")
	e := newSpecEnv(t)
	f := e.seedRow(t, statemachine.Scheduled, "", "")
	if _, err := e.db.Exec(t.Context(), `INSERT INTO nodes (id, gpu_type, capacity, allocated) VALUES ('Bad_Node', 'H100', 8, 2)`); err != nil {
		t.Fatalf("seed bad node: %v", err)
	}
	if _, err := e.db.Exec(t.Context(), `UPDATE nodes SET allocated = 0 WHERE id = $1`, f.node); err != nil {
		t.Fatalf("move allocation: %v", err)
	}
	if _, err := e.db.Exec(t.Context(), `UPDATE instances SET node_id = 'Bad_Node' WHERE id = $1`, f.id); err != nil {
		t.Fatalf("repoint instance: %v", err)
	}
	e.startController(t)
	specEventually(t, specTimeout, "FAILED pod_create_failed", func() (bool, string) {
		r := e.readRow(t, f.id)
		return r.state == statemachine.Failed && r.reason == "pod_create_failed", fmt.Sprintf("%s %q", r.state, r.reason)
	})
	if got := e.nodeAllocated(t, "Bad_Node"); got != 0 {
		t.Errorf("allocated %d after FAILED, want 0", got)
	}
}
