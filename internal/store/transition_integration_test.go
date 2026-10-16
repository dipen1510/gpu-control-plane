//go:build integration

package store

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/scheduler"

	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
)

// allStates is every lifecycle state, for exhaustive checks.
var allStates = []statemachine.State{
	statemachine.Pending, statemachine.Scheduled, statemachine.Provisioning, statemachine.Running,
	statemachine.Terminating, statemachine.Terminated, statemachine.Failed,
}

// createInstance creates a 2-GPU H100 instance for tenant t1 and returns it
// with the totals from just before the create, so tests can check that
// release brings allocated/used back to where they started.
func createInstance(t *testing.T, s *Store, db *pgxpool.Pool, id string) (Instance, totals) {
	t.Helper()
	before := readTotals(t, db)
	hash := sha256.Sum256([]byte(id))
	res, err := s.Create(t.Context(), CreateRequest{
		ID: id, TenantID: "t1", GPUType: "H100", GPUCount: 2,
		IdempotencyKey: "key-" + id, RequestHash: hash[:],
	})
	if err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	return res.Instance, before
}

// walk applies each transition in order, chaining the returned version.
func walk(t *testing.T, s *Store, inst Instance, states ...statemachine.State) Instance {
	t.Helper()
	for _, to := range states {
		next, err := s.Transition(t.Context(), inst.ID, to, inst.ResourceVersion, "")
		if err != nil {
			t.Fatalf("Transition %s → %s: %v", inst.State, to, err)
		}
		inst = next
	}
	return inst
}

// outboxTypes returns the event types written for instance id, in order.
func outboxTypes(t *testing.T, db *pgxpool.Pool, id string) []string {
	t.Helper()
	rows, err := db.Query(t.Context(), `SELECT event_type FROM outbox WHERE aggregate_id = $1 ORDER BY id`, id)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	var types []string
	for rows.Next() {
		var et string
		if err := rows.Scan(&et); err != nil {
			t.Fatalf("scan outbox: %v", err)
		}
		types = append(types, et)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	return types
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestTransitionFullLifecycle(t *testing.T) {
	s, db := newStore(t)
	inst, before := createInstance(t, s, db, "inst_life")

	steps := []statemachine.State{
		statemachine.Provisioning, statemachine.Running, statemachine.Terminating, statemachine.Terminated,
	}
	for i, to := range steps {
		// reason is only for FAILED; any other move must not store it.
		next, err := s.Transition(t.Context(), inst.ID, to, inst.ResourceVersion, "not a failure")
		if err != nil {
			t.Fatalf("Transition %s → %s: %v", inst.State, to, err)
		}
		if next.FailureReason != "" {
			t.Fatalf("→ %s stored failure_reason %q, want empty", to, next.FailureReason)
		}
		if next.State != to || next.ResourceVersion != inst.ResourceVersion+1 {
			t.Fatalf("step %d: got state %s rv %d, want %s rv %d",
				i, next.State, next.ResourceVersion, to, inst.ResourceVersion+1)
		}
		got, err := s.Get(t.Context(), "t1", inst.ID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.State != next.State || got.ResourceVersion != next.ResourceVersion {
			t.Fatalf("step %d: stored %s rv %d, returned %s rv %d",
				i, got.State, got.ResourceVersion, next.State, next.ResourceVersion)
		}
		// Capacity is held until the very last step.
		held := readTotals(t, db)
		wantAlloc, wantUsed := before.allocated+2, before.used+2
		if to == statemachine.Terminated {
			wantAlloc, wantUsed = before.allocated, before.used
		}
		if held.allocated != wantAlloc || held.used != wantUsed {
			t.Fatalf("after → %s: allocated %d used %d, want %d %d", to, held.allocated, held.used, wantAlloc, wantUsed)
		}
		inst = next
	}

	wantEvents := []string{
		"instance.scheduled", "instance.provisioning", "instance.running",
		"instance.terminating", "instance.terminated",
	}
	if got := outboxTypes(t, db, inst.ID); !equalStrings(got, wantEvents) {
		t.Errorf("outbox events = %v, want %v", got, wantEvents)
	}
	after := readTotals(t, db)
	if after.allocated != before.allocated || after.used != before.used {
		t.Errorf("allocated/used = %d/%d, want back to %d/%d", after.allocated, after.used, before.allocated, before.used)
	}
}

func TestTransitionToFailedReleasesCapacity(t *testing.T) {
	tests := []struct {
		name  string
		setup []statemachine.State // steps after create, before → FAILED
	}{
		{name: "from SCHEDULED"},
		{name: "from PROVISIONING", setup: []statemachine.State{statemachine.Provisioning}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, db := newStore(t)
			inst, before := createInstance(t, s, db, "inst_fail")
			inst = walk(t, s, inst, tc.setup...)

			failed, err := s.Transition(t.Context(), inst.ID, statemachine.Failed, inst.ResourceVersion, "boot timeout")
			if err != nil {
				t.Fatalf("Transition → FAILED: %v", err)
			}
			if failed.State != statemachine.Failed || failed.ResourceVersion != inst.ResourceVersion+1 {
				t.Errorf("got %s rv %d, want FAILED rv %d", failed.State, failed.ResourceVersion, inst.ResourceVersion+1)
			}
			got, err := s.Get(t.Context(), "t1", inst.ID)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.FailureReason != "boot timeout" {
				t.Errorf("failure_reason = %q, want %q", got.FailureReason, "boot timeout")
			}
			after := readTotals(t, db)
			if after.allocated != before.allocated || after.used != before.used {
				t.Errorf("allocated/used = %d/%d, want back to %d/%d",
					after.allocated, after.used, before.allocated, before.used)
			}
			types := outboxTypes(t, db, inst.ID)
			if n := len(types); n != len(tc.setup)+2 || types[n-1] != "instance.failed" {
				t.Errorf("outbox events = %v, want %d ending in instance.failed", types, len(tc.setup)+2)
			}
		})
	}
}

func TestTransitionStaleVersion(t *testing.T) {
	tests := []struct {
		name string
		to   statemachine.State
	}{
		// A non-releasing move and a releasing one: a stale write must not
		// change state, version, outbox or capacity either way.
		{name: "to TERMINATING", to: statemachine.Terminating},
		{name: "to FAILED", to: statemachine.Failed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, db := newStore(t)
			inst, _ := createInstance(t, s, db, "inst_stale")
			inst = walk(t, s, inst, statemachine.Provisioning) // rv 2

			beforeTot := readTotals(t, db)
			for _, rv := range []int64{inst.ResourceVersion - 1, inst.ResourceVersion + 1} {
				_, err := s.Transition(t.Context(), inst.ID, tc.to, rv, "stale")
				if !errors.Is(err, ErrStaleVersion) {
					t.Errorf("Transition with rv %d (current %d): error = %v, want ErrStaleVersion",
						rv, inst.ResourceVersion, err)
				}
			}
			got, err := s.Get(t.Context(), "t1", inst.ID)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.State != inst.State || got.ResourceVersion != inst.ResourceVersion || got.FailureReason != "" {
				t.Errorf("instance changed: %s rv %d reason %q, want %s rv %d no reason",
					got.State, got.ResourceVersion, got.FailureReason, inst.State, inst.ResourceVersion)
			}
			if after := readTotals(t, db); after != beforeTot {
				t.Errorf("totals changed: %+v, want %+v", after, beforeTot)
			}
		})
	}
}

func TestTransitionRepeatIsNoop(t *testing.T) {
	tests := []struct {
		name  string
		setup []statemachine.State // brings the instance to the state before the step
		step  statemachine.State   // applied once, then repeated with the same expectedRV
	}{
		{name: "SCHEDULED → PROVISIONING", step: statemachine.Provisioning},
		{
			name:  "TERMINATING → TERMINATED (releases)",
			setup: []statemachine.State{statemachine.Provisioning, statemachine.Running, statemachine.Terminating},
			step:  statemachine.Terminated,
		},
		{name: "SCHEDULED → FAILED (releases)", step: statemachine.Failed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, db := newStore(t)
			inst, _ := createInstance(t, s, db, "inst_repeat")
			inst = walk(t, s, inst, tc.setup...)

			applied, err := s.Transition(t.Context(), inst.ID, tc.step, inst.ResourceVersion, "first")
			if err != nil {
				t.Fatalf("first Transition → %s: %v", tc.step, err)
			}
			afterFirst := readTotals(t, db)

			// The retry a controller would send after a timeout: same target,
			// same version it originally read.
			_, err = s.Transition(t.Context(), inst.ID, tc.step, inst.ResourceVersion, "retry")
			if !errors.Is(err, ErrStaleVersion) {
				t.Errorf("repeat Transition → %s: error = %v, want ErrStaleVersion", tc.step, err)
			}
			if after := readTotals(t, db); after != afterFirst {
				t.Errorf("repeat changed totals (outbox/allocated/used): %+v, want %+v", after, afterFirst)
			}
			got, err := s.Get(t.Context(), "t1", inst.ID)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.ResourceVersion != applied.ResourceVersion || got.FailureReason != applied.FailureReason {
				t.Errorf("repeat changed instance: rv %d reason %q, want rv %d reason %q",
					got.ResourceVersion, got.FailureReason, applied.ResourceVersion, applied.FailureReason)
			}
		})
	}
}

func TestTerminalNeverChanges(t *testing.T) {
	ends := []struct {
		name string
		path []statemachine.State
	}{
		{name: "TERMINATED", path: []statemachine.State{
			statemachine.Provisioning, statemachine.Running, statemachine.Terminating, statemachine.Terminated,
		}},
		{name: "FAILED", path: []statemachine.State{statemachine.Failed}},
	}
	for _, end := range ends {
		t.Run(end.name, func(t *testing.T) {
			s, db := newStore(t)
			inst, _ := createInstance(t, s, db, "inst_end")
			inst = walk(t, s, inst, end.path...)
			before := readTotals(t, db)

			for _, to := range allStates {
				// The current version, so only the state machine can refuse.
				_, err := s.Transition(t.Context(), inst.ID, to, inst.ResourceVersion, "revive")
				if !errors.Is(err, ErrInvalidTransition) {
					t.Errorf("Transition %s → %s: error = %v, want ErrInvalidTransition", inst.State, to, err)
				}
			}
			if after := readTotals(t, db); after != before {
				t.Errorf("totals changed: %+v, want %+v", after, before)
			}
			got, err := s.Get(t.Context(), "t1", inst.ID)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.State != inst.State || got.ResourceVersion != inst.ResourceVersion {
				t.Errorf("instance changed to %s rv %d, want %s rv %d",
					got.State, got.ResourceVersion, inst.State, inst.ResourceVersion)
			}
		})
	}
}

func TestTransitionNotFound(t *testing.T) {
	s, _ := newStore(t)
	_, err := s.Transition(t.Context(), "inst_missing", statemachine.Provisioning, 1, "")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

// TestTransitionConcurrentRepeatReleasesOnce races many identical
// TERMINATING → TERMINATED calls (same expectedRV), as N controller replicas
// or retries would. The instance is the tenant's only one, so a second
// release would drive used below zero. (S1-12 has the larger spec version.)
func TestTransitionConcurrentRepeatReleasesOnce(t *testing.T) {
	const workers = 100
	s, db := newStore(t)
	inst, before := createInstance(t, s, db, "inst_race")
	inst = walk(t, s, inst, statemachine.Provisioning, statemachine.Running, statemachine.Terminating)
	outboxBefore := readTotals(t, db).outbox

	start := make(chan struct{})
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			<-start
			_, errs[i] = s.Transition(t.Context(), inst.ID, statemachine.Terminated, inst.ResourceVersion, "")
		})
	}
	close(start)
	wg.Wait()

	wins := 0
	for i, err := range errs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrStaleVersion):
			// Lost the race, whether it read before the winner committed
			// (caught by the compare-and-set) or after (caught by the
			// version check on the read).
		default:
			t.Errorf("worker %d: error = %v, want nil or ErrStaleVersion", i, err)
		}
	}
	if wins != 1 {
		t.Errorf("%d transitions succeeded, want exactly 1", wins)
	}
	after := readTotals(t, db)
	if after.outbox != outboxBefore+1 {
		t.Errorf("outbox rows = %d, want %d (exactly one new event)", after.outbox, outboxBefore+1)
	}
	if after.allocated != before.allocated || after.used != before.used {
		t.Errorf("allocated/used = %d/%d, want %d/%d (released exactly once)",
			after.allocated, after.used, before.allocated, before.used)
	}
	got, err := s.Get(t.Context(), "t1", inst.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != statemachine.Terminated || got.ResourceVersion != inst.ResourceVersion+1 {
		t.Errorf("instance %s rv %d, want TERMINATED rv %d", got.State, got.ResourceVersion, inst.ResourceVersion+1)
	}
}

// TestTransitionFromPendingDoesNotRelease covers the v2 edge PENDING →
// FAILED: a PENDING instance was never placed, so failing it must not touch
// quota or nodes even though FAILED is terminal (S1-1 review).
func TestTransitionFromPendingDoesNotRelease(t *testing.T) {
	s, db := newStore(t)
	// Something else holds capacity, so a wrong release would show.
	createInstance(t, s, db, "inst_other")
	if _, err := db.Exec(t.Context(), `INSERT INTO instances (id, tenant_id, gpu_type, gpu_count, node_id, state)
		VALUES ('inst_pending', 't1', 'H100', 2, NULL, 'PENDING')`); err != nil {
		t.Fatalf("insert PENDING instance: %v", err)
	}
	before := readTotals(t, db)

	got, err := s.Transition(t.Context(), "inst_pending", statemachine.Failed, 1, "no capacity in time")
	if err != nil {
		t.Fatalf("Transition PENDING → FAILED: %v", err)
	}
	if got.State != statemachine.Failed || got.ResourceVersion != 2 || got.FailureReason != "no capacity in time" {
		t.Errorf("got %s rv %d reason %q, want FAILED rv 2 reason %q",
			got.State, got.ResourceVersion, got.FailureReason, "no capacity in time")
	}
	after := readTotals(t, db)
	if after.outbox != before.outbox+1 {
		t.Errorf("outbox rows = %d, want %d", after.outbox, before.outbox+1)
	}
	if after.allocated != before.allocated || after.used != before.used {
		t.Errorf("allocated/used = %d/%d, want unchanged %d/%d",
			after.allocated, after.used, before.allocated, before.used)
	}
}

// TestCreateTransitionChurnNoDeadlock runs creates and releases at the same
// time on a small fleet, so Create (quota → node) and Transition's release
// contend for the same quota and node rows. If the two ever took those
// locks in different orders, Postgres would abort one with a deadlock
// (SQLSTATE 40P01).
func TestCreateTransitionChurnNoDeadlock(t *testing.T) {
	const (
		workers = 30
		rounds  = 4
	)
	_, small := newStore(t) // seeds 4 nodes: 16 H100 GPUs
	// The default test pool is tiny; give every worker a connection so the
	// transactions really overlap in Postgres instead of queueing in the pool.
	cfg := small.Config().Copy()
	cfg.MaxConns = workers + 2
	db, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(db.Close)
	s := New(db, scheduler.New(nil))

	start := make(chan struct{})
	errs := make(chan error, workers*rounds*5)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			<-start
			tenant := fmt.Sprintf("t%d", w%3) // few tenants: shared quota rows
			for r := range rounds {
				id := fmt.Sprintf("inst_%d_%d", w, r)
				hash := sha256.Sum256([]byte(id))
				res, err := s.Create(t.Context(), CreateRequest{
					ID: id, TenantID: tenant, GPUType: "H100", GPUCount: 2,
					IdempotencyKey: "key-" + id, RequestHash: hash[:],
				})
				if err != nil {
					errs <- err
					continue
				}
				// Alternate the two releasing paths.
				path := []statemachine.State{statemachine.Failed}
				if r%2 == 0 {
					path = []statemachine.State{statemachine.Provisioning, statemachine.Running,
						statemachine.Terminating, statemachine.Terminated}
				}
				inst := res.Instance
				for _, to := range path {
					next, err := s.Transition(t.Context(), id, to, inst.ResourceVersion, "churn")
					if err != nil {
						errs <- err
						break
					}
					inst = next
				}
			}
		})
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		var pgErr *pgconn.PgError
		switch {
		case errors.As(err, &pgErr) && pgErr.Code == "40P01":
			t.Errorf("deadlock between create and release (lock order broken): %v", err)
		case errors.Is(err, ErrQuotaExceeded), errors.Is(err, ErrInsufficientCapacity),
			errors.Is(err, ErrStaleVersion):
			// Expected under load; nothing was written.
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	// Every created instance was driven to a terminal state, or its create
	// failed cleanly, so everything must be given back.
	if tot := readTotals(t, db); tot.allocated != 0 || tot.used != 0 {
		t.Errorf("after churn allocated/used = %d/%d, want 0/0", tot.allocated, tot.used)
	}
}
