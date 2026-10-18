//go:build integration

// S1-12 spec tests for store.Transition and the outbox, written from
// ADR-0001 (§Lifecycle transitions, Amendment 2 item 2) and CLAUDE.md
// (state machine, invariants 3, 4, 5 and candidate 7), not from the code.
//
// They live in the external package store_test so they can only use the
// exported contract. Every helper is prefixed s112 so it cannot collide with
// helpers of other stories in the same directory.
package store_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/seed"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

// s112PoolConns is the pool size for these tests. The testutil default is
// max(4, NumCPU), which serializes most of 100 racers in the pool instead of
// in Postgres, so races would not be real (S1-8 review). Postgres allows 100
// connections and other packages run at the same time, so stay modest.
const s112PoolConns = 25

// s112Racers is how many goroutines race in each concurrency test.
const s112Racers = 100

// s112Spec is the CLAUDE.md state machine diagram, written out again here
// on purpose: the tests use it as the oracle, so a wrong edge in
// internal/statemachine shows up as a failure instead of being trusted.
var s112Spec = map[statemachine.State][]statemachine.State{
	statemachine.Pending:      {statemachine.Scheduled, statemachine.Failed},
	statemachine.Scheduled:    {statemachine.Provisioning, statemachine.Terminating, statemachine.Failed},
	statemachine.Provisioning: {statemachine.Running, statemachine.Terminating, statemachine.Failed},
	statemachine.Running:      {statemachine.Terminating, statemachine.Failed}, // D29, S2-9
	statemachine.Terminating:  {statemachine.Terminated},
	statemachine.Terminated:   nil,
	statemachine.Failed:       nil,
}

var s112AllStates = []statemachine.State{
	statemachine.Pending, statemachine.Scheduled, statemachine.Provisioning, statemachine.Running,
	statemachine.Terminating, statemachine.Terminated, statemachine.Failed,
}

// s112Rank orders states along the diagram so "never moves backward" is a
// number comparison. FAILED is ranked after every non-terminal state.
var s112Rank = map[statemachine.State]int{
	statemachine.Pending: 0, statemachine.Scheduled: 1, statemachine.Provisioning: 2,
	statemachine.Running: 3, statemachine.Terminating: 4, statemachine.Terminated: 5,
	statemachine.Failed: 5,
}

func s112Legal(from, to statemachine.State) bool {
	for _, s := range s112Spec[from] {
		if s == to {
			return true
		}
	}
	return false
}

func s112Holds(s statemachine.State) bool {
	switch s {
	case statemachine.Scheduled, statemachine.Provisioning, statemachine.Running, statemachine.Terminating:
		return true
	}
	return false
}

func s112Terminal(s statemachine.State) bool {
	return s == statemachine.Terminated || s == statemachine.Failed
}

// s112HoldingSQL is the SQL list of capacity-holding states (ADR §Data model).
const s112HoldingSQL = `('SCHEDULED','PROVISIONING','RUNNING','TERMINATING')`

// s112Env is one fresh database with seeded nodes, a big pool and a store.
type s112Env struct {
	db    *pgxpool.Pool
	st    *store.Store
	nodes []seed.Node
	// ctx bounds every database call of the test (deadline set by s112NewEnv),
	// so a hang fails the test instead of waiting for go test's 10m timeout.
	ctx context.Context
}

// s112NewEnv returns a fresh migrated database with nNodes seeded nodes
// (odd ids H100, even A100, 8 GPUs each) and a pool of s112PoolConns. Every
// helper of the returned env runs under a deadline of budget from now.
func s112NewEnv(t *testing.T, nNodes int, budget time.Duration) s112Env {
	t.Helper()
	small := testutil.NewDB(t)
	cfg := small.Config()
	cfg.MaxConns = s112PoolConns
	ctx, cancel := context.WithTimeout(t.Context(), budget)
	t.Cleanup(cancel)
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("s112: open pool: %v", err)
	}
	// Registered after testutil's cleanups, so it runs before the DROP DATABASE.
	t.Cleanup(db.Close)

	nodes, err := seed.Nodes(nNodes)
	if err != nil {
		t.Fatalf("s112: seed nodes: %v", err)
	}
	if err := seed.Upsert(ctx, db, nodes); err != nil {
		t.Fatalf("s112: seed nodes: %v", err)
	}
	return s112Env{db: db, st: store.New(db, scheduler.New(nil)), nodes: nodes, ctx: ctx}
}

// s112Create creates one instance through the real create transaction.
func (e s112Env) s112Create(t *testing.T, id, tenant, gpuType string, n int) store.Instance {
	t.Helper()
	h := sha256.Sum256([]byte(id))
	res, err := e.st.Create(e.ctx, store.CreateRequest{
		ID: id, TenantID: tenant, GPUType: gpuType, GPUCount: n,
		IdempotencyKey: "s112-key-" + id, RequestHash: h[:],
	})
	if err != nil {
		t.Fatalf("s112: create %s: %v", id, err)
	}
	if res.Replayed {
		t.Fatalf("s112: create %s was a replay", id)
	}
	return res.Instance
}

// s112WalkTo moves inst along path (each step chained on the returned rv).
func (e s112Env) s112WalkTo(t *testing.T, inst store.Instance, path ...statemachine.State) store.Instance {
	t.Helper()
	for _, to := range path {
		next, err := e.st.Transition(e.ctx, inst.ID, to, inst.ResourceVersion, "s112 setup")
		if err != nil {
			t.Fatalf("s112: setup %s %s → %s: %v", inst.ID, inst.State, to, err)
		}
		inst = next
	}
	return inst
}

type s112Row struct {
	state statemachine.State
	rv    int64
}

func (e s112Env) s112Stored(t *testing.T, id string) s112Row {
	t.Helper()
	var r s112Row
	var st string
	if err := e.db.QueryRow(e.ctx,
		`SELECT state, resource_version FROM instances WHERE id = $1`, id).Scan(&st, &r.rv); err != nil {
		t.Fatalf("s112: read instance %s: %v", id, err)
	}
	r.state = statemachine.State(st)
	return r
}

// s112Events returns the outbox event types for id in insertion order.
func (e s112Env) s112Events(t *testing.T, id string) []string {
	t.Helper()
	rows, err := e.db.Query(e.ctx,
		`SELECT event_type FROM outbox WHERE aggregate_id = $1 ORDER BY id`, id)
	if err != nil {
		t.Fatalf("s112: read outbox: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var et string
		if err := rows.Scan(&et); err != nil {
			t.Fatalf("s112: scan outbox: %v", err)
		}
		out = append(out, et)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("s112: read outbox: %v", err)
	}
	return out
}

func (e s112Env) s112Count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.db.QueryRow(e.ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("s112: %s: %v", sql, err)
	}
	return n
}

// s112Counters is (allocated on node, used for tenant/type) for one instance.
func (e s112Env) s112Counters(t *testing.T, inst store.Instance) (allocated, used int) {
	t.Helper()
	if err := e.db.QueryRow(e.ctx, `SELECT
		(SELECT allocated FROM nodes WHERE id = $1),
		(SELECT used FROM tenant_quota WHERE tenant_id = $2 AND gpu_type = $3)`,
		inst.NodeID, inst.TenantID, inst.GPUType).Scan(&allocated, &used); err != nil {
		t.Fatalf("s112: read counters: %v", err)
	}
	return allocated, used
}

// s112CheckAccounting asserts invariant 7 and non-negative counters for
// every node and every quota row: allocated / used equal the sum of
// gpu_count of capacity-holding instances, and are never below 0 or above
// their limit.
func (e s112Env) s112CheckAccounting(t *testing.T, when string) {
	t.Helper()
	ctx := e.ctx
	rows, err := e.db.Query(ctx, `
		SELECT n.id, n.allocated, n.capacity,
		       COALESCE((SELECT SUM(i.gpu_count) FROM instances i
		                 WHERE i.node_id = n.id AND i.state IN `+s112HoldingSQL+`), 0)
		FROM nodes n ORDER BY n.id`)
	if err != nil {
		t.Fatalf("s112: read nodes: %v", err)
	}
	for rows.Next() {
		var id string
		var alloc, capacity, sum int
		if err := rows.Scan(&id, &alloc, &capacity, &sum); err != nil {
			t.Fatalf("s112: scan nodes: %v", err)
		}
		if alloc < 0 || alloc > capacity || alloc != sum {
			t.Errorf("%s: node %s allocated=%d capacity=%d, holding instances sum=%d (invariants 1, 7)",
				when, id, alloc, capacity, sum)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("s112: read nodes: %v", err)
	}

	rows, err = e.db.Query(ctx, `
		SELECT q.tenant_id, q.gpu_type, q.used, q.quota_limit,
		       COALESCE((SELECT SUM(i.gpu_count) FROM instances i
		                 WHERE i.tenant_id = q.tenant_id AND i.gpu_type = q.gpu_type
		                   AND i.state IN `+s112HoldingSQL+`), 0)
		FROM tenant_quota q ORDER BY 1, 2`)
	if err != nil {
		t.Fatalf("s112: read quota: %v", err)
	}
	for rows.Next() {
		var tenant, gpuType string
		var used, limit, sum int
		if err := rows.Scan(&tenant, &gpuType, &used, &limit, &sum); err != nil {
			t.Fatalf("s112: scan quota: %v", err)
		}
		if used < 0 || used > limit || used != sum {
			t.Errorf("%s: quota %s/%s used=%d limit=%d, holding instances sum=%d (invariants 6, 7)",
				when, tenant, gpuType, used, limit, sum)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("s112: read quota: %v", err)
	}
}

// s112Describe adds the SQLSTATE to an error, so a 40P01 deadlock or a
// 23514 CHECK violation is obvious in the failure message.
func s112Describe(err error) string {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return fmt.Sprintf("%v (SQLSTATE %s)", err, pg.Code)
	}
	return err.Error()
}

func s112Event(s statemachine.State) string {
	return "instance." + strings.ToLower(string(s))
}

// TestConcurrentTransitionsOneWinner: ≥100 goroutines call Transition with
// the same expectedRV. Exactly one wins; every loser gets ErrStaleVersion
// (never ErrInvalidTransition, a CHECK violation or a deadlock); exactly one
// outbox row is written; rv goes up by exactly 1; capacity is released at
// most once (ADR-0001 Amendment 2 item 2; invariants 3, 4, 5).
func TestConcurrentTransitionsOneWinner(t *testing.T) {
	e := s112NewEnv(t, 8, 2*time.Minute)

	cases := []struct {
		name    string
		setup   []statemachine.State // path from SCHEDULED to the start state
		targets []statemachine.State // racer i asks for targets[i % len]
		release bool                 // the winning move gives capacity back
	}{
		{name: "SCHEDULED→PROVISIONING", targets: []statemachine.State{statemachine.Provisioning}},
		{name: "PROVISIONING→RUNNING", setup: []statemachine.State{statemachine.Provisioning},
			targets: []statemachine.State{statemachine.Running}},
		{name: "RUNNING→TERMINATING",
			setup:   []statemachine.State{statemachine.Provisioning, statemachine.Running},
			targets: []statemachine.State{statemachine.Terminating}},
		{name: "TERMINATING→TERMINATED releases once",
			setup:   []statemachine.State{statemachine.Provisioning, statemachine.Running, statemachine.Terminating},
			targets: []statemachine.State{statemachine.Terminated}, release: true},
		{name: "SCHEDULED→FAILED releases once",
			targets: []statemachine.State{statemachine.Failed}, release: true},
		{name: "PROVISIONING→FAILED releases once", setup: []statemachine.State{statemachine.Provisioning},
			targets: []statemachine.State{statemachine.Failed}, release: true},
		// Same expectedRV, different (all legal) targets: still one winner,
		// and every loser must hear "stale", not "illegal move from a state
		// you never saw". release is decided from the winner below.
		{name: "SCHEDULED→mixed targets", targets: []statemachine.State{
			statemachine.Provisioning, statemachine.Terminating, statemachine.Failed}},
	}

	for i, tc := range cases {
		// After the first failing case, stop: the rest would most likely fail
		// the same way and only bury the first message.
		if t.Failed() {
			t.Logf("skipping %q and later cases after an earlier failure", tc.name)
			break
		}
		t.Run(tc.name, func(t *testing.T) {
			tenant := fmt.Sprintf("s112-race-%d", i)
			inst := e.s112Create(t, fmt.Sprintf("inst_s112_race_%d", i), tenant, "H100", 2)
			inst = e.s112WalkTo(t, inst, tc.setup...)
			allocBefore, usedBefore := e.s112Counters(t, inst)
			eventsBefore := len(e.s112Events(t, inst.ID))

			// 100 small transactions finish in well under a second; 15s means
			// something is stuck (e.g. the pool ran dry), so fail fast.
			ctx, cancel := context.WithTimeout(e.ctx, 15*time.Second)
			defer cancel()
			start := make(chan struct{})
			type result struct {
				to   statemachine.State
				inst store.Instance
				err  error
			}
			results := make([]result, s112Racers)
			var wg sync.WaitGroup
			for g := range s112Racers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					to := tc.targets[g%len(tc.targets)]
					<-start
					got, err := e.st.Transition(ctx, inst.ID, to, inst.ResourceVersion, "s112 race")
					results[g] = result{to: to, inst: got, err: err}
				}()
			}
			close(start)
			wg.Wait()

			var winners []result
			const maxShown = 5
			wrong := 0
			for g, r := range results {
				switch {
				case r.err == nil:
					winners = append(winners, r)
				case errors.Is(r.err, store.ErrStaleVersion):
				default:
					if wrong < maxShown {
						t.Errorf("racer %d (→ %s): got %s, want nil or ErrStaleVersion", g, r.to, s112Describe(r.err))
					}
					wrong++
				}
			}
			if wrong > maxShown {
				t.Errorf("... and %d more racers with a wrong error (%d of %d in total)",
					wrong-maxShown, wrong, len(results))
			}
			if len(winners) != 1 {
				t.Fatalf("%d racers succeeded, want exactly 1", len(winners))
			}
			w := winners[0]
			if w.inst.State != w.to || w.inst.ResourceVersion != inst.ResourceVersion+1 {
				t.Errorf("winner returned state %s rv %d, want %s rv %d",
					w.inst.State, w.inst.ResourceVersion, w.to, inst.ResourceVersion+1)
			}
			if got := e.s112Stored(t, inst.ID); got.state != w.to || got.rv != inst.ResourceVersion+1 {
				t.Errorf("stored state %s rv %d, want %s rv %d", got.state, got.rv, w.to, inst.ResourceVersion+1)
			}

			events := e.s112Events(t, inst.ID)
			if len(events) != eventsBefore+1 {
				t.Fatalf("outbox has %d rows for the instance, want %d (exactly one new): %v",
					len(events), eventsBefore+1, events)
			}
			if last := events[len(events)-1]; last != s112Event(w.to) {
				t.Errorf("new outbox event %q, want %q", last, s112Event(w.to))
			}

			release := tc.release || (s112Holds(inst.State) && s112Terminal(w.to))
			wantAlloc, wantUsed := allocBefore, usedBefore
			if release {
				wantAlloc -= inst.GPUCount
				wantUsed -= inst.GPUCount
			}
			if alloc, used := e.s112Counters(t, inst); alloc != wantAlloc || used != wantUsed {
				t.Errorf("allocated=%d used=%d, want %d and %d (release=%v: exactly once or not at all)",
					alloc, used, wantAlloc, wantUsed, release)
			}
			e.s112CheckAccounting(t, "after race")
		})
	}
}

// s112FailCommitSQL installs, in this test's own database only, deferred
// constraint triggers that make COMMIT fail for chosen rows. A row in
// s112_fail_commit (table, id) arms the trigger for that table and id:
//
//   - on instances (AFTER INSERT OR UPDATE), keyed by instances.id
//   - on outbox (AFTER INSERT), keyed by outbox.aggregate_id
//
// Being DEFERRABLE INITIALLY DEFERRED, they fire at COMMIT, after every
// statement of the transaction has run: the exact moment a crash or a lost
// connection between "UPDATE" and "COMMIT" would hit.
const s112FailCommitSQL = `
CREATE TABLE s112_fail_commit (tbl text NOT NULL, id text NOT NULL, PRIMARY KEY (tbl, id));

CREATE FUNCTION s112_fail_commit_fn() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE key text;
BEGIN
  IF TG_TABLE_NAME = 'outbox' THEN key := NEW.aggregate_id; ELSE key := NEW.id; END IF;
  IF EXISTS (SELECT 1 FROM s112_fail_commit WHERE tbl = TG_TABLE_NAME AND id = key) THEN
    RAISE EXCEPTION 's112 injected commit failure on % %', TG_TABLE_NAME, key
      USING ERRCODE = 'P0001';
  END IF;
  RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER s112_fail_commit_instances
  AFTER INSERT OR UPDATE ON instances DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION s112_fail_commit_fn();

CREATE CONSTRAINT TRIGGER s112_fail_commit_outbox
  AFTER INSERT ON outbox DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION s112_fail_commit_fn();
`

func (e s112Env) s112ArmCommitFailure(t *testing.T, table, id string) {
	t.Helper()
	if _, err := e.db.Exec(e.ctx,
		`INSERT INTO s112_fail_commit (tbl, id) VALUES ($1, $2)`, table, id); err != nil {
		t.Fatalf("s112: arm commit failure: %v", err)
	}
}

func (e s112Env) s112Disarm(t *testing.T) {
	t.Helper()
	if _, err := e.db.Exec(e.ctx, `DELETE FROM s112_fail_commit`); err != nil {
		t.Fatalf("s112: disarm: %v", err)
	}
}

// TestOutboxCommitsAtomicallyWithStateChange proves invariant 3's "same
// transaction" rule: the state change and its outbox row commit together or
// not at all. It makes COMMIT itself fail, once on the instance side and once
// on the outbox side:
//
//   - instance side fails: if the outbox row were written in a separate
//     (earlier or autocommitted) transaction, it would survive → an event for
//     a change that never happened.
//   - outbox side fails: if the outbox row were written in a separate later
//     transaction, the state change would already be committed → a change
//     with no event (a lost event).
//
// Either mutation of the code makes one of these sub-tests fail. Both
// Transition and Create are covered, since both write events.
func TestOutboxCommitsAtomicallyWithStateChange(t *testing.T) {
	e := s112NewEnv(t, 4, 45*time.Second)
	if _, err := e.db.Exec(e.ctx, s112FailCommitSQL); err != nil {
		t.Fatalf("s112: install commit-failure triggers: %v", err)
	}

	for _, table := range []string{"instances", "outbox"} {
		t.Run("Transition/commit fails on "+table, func(t *testing.T) {
			inst := e.s112Create(t, "inst_s112_atomic_tr_"+table, "s112-atomic-tr-"+table, "H100", 2)
			// Release moves are the widest transaction (quota, node, instance,
			// outbox), so use one: SCHEDULED → FAILED.
			before := e.s112Stored(t, inst.ID)
			eventsBefore := e.s112Events(t, inst.ID)
			allocBefore, usedBefore := e.s112Counters(t, inst)
			outboxBefore := e.s112Count(t, `SELECT count(*) FROM outbox`)

			e.s112ArmCommitFailure(t, table, inst.ID)
			_, err := e.st.Transition(e.ctx, inst.ID, statemachine.Failed, inst.ResourceVersion, "s112")
			e.s112Disarm(t)
			if err == nil {
				t.Fatalf("Transition succeeded although its commit was made to fail on %s", table)
			}

			if got := e.s112Stored(t, inst.ID); got != before {
				t.Errorf("instance is %+v after the failed commit, want unchanged %+v", got, before)
			}
			if got := e.s112Events(t, inst.ID); len(got) != len(eventsBefore) {
				t.Errorf("outbox for instance is %v after the failed commit, want unchanged %v", got, eventsBefore)
			}
			if got := e.s112Count(t, `SELECT count(*) FROM outbox`); got != outboxBefore {
				t.Errorf("outbox has %d rows, want unchanged %d", got, outboxBefore)
			}
			if alloc, used := e.s112Counters(t, inst); alloc != allocBefore || used != usedBefore {
				t.Errorf("allocated=%d used=%d after the failed commit, want unchanged %d and %d",
					alloc, used, allocBefore, usedBefore)
			}
			e.s112CheckAccounting(t, "after failed commit")

			// Not poisoned: the same call now succeeds with exactly one event.
			got, err := e.st.Transition(e.ctx, inst.ID, statemachine.Failed, inst.ResourceVersion, "s112")
			if err != nil {
				t.Fatalf("Transition after disarming: %s", s112Describe(err))
			}
			if got.State != statemachine.Failed || got.ResourceVersion != inst.ResourceVersion+1 {
				t.Errorf("after retry: state %s rv %d, want FAILED rv %d", got.State, got.ResourceVersion, inst.ResourceVersion+1)
			}
			want := append(eventsBefore, s112Event(statemachine.Failed))
			if evs := e.s112Events(t, inst.ID); strings.Join(evs, ",") != strings.Join(want, ",") {
				t.Errorf("outbox after retry %v, want %v", evs, want)
			}
			e.s112CheckAccounting(t, "after retry")
		})

		t.Run("Create/commit fails on "+table, func(t *testing.T) {
			id := "inst_s112_atomic_cr_" + table
			tenant := "s112-atomic-cr-" + table
			outboxBefore := e.s112Count(t, `SELECT count(*) FROM outbox`)
			allocBefore := e.s112Count(t, `SELECT COALESCE(SUM(allocated), 0) FROM nodes`)

			e.s112ArmCommitFailure(t, table, id)
			h := sha256.Sum256([]byte(id))
			_, err := e.st.Create(e.ctx, store.CreateRequest{
				ID: id, TenantID: tenant, GPUType: "H100", GPUCount: 2,
				IdempotencyKey: "s112-key-" + id, RequestHash: h[:],
			})
			e.s112Disarm(t)
			if err == nil {
				t.Fatalf("Create succeeded although its commit was made to fail on %s", table)
			}

			if n := e.s112Count(t, `SELECT count(*) FROM instances WHERE id = $1`, id); n != 0 {
				t.Errorf("%d instance rows after the failed commit, want 0", n)
			}
			if got := e.s112Count(t, `SELECT count(*) FROM outbox`); got != outboxBefore {
				t.Errorf("outbox has %d rows, want unchanged %d", got, outboxBefore)
			}
			if n := e.s112Count(t, `SELECT count(*) FROM idempotency_keys WHERE tenant_id = $1`, tenant); n != 0 {
				t.Errorf("%d idempotency keys kept after the failed commit, want 0", n)
			}
			if got := e.s112Count(t, `SELECT COALESCE(SUM(allocated), 0) FROM nodes`); got != allocBefore {
				t.Errorf("total allocated %d, want unchanged %d", got, allocBefore)
			}
			e.s112CheckAccounting(t, "after failed create")
		})
	}
}

// s112Seed returns the seed for the property test: S112_SEED if set (to
// replay a failure), else the clock. The caller logs it.
func s112Seed(t *testing.T) uint64 {
	t.Helper()
	if v := os.Getenv("S112_SEED"); v != "" {
		s, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("S112_SEED=%q: %v", v, err)
		}
		return s
	}
	return uint64(time.Now().UnixNano())
}

// TestTransitionPropertyRandomSequences runs random Transition calls (random
// instance, random target among all 7 states, sometimes a stale or future
// rv) against a real database and checks after every call, against a model
// built from the spec:
//
//   - wrong rv → ErrStaleVersion and nothing changes (checked before legality)
//   - right rv, illegal edge per CLAUDE.md → ErrInvalidTransition, nothing changes
//   - right rv, legal edge → success, rv + 1, exactly one outbox row
//     instance.<state>, and the state moved forward
//   - a terminal state never changes
//   - every node's allocated and every tenant's used are ≥ 0 and equal the
//     sum of gpu_count of capacity-holding instances (invariant 7)
//
// Interleaved random creates (3 tenants, 1/2/4 GPUs and sometimes 8) keep
// instances alive; the model predicts each outcome — ErrQuotaExceeded over
// 16 per tenant and type (invariant 6), else ErrInsufficientCapacity if no
// node has room, else success — and a failed create must write nothing.
//
// Replay a failure with S112_SEED=<seed from the log>.
func TestTransitionPropertyRandomSequences(t *testing.T) {
	const (
		rounds    = 3
		instances = 10
		steps     = 200
	)
	base := s112Seed(t)
	for round := range rounds {
		seed := base + uint64(round)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Logf("property seed %d (replay with S112_SEED=%d and -run 'Property/seed=%d')", seed, seed, seed)
			s112RunProperty(t, rand.New(rand.NewPCG(seed, seed^0x5112)), instances, steps)
		})
	}
}

type s112Model struct {
	inst  store.Instance // id, tenant, type, count, node as created
	state statemachine.State
	rv    int64
	evs   int
}

func s112RunProperty(t *testing.T, rng *rand.Rand, nInst, steps int) {
	// 6 nodes = 3 per type, 8 GPUs each. Creates can run into the quota
	// (16 per tenant and type, Discussion D2) or into capacity; the model
	// predicts which, and the test asserts the store agrees.
	e := s112NewEnv(t, 6, 90*time.Second)
	const quotaLimit = 16
	tenants := []string{"s112-prop-0", "s112-prop-1", "s112-prop-2"}
	types := []string{"H100", "A100"}
	var models []*s112Model
	stats := map[string]int{}
	totalEvents := 0
	created := 0

	// held is what the model says a tenant holds of a GPU type, and nodeFree
	// what it says a node has free: both from the capacity-holding instances
	// in the model, never from the database.
	held := func(tenant, gpuType string) int {
		n := 0
		for _, m := range models {
			if s112Holds(m.state) && m.inst.TenantID == tenant && m.inst.GPUType == gpuType {
				n += m.inst.GPUCount
			}
		}
		return n
	}
	nodeFree := func(node seed.Node) int {
		free := node.Capacity
		for _, m := range models {
			if s112Holds(m.state) && m.inst.NodeID == node.ID {
				free -= m.inst.GPUCount
			}
		}
		return free
	}

	// tryCreate asks for a random instance and checks the outcome the model
	// predicts: 403 if it would take the tenant over quota (checked first,
	// ADR step 2), else 503 if no node of the type has n free GPUs (step 3),
	// else success. A failed create must write nothing at all.
	tryCreate := func(where string) {
		tenant := tenants[rng.IntN(len(tenants))]
		gpuType := types[rng.IntN(len(types))]
		n := []int{1, 2, 4}[rng.IntN(3)]
		if rng.IntN(6) == 0 {
			n = 8
		}
		id := fmt.Sprintf("inst_s112_prop_%d", created)
		created++
		where = fmt.Sprintf("%s: create %s %s x%d for %s", where, id, gpuType, n, tenant)

		var want error
		fits := false
		for _, node := range e.nodes {
			if node.GPUType == gpuType && nodeFree(node) >= n {
				fits = true
			}
		}
		switch {
		case held(tenant, gpuType)+n > quotaLimit:
			want = store.ErrQuotaExceeded
		case !fits:
			want = store.ErrInsufficientCapacity
		}

		h := sha256.Sum256([]byte(id))
		res, err := e.st.Create(e.ctx, store.CreateRequest{
			ID: id, TenantID: tenant, GPUType: gpuType, GPUCount: n,
			IdempotencyKey: "s112-key-" + id, RequestHash: h[:],
		})
		if want != nil {
			if want == store.ErrQuotaExceeded {
				stats["create-403"]++
			} else {
				stats["create-503"]++
			}
			if !errors.Is(err, want) {
				t.Fatalf("%s: got %v, want %v (tenant holds %d, node free %v)", where, err, want,
					held(tenant, gpuType), s112FreeList(e.nodes, gpuType, nodeFree))
			}
			if c := e.s112Count(t, `SELECT count(*) FROM instances WHERE id = $1`, id); c != 0 {
				t.Fatalf("%s: failed create left %d instance rows", where, c)
			}
			if c := e.s112Count(t, `SELECT count(*) FROM idempotency_keys WHERE instance_id = $1`, id); c != 0 {
				t.Fatalf("%s: failed create kept its idempotency key", where)
			}
			if c := e.s112Count(t, `SELECT count(*) FROM outbox`); c != totalEvents {
				t.Fatalf("%s: outbox has %d rows after a failed create, want %d", where, c, totalEvents)
			}
			e.s112CheckAccounting(t, where) // counters unchanged ⇔ still equal to the model's instances
			return
		}
		stats["create-ok"]++
		if err != nil {
			t.Fatalf("%s: got %s, want success (tenant holds %d, node free %v)", where, s112Describe(err),
				held(tenant, gpuType), s112FreeList(e.nodes, gpuType, nodeFree))
		}
		if res.Replayed || res.ID != id || res.State != statemachine.Scheduled || res.ResourceVersion != 1 ||
			res.NodeID == "" {
			t.Fatalf("%s: got %+v, want a fresh SCHEDULED rv 1 instance on a node", where, res)
		}
		models = append(models, &s112Model{inst: res.Instance, state: res.State, rv: res.ResourceVersion, evs: 1})
		totalEvents++
		if c := e.s112Count(t, `SELECT count(*) FROM outbox`); c != totalEvents {
			t.Fatalf("%s: outbox has %d rows, want %d", where, c, totalEvents)
		}
		e.s112CheckAccounting(t, where)
	}

	for i := range nInst {
		tryCreate(fmt.Sprintf("setup %d", i))
	}

	for step := range steps {
		// Replace dead instances now and then, so moves keep happening and
		// released GPUs and quota get reused by new creates.
		if rng.IntN(3) == 0 {
			live := 0
			for _, m := range models {
				if !s112Terminal(m.state) {
					live++
				}
			}
			if live < nInst+4 {
				tryCreate(fmt.Sprintf("step %d", step))
			}
		}
		var alive []*s112Model
		for _, m := range models {
			if !s112Terminal(m.state) {
				alive = append(alive, m)
			}
		}
		if len(models) == 0 {
			continue
		}

		// Mostly live instances; sometimes any, so terminal ones get poked too.
		m := models[rng.IntN(len(models))]
		if len(alive) > 0 && rng.IntN(4) != 0 {
			m = alive[rng.IntN(len(alive))]
		}
		to := s112AllStates[rng.IntN(len(s112AllStates))]
		// Bias toward legal moves so instances actually travel the diagram.
		if next := s112Spec[m.state]; len(next) > 0 && rng.IntN(2) == 0 {
			to = next[rng.IntN(len(next))]
		}
		rv := m.rv
		if rng.IntN(4) == 0 {
			rv += []int64{-2, -1, 1, 2}[rng.IntN(4)] // stale or from the future
		}
		where := fmt.Sprintf("step %d: %s %s rv %d → %s with rv %d", step, m.inst.ID, m.state, m.rv, to, rv)

		got, err := e.st.Transition(e.ctx, m.inst.ID, to, rv, "s112 property")

		switch {
		case rv != m.rv:
			stats["stale"]++
			if !errors.Is(err, store.ErrStaleVersion) {
				t.Fatalf("%s: got %v, want ErrStaleVersion", where, err)
			}
		case !s112Legal(m.state, to):
			stats["illegal"]++
			if !errors.Is(err, store.ErrInvalidTransition) {
				t.Fatalf("%s: got %v, want ErrInvalidTransition", where, err)
			}
		default:
			stats["ok"]++
			if err != nil {
				t.Fatalf("%s: got %s, want success", where, s112Describe(err))
			}
			if got.State != to || got.ResourceVersion != m.rv+1 || got.ID != m.inst.ID {
				t.Fatalf("%s: returned %s %s rv %d, want %s rv %d", where, got.ID, got.State,
					got.ResourceVersion, to, m.rv+1)
			}
			if s112Rank[to] <= s112Rank[m.state] {
				t.Fatalf("%s: state moved backward or sideways", where)
			}
			if s112Terminal(m.state) {
				t.Fatalf("%s: a terminal state changed", where)
			}
			m.state, m.rv = to, m.rv+1
			m.evs++
			totalEvents++
		}

		// The database must match the model exactly, success or not.
		if stored := e.s112Stored(t, m.inst.ID); stored.state != m.state || stored.rv != m.rv {
			t.Fatalf("%s: stored %s rv %d, model %s rv %d", where, stored.state, stored.rv, m.state, m.rv)
		}
		evs := e.s112Events(t, m.inst.ID)
		if len(evs) != m.evs {
			t.Fatalf("%s: instance has %d outbox rows, want %d: %v", where, len(evs), m.evs, evs)
		}
		if err == nil && evs[len(evs)-1] != s112Event(to) {
			t.Fatalf("%s: last event %q, want %q", where, evs[len(evs)-1], s112Event(to))
		}
		if n := e.s112Count(t, `SELECT count(*) FROM outbox`); n != totalEvents {
			t.Fatalf("%s: outbox has %d rows in total, want %d", where, n, totalEvents)
		}
		e.s112CheckAccounting(t, where)
		if t.Failed() {
			t.FailNow()
		}
	}
	t.Logf("steps: %v", stats)
	if stats["ok"] == 0 || stats["illegal"] == 0 || stats["stale"] == 0 || stats["create-ok"] == 0 {
		t.Errorf("generator did not cover the basic outcomes: %v", stats)
	}
}

// s112FreeList renders the model's free GPUs per node of gpuType, for
// failure messages.
func s112FreeList(nodes []seed.Node, gpuType string, free func(seed.Node) int) string {
	var parts []string
	for _, n := range nodes {
		if n.GPUType == gpuType {
			parts = append(parts, fmt.Sprintf("%s=%d", n.ID, free(n)))
		}
	}
	return strings.Join(parts, " ")
}
