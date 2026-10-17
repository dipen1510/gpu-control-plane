//go:build integration

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

type nodeRow struct {
	id        NodeID
	gpuType   string
	capacity  int
	allocated int
	status    NodeStatus
	domain    string
}

func h100Row(id NodeID, allocated int, domain string) nodeRow {
	return nodeRow{id: id, gpuType: "H100", capacity: 8, allocated: allocated, status: NodeReady, domain: domain}
}

func insertNodes(t *testing.T, db *pgxpool.Pool, nodes ...nodeRow) {
	t.Helper()
	for _, n := range nodes {
		if _, err := db.Exec(t.Context(), `INSERT INTO nodes
			(id, gpu_type, capacity, allocated, status, failure_domain)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			n.id, n.gpuType, n.capacity, n.allocated, string(n.status), n.domain); err != nil {
			t.Fatalf("insert node %s: %v", n.id, err)
		}
	}
}

func allocatedByNode(t *testing.T, db *pgxpool.Pool) map[NodeID]int {
	t.Helper()
	rows, err := db.Query(t.Context(), `SELECT id, allocated FROM nodes`)
	if err != nil {
		t.Fatalf("query nodes: %v", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (struct {
		id    NodeID
		alloc int
	}, error) {
		var v struct {
			id    NodeID
			alloc int
		}
		err := r.Scan(&v.id, &v.alloc)
		return v, err
	})
	if err != nil {
		t.Fatalf("read nodes: %v", err)
	}
	m := map[NodeID]int{}
	for _, v := range out {
		m[v.id] = v.alloc
	}
	return m
}

type atomicCounter struct{ n atomic.Int64 }

func (c *atomicCounter) Inc() { c.n.Add(1) }

func begin(t *testing.T, db *pgxpool.Pool) pgx.Tx {
	t.Helper()
	tx, err := db.Begin(t.Context())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) }) // no-op after commit
	return tx
}

func commit(t *testing.T, tx pgx.Tx) {
	t.Helper()
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func h100Req(n int) Request { return Request{TenantID: "t1", GPUType: "H100", GPUCount: n} }

// The best-fit node fills up after the snapshot was read. Its conditional
// UPDATE matches 0 rows, so Place falls back to the next candidate, counts
// one conflict, and the filled node is not over-allocated.
func TestPlaceFallsBackWhenBestNodeFills(t *testing.T) {
	db := testutil.NewDB(t)
	insertNodes(t, db,
		h100Row("best", 6, "rack-0"),   // 2 free: exact fit for a 2-GPU request
		h100Row("second", 4, "rack-1"), // 4 free
		h100Row("third", 0, "rack-2"),  // 8 free
	)

	conflicts := &atomicCounter{}
	s := New(conflicts)
	var sawCandidates []NodeID
	s.beforeReserve = func(ctx context.Context, candidates []NodeID) {
		sawCandidates = candidates
		// Another replica takes the last 2 GPUs on "best" and commits.
		if _, err := db.Exec(ctx, `UPDATE nodes SET allocated = 8 WHERE id = 'best'`); err != nil {
			t.Errorf("fill best node: %v", err)
		}
	}

	tx := begin(t, db)
	got, err := s.Place(t.Context(), tx, h100Req(2))
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	commit(t, tx)

	if len(sawCandidates) == 0 || sawCandidates[0] != "best" {
		t.Fatalf("snapshot ranking = %v, want \"best\" first (test setup is wrong)", sawCandidates)
	}
	if got != "second" {
		t.Errorf("Place = %s, want second", got)
	}
	if n := conflicts.n.Load(); n != 1 {
		t.Errorf("reserve conflicts = %d, want 1", n)
	}
	want := map[NodeID]int{"best": 8, "second": 6, "third": 0}
	if alloc := allocatedByNode(t, db); !maps.Equal(alloc, want) {
		t.Errorf("allocated = %v, want %v", alloc, want)
	}
}

func TestPlaceInsufficientCapacity(t *testing.T) {
	cases := []struct {
		name          string
		nodes         []nodeRow
		fill          bool // fill every candidate between snapshot and reserve
		wantAlloc     map[NodeID]int
		wantConflicts int64
	}{
		{
			name: "all nodes full or ineligible",
			nodes: []nodeRow{
				h100Row("full-1", 8, "rack-0"),
				h100Row("full-2", 7, "rack-1"), // 1 free, request needs 2
				{id: "draining", gpuType: "H100", capacity: 8, allocated: 0, status: NodeCordoned, domain: "rack-2"},
				{id: "a100", gpuType: "A100", capacity: 8, allocated: 0, status: NodeReady, domain: "rack-3"},
			},
			wantAlloc: map[NodeID]int{"full-1": 8, "full-2": 7, "draining": 0, "a100": 0},
		},
		{
			name:          "every candidate fills after the snapshot",
			nodes:         []nodeRow{h100Row("n1", 4, "rack-0"), h100Row("n2", 6, "rack-1")},
			fill:          true,
			wantAlloc:     map[NodeID]int{"n1": 8, "n2": 8}, // only the hook's fill
			wantConflicts: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := testutil.NewDB(t)
			insertNodes(t, db, tc.nodes...)
			conflicts := &atomicCounter{}
			s := New(conflicts)
			if tc.fill {
				s.beforeReserve = func(ctx context.Context, _ []NodeID) {
					if _, err := db.Exec(ctx, `UPDATE nodes SET allocated = capacity WHERE gpu_type = 'H100'`); err != nil {
						t.Errorf("fill nodes: %v", err)
					}
				}
			}

			tx := begin(t, db)
			got, err := s.Place(t.Context(), tx, h100Req(2))
			if !errors.Is(err, ErrInsufficientCapacity) {
				t.Fatalf("Place = %q, %v; want ErrInsufficientCapacity", got, err)
			}
			commit(t, tx) // even a committed tx must not have changed anything

			if alloc := allocatedByNode(t, db); !maps.Equal(alloc, tc.wantAlloc) {
				t.Errorf("allocated = %v, want %v", alloc, tc.wantAlloc)
			}
			if n := conflicts.n.Load(); n != tc.wantConflicts {
				t.Errorf("reserve conflicts = %d, want %d", n, tc.wantConflicts)
			}
		})
	}
}

// A rollback by the caller undoes the reservation.
func TestPlaceRollbackLeavesAllocatedUnchanged(t *testing.T) {
	db := testutil.NewDB(t)
	insertNodes(t, db, h100Row("n1", 2, "rack-0"), h100Row("n2", 0, "rack-1"))
	before := allocatedByNode(t, db)

	tx := begin(t, db)
	if _, err := New(nil).Place(t.Context(), tx, h100Req(4)); err != nil {
		t.Fatalf("Place: %v", err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if alloc := allocatedByNode(t, db); !maps.Equal(alloc, before) {
		t.Errorf("allocated = %v after rollback, want %v", alloc, before)
	}
}

// Place locks only the node it reserved: the snapshot read takes no locks and
// a candidate that was already full when its UPDATE ran stays unlocked.
// Probed from a second connection with FOR UPDATE NOWAIT while Place's
// transaction is still open. The contended case is TestPlaceReleasesLockOnLostRace.
func TestPlaceLocksOnlyChosenNodeUncontended(t *testing.T) {
	db := testutil.NewDB(t)
	insertNodes(t, db,
		h100Row("lost", 6, "rack-0"),
		h100Row("other-1", 4, "rack-1"),
		h100Row("other-2", 0, "rack-2"),
		h100Row("other-3", 0, "rack-3"),
	)
	s := New(nil)
	s.beforeReserve = func(ctx context.Context, _ []NodeID) {
		if _, err := db.Exec(ctx, `UPDATE nodes SET allocated = 8 WHERE id = 'lost'`); err != nil {
			t.Errorf("fill node: %v", err)
		}
	}

	tx := begin(t, db)
	chosen, err := s.Place(t.Context(), tx, h100Req(2))
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if chosen != "other-1" {
		t.Fatalf("Place = %s, want other-1", chosen)
	}

	for _, id := range []NodeID{"lost", "other-1", "other-2", "other-3"} {
		locked := isLocked(t, db, id)
		if want := id == chosen; locked != want {
			t.Errorf("node %s locked = %v, want %v", id, locked, want)
		}
	}
}

// A competing transaction has reserved the last GPUs of the best node but not
// committed yet, so it holds that row's lock. Place's UPDATE on the best node
// blocks; when the competitor commits, the re-check fails and Place falls back.
// Postgres would keep the row lock the UPDATE waited for until Place's
// transaction ends; Place must release it (savepoint), or two creates that
// each hold a lost node and want the other's would deadlock.
func TestPlaceReleasesLockOnLostRace(t *testing.T) {
	db := testutil.NewDB(t)
	ctx := t.Context()
	insertNodes(t, db,
		h100Row("best", 6, "rack-0"),
		h100Row("second", 4, "rack-1"),
		h100Row("third", 0, "rack-2"),
	)

	competitor := begin(t, db)
	if _, err := competitor.Exec(ctx, `UPDATE nodes SET allocated = allocated + 2 WHERE id = 'best'`); err != nil {
		t.Fatalf("competitor reserve: %v", err)
	}

	tx := begin(t, db)
	var pid uint32
	if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatalf("backend pid: %v", err)
	}
	conflicts := &atomicCounter{}
	s := New(conflicts)

	type result struct {
		id  NodeID
		err error
	}
	done := make(chan result, 1) // buffered so the goroutine never blocks; it is the only sender
	go func() {
		id, err := s.Place(ctx, tx, h100Req(2))
		done <- result{id, err}
	}()

	// Commit the competitor only once Place is waiting on its lock, and always
	// wait for the goroutine before touching tx again.
	if !waitUntilBlocked(t, db, pid) {
		t.Error("Place never blocked on the competitor's row lock")
	}
	if err := competitor.Commit(ctx); err != nil {
		t.Errorf("competitor commit: %v", err) // not Fatal: Place still uses tx
	}
	r := <-done
	if r.err != nil {
		t.Fatalf("Place: %v", r.err)
	}
	if r.id != "second" {
		t.Fatalf("Place = %s, want second", r.id)
	}
	if n := conflicts.n.Load(); n != 1 {
		t.Errorf("reserve conflicts = %d, want 1", n)
	}
	for _, id := range []NodeID{"best", "second", "third"} {
		locked := isLocked(t, db, id)
		if want := id == r.id; locked != want {
			t.Errorf("node %s locked = %v, want %v while Place's tx is open", id, locked, want)
		}
	}
}

// waitUntilBlocked polls until backend pid waits on another session's lock.
func waitUntilBlocked(t *testing.T, db *pgxpool.Pool, pid uint32) bool {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var blocked bool
		if err := db.QueryRow(t.Context(),
			`SELECT cardinality(pg_blocking_pids($1)) > 0`, int32(pid)).Scan(&blocked); err != nil {
			t.Errorf("check blocking pids: %v", err)
			return false
		}
		if blocked {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// The best node turns NOT_READY after the snapshot; the reserve UPDATE's
// "status = 'READY'" refuses it and Place falls back to the next node.
func TestPlaceRefusesNodeTurnedNotReady(t *testing.T) {
	db := testutil.NewDB(t)
	insertNodes(t, db, h100Row("best", 6, "rack-0"), h100Row("second", 4, "rack-1"))
	conflicts := &atomicCounter{}
	s := New(conflicts)
	s.beforeReserve = func(ctx context.Context, candidates []NodeID) {
		if len(candidates) == 0 || candidates[0] != "best" {
			t.Errorf("snapshot ranking = %v, want best first", candidates)
		}
		if _, err := db.Exec(ctx, `UPDATE nodes SET status = 'NOT_READY' WHERE id = 'best'`); err != nil {
			t.Errorf("mark node NOT_READY: %v", err)
		}
	}

	tx := begin(t, db)
	got, err := s.Place(t.Context(), tx, h100Req(2))
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	commit(t, tx)

	if got != "second" {
		t.Errorf("Place = %s, want second", got)
	}
	if n := conflicts.n.Load(); n != 1 {
		t.Errorf("reserve conflicts = %d, want 1", n)
	}
	want := map[NodeID]int{"best": 6, "second": 6}
	if alloc := allocatedByNode(t, db); !maps.Equal(alloc, want) {
		t.Errorf("allocated = %v, want %v", alloc, want)
	}
}

// isLocked reports whether another transaction holds a row lock on node id.
func isLocked(t *testing.T, db *pgxpool.Pool, id NodeID) bool {
	t.Helper()
	ctx := t.Context()
	probe, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin probe: %v", err)
	}
	defer func() { _ = probe.Rollback(context.Background()) }() // read-only probe
	_, err = probe.Exec(ctx, `SELECT 1 FROM nodes WHERE id = $1 FOR UPDATE NOWAIT`, id)
	var pgErr *pgconn.PgError
	switch {
	case err == nil:
		return false
	case errors.As(err, &pgErr) && pgErr.Code == "55P03": // lock_not_available
		return true
	default:
		t.Fatalf("probe lock on %s: %v", id, err)
		return false
	}
}

// Place fills Request.TenantFailureDomains from instances that hold
// capacity. Nodes "a" and "b" tie on best fit; the tenant has a RUNNING
// instance in rack-0 (node a) and only a TERMINATED one in rack-1 (node b),
// so b must win every time. With the domains not filled, the random
// tie-break would pick a about half the time.
func TestPlaceSpreadsTenantAcrossFailureDomains(t *testing.T) {
	db := testutil.NewDB(t)
	insertNodes(t, db, h100Row("a", 4, "rack-0"), h100Row("b", 4, "rack-1"))
	if _, err := db.Exec(t.Context(), `INSERT INTO instances
		(id, tenant_id, gpu_type, gpu_count, node_id, state) VALUES
		('i-running', 't1', 'H100', 4, 'a', 'RUNNING'),
		('i-done',    't1', 'H100', 4, 'b', 'TERMINATED'),
		('i-other',   't2', 'H100', 4, 'b', 'RUNNING')`); err != nil {
		t.Fatalf("insert instances: %v", err)
	}

	s := New(nil)
	for i := range 20 {
		tx := begin(t, db)
		got, err := s.Place(t.Context(), tx, h100Req(4))
		if err != nil {
			t.Fatalf("run %d: Place: %v", i, err)
		}
		if err := tx.Rollback(t.Context()); err != nil {
			t.Fatalf("rollback: %v", err)
		}
		if got != "b" {
			t.Fatalf("run %d: Place = %s, want b (tenant already holds capacity in rack-0)", i, got)
		}
	}
}

// TestPlaceSpreadCountsEveryHoldingState: an instance counts for the tenant's
// failure-domain spread exactly while it holds capacity (CLAUDE.md: SCHEDULED,
// PROVISIONING, RUNNING, TERMINATING). The states are listed here on purpose,
// not taken from statemachine.HoldingStates(), so the test fails if the
// snapshot query's list loses one (TERMINATING is the easy one to forget: its
// GPUs are still reserved until the release step).
//
// Nodes a (rack-0) and b (rack-1) tie on best fit. The tenant's instance in
// the state under test sits on a. Holding: rack-0 is taken, so b must win
// every time. Not holding: the tenant also has a RUNNING instance on b, so a
// must win every time. A wrong list turns either case into a coin flip, which
// 20 runs catch with probability 1 - 2^-20.
func TestPlaceSpreadCountsEveryHoldingState(t *testing.T) {
	tests := []struct {
		state string
		want  NodeID
	}{
		{state: "SCHEDULED", want: "b"},
		{state: "PROVISIONING", want: "b"},
		{state: "RUNNING", want: "b"},
		{state: "TERMINATING", want: "b"},
		{state: "TERMINATED", want: "a"},
		{state: "FAILED", want: "a"},
	}
	for _, tc := range tests {
		t.Run(tc.state, func(t *testing.T) {
			db := testutil.NewDB(t)
			insertNodes(t, db, h100Row("a", 4, "rack-0"), h100Row("b", 4, "rack-1"))
			if _, err := db.Exec(t.Context(), `INSERT INTO instances
				(id, tenant_id, gpu_type, gpu_count, node_id, state) VALUES
				('i-under-test', 't1', 'H100', 4, 'a', $1)`, tc.state); err != nil {
				t.Fatalf("insert instance: %v", err)
			}
			if tc.want == "a" {
				if _, err := db.Exec(t.Context(), `INSERT INTO instances
					(id, tenant_id, gpu_type, gpu_count, node_id, state) VALUES
					('i-on-b', 't1', 'H100', 4, 'b', 'RUNNING')`); err != nil {
					t.Fatalf("insert instance on b: %v", err)
				}
			}

			s := New(nil)
			for i := range 20 {
				tx := begin(t, db)
				got, err := s.Place(t.Context(), tx, h100Req(4))
				if err != nil {
					t.Fatalf("run %d: Place: %v", i, err)
				}
				if err := tx.Rollback(t.Context()); err != nil {
					t.Fatalf("rollback: %v", err)
				}
				if got != tc.want {
					t.Fatalf("run %d: Place = %s, want %s (tenant instance on a is %s)", i, got, tc.want, tc.state)
				}
			}
		})
	}
}

// Invariant 1 at the Place level: many transactions race for a small fleet
// and the sum reserved on a node never exceeds its capacity. Each goroutine
// runs its own transaction, as separate API replicas would.
func TestPlaceConcurrentNeverOverAllocates(t *testing.T) {
	const (
		workers  = 128
		nodes    = 4
		capacity = 8
	)
	cases := []struct {
		name  string
		count func(i int) int
		// wantPlaced is the exact number of successes, or -1 if it depends
		// on the interleaving.
		wantPlaced int
	}{
		// Sizes divide the capacity and no GPUs are released, so the fleet
		// fills exactly: 32 / 2 = 16 successes.
		{name: "uniform 2-GPU", count: func(int) int { return 2 }, wantPlaced: nodes * capacity / 2},
		{name: "mixed sizes", count: func(i int) int { return []int{1, 2, 4, 8}[i%4] }, wantPlaced: -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := testutil.NewDB(t)
			for i := range nodes {
				insertNodes(t, base, h100Row(NodeID(fmt.Sprintf("n%d", i)), 0, fmt.Sprintf("rack-%d", i)))
			}
			// More connections than the default pool so transactions really overlap.
			cfg := base.Config().Copy()
			cfg.MaxConns = 32
			db, err := pgxpool.NewWithConfig(t.Context(), cfg)
			if err != nil {
				t.Fatalf("pool: %v", err)
			}
			t.Cleanup(db.Close)

			s := New(&atomicCounter{})
			var (
				mu       sync.Mutex
				placed   = map[NodeID]int{} // GPUs committed per node, by Place's answer
				success  int
				failures []error
			)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range workers {
				wg.Go(func() {
					<-start
					n := tc.count(i)
					id, err := placeOnce(t.Context(), db, s, h100Req(n))
					mu.Lock()
					defer mu.Unlock()
					switch {
					case err == nil:
						placed[id] += n
						success++
					case errors.Is(err, ErrInsufficientCapacity):
					default:
						failures = append(failures, err)
					}
				})
			}
			close(start)
			wg.Wait()

			for _, err := range failures {
				t.Errorf("unexpected error: %v", err)
			}
			alloc := allocatedByNode(t, base)
			for id, a := range alloc {
				if a > capacity {
					t.Errorf("node %s allocated %d > capacity %d", id, a, capacity)
				}
				if a != placed[id] {
					t.Errorf("node %s allocated %d, but Place reported %d GPUs there", id, a, placed[id])
				}
			}
			if tc.wantPlaced >= 0 && success != tc.wantPlaced {
				t.Errorf("placed %d instances, want %d", success, tc.wantPlaced)
			}
		})
	}
}

// placeOnce runs Place in its own transaction and commits on success.
func placeOnce(ctx context.Context, db *pgxpool.Pool, s *Scheduler, req Request) (NodeID, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }() // no-op after commit
	id, err := s.Place(ctx, tx, req)
	if err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

// Only READY nodes are candidates. The NOT_READY and CORDONED nodes would be
// the best fit (fullest), so Place picks them if the status filter is missing.
func TestPlaceSkipsNonReady(t *testing.T) {
	db := testutil.NewDB(t)
	notReady := h100Row("not-ready", 6, "rack-0")
	notReady.status = NodeNotReady
	cordoned := h100Row("cordoned", 6, "rack-1")
	cordoned.status = NodeCordoned
	insertNodes(t, db, notReady, cordoned, h100Row("ready", 0, "rack-2"))
	s := New(nil)
	s.beforeReserve = func(_ context.Context, candidates []NodeID) {
		if !slices.Equal(candidates, []NodeID{"ready"}) {
			t.Errorf("candidates = %v, want [ready]", candidates)
		}
	}

	tx := begin(t, db)
	got, err := s.Place(t.Context(), tx, h100Req(2))
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	commit(t, tx)

	if got != "ready" {
		t.Errorf("Place = %s, want ready", got)
	}
	want := map[NodeID]int{"not-ready": 6, "cordoned": 6, "ready": 2}
	if alloc := allocatedByNode(t, db); !maps.Equal(alloc, want) {
		t.Errorf("allocated = %v, want %v", alloc, want)
	}
}
