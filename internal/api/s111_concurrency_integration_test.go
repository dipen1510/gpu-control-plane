//go:build integration

package api

// S1-11: concurrency tests for invariants 1 (no GPU over-allocation), 6 (quota
// under concurrency) and 7 (accounting matches), plus 3 (one outbox row per
// state change) and 5 (release exactly once) under create/delete churn.
//
// Written from ADR-0001 (§Failure modes, §Invariants, Amendments 1 and 2) and
// Discussion D18. Every test drives the real HTTP handler of several
// independent "replicas" (own store, scheduler, lifecycle driver and pgxpool
// each) against one Postgres, with goroutines released together from a start
// barrier. Any 500, unexpected status, server warning/error log or SQLSTATE
// 40P01 fails the test.

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var s111Counts = []int{1, 2, 4, 8}

// TestConcurrentCreatesNeverOverAllocate: 120 creates from 12 tenants, mixed
// GPU types and counts, ~450 GPUs of demand against a 48-GPU fleet, all at
// once across 4 replicas. Invariant 1 must hold on every node, invariant 7
// must hold exactly, and the GPUs the API said yes to must be exactly the
// GPUs the nodes have allocated.
func TestConcurrentCreatesNeverOverAllocate(t *testing.T) {
	const (
		goroutines = 120
		tenants    = 12
		nodes      = 6 // 3 H100 + 3 A100, 8 GPUs each: 48 GPUs
	)
	env := s111NewEnv(t, nodes)
	ctx, cancel := context.WithTimeout(t.Context(), s111TestTimeout)
	defer cancel()

	var tally s111Tally
	var acceptedGPUs atomic.Int64
	ids := make([]string, goroutines)
	s111RunConcurrently(goroutines, func(i int) {
		tenant := fmt.Sprintf("s111-t%02d", i%tenants)
		gpuType := []string{"H100", "A100"}[(i/tenants)%2]
		n := s111Counts[(i*3)%4]
		r, err := env.create(ctx, i, tenant, fmt.Sprintf("k-%d", i), gpuType, n)
		if err != nil {
			tally.problem("create %d: %v", i, err)
			return
		}
		tally.add(fmt.Sprintf("POST %d", r.Status))
		if msg := s111CheckCreateResp(r); msg != "" {
			tally.problem("create %d (%s, %d %s): %s", i, tenant, n, gpuType, msg)
			return
		}
		if r.Status == http.StatusAccepted {
			if r.GPUs != n {
				tally.problem("create %d: 202 with gpu_count %d, asked %d", i, r.GPUs, n)
			}
			acceptedGPUs.Add(int64(n))
			ids[i] = r.ID
		}
	})
	tally.report(t)
	env.logs.check(t)
	t.Logf("answers: %v", tally.counts)

	// The race must actually have been run: some won, and the fleet ran out.
	if tally.get("POST 202") == 0 || tally.get("POST 503") == 0 {
		t.Errorf("want both 202s and 503s with demand far above capacity, got %v", tally.counts)
	}

	s111CheckAccounting(t, ctx, env.db)
	allocated, used := s111Totals(t, ctx, env.db)
	if int64(allocated) != acceptedGPUs.Load() || int64(used) != acceptedGPUs.Load() {
		t.Errorf("202s accepted %d GPUs, but nodes have %d allocated and tenants %d used; all three must match",
			acceptedGPUs.Load(), allocated, used)
	}
	if allocated > nodes*8 {
		t.Errorf("fleet allocated %d GPUs, capacity is %d", allocated, nodes*8)
	}

	// Exactly the 202s exist, each walked to RUNNING with one event per step.
	var instances int
	if err := env.db.QueryRow(ctx, `SELECT count(*) FROM instances`).Scan(&instances); err != nil {
		t.Fatalf("count instances: %v", err)
	}
	if instances != tally.get("POST 202") {
		t.Errorf("%d instance rows, but %d creates got 202 (a refused create left a row, or a 202 has none)",
			instances, tally.get("POST 202"))
	}
	s111CheckHistories(t, ctx, env.db, "RUNNING")
	s111CheckIDsExist(t, ctx, env.db, ids)
}

// TestConcurrentCreatesNeverExceedQuota: one tenant fires ≥100 creates at
// once for far more than its 16 GPUs, with capacity to spare (80 H100s), so
// only the quota can say no. Invariant 6: active GPUs never exceed 16, used
// equals what is really held, and every refusal is 403 (never 503, never 500).
func TestConcurrentCreatesNeverExceedQuota(t *testing.T) {
	cases := []struct {
		name   string
		counts []int
		// exactAccept: with 1-GPU requests the quota fills exactly, so the
		// number of 202s is known.
		exactAccept int
	}{
		{name: "one_gpu_each", counts: []int{1}, exactAccept: s111Quota},
		{name: "mixed_sizes", counts: s111Counts},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const (
				goroutines = 120
				tenant     = "s111-solo"
				nodes      = 20 // 10 H100 nodes: 80 GPUs, capacity never the limit
			)
			env := s111NewEnv(t, nodes)
			ctx, cancel := context.WithTimeout(t.Context(), s111TestTimeout)
			defer cancel()

			var tally s111Tally
			var acceptedGPUs atomic.Int64
			s111RunConcurrently(goroutines, func(i int) {
				n := tc.counts[i%len(tc.counts)]
				r, err := env.create(ctx, i, tenant, fmt.Sprintf("k-%d", i), "H100", n)
				if err != nil {
					tally.problem("create %d: %v", i, err)
					return
				}
				tally.add(fmt.Sprintf("POST %d", r.Status))
				if r.Status == http.StatusServiceUnavailable {
					tally.problem("create %d: 503 %s with ample capacity; only quota may refuse here", i, r.Code)
					return
				}
				if msg := s111CheckCreateResp(r); msg != "" {
					tally.problem("create %d (%d H100): %s", i, n, msg)
					return
				}
				if r.Status == http.StatusAccepted {
					acceptedGPUs.Add(int64(n))
				}
			})
			tally.report(t)
			env.logs.check(t)
			t.Logf("answers: %v, accepted GPUs %d", tally.counts, acceptedGPUs.Load())

			accepted, refused := tally.get("POST 202"), tally.get("POST 403")
			if accepted+refused != goroutines {
				t.Errorf("202s %d + 403s %d != %d requests", accepted, refused, goroutines)
			}
			if acceptedGPUs.Load() > s111Quota || acceptedGPUs.Load() == 0 {
				t.Errorf("202s accepted %d GPUs for one tenant; want 1..%d (invariant 6)", acceptedGPUs.Load(), s111Quota)
			}
			if tc.exactAccept > 0 && accepted != tc.exactAccept {
				t.Errorf("%d creates of 1 GPU accepted, want exactly %d (quota fills exactly; rest 403)",
					accepted, tc.exactAccept)
			}

			var active, used, limit int
			if err := env.db.QueryRow(ctx, `SELECT
				(SELECT COALESCE(SUM(gpu_count), 0)::int FROM instances
				   WHERE tenant_id = $1 AND gpu_type = 'H100' AND state = ANY($2)),
				(SELECT used FROM tenant_quota WHERE tenant_id = $1 AND gpu_type = 'H100'),
				(SELECT quota_limit FROM tenant_quota WHERE tenant_id = $1 AND gpu_type = 'H100')`,
				tenant, s111HoldingStates).Scan(&active, &used, &limit); err != nil {
				t.Fatalf("read quota: %v", err)
			}
			if limit != s111Quota || active > limit || used != active || int64(used) != acceptedGPUs.Load() {
				t.Errorf("active GPUs %d, used %d, quota_limit %d, accepted by 202s %d; want active == used == accepted <= %d",
					active, used, limit, acceptedGPUs.Load(), s111Quota)
			}
			s111CheckAccounting(t, ctx, env.db)
			s111CheckHistories(t, ctx, env.db, "RUNNING")
		})
	}
}

// TestConcurrentLastGPUs: 100 creates from 100 different tenants (quota is
// never the limit) race for the fleet's very last GPUs.
//   - two_free_on_one_node: one node has 2 free, everyone asks for 2 →
//     exactly one 202, 99 × 503, node exactly full.
//   - one_free_on_two_nodes: two nodes have 1 free each, everyone asks for 1 →
//     exactly two 202s, one per node. Losers of the first node must fall back
//     to the second (the savepoint path of ADR-0001 Amendment 2, where 40P01
//     used to appear).
func TestConcurrentLastGPUs(t *testing.T) {
	cases := []struct {
		name string
		// fill: GPUs created on each H100 node (by id) before the race.
		fill       map[string][]int
		ask        int
		wantWins   int
		wantNodes  int // distinct nodes among the winners
		seedNodes  int
		fullGPUs   int // H100 capacity in the fleet
		freeBefore int
	}{
		{
			name: "two_free_on_one_node", seedNodes: 1,
			fill: map[string][]int{"node-0001": {4, 2}},
			ask:  2, wantWins: 1, wantNodes: 1, fullGPUs: 8, freeBefore: 2,
		},
		{
			name: "one_free_on_two_nodes", seedNodes: 3, // node-0001 and node-0003 are H100
			fill: map[string][]int{"node-0001": {4, 2, 1}, "node-0003": {4, 2, 1}},
			ask:  1, wantWins: 2, wantNodes: 2, fullGPUs: 16, freeBefore: 2,
		},
		{
			// Ten equally good nodes: racers spread over them by the random
			// tie-break, lose, and fall back to nodes other losers are
			// trying. This is the D18 Lesson 1 shape: if a lost race kept
			// its row lock, two losers would wait on each other (40P01).
			name: "one_free_on_ten_nodes", seedNodes: 20, // 10 H100 nodes, odd ids
			fill: s111FillEachH100(10, []int{4, 2, 1}),
			ask:  1, wantWins: 10, wantNodes: 10, fullGPUs: 80, freeBefore: 10,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const goroutines = 100
			env := s111NewEnv(t, tc.seedNodes)
			ctx, cancel := context.WithTimeout(t.Context(), s111TestTimeout)
			defer cancel()
			for node, counts := range tc.fill {
				s111FillNode(t, ctx, env, node, counts)
			}
			var free int
			if err := env.db.QueryRow(ctx,
				`SELECT COALESCE(SUM(capacity - allocated), 0)::int FROM nodes WHERE gpu_type = 'H100'`).Scan(&free); err != nil {
				t.Fatalf("read free GPUs: %v", err)
			}
			if free != tc.freeBefore {
				t.Fatalf("setup: %d free H100 GPUs, want %d", free, tc.freeBefore)
			}
			// Winners hold their node lock ~20ms before committing, so losers
			// really wait on it and then lose the re-check.
			s111SlowWrites(t, env.db, 20, 0)

			var tally s111Tally
			var mu sync.Mutex
			winners := map[string]int{} // node -> wins
			s111RunConcurrently(goroutines, func(i int) {
				r, err := env.create(ctx, i, fmt.Sprintf("s111-racer-%03d", i), "k", "H100", tc.ask)
				if err != nil {
					tally.problem("create %d: %v", i, err)
					return
				}
				tally.add(fmt.Sprintf("POST %d", r.Status))
				if r.Status == http.StatusForbidden {
					tally.problem("create %d: 403 for a fresh tenant asking %d GPUs", i, tc.ask)
					return
				}
				if msg := s111CheckCreateResp(r); msg != "" {
					tally.problem("create %d: %s", i, msg)
					return
				}
				if r.Status == http.StatusAccepted {
					mu.Lock()
					winners[r.NodeID]++
					mu.Unlock()
				}
			})
			tally.report(t)
			env.logs.check(t)

			if got := tally.get("POST 202"); got != tc.wantWins || tally.get("POST 503") != goroutines-tc.wantWins {
				t.Errorf("answers %v; want exactly %d × 202 and %d × 503", tally.counts, tc.wantWins, goroutines-tc.wantWins)
			}
			if len(winners) != tc.wantNodes {
				t.Errorf("winners placed on %v; want %d distinct nodes", winners, tc.wantNodes)
			}
			var allocated int
			if err := env.db.QueryRow(ctx,
				`SELECT COALESCE(SUM(allocated), 0)::int FROM nodes WHERE gpu_type = 'H100'`).Scan(&allocated); err != nil {
				t.Fatalf("read allocated: %v", err)
			}
			if allocated != tc.fullGPUs {
				t.Errorf("H100 allocated %d after the race, want exactly %d (full, not over)", allocated, tc.fullGPUs)
			}
			s111CheckAccounting(t, ctx, env.db)
			s111CheckHistories(t, ctx, env.db, "RUNNING")
		})
	}
}

// TestConcurrentCreateDeleteChurn: 64 workers each loop create → DELETE (no
// If-Match) for several rounds; half of them also start a second DELETE on
// the same instance the moment its create commits, so it races the create's
// lifecycle walk (SCHEDULED → PROVISIONING → RUNNING) and the worker's own
// DELETE. Demand over time is far above the 64-GPU fleet and the tenants'
// quotas. A sampler checks invariants 1, 6 and 7 throughout the run. At the
// end every instance is TERMINATED with a legal one-row-per-state outbox
// history, and all GPUs and quota are back (released exactly once).
func TestConcurrentCreateDeleteChurn(t *testing.T) {
	const (
		workers = 64
		rounds  = 8
		tenants = 8
		// 4 H100 + 4 A100: 64 GPUs. 64 workers want ~240 GPUs at any moment
		// (~1900 over the run), so creates are refused all the time, yet each
		// snapshot usually lists several partly free nodes: the shape where a
		// lost-race lock could deadlock (D18 Lesson 1).
		nodes = 8
	)
	env := s111NewEnv(t, nodes)
	ctx, cancel := context.WithTimeout(t.Context(), s111TestTimeout)
	defer cancel()

	// Racing deleters poll for the committed id on their own small pool, so
	// they don't queue behind the sampler on the test pool.
	ocfg := env.db.Config()
	ocfg.MaxConns = 6
	observer, err := pgxpool.NewWithConfig(ctx, ocfg)
	if err != nil {
		t.Fatalf("observer pool: %v", err)
	}
	t.Cleanup(observer.Close)

	// Sampler: invariants 1, 6, 7 must hold at every instant, not just at the end.
	var samples atomic.Int64
	var sampleMu sync.Mutex
	var sampleViolations []string
	stopSampler := make(chan struct{})
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		tick := time.NewTicker(2 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stopSampler:
				return
			case <-tick.C:
			}
			v, err := s111Violations(ctx, env.db)
			sampleMu.Lock()
			if err != nil {
				sampleViolations = append(sampleViolations, "sample query: "+err.Error())
			} else if len(v) > 0 && len(sampleViolations) < 20 {
				sampleViolations = append(sampleViolations, v...)
			}
			sampleMu.Unlock()
			samples.Add(1)
		}
	}()

	var tally s111Tally
	var racedDeletes atomic.Int64
	s111RunConcurrently(workers, func(w int) {
		tenant := fmt.Sprintf("s111-churn-%d", w%tenants)
		racing := w%2 == 0
		for round := range rounds {
			gpuType := []string{"H100", "A100"}[(w/tenants+round)%2]
			n := s111Counts[(w+round)%4]
			key := fmt.Sprintf("w%d-r%d", w, round)

			var raceDone chan struct{}
			raceCtx, raceCancel := context.WithCancel(ctx)
			if racing {
				raceDone = make(chan struct{})
				go func() {
					defer close(raceDone)
					id, err := s111InstanceIDForKey(raceCtx, observer, tenant, key)
					if err != nil {
						return // the create was refused (we were cancelled) or ctx ended
					}
					r, err := env.del(ctx, w+1, tenant, id) // another replica than the create
					if err != nil {
						tally.problem("racing DELETE %s: %v", id, err)
						return
					}
					racedDeletes.Add(1)
					tally.add(fmt.Sprintf("DELETE %d", r.Status))
					if r.Status != http.StatusAccepted {
						tally.problem("racing DELETE %s (no If-Match): status %d code %q, want 202", id, r.Status, r.Code)
					}
				}()
			}

			r, err := env.create(ctx, w, tenant, key, gpuType, n)
			if err != nil {
				tally.problem("create %s: %v", key, err)
			} else {
				tally.add(fmt.Sprintf("POST %d", r.Status))
				if msg := s111CheckCreateResp(r); msg != "" {
					tally.problem("create %s (%s, %d %s): %s", key, tenant, n, gpuType, msg)
				}
			}
			if err != nil || r.Status != http.StatusAccepted {
				raceCancel() // nothing committed: stop the racing deleter
			}
			if err == nil && r.Status == http.StatusAccepted {
				d, err := env.del(ctx, w+2, tenant, r.ID)
				if err != nil {
					tally.problem("DELETE %s: %v", r.ID, err)
				} else {
					tally.add(fmt.Sprintf("DELETE %d", d.Status))
					if d.Status != http.StatusAccepted {
						tally.problem("DELETE %s (no If-Match): status %d code %q, want 202", r.ID, d.Status, d.Code)
					}
				}
			}
			if raceDone != nil {
				<-raceDone
			}
			raceCancel()
		}
	})
	close(stopSampler)
	<-samplerDone

	tally.report(t)
	env.logs.check(t)
	t.Logf("answers: %v; racing DELETEs sent: %d; accounting samples: %d", tally.counts, racedDeletes.Load(), samples.Load())

	sampleMu.Lock()
	if len(sampleViolations) > 0 {
		t.Errorf("accounting broken DURING the run (%d sample findings):\n  %v", len(sampleViolations), sampleViolations)
	}
	sampleMu.Unlock()
	if samples.Load() < 5 {
		t.Errorf("only %d accounting samples were taken during the run; the sampler did not really run alongside", samples.Load())
	}
	if tally.get("POST 202") == 0 || tally.get("POST 503")+tally.get("POST 403") == 0 {
		t.Errorf("want both accepted and refused creates (demand over capacity), got %v", tally.counts)
	}
	if want := tally.get("POST 202") + int(racedDeletes.Load()); tally.get("DELETE 202") != want {
		t.Errorf("DELETE 202s = %d, want %d (one per accepted create plus one per racing DELETE)",
			tally.get("DELETE 202"), want)
	}

	s111CheckAccounting(t, ctx, env.db)
	allocated, used := s111Totals(t, ctx, env.db)
	if allocated != 0 || used != 0 {
		t.Errorf("after deleting everything: allocated %d, used %d; want 0 and 0 (released exactly once)", allocated, used)
	}
	var instances int
	if err := env.db.QueryRow(ctx, `SELECT count(*) FROM instances`).Scan(&instances); err != nil {
		t.Fatalf("count instances: %v", err)
	}
	if instances != tally.get("POST 202") {
		t.Errorf("%d instance rows, %d creates got 202", instances, tally.get("POST 202"))
	}
	cut := s111CheckHistories(t, ctx, env.db, "TERMINATED")
	t.Logf("%d of %d instances had their create walk cut short by a racing DELETE", cut, instances)
}

// s111FillNode creates the given GPU counts on one node through the API
// (so every allocated GPU belongs to a real instance and invariant 7 holds),
// by making that node the only READY one for the duration. Each
// create uses a fresh tenant so quota never interferes.
func s111FillNode(t *testing.T, ctx context.Context, env *s111Env, node string, counts []int) {
	t.Helper()
	if _, err := env.db.Exec(ctx, `UPDATE nodes SET status = CASE WHEN id = $1 THEN 'READY' ELSE 'CORDONED' END`, node); err != nil {
		t.Fatalf("isolate %s: %v", node, err)
	}
	for i, n := range counts {
		var gpuType string
		if err := env.db.QueryRow(ctx, `SELECT gpu_type FROM nodes WHERE id = $1`, node).Scan(&gpuType); err != nil {
			t.Fatalf("read %s: %v", node, err)
		}
		r, err := env.create(ctx, i, fmt.Sprintf("s111-filler-%s-%d", node, i), "k", gpuType, n)
		if err != nil || r.Status != http.StatusAccepted || r.NodeID != node {
			t.Fatalf("fill %s with %d GPUs: %+v, %v", node, n, r, err)
		}
	}
	if _, err := env.db.Exec(ctx, `UPDATE nodes SET status = 'READY'`); err != nil {
		t.Fatalf("restore READY: %v", err)
	}
}

// s111FillEachH100 returns a fill plan giving each of the first h100 H100
// nodes (odd ids in seed.Nodes) the same counts.
func s111FillEachH100(h100 int, counts []int) map[string][]int {
	plan := map[string][]int{}
	for k := range h100 {
		plan[fmt.Sprintf("node-%04d", 2*k+1)] = counts
	}
	return plan
}

// s111CheckIDsExist: every id a 202 returned is a distinct, stored instance.
func s111CheckIDsExist(t *testing.T, ctx context.Context, db *pgxpool.Pool, ids []string) {
	t.Helper()
	var want []string
	seen := map[string]bool{}
	for _, id := range ids {
		if id == "" {
			continue
		}
		if seen[id] {
			t.Errorf("two 202s returned the same instance id %s", id)
		}
		seen[id] = true
		want = append(want, id)
	}
	var found int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM instances WHERE id = ANY($1)`, want).Scan(&found); err != nil {
		t.Fatalf("look up ids: %v", err)
	}
	if found != len(want) {
		t.Errorf("%d of the %d ids returned by 202s exist", found, len(want))
	}
}
