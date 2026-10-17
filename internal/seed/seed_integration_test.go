//go:build integration

package seed_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/seed"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

type row struct {
	GPUType       string
	Capacity      int
	Allocated     int
	Status        string
	FailureDomain string
	LastSeen      time.Time
}

func readNodes(t *testing.T, db *pgxpool.Pool) map[string]row {
	t.Helper()
	rows, err := db.Query(t.Context(),
		`SELECT id, gpu_type, capacity, allocated, status, failure_domain, last_seen FROM nodes`)
	if err != nil {
		t.Fatalf("query nodes: %v", err)
	}
	defer rows.Close()
	out := map[string]row{}
	for rows.Next() {
		var id string
		var r row
		if err := rows.Scan(&id, &r.GPUType, &r.Capacity, &r.Allocated, &r.Status,
			&r.FailureDomain, &r.LastSeen); err != nil {
			t.Fatalf("scan node: %v", err)
		}
		out[id] = r
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read nodes: %v", err)
	}
	return out
}

func mustRun(t *testing.T, db *pgxpool.Pool, n int) {
	t.Helper()
	if err := seed.Run(t.Context(), db, n); err != nil {
		t.Fatalf("seed.Run(%d): %v", n, err)
	}
}

func TestSeedNodesIsIdempotent(t *testing.T) {
	db := testutil.NewDB(t)
	const n = 10
	mustRun(t, db, n)
	mustRun(t, db, n)

	got := readNodes(t, db)
	if len(got) != n {
		t.Fatalf("got %d node rows after seeding twice, want %d", len(got), n)
	}
	want, err := seed.Nodes(n)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range want {
		r, ok := got[w.ID]
		if !ok {
			t.Errorf("node %s missing", w.ID)
			continue
		}
		if r.GPUType != w.GPUType || r.Capacity != w.Capacity || r.Status != w.Status ||
			r.FailureDomain != w.FailureDomain || r.Allocated != 0 {
			t.Errorf("node %s = %+v, want %+v with allocated 0", w.ID, r, w)
		}
	}
}

func TestSeedNodesNeverTouchesAllocated(t *testing.T) {
	db := testutil.NewDB(t)
	ctx := t.Context()
	mustRun(t, db, 4)
	// Stand in for placement, and for a stale sync: drift the inventory
	// columns so the re-seed has something to restore.
	if _, err := db.Exec(ctx, `UPDATE nodes SET allocated = 4, failure_domain = 'drifted',
		last_seen = now() - interval '1 hour' WHERE id = 'node-0001'`); err != nil {
		t.Fatalf("reserve GPUs: %v", err)
	}
	before := readNodes(t, db)["node-0001"]

	mustRun(t, db, 4)

	after := readNodes(t, db)["node-0001"]
	if after.Allocated != 4 {
		t.Errorf("allocated = %d after re-seed, want 4", after.Allocated)
	}
	// Proves the re-seed really updated this row, so allocated surviving is
	// not just because the row was skipped.
	if after.FailureDomain == "drifted" || !after.LastSeen.After(before.LastSeen) {
		t.Errorf("re-seed did not refresh inventory columns: before %+v, after %+v", before, after)
	}
}

// D15: the seed never deletes or disables nodes. Seeding fewer nodes than
// before leaves the extra ones exactly as they were.
func TestSeedNodesWithSmallerCountKeepsOtherNodes(t *testing.T) {
	db := testutil.NewDB(t)
	mustRun(t, db, 6)
	before := readNodes(t, db)

	mustRun(t, db, 2)

	after := readNodes(t, db)
	if len(after) != 6 {
		t.Fatalf("got %d rows after re-seeding with N=2, want 6", len(after))
	}
	for _, id := range []string{"node-0003", "node-0004", "node-0005", "node-0006"} {
		if after[id] != before[id] {
			t.Errorf("node %s changed: before %+v, after %+v", id, before[id], after[id])
		}
	}
}

func TestSeedNodesMigratesEmptyDatabase(t *testing.T) {
	db := testutil.NewEmptyDB(t)
	mustRun(t, db, 3)
	if got := len(readNodes(t, db)); got != 3 {
		t.Fatalf("got %d rows, want 3", got)
	}
}
