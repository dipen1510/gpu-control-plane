//go:build integration

package store

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

type nodeRow struct {
	gpuType, domain, status string
	capacity, allocated     int
	statusChangedAt         time.Time
	lastSeen                time.Time
	xmin                    uint32
}

func readNode(t *testing.T, db *pgxpool.Pool, id string) nodeRow {
	t.Helper()
	var r nodeRow
	if err := db.QueryRow(t.Context(), `SELECT gpu_type, failure_domain, status, capacity, allocated,
		status_changed_at, last_seen, xmin::text::bigint FROM nodes WHERE id = $1`, id).
		Scan(&r.gpuType, &r.domain, &r.status, &r.capacity, &r.allocated, &r.statusChangedAt, &r.lastSeen, &r.xmin); err != nil {
		t.Fatalf("read node %s: %v", id, err)
	}
	return r
}

func syncNode(t *testing.T, s *Store, obs NodeObservation) bool {
	t.Helper()
	wrote, err := s.SyncNode(t.Context(), obs)
	if err != nil {
		t.Fatalf("sync node: %v", err)
	}
	return wrote
}

func TestNodeSyncNeverTouchesAllocated(t *testing.T) {
	db := testutil.NewDB(t)
	s := New(db, nil)
	obs := NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, FailureDomain: "zone-a", Status: NodeReady}
	syncNode(t, s, obs)
	if _, err := db.Exec(t.Context(), `UPDATE nodes SET allocated = 4 WHERE id = 'w1'`); err != nil {
		t.Fatalf("set allocated: %v", err)
	}

	obs.Status, obs.FailureDomain, obs.Capacity = NodeCordoned, "zone-b", 6
	if !syncNode(t, s, obs) {
		t.Fatal("changed observation wrote nothing")
	}
	got := readNode(t, db, "w1")
	if got.allocated != 4 {
		t.Fatalf("allocated = %d after sync, want 4", got.allocated)
	}
	if got.status != NodeCordoned || got.domain != "zone-b" || got.capacity != 6 {
		t.Fatalf("row = %+v, want CORDONED/zone-b/6", got)
	}
}

func TestNodeSyncInsertSetsColumns(t *testing.T) {
	db := testutil.NewDB(t)
	s := New(db, nil)
	if !syncNode(t, s, NodeObservation{Name: "w1", GPUType: "A100", Capacity: 8, FailureDomain: "z", Status: NodeNotReady}) {
		t.Fatal("insert reported no write")
	}
	got := readNode(t, db, "w1")
	// status must be written explicitly: the column default is READY (S2-17).
	if got.gpuType != "A100" || got.capacity != 8 || got.domain != "z" || got.status != NodeNotReady || got.allocated != 0 {
		t.Fatalf("inserted row = %+v", got)
	}
}

func TestNodeSyncGPUTypeInsertOnly(t *testing.T) {
	db := testutil.NewDB(t)
	s := New(db, nil)
	syncNode(t, s, NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, Status: NodeReady})
	syncNode(t, s, NodeObservation{Name: "w1", GPUType: "A100", Capacity: 8, Status: NodeCordoned})
	if got := readNode(t, db, "w1"); got.gpuType != "H100" {
		t.Fatalf("gpu_type = %q, want H100 (insert only)", got.gpuType)
	}
}

func TestNodeSyncIdempotent(t *testing.T) {
	db := testutil.NewDB(t)
	s := New(db, nil)
	obs := NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, FailureDomain: "zone-a", Status: NodeReady}
	syncNode(t, s, obs)
	before := readNode(t, db, "w1")
	for range 3 {
		if syncNode(t, s, obs) {
			t.Fatal("unchanged observation reported a write")
		}
	}
	after := readNode(t, db, "w1")
	if after != before {
		t.Fatalf("row changed on unchanged sync: before %+v after %+v", before, after)
	}
}

func TestNodeSyncRefreshesStaleLastSeen(t *testing.T) {
	db := testutil.NewDB(t)
	s := New(db, nil)
	obs := NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, Status: NodeReady}
	syncNode(t, s, obs)
	if _, err := db.Exec(t.Context(), `UPDATE nodes SET last_seen = now() - interval '2 minutes', status_changed_at = now() - interval '1 hour' WHERE id = 'w1'`); err != nil {
		t.Fatalf("age row: %v", err)
	}
	before := readNode(t, db, "w1")
	if !syncNode(t, s, obs) {
		t.Fatal("stale last_seen not refreshed")
	}
	after := readNode(t, db, "w1")
	if !after.lastSeen.After(before.lastSeen) {
		t.Fatalf("last_seen %v not after %v", after.lastSeen, before.lastSeen)
	}
	if !after.statusChangedAt.Equal(before.statusChangedAt) {
		t.Fatalf("status_changed_at moved without a status change: %v -> %v", before.statusChangedAt, after.statusChangedAt)
	}
}

func TestNodeSyncStatusChangedAtOnlyOnChange(t *testing.T) {
	db := testutil.NewDB(t)
	s := New(db, nil)
	obs := NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, FailureDomain: "a", Status: NodeReady}
	syncNode(t, s, obs)
	if _, err := db.Exec(t.Context(), `UPDATE nodes SET status_changed_at = now() - interval '1 hour' WHERE id = 'w1'`); err != nil {
		t.Fatalf("age row: %v", err)
	}
	old := readNode(t, db, "w1").statusChangedAt

	obs.FailureDomain = "b" // a non-status change
	syncNode(t, s, obs)
	if got := readNode(t, db, "w1").statusChangedAt; !got.Equal(old) {
		t.Fatalf("status_changed_at moved on a domain change: %v -> %v", old, got)
	}
	obs.Status = NodeNotReady
	syncNode(t, s, obs)
	if got := readNode(t, db, "w1").statusChangedAt; !got.After(old) {
		t.Fatalf("status_changed_at %v not moved on status change (was %v)", got, old)
	}
}
