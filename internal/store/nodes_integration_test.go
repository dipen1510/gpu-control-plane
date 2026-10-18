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
	res, err := s.SyncNode(t.Context(), obs)
	if err != nil {
		t.Fatalf("sync node: %v", err)
	}
	return res.Wrote
}

func TestNodeSyncNeverTouchesAllocated(t *testing.T) {
	db := testutil.NewDB(t)
	s := New(db, nil)
	obs := NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, CapacityKnown: true, FailureDomain: "zone-a", Status: NodeReady}
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
	if !syncNode(t, s, NodeObservation{Name: "w1", GPUType: "A100", Capacity: 8, CapacityKnown: true, FailureDomain: "z", Status: NodeNotReady}) {
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
	syncNode(t, s, NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, CapacityKnown: true, Status: NodeReady})
	syncNode(t, s, NodeObservation{Name: "w1", GPUType: "A100", Capacity: 8, CapacityKnown: true, Status: NodeCordoned})
	if got := readNode(t, db, "w1"); got.gpuType != "H100" {
		t.Fatalf("gpu_type = %q, want H100 (insert only)", got.gpuType)
	}
}

func TestNodeSyncIdempotent(t *testing.T) {
	db := testutil.NewDB(t)
	s := New(db, nil)
	obs := NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, CapacityKnown: true, FailureDomain: "zone-a", Status: NodeReady}
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
	obs := NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, CapacityKnown: true, Status: NodeReady}
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
	obs := NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, CapacityKnown: true, FailureDomain: "a", Status: NodeReady}
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

func TestNodeCapacityBelowAllocatedNotClobbered(t *testing.T) {
	db := testutil.NewDB(t)
	s := New(db, nil)
	syncNode(t, s, NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, CapacityKnown: true, FailureDomain: "a", Status: NodeReady})
	if _, err := db.Exec(t.Context(), `UPDATE nodes SET allocated = 6 WHERE id = 'w1'`); err != nil {
		t.Fatalf("set allocated: %v", err)
	}
	res, err := s.SyncNode(t.Context(), NodeObservation{Name: "w1", GPUType: "H100", Capacity: 4, CapacityKnown: true, FailureDomain: "b", Status: NodeCordoned})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if !res.CapacityConflict || !res.Wrote || res.TypeConflict {
		t.Fatalf("result = %+v, want capacity conflict, written, no type conflict", res)
	}
	got := readNode(t, db, "w1")
	if got.capacity != 8 || got.allocated != 6 {
		t.Fatalf("capacity/allocated = %d/%d, want 8/6", got.capacity, got.allocated)
	}
	if got.domain != "b" || got.status != NodeCordoned {
		t.Fatalf("other columns not synced: %+v", got)
	}
	// Capacity equal to allocated is fine.
	res, err = s.SyncNode(t.Context(), NodeObservation{Name: "w1", GPUType: "H100", Capacity: 6, CapacityKnown: true, FailureDomain: "b", Status: NodeCordoned})
	if err != nil || res.CapacityConflict {
		t.Fatalf("capacity = allocated: %+v, %v", res, err)
	}
	if got := readNode(t, db, "w1"); got.capacity != 6 {
		t.Fatalf("capacity = %d, want 6", got.capacity)
	}
}

func TestNodeSyncUnknownCapacity(t *testing.T) {
	db := testutil.NewDB(t)
	s := New(db, nil)
	// New row: inserted NOT_READY with capacity 0, whatever the Ready condition says.
	syncNode(t, s, NodeObservation{Name: "new", GPUType: "H100", Status: NodeReady})
	if got := readNode(t, db, "new"); got.capacity != 0 || got.status != NodeNotReady {
		t.Fatalf("new row = %+v, want capacity 0 NOT_READY", got)
	}
	// Existing row: capacity kept, status still synced.
	syncNode(t, s, NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, CapacityKnown: true, Status: NodeReady})
	syncNode(t, s, NodeObservation{Name: "w1", GPUType: "H100", Status: NodeCordoned})
	if got := readNode(t, db, "w1"); got.capacity != 8 || got.status != NodeCordoned {
		t.Fatalf("existing row = %+v, want capacity 8 CORDONED", got)
	}
}

func TestNodeSyncTypeConflictReported(t *testing.T) {
	db := testutil.NewDB(t)
	s := New(db, nil)
	obs := NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, CapacityKnown: true, Status: NodeReady}
	syncNode(t, s, obs)
	obs.GPUType = "A100"
	// Reported even when nothing else changed and no row is written.
	res, err := s.SyncNode(t.Context(), obs)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if res.Wrote || !res.TypeConflict || res.StoredGPUType != "H100" {
		t.Fatalf("result = %+v, want no write, type conflict, stored H100", res)
	}
}

func TestNodeDeletedMarksNotReady(t *testing.T) {
	db := testutil.NewDB(t)
	s := New(db, nil)
	syncNode(t, s, NodeObservation{Name: "w1", GPUType: "H100", Capacity: 8, CapacityKnown: true, Status: NodeReady})
	if _, err := db.Exec(t.Context(), `UPDATE nodes SET allocated = 2, status_changed_at = now() - interval '1 hour' WHERE id = 'w1'`); err != nil {
		t.Fatal(err)
	}
	before := readNode(t, db, "w1")
	changed, err := s.MarkNodeNotReady(t.Context(), "w1")
	if err != nil || !changed {
		t.Fatalf("mark = %v, %v; want true", changed, err)
	}
	got := readNode(t, db, "w1")
	if got.status != NodeNotReady || got.allocated != 2 || got.capacity != 8 || !got.statusChangedAt.After(before.statusChangedAt) {
		t.Fatalf("row = %+v, want NOT_READY, allocated 2, capacity 8, status_changed_at moved", got)
	}
	if changed, err := s.MarkNodeNotReady(t.Context(), "w1"); err != nil || changed {
		t.Fatalf("second mark = %v, %v; want no write", changed, err)
	}
	if changed, err := s.MarkNodeNotReady(t.Context(), "never-had-a-row"); err != nil || changed {
		t.Fatalf("missing row = %v, %v; want false, nil", changed, err)
	}
}

func TestNodeSweepMarksMissing(t *testing.T) {
	db := testutil.NewDB(t)
	s := New(db, nil)
	for _, n := range []string{"w1", "w2", "w3"} {
		syncNode(t, s, NodeObservation{Name: n, GPUType: "H100", Capacity: 8, CapacityKnown: true, Status: NodeReady})
	}
	marked, err := s.SweepNodes(t.Context(), []string{"w1", "w3"})
	if err != nil {
		t.Fatal(err)
	}
	if len(marked) != 1 || marked[0] != "w2" {
		t.Fatalf("marked = %v, want [w2]", marked)
	}
	for n, want := range map[string]string{"w1": NodeReady, "w2": NodeNotReady, "w3": NodeReady} {
		if got := readNode(t, db, n).status; got != want {
			t.Fatalf("%s status = %s, want %s", n, got, want)
		}
	}
	counts, err := s.NodeCounts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]int{}
	for _, c := range counts {
		byKey[c.GPUType+"/"+c.Status] = c.Count
	}
	if byKey["H100/READY"] != 2 || byKey["H100/NOT_READY"] != 1 || len(byKey) != 2 {
		t.Fatalf("counts = %v", byKey)
	}
}
