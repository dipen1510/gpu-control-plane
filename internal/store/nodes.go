package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Node statuses written by node sync in M2a (ADR-0002 §Node sync, D39).
// PROBATION and QUARANTINED are for the M2b health policy.
const (
	NodeReady    = "READY"
	NodeNotReady = "NOT_READY"
	NodeCordoned = "CORDONED"
)

// LastSeenRefresh is how stale last_seen may get before an otherwise
// unchanged observation rewrites the row. Below it, re-syncing an unchanged
// Node writes nothing, so informer resyncs and no-op Node updates cost no
// row versions.
const LastSeenRefresh = time.Minute

// NodeObservation is what node sync saw for one Kubernetes GPU Node.
type NodeObservation struct {
	Name     string // nodes.id
	GPUType  string // written on insert only
	Capacity int
	// CapacityKnown is false when the Node reports no GPU resource yet
	// (kubelet starting). The stored capacity is then kept; a new row is
	// inserted NOT_READY with capacity 0.
	CapacityKnown bool
	FailureDomain string
	Status        string // NodeReady, NodeNotReady or NodeCordoned
}

// NodeSyncResult is what SyncNode did with one observation.
type NodeSyncResult struct {
	// Wrote is false when the row already matched and last_seen was fresh.
	Wrote bool
	// CapacityConflict: the reported capacity was below nodes.allocated, so
	// the stored capacity was kept (invariant 1, ADR-0002 §Node sync).
	CapacityConflict bool
	// TypeConflict: the Node's gpu type label differs from the stored
	// gpu_type, which was kept (gpu_type is insert only).
	TypeConflict bool
	// StoredGPUType is nodes.gpu_type after the call.
	StoredGPUType string
}

// constraintAllocatedWithinCapacity is the CHECK from migration 0001.
const constraintAllocatedWithinCapacity = "nodes_allocated_within_capacity"

// syncNodeSQL upserts one node by name. Column ownership (D15): node sync
// owns gpu_type (insert only), capacity, failure_domain, status,
// status_changed_at and last_seen; the allocation column is never named
// here, so placement and release keep sole ownership of it. status is always
// set explicitly because the column default is READY (S2-17).
//
// $3 NULL means "keep the stored capacity" (unknown capacity, or the retry
// after a capacity conflict); a new row then gets capacity 0 and NOT_READY
// (no GPUs reported yet), an existing row keeps its capacity and takes $5.
// Capacity below the allocation is rejected by the row's CHECK constraint,
// which Postgres evaluates under the row lock, so a concurrent placement can
// never slip between a read and this write (SyncNode retries with $3 NULL).
//
// The DO UPDATE runs only when a synced column differs or last_seen is older
// than $6 seconds, so an unchanged Node writes nothing. The outer SELECT
// returns the stored gpu_type either way (from the statement snapshot when
// nothing was written), so a type conflict is seen even without a write.
const syncNodeSQL = `
WITH up AS (
    INSERT INTO nodes (id, gpu_type, capacity, failure_domain, status, status_changed_at, last_seen)
    VALUES ($1, $2, COALESCE($3::integer, 0), $4,
            CASE WHEN $3::integer IS NULL THEN 'NOT_READY' ELSE $5::text END, now(), now())
    ON CONFLICT (id) DO UPDATE SET
        capacity          = COALESCE($3::integer, nodes.capacity),
        failure_domain    = EXCLUDED.failure_domain,
        status_changed_at = CASE WHEN nodes.status = $5::text
                                 THEN nodes.status_changed_at ELSE now() END,
        status            = $5::text,
        last_seen         = now()
    WHERE nodes.capacity       IS DISTINCT FROM COALESCE($3::integer, nodes.capacity)
       OR nodes.failure_domain IS DISTINCT FROM EXCLUDED.failure_domain
       OR nodes.status         IS DISTINCT FROM $5::text
       OR nodes.last_seen < now() - make_interval(secs => $6)
    RETURNING gpu_type
)
SELECT gpu_type, true FROM up
UNION ALL
SELECT gpu_type, false FROM nodes WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM up)`

// SyncNode upserts the inventory row for obs. It is safe for concurrent
// use; two writers of the same name serialize on the row lock taken by ON
// CONFLICT.
func (s *Store) SyncNode(ctx context.Context, obs NodeObservation) (NodeSyncResult, error) {
	var capacity *int
	if obs.CapacityKnown {
		capacity = &obs.Capacity
	}
	res, err := s.syncNode(ctx, obs, capacity)
	if capacity != nil && isAllocatedWithinCapacity(err) {
		res, err = s.syncNode(ctx, obs, nil)
		res.CapacityConflict = true
	}
	if err != nil {
		return NodeSyncResult{}, fmt.Errorf("sync node %s: %w", obs.Name, err)
	}
	res.TypeConflict = res.StoredGPUType != obs.GPUType
	return res, nil
}

func (s *Store) syncNode(ctx context.Context, obs NodeObservation, capacity *int) (NodeSyncResult, error) {
	var res NodeSyncResult
	err := s.db.QueryRow(ctx, syncNodeSQL,
		obs.Name, obs.GPUType, capacity, obs.FailureDomain, obs.Status,
		LastSeenRefresh.Seconds()).Scan(&res.StoredGPUType, &res.Wrote)
	if errors.Is(err, pgx.ErrNoRows) {
		// Only possible if the row was inserted and deleted concurrently;
		// nodes rows are never deleted (D15).
		return res, fmt.Errorf("node row vanished")
	}
	return res, err
}

func isAllocatedWithinCapacity(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.ConstraintName == constraintAllocatedWithinCapacity
}

// markNotReadySQL sets one row NOT_READY (Node deleted or lost its GPU
// label). The row is kept, never deleted (D15). last_seen is not touched:
// the Node was not seen.
const markNotReadySQL = `
UPDATE nodes SET status = 'NOT_READY', status_changed_at = now()
WHERE id = $1 AND status <> 'NOT_READY'
RETURNING id`

// MarkNodeNotReady sets the row for name NOT_READY. It reports whether a
// row changed; a missing row (a Node that never had one) is not an error.
func (s *Store) MarkNodeNotReady(ctx context.Context, name string) (bool, error) {
	var id string
	err := s.db.QueryRow(ctx, markNotReadySQL, name).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("mark node %s not ready: %w", name, err)
	}
	return true, nil
}

// sweepNodesSQL sets NOT_READY every row whose id is not in $1 (the GPU
// Nodes currently in the informer cache) and returns the ids it changed.
const sweepNodesSQL = `
WITH m AS (
    UPDATE nodes SET status = 'NOT_READY', status_changed_at = now()
    WHERE status <> 'NOT_READY' AND NOT (id = ANY($1::text[]))
    RETURNING id
)
SELECT COALESCE(array_agg(id ORDER BY id), '{}') FROM m`

// SweepNodes marks NOT_READY every row not named in present and returns
// the names it changed. The caller must pass a complete list (a synced
// cache): an empty list marks the whole fleet NOT_READY.
func (s *Store) SweepNodes(ctx context.Context, present []string) ([]string, error) {
	if present == nil {
		present = []string{}
	}
	var marked []string
	if err := s.db.QueryRow(ctx, sweepNodesSQL, present).Scan(&marked); err != nil {
		return nil, fmt.Errorf("sweep nodes: %w", err)
	}
	return marked, nil
}

// NodeCount is the number of nodes with one gpu_type and status.
type NodeCount struct {
	GPUType string
	Status  string
	Count   int
}

const nodeCountsSQL = `
SELECT COALESCE(array_agg(gpu_type), '{}'), COALESCE(array_agg(status), '{}'), COALESCE(array_agg(n), '{}')
FROM (SELECT gpu_type, status, count(*)::integer AS n FROM nodes GROUP BY 1, 2) c`

// NodeCounts returns the node count per gpu_type and status (the
// gpucp_nodes gauge).
func (s *Store) NodeCounts(ctx context.Context) ([]NodeCount, error) {
	var types, statuses []string
	var counts []int32
	if err := s.db.QueryRow(ctx, nodeCountsSQL).Scan(&types, &statuses, &counts); err != nil {
		return nil, fmt.Errorf("count nodes: %w", err)
	}
	out := make([]NodeCount, len(types))
	for i := range types {
		out[i] = NodeCount{GPUType: types[i], Status: statuses[i], Count: int(counts[i])}
	}
	return out, nil
}
