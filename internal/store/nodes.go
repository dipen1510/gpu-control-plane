package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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
	Name          string // nodes.id
	GPUType       string // written on insert only
	Capacity      int
	FailureDomain string
	Status        string // NodeReady, NodeNotReady or NodeCordoned
}

// syncNodeSQL upserts one node by name. Column ownership (D15): node sync
// owns gpu_type (insert only), capacity, failure_domain, status,
// status_changed_at and last_seen; the allocation column is never named
// here, so placement and release keep sole ownership of it. status is always
// set explicitly because the column default is READY (S2-17).
//
// The DO UPDATE runs only when a synced column differs or last_seen is older
// than $6 seconds, so an unchanged Node writes nothing; RETURNING then yields no row.
const syncNodeSQL = `
INSERT INTO nodes (id, gpu_type, capacity, failure_domain, status, status_changed_at, last_seen)
VALUES ($1, $2, $3, $4, $5, now(), now())
ON CONFLICT (id) DO UPDATE SET
    capacity          = EXCLUDED.capacity,
    failure_domain    = EXCLUDED.failure_domain,
    status_changed_at = CASE WHEN nodes.status = EXCLUDED.status
                             THEN nodes.status_changed_at ELSE now() END,
    status            = EXCLUDED.status,
    last_seen         = now()
WHERE nodes.capacity       IS DISTINCT FROM EXCLUDED.capacity
   OR nodes.failure_domain IS DISTINCT FROM EXCLUDED.failure_domain
   OR nodes.status         IS DISTINCT FROM EXCLUDED.status
   OR nodes.last_seen < now() - make_interval(secs => $6)
RETURNING id`

// SyncNode upserts the inventory row for obs. It reports whether a row was
// written (false: the row already matched and last_seen was fresh). It is
// safe for concurrent use; two writers of the same name serialize on the row
// lock taken by ON CONFLICT.
func (s *Store) SyncNode(ctx context.Context, obs NodeObservation) (bool, error) {
	var id string
	err := s.db.QueryRow(ctx, syncNodeSQL,
		obs.Name, obs.GPUType, obs.Capacity, obs.FailureDomain, obs.Status,
		LastSeenRefresh.Seconds()).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sync node %s: %w", obs.Name, err)
	}
	return true, nil
}
