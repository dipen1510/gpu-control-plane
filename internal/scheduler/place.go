package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
)

// ErrInsufficientCapacity is returned by Place when no node could take the
// request. The API maps it to 503 insufficient_capacity with Retry-After.
var ErrInsufficientCapacity = errors.New("insufficient capacity")

// Counter counts events. It must be safe for concurrent use. A Prometheus
// counter satisfies it.
type Counter interface {
	Inc()
}

// Observer records a value, e.g. a duration in seconds. It must be safe for
// concurrent use. A Prometheus histogram satisfies it.
type Observer interface {
	Observe(float64)
}

// Option configures a Scheduler.
type Option func(*Scheduler)

// WithPlaceDuration makes Place report how long each call took, in seconds,
// whatever its outcome (gpucp_scheduler_place_duration_seconds). Each reserve
// attempt costs 3 round trips (ADR-0001 Amendment 2), so this shows the price
// of contention.
func WithPlaceDuration(o Observer) Option {
	return func(s *Scheduler) { s.placeDuration = o }
}

// Scheduler places instances on nodes. It holds no per-request state, so one
// Scheduler is safe for concurrent use by many goroutines (and correctness
// across replicas comes from the database, not from anything in here).
type Scheduler struct {
	ranker *Ranker

	// reserveConflicts counts candidates that looked free in the snapshot but
	// refused the reservation, i.e. lost a race (gpucp_scheduler_reserve_conflicts_total).
	reserveConflicts Counter

	// placeDuration, if set, observes the duration of each Place call.
	placeDuration Observer

	// beforeReserve, if set, runs between the snapshot and the first reserve.
	// Tests use it to change nodes behind Place's back; production leaves it nil.
	beforeReserve func(ctx context.Context, candidates []NodeID)
}

// New returns a Scheduler with the default ranking. Its random tie-break
// uses math/rand/v2's global source, which is safe for concurrent use; a
// seeded *rand.Rand shared across requests would be a data race.
// reserveConflicts may be nil.
func New(reserveConflicts Counter, opts ...Option) *Scheduler {
	s := &Scheduler{ranker: NewRanker(nil), reserveConflicts: reserveConflicts}
	for _, o := range opts {
		o(s)
	}
	return s
}

// snapshotSQL reads candidate nodes and, in the same statement and therefore
// the same snapshot, whether the tenant already holds capacity in each node's
// failure domain. It takes no row locks: the snapshot is only a hint, the
// conditional UPDATE in reserveSQL decides.
const snapshotSQL = `
WITH tenant_domains AS (
    SELECT DISTINCT m.failure_domain
    FROM instances i JOIN nodes m ON m.id = i.node_id
    WHERE i.tenant_id = $3 AND i.state = ANY($4::text[])
)
SELECT n.id, n.gpu_type, n.capacity, n.allocated, n.schedulable, n.failure_domain,
       n.failure_domain IN (SELECT failure_domain FROM tenant_domains)
FROM nodes n
WHERE n.gpu_type = $1 AND n.schedulable AND n.allocated + $2 <= n.capacity`

// reserveSQL is the reservation and the guard for invariant 1. Under READ
// COMMITTED, if another transaction holds the row, Postgres waits for it to
// finish and re-checks the WHERE against the newly committed row, so two
// transactions can never both reserve the last GPUs.
//
// Losing that race still leaves a lock behind: an UPDATE that waited for a
// row lock keeps it until our transaction ends, even when the re-check then
// matches 0 rows. Place therefore runs each attempt inside a savepoint and
// rolls back to it on 0 rows, which releases locks taken since the savepoint.
const reserveSQL = `
UPDATE nodes SET allocated = allocated + $2
WHERE id = $1 AND schedulable AND allocated + $2 <= capacity`

// Place reserves req.GPUCount GPUs of req.GPUType on one node inside tx and
// returns that node. The caller commits or rolls back tx; a rollback undoes
// the reservation.
//
// It reads a lock-free snapshot, ranks the candidates, then tries a
// conditional UPDATE on each, best first, each in its own savepoint. A
// candidate that filled up or was drained since the snapshot updates 0 rows;
// Place rolls back to the savepoint, which drops any row lock the UPDATE
// waited for, and moves on to the next one. So when Place returns, tx holds a
// lock on the reserved node only, never on a losing candidate; holding
// several node locks would let two creates deadlock on each other.
// If no candidate can be reserved, Place returns ErrInsufficientCapacity.
//
// req.TenantFailureDomains is ignored; Place fills it from the snapshot.
func (s *Scheduler) Place(ctx context.Context, tx pgx.Tx, req Request) (NodeID, error) {
	if s.placeDuration != nil {
		start := time.Now()
		defer func() { s.placeDuration.Observe(time.Since(start).Seconds()) }()
	}
	if req.GPUCount < 1 {
		// A zero or negative count would "reserve" nothing or even free GPUs.
		return "", fmt.Errorf("place %d %s GPUs: gpu count must be at least 1", req.GPUCount, req.GPUType)
	}

	nodes, domains, err := snapshot(ctx, tx, req)
	if err != nil {
		return "", fmt.Errorf("place %d %s GPUs: %w", req.GPUCount, req.GPUType, err)
	}
	req.TenantFailureDomains = domains
	candidates := s.ranker.Rank(nodes, req)

	if s.beforeReserve != nil {
		s.beforeReserve(ctx, candidates)
	}

	for _, id := range candidates {
		ok, err := reserve(ctx, tx, id, req.GPUCount)
		if err != nil {
			return "", fmt.Errorf("reserve %d %s GPUs on node %s: %w", req.GPUCount, req.GPUType, id, err)
		}
		if ok {
			return id, nil
		}
		if s.reserveConflicts != nil {
			s.reserveConflicts.Inc()
		}
	}
	return "", fmt.Errorf("place %d %s GPUs (%d candidates): %w",
		req.GPUCount, req.GPUType, len(candidates), ErrInsufficientCapacity)
}

// reserve tries to reserve n GPUs on node id. It reports false, and leaves
// tx holding no lock on the node, if the node no longer has room or is no
// longer schedulable. On error tx is unusable and the caller must roll it
// back, which releases everything.
func reserve(ctx context.Context, tx pgx.Tx, id NodeID, n int) (bool, error) {
	sp, err := tx.Begin(ctx) // SAVEPOINT
	if err != nil {
		return false, fmt.Errorf("savepoint: %w", err)
	}
	tag, err := sp.Exec(ctx, reserveSQL, id, n)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 1 {
		if err := sp.Commit(ctx); err != nil { // RELEASE SAVEPOINT keeps the update and its lock
			return false, fmt.Errorf("release savepoint: %w", err)
		}
		return true, nil
	}
	// ROLLBACK TO SAVEPOINT releases a row lock the UPDATE may have waited
	// for before its re-check failed.
	if err := sp.Rollback(ctx); err != nil {
		return false, fmt.Errorf("rollback to savepoint: %w", err)
	}
	return false, nil
}

// snapshot returns the candidate nodes for req and the failure domains where
// req's tenant has instances that hold capacity.
func snapshot(ctx context.Context, tx pgx.Tx, req Request) ([]Node, map[string]bool, error) {
	rows, err := tx.Query(ctx, snapshotSQL, req.GPUType, req.GPUCount, req.TenantID, holdingStates())
	if err != nil {
		return nil, nil, fmt.Errorf("read node snapshot: %w", err)
	}
	defer rows.Close()

	var nodes []Node
	domains := map[string]bool{}
	for rows.Next() {
		var n Node
		var tenantHere bool
		if err := rows.Scan(&n.ID, &n.GPUType, &n.Capacity, &n.Allocated, &n.Schedulable,
			&n.FailureDomain, &tenantHere); err != nil {
			return nil, nil, fmt.Errorf("scan node snapshot: %w", err)
		}
		nodes = append(nodes, n)
		if tenantHere {
			domains[n.FailureDomain] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read node snapshot: %w", err)
	}
	return nodes, domains, nil
}

// holdingStates lists the states for which statemachine.HoldsCapacity is
// true, so the SQL filter and the state machine cannot disagree.
func holdingStates() []string {
	all := []statemachine.State{
		statemachine.Pending, statemachine.Scheduled, statemachine.Provisioning,
		statemachine.Running, statemachine.Terminating, statemachine.Terminated,
		statemachine.Failed,
	}
	var out []string
	for _, st := range all {
		if statemachine.HoldsCapacity(st) {
			out = append(out, string(st))
		}
	}
	return out
}
