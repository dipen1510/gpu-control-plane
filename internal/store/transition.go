package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
)

// Sentinel errors returned by Transition.
var (
	// ErrInvalidTransition means the caller is at the instance's current
	// version but the state machine does not allow moving from its current
	// state to the requested one (e.g. any move out of a terminal state).
	ErrInvalidTransition = errors.New("invalid state transition")
	// ErrStaleVersion means the instance changed since the caller read it:
	// its resource_version (or state) no longer matches. This is also what a
	// retry of a transition that already happened gets, and what every
	// loser of a race gets. Nothing was written; the caller should re-read
	// and decide again (invariant 4).
	ErrStaleVersion = errors.New("stale resource version")
)

// Transition moves instance id to state to, provided it is still at
// resource_version expectedRV (ADR-0001 §Lifecycle transitions). reason is
// stored as failure_reason when to is FAILED and ignored otherwise.
//
// On success the instance's resource_version goes up by exactly 1 and one
// outbox event instance.<state> is written in the same transaction
// (invariant 3). If the move takes the instance from a state that holds
// GPUs to a terminal state, the tenant's quota and the node's GPUs are
// released in that same transaction, so capacity is given back exactly once
// (invariant 5). It returns the instance as committed, so callers can chain
// the next step with the new resource_version.
//
// Errors, checked in this order: ErrNotFound (no such id), ErrStaleVersion
// (the instance is no longer at expectedRV: someone else, or an earlier
// attempt of this same call, changed it first), ErrInvalidTransition (the
// caller is current but the state machine forbids the move). The version is
// checked first so a caller that lost a race always learns "re-read", never
// a misleading "illegal move" computed from a state it never saw
// (ADR-0001 Amendment 1). In each case nothing is written.
func (s *Store) Transition(ctx context.Context, id string, to statemachine.State, expectedRV int64, reason string) (Instance, error) {
	// Step 1: read without a lock. This only decides what to attempt; the
	// conditional UPDATE in step 3 is what makes the write safe.
	cur, err := s.getByID(ctx, id)
	if err != nil {
		return Instance{}, fmt.Errorf("transition instance %s to %s: %w", id, to, err)
	}
	if cur.ResourceVersion != expectedRV {
		// Step 3 would match 0 rows, so don't take any locks. Step 3 still
		// re-checks, because the row can change after this read.
		return Instance{}, fmt.Errorf("transition instance %s to %s: at version %d, expected %d: %w",
			id, to, cur.ResourceVersion, expectedRV, ErrStaleVersion)
	}
	from := cur.State
	if !statemachine.CanTransition(from, to) {
		return Instance{}, fmt.Errorf("transition instance %s from %s to %s: %w", id, from, to, ErrInvalidTransition)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Instance{}, fmt.Errorf("transition instance %s: begin: %w", id, err)
	}
	// Rollback after a successful Commit returns ErrTxClosed; nothing to undo.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	// Release only when leaving a state that holds GPUs for a terminal one.
	// "to is terminal" alone would be wrong for PENDING → FAILED (v2), which
	// never reserved anything (S1-1 review).
	release := statemachine.HoldsCapacity(from) && statemachine.IsTerminal(to)

	// Step 2: lock the counters we may release, in the lock order every
	// transaction uses (tenant_quota → nodes → instances → outbox), so
	// Transition and Create can never deadlock. We lock first and decrement
	// only after step 3 proves we won: if we decremented first, a loser that
	// waited here for the winner to commit would compute used - n on an
	// already-released row and fail the used >= 0 CHECK instead of returning
	// ErrStaleVersion.
	//
	// The rows to lock come from the unlocked read. That is safe only because
	// tenant_id, gpu_type, gpu_count and node_id never change once an
	// instance holds capacity; step 3 re-reads them under the instance lock
	// and refuses to release if they differ from what was locked here.
	if release {
		if err := lockCapacity(ctx, tx, cur); err != nil {
			return Instance{}, fmt.Errorf("transition instance %s from %s to %s: %w", id, from, to, err)
		}
	}

	// Step 3: compare-and-set on (state, resource_version). failure_reason
	// is written only on → FAILED (NULL keeps the stored value).
	var failureReason *string
	if to == statemachine.Failed {
		failureReason = &reason
	}
	next, err := scanInstance(tx.QueryRow(ctx, transitionSQL, id, string(to), failureReason, string(from), expectedRV))
	if errors.Is(err, pgx.ErrNoRows) {
		// Someone else moved the instance first. Returning rolls back and
		// drops the step 2 locks; nothing was changed.
		return Instance{}, fmt.Errorf("transition instance %s from %s to %s at version %d: %w",
			id, from, to, expectedRV, ErrStaleVersion)
	}
	if err != nil {
		return Instance{}, fmt.Errorf("transition instance %s: update instance: %w", id, err)
	}

	// We won, so this transaction is the one release for this instance.
	// The rows are already locked, so no new lock is taken out of order.
	if release {
		if next.TenantID != cur.TenantID || next.GPUType != cur.GPUType ||
			next.GPUCount != cur.GPUCount || next.NodeID != cur.NodeID {
			return Instance{}, fmt.Errorf("transition instance %s: placement changed after read (%+v → %+v); refusing to release",
				id, cur, next)
		}
		if err := releaseCapacity(ctx, tx, next); err != nil {
			return Instance{}, fmt.Errorf("transition instance %s from %s to %s: %w", id, from, to, err)
		}
	}

	// Step 4: the event, in the same transaction as the change.
	if err := insertOutbox(ctx, tx, eventType(to), next); err != nil {
		return Instance{}, fmt.Errorf("transition instance %s: %w", id, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Instance{}, fmt.Errorf("transition instance %s: commit: %w", id, err)
	}
	if s.transitions != nil {
		s.transitions.Transitioned(from, to)
	}
	return next, nil
}

// eventType is the outbox event type for entering state s, e.g.
// "instance.terminated".
func eventType(s statemachine.State) string {
	return "instance." + strings.ToLower(string(s))
}

// getByID reads an instance by id alone. Transition is called by the system
// (driver, controller), not on behalf of a tenant; the API checks ownership
// with Get before it calls Transition.
func (s *Store) getByID(ctx context.Context, id string) (Instance, error) {
	inst, err := scanInstance(s.db.QueryRow(ctx, getInstanceByIDSQL, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Instance{}, ErrNotFound
	}
	if err != nil {
		return Instance{}, fmt.Errorf("read instance: %w", err)
	}
	return inst, nil
}

// scanInstance scans the column list instanceColumns.
func scanInstance(row pgx.Row) (Instance, error) {
	var inst Instance
	var state string
	if err := row.Scan(
		&inst.ID, &inst.TenantID, &inst.GPUType, &inst.GPUCount, &inst.NodeID, &state,
		&inst.ResourceVersion, &inst.FailureReason, &inst.CreatedAt, &inst.UpdatedAt); err != nil {
		return Instance{}, err
	}
	inst.State = statemachine.State(state)
	return inst, nil
}

// lockCapacity row-locks the quota row and node row that inst holds GPUs
// on, in that order. It changes nothing.
func lockCapacity(ctx context.Context, tx pgx.Tx, inst Instance) error {
	if inst.NodeID == "" {
		return fmt.Errorf("lock capacity: instance holds capacity but has no node")
	}
	var n int
	err := tx.QueryRow(ctx, lockQuotaSQL, inst.TenantID, inst.GPUType).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("lock quota: no quota row for tenant %s %s", inst.TenantID, inst.GPUType)
	}
	if err != nil {
		return fmt.Errorf("lock quota: %w", err)
	}
	err = tx.QueryRow(ctx, lockNodeSQL, inst.NodeID).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("lock node: node %s not found", inst.NodeID)
	}
	if err != nil {
		return fmt.Errorf("lock node: %w", err)
	}
	return nil
}

// releaseCapacity gives inst's GPUs back to its tenant's quota and its node.
// The caller has locked both rows with lockCapacity. The CHECK constraints
// (used >= 0, allocated >= 0) turn an accounting bug into an error here
// rather than a negative count.
func releaseCapacity(ctx context.Context, tx pgx.Tx, inst Instance) error {
	if _, err := tx.Exec(ctx, releaseQuotaSQL, inst.TenantID, inst.GPUType, inst.GPUCount); err != nil {
		return fmt.Errorf("release quota: %w", err)
	}
	if _, err := tx.Exec(ctx, releaseNodeSQL, inst.NodeID, inst.GPUCount); err != nil {
		return fmt.Errorf("release node: %w", err)
	}
	return nil
}

const instanceColumns = `id, tenant_id, gpu_type, gpu_count, COALESCE(node_id, ''), state,
       resource_version, failure_reason, created_at, updated_at`

const getInstanceByIDSQL = `SELECT ` + instanceColumns + ` FROM instances WHERE id = $1`

const lockQuotaSQL = `
SELECT used FROM tenant_quota WHERE tenant_id = $1 AND gpu_type = $2 FOR UPDATE`

const lockNodeSQL = `
SELECT allocated FROM nodes WHERE id = $1 FOR UPDATE`

const releaseQuotaSQL = `
UPDATE tenant_quota SET used = used - $3 WHERE tenant_id = $1 AND gpu_type = $2`

const releaseNodeSQL = `
UPDATE nodes SET allocated = allocated - $2 WHERE id = $1`

// transitionSQL is the compare-and-set: it matches only if the instance is
// still in the state we checked and at the version the caller saw. Under
// READ COMMITTED a concurrent writer of the same row makes this wait, then
// re-checks the WHERE against the committed row, so at most one of many
// identical transitions can match.
const transitionSQL = `
UPDATE instances
SET state = $2, resource_version = resource_version + 1,
    failure_reason = COALESCE($3, failure_reason), updated_at = now()
WHERE id = $1 AND state = $4 AND resource_version = $5
RETURNING ` + instanceColumns
