// Package lifecycle is the M1 instant lifecycle driver (ADR-0001 Amendment 1,
// Discussion D17). Right after the API commits a create or a delete, it walks
// the instance along the legal path of the state machine, one
// store.Transition per step:
//
//	AfterCreate: SCHEDULED → PROVISIONING → RUNNING
//	Delete:      <current> → TERMINATING → TERMINATED   (GPUs and quota released)
//
// Each step is its own transaction with one outbox event and a
// resource_version check, and uses the version returned by the previous step.
// The driver never retries and never waits: on any error it logs and stops,
// and the instance stays in its last committed state. It runs entirely
// inside the caller's call (no goroutines, sleeps or timers), so a GET right
// after the API responds sees the final state.
//
// M1 only: the M2 controller replaces it, and this package is deleted.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// Transitioner moves one instance one step. *store.Store satisfies it.
type Transitioner interface {
	Transition(ctx context.Context, id string, to statemachine.State, expectedRV int64, reason string) (store.Instance, error)
}

// Driver walks instances through their lifecycle. It holds no per-instance
// state and is safe for concurrent use; two drivers racing on one instance
// are kept apart by the store's resource_version check, not by anything here.
type Driver struct {
	store Transitioner
	log   *slog.Logger
}

// New returns a Driver that moves instances with s and logs to log.
func New(s Transitioner, log *slog.Logger) *Driver {
	return &Driver{store: s, log: log}
}

// Paths the driver walks. The state machine (S1-1) decides they are legal;
// the driver only picks them.
var (
	createPath = []statemachine.State{statemachine.Provisioning, statemachine.Running}
	deletePath = []statemachine.State{statemachine.Terminating, statemachine.Terminated}
)

// AfterCreate walks a just-created instance, which is SCHEDULED at version
// rv, to RUNNING. It returns the instance as its last successful step left
// it (zero if no step succeeded) and the error that stopped the walk, if
// any. The error is already logged; a caller that only needs the walk to be
// attempted can ignore it.
//
// ErrStaleVersion here usually means a DELETE moved the instance first. That
// is expected: the delete owns the instance from then on.
func (d *Driver) AfterCreate(ctx context.Context, id string, rv int64) (store.Instance, error) {
	return d.walk(ctx, "create", id, statemachine.Scheduled, rv, createPath)
}

// Delete walks an instance that is at version expectedRV to TERMINATED,
// through TERMINATING; the last step releases its GPUs and quota. It returns
// the instance as its last successful step left it (zero if no step
// succeeded) and the error that stopped the walk, if any, already logged.
// An error on the first step means nothing changed: the caller can map
// ErrStaleVersion and ErrInvalidTransition to a client error.
func (d *Driver) Delete(ctx context.Context, id string, expectedRV int64) (store.Instance, error) {
	return d.walk(ctx, "delete", id, "", expectedRV, deletePath)
}

// walk applies path one step at a time, chaining resource versions. from is
// the state the instance is known to be in at rv, or "" if the caller didn't
// say; it is only used for logging.
func (d *Driver) walk(ctx context.Context, op, id string, from statemachine.State, rv int64,
	path []statemachine.State) (store.Instance, error) {
	var last store.Instance
	for _, to := range path {
		next, err := d.store.Transition(ctx, id, to, rv, "")
		if err != nil {
			d.logStop(ctx, op, id, from, rv, to, err)
			return last, fmt.Errorf("lifecycle %s: instance %s to %s: %w", op, id, to, err)
		}
		last, from, rv = next, next.State, next.ResourceVersion
	}
	return last, nil
}

// logStop records why a walk stopped and the state the instance was left
// in. Losing a race (ErrStaleVersion) is normal, so it is logged at Info;
// anything else at Error.
func (d *Driver) logStop(ctx context.Context, op, id string, state statemachine.State, rv int64,
	to statemachine.State, err error) {
	left := string(state)
	if left == "" {
		left = "unchanged" // first delete step failed; the instance was not touched
	}
	level, msg := slog.LevelError, "lifecycle step failed; instance left in its last committed state"
	if errors.Is(err, store.ErrStaleVersion) {
		level, msg = slog.LevelInfo, "lifecycle step lost a race; instance left in its last committed state"
	}
	d.log.Log(ctx, level, msg, "op", op, "instance_id", id, "state", left,
		"resource_version", rv, "to", string(to), "err", err)
}
