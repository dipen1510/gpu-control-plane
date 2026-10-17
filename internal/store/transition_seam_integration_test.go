//go:build integration

package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/metrics"
	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
)

// afterReadDB is the pool with one seam: the first QueryRow's Scan (in
// Transition, the unlocked read) runs afterRead once it has scanned, so the
// test can change the database between Transition's read and its
// transaction. Deterministic: no sleeps, no lock polling, nothing concurrent.
type afterReadDB struct {
	*pgxpool.Pool
	afterRead func()
	fired     bool
}

func (d *afterReadDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return afterReadRow{Row: d.Pool.QueryRow(ctx, sql, args...), db: d}
}

type afterReadRow struct {
	pgx.Row
	db *afterReadDB
}

func (r afterReadRow) Scan(dest ...any) error {
	err := r.Row.Scan(dest...)
	if !r.db.fired && r.db.afterRead != nil {
		r.db.fired = true
		r.db.afterRead()
	}
	return err
}

// transitionsCounted is the sum of every gpucp_instance_transitions_total
// series on m.
func transitionsCounted(t *testing.T, m *metrics.Metrics) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var sum float64
	for _, f := range families {
		if f.GetName() != "gpucp_instance_transitions_total" {
			continue
		}
		for _, s := range f.GetMetric() {
			sum += s.GetCounter().GetValue()
		}
	}
	return sum
}

// TestTransitionLostCASNotCounted: a Transition whose unlocked read saw the
// expected version, but whose compare-and-set then matched 0 rows because a
// competing Transition committed in between, returns ErrStaleVersion and adds
// 0 to gpucp_instance_transitions_total. The counter reports committed
// transitions only, so it must move after the CAS (and the commit), never
// before (S1-13 review follow-up).
func TestTransitionLostCASNotCounted(t *testing.T) {
	tests := []struct {
		name  string
		setup []statemachine.State
		to    statemachine.State
	}{
		{name: "SCHEDULED → PROVISIONING", to: statemachine.Provisioning},
		{name: "TERMINATING → TERMINATED (releases)",
			setup: []statemachine.State{statemachine.Provisioning, statemachine.Running, statemachine.Terminating},
			to:    statemachine.Terminated},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// rival has no observer: only the loser's counter is watched.
			rival, db := newStore(t)
			inst, _ := createInstance(t, rival, db, "inst_lost_cas")
			inst = walk(t, rival, inst, tc.setup...)

			seam := &afterReadDB{Pool: db}
			m := metrics.New()
			loser := New(seam, scheduler.New(nil), WithTransitionObserver(m))

			// The rival makes the same move after the loser's read, so the
			// loser's version check passes and its CAS finds the row moved.
			seam.afterRead = func() {
				if _, err := rival.Transition(t.Context(), inst.ID, tc.to, inst.ResourceVersion, ""); err != nil {
					t.Errorf("rival Transition → %s: %v", tc.to, err)
				}
			}
			_, err := loser.Transition(t.Context(), inst.ID, tc.to, inst.ResourceVersion, "")
			if !seam.fired {
				t.Fatalf("seam never fired: the loser did not read the instance")
			}
			if !errors.Is(err, ErrStaleVersion) {
				t.Fatalf("loser Transition → %s: error = %v, want ErrStaleVersion from the CAS", tc.to, err)
			}
			if got := transitionsCounted(t, m); got != 0 {
				t.Errorf("gpucp_instance_transitions_total after a lost CAS = %v, want 0", got)
			}
			wantEvent := eventType(tc.to)
			n := 0
			for _, et := range outboxTypes(t, db, inst.ID) {
				if et == wantEvent {
					n++
				}
			}
			if n != 1 {
				t.Errorf("%d %s outbox rows, want 1 (the rival's)", n, wantEvent)
			}

			// Control: a transition the same store commits is counted, so
			// the 0 above is not a broken observer.
			cur, err := loser.Get(t.Context(), "t1", inst.ID)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if next := nextState(cur.State); next != "" {
				if _, err := loser.Transition(t.Context(), inst.ID, next, cur.ResourceVersion, ""); err != nil {
					t.Fatalf("control Transition → %s: %v", next, err)
				}
				if got := transitionsCounted(t, m); got != 1 {
					t.Errorf("gpucp_instance_transitions_total after one commit = %v, want 1", got)
				}
			}
		})
	}
}

// nextState is a legal next state on the normal path, or "" from a terminal
// state.
func nextState(s statemachine.State) statemachine.State {
	switch s {
	case statemachine.Scheduled:
		return statemachine.Provisioning
	case statemachine.Provisioning:
		return statemachine.Running
	case statemachine.Running:
		return statemachine.Terminating
	case statemachine.Terminating:
		return statemachine.Terminated
	default:
		return ""
	}
}

// TestTransitionRefusesReleaseWhenPlacementChanged covers the guard in
// Transition's release path (S1-9 follow-up). Transition locks the quota and
// node rows named by its unlocked read, then CASes on (state,
// resource_version) only. If node_id or gpu_count changed in between without
// a version bump (impossible through the store; this is the bug the guard is
// for), the CAS still matches, and releasing the returned row would
// decrement a row that was never locked, by an amount that was never
// reserved. Transition must refuse: an error, no outbox row, no release, the
// instance as it was.
func TestTransitionRefusesReleaseWhenPlacementChanged(t *testing.T) {
	tests := []struct {
		name string
		// tamper changes the instance's placement, leaving state and
		// resource_version alone. otherNode is the other H100 node.
		tamper func(id, otherNode string) (sql string, args []any)
	}{
		// The other node holds 8 GPUs, so without the guard releasing 2 there
		// would succeed and silently corrupt it.
		{name: "node_id changed", tamper: func(id, otherNode string) (string, []any) {
			return `UPDATE instances SET node_id = $2 WHERE id = $1`, []any{id, otherNode}
		}},
		// Releasing 1 instead of 2 would also succeed without the guard.
		{name: "gpu_count changed", tamper: func(id, _ string) (string, []any) {
			return `UPDATE instances SET gpu_count = 1 WHERE id = $1`, []any{id}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, db := newStore(t)
			inst, _ := createInstance(t, s, db, "inst_moved")
			inst = walk(t, s, inst, statemachine.Provisioning, statemachine.Running, statemachine.Terminating)
			// Fill the other H100 node (best fit puts 8 GPUs on the empty one).
			res, err := s.Create(t.Context(), CreateRequest{
				ID: "inst_full", TenantID: "t2", GPUType: "H100", GPUCount: 8,
				IdempotencyKey: "key-full", RequestHash: hashOf("full"),
			})
			if err != nil {
				t.Fatalf("create filler: %v", err)
			}
			if res.NodeID == inst.NodeID {
				t.Fatalf("sanity: filler landed on %s with the instance", res.NodeID)
			}

			seam := &afterReadDB{Pool: db}
			m := metrics.New()
			guarded := New(seam, scheduler.New(nil), WithTransitionObserver(m))
			// After Transition's read: tamper, then snapshot what Transition
			// must leave exactly as it is.
			var tampered Instance
			var before totals
			seam.afterRead = func() {
				sql, args := tc.tamper(inst.ID, res.NodeID)
				tag, err := db.Exec(t.Context(), sql, args...)
				if err != nil || tag.RowsAffected() != 1 {
					t.Errorf("tamper: %v (%d rows)", err, tag.RowsAffected())
				}
				before = readTotals(t, db)
				if tampered, err = s.Get(t.Context(), "t1", inst.ID); err != nil {
					t.Errorf("Get after tamper: %v", err)
				}
			}

			_, err = guarded.Transition(t.Context(), inst.ID, statemachine.Terminated, inst.ResourceVersion, "")
			if !seam.fired {
				t.Fatalf("seam never fired")
			}
			if err == nil || !strings.Contains(err.Error(), "refusing to release") {
				t.Fatalf("Transition → TERMINATED after %s: error = %v, want the refusal", tc.name, err)
			}
			if errors.Is(err, ErrStaleVersion) || errors.Is(err, ErrInvalidTransition) {
				t.Errorf("refusal must not look like a lost race or illegal move: %v", err)
			}
			if after := readTotals(t, db); after != before {
				t.Errorf("totals changed (outbox/allocated/used): %+v, want %+v", after, before)
			}
			got, err := s.Get(t.Context(), "t1", inst.ID)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.State != statemachine.Terminating || got.ResourceVersion != inst.ResourceVersion ||
				got.NodeID != tampered.NodeID || got.GPUCount != tampered.GPUCount {
				t.Errorf("instance = %s rv %d node %s gpus %d, want unchanged: TERMINATING rv %d node %s gpus %d",
					got.State, got.ResourceVersion, got.NodeID, got.GPUCount,
					inst.ResourceVersion, tampered.NodeID, tampered.GPUCount)
			}
			if types := outboxTypes(t, db, inst.ID); types[len(types)-1] != "instance.terminating" {
				t.Errorf("outbox events = %v, want none after instance.terminating", types)
			}
			if got := transitionsCounted(t, m); got != 0 {
				t.Errorf("gpucp_instance_transitions_total = %v, want 0", got)
			}
		})
	}
}
