//go:build integration

package lifecycle

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/seed"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

// accounting is everything a release changes.
type accounting struct {
	allocated int // sum over all nodes
	used      int // tenant t1, H100 (0 if no quota row yet)
}

func readAccounting(t *testing.T, db *pgxpool.Pool) accounting {
	t.Helper()
	var a accounting
	if err := db.QueryRow(t.Context(), `SELECT
		(SELECT COALESCE(sum(allocated), 0) FROM nodes),
		(SELECT COALESCE(sum(used), 0) FROM tenant_quota WHERE tenant_id = 't1' AND gpu_type = 'H100')`).
		Scan(&a.allocated, &a.used); err != nil {
		t.Fatalf("read accounting: %v", err)
	}
	return a
}

// setup returns a store over a fresh database with 4 seeded nodes.
func setup(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	db := testutil.NewDB(t)
	nodes, err := seed.Nodes(4)
	if err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	if err := seed.Upsert(t.Context(), db, nodes); err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	return store.New(db, scheduler.New(nil)), db
}

// create commits a 4-GPU H100 instance for t1, as the create handler does
// before it runs the driver. It is SCHEDULED at rv 1.
func create(t *testing.T, s *store.Store, id string) store.Instance {
	t.Helper()
	hash := sha256.Sum256([]byte(id))
	res, err := s.Create(t.Context(), store.CreateRequest{
		ID: id, TenantID: "t1", GPUType: "H100", GPUCount: 4,
		IdempotencyKey: "key-" + id, RequestHash: hash[:],
	})
	if err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	return res.Instance
}

func stored(t *testing.T, db *pgxpool.Pool, id string) (statemachine.State, int64) {
	t.Helper()
	var state string
	var rv int64
	if err := db.QueryRow(t.Context(), `SELECT state, resource_version FROM instances WHERE id = $1`, id).
		Scan(&state, &rv); err != nil {
		t.Fatalf("read instance %s: %v", id, err)
	}
	return statemachine.State(state), rv
}

func eventTypes(t *testing.T, db *pgxpool.Pool, id string) []string {
	t.Helper()
	rows, err := db.Query(t.Context(), `SELECT event_type FROM outbox WHERE aggregate_id = $1 ORDER BY id`, id)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	types, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	return types
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestDeleteEndsTerminatedAndReleases: from RUNNING, Delete ends at
// TERMINATED with the GPUs and quota back at their pre-create values, and a
// second Delete can't release them again.
func TestDeleteEndsTerminatedAndReleases(t *testing.T) {
	s, db := setup(t)
	d := New(s, discard())
	before := readAccounting(t, db)

	inst := create(t, s, "inst_del")
	running, err := d.AfterCreate(t.Context(), inst.ID, inst.ResourceVersion)
	if err != nil || running.State != statemachine.Running || running.ResourceVersion != 3 {
		t.Fatalf("AfterCreate = %s rv %d, %v; want RUNNING rv 3", running.State, running.ResourceVersion, err)
	}
	if held := readAccounting(t, db); held.allocated != before.allocated+4 || held.used != before.used+4 {
		t.Fatalf("while RUNNING accounting = %+v, want %+v plus 4 each", held, before)
	}

	got, err := d.Delete(t.Context(), inst.ID, statemachine.Running, running.ResourceVersion)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got.State != statemachine.Terminated || got.ResourceVersion != 5 {
		t.Errorf("Delete returned %s rv %d, want TERMINATED rv 5", got.State, got.ResourceVersion)
	}
	if state, rv := stored(t, db, inst.ID); state != statemachine.Terminated || rv != 5 {
		t.Errorf("stored %s rv %d, want TERMINATED rv 5", state, rv)
	}
	if after := readAccounting(t, db); after != before {
		t.Errorf("after delete accounting = %+v, want pre-create %+v", after, before)
	}

	// Released exactly once: deleting again (current or stale version)
	// changes nothing and writes no event.
	for _, rv := range []int64{5, running.ResourceVersion} {
		if _, err := d.Delete(t.Context(), inst.ID, statemachine.Running, rv); err == nil {
			t.Errorf("second Delete at rv %d succeeded, want an error", rv)
		}
	}
	if after := readAccounting(t, db); after != before {
		t.Errorf("after second delete accounting = %+v, want pre-create %+v", after, before)
	}
	want := []string{"instance.scheduled", "instance.provisioning", "instance.running",
		"instance.terminating", "instance.terminated"}
	if got := eventTypes(t, db, inst.ID); !slices.Equal(got, want) {
		t.Errorf("events = %v, want %v (one instance.terminated)", got, want)
	}
}

// raceStore runs beforeCall just before Transition call number at (1-based)
// reaches the real store, to make a concurrent writer win deterministically.
type raceStore struct {
	*store.Store
	at         int
	calls      int
	beforeCall func(ctx context.Context)
}

func (r *raceStore) Transition(ctx context.Context, id string, to statemachine.State, expectedRV int64, reason string) (store.Instance, error) {
	r.calls++
	if r.calls == r.at {
		r.beforeCall(ctx)
	}
	return r.Store.Transition(ctx, id, to, expectedRV, reason)
}

// TestDriverLosesToDelete: a DELETE commits between the create commit and a
// create-driver step. The create driver must stop on ErrStaleVersion without
// writing anything, and the instance ends TERMINATED with its capacity
// released once.
func TestDriverLosesToDelete(t *testing.T) {
	tests := []struct {
		name       string
		at         int                // create-driver step the delete wins before
		deleteFrom statemachine.State // state the delete sees
		deleteAtRV int64              // version the delete sees
		wantEvents []string
	}{
		{
			name: "before PROVISIONING", at: 1, deleteFrom: statemachine.Scheduled, deleteAtRV: 1,
			wantEvents: []string{"instance.scheduled", "instance.terminating", "instance.terminated"},
		},
		{
			name: "before RUNNING", at: 2, deleteFrom: statemachine.Provisioning, deleteAtRV: 2,
			wantEvents: []string{"instance.scheduled", "instance.provisioning",
				"instance.terminating", "instance.terminated"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, db := setup(t)
			before := readAccounting(t, db)
			inst := create(t, s, "inst_race")

			deleter := New(s, discard())
			var deleteErr error
			rs := &raceStore{Store: s, at: tc.at, beforeCall: func(ctx context.Context) {
				_, deleteErr = deleter.Delete(ctx, inst.ID, tc.deleteFrom, tc.deleteAtRV)
			}}
			_, err := New(rs, discard()).AfterCreate(t.Context(), inst.ID, inst.ResourceVersion)

			if deleteErr != nil {
				t.Fatalf("the winning Delete failed: %v", deleteErr)
			}
			if !errors.Is(err, store.ErrStaleVersion) {
				t.Errorf("AfterCreate err = %v, want ErrStaleVersion", err)
			}
			if rs.calls != tc.at {
				t.Errorf("AfterCreate made %d Transition calls, want %d (stop after losing)", rs.calls, tc.at)
			}
			if state, _ := stored(t, db, inst.ID); state != statemachine.Terminated {
				t.Errorf("stored state %s, want TERMINATED", state)
			}
			if after := readAccounting(t, db); after != before {
				t.Errorf("accounting = %+v, want pre-create %+v (released exactly once)", after, before)
			}
			if got := eventTypes(t, db, inst.ID); !slices.Equal(got, tc.wantEvents) {
				t.Errorf("events = %v, want %v", got, tc.wantEvents)
			}
		})
	}
}
