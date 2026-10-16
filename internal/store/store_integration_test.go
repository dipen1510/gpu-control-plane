//go:build integration

package store

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/seed"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

// totals is the accounting the create transaction touches.
type totals struct {
	instances, outbox, allocated, used int
}

func readTotals(t *testing.T, db *pgxpool.Pool) totals {
	t.Helper()
	var tot totals
	if err := db.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM instances),
		(SELECT count(*) FROM outbox),
		(SELECT COALESCE(sum(allocated), 0) FROM nodes),
		(SELECT COALESCE(sum(used), 0) FROM tenant_quota)`).
		Scan(&tot.instances, &tot.outbox, &tot.allocated, &tot.used); err != nil {
		t.Fatalf("read totals: %v", err)
	}
	return tot
}

func newStore(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	db := testutil.NewDB(t)
	nodes, err := seed.Nodes(4)
	if err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	if err := seed.Upsert(t.Context(), db, nodes); err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	return New(db, scheduler.New(nil)), db
}

func TestCreateRollsBackOnFailure(t *testing.T) {
	s, db := newStore(t)

	// An earlier successful create, so "unchanged" is checked against
	// non-zero allocated/used and an existing quota row.
	if _, err := s.Create(t.Context(), CreateRequest{
		ID: "inst_before", TenantID: "t1", GPUType: "H100", GPUCount: 2,
	}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	before := readTotals(t, db)

	injected := errors.New("injected failure after place")
	placed := false
	s.afterPlace = func(ctx context.Context, tx pgx.Tx) error {
		// Prove Place really reserved inside this transaction, so the
		// assertion below shows a rollback, not a reservation that never
		// happened.
		var alloc int
		if err := tx.QueryRow(ctx, `SELECT sum(allocated) FROM nodes`).Scan(&alloc); err != nil {
			t.Errorf("read allocated inside tx: %v", err)
		}
		placed = alloc == before.allocated+4
		return injected
	}

	_, err := s.Create(t.Context(), CreateRequest{
		ID: "inst_doomed", TenantID: "t1", GPUType: "H100", GPUCount: 4,
	})
	if !errors.Is(err, injected) {
		t.Fatalf("Create error = %v, want the injected error", err)
	}
	if !placed {
		t.Fatalf("Place did not reserve 4 GPUs inside the transaction before the injected failure")
	}

	if after := readTotals(t, db); after != before {
		t.Errorf("after failed create: %+v, want unchanged %+v", after, before)
	}
	if _, err := s.Get(t.Context(), "t1", "inst_doomed"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(inst_doomed) error = %v, want ErrNotFound", err)
	}
}

func TestGetIsScopedByTenant(t *testing.T) {
	s, _ := newStore(t)
	created, err := s.Create(t.Context(), CreateRequest{
		ID: "inst_a", TenantID: "t1", GPUType: "A100", GPUCount: 1,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := s.Get(t.Context(), "t1", "inst_a")
	if err != nil {
		t.Fatalf("Get by owner: %v", err)
	}
	if got.ID != created.ID || got.NodeID != created.NodeID || got.ResourceVersion != created.ResourceVersion {
		t.Errorf("Get = %+v, want %+v", got, created)
	}

	if _, err := s.Get(t.Context(), "t2", "inst_a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get by other tenant error = %v, want ErrNotFound", err)
	}
	if _, err := s.Get(t.Context(), "t1", "inst_missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get unknown id error = %v, want ErrNotFound", err)
	}
}
