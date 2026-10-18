//go:build integration

package store

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestGetByK8sName resolves the controller's queue key (K8sName(id)) back to
// the row through the generated k8s_name column (D40).
func TestGetByK8sName(t *testing.T) {
	s, db := newStore(t)
	inst, _ := createInstance(t, s, db, "inst_k8sname01")

	got, err := s.GetByK8sName(t.Context(), "inst-k8sname01")
	if err != nil {
		t.Fatalf("GetByK8sName: %v", err)
	}
	if got.ID != inst.ID || got.State != inst.State || got.ResourceVersion != inst.ResourceVersion || got.NodeID != inst.NodeID {
		t.Errorf("got %+v, want %+v", got, inst)
	}
	// The DB id itself is not a k8s name; it must not match.
	if _, err := s.GetByK8sName(t.Context(), "inst_k8sname01"); !errors.Is(err, ErrNotFound) {
		t.Errorf("lookup by raw id: err = %v, want ErrNotFound", err)
	}
	if _, err := s.GetByK8sName(t.Context(), "inst-missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("lookup of missing name: err = %v, want ErrNotFound", err)
	}
}

// TestK8sNameCollisionRejected: two ids that map to the same Kubernetes name
// cannot both exist, so a k8s_name lookup can never match two rows (D40).
func TestK8sNameCollisionRejected(t *testing.T) {
	_, db := newStore(t)
	const ins = `INSERT INTO instances (id, tenant_id, gpu_type, gpu_count, state) VALUES ($1, 't1', 'H100', 1, 'PENDING')`
	if _, err := db.Exec(t.Context(), ins, "inst_ab"); err != nil {
		t.Fatalf("insert inst_ab: %v", err)
	}
	_, err := db.Exec(t.Context(), ins, "inst-ab")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "instances_k8s_name_key" {
		t.Fatalf("insert colliding inst-ab: err = %v, want unique violation on instances_k8s_name_key", err)
	}
	// The column is computed by Postgres; it cannot be written.
	if _, err := db.Exec(t.Context(), `UPDATE instances SET k8s_name = 'x' WHERE id = 'inst_ab'`); err == nil {
		t.Fatal("writing generated column k8s_name succeeded, want error")
	}
}
