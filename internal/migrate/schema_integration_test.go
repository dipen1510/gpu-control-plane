//go:build integration

package migrate_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/migrate"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

const (
	sqlstateCheckViolation      = "23514"
	sqlstateForeignKeyViolation = "23503"
)

// requirePgError fails unless err is a Postgres error with the given SQLSTATE
// and, if constraint is non-empty, raised by that named constraint. Checking
// the code and name proves the CHECK we meant fired, not some other failure.
func requirePgError(t *testing.T, err error, code, constraint string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("want Postgres error %s (%s), got %v", code, constraint, err)
	}
	if pgErr.Code != code {
		t.Fatalf("SQLSTATE = %s, want %s (err: %v)", pgErr.Code, code, err)
	}
	if constraint != "" && pgErr.ConstraintName != constraint {
		t.Fatalf("constraint = %q, want %q", pgErr.ConstraintName, constraint)
	}
}

func insertNode(t *testing.T, db *pgxpool.Pool, id string, capacity int) {
	t.Helper()
	if _, err := db.Exec(t.Context(),
		`INSERT INTO nodes (id, gpu_type, capacity) VALUES ($1, 'H100', $2)`, id, capacity); err != nil {
		t.Fatalf("insert node: %v", err)
	}
}

func TestSchemaRejectsOverAllocation(t *testing.T) {
	db := testutil.NewDB(t)
	insertNode(t, db, "n1", 8)

	tests := []struct {
		name      string
		allocated int
		wantErr   bool
	}{
		{"full node is allowed", 8, false},
		{"above capacity", 9, true},
		{"negative", -1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec(t.Context(), `UPDATE nodes SET allocated = $1 WHERE id = 'n1'`, tc.allocated)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("allocated = %d: %v", tc.allocated, err)
				}
				return
			}
			requirePgError(t, err, sqlstateCheckViolation, "nodes_allocated_within_capacity")
		})
	}
	// Shrinking capacity below what is allocated is the same violation.
	_, err := db.Exec(t.Context(), `UPDATE nodes SET capacity = 4 WHERE id = 'n1'`)
	requirePgError(t, err, sqlstateCheckViolation, "nodes_allocated_within_capacity")
}

func TestSchemaRejectsOverQuota(t *testing.T) {
	db := testutil.NewDB(t)
	ctx := t.Context()
	if _, err := db.Exec(ctx, `INSERT INTO tenant_quota (tenant_id, gpu_type) VALUES ('t1', 'H100')`); err != nil {
		t.Fatalf("insert quota: %v", err)
	}
	var limit, used int
	if err := db.QueryRow(ctx, `SELECT quota_limit, used FROM tenant_quota WHERE tenant_id = 't1'`).
		Scan(&limit, &used); err != nil {
		t.Fatalf("read quota: %v", err)
	}
	if limit != 16 || used != 0 {
		t.Fatalf("defaults: quota_limit = %d, used = %d; want 16, 0", limit, used)
	}

	tests := []struct {
		name    string
		used    int
		wantErr bool
	}{
		{"at limit is allowed", 16, false},
		{"above limit", 17, true},
		{"negative", -1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec(t.Context(), `UPDATE tenant_quota SET used = $1 WHERE tenant_id = 't1'`, tc.used)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("used = %d: %v", tc.used, err)
				}
				return
			}
			requirePgError(t, err, sqlstateCheckViolation, "tenant_quota_used_within_limit")
		})
	}
}

func TestSchemaRejectsBadGPUCount(t *testing.T) {
	db := testutil.NewDB(t)
	insertNode(t, db, "n1", 8)

	tests := []struct {
		gpuCount int
		wantErr  bool
	}{
		{1, false}, {2, false}, {4, false}, {8, false},
		{0, true}, {3, true}, {16, true}, {-2, true},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("gpu_count=%d", tc.gpuCount), func(t *testing.T) {
			_, err := db.Exec(t.Context(),
				`INSERT INTO instances (id, tenant_id, gpu_type, gpu_count, node_id, state)
				 VALUES ($1, 't1', 'H100', $2, 'n1', 'SCHEDULED')`, fmt.Sprintf("inst_%d", i), tc.gpuCount)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("gpu_count = %d: %v", tc.gpuCount, err)
				}
				return
			}
			requirePgError(t, err, sqlstateCheckViolation, "instances_gpu_count_valid")
		})
	}
}

// TestSchemaStatesMatchStateMachine keeps the SQL CHECK and the Go constants
// in sync: every statemachine state is storable, anything else is not.
func TestSchemaStatesMatchStateMachine(t *testing.T) {
	db := testutil.NewDB(t)
	states := []statemachine.State{
		statemachine.Pending, statemachine.Scheduled, statemachine.Provisioning,
		statemachine.Running, statemachine.Terminating, statemachine.Terminated,
		statemachine.Failed,
	}
	for i, s := range states {
		if _, err := db.Exec(t.Context(),
			`INSERT INTO instances (id, tenant_id, gpu_type, gpu_count, state)
			 VALUES ($1, 't1', 'H100', 1, $2)`, fmt.Sprintf("inst_%d", i), string(s)); err != nil {
			t.Errorf("state %s rejected: %v", s, err)
		}
	}
	for _, bad := range []string{"BOGUS", "running", ""} {
		_, err := db.Exec(t.Context(),
			`INSERT INTO instances (id, tenant_id, gpu_type, gpu_count, state)
			 VALUES ('inst_bad', 't1', 'H100', 1, $1)`, bad)
		requirePgError(t, err, sqlstateCheckViolation, "instances_state_valid")
	}
}

// TestIdempotencyKeyFKIsDeferred: the create transaction inserts the key
// before the instance (ADR-0001 step 1 vs step 4), so the FK may only be
// checked at COMMIT, and must still be checked then.
func TestIdempotencyKeyFKIsDeferred(t *testing.T) {
	db := testutil.NewDB(t)
	ctx := t.Context()

	insertKey := `INSERT INTO idempotency_keys (tenant_id, key, request_hash, instance_id)
	              VALUES ('t1', $1, '\x00', $2)`
	insertInst := `INSERT INTO instances (id, tenant_id, gpu_type, gpu_count, state)
	               VALUES ($1, 't1', 'H100', 1, 'SCHEDULED')`

	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, insertKey, "k1", "inst_1"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, insertInst, "inst_1")
		return err
	})
	if err != nil {
		t.Fatalf("key before instance in one transaction: %v", err)
	}

	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, insertKey, "k2", "inst_missing")
		return err // the INSERT itself succeeds; COMMIT must fail
	})
	requirePgError(t, err, sqlstateForeignKeyViolation, "")
}

func tableNames(t *testing.T, db *pgxpool.Pool) map[string]bool {
	t.Helper()
	rows, err := db.Query(t.Context(),
		`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	out := make(map[string]bool, len(names))
	for _, n := range names {
		out[n] = true
	}
	return out
}

type appliedRow struct {
	Version int64
	Name    string
	At      string
}

func appliedVersions(t *testing.T, db *pgxpool.Pool) []appliedRow {
	t.Helper()
	rows, err := db.Query(t.Context(),
		`SELECT version, name, applied_at::text FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[appliedRow])
	if err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	return got
}

func TestMigrateTwiceIsNoop(t *testing.T) {
	db := testutil.NewEmptyDB(t)
	ctx := t.Context()
	migs, err := migrate.Migrations()
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}

	if err := migrate.Up(ctx, db); err != nil {
		t.Fatalf("first Up: %v", err)
	}
	first := appliedVersions(t, db)
	if len(first) != len(migs) {
		t.Fatalf("after first Up: %d versions recorded, want %d", len(first), len(migs))
	}
	for _, table := range []string{"nodes", "tenant_quota", "instances", "idempotency_keys", "outbox"} {
		if !tableNames(t, db)[table] {
			t.Errorf("table %s missing after Up", table)
		}
	}

	if err := migrate.Up(ctx, db); err != nil {
		t.Fatalf("second Up: %v", err)
	}
	second := appliedVersions(t, db)
	if len(second) != len(first) {
		t.Fatalf("second Up changed schema_migrations: %v -> %v", first, second)
	}
	for i := range first {
		if first[i] != second[i] {
			t.Errorf("row %d changed: %+v -> %+v", i, first[i], second[i])
		}
	}
}

// TestConcurrentMigrateAppliesOnce simulates several replicas starting at the
// same time against an empty database.
func TestConcurrentMigrateAppliesOnce(t *testing.T) {
	db := testutil.NewEmptyDB(t)
	migs, err := migrate.Migrations()
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}

	const replicas = 8
	start := make(chan struct{})
	errs := make([]error, replicas)
	var wg sync.WaitGroup
	for i := range replicas {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = migrate.Up(t.Context(), db)
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("replica %d: %v", i, err)
		}
	}
	if got := appliedVersions(t, db); len(got) != len(migs) {
		t.Fatalf("%d versions recorded, want %d", len(got), len(migs))
	}
}

func TestSchemaRejectsUnknownNodeStatus(t *testing.T) {
	db := testutil.NewDB(t)
	insertNode(t, db, "n1", 8)

	for _, s := range []string{"READY", "NOT_READY", "PROBATION", "CORDONED", "QUARANTINED"} {
		if _, err := db.Exec(t.Context(), `UPDATE nodes SET status = $1 WHERE id = 'n1'`, s); err != nil {
			t.Fatalf("status %s: %v", s, err)
		}
	}
	_, err := db.Exec(t.Context(), `UPDATE nodes SET status = 'DRAINING' WHERE id = 'n1'`)
	requirePgError(t, err, sqlstateCheckViolation, "nodes_status_valid")
}

// TestNodesStatusBackfill applies the migrations before 0006 in a scratch
// schema, inserts nodes with the old boolean true and false, then applies 0006 and
// checks the backfill (true → READY, false → CORDONED) and the dropped column.
func TestNodesStatusBackfill(t *testing.T) {
	db := testutil.NewDB(t)
	ctx := t.Context()
	migs, err := migrate.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // scratch schema is never committed

	if _, err := tx.Exec(ctx, `CREATE SCHEMA backfill; SET LOCAL search_path = backfill`); err != nil {
		t.Fatal(err)
	}
	run := func(m migrate.Migration) {
		t.Helper()
		if _, err := tx.Exec(ctx, m.SQL); err != nil {
			t.Fatalf("apply %d_%s: %v", m.Version, m.Name, err)
		}
	}
	var statusMig *migrate.Migration
	for i := range migs {
		if migs[i].Name == "nodes_status" {
			statusMig = &migs[i]
			break
		}
		run(migs[i])
	}
	if statusMig == nil {
		t.Fatal("nodes_status migration not found")
	}
	if _, err := tx.Exec(ctx, `INSERT INTO nodes (id, gpu_type, capacity, schedulable)
		VALUES ('on', 'H100', 8, true), ('off', 'H100', 8, false)`); err != nil {
		t.Fatal(err)
	}
	run(*statusMig)

	got := map[string]string{}
	rows, err := tx.Query(ctx, `SELECT id, status FROM nodes`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, s string
		if err := rows.Scan(&id, &s); err != nil {
			t.Fatal(err)
		}
		got[id] = s
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if got["on"] != "READY" || got["off"] != "CORDONED" {
		t.Fatalf("backfill = %v, want on=READY off=CORDONED", got)
	}
	var n int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns
		WHERE table_schema = 'backfill' AND table_name = 'nodes' AND column_name = 'schedulable'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("old boolean column still exists after the migration")
	}
}
