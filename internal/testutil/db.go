// Package testutil holds helpers for integration tests (build tag
// `integration`). It is not imported by production code.
//
// Each test gets its own Postgres database, so tests can run in parallel and
// never see each other's rows (ADR-0001 M1-d). Creating a database by
// copying a template is much faster than running the migrations every time:
// the migrated template is built once per migration set and reused, even
// across `go test` processes and runs.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/migrate"
)

// templateLockKey serializes template builds across test processes; `go test
// ./...` runs packages in parallel, each in its own process.
const templateLockKey int64 = 0x67707563705f7464 // "gpucp_td"

const setupTimeout = 60 * time.Second

// NewDB returns a pool connected to a fresh database with every migration
// applied. The database is dropped when the test ends. The test is skipped if
// DATABASE_URL is unset.
func NewDB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	cfg := baseConfig(t)
	ctx, cancel := context.WithTimeout(t.Context(), setupTimeout)
	defer cancel()

	tmpl, err := ensureTemplate(ctx, cfg)
	if err != nil {
		t.Fatalf("testutil: prepare template database: %v", err)
	}
	return newDBFrom(t, cfg, tmpl)
}

// NewEmptyDB returns a pool connected to a fresh database with no
// migrations applied, for testing the migration runner itself. It is dropped
// when the test ends and skipped if DATABASE_URL is unset.
func NewEmptyDB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	return newDBFrom(t, baseConfig(t), "template0")
}

func baseConfig(t testing.TB) *pgx.ConnConfig {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL is not set: run `make db-up` and export " +
			"DATABASE_URL=postgres://gpucp:gpucp@127.0.0.1:5432/gpucp?sslmode=disable")
	}
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatalf("testutil: parse DATABASE_URL: %v", err)
	}
	return cfg
}

// newDBFrom creates a uniquely named database copied from template, registers
// its cleanup, and returns a pool connected to it.
func newDBFrom(t testing.TB, base *pgx.ConnConfig, template string) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), setupTimeout)
	defer cancel()

	name := "gpucp_test_" + randomHex(t, 8)
	if err := withAdmin(ctx, base, func(admin *pgx.Conn) error {
		_, err := admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s",
			pgx.Identifier{name}.Sanitize(), pgx.Identifier{template}.Sanitize()))
		return err
	}); err != nil {
		t.Fatalf("testutil: create database %s: %v", name, err)
	}

	// Registered before the pool's Close so it runs after it (Cleanup is LIFO).
	// t.Context() is already cancelled when cleanups run, hence Background.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
		defer cancel()
		if err := withAdmin(ctx, base, func(admin *pgx.Conn) error {
			_, err := admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)",
				pgx.Identifier{name}.Sanitize()))
			return err
		}); err != nil {
			t.Errorf("testutil: drop database %s: %v", name, err)
		}
	})

	pcfg, err := pgxpool.ParseConfig(base.ConnString())
	if err != nil {
		t.Fatalf("testutil: parse pool config: %v", err)
	}
	pcfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		t.Fatalf("testutil: connect to %s: %v", name, err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("testutil: ping %s: %v", name, err)
	}
	return pool
}

// ensureTemplate returns the name of a migrated template database for the
// current migration set, building it if needed. The build happens under a
// session advisory lock, under a temporary name that is renamed only after
// every migration succeeded, so a crashed or failed build never leaves a
// half-migrated template behind for later tests to copy.
func ensureTemplate(ctx context.Context, base *pgx.ConnConfig) (string, error) {
	sum, err := migrate.Checksum()
	if err != nil {
		return "", err
	}
	tmpl := "gpucp_tmpl_" + sum
	building := tmpl + "_building"

	err = withAdmin(ctx, base, func(admin *pgx.Conn) error {
		// Session lock; released explicitly below, and by Postgres anyway when
		// withAdmin closes the connection, even on error.
		if _, err := admin.Exec(ctx, "SELECT pg_advisory_lock($1)", templateLockKey); err != nil {
			return fmt.Errorf("lock: %w", err)
		}
		defer func() {
			_, _ = admin.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", templateLockKey)
		}() // best effort: closing the session releases it too

		var exists bool
		if err := admin.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", tmpl).Scan(&exists); err != nil {
			return fmt.Errorf("look up template: %w", err)
		}
		if exists {
			return nil
		}

		ident := pgx.Identifier{building}.Sanitize()
		if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+ident+" WITH (FORCE)"); err != nil {
			return fmt.Errorf("drop leftover %s: %w", building, err)
		}
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+ident); err != nil {
			return fmt.Errorf("create %s: %w", building, err)
		}
		if err := migrateDB(ctx, base, building); err != nil {
			return err
		}
		// RENAME and later CREATE DATABASE ... TEMPLATE both require that no
		// session is connected to it; migrateDB has closed its connection.
		if _, err := admin.Exec(ctx, fmt.Sprintf("ALTER DATABASE %s RENAME TO %s",
			ident, pgx.Identifier{tmpl}.Sanitize())); err != nil {
			return fmt.Errorf("rename %s: %w", building, err)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return tmpl, nil
}

func migrateDB(ctx context.Context, base *pgx.ConnConfig, dbName string) error {
	cfg := base.Copy()
	cfg.Database = dbName
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", dbName, err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }() // close errors leave nothing to undo
	if err := migrate.Up(ctx, conn); err != nil {
		return fmt.Errorf("migrate %s: %w", dbName, err)
	}
	return nil
}

// withAdmin runs fn on a short-lived connection to the DATABASE_URL database.
func withAdmin(ctx context.Context, base *pgx.ConnConfig, fn func(*pgx.Conn) error) error {
	conn, err := pgx.ConnectConfig(ctx, base)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }() // close errors leave nothing to undo
	return fn(conn)
}

func randomHex(t testing.TB, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("testutil: random name: %v", err)
	}
	return hex.EncodeToString(b)
}
