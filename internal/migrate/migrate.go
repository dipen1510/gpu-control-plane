// Package migrate applies the numbered, forward-only SQL migrations embedded
// in this package (ADR-0001 §Data model) and records each applied version in
// the schema_migrations table.
//
// Up is safe to run many times and from many processes at once (N apiserver
// replicas starting together): every migration runs in its own transaction
// that first takes a Postgres advisory lock, re-checks whether the version is
// already applied, and only then runs the SQL. A crash mid-migration rolls
// that migration back as a whole, so the next Up retries it cleanly.
package migrate

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// embedded holds the migration files. It is read-only, so sharing it is safe.
//
//go:embed migrations/*.sql
var embedded embed.FS

// lockKey is the pg_advisory_xact_lock key that serializes migrators. The
// value is arbitrary but fixed; it only has to be unique within this database.
const lockKey int64 = 0x67707563705f6d67 // "gpucp_mg"

// fileName matches "0001_create_nodes.sql": a positive version, then a name.
var fileName = regexp.MustCompile(`^([0-9]+)_([a-z0-9_]+)\.sql$`)

// DB is what Up needs: the ability to open a transaction. *pgxpool.Pool and
// *pgx.Conn both satisfy it.
type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Migration is one numbered SQL file.
type Migration struct {
	Version int64
	Name    string
	SQL     string
}

// Migrations returns the embedded migrations in ascending version order.
func Migrations() ([]Migration, error) {
	return load(embedded, "migrations")
}

// Checksum identifies the exact embedded migration set (versions, names and
// SQL). Test helpers use it to name a template database, so editing a
// migration during development never reuses a stale template.
func Checksum() (string, error) {
	migs, err := Migrations()
	if err != nil {
		return "", err
	}
	h := sha256.New()
	for _, m := range migs {
		// Length-prefix each field so different splits can't collide.
		_, _ = fmt.Fprintf(h, "%d:%d:%s:%d:%s;", m.Version, len(m.Name), m.Name, len(m.SQL), m.SQL) // hash.Hash writes never fail
	}
	return hex.EncodeToString(h.Sum(nil))[:16], nil
}

// Up applies every embedded migration that is not yet recorded in
// schema_migrations, in version order. Running it again is a no-op.
func Up(ctx context.Context, db DB) error {
	migs, err := Migrations()
	if err != nil {
		return err
	}
	return apply(ctx, db, migs)
}

func apply(ctx context.Context, db DB, migs []Migration) error {
	if err := inTx(ctx, db, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
			version    bigint      PRIMARY KEY,
			name       text        NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`)
		return err
	}); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	for _, m := range migs {
		if err := inTx(ctx, db, func(tx pgx.Tx) error {
			var applied bool
			if err := tx.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`,
				m.Version).Scan(&applied); err != nil {
				return fmt.Errorf("check version: %w", err)
			}
			if applied {
				return nil
			}
			// No arguments, so pgx uses the simple protocol, which allows
			// several statements in one file.
			if _, err := tx.Exec(ctx, m.SQL); err != nil {
				return err
			}
			_, err := tx.Exec(ctx,
				`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`,
				m.Version, m.Name)
			return err
		}); err != nil {
			return fmt.Errorf("apply migration %04d_%s: %w", m.Version, m.Name, err)
		}
	}
	return nil
}

// inTx runs fn in a transaction that holds the migration advisory lock. The
// lock is transaction-scoped, so Postgres releases it on COMMIT or ROLLBACK
// and a failed or cancelled migrator can never leave it held.
func inTx(ctx context.Context, db DB, fn func(pgx.Tx) error) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	// Rollback after a successful Commit returns ErrTxClosed; nothing to undo.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, lockKey); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// load reads and validates the *.sql files in dir. A file whose name does not
// match NNNN_name.sql, or two files with the same version, is an error rather
// than being silently skipped.
func load(fsys fs.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	var migs []Migration
	seen := make(map[int64]string)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		parts := fileName.FindStringSubmatch(e.Name())
		if parts == nil {
			return nil, fmt.Errorf("migration %q: name must look like 0001_name.sql", e.Name())
		}
		v, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("migration %q: version must be a positive integer", e.Name())
		}
		if prev, ok := seen[v]; ok {
			return nil, fmt.Errorf("migrations %q and %q share version %d", prev, e.Name(), v)
		}
		seen[v] = e.Name()
		body, err := fs.ReadFile(fsys, path.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", e.Name(), err)
		}
		migs = append(migs, Migration{Version: v, Name: parts[2], SQL: string(body)})
	}
	if len(migs) == 0 {
		return nil, errors.New("no migrations found")
	}
	sort.Slice(migs, func(i, j int) bool { return migs[i].Version < migs[j].Version })
	return migs, nil
}
