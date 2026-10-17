//go:build integration

package main

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/migrate"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

// connURL rebuilds a URL for the pool's own database, so run() is exercised
// through the same DATABASE_URL path the Job uses.
func connURL(pool *pgxpool.Pool) string {
	c := pool.Config().ConnConfig
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.User, c.Password),
		Host:     net.JoinHostPort(c.Host, strconv.Itoa(int(c.Port))),
		Path:     "/" + c.Database,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

type row struct {
	version int64
	applied string
}

func applied(t *testing.T, pool *pgxpool.Pool) []row {
	t.Helper()
	rows, err := pool.Query(t.Context(),
		`SELECT version, applied_at::text FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}
	defer rows.Close()
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.version, &r.applied); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return got
}

func TestMigrateCommandIdempotent(t *testing.T) {
	pool := testutil.NewEmptyDB(t)
	dsn := connURL(pool)
	migs, err := migrate.Migrations()
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}

	if err := run(t.Context(), dsn); err != nil {
		t.Fatalf("first run: %v", err)
	}
	first := applied(t, pool)
	if len(first) != len(migs) {
		t.Fatalf("after first run: %d versions, want %d", len(first), len(migs))
	}

	if err := run(t.Context(), dsn); err != nil {
		t.Fatalf("second run: %v", err)
	}
	second := applied(t, pool)
	if fmt.Sprint(first) != fmt.Sprint(second) {
		t.Fatalf("second run changed schema_migrations: %v -> %v", first, second)
	}
}

func TestMigrateCommandBadURL(t *testing.T) {
	if err := run(t.Context(), "postgres://nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=2"); err == nil {
		t.Fatal("run against unreachable DB: want error, got nil")
	}
}
