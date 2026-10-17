package store

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestIsUnavailable: connection failures are recognized by type and
// SQLSTATE, wherever they sit in the chain; errors that merely read like one,
// a request's own deadline, and Postgres rejecting the request are not.
// Needs no database: the connection errors come from 127.0.0.1:1, where
// nothing listens.
func TestIsUnavailable(t *testing.T) {
	const nowhere = "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1"

	_, connectErr := pgconn.Connect(t.Context(), nowhere)
	var ce *pgconn.ConnectError
	if !errors.As(connectErr, &ce) {
		t.Fatalf("sanity: connecting to %s gave %T %v, want *pgconn.ConnectError", nowhere, connectErr, connectErr)
	}

	closed, err := pgxpool.New(t.Context(), nowhere)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	closed.Close()
	var n int
	closedErr := closed.QueryRow(t.Context(), "SELECT 1").Scan(&n)
	_, closedBeginErr := closed.Begin(t.Context())

	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "connect error", err: connectErr, want: true},
		{name: "connect error wrapped", err: fmt.Errorf("create instance x: begin: %w", connectErr), want: true},
		{name: "closed pool QueryRow", err: closedErr, want: true},
		{name: "closed pool Begin wrapped", err: fmt.Errorf("get instance x: %w", closedBeginErr), want: true},
		{name: "socket error mid-request", err: fmt.Errorf("read: %w",
			&net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}), want: true},
		{name: "SQLSTATE 08006 connection failure", err: &pgconn.PgError{Code: "08006"}, want: true},
		{name: "SQLSTATE 57P01 admin shutdown", err: fmt.Errorf("x: %w", &pgconn.PgError{Code: "57P01"}), want: true},
		{name: "SQLSTATE 57P03 cannot connect now", err: &pgconn.PgError{Code: "57P03"}, want: true},

		{name: "nil", err: nil, want: false},
		{name: "deadlock", err: &pgconn.PgError{Code: "40P01"}, want: false},
		{name: "check violation", err: &pgconn.PgError{Code: "23514"}, want: false},
		{name: "query canceled 57014", err: &pgconn.PgError{Code: "57014"}, want: false},
		{name: "request deadline", err: fmt.Errorf("x: %w", context.DeadlineExceeded), want: false},
		{name: "request deadline from ctx", err: expired.Err(), want: false},
		{name: "canceled", err: context.Canceled, want: false},
		{name: "domain error", err: ErrNotFound, want: false},
		// Same text as the real thing, wrong type: no string matching.
		{name: "lookalike closed pool", err: errors.New("closed pool"), want: false},
		{name: "lookalike connect error", err: errors.New("failed to connect to `user=u database=db`: dial tcp 127.0.0.1:1: connect: connection refused"), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsUnavailable(tc.err); got != tc.want {
				t.Errorf("IsUnavailable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
