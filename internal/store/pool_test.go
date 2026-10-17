package store

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestParsePoolConfigConnectTimeout: a connect_timeout from the DSN or
// PGCONNECT_TIMEOUT is kept; without one (or with 0, "forever"), connects
// are bounded at DefaultConnectTimeout.
func TestParsePoolConfigConnectTimeout(t *testing.T) {
	const url = "postgres://u:p@127.0.0.1:5432/db?sslmode=disable"
	tests := []struct {
		name string
		dsn  string
		env  string // PGCONNECT_TIMEOUT
		want time.Duration
	}{
		{name: "url without connect_timeout", dsn: url, want: DefaultConnectTimeout},
		{name: "key=value without connect_timeout", dsn: "host=127.0.0.1 user=u dbname=db sslmode=disable", want: DefaultConnectTimeout},
		{name: "url connect_timeout kept", dsn: url + "&connect_timeout=7", want: 7 * time.Second},
		{name: "key=value connect_timeout kept", dsn: "host=127.0.0.1 user=u connect_timeout=5", want: 5 * time.Second},
		{name: "connect_timeout=1 kept even if shorter", dsn: url + "&connect_timeout=1", want: time.Second},
		{name: "PGCONNECT_TIMEOUT kept", dsn: url, env: "9", want: 9 * time.Second},
		{name: "connect_timeout=0 gets the default", dsn: url + "&connect_timeout=0", want: DefaultConnectTimeout},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PGCONNECT_TIMEOUT", tc.env) // "" = unset for pgconn
			cfg, err := ParsePoolConfig(tc.dsn)
			if err != nil {
				t.Fatalf("ParsePoolConfig: %v", err)
			}
			if got := cfg.ConnConfig.ConnectTimeout; got != tc.want {
				t.Errorf("ConnectTimeout = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestParsePoolConfigInvalid(t *testing.T) {
	_, err := ParsePoolConfig("postgres://u:hunter2@127.0.0.1:5432/db?connect_timeout=soon")
	if err == nil {
		t.Fatal("ParsePoolConfig accepted an invalid connect_timeout")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("error leaks the password: %v", err)
	}
}

// blackHole returns the address of a TCP listener that accepts connections
// and never sends a byte, like a database behind a firewall that drops
// packets after the handshake.
func blackHole(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close() // stops the accept loop; nothing else to report
		<-done
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close() // test teardown
		}
	})
	return ln.Addr().String()
}

// TestBlackHoleClassification: against a database that never answers, the
// pool's connect timeout ends in a *pgconn.ConnectError, which is
// "unavailable" even though a deadline sits in its chain; the request's own
// deadline running out first is still not.
func TestBlackHoleClassification(t *testing.T) {
	addr := blackHole(t)
	newPool := func(t *testing.T, connectTimeout time.Duration) *pgxpool.Pool {
		t.Helper()
		cfg, err := ParsePoolConfig("postgres://u:p@" + addr + "/db?sslmode=disable")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		cfg.ConnConfig.ConnectTimeout = connectTimeout
		pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
		if err != nil {
			t.Fatalf("new pool: %v", err)
		}
		t.Cleanup(pool.Close)
		return pool
	}

	t.Run("connect timeout first", func(t *testing.T) {
		pool := newPool(t, 200*time.Millisecond)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		start := time.Now()
		var n int
		err := pool.QueryRow(ctx, "SELECT 1").Scan(&n)
		if elapsed := time.Since(start); elapsed >= 5*time.Second {
			t.Errorf("failed after %s, want about the 200ms connect timeout", elapsed)
		}
		var ce *pgconn.ConnectError
		if !errors.As(err, &ce) {
			t.Fatalf("err = %T %v, want a *pgconn.ConnectError", err, err)
		}
		if !IsUnavailable(err) {
			t.Errorf("IsUnavailable(%v) = false, want true", err)
		}
	})
	t.Run("request deadline first", func(t *testing.T) {
		pool := newPool(t, 10*time.Second)
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		var n int
		err := pool.QueryRow(ctx, "SELECT 1").Scan(&n)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %T %v, want the request's context.DeadlineExceeded", err, err)
		}
		if IsUnavailable(err) {
			t.Errorf("IsUnavailable(%v) = true, want false (the request ran out of time)", err)
		}
	})
}
