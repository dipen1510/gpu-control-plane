package api

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/lifecycle"
	"github.com/dipen1510/gpu-control-plane/internal/metrics"
	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// unreachableDSN points at 127.0.0.1:1, where nothing listens: connecting
// is refused at once. pgxpool.New does not connect, so no database is needed.
const unreachableDSN = "postgres://gpucp:secret@127.0.0.1:1/gpucp?sslmode=disable&connect_timeout=1"

// assertUnavailable checks a 503 unavailable with its Retry-After, an Error
// log entry, and no database detail in the body.
func assertUnavailable(t *testing.T, rec *httptest.ResponseRecorder, logs *bytes.Buffer) {
	t.Helper()
	assertError(t, rec, http.StatusServiceUnavailable, codeUnavailable)
	if ra := rec.Header().Get("Retry-After"); ra != retryAfterUnavailableSeconds {
		t.Errorf("Retry-After = %q, want %q", ra, retryAfterUnavailableSeconds)
	}
	for _, leak := range []string{"127.0.0.1", "gpucp", "closed pool", "connect"} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Errorf("body %s leaks %q", rec.Body.String(), leak)
		}
	}
	if !strings.Contains(logs.String(), `"level":"ERROR"`) || !strings.Contains(logs.String(), "database unreachable") {
		t.Errorf("not logged at ERROR as database unreachable: %s", logs.String())
	}
}

// TestDBUnreachableIs503: with the real store on a pool that cannot reach
// Postgres (closed, or nothing listening), create and DELETE answer
// 503 unavailable, not 500 (ADR-0001 §Failure modes), and so does GET. A
// create counts as result "error".
func TestDBUnreachableIs503(t *testing.T) {
	pools := []struct {
		name string
		pool func(t *testing.T) *pgxpool.Pool
	}{
		{name: "closed pool", pool: func(t *testing.T) *pgxpool.Pool {
			p, err := pgxpool.New(t.Context(), unreachableDSN)
			if err != nil {
				t.Fatalf("new pool: %v", err)
			}
			p.Close()
			return p
		}},
		{name: "nothing listening", pool: func(t *testing.T) *pgxpool.Pool {
			p, err := pgxpool.New(t.Context(), unreachableDSN)
			if err != nil {
				t.Fatalf("new pool: %v", err)
			}
			t.Cleanup(p.Close)
			return p
		}},
	}
	for _, pc := range pools {
		t.Run(pc.name, func(t *testing.T) {
			var logs bytes.Buffer
			log := slog.New(slog.NewJSONHandler(&logs, nil))
			st := store.New(pc.pool(t), scheduler.New(nil))
			m := metrics.New()
			h := NewHandler(st, lifecycle.New(st, log), log, WithMetrics(m))

			t.Run("create", func(t *testing.T) {
				logs.Reset()
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, newCreateRequest(strings.NewReader(`{"gpu_type":"H100","gpu_count":2}`)))
				assertUnavailable(t, rec, &logs)
				if got := series(t, m.Registry(), "gpucp_create_results_total")["result="+metrics.CreateError]; got != 1 {
					t.Errorf("create result error = %v, want 1", got)
				}
			})
			t.Run("delete", func(t *testing.T) {
				logs.Reset()
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, deleteRequest(t.Context(), nil))
				assertUnavailable(t, rec, &logs)
			})
			t.Run("get", func(t *testing.T) {
				logs.Reset()
				before := series(t, m.Registry(), "gpucp_create_results_total")
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, getRequest(t.Context()))
				assertUnavailable(t, rec, &logs)
				// GET has no create-result metric; it shows up only as a 503
				// on its route.
				if got := series(t, m.Registry(), "gpucp_api_requests_total")["code=503,route=GET /v1/instances/{id}"]; got != 1 {
					t.Errorf("GET 503 requests = %v, want 1", got)
				}
				if after := series(t, m.Registry(), "gpucp_create_results_total"); !maps.Equal(after, before) {
					t.Errorf("create results changed on GET: %v → %v", before, after)
				}
			})
		})
	}
}

func getRequest(ctx context.Context) *http.Request {
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/instances/inst_x", nil)
	req.Header.Set(headerTenant, "t1")
	return req
}

// TestGetUnavailable: a store error that means the database is unreachable
// is 503 unavailable on GET, like on create and DELETE; any other store
// error stays a 500 (TestStoreErrorsAreInternal).
func TestGetUnavailable(t *testing.T) {
	var logs bytes.Buffer
	h := NewHandler(&fakeStore{getErr: unavailableErr()}, &fakeLifecycle{}, slog.New(slog.NewJSONHandler(&logs, nil)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, getRequest(t.Context()))
	assertUnavailable(t, rec, &logs)
}

// blackHole returns the address of a TCP listener that accepts connections
// and never sends a byte, like a database behind a firewall that drops
// packets after the handshake: a client waits until its own timeout.
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

// TestBlackHoledDBIs503BeforeRequestDeadline: with a database that accepts
// TCP but never answers, and a DSN without connect_timeout, the pool built
// by store.ParsePoolConfig gives up after store.DefaultConnectTimeout, so
// create, GET and DELETE answer 503 unavailable long before the 10s request
// deadline (which would end as a 500).
func TestBlackHoledDBIs503BeforeRequestDeadline(t *testing.T) {
	const limit = 5 * time.Second
	if store.DefaultConnectTimeout >= limit || limit >= requestTimeout {
		t.Fatalf("test assumes DefaultConnectTimeout (%s) < %s < requestTimeout (%s)",
			store.DefaultConnectTimeout, limit, requestTimeout)
	}
	cfg, err := store.ParsePoolConfig("postgres://gpucp:secret@" + blackHole(t) + "/gpucp?sslmode=disable")
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(pool.Close)

	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	st := store.New(pool, scheduler.New(nil))
	h := NewHandler(st, lifecycle.New(st, log), log)

	for _, tc := range []struct {
		name string
		req  func(ctx context.Context) *http.Request
	}{
		{name: "get", req: getRequest},
		{name: "create", req: func(ctx context.Context) *http.Request {
			return newCreateRequest(strings.NewReader(`{"gpu_type":"H100","gpu_count":2}`)).WithContext(ctx)
		}},
		{name: "delete", req: func(ctx context.Context) *http.Request { return deleteRequest(ctx, nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs.Reset()
			rec := httptest.NewRecorder()
			start := time.Now()
			h.ServeHTTP(rec, tc.req(t.Context()))
			elapsed := time.Since(start)
			assertUnavailable(t, rec, &logs)
			if elapsed >= limit {
				t.Errorf("answered after %s, want < %s", elapsed, limit)
			}
		})
	}
}

// TestConnectTimeoutBelowLifecycleBudget: a lifecycle walk that has to open
// a connection must see the connect error (503 or a logged walk error), not
// its own deadline.
func TestConnectTimeoutBelowLifecycleBudget(t *testing.T) {
	if store.DefaultConnectTimeout >= lifecycleTimeout {
		t.Errorf("store.DefaultConnectTimeout = %s, want < lifecycleTimeout (%s)",
			store.DefaultConnectTimeout, lifecycleTimeout)
	}
}

// unavailableErr is a connection failure as the store wraps it.
func unavailableErr() error {
	return fmt.Errorf("transition instance inst_x: begin: %w",
		&pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"})
}

// TestDeleteUnavailableBeforeAndAfterTerminating: a walk that loses the
// database before anything is committed is 503 unavailable (retry is safe);
// once TERMINATING is committed (by this walk, or an earlier one being
// resumed) the delete was accepted, so it is 202 like any other release-step
// failure.
func TestDeleteUnavailableBeforeAndAfterTerminating(t *testing.T) {
	t.Run("first step", func(t *testing.T) {
		fs := &fakeStore{getInst: instanceAt(statemachine.Running, 3)}
		lc := &fakeLifecycle{deleteResults: []deleteResult{{err: unavailableErr()}}}
		rec, logs := serveDeleteLogged(context.Background(), fs, lc, nil)
		assertUnavailable(t, rec, logs)
	})
	t.Run("release step after TERMINATING committed", func(t *testing.T) {
		fs := &fakeStore{getInst: instanceAt(statemachine.Running, 3)}
		lc := &fakeLifecycle{deleteResults: []deleteResult{{inst: instanceAt(statemachine.Terminating, 4), err: unavailableErr()}}}
		rec := serveDelete(fs, lc, nil)
		assertAccepted(t, rec, statemachine.Terminating, 4)
	})
	t.Run("resuming TERMINATING", func(t *testing.T) {
		fs := &fakeStore{getInst: instanceAt(statemachine.Terminating, 4)}
		lc := &fakeLifecycle{deleteResults: []deleteResult{{err: unavailableErr()}}}
		rec := serveDelete(fs, lc, nil)
		assertAccepted(t, rec, statemachine.Terminating, 4)
	})
}
