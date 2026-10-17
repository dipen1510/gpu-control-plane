package api

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
// 503 unavailable, not 500 (ADR-0001 §Failure modes). A create counts as
// result "error".
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
		})
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
