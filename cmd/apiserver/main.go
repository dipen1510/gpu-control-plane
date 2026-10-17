// Command apiserver serves the GPU instance API (ADR-0001 §API (M1)).
// It applies pending migrations at startup (dev), then listens until SIGINT
// or SIGTERM, and drains in-flight requests before exiting.
//
// Configuration: DATABASE_URL (required); listen address from -addr, else
// the ADDR environment variable, else :8080. Besides /v1 it serves
// /metrics, /healthz (process up) and /readyz (database reachable).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/api"
	"github.com/dipen1510/gpu-control-plane/internal/lifecycle"
	"github.com/dipen1510/gpu-control-plane/internal/metrics"
	"github.com/dipen1510/gpu-control-plane/internal/migrate"
	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

const (
	startupTimeout  = 30 * time.Second
	shutdownTimeout = 15 * time.Second
)

func main() {
	defaultAddr := os.Getenv("ADDR")
	if defaultAddr == "" {
		defaultAddr = ":8080"
	}
	addr := flag.String("addr", defaultAddr, "listen address (env ADDR)")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log, *addr); err != nil {
		log.Error("apiserver failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, addr string) error {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return errors.New("DATABASE_URL is not set")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startCtx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()
	pool, err := pgxpool.New(startCtx, url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()
	if err := migrate.Up(startCtx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	// Metrics live on their own registry (internal/metrics). The database
	// gauges are read at scrape time and never write (D15).
	m := metrics.New()
	m.Registry().MustRegister(metrics.NewDBCollector(pool, log))
	sched := scheduler.New(m.ReserveConflicts, scheduler.WithPlaceDuration(m.PlaceDuration))
	st := store.New(pool, sched, store.WithTransitionObserver(m))
	// M1 only: walks each new instance to RUNNING and each deleted one to
	// TERMINATED (ADR-0001 Amendment 1); replaced by the controller in M2.
	driver := lifecycle.New(st, log)
	// Timeouts bound how long one connection can hold a goroutine, so a slow
	// or idle client can neither exhaust the server nor stall shutdown:
	//   ReadHeaderTimeout 5s: headers are a few hundred bytes.
	//   ReadTimeout 10s: headers plus the whole body (≤4 KB); a client
	//     trickling the body (slowloris) is cut off here.
	//   WriteTimeout 15s: longer than the handler's 10s database budget
	//     (api.requestTimeout) plus the 3s lifecycle budget of a create or
	//     delete (api.lifecycleTimeout), so a slow-but-successful request still
	//     gets its response.
	//   IdleTimeout 60s: keep-alive connections are reused for polling GETs
	//     but not held forever.
	// Shutdown closes idle connections at once, and an active one is bounded
	// by ReadTimeout/WriteTimeout (≤ the 15s shutdownTimeout), so a client
	// cannot keep Shutdown waiting past its deadline.
	srv := &http.Server{
		Addr:              addr,
		Handler:           api.NewHandler(st, driver, log, api.WithMetrics(m), api.WithReadiness(pool)),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// The serve goroutine is owned here: run waits for it on every path.
	serveErr := make(chan error, 1)
	go func() {
		log.Info("apiserver listening", "addr", addr)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}

	log.Info("shutting down", "timeout", shutdownTimeout.String())
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	// Shutdown stops accepting connections and waits for in-flight requests.
	shutdownErr := srv.Shutdown(shutdownCtx)
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	if shutdownErr != nil {
		return fmt.Errorf("shutdown: %w", shutdownErr)
	}
	log.Info("apiserver stopped")
	return nil
}
