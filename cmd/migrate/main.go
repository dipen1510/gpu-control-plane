// Command migrate applies the embedded internal/migrate migrations to the
// database named by DATABASE_URL and exits. Running it again is a no-op, so
// it is safe as a Kubernetes Job that may be retried (D26).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dipen1510/gpu-control-plane/internal/migrate"
)

// timeout bounds the whole run so a Job against an unreachable database
// fails and is retried by Kubernetes instead of hanging forever.
const timeout = 2 * time.Minute

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Getenv("DATABASE_URL")); err != nil {
		slog.Error("migrate failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, url string) error {
	if url == "" {
		return fmt.Errorf("DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }() // nothing to undo on close error

	if err := migrate.Up(ctx, conn); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	slog.Info("migrations applied")
	return nil
}
