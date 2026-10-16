// Command seed-nodes upserts N fake GPU nodes into the database named by
// DATABASE_URL, applying migrations first. Usage: seed-nodes -n 10
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dipen1510/gpu-control-plane/internal/seed"
)

func main() {
	n := flag.Int("n", 10, "number of fake nodes to upsert")
	flag.Parse()
	if err := run(*n); err != nil {
		slog.Error("seed nodes failed", "err", err)
		os.Exit(1)
	}
}

func run(n int) error {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return fmt.Errorf("DATABASE_URL is not set")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }() // nothing to undo on close error

	if err := seed.Run(ctx, conn, n); err != nil {
		return err
	}
	slog.Info("seeded nodes", "count", n)
	return nil
}
