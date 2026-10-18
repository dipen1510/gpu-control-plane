// Command controller runs the GPUInstance controller Manager (ADR-0002).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/dipen1510/gpu-control-plane/internal/controller"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("controller exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var opts controller.Options
	flag.StringVar(&opts.MetricsAddr, "metrics-bind-address", ":8081", "address for Prometheus /metrics (\"0\" disables)")
	flag.StringVar(&opts.ProbeAddr, "health-probe-bind-address", ":8082", "address for /healthz and /readyz")
	flag.BoolVar(&opts.LeaderElection, "leader-elect", false, "enable leader election (Lease); off in M2a (1 replica)")
	flag.StringVar(&opts.LeaderElectionNS, "leader-election-namespace", "", "namespace of the leader Lease (default: in-cluster namespace)")
	flag.IntVar(&opts.InstanceWorkers, "instance-workers", controller.DefaultInstanceWorkers, "concurrent GPUInstance reconciles")
	flag.IntVar(&opts.NodeWorkers, "node-workers", controller.DefaultNodeWorkers, "concurrent Node reconciles")
	var dbMaxConns int
	flag.IntVar(&dbMaxConns, "db-max-conns", controller.DefaultDBMaxConns, "max Postgres connections in the controller pool")
	var kubeQPS float64
	flag.Float64Var(&kubeQPS, "kube-api-qps", controller.DefaultKubeQPS, "client-go QPS to the Kubernetes API server")
	flag.IntVar(&opts.KubeBurst, "kube-api-burst", controller.DefaultKubeBurst, "client-go burst to the Kubernetes API server")
	flag.Parse()
	opts.DBMaxConns = int32(min(max(dbMaxConns, 0), math.MaxInt32))
	opts.KubeQPS = float32(kubeQPS)

	ctrl.SetLogger(zap.New())

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load kubeconfig: %w", err)
	}
	// Postgres for node sync (S2-18). Migrations are applied by the migrate
	// Job (D26), not here.
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return errors.New("DATABASE_URL is not set")
	}
	poolCfg, err := store.ParsePoolConfig(dbURL)
	if err != nil {
		return err
	}
	if opts.DBMaxConns <= 0 {
		opts.DBMaxConns = controller.DefaultDBMaxConns // 0 means default, as for every sizing flag
	}
	poolCfg.MaxConns = opts.DBMaxConns
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return fmt.Errorf("open database pool: %w", err)
	}
	defer pool.Close()
	opts.Nodes = store.New(pool, nil)

	mgr, err := controller.NewManager(cfg, opts)
	if err != nil {
		return err
	}
	slog.Info("starting controller manager", "metrics", opts.MetricsAddr, "probes", opts.ProbeAddr, "leader_elect", opts.LeaderElection,
		"instance_workers", opts.InstanceWorkers, "node_workers", opts.NodeWorkers, "kube_qps", opts.KubeQPS, "kube_burst", opts.KubeBurst)
	// SetupSignalHandler cancels on SIGTERM/SIGINT; Start drains the
	// controllers (GracefulShutdownTimeout, 30s default) before returning.
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		return fmt.Errorf("run manager: %w", err)
	}
	return nil
}
