// Command controller runs the GPUInstance controller Manager (ADR-0002).
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/dipen1510/gpu-control-plane/internal/controller"
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
	flag.Parse()

	ctrl.SetLogger(zap.New())

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load kubeconfig: %w", err)
	}
	mgr, err := controller.NewManager(cfg, opts)
	if err != nil {
		return err
	}
	slog.Info("starting controller manager", "metrics", opts.MetricsAddr, "probes", opts.ProbeAddr, "leader_elect", opts.LeaderElection)
	// SetupSignalHandler cancels on SIGTERM/SIGINT; Start drains the
	// controllers (GracefulShutdownTimeout, 30s default) before returning.
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		return fmt.Errorf("run manager: %w", err)
	}
	return nil
}
