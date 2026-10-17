package controller

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
)

// LeaderElectionID is the Lease name used when leader election is enabled.
const LeaderElectionID = "gpucp-controller.gpu.example.com"

// Options configures NewManager.
type Options struct {
	MetricsAddr      string // e.g. ":8081"; "0" disables
	ProbeAddr        string // serves /healthz and /readyz; "0" disables
	LeaderElection   bool
	LeaderElectionNS string // namespace of the Lease; empty = in-cluster namespace
	// SkipNameValidation disables controller-runtime's process-wide unique
	// controller name check. Tests only (several Managers in one process);
	// cmd/controller leaves it false.
	SkipNameValidation bool
}

// NewScheme returns a scheme with the built-in types and GPUInstance.
func NewScheme() (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		return nil, fmt.Errorf("add client-go scheme: %w", err)
	}
	if err := gpuv1alpha1.AddToScheme(s); err != nil {
		return nil, fmt.Errorf("add gpu.example.com/v1alpha1 scheme: %w", err)
	}
	return s, nil
}

// NewManager builds the Manager (one shared cache and client per process),
// adds health checks and registers every reconciler.
func NewManager(cfg *rest.Config, opts Options) (ctrl.Manager, error) {
	s, err := NewScheme()
	if err != nil {
		return nil, err
	}
	skipNameValidation := opts.SkipNameValidation
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                  s,
		Metrics:                 metricsserver.Options{BindAddress: opts.MetricsAddr},
		HealthProbeBindAddress:  opts.ProbeAddr,
		LeaderElection:          opts.LeaderElection,
		LeaderElectionID:        LeaderElectionID,
		LeaderElectionNamespace: opts.LeaderElectionNS,
		Controller:              config.Controller{SkipNameValidation: &skipNameValidation},
	})
	if err != nil {
		return nil, fmt.Errorf("new manager: %w", err)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return nil, fmt.Errorf("add healthz: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return nil, fmt.Errorf("add readyz: %w", err)
	}
	if err := (&InstanceReconciler{}).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("setup instance reconciler: %w", err)
	}
	return mgr, nil
}
