package controller

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
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

	// Sizing (ADR-0002 M2-16). Zero means the Default* constant.
	InstanceWorkers int     // MaxConcurrentReconciles for GPUInstance
	NodeWorkers     int     // MaxConcurrentReconciles for Node (S2-18)
	DBMaxConns      int32   // pgxpool MaxConns for the controller's DB pool
	KubeQPS         float32 // client-go QPS to the API server
	KubeBurst       int     // client-go burst to the API server

	// Nodes is where the NodeReconciler writes inventory rows (S2-18). Nil
	// skips registering it (tests that run no Postgres).
	Nodes NodeSyncer
}

// withDefaults fills zero sizing fields with the Default* constants.
func (o Options) withDefaults() Options {
	if o.InstanceWorkers <= 0 {
		o.InstanceWorkers = DefaultInstanceWorkers
	}
	if o.NodeWorkers <= 0 {
		o.NodeWorkers = DefaultNodeWorkers
	}
	if o.DBMaxConns <= 0 {
		o.DBMaxConns = DefaultDBMaxConns
	}
	if o.KubeQPS <= 0 {
		o.KubeQPS = DefaultKubeQPS
	}
	if o.KubeBurst <= 0 {
		o.KubeBurst = DefaultKubeBurst
	}
	return o
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
	opts = opts.withDefaults()
	cfg = rest.CopyConfig(cfg)
	cfg.QPS = opts.KubeQPS
	cfg.Burst = opts.KubeBurst
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
	m, err := NewMetrics(crmetrics.Registry)
	if err != nil {
		return nil, err
	}
	if err := (&InstanceReconciler{Workers: opts.InstanceWorkers, Metrics: m}).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("setup instance reconciler: %w", err)
	}
	if opts.Nodes != nil {
		nr := &NodeReconciler{Reader: mgr.GetClient(), Nodes: opts.Nodes, Workers: opts.NodeWorkers}
		if err := nr.SetupWithManager(mgr); err != nil {
			return nil, fmt.Errorf("setup node reconciler: %w", err)
		}
	}
	return mgr, nil
}
