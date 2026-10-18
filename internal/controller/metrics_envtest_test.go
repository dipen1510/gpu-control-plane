package controller

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
)

// TestControllerMetricsExposed starts the Manager against envtest and checks
// that /metrics serves the controller-runtime workqueue and reconcile metrics
// plus the gpucp_controller_* metrics this package owns.
func TestControllerMetricsExposed(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run `make test-envtest`")
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "deploy", "crds")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})

	addr := freeAddr(t)
	mgr, err := NewManager(cfg, Options{MetricsAddr: addr, ProbeAddr: "0", SkipNameValidation: true})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("manager stopped with error: %v", err)
		}
	})

	// workqueue_depth series appear on the first add, so enqueue one item.
	syncCtx, syncCancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer syncCancel()
	if !mgr.GetCache().WaitForCacheSync(syncCtx) {
		t.Fatal("cache did not sync")
	}
	id := "inst_metrics01"
	cr := &gpuv1alpha1.GPUInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gpuv1alpha1.K8sName(id),
			Namespace: "default",
			Labels:    map[string]string{gpuv1alpha1.LabelInstanceID: id},
		},
		Spec: gpuv1alpha1.GPUInstanceSpec{
			InstanceID: id, TenantID: "t1", GPUType: "H100", GPUCount: 1, NodeName: "gpu-cp-worker",
		},
	}
	if err := mgr.GetClient().Create(t.Context(), cr); err != nil {
		t.Fatalf("create GPUInstance: %v", err)
	}

	want := []string{
		`workqueue_depth{controller="gpuinstance"`,
		`controller_runtime_reconcile_total{controller="gpuinstance"`,
		`controller_runtime_max_concurrent_reconciles{controller="gpuinstance"} 8`,
		`gpucp_controller_orphans_total{kind="pod"} 0`,
		`gpucp_controller_orphans_total{kind="gpuinstance"} 0`,
	}
	var body string
	deadline := time.Now().Add(30 * time.Second)
	for {
		body = scrape(t, "http://"+addr+"/metrics")
		missing := ""
		for _, w := range want {
			if !strings.Contains(body, w) {
				missing = w
				break
			}
		}
		if missing == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("/metrics missing %q\n%s", missing, body)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The transitions CounterVec has no series until the first transition,
	// so check it is registered on the served registry instead.
	dup := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gpucp_controller_transitions_total",
		Help: "Instance state transitions committed by the controller.",
	}, []string{"from", "to", "reason"})
	var are prometheus.AlreadyRegisteredError
	if err := crmetrics.Registry.Register(dup); !errors.As(err, &are) {
		if err == nil {
			crmetrics.Registry.Unregister(dup)
		}
		t.Fatalf("gpucp_controller_transitions_total not registered (Register err = %v)", err)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}

// scrape returns the body of url, or "" if the server is not up yet.
func scrape(t *testing.T, url string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ""
	}
	defer func() { _ = resp.Body.Close() }() // read-only; close error is irrelevant
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return string(b)
}
