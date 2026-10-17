package controller

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
)

// TestEnvtestCRDInstallsAndCacheSeesInstance starts a real kube-apiserver +
// etcd (envtest), installs the generated CRD, starts the Manager, and checks
// that a created GPUInstance becomes visible through the Manager's cache
// after WaitForCacheSync. Needs KUBEBUILDER_ASSETS: run `make test-envtest`.
func TestEnvtestCRDInstallsAndCacheSeesInstance(t *testing.T) {
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
	if len(env.CRDs) != 1 || env.CRDs[0].Spec.Names.Kind != "GPUInstance" {
		t.Fatalf("installed CRDs = %d, want the GPUInstance CRD", len(env.CRDs))
	}

	mgr, err := NewManager(cfg, Options{MetricsAddr: "0", ProbeAddr: "0", SkipNameValidation: true})
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

	syncCtx, syncCancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer syncCancel()
	if !mgr.GetCache().WaitForCacheSync(syncCtx) {
		t.Fatal("cache did not sync")
	}

	id := "inst_envtest01"
	cr := &gpuv1alpha1.GPUInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gpuv1alpha1.K8sName(id),
			Namespace: "default",
			Labels:    map[string]string{gpuv1alpha1.LabelInstanceID: id},
		},
		Spec: gpuv1alpha1.GPUInstanceSpec{
			InstanceID: id, TenantID: "t1", GPUType: "H100", GPUCount: 2, NodeName: "gpu-cp-worker",
		},
	}
	// The Manager client writes to the API server and reads from the cache.
	if err := mgr.GetClient().Create(t.Context(), cr); err != nil {
		t.Fatalf("create GPUInstance: %v", err)
	}

	key := types.NamespacedName{Namespace: "default", Name: "inst-envtest01"}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var got gpuv1alpha1.GPUInstance
		err := mgr.GetCache().Get(t.Context(), key, &got)
		if err == nil {
			if got.Spec.InstanceID != id || got.Labels[gpuv1alpha1.LabelInstanceID] != id {
				t.Fatalf("cached CR = spec.instanceID %q label %q, want %q", got.Spec.InstanceID, got.Labels[gpuv1alpha1.LabelInstanceID], id)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("GPUInstance %s not visible in cache: %v", key, err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Spec is immutable (CEL rule on GPUInstanceSpec); the status subresource
	// stays writable. Use the API reader so we act on the stored object.
	c := mgr.GetClient()
	for _, tc := range []struct {
		name   string
		mutate func(*gpuv1alpha1.GPUInstanceSpec)
	}{
		{name: "gpuCount", mutate: func(s *gpuv1alpha1.GPUInstanceSpec) { s.GPUCount = 4 }},
		{name: "nodeName", mutate: func(s *gpuv1alpha1.GPUInstanceSpec) { s.NodeName = "gpu-cp-worker2" }},
	} {
		t.Run("spec update rejected: "+tc.name, func(t *testing.T) {
			var cur gpuv1alpha1.GPUInstance
			if err := mgr.GetAPIReader().Get(t.Context(), key, &cur); err != nil {
				t.Fatalf("get: %v", err)
			}
			tc.mutate(&cur.Spec)
			err := c.Update(t.Context(), &cur)
			if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec is immutable") {
				t.Fatalf("update spec.%s: err = %v, want Invalid \"spec is immutable\"", tc.name, err)
			}
		})
	}

	t.Run("status update allowed", func(t *testing.T) {
		var cur gpuv1alpha1.GPUInstance
		if err := mgr.GetAPIReader().Get(t.Context(), key, &cur); err != nil {
			t.Fatalf("get: %v", err)
		}
		cur.Status.State = gpuv1alpha1.StateProvisioning
		cur.Status.ResourceVersion = 2
		if err := c.Status().Update(t.Context(), &cur); err != nil {
			t.Fatalf("status update: %v", err)
		}
		var got gpuv1alpha1.GPUInstance
		if err := mgr.GetAPIReader().Get(t.Context(), key, &got); err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Status.State != gpuv1alpha1.StateProvisioning || got.Spec.GPUCount != 2 || got.Spec.NodeName != "gpu-cp-worker" {
			t.Fatalf("stored = state %q gpuCount %d node %q, want PROVISIONING/2/gpu-cp-worker", got.Status.State, got.Spec.GPUCount, got.Spec.NodeName)
		}
	})
}
