package controller

import (
	"errors"
	"os/exec"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
)

func testInstance() *gpuv1alpha1.GPUInstance {
	return &gpuv1alpha1.GPUInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "inst-abc", Namespace: "gpu", UID: types.UID("uid-1")},
		Spec: gpuv1alpha1.GPUInstanceSpec{
			InstanceID: "inst_abc", TenantID: "t1", GPUType: "H100", GPUCount: 4, NodeName: "worker-2",
		},
	}
}

func envOf(c corev1.Container) map[string]string {
	m := map[string]string{}
	for _, e := range c.Env {
		m[e.Name] = e.Value
	}
	return m
}

func TestBuildPod(t *testing.T) {
	cfg := PodConfig{Image: "busybox:1.36", BootSecondsMin: 12, BootSecondsMax: 25, BootFailRate: 0.25, ShutdownSeconds: 7}
	pod := BuildPod(testInstance(), cfg)

	if len(pod.Spec.InitContainers) != 1 || len(pod.Spec.Containers) != 1 {
		t.Fatalf("containers: init=%d main=%d, want 1/1", len(pod.Spec.InitContainers), len(pod.Spec.Containers))
	}
	initC, mainC := pod.Spec.InitContainers[0], pod.Spec.Containers[0]
	req := mainC.Resources.Requests[GPUResourceName]
	lim := mainC.Resources.Limits[GPUResourceName]
	env := envOf(initC)

	ownerOK := func() bool {
		if len(pod.OwnerReferences) != 1 {
			return false
		}
		o := pod.OwnerReferences[0]
		return o.APIVersion == "gpu.example.com/v1alpha1" && o.Kind == "GPUInstance" &&
			o.Name == "inst-abc" && o.UID == "uid-1" && o.Controller != nil && *o.Controller
	}

	cases := []struct {
		name      string
		got, want any
	}{
		{"name is K8sName(id)", pod.Name, "inst-abc"},
		{"namespace follows CR", pod.Namespace, "gpu"},
		{"nodeName pinned", pod.Spec.NodeName, "worker-2"},
		{"label app", pod.Labels["app"], "gpu-instance"},
		{"label instance-id", pod.Labels[gpuv1alpha1.LabelInstanceID], "inst_abc"},
		{"ownerRef to CR", ownerOK(), true},
		{"restartPolicy Never", pod.Spec.RestartPolicy, corev1.RestartPolicyNever},
		{"gpu request", req.Value(), int64(4)},
		{"gpu limit", lim.Value(), int64(4)},
		{"init image", initC.Image, "busybox:1.36"},
		{"init BOOT_MIN", env["BOOT_MIN"], "12"},
		{"init BOOT_MAX", env["BOOT_MAX"], "25"},
		{"init BOOT_FAIL_RATE", env["BOOT_FAIL_RATE"], "0.25"},
		{"init runs boot script", strings.Join(initC.Command, " "), "sh -c " + bootScript},
		{"main sleeps forever", strings.Join(mainC.Command, " "), "sleep infinity"},
		{"preStop sleeps shutdown", strings.Join(mainC.Lifecycle.PreStop.Exec.Command, " "), "sleep 7"},
		{"grace > shutdown", *pod.Spec.TerminationGracePeriodSeconds > int64(cfg.ShutdownSeconds), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("got %v, want %v", tc.got, tc.want)
			}
		})
	}
}

func TestPodConfigValidate(t *testing.T) {
	ok := DefaultPodConfig()
	cases := []struct {
		name    string
		mutate  func(*PodConfig)
		wantErr bool
	}{
		{"default ok", func(*PodConfig) {}, false},
		{"empty image", func(c *PodConfig) { c.Image = "" }, true},
		{"min > max", func(c *PodConfig) { c.BootSecondsMin, c.BootSecondsMax = 30, 10 }, true},
		{"negative min", func(c *PodConfig) { c.BootSecondsMin = -1 }, true},
		{"fail rate > 1", func(c *PodConfig) { c.BootFailRate = 1.5 }, true},
		{"fail rate < 0", func(c *PodConfig) { c.BootFailRate = -0.1 }, true},
		{"negative shutdown", func(c *PodConfig) { c.ShutdownSeconds = -1 }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := ok
			tc.mutate(&c)
			err := c.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalidPodConfig) {
				t.Errorf("err %v is not ErrInvalidPodConfig", err)
			}
		})
	}
}

// TestBootScriptExitCode runs the init script with a local sh/awk and zero
// boot seconds, checking the fail-rate extremes produce the right exit code.
func TestBootScriptExitCode(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}
	if _, err := exec.LookPath("awk"); err != nil {
		t.Skip("awk not available")
	}
	cases := []struct {
		name     string
		rate     string
		wantCode int
	}{
		{"never fails", "0", 0},
		{"always fails", "1", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), "sh", "-c", bootScript)
			cmd.Env = []string{"PATH=/usr/bin:/bin", "BOOT_MIN=0", "BOOT_MAX=0", "BOOT_FAIL_RATE=" + tc.rate}
			out, err := cmd.CombinedOutput()
			code := 0
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				code = ee.ExitCode()
			} else if err != nil {
				t.Fatalf("run: %v (%s)", err, out)
			}
			if code != tc.wantCode {
				t.Errorf("exit %d, want %d; output %q", code, tc.wantCode, out)
			}
		})
	}
}

// TestBuildPodSecurityContext: both containers run hardened (S2-7): non-root
// UID 65534, no privilege escalation, every capability dropped, read-only
// root filesystem. Each container gets its own SecurityContext value.
func TestBuildPodSecurityContext(t *testing.T) {
	pod := BuildPod(testInstance(), DefaultPodConfig())
	containers := append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...)
	if len(containers) != 2 {
		t.Fatalf("containers = %d, want 2", len(containers))
	}
	for _, c := range containers {
		t.Run(c.Name, func(t *testing.T) {
			sc := c.SecurityContext
			if sc == nil {
				t.Fatal("SecurityContext is nil")
			}
			if sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot {
				t.Error("runAsNonRoot is not true")
			}
			if sc.RunAsUser == nil || *sc.RunAsUser != 65534 {
				t.Errorf("runAsUser = %v, want 65534", sc.RunAsUser)
			}
			if sc.RunAsGroup == nil || *sc.RunAsGroup != 65534 {
				t.Errorf("runAsGroup = %v, want 65534", sc.RunAsGroup)
			}
			if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
				t.Error("allowPrivilegeEscalation is not false")
			}
			if sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem {
				t.Error("readOnlyRootFilesystem is not true")
			}
			if sc.Capabilities == nil || len(sc.Capabilities.Drop) != 1 || sc.Capabilities.Drop[0] != "ALL" || len(sc.Capabilities.Add) != 0 {
				t.Errorf("capabilities = %+v, want drop [ALL], add none", sc.Capabilities)
			}
			if sc.Privileged != nil && *sc.Privileged {
				t.Error("privileged is true")
			}
		})
	}
	if psc := pod.Spec.SecurityContext; psc == nil || psc.SeccompProfile == nil || psc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("pod securityContext = %+v, want seccompProfile RuntimeDefault", psc)
	}
	if a := pod.Spec.AutomountServiceAccountToken; a == nil || *a {
		t.Errorf("automountServiceAccountToken = %v, want false", a)
	}
	if pod.Spec.InitContainers[0].SecurityContext == pod.Spec.Containers[0].SecurityContext {
		t.Error("containers share one SecurityContext pointer")
	}
}
