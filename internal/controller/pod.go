package controller

import (
	"errors"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
)

const (
	// GPUResourceName is the extended resource the fake GPU nodes advertise.
	GPUResourceName corev1.ResourceName = "gpu.example.com/gpu"

	// PodAppLabel / PodAppValue mark every fake GPU Pod.
	PodAppLabel = "app"
	PodAppValue = "gpu-instance"

	bootContainerName = "boot"
	mainContainerName = "gpu"

	// terminationGraceSlackSeconds keeps the grace period strictly above the
	// preStop sleep so the kubelet never kills the hook mid-sleep.
	terminationGraceSlackSeconds = 10
)

// PodConfig shapes the fake GPU Pod (D35). All delays happen inside the Pod;
// our Go code never sleeps.
type PodConfig struct {
	Image           string  // busybox image for both containers
	BootSecondsMin  int     // inclusive lower bound of the boot sleep
	BootSecondsMax  int     // inclusive upper bound of the boot sleep
	BootFailRate    float64 // probability in [0,1] the init container exits 1
	ShutdownSeconds int     // preStop sleep
}

// DefaultPodConfig matches ADR-0002: boot 10–30 s, no failures, 5 s shutdown.
func DefaultPodConfig() PodConfig {
	return PodConfig{
		Image:           "busybox:1.36",
		BootSecondsMin:  10,
		BootSecondsMax:  30,
		BootFailRate:    0,
		ShutdownSeconds: 5,
	}
}

// ErrInvalidPodConfig is returned by PodConfig.Validate.
var ErrInvalidPodConfig = errors.New("invalid pod config")

// Validate rejects configs BuildPod cannot turn into a sane Pod.
func (c PodConfig) Validate() error {
	switch {
	case c.Image == "":
		return fmt.Errorf("%w: image is empty", ErrInvalidPodConfig)
	case c.BootSecondsMin < 0 || c.BootSecondsMax < c.BootSecondsMin:
		return fmt.Errorf("%w: boot seconds [%d,%d]", ErrInvalidPodConfig, c.BootSecondsMin, c.BootSecondsMax)
	case c.BootFailRate < 0 || c.BootFailRate > 1:
		return fmt.Errorf("%w: boot fail rate %v not in [0,1]", ErrInvalidPodConfig, c.BootFailRate)
	case c.ShutdownSeconds < 0:
		return fmt.Errorf("%w: shutdown seconds %d", ErrInvalidPodConfig, c.ShutdownSeconds)
	}
	return nil
}

// bootScript sleeps a random number of seconds in [BOOT_MIN,BOOT_MAX] and then
// exits 1 with probability BOOT_FAIL_RATE. $RANDOM seeds awk so Pods started
// in the same second do not share a sequence.
const bootScript = `set -e
out=$(awk -v seed="$RANDOM" -v min="$BOOT_MIN" -v max="$BOOT_MAX" -v rate="$BOOT_FAIL_RATE" \
  'BEGIN { srand(seed); s = int(min + rand() * (max - min + 1)); if (s > max) s = max; f = (rand() < rate) ? 1 : 0; print s, f }')
set -- $out
echo "boot: sleeping $1s, fail=$2"
sleep "$1"
exit "$2"
`

// BuildPod returns the fake GPU Pod for inst. It is pure: no I/O, no clock,
// no randomness in Go (the randomness runs inside the init container).
func BuildPod(inst *gpuv1alpha1.GPUInstance, cfg PodConfig) *corev1.Pod {
	gpus := resource.MustParse(strconv.Itoa(int(inst.Spec.GPUCount)))
	res := corev1.ResourceList{GPUResourceName: gpus}
	grace := int64(cfg.ShutdownSeconds + terminationGraceSlackSeconds)
	controller := true
	blockOwnerDeletion := true

	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      gpuv1alpha1.K8sName(inst.Spec.InstanceID),
			Namespace: inst.Namespace,
			Labels: map[string]string{
				PodAppLabel:                 PodAppValue,
				gpuv1alpha1.LabelInstanceID: inst.Spec.InstanceID,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         gpuv1alpha1.GroupVersion.String(),
				Kind:               "GPUInstance",
				Name:               inst.Name,
				UID:                inst.UID,
				Controller:         &controller,
				BlockOwnerDeletion: &blockOwnerDeletion,
			}},
		},
		Spec: corev1.PodSpec{
			NodeName:                      inst.Spec.NodeName,
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: &grace,
			InitContainers: []corev1.Container{{
				Name:    bootContainerName,
				Image:   cfg.Image,
				Command: []string{"sh", "-c", bootScript},
				Env: []corev1.EnvVar{
					{Name: "BOOT_MIN", Value: strconv.Itoa(cfg.BootSecondsMin)},
					{Name: "BOOT_MAX", Value: strconv.Itoa(cfg.BootSecondsMax)},
					{Name: "BOOT_FAIL_RATE", Value: strconv.FormatFloat(cfg.BootFailRate, 'f', -1, 64)},
				},
			}},
			Containers: []corev1.Container{{
				Name:      mainContainerName,
				Image:     cfg.Image,
				Command:   []string{"sleep", "infinity"},
				Resources: corev1.ResourceRequirements{Requests: res, Limits: res.DeepCopy()},
				Lifecycle: &corev1.Lifecycle{
					PreStop: &corev1.LifecycleHandler{
						Exec: &corev1.ExecAction{
							Command: []string{"sleep", strconv.Itoa(cfg.ShutdownSeconds)},
						},
					},
				},
			}},
		},
	}
}
