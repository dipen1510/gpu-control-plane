package v1alpha1

import "strings"

const (
	// LabelInstanceID holds the original instance ID on the CR and its Pod
	// ("_" is legal in label values, not in names). Lookups use this label.
	LabelInstanceID = "gpu.example.com/instance-id"

	// FinalizerReleaseCapacity keeps the CR until the DB row is terminal and
	// its capacity released (invariant 5, safe deletion).
	FinalizerReleaseCapacity = "gpu.example.com/release-capacity"
)

// K8sName returns the Kubernetes object name for an instance ID: "_" becomes
// "-" ("inst_ab" → "inst-ab"), because names must be DNS-1123 (D37). IDs have
// exactly one "_" and a lower-case base32 random part with no "-", so the
// mapping cannot collide. It is deliberately one-way: nothing parses a name
// back into an ID; use LabelInstanceID or spec.instanceID instead.
func K8sName(instanceID string) string {
	return strings.ReplaceAll(instanceID, "_", "-")
}
