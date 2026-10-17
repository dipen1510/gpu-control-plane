package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Instance states as stored in Postgres (CLAUDE.md §Instance state machine).
// The CR only mirrors them; the controller never decides a state from the CR.
const (
	StatePending      = "PENDING"
	StateScheduled    = "SCHEDULED"
	StateProvisioning = "PROVISIONING"
	StateRunning      = "RUNNING"
	StateTerminating  = "TERMINATING"
	StateTerminated   = "TERMINATED"
	StateFailed       = "FAILED"
)

// GPUInstanceSpec is copied once from the DB row when the CR is created and
// never changes in M2a. The API server rejects any spec update.
//
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
type GPUInstanceSpec struct {
	// InstanceID is the original DB ID (e.g. "inst_ab"). Lookups use this or
	// the instance-id label, never the object name (D37).
	// +kubebuilder:validation:MinLength=1
	InstanceID string `json:"instanceID"`

	// TenantID owns the instance.
	// +kubebuilder:validation:MinLength=1
	TenantID string `json:"tenantID"`

	// GPUType is the requested GPU model, e.g. H100.
	// +kubebuilder:validation:MinLength=1
	GPUType string `json:"gpuType"`

	// GPUCount is the number of GPUs held on NodeName.
	// +kubebuilder:validation:Minimum=1
	GPUCount int32 `json:"gpuCount"`

	// NodeName is the node the scheduler placed the instance on.
	// +kubebuilder:validation:MinLength=1
	NodeName string `json:"nodeName"`
}

// GPUInstanceStatus is copied from the DB row after each committed
// transition; on any disagreement the DB wins.
type GPUInstanceStatus struct {
	// State is the DB state.
	// +kubebuilder:validation:Enum=PENDING;SCHEDULED;PROVISIONING;RUNNING;TERMINATING;TERMINATED;FAILED
	// +optional
	State string `json:"state,omitempty"`

	// ResourceVersion is the DB row's resource_version (not the Kubernetes one).
	// +optional
	ResourceVersion int64 `json:"resourceVersion,omitempty"`

	// Reason explains a FAILED or TERMINATING state (e.g. boot_failed).
	// +optional
	Reason string `json:"reason,omitempty"`

	// PodName is the Pod running the instance.
	// +optional
	PodName string `json:"podName,omitempty"`

	// LastSyncTime is when the status was last copied from the DB.
	// +optional
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`
}

// GPUInstance mirrors one Postgres instance row (ADR-0002 §GPUInstance CRD).
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=gpui
// +kubebuilder:printcolumn:name="STATE",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="NODE",type=string,JSONPath=`.spec.nodeName`
// +kubebuilder:printcolumn:name="GPUS",type=integer,JSONPath=`.spec.gpuCount`
// +kubebuilder:printcolumn:name="AGE",type=date,JSONPath=`.metadata.creationTimestamp`
type GPUInstance struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GPUInstanceSpec   `json:"spec"`
	Status GPUInstanceStatus `json:"status,omitempty"`
}

// GPUInstanceList is a list of GPUInstance.
//
// +kubebuilder:object:root=true
type GPUInstanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GPUInstance `json:"items"`
}
