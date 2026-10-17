// Package v1alpha1 holds the GPUInstance custom resource (ADR-0002 §GPUInstance
// CRD). The CR mirrors a Postgres instance row for kubectl visibility, Pod
// ownership and the release-capacity finalizer (D19); Postgres stays the
// source of truth and the CR status is copied from it.
//
// +kubebuilder:object:generate=true
// +groupName=gpu.example.com
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is the API group and version of every type in this package.
var GroupVersion = schema.GroupVersion{Group: "gpu.example.com", Version: "v1alpha1"}

// AddToScheme registers GPUInstance and GPUInstanceList with s. It is a plain
// function (no init-time registration) per the go-conventions skill.
func AddToScheme(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &GPUInstance{}, &GPUInstanceList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
