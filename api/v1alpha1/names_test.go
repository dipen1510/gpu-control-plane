package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"
)

func TestK8sName(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want string
	}{
		{name: "short id", id: "inst_ab", want: "inst-ab"},
		{name: "real-shaped id (128-bit lower base32)", id: "inst_mfrggzdfmztwq2lknnwg23tpobyhe5dv", want: "inst-mfrggzdfmztwq2lknnwg23tpobyhe5dv"},
		{name: "no underscore is unchanged", id: "inst-ab", want: "inst-ab"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := K8sName(tc.id)
			if got != tc.want {
				t.Fatalf("K8sName(%q) = %q, want %q", tc.id, got, tc.want)
			}
			// Both CR (DNS-1123 subdomain) and Pod names must be valid.
			if errs := validation.IsDNS1123Subdomain(got); len(errs) > 0 {
				t.Fatalf("K8sName(%q) = %q is not a DNS-1123 subdomain: %v", tc.id, got, errs)
			}
			// The original ID must fit in a label value (D37).
			if errs := validation.IsValidLabelValue(tc.id); len(errs) > 0 {
				t.Fatalf("id %q is not a valid label value: %v", tc.id, errs)
			}
		})
	}
}
