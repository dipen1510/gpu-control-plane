package metrics

import (
	"maps"
	"slices"
	"testing"

	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
)

// TestFragmentedGPUsPerType: fragmentation is computed per GPU type. A100
// and H100 free GPUs never combine into one request, so each type's figure
// counts only its own nodes (S1-4 review).
func TestFragmentedGPUsPerType(t *testing.T) {
	nodes := []scheduler.Node{
		// H100: 4 free (fragmented), 8 free (a whole node: not fragmented),
		// 3 free on a cordoned node (not usable, not counted).
		{ID: "h1", GPUType: "H100", Capacity: 8, Allocated: 4, Status: scheduler.NodeReady},
		{ID: "h2", GPUType: "H100", Capacity: 8, Allocated: 0, Status: scheduler.NodeReady},
		{ID: "h3", GPUType: "H100", Capacity: 8, Allocated: 5, Status: scheduler.NodeCordoned},
		// A100: 2 free and 6 free: 8 free in total, all fragmented.
		{ID: "a1", GPUType: "A100", Capacity: 8, Allocated: 6, Status: scheduler.NodeReady},
		{ID: "a2", GPUType: "A100", Capacity: 8, Allocated: 2, Status: scheduler.NodeReady},
	}
	got := summarize(nodes)
	want := map[string]typeSummary{
		"H100": {Capacity: 24, Allocated: 9, Fragmented: 4},
		"A100": {Capacity: 16, Allocated: 8, Fragmented: 8},
	}
	if !maps.Equal(got, want) {
		t.Errorf("summarize = %+v, want %+v", got, want)
	}
	// The fleet-wide number would be 12 and say nothing about which type
	// is fragmented.
	if all := scheduler.FragmentedGPUs(nodes); all != 12 {
		t.Fatalf("sanity: fleet-wide FragmentedGPUs = %d, want 12", all)
	}
}

func TestSummarizeEmpty(t *testing.T) {
	if got := summarize(nil); len(got) != 0 {
		t.Errorf("summarize(nil) = %v, want empty", got)
	}
}

// TestNewRegistersADRMetrics: the event metrics from ADR-0001 §Observability
// are on the registry from the start (the DB gauges come from DBCollector,
// tested with a database).
func TestNewRegistersADRMetrics(t *testing.T) {
	m := New()
	// Vector metrics appear in Gather only once a series exists.
	m.ObserveRequest("POST /v1/instances", 202, 0.01)
	m.Transitioned(statemachine.Scheduled, statemachine.Provisioning)
	m.PlaceDuration.Observe(0.001)
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var names []string
	for _, f := range families {
		names = append(names, f.GetName())
	}
	for _, want := range []string{
		"gpucp_api_requests_total",
		"gpucp_api_request_duration_seconds",
		"gpucp_create_results_total",
		"gpucp_scheduler_reserve_conflicts_total",
		"gpucp_scheduler_place_duration_seconds",
		"gpucp_instance_transitions_total",
	} {
		if !slices.Contains(names, want) {
			t.Errorf("registry lacks %s", want)
		}
	}
}

// TestRegistriesAreIndependent: two Metrics never share counts, so parallel
// tests cannot see each other's traffic.
func TestRegistriesAreIndependent(t *testing.T) {
	a, b := New(), New()
	a.CreateResult(CreateScheduled)
	fa, _ := a.Registry().Gather()
	fb, _ := b.Registry().Gather()
	if count(fa, "gpucp_create_results_total", CreateScheduled) != 1 ||
		count(fb, "gpucp_create_results_total", CreateScheduled) != 0 {
		t.Errorf("counts leaked between registries")
	}
}
