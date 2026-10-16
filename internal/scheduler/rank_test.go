package scheduler

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// seeded returns a deterministic IntN so random tie-breaks are repeatable.
func seeded(seed uint64) func(int) int {
	return rand.New(rand.NewPCG(seed, seed)).IntN
}

func h100(id NodeID, free int) Node {
	return Node{ID: id, GPUType: "H100", Capacity: 8, Allocated: 8 - free, Schedulable: true, FailureDomain: "fd-a"}
}

// TestRankWorkedExample reproduces the D5 table: 10 H100 nodes, a 2-GPU
// request. n2 fits exactly and ranks first; n5 has too few free GPUs and n7
// is draining, so both are excluded.
func TestRankWorkedExample(t *testing.T) {
	nodes := []Node{
		h100("n1", 8), h100("n2", 2), h100("n3", 5), h100("n4", 8), h100("n5", 1),
		h100("n6", 4), h100("n7", 8), h100("n8", 6), h100("n9", 8), h100("n10", 7),
	}
	nodes[6].Schedulable = false // n7 draining
	req := Request{TenantID: "t1", GPUType: "H100", GPUCount: 2}

	for seed := range uint64(20) {
		got := NewRanker(seeded(seed)).Rank(nodes, req)

		if len(got) != 8 {
			t.Fatalf("seed %d: got %d candidates %v, want 8", seed, len(got), got)
		}
		wantHead := []NodeID{"n2", "n6", "n3", "n8", "n10"}
		if !slices.Equal(got[:5], wantHead) {
			t.Errorf("seed %d: head = %v, want %v", seed, got[:5], wantHead)
		}
		tail := slices.Clone(got[5:])
		slices.Sort(tail)
		if want := []NodeID{"n1", "n4", "n9"}; !slices.Equal(tail, want) {
			t.Errorf("seed %d: tied 6th place = %v, want %v in any order", seed, tail, want)
		}
		for _, excluded := range []NodeID{"n5", "n7"} {
			if slices.Contains(got, excluded) {
				t.Errorf("seed %d: %s should be excluded, got %v", seed, excluded, got)
			}
		}
	}
}

func TestRankFilters(t *testing.T) {
	req := Request{TenantID: "t1", GPUType: "H100", GPUCount: 4}
	tests := []struct {
		name string
		node Node
		want bool // included?
	}{
		{name: "fits", node: h100("n", 4), want: true},
		{name: "wrong gpu type", node: Node{ID: "n", GPUType: "A100", Capacity: 8, Schedulable: true}, want: false},
		{name: "not schedulable", node: Node{ID: "n", GPUType: "H100", Capacity: 8, Schedulable: false}, want: false},
		{name: "too few free", node: h100("n", 3), want: false},
		{name: "over-allocated snapshot", node: Node{ID: "n", GPUType: "H100", Capacity: 8, Allocated: 10, Schedulable: true}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NewRanker(seeded(1)).Rank([]Node{tc.node}, req)
			if included := len(got) == 1; included != tc.want {
				t.Errorf("included = %v, want %v (got %v)", included, tc.want, got)
			}
		})
	}
}

func TestRankSpreadsTenantAcrossFailureDomains(t *testing.T) {
	req := Request{
		TenantID: "t1", GPUType: "H100", GPUCount: 2,
		TenantFailureDomains: map[string]bool{"fd-a": true},
	}
	inA := h100("in-a", 4)
	inB := h100("in-b", 4)
	inB.FailureDomain = "fd-b"

	// Many seeds, so a pass is not luck of the random tie-break.
	for seed := range uint64(50) {
		got := NewRanker(seeded(seed)).Rank([]Node{inA, inB}, req)
		if want := []NodeID{"in-b", "in-a"}; !slices.Equal(got, want) {
			t.Fatalf("seed %d: got %v, want %v", seed, got, want)
		}
	}

	// Spreading is only a tie-break: a better fit in the tenant's domain wins.
	tighter := h100("tight-a", 2)
	got := NewRanker(seeded(1)).Rank([]Node{inB, tighter}, req)
	if want := []NodeID{"tight-a", "in-b"}; !slices.Equal(got, want) {
		t.Errorf("best fit vs spread: got %v, want %v", got, want)
	}
}

func TestRankPrefersHealthierOnTie(t *testing.T) {
	req := Request{TenantID: "t1", GPUType: "H100", GPUCount: 2,
		TenantFailureDomains: map[string]bool{"fd-a": true}}
	flaky := h100("flaky", 4)
	flaky.FailureDomain = "fd-b" // would win on spread alone
	flaky.RecentFailures = 3
	healthy := h100("healthy", 4)

	for seed := range uint64(50) {
		got := NewRanker(seeded(seed)).Rank([]Node{flaky, healthy}, req)
		if want := []NodeID{"healthy", "flaky"}; !slices.Equal(got, want) {
			t.Fatalf("seed %d: got %v, want %v", seed, got, want)
		}
	}
}

func TestRankRandomTieBreak(t *testing.T) {
	nodes := []Node{h100("a", 8), h100("b", 8), h100("c", 8)}
	req := Request{TenantID: "t1", GPUType: "H100", GPUCount: 1}

	// Same seed → same order (deterministic tests).
	first := NewRanker(seeded(7)).Rank(nodes, req)
	if again := NewRanker(seeded(7)).Rank(nodes, req); !slices.Equal(first, again) {
		t.Fatalf("same seed gave %v then %v", first, again)
	}

	// Across seeds every tied node comes first sometimes, so replicas spread
	// their reservation attempts instead of all hitting the same node.
	leaders := map[NodeID]bool{}
	for seed := range uint64(100) {
		leaders[NewRanker(seeded(seed)).Rank(nodes, req)[0]] = true
	}
	if len(leaders) != 3 {
		t.Errorf("tied nodes that ranked first: %v, want all 3", leaders)
	}
}

func TestRankPluggable(t *testing.T) {
	nodes := []Node{h100("full-ish", 2), h100("empty", 8)}
	req := Request{TenantID: "t1", GPUType: "H100", GPUCount: 2}

	// Swap best fit for spreading (emptiest first) and add a custom filter.
	r := &Ranker{
		Filters: append(DefaultFilters(), func(n Node, _ Request) bool { return n.ID != "banned" }),
		Scorers: []Scorer{func(n Node, req Request) int { return -BestFit(n, req) }},
		IntN:    seeded(1),
	}
	got := r.Rank(append(nodes, h100("banned", 8)), req)
	if want := []NodeID{"empty", "full-ish"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRankDoesNotModifyInputAndHandlesNoCandidates(t *testing.T) {
	nodes := []Node{h100("a", 1), h100("b", 8), h100("c", 4)}
	before := slices.Clone(nodes)

	got := NewRanker(nil).Rank(nodes, Request{GPUType: "H100", GPUCount: 8})
	if !slices.Equal(got, []NodeID{"b"}) {
		t.Errorf("got %v, want [b]", got)
	}
	if !slices.Equal(nodes, before) {
		t.Errorf("Rank modified its input: %v", nodes)
	}
	if got := NewRanker(nil).Rank(nodes, Request{GPUType: "A100", GPUCount: 1}); len(got) != 0 {
		t.Errorf("got %v, want no candidates", got)
	}
}

func TestFragmentedGPUs(t *testing.T) {
	draining := h100("d", 4)
	draining.Schedulable = false
	tests := []struct {
		name  string
		nodes []Node
		want  int
	}{
		{name: "no nodes", nodes: nil, want: 0},
		{name: "whole free node is not fragmented", nodes: []Node{h100("a", 8)}, want: 0},
		{name: "full node", nodes: []Node{h100("a", 0)}, want: 0},
		{name: "two half-free nodes: 8 free but no 8-GPU slot", nodes: []Node{h100("a", 4), h100("b", 4)}, want: 8},
		{name: "mixed", nodes: []Node{h100("a", 8), h100("b", 3), h100("c", 1)}, want: 4},
		{name: "draining node not counted", nodes: []Node{draining, h100("a", 2)}, want: 2},
		{name: "16-GPU node with 12 free leaves 4 stranded", nodes: []Node{{ID: "big", GPUType: "H100", Capacity: 16, Allocated: 4, Schedulable: true}}, want: 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := FragmentedGPUs(tc.nodes); got != tc.want {
				t.Errorf("FragmentedGPUs = %d, want %d", got, tc.want)
			}
		})
	}
}
