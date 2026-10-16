// Package scheduler decides which GPU node an instance goes on.
//
// It runs in two halves. Ranking (this file) is pure logic: given a snapshot
// of nodes and a request, it returns candidate nodes best-first. It touches
// no database, so it is easy to test. Placement (S1-5) then tries to reserve
// GPUs on each candidate in order with a conditional UPDATE; that UPDATE, not
// the ranking, is what prevents over-allocation (invariant 1), because the
// snapshot ranking is based on may already be stale.
//
// Ranking follows the kube-scheduler pattern (Discussion D5):
//
//  1. Filter: drop nodes that cannot take the request at all.
//  2. Score: best fit, i.e. the fewest free GPUs left after placement.
//  3. Tie-break: healthier node, then a failure domain where the tenant has
//     no instance yet, then random.
package scheduler

import (
	"math/rand/v2"
	"slices"
)

// WholeNodeGPUs is the size of the largest request (D1, D6): an 8-GPU
// instance needs one whole node.
const WholeNodeGPUs = 8

// NodeID identifies a node (the `nodes.id` column).
type NodeID string

// Node is one row of the scheduling snapshot. It mirrors the `nodes` table
// (ADR-0001) plus an optional health signal.
type Node struct {
	ID            NodeID
	GPUType       string
	Capacity      int
	Allocated     int
	Schedulable   bool // false when cordoned, draining or missing heartbeats
	FailureDomain string

	// RecentFailures is a simple health signal: lower is healthier. The M1
	// schema has no such column, so it is zero for every node and this
	// tie-break has no effect until something fills it in (e.g. counting
	// instances that recently went FAILED on the node).
	RecentFailures int
}

// Free returns the GPUs on the node that are not allocated. It never goes
// below zero, even if the snapshot is inconsistent.
func (n Node) Free() int {
	return max(n.Capacity-n.Allocated, 0)
}

// Request is what ranking needs to know about one create request.
type Request struct {
	TenantID string
	GPUType  string
	GPUCount int

	// TenantFailureDomains holds the failure domains where this tenant
	// already has an active instance. Placement fills it from the same
	// snapshot as the nodes. Nil means "none", so no domain is penalised.
	TenantFailureDomains map[string]bool
}

// Filter reports whether a node can take the request at all. A node is a
// candidate only if every filter returns true.
type Filter func(n Node, req Request) bool

// Scorer gives a node a score for the request; lower is better. Scorers are
// compared in order, so the first scorer decides and later ones only break
// ties left by earlier ones.
type Scorer func(n Node, req Request) int

// MatchesGPUType drops nodes with a different GPU type than requested.
func MatchesGPUType(n Node, req Request) bool { return n.GPUType == req.GPUType }

// IsSchedulable drops cordoned, draining or unhealthy nodes.
func IsSchedulable(n Node, _ Request) bool { return n.Schedulable }

// HasRoom drops nodes with fewer free GPUs than requested.
func HasRoom(n Node, req Request) bool { return n.Free() >= req.GPUCount }

// BestFit scores a node by the free GPUs left after placing the request.
// Preferring the fullest node that still fits (bin-packing, D7) keeps other
// nodes completely empty, so a later 8-GPU request still finds a whole node.
func BestFit(n Node, req Request) int { return n.Free() - req.GPUCount }

// Healthier prefers nodes with fewer recent failures.
func Healthier(n Node, _ Request) int { return n.RecentFailures }

// SpreadTenant prefers a node in a failure domain where the tenant has no
// instance yet, so one rack or zone outage does not take down all of a
// tenant's instances. It is only a tie-break: it never beats best fit.
func SpreadTenant(n Node, req Request) int {
	if req.TenantFailureDomains[n.FailureDomain] {
		return 1
	}
	return 0
}

// DefaultFilters are the D5 filters.
func DefaultFilters() []Filter {
	return []Filter{MatchesGPUType, IsSchedulable, HasRoom}
}

// DefaultScorers are the D5 score and tie-breaks, in priority order.
func DefaultScorers() []Scorer {
	return []Scorer{BestFit, Healthier, SpreadTenant}
}

// Ranker orders candidate nodes. Filters and scorers are plain functions so
// alternatives (spreading, rack preference, ...) can be swapped in without
// touching Rank. A Ranker is safe for concurrent use if IntN is.
type Ranker struct {
	Filters []Filter
	Scorers []Scorer

	// IntN returns a random int in [0, n). It breaks the final ties at
	// random, so that scheduler replicas looking at the same snapshot do not
	// all try to reserve on the same node and conflict. Tests inject a
	// seeded source to get a deterministic order. Nil uses math/rand/v2's
	// global IntN, which is safe for concurrent use.
	IntN func(n int) int
}

// NewRanker returns a Ranker with the default filters and scorers. intn may
// be nil (see Ranker.IntN).
func NewRanker(intn func(n int) int) *Ranker {
	return &Ranker{Filters: DefaultFilters(), Scorers: DefaultScorers(), IntN: intn}
}

// Rank returns the IDs of nodes that pass every filter, best first. An empty
// result means no node can take the request (the caller answers 503). Rank
// does not modify nodes and does no I/O.
func (r *Ranker) Rank(nodes []Node, req Request) []NodeID {
	type candidate struct {
		id     NodeID
		scores []int
	}

	var cands []candidate
	for _, n := range nodes {
		if !r.passes(n, req) {
			continue
		}
		scores := make([]int, len(r.Scorers))
		for i, s := range r.Scorers {
			scores[i] = s(n, req)
		}
		cands = append(cands, candidate{id: n.ID, scores: scores})
	}

	// Shuffle first, then sort stably: nodes with equal scores keep their
	// shuffled order, which is the random tie-break.
	intn := r.IntN
	if intn == nil {
		intn = rand.IntN
	}
	for i := len(cands) - 1; i > 0; i-- {
		j := intn(i + 1)
		cands[i], cands[j] = cands[j], cands[i]
	}
	slices.SortStableFunc(cands, func(a, b candidate) int {
		return slices.Compare(a.scores, b.scores)
	})

	ids := make([]NodeID, len(cands))
	for i, c := range cands {
		ids[i] = c.id
	}
	return ids
}

func (r *Ranker) passes(n Node, req Request) bool {
	for _, f := range r.Filters {
		if !f(n, req) {
			return false
		}
	}
	return true
}

// FragmentedGPUs returns the free GPUs that cannot serve an 8-GPU request
// (D7). Two nodes with 4 free GPUs each have 8 free in total but cannot run
// one 8-GPU instance, so all 8 count as fragmented. Only schedulable nodes
// count: free GPUs on a draining node are not usable capacity at all. It is
// the input for the fragmentation metric.
func FragmentedGPUs(nodes []Node) int {
	total := 0
	for _, n := range nodes {
		if !n.Schedulable {
			continue
		}
		total += n.Free() % WholeNodeGPUs
	}
	return total
}
