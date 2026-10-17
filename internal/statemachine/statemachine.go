// Package statemachine holds the pure rules of the GPU instance lifecycle:
// which states exist, which transitions are legal, which states are final,
// and which states hold GPU capacity.
//
// It is deliberately tiny and does no I/O. Keeping the rules in one place
// means the API server (M1) and the Kubernetes controller (M2) ask the same
// question and can never disagree about what a legal move is. Persistence,
// optimistic concurrency and outbox events live in the store, which calls
// CanTransition before it writes (ADR-0001 §Lifecycle transitions).
package statemachine

// State is the lifecycle state of a GPU instance. The string values are what
// the database stores in instances.state and what the API returns.
type State string

// The seven lifecycle states (CLAUDE.md §Instance state machine).
const (
	// Pending means accepted but not yet placed. Unused in v1, where create
	// places synchronously and returns at Scheduled; reserved for the v2 queue.
	Pending State = "PENDING"
	// Scheduled means GPUs on a node are reserved for the instance.
	Scheduled State = "SCHEDULED"
	// Provisioning means the workload is being started on its node.
	Provisioning State = "PROVISIONING"
	// Running means the instance is up and usable.
	Running State = "RUNNING"
	// Terminating means shutdown has started; GPUs are still held until it ends.
	Terminating State = "TERMINATING"
	// Terminated is the normal end state; capacity has been released.
	Terminated State = "TERMINATED"
	// Failed is the abnormal end state; capacity has been released.
	Failed State = "FAILED"
)

// transitions lists every legal edge. Anything not listed is illegal,
// including every backward move and every move out of a terminal state.
// An allow-list (rather than rules like "forward only") makes the diagram
// directly reviewable: each line here is one arrow in CLAUDE.md.
//
// It is never mutated after initialization; callers only read it through
// CanTransition, so concurrent use is safe.
var transitions = map[State][]State{
	Pending: {Scheduled, Failed},
	// Scheduled/Provisioning → Terminating: the user may delete an instance
	// before it ever reaches Running.
	Scheduled:    {Provisioning, Terminating, Failed},
	Provisioning: {Running, Terminating, Failed},
	// Running has no edge to Failed: once running, the instance ends through
	// Terminating so shutdown and release follow one path.
	Running:     {Terminating},
	Terminating: {Terminated},
	// Terminated and Failed have no outgoing edges: a terminal state never
	// changes, so a late or retried write can never revive an instance whose
	// capacity was already given back.
	Terminated: nil,
	Failed:     nil,
}

// CanTransition reports whether moving an instance from one state to another
// is legal. It returns false for self-transitions, backward moves, any move
// out of a terminal state, and unknown states. Because a repeated transition
// (e.g. RUNNING → RUNNING) is illegal, retries cannot apply a change twice.
func CanTransition(from, to State) bool {
	for _, next := range transitions[from] {
		if next == to {
			return true
		}
	}
	return false
}

// IsTerminal reports whether s is a final state (TERMINATED or FAILED).
// Terminal states never change and no longer hold capacity.
func IsTerminal(s State) bool {
	return s == Terminated || s == Failed
}

// HoldsCapacity reports whether an instance in state s has GPUs reserved on a
// node and counted against its tenant's quota. That is true from SCHEDULED
// (when placement reserves GPUs) through TERMINATING (shutdown not finished),
// and false for PENDING (not placed yet) and the terminal states. The store
// releases capacity exactly in the transition that moves an instance from a
// holding state to a terminal one (invariant 5, safe deletion).
func HoldsCapacity(s State) bool {
	switch s {
	case Scheduled, Provisioning, Running, Terminating:
		return true
	default:
		return false
	}
}

// states is every state, in lifecycle order. It is never mutated.
var states = []State{Pending, Scheduled, Provisioning, Running, Terminating, Terminated, Failed}

// HoldingStates returns the states for which HoldsCapacity is true, in
// lifecycle order. SQL that counts held GPUs (placement's snapshot, the
// allocation drift gauge) filters on this list, so the queries and the state
// machine cannot disagree. Each call returns a new slice the caller may keep.
func HoldingStates() []State {
	var out []State
	for _, s := range states {
		if HoldsCapacity(s) {
			out = append(out, s)
		}
	}
	return out
}
