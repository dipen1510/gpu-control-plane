package statemachine

import (
	"fmt"
	"slices"
	"testing"
)

// allStates is written out here, independently of the implementation, so a
// state accidentally dropped from the transition table still gets tested.
var allStates = []State{Pending, Scheduled, Provisioning, Running, Terminating, Terminated, Failed}

type edge struct{ from, to State }

// legalEdges is the CLAUDE.md diagram, one entry per arrow.
var legalEdges = map[edge]bool{
	{Pending, Scheduled}:        true,
	{Scheduled, Provisioning}:   true,
	{Provisioning, Running}:     true,
	{Running, Terminating}:      true,
	{Terminating, Terminated}:   true,
	{Pending, Failed}:           true,
	{Scheduled, Failed}:         true,
	{Provisioning, Failed}:      true,
	{Running, Failed}:           true, // D29, S2-9: Pod or node lost
	{Scheduled, Terminating}:    true,
	{Provisioning, Terminating}: true,
}

func TestCanTransition(t *testing.T) {
	type tc struct {
		name     string
		from, to State
		want     bool
	}
	var cases []tc
	for _, from := range allStates {
		for _, to := range allStates {
			cases = append(cases, tc{
				name: fmt.Sprintf("%s->%s", from, to),
				from: from,
				to:   to,
				want: legalEdges[edge{from, to}],
			})
		}
	}
	if len(cases) != 49 {
		t.Fatalf("expected 49 (from, to) pairs, got %d", len(cases))
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := CanTransition(c.from, c.to); got != c.want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", c.from, c.to, got, c.want)
			}
		})
	}
}

func TestNoBackwardEdges(t *testing.T) {
	// Position along the lifecycle; FAILED sits after everything it can be
	// reached from. Every legal edge must strictly increase the rank.
	rank := map[State]int{
		Pending: 0, Scheduled: 1, Provisioning: 2, Running: 3,
		Terminating: 4, Terminated: 5, Failed: 5,
	}
	for _, from := range allStates {
		for _, to := range allStates {
			if CanTransition(from, to) && rank[to] <= rank[from] {
				t.Errorf("backward or self edge allowed: %s -> %s", from, to)
			}
		}
	}
}

func TestTerminalStatesNeverChange(t *testing.T) {
	for _, from := range []State{Terminated, Failed} {
		for _, to := range allStates {
			t.Run(fmt.Sprintf("%s->%s", from, to), func(t *testing.T) {
				if CanTransition(from, to) {
					t.Errorf("terminal state %s must not transition to %s", from, to)
				}
			})
		}
	}
}

func TestUnknownStatesAreRejected(t *testing.T) {
	unknown := State("BOGUS")
	for _, s := range allStates {
		if CanTransition(unknown, s) {
			t.Errorf("CanTransition(%s, %s) = true, want false", unknown, s)
		}
		if CanTransition(s, unknown) {
			t.Errorf("CanTransition(%s, %s) = true, want false", s, unknown)
		}
	}
	if CanTransition("", Scheduled) {
		t.Error("CanTransition(\"\", SCHEDULED) = true, want false")
	}
	if IsTerminal(unknown) || HoldsCapacity(unknown) {
		t.Errorf("unknown state %s must be neither terminal nor holding capacity", unknown)
	}
}

func TestIsTerminal(t *testing.T) {
	want := map[State]bool{
		Pending: false, Scheduled: false, Provisioning: false, Running: false,
		Terminating: false, Terminated: true, Failed: true,
	}
	for _, s := range allStates {
		t.Run(string(s), func(t *testing.T) {
			if got := IsTerminal(s); got != want[s] {
				t.Errorf("IsTerminal(%s) = %v, want %v", s, got, want[s])
			}
		})
	}
}

func TestHoldsCapacity(t *testing.T) {
	// ADR-0001 §Data model: SCHEDULED, PROVISIONING, RUNNING, TERMINATING.
	want := map[State]bool{
		Pending: false, Scheduled: true, Provisioning: true, Running: true,
		Terminating: true, Terminated: false, Failed: false,
	}
	for _, s := range allStates {
		t.Run(string(s), func(t *testing.T) {
			if got := HoldsCapacity(s); got != want[s] {
				t.Errorf("HoldsCapacity(%s) = %v, want %v", s, got, want[s])
			}
		})
	}
}

// TestHoldingStates: the exported list that SQL filters on (placement's
// snapshot, the drift gauge) is exactly the states that hold capacity, in
// lifecycle order. It is compared with a list written out here and with
// HoldsCapacity over every state, so a state added to the package but
// missing from the list (or the reverse) fails.
func TestHoldingStates(t *testing.T) {
	want := []State{Scheduled, Provisioning, Running, Terminating}
	got := HoldingStates()
	if !slices.Equal(got, want) {
		t.Fatalf("HoldingStates() = %v, want %v", got, want)
	}
	for _, s := range allStates {
		if HoldsCapacity(s) != slices.Contains(got, s) {
			t.Errorf("%s: HoldsCapacity = %v, but in HoldingStates = %v", s, HoldsCapacity(s), slices.Contains(got, s))
		}
	}
	// Every state with outgoing edges is known to the list builder.
	for from := range transitions {
		if !slices.Contains(states, from) {
			t.Errorf("state %s is in the transition table but not in states", from)
		}
	}
	// Callers may keep or modify the result; that must not leak.
	got[0] = Failed
	if again := HoldingStates(); !slices.Equal(again, want) {
		t.Errorf("modifying a returned list changed the next result: %v", again)
	}
}

// TestTerminalAndHoldsCapacityAreConsistent ties the helpers together:
// a terminal state never holds capacity, so every transition into a terminal
// state from a holding state is exactly where the store must release.
func TestTerminalAndHoldsCapacityAreConsistent(t *testing.T) {
	for _, s := range allStates {
		if IsTerminal(s) && HoldsCapacity(s) {
			t.Errorf("%s is terminal but holds capacity", s)
		}
	}
}
