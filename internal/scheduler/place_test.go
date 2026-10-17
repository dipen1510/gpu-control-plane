package scheduler

import "testing"

// Production must use the global, concurrency-safe rand (S1-4 review).
func TestNewUsesGlobalRand(t *testing.T) {
	if s := New(nil); s.ranker == nil || s.ranker.IntN != nil {
		t.Errorf("New: ranker %+v, want default ranker with nil IntN", s.ranker)
	}
}

// An invalid count is rejected before any SQL runs (tx is nil here).
func TestPlaceRejectsNonPositiveCount(t *testing.T) {
	for _, n := range []int{0, -2} {
		if _, err := New(nil).Place(t.Context(), nil, Request{GPUType: "H100", GPUCount: n}); err == nil {
			t.Errorf("GPUCount %d: got nil error", n)
		}
	}
}
