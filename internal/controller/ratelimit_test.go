package controller

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestRateLimiterBacksOffToCapAndForgetResets(t *testing.T) {
	rl := NewRateLimiter()
	key := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "inst-a"}}

	// The first calls also draw from the token bucket (burst 100), so delays
	// stay exponential: 5ms, 10ms, 20ms, ...
	var prev time.Duration
	for i := range 30 {
		d := rl.When(key)
		if d > RetryMaxDelay {
			t.Fatalf("failure %d: delay %v exceeds cap %v", i+1, d, RetryMaxDelay)
		}
		if i < 10 && d != RetryBaseDelay<<i {
			t.Fatalf("failure %d: delay %v, want %v", i+1, d, RetryBaseDelay<<i)
		}
		if d < prev {
			t.Fatalf("failure %d: delay %v shrank from %v", i+1, d, prev)
		}
		prev = d
	}
	if prev != RetryMaxDelay {
		t.Fatalf("delay after 30 failures = %v, want cap %v", prev, RetryMaxDelay)
	}
	if got := rl.NumRequeues(key); got != 30 {
		t.Fatalf("NumRequeues = %d, want 30", got)
	}

	// Success: controller-runtime calls Forget, which resets the backoff.
	rl.Forget(key)
	if got := rl.NumRequeues(key); got != 0 {
		t.Fatalf("NumRequeues after Forget = %d, want 0", got)
	}
	if d := rl.When(key); d > RetryBaseDelay {
		t.Fatalf("delay after Forget = %v, want <= %v", d, RetryBaseDelay)
	}

	// Another key is unaffected by the first key's failures.
	other := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "inst-b"}}
	if d := rl.When(other); d > RetryBaseDelay {
		t.Fatalf("delay for fresh key = %v, want <= %v", d, RetryBaseDelay)
	}
}

func TestOptionsWithDefaults(t *testing.T) {
	got := Options{}.withDefaults()
	if got.InstanceWorkers != 8 || got.NodeWorkers != 2 || got.DBMaxConns != 16 || got.KubeQPS != 50 || got.KubeBurst != 100 {
		t.Fatalf("defaults = %+v", got)
	}
	set := Options{InstanceWorkers: 3, NodeWorkers: 1, DBMaxConns: 4, KubeQPS: 5, KubeBurst: 6}.withDefaults()
	if set.InstanceWorkers != 3 || set.NodeWorkers != 1 || set.DBMaxConns != 4 || set.KubeQPS != 5 || set.KubeBurst != 6 {
		t.Fatalf("explicit values overridden: %+v", set)
	}
}
