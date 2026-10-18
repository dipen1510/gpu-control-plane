package controller

import (
	"time"

	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Workqueue and client sizing defaults (ADR-0002 M2-16, Discussion D34).
const (
	DefaultInstanceWorkers = 8
	DefaultNodeWorkers     = 2
	DefaultDBMaxConns      = 16
	DefaultKubeQPS         = 50
	DefaultKubeBurst       = 100

	// RetryBaseDelay and RetryMaxDelay bound the per-item exponential backoff.
	RetryBaseDelay = 5 * time.Millisecond
	RetryMaxDelay  = 5 * time.Minute
	// QueueQPS and QueueBurst size the token bucket shared by all items.
	QueueQPS   = 10
	QueueBurst = 100
)

// NewRateLimiter returns the workqueue rate limiter for every reconciler:
// the larger of a per-item exponential backoff (5ms doubling up to 5min) and
// an overall token bucket (10 qps, burst 100). The per-item part stops one
// failing key from hot-looping against Postgres or the API server; the bucket
// caps total retry pressure when many keys fail at once (e.g. a DB outage).
// Each controller needs its own instance (the limiter holds per-key state).
func NewRateLimiter() workqueue.TypedRateLimiter[reconcile.Request] {
	return workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](RetryBaseDelay, RetryMaxDelay),
		&workqueue.TypedBucketRateLimiter[reconcile.Request]{Limiter: rate.NewLimiter(rate.Limit(QueueQPS), QueueBurst)},
	)
}

// workersOrDefault returns n, or def when n is not positive.
func workersOrDefault(n, def int) int {
	if n <= 0 {
		return def
	}
	return n
}
