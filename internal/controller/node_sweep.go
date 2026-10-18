package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// DefaultNodeSweepInterval is how often the node sweep runs after the first
// one (ADR-0002 §Node sync).
const DefaultNodeSweepInterval = 5 * time.Minute

// cacheSyncer is the part of the Manager's cache the sweep waits on.
type cacheSyncer interface {
	WaitForCacheSync(ctx context.Context) bool
}

// NodeSweeper marks NOT_READY every nodes row whose name is not a GPU Node
// in the informer cache. It catches deletes missed while the controller was
// down. It runs once after WaitForCacheSync and then every Interval; it never
// runs before the cache is synced, so an empty cache cannot mark the whole
// fleet NOT_READY. It is a Manager Runnable.
type NodeSweeper struct {
	Cache    cacheSyncer
	Reader   client.Reader // cache-backed
	Nodes    NodeSyncer
	Interval time.Duration // <= 0 means DefaultNodeSweepInterval
}

// NeedLeaderElection: only the leader writes node rows.
func (s *NodeSweeper) NeedLeaderElection() bool { return true }

// Start waits for the cache, then sweeps until ctx is done. A failed sweep
// is logged and retried at the next tick; it does not stop the Manager.
func (s *NodeSweeper) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("node-sweep")
	if !s.Cache.WaitForCacheSync(ctx) {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("node sweep: cache did not sync")
	}
	interval := s.Interval
	if interval <= 0 {
		interval = DefaultNodeSweepInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if marked, err := s.Sweep(ctx); err != nil {
			logger.Error(err, "node sweep failed")
		} else if len(marked) > 0 {
			logger.Info("nodes missing from the cluster marked NOT_READY", "nodes", marked)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Sweep runs one pass and returns the names it marked NOT_READY. Callers
// must have waited for cache sync.
func (s *NodeSweeper) Sweep(ctx context.Context) ([]string, error) {
	var list corev1.NodeList
	if err := s.Reader.List(ctx, &list, client.HasLabels{LabelGPUType}); err != nil {
		return nil, fmt.Errorf("list gpu nodes: %w", err)
	}
	present := make([]string, 0, len(list.Items))
	for _, n := range list.Items {
		present = append(present, n.Name)
	}
	return s.Nodes.SweepNodes(ctx, present)
}
