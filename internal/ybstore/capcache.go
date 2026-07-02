package ybstore

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/fivetime/sbw-contract/model"
)

// CapacityCache holds a periodically-refreshed snapshot of per-edge used capacity
// (ybstore.UsedByEdge), so pool placement (scheduler.SelectHomes) computes an
// edge's REMAINING tokens from an in-memory map instead of hitting Yugabyte once
// per candidate per create. Remaining(edge) = sellable(edge) − used(edge); the
// caller supplies sellable (the ledger's seeded balance / NIC×90%), this cache
// supplies used. A few-seconds staleness is acceptable: placement is purely
// advisory/optimistic — there is NO authoritative reserve gate behind it (the create
// path writes no etcd ledger reservation; strict no-oversell is a future Yugabyte
// capacity counter checked in the create txn, see the TODO in CreatePoolNonce), so a
// slightly stale "used" only ever causes a benign re-placement under contention.
type CapacityCache struct {
	store    *Store
	interval time.Duration
	log      *slog.Logger

	mu      sync.RWMutex
	used    map[model.EdgeID]int64 // bandwidth: Σ cost per home_edge (UsedByEdge)
	members map[model.EdgeID]int64 // materialization: member COUNT per home_edge (§9.1)
}

// NewCapacityCache builds a cache over store, refreshing every interval (0 → 5s).
// Call Refresh once before serving (or rely on the first tick) and run Run in a
// goroutine.
func NewCapacityCache(store *Store, interval time.Duration) *CapacityCache {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &CapacityCache{store: store, interval: interval, log: slog.Default(),
		used: map[model.EdgeID]int64{}, members: map[model.EdgeID]int64{}}
}

// WithLogger sets the logger used to surface refresh failures (stale-capacity
// visibility). nil keeps slog.Default(). Returns the cache for chaining.
func (c *CapacityCache) WithLogger(l *slog.Logger) *CapacityCache {
	if l != nil {
		c.log = l
	}
	return c
}

// Refresh pulls the current per-edge used capacity from Yugabyte into the cache.
// Best-effort: on error the previous snapshot is retained (placement degrades to
// slightly-stale, never to wrong-direction).
func (c *CapacityCache) Refresh(ctx context.Context) error {
	used, err := c.store.UsedByEdge(ctx)
	if err != nil {
		return err
	}
	members, err := c.store.MembersByEdge(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.used = used
	c.members = members
	c.mu.Unlock()
	return nil
}

// Used returns the cached used capacity for edge (0 if unknown — a brand-new edge
// with no pools yet, or before the first refresh).
func (c *CapacityCache) Used(edge model.EdgeID) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.used[edge]
}

// Members returns the cached materialized member count for edge (0 if unknown) — the
// §9.1 session-dimension "used" that placement bounds against the reported SessionBudget.
func (c *CapacityCache) Members(edge model.EdgeID) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.members[edge]
}

// Run refreshes the cache every interval until ctx is cancelled. Blocks; run in a
// goroutine. It refreshes once immediately so placement is warm before the first
// tick.
func (c *CapacityCache) Run(ctx context.Context) {
	if err := c.Refresh(ctx); err != nil && ctx.Err() == nil {
		// Placement now serves slightly-stale "used" — log LOUD so the staleness is
		// visible (a silently-failing refresh would let placement drift unnoticed).
		c.log.Error("capacity cache refresh failed (placement using stale used-capacity)", "err", err)
	}
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.Refresh(ctx); err != nil && ctx.Err() == nil {
				c.log.Error("capacity cache refresh failed (placement using stale used-capacity)", "err", err)
			}
		}
	}
}
