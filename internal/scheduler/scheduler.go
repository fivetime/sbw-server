// Package scheduler does pool placement (controller §4.1/§4.3): pick the home
// agents for a pool from the registered candidates, honoring the two hard rules
// — each home must have enough remaining tokens (Σhome ≤ NIC×90%, computed
// optimistically from the in-memory CapacityCache: sellable(NIC×90%) − cached
// Yugabyte used, with the create reserving nothing), and the primary/backup
// must be DISTINCT agents
// (anti-affinity / different failure domain). Because the CONTROLLER selects the
// agents (not agents grabbing work), anti-affinity is guaranteed by picking
// distinct candidates (§4.3 "controller 选 agent 天然保证主备分散").
//
// The selection STRATEGY (which qualifying agents to prefer) is a replaceable
// detail; V1 uses worst-fit (most-remaining-first) to spread load and leave
// headroom. The architectural invariants — enough tokens, distinct homes — are
// what this package guarantees regardless of strategy.
package scheduler

import (
	"context"
	"errors"
	"math/rand/v2"
	"sort"

	"github.com/fivetime/sbw-contract/model"
)

// ErrInsufficientCapacity is returned when fewer than the requested number of distinct agents
// have enough remaining tokens (the pool can't be placed → caller alarms /
// triggers rebalance).
var ErrInsufficientCapacity = errors.New("scheduler: not enough agents with capacity")

// Remaining reports an agent's remaining tokens. In production it is satisfied by
// orchestrator.remaining = sellable(NIC×90%) − cached Yugabyte used (in-memory),
// not the etcd ledger's Remaining.
type Remaining func(ctx context.Context, edge model.EdgeID) (int64, error)

// randomTieBreak controls SelectHomes' tie-break among equally-free candidates: random in
// production (spreads create bursts evenly — the orchestrator's optimistic "used" cache
// lags by a few seconds so a burst sees every edge equally free), deterministic edge-id in
// tests (reproducible placement). Tests flip it via DisableRandomTieBreak in TestMain.
var randomTieBreak = true

// DisableRandomTieBreak switches SelectHomes to a deterministic edge-id tie-break, for test
// reproducibility. Production leaves the random spread. Not safe to toggle concurrently.
func DisableRandomTieBreak() { randomTieBreak = false }

// SelectHomes picks n distinct home agents from candidates, each with at least
// need remaining tokens, preferring those with the most remaining (worst-fit /
// spread). The result is ordered most-free-first, so [0] is the natural primary
// and [1] the backup. Returns ErrInsufficientCapacity if fewer than n qualify.
func SelectHomes(ctx context.Context, candidates []model.EdgeID, rem Remaining, need int64, n int) ([]model.EdgeID, error) {
	if n <= 0 {
		return nil, nil
	}
	type cand struct {
		edge model.EdgeID
		free int64
	}
	var qual []cand
	seen := make(map[model.EdgeID]struct{}, len(candidates))
	for _, e := range candidates {
		if _, dup := seen[e]; dup {
			continue // distinct candidates only
		}
		seen[e] = struct{}{}
		free, err := rem(ctx, e)
		if err != nil {
			return nil, err
		}
		if free >= need {
			qual = append(qual, cand{edge: e, free: free})
		}
	}
	if len(qual) < n {
		return nil, ErrInsufficientCapacity
	}
	// Worst-fit: most remaining first, RANDOM tie-break (not edge id). The orchestrator's
	// per-edge "used" is a cache refreshed every few seconds, so during a create burst all
	// edges look equally free; an edge-id tie-break then piles the whole burst onto the
	// lowest id (observed: 998 pools landed 372/171/455 across l1/l2/l3 instead of ~333
	// each). Shuffle first, then a STABLE sort by free, so equally-free edges keep the
	// random order and the burst spreads evenly; worst-fit still dominates and corrects
	// toward least-loaded once "used" refreshes.
	if randomTieBreak {
		rand.Shuffle(len(qual), func(i, j int) { qual[i], qual[j] = qual[j], qual[i] })
		sort.SliceStable(qual, func(i, j int) bool { return qual[i].free > qual[j].free })
	} else {
		sort.Slice(qual, func(i, j int) bool {
			if qual[i].free != qual[j].free {
				return qual[i].free > qual[j].free
			}
			return qual[i].edge < qual[j].edge
		})
	}
	out := make([]model.EdgeID, n)
	for i := 0; i < n; i++ {
		out[i] = qual[i].edge
	}
	return out, nil
}
