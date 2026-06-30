// Package coverage is the controller-sharding "brain" (DESIGN-liveness §8/§10):
// it turns the live controller set (ctrlreg) + the registered edges (agent
// registry) into this replica's two jobs — keep the tap peered with exactly the
// edges THIS replica covers (shard top-K), and answer "who covers edge X" so an
// agent can be told its primary/fallback coverers.
//
// It is deliberately decoupled from the live dial mechanism: the TapSink
// interface (Ensure(coveredEdges)) is satisfied by the active-dial ribtap
// adapter in production and by a fake in tests, so the coverage logic is fully
// unit-testable without etcd or GoBGP.
package coverage

import (
	"context"
	"time"

	"github.com/fivetime/sbw-contract/model"

	"github.com/fivetime/sbw-server/internal/shard"
)

// Members supplies the live controller ids (ctrlreg.Registry.IDs satisfies it).
type Members interface {
	IDs(ctx context.Context) ([]string, error)
}

// Edges supplies the registered edge ids (an adapter over the agent registry).
type Edges interface {
	EdgeIDs(ctx context.Context) ([]model.EdgeID, error)
}

// TapSink makes the tap peer with EXACTLY the given edges (add missing, drop
// extra). The active-dial ribtap adapter satisfies it in production.
type TapSink interface {
	Ensure(ctx context.Context, edges []model.EdgeID) error
}

// Reconciler keeps this replica's tap matched to its covered edge set and
// answers coverer lookups for agent assignment.
type Reconciler struct {
	self    string // this replica's controller id (shard hash key)
	k       int    // coverage redundancy (K=2)
	members Members
	edges   Edges
	tap     TapSink
}

// New builds a Reconciler for replica `self` with K-redundant coverage.
func New(self string, k int, members Members, edges Edges, tap TapSink) *Reconciler {
	if k < 1 {
		k = 1
	}
	return &Reconciler{self: self, k: k, members: members, edges: edges, tap: tap}
}

// Reconcile computes the edges this replica covers from the current membership +
// edge set and drives the tap to peer exactly those. Call it on membership/edge
// changes and on a timer.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	ctrls, err := r.members.IDs(ctx)
	if err != nil {
		return err
	}
	es, err := r.edges.EdgeIDs(ctx)
	if err != nil {
		return err
	}
	return r.tap.Ensure(ctx, r.covered(es, ctrls))
}

// Run drives Reconcile continuously until ctx is done: once at start, then on
// every trigger (membership-change signal, e.g. ctrlreg.Watch coalesced to a
// tick) and on a periodic interval (catches edge-registration changes the
// trigger doesn't carry). A reconcile error is passed to onErr (nil → swallowed)
// so a transient etcd/tap hiccup doesn't kill the loop.
func (r *Reconciler) Run(ctx context.Context, trigger <-chan struct{}, interval time.Duration, onErr func(error)) {
	report := func() {
		if err := r.Reconcile(ctx); err != nil && onErr != nil {
			onErr(err)
		}
	}
	report() // converge immediately on startup
	var tick <-chan time.Time
	if interval > 0 {
		t := time.NewTicker(interval)
		defer t.Stop()
		tick = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-trigger:
			if !ok {
				trigger = nil // closed: stop selecting on it, keep the timer
				continue
			}
			report()
		case <-tick:
			report()
		}
	}
}

func (r *Reconciler) covered(edges []model.EdgeID, ctrls []string) []model.EdgeID {
	out := make([]model.EdgeID, 0, len(edges))
	for _, e := range edges {
		if shard.Covers(r.self, string(e), ctrls, r.k) {
			out = append(out, e)
		}
	}
	return out
}

// CoverersOf returns the controller ids covering edge, primary (index 0) first —
// what an agent is told as {primary, fallback...} (DESIGN-liveness §10).
func (r *Reconciler) CoverersOf(ctx context.Context, edge model.EdgeID) ([]string, error) {
	ctrls, err := r.members.IDs(ctx)
	if err != nil {
		return nil, err
	}
	return shard.Coverers(string(edge), ctrls, r.k), nil
}

// Covers reports whether this replica currently covers edge.
func (r *Reconciler) Covers(ctx context.Context, edge model.EdgeID) (bool, error) {
	ctrls, err := r.members.IDs(ctx)
	if err != nil {
		return false, err
	}
	return shard.Covers(r.self, string(edge), ctrls, r.k), nil
}
