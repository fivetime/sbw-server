package orchestrator

import (
	"strings"
	"testing"
	"time"

	"github.com/fivetime/sbw-contract/model"
)

// L-08: CRUD delivery must reach a home subscribed on a PEER replica (cross-shard).
// The orchestrator pushes synchronously only to locally-subscribed homes. For the
// rest, a peer's converge loop delivers — the FAILOVER/DESTROY paths bump edgever to
// trigger it. The CREATE hot path no longer bumps (hybrid scalability: zero per-create
// etcd writes): the authoritative pool+members are in Yugabyte, so the owning replica
// converges a cross-shard create via its agent's report → DriftSweep recompute +
// resync. These tests stand in for the lab scenario that previously needed a K=2 ->
// K=1 degradation to create an l1-primary pool with a cross-shard backup.

// CreatePool with a cross-shard backup: the local primary is pushed; the remote backup
// is NEITHER pushed NOR bumped here (hybrid: no per-create etcd write — convergence is
// via the report→DriftSweep backstop on the owning replica). The create still succeeds.
func TestCreatePoolCrossShardBackupNoBump(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 60) // less free -> backup
	h.push.markRemote("edge-b")           // backup owned by a peer replica

	rec, err := h.o.CreatePool(h.ctx, ratePool(200, "203.0.113.0/24"), 30)
	if err != nil {
		t.Fatalf("CreatePool (cross-shard backup) should succeed, got: %v", err)
	}
	if rec.Primary != "edge-a" || rec.Backup != "edge-b" {
		t.Fatalf("placement = %s/%s, want edge-a/edge-b", rec.Primary, rec.Backup)
	}
	// Local primary pushed; its desired version was never bumped (sync path).
	if _, ok := h.push.get("edge-a"); !ok {
		t.Errorf("local primary edge-a should have been pushed")
	}
	if d, _ := h.ev.Desired(h.ctx, "edge-a"); d != 0 {
		t.Errorf("local primary edge-a desired = %d, want 0 (pushed, not bumped)", d)
	}
	// Cross-shard backup NOT pushed here AND NOT bumped — the create writes no edgever
	// key. The owning replica converges it via report→DriftSweep (recompute from the
	// authoritative pool store), not a per-create bump.
	if _, ok := h.push.get("edge-b"); ok {
		t.Errorf("cross-shard backup edge-b must NOT be pushed by this replica")
	}
	if d, _ := h.ev.Desired(h.ctx, "edge-b"); d != 0 {
		t.Errorf("cross-shard backup edge-b desired = %d, want 0 (no per-create bump)", d)
	}
	// Optimistic create never touches the ledger.
	if free, _ := h.led.Remaining(h.ctx, "edge-b"); free != 60 {
		t.Errorf("edge-b remaining = %d, want 60 (optimistic create, no debit)", free)
	}
}

// The exact lab failure: BOTH homes owned by a peer replica (l1-primary pool whose
// homes neither resolve to the creating replica). The create succeeds with ZERO pushes
// and ZERO edgever bumps (no per-create etcd write); the owning replica(s) converge via
// the report→DriftSweep backstop, reading the pool from the authoritative store.
func TestCreatePoolBothHomesCrossShard(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 60)
	h.push.markRemote("edge-a", "edge-b")

	if _, err := h.o.CreatePool(h.ctx, ratePool(201, "203.0.113.0/24"), 30); err != nil {
		t.Fatalf("CreatePool with both homes cross-shard should succeed, got: %v", err)
	}
	if h.push.pushes != 0 {
		t.Errorf("no edge is local -> expected 0 pushes, got %d", h.push.pushes)
	}
	// No per-create edgever write on either home.
	for _, e := range []model.EdgeID{"edge-a", "edge-b"} {
		if d, _ := h.ev.Desired(h.ctx, e); d != 0 {
			t.Errorf("%s desired = %d, want 0 (create writes no edgever)", e, d)
		}
	}
	// But the pool record IS persisted, so the owning replica's DriftSweep will find
	// and converge it.
	if _, ok := h.yb.getRec(201); !ok {
		t.Error("pool record must be persisted for the cross-shard converge backstop")
	}
}

// DestroyPool gates quota-release on the cross-shard home confirming the withdraw
// (applied >= desired). Here a helper advances applied to mirror the agent, so the
// gate clears, the pool is gone, and quota is freed.
func TestDestroyPoolCrossShardGateClears(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 60)
	h.push.markRemote("edge-b")

	if _, err := h.o.CreatePool(h.ctx, ratePool(202, "203.0.113.0/24"), 30); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	// Mirror the peer converge + agent: keep applied[edge-b] caught up to desired so
	// the destroy withdraw gate clears.
	stop := make(chan struct{})
	go func() {
		tk := time.NewTicker(30 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				if d, err := h.ev.Desired(h.ctx, "edge-b"); err == nil && d > 0 {
					_ = h.ev.Advance(h.ctx, "edge-b", d)
				}
			}
		}
	}()
	err := h.o.DestroyPool(h.ctx, 202)
	close(stop)
	if err != nil {
		t.Fatalf("DestroyPool (gate should clear) failed: %v", err)
	}
	if _, ok := h.yb.getRec(202); ok {
		t.Errorf("pool 202 still present after destroy")
	}
	if free, _ := h.led.Remaining(h.ctx, "edge-a"); free != 100 {
		t.Errorf("edge-a remaining = %d, want 100 (freed)", free)
	}
	if free, _ := h.led.Remaining(h.ctx, "edge-b"); free != 60 {
		t.Errorf("edge-b remaining = %d, want 60 (freed)", free)
	}
}

// If the cross-shard home never confirms the withdraw, DestroyPool times out, KEEPS
// the pool (record restored), and does NOT free the quota — the load-bearing order.
func TestDestroyPoolCrossShardTimeoutKeepsPool(t *testing.T) {
	h := setup(t, WithWithdrawConfirmTimeout(400*time.Millisecond))
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 60)
	h.push.markRemote("edge-b")

	if _, err := h.o.CreatePool(h.ctx, ratePool(203, "203.0.113.0/24"), 30); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	// No one advances applied[edge-b] -> the gate must time out.
	err := h.o.DestroyPool(h.ctx, 203)
	if err == nil {
		t.Fatalf("DestroyPool should fail when cross-shard withdraw is unconfirmed")
	}
	if !strings.Contains(err.Error(), "pool kept, retry destroy") {
		t.Errorf("error = %v, want a retriable 'pool kept' error", err)
	}
	// Pool restored (kept) — the load-bearing invariant: a cross-shard withdraw that
	// is not confirmed must NOT tear the pool down. (No ledger assertion: the optimistic
	// create wrote no reservation, so there is no per-create quota to free here.)
	if _, ok := h.yb.getRec(203); !ok {
		t.Errorf("pool 203 should be restored (kept) after a failed destroy")
	}
}

// Single-replica (no edgever): even a notSub edge is delivered synchronously — the
// one controller owns every edge, so locallyDeliverable is always true and nothing
// is bumped. Guards the zero-regression promise.
func TestSingleReplicaIgnoresSubscription(t *testing.T) {
	h := setup(t)
	h.o.SetEdgeVer(nil) // drop to single-replica mode
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 60)
	h.push.markRemote("edge-a", "edge-b") // would be cross-shard IF sharded

	if _, err := h.o.CreatePool(h.ctx, ratePool(204, "203.0.113.0/24"), 30); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	// edgever nil -> always local -> both pushed, none bumped.
	if _, ok := h.push.get("edge-a"); !ok {
		t.Errorf("single-replica: edge-a should be pushed")
	}
	if _, ok := h.push.get("edge-b"); !ok {
		t.Errorf("single-replica: edge-b should be pushed")
	}
}
