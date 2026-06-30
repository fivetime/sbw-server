package orchestrator

import (
	"context"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/fivetime/sbw-contract/model"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// etcdKeys returns every key under the harness prefix + suffix (e.g. "resv/",
// "tok/", "srcmap/"), so a test can assert a code path wrote ZERO of a given kind.
func (h *harness) etcdKeys(t *testing.T, suffix string) []string {
	t.Helper()
	resp, err := h.cli.Get(h.ctx, h.pfx+suffix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		t.Fatalf("etcd get %s%s: %v", h.pfx, suffix, err)
	}
	out := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		out = append(out, string(kv.Key))
	}
	return out
}

// setupRec wires a mutable dead-set liveness oracle onto the shared harness (which
// already wires edgever) so the async reconciler can be driven step by step.
func setupRec(t *testing.T) (*harness, map[model.EdgeID]bool) {
	dead := map[model.EdgeID]bool{}
	h := setup(t, WithLiveness(func(e model.EdgeID) bool { return !dead[e] }))
	return h, dead
}

// markApplied advances an edge's etcd applied-version up to its desired-version —
// the test stand-in for the agent echoing it built the data plane (RunConverge +
// onReport do this in production).
func (h *harness) markApplied(t *testing.T, edge model.EdgeID) {
	t.Helper()
	des, err := h.ev.Desired(h.ctx, edge)
	if err != nil {
		t.Fatalf("desired %s: %v", edge, err)
	}
	if err := h.ev.Advance(h.ctx, edge, des); err != nil {
		t.Fatalf("advance %s: %v", edge, err)
	}
}

func (h *harness) reconcile(t *testing.T, id model.PoolID) ReconcileStatus {
	t.Helper()
	st, err := h.o.ReconcilePool(h.ctx, id)
	if err != nil {
		t.Fatalf("ReconcilePool(%d) status=%d: %v", id, st, err)
	}
	return st
}

// TestFailoverEdgeBulkRehomesAll proves the edge-death FAST path: FailoverEdgeBulk
// re-homes ALL of a dead edge's pools to their live+ready backups in ONE pass
// (primary→backup, backup cleared) and enqueues each for asyncProvisionBackup —
// instead of one ReconcilePool per pool (the per-pool same-key edgever-bump storm
// that made a real death a minutes-long gradual evacuation).
func TestFailoverEdgeBulkRehomesAll(t *testing.T) {
	h, dead := setupRec(t)
	h.registerAgent(t, "edge-a", 1000, 1000)
	h.registerAgent(t, "edge-b", 1000, 1000)
	h.registerAgent(t, "edge-c", 1000, 1000)

	cidrs := []string{"203.0.1.0/24", "203.0.2.0/24", "203.0.3.0/24", "203.0.4.0/24"}
	var ids []model.PoolID
	for i, c := range cidrs {
		rec, err := h.o.CreatePool(h.ctx, ratePool(model.PoolID(600+i), c), 10)
		if err != nil {
			t.Fatalf("CreatePool: %v", err)
		}
		ids = append(ids, rec.Pool.ID)
	}
	h.markApplied(t, "edge-a")
	h.markApplied(t, "edge-b")
	h.markApplied(t, "edge-c")

	// Kill the primary of the first pool; collect every pool primaried there with a
	// live backup (its expected new primary).
	r0, _, _ := h.yb.getRev(ids[0])
	deadEdge := r0.Primary
	dead[deadEdge] = true
	want := map[model.PoolID]model.EdgeID{}
	for _, id := range ids {
		r, _, _ := h.yb.getRev(id)
		if r.Primary == deadEdge && r.Backup != "" && !dead[r.Backup] {
			want[id] = r.Backup
		}
	}
	if len(want) == 0 {
		t.Skip("placement gave the dead edge no pool with a live backup")
	}

	var mu sync.Mutex
	enq := map[model.PoolID]bool{}
	h.o.FailoverEdgeBulk(h.ctx, deadEdge, func(id model.PoolID) { mu.Lock(); enq[id] = true; mu.Unlock() })

	for id, newPrimary := range want {
		r, _, _ := h.yb.getRev(id)
		if r.Primary != newPrimary {
			t.Errorf("pool %d primary=%s, want %s (bulk re-home to backup)", id, r.Primary, newPrimary)
		}
		if r.Backup != "" {
			t.Errorf("pool %d backup=%s, want cleared after promote", id, r.Backup)
		}
		if !enq[id] {
			t.Errorf("pool %d not enqueued for asyncProvisionBackup", id)
		}
	}
}

// Full evacuation cycle: primary dead + backup dead → provision a fresh backup,
// GATE the promote on the fresh backup's applied-version, promote, GATE the old-
// primary retire on the new primary's applied-version, retire, then reprovision.
func TestReconcileProvisionGatePromoteRetire(t *testing.T) {
	h, dead := setupRec(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)
	h.registerAgent(t, "edge-c", 100, 80)

	rec, err := h.o.CreatePool(h.ctx, ratePool(500, "203.0.113.0/24"), 30)
	if err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if rec.Primary != "edge-a" || rec.Backup != "edge-b" {
		t.Fatalf("placement primary %s backup %s, want edge-a/edge-b", rec.Primary, rec.Backup)
	}

	// Both homes die hard.
	dead["edge-a"], dead["edge-b"] = true, true

	// Pass 1: no live backup → provision a fresh one (edge-c). Acted, but NOT ready.
	if st := h.reconcile(t, 500); st != StatusActed {
		t.Fatalf("provision pass status=%d, want Acted", st)
	}
	cur, _, _ := h.yb.getRev(500)
	if cur.Backup != "edge-c" {
		t.Fatalf("fresh backup = %s, want edge-c", cur.Backup)
	}
	// Pass 2: backup alive but applied<desired → GATED (先就位再切).
	if st := h.reconcile(t, 500); st != StatusGated {
		t.Fatalf("promote pre-ready status=%d, want Gated", st)
	}
	cur, _, _ = h.yb.getRev(500)
	if cur.Primary != "edge-a" {
		t.Fatalf("primary moved before backup ready: %s", cur.Primary)
	}

	// edge-c builds its data plane (applied catches up).
	h.markApplied(t, "edge-c")

	// Pass 3: promote edge-c; old primary edge-a marked Retiring (not yet withdrawn).
	if st := h.reconcile(t, 500); st != StatusActed {
		t.Fatalf("promote status=%d, want Acted", st)
	}
	cur, _, _ = h.yb.getRev(500)
	if cur.Primary != "edge-c" || cur.Retiring != "edge-a" {
		t.Fatalf("after promote primary=%s retiring=%s, want edge-c/edge-a", cur.Primary, cur.Retiring)
	}
	// Egress pivot: src re-homed to the new primary.
	if sr, ok := h.yb.smGet(netip.MustParsePrefix("203.0.113.0/24")); !ok || sr.Home != "edge-c" {
		t.Fatalf("src not re-homed to edge-c: %+v", sr)
	}

	// Pass 4: retire gated until the NEW primary is applied.
	if st := h.reconcile(t, 500); st != StatusGated {
		t.Fatalf("retire pre-ready status=%d, want Gated", st)
	}
	cur, _, _ = h.yb.getRev(500)
	if cur.Retiring != "edge-a" {
		t.Fatalf("old primary retired before new primary ready")
	}
	h.markApplied(t, "edge-c")

	// Pass 5: retire edge-a — withdraw bump + free its quota.
	if st := h.reconcile(t, 500); st != StatusActed {
		t.Fatalf("retire status=%d, want Acted", st)
	}
	cur, _, _ = h.yb.getRev(500)
	if cur.Retiring != "" {
		t.Fatalf("retiring not cleared: %s", cur.Retiring)
	}
	if free, _ := h.led.Remaining(h.ctx, "edge-a"); free != 100 {
		t.Fatalf("dead primary edge-a tokens not freed: %d", free)
	}

	// Pass 6: N+0 → reprovision a backup. Only edge-b is left and it is dead → no
	// spare → degraded-but-stable (does not busy-loop).
	if st := h.reconcile(t, 500); st != StatusDegraded {
		t.Fatalf("reprovision-no-spare status=%d, want Degraded", st)
	}
}

// A live spare lets the post-promote reprovision restore N+1 and then settle.
func TestReconcileReprovisionRestoresRedundancy(t *testing.T) {
	h, dead := setupRec(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)
	h.registerAgent(t, "edge-c", 100, 80)
	if _, err := h.o.CreatePool(h.ctx, ratePool(510, "203.0.113.0/24"), 30); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	dead["edge-a"] = true // only the primary dies; backup edge-b is a live hot standby

	// Promote (hot standby edge-b is ready: never async-versioned). Then retire,
	// then reprovision onto edge-c, then settle healthy. Drive to convergence.
	steps := 0
	for {
		st := h.reconcile(t, 510)
		if st == StatusHealthy {
			break
		}
		if st == StatusGated {
			h.markApplied(t, "edge-b") // new primary applies
			h.markApplied(t, "edge-c") // fresh backup applies
		}
		if steps++; steps > 12 {
			cur, _, _ := h.yb.getRev(510)
			t.Fatalf("did not converge: %+v", cur)
		}
	}
	cur, _, _ := h.yb.getRev(510)
	if cur.Primary != "edge-b" || cur.Backup != "edge-c" || cur.Retiring != "" {
		t.Fatalf("converged state primary=%s backup=%s retiring=%s, want edge-b/edge-c/\"\"", cur.Primary, cur.Backup, cur.Retiring)
	}
	if free, _ := h.led.Remaining(h.ctx, "edge-a"); free != 100 {
		t.Fatalf("old primary tokens not freed: %d", free)
	}
}

// TestCreateLifecycleWritesNoLedgerKeys is the hybrid-architecture invariant guard:
// the WHOLE pool-create lifecycle — the synchronous optimistic create AND the
// async reconcile pass it triggers (which provisions/replaces the backup) — writes
// ZERO etcd ledger RESERVATION keys (sbw/resv + the per-pool sbw/tok debit). Capacity
// is optimistic from the cached UsedByEdge (or, in this DB-less harness, a ledger
// READ that never debits), so a create followed by a backup-provision reconcile must
// leave the ledger balances untouched and no reservation key behind. This is the
// regression guard for the two ledger keys (sbw/resv/pool-<id>-<backup> + sbw/tok)
// the prior pass left on the backup-provision path.
func TestCreateLifecycleWritesNoLedgerKeys(t *testing.T) {
	h, dead := setupRec(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)
	h.registerAgent(t, "edge-c", 100, 80)

	if _, err := h.o.CreatePool(h.ctx, ratePool(530, "203.0.113.0/24"), 30); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	// The create itself reserves nothing (optimistic placement).
	if keys := h.etcdKeys(t, "resv/"); len(keys) != 0 {
		t.Fatalf("create wrote ledger reservation keys: %v", keys)
	}

	// The backup dies; the reconcile pass must provision a FRESH backup (edge-c).
	dead["edge-b"] = true
	if st := h.reconcile(t, 530); st != StatusActed {
		t.Fatalf("backup-replace reconcile status=%d, want Acted", st)
	}
	cur, _, _ := h.yb.getRev(530)
	if cur.Backup != "edge-c" {
		t.Fatalf("fresh backup = %s, want edge-c", cur.Backup)
	}

	// The reconcile-driven backup provision must ALSO have reserved nothing: no resv
	// key, and every edge's ledger balance still at its seeded value (no debit).
	if keys := h.etcdKeys(t, "resv/"); len(keys) != 0 {
		t.Fatalf("reconcile backup-provision wrote ledger reservation keys: %v", keys)
	}
	for edge, want := range map[model.EdgeID]int64{"edge-a": 100, "edge-b": 90, "edge-c": 80} {
		if free, _ := h.led.Remaining(h.ctx, string(edge)); free != want {
			t.Errorf("%s ledger debited by create lifecycle = %d, want untouched %d", edge, free, want)
		}
	}
}

// Two coverers reconciling the same primary-dead pool concurrently must not split
// it: the promote serializes on the record revision (PutCAS), so the pool ends
// with exactly one new primary and the dead primary's quota freed once.
func TestReconcileConcurrentNoSplit(t *testing.T) {
	h, dead := setupRec(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)
	h.registerAgent(t, "edge-c", 100, 80)
	if _, err := h.o.CreatePool(h.ctx, ratePool(520, "203.0.113.0/24"), 30); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	dead["edge-a"] = true

	// Hammer ReconcilePool from two goroutines, advancing applied as versions bump,
	// until the pool converges. CAS conflicts are expected and benign.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	worker := func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			st, err := h.o.ReconcilePool(context.Background(), 520)
			if err != nil {
				continue
			}
			if st == StatusGated {
				h.markApplied(t, "edge-b")
				h.markApplied(t, "edge-c")
			}
		}
	}
	wg.Add(2)
	go worker()
	go worker()

	// Wait for convergence (primary moved off edge-a, backup restored, no retiring).
	// Poll over real time: the multi-step reconcile (provision backup → gate →
	// promote → gate → clear retiring) interleaves the two workers with the
	// markApplied gate advances, so give the scheduler a slice each pass.
	deadline := 0
	for {
		cur, _, ok := h.yb.getRev(520)
		if ok && cur.Primary == "edge-b" && cur.Backup == "edge-c" && cur.Retiring == "" {
			break
		}
		if deadline++; deadline > 200 {
			close(stop)
			wg.Wait()
			cur, _, _ := h.yb.getRev(520)
			t.Fatalf("did not converge under concurrency: %+v", cur)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	// Invariants: dead primary freed exactly once (balance back to full), surviving
	// homes not over-debited (no oversell / no double-free → non-negative, ≤ cap).
	if free, _ := h.led.Remaining(h.ctx, "edge-a"); free != 100 {
		t.Fatalf("edge-a (dead primary) tokens = %d, want 100 (freed once)", free)
	}
	for _, e := range []model.EdgeID{"edge-b", "edge-c"} {
		free, _ := h.led.Remaining(h.ctx, string(e))
		if free < 0 || free > 100 {
			t.Fatalf("%s balance out of range: %d (oversell/double-free)", e, free)
		}
	}
}

// A live primary with no spare capacity for a backup settles to degraded-but-
// stable rather than churning.
func TestReconcileDegradedNoSpare(t *testing.T) {
	h, dead := setupRec(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)
	if _, err := h.o.CreatePool(h.ctx, ratePool(530, "203.0.113.0/24"), 30); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	// The backup dies and there is no third home to replace it: primary alive, no
	// spare → degraded-but-stable (run primary-only until capacity frees up).
	dead["edge-b"] = true

	if st := h.reconcile(t, 530); st != StatusDegraded {
		t.Fatalf("no-spare status=%d, want Degraded", st)
	}
	// Stable: a second pass is still Degraded (no busy-loop, no spurious mutation).
	if st := h.reconcile(t, 530); st != StatusDegraded {
		t.Fatalf("second no-spare pass status=%d, want Degraded", st)
	}
}
