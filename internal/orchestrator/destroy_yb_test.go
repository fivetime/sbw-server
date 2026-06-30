package orchestrator

import (
	"net/netip"
	"testing"

	"github.com/fivetime/sbw-contract/model"
)

// TestDestroyPool_YB_NoEtcdPoolMemberWrite is the destroy-lifecycle SUCCESS CRITERION:
// with the bulk store wired, an admin destroy (a) removes the pool + its members from
// Yugabyte and (b) grows ZERO etcd keys under the bulk-data keyspaces
// (pools/idx/srcmap/resv/tok). The whole pool/member lifecycle on the production path
// is etcd-free; etcd keeps only coordination (edgever bumps for the withdraw). Requires
// YB_TEST_DSN.
func TestDestroyPool_YB_NoEtcdPoolMemberWrite(t *testing.T) {
	h := setup(t)
	yb := wireYB(t, h)
	h.registerAgent(t, "edge-a", 1_000_000, 1_000_000)
	h.registerAgent(t, "edge-b", 1_000_000, 1_000_000)

	const id = model.PoolID(990300)
	_ = yb.Delete(h.ctx, id)
	t.Cleanup(func() { _ = yb.Delete(h.ctx, id) })

	if _, err := h.o.CreatePool(h.ctx, ratePool(id, "192.0.2.10/32", "192.0.2.11/32"), 100); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if _, ok, _ := yb.Get(h.ctx, id); !ok {
		t.Fatalf("create must land the pool in yb")
	}
	etcdBefore := countBulkKeys(t, h)

	if err := h.o.DestroyPool(h.ctx, id); err != nil {
		t.Fatalf("DestroyPool: %v", err)
	}

	// (a) the pool + members are gone from Yugabyte.
	if _, ok, err := yb.Get(h.ctx, id); err != nil || ok {
		t.Fatalf("destroy must remove the pool from yb: ok=%v err=%v", ok, err)
	}
	if cs, err := yb.MemberConflicts(h.ctx, []netip.Prefix{
		netip.MustParsePrefix("192.0.2.10/32"), netip.MustParsePrefix("192.0.2.11/32"),
	}, 0); err != nil || len(cs) != 0 {
		t.Fatalf("destroy must remove the members from yb: cs=%v err=%v", cs, err)
	}
	// (b) ZERO etcd growth under the bulk-data keyspaces (pools/idx/srcmap/resv/tok).
	if etcdAfter := countBulkKeys(t, h); etcdAfter != etcdBefore {
		t.Fatalf("DestroyPool grew etcd bulk keys: before=%d after=%d (must be flat)", etcdBefore, etcdAfter)
	}
	// Belt-and-suspenders: each bulk keyspace is individually empty for this pool.
	for _, suffix := range []string{"pools/", "idx/", "srcmap/", "resv/", "tok/"} {
		if keys := h.etcdKeys(t, suffix); len(keys) != 0 {
			t.Fatalf("destroy wrote etcd %s keys: %v", suffix, keys)
		}
	}
}

// TestAsyncDoubleDeath_YB_NoEtcdPoolMemberWrite asserts the ASYNC double-death teardown
// (the production reconcile path, edgever wired) tears the pool down in Yugabyte
// (DeleteCAS removes pool+members) and writes ZERO etcd pool/member data: no srcmap
// release, no ledger return, no poolstore key — only the edgever withdraw bumps. A
// fail-OPEN pool with both homes dead and no spare drives asyncDoubleDeath via
// ReconcilePool. Requires YB_TEST_DSN.
func TestAsyncDoubleDeath_YB_NoEtcdPoolMemberWrite(t *testing.T) {
	h, dead := setupRec(t)
	yb := wireYB(t, h)
	h.registerAgent(t, "edge-a", 1_000_000, 1_000_000)
	h.registerAgent(t, "edge-b", 1_000_000, 1_000_000)

	const id = model.PoolID(990301)
	_ = yb.Delete(h.ctx, id)
	t.Cleanup(func() { _ = yb.Delete(h.ctx, id) })

	failOpen := ratePool(id, "192.0.2.20/32")
	failOpen.FailOpen = true
	if _, err := h.o.CreatePool(h.ctx, failOpen, 100); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if _, ok, _ := yb.Get(h.ctx, id); !ok {
		t.Fatalf("create must land the pool in yb")
	}
	etcdBefore := countBulkKeys(t, h)

	// Both homes die; no third edge → no spare → double death on a fail-open pool.
	dead["edge-a"], dead["edge-b"] = true, true

	// Drive the reconcile to its terminal (double-death) state.
	st := h.reconcile(t, id)
	if st != StatusDegraded {
		// asyncProvisionBackup → no spare → asyncDoubleDeath → StatusDegraded.
		t.Fatalf("double-death reconcile status=%d, want Degraded", st)
	}

	// Fail-OPEN double death tore the pool down IN YUGABYTE (DeleteCAS).
	if _, ok, err := yb.Get(h.ctx, id); err != nil || ok {
		t.Fatalf("fail-open double death must delete the pool from yb: ok=%v err=%v", ok, err)
	}
	// ZERO etcd bulk-data growth: no srcmap release / ledger return / poolstore write.
	if etcdAfter := countBulkKeys(t, h); etcdAfter != etcdBefore {
		t.Fatalf("async double-death grew etcd bulk keys: before=%d after=%d (must be flat)", etcdBefore, etcdAfter)
	}
	for _, suffix := range []string{"pools/", "idx/", "srcmap/", "resv/", "tok/"} {
		if keys := h.etcdKeys(t, suffix); len(keys) != 0 {
			t.Fatalf("async double-death wrote etcd %s keys: %v", suffix, keys)
		}
	}
}
