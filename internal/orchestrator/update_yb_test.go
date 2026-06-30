package orchestrator

import (
	"context"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/fivetime/sbw-contract/model"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/fivetime/sbw-server/internal/ybstore"
)

// bulkDataKeyPrefixes are the etcd keyspaces that hold PER-POOL bulk DATA — the ones
// the hybrid migration moved into Yugabyte and that a member mutation must therefore
// NOT grow. (Coordination keyspaces — leader/sharding/liveness/edgever/registry —
// are intentionally excluded; those stay on etcd.) Each is relative to the harness's
// test prefix.
var bulkDataKeyPrefixes = []string{
	"pools/",  // poolstore Record
	"idx/",    // poolstore secondary indexes
	"srcmap/", // src→home claims
	"resv/",   // ledger reservations
	"tok/",    // ledger token debits
}

// countBulkKeys returns the total number of etcd keys under the harness prefix that
// live in a bulk-DATA keyspace (pools/idx/srcmap/resv/tok) — the count that must stay
// FLAT across a yb-wired member mutation.
func countBulkKeys(t *testing.T, h *harness) int {
	t.Helper()
	resp, err := h.cli.Get(h.ctx, h.pfx, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		t.Fatalf("etcd scan: %v", err)
	}
	n := 0
	for _, kv := range resp.Kvs {
		rel := strings.TrimPrefix(string(kv.Key), h.pfx)
		for _, p := range bulkDataKeyPrefixes {
			if strings.HasPrefix(rel, p) {
				n++
				break
			}
		}
	}
	return n
}

// wireYB dials the test Yugabyte (YB_TEST_DSN) and wires it as the harness's bulk
// store + capacity cache; it skips the test when no DB is configured/reachable. The
// schema (pools/members) must already be deployed (it is, in the lab).
func wireYB(t *testing.T, h *harness) *ybstore.Store {
	t.Helper()
	dsn := os.Getenv("YB_TEST_DSN")
	if dsn == "" {
		t.Skip("YB_TEST_DSN not set; skipping yb-wired orchestrator test")
	}
	ctx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
	defer cancel()
	pool, err := ybstore.Connect(ctx, dsn)
	if err != nil {
		t.Skipf("Yugabyte unreachable (%v); skipping", err)
	}
	t.Cleanup(pool.Close)
	yb := ybstore.New(pool)
	cap := ybstore.NewCapacityCache(yb, time.Second)
	h.o.SetYBStore(yb, cap)
	return yb
}

// TestUpdatePool_YB_LandsMembersInYugabyteNoEtcdWrite is the SUCCESS CRITERION: with
// the bulk store wired, a PUT that ADDS members to a pool (a) lands those members in
// the Yugabyte members table, and (b) grows ZERO etcd keys under the bulk-data
// keyspaces (pools/idx/srcmap/resv/tok). RemoveMember is likewise asserted to drop
// the member from Yugabyte without an etcd write. Requires YB_TEST_DSN.
func TestUpdatePool_YB_LandsMembersInYugabyteNoEtcdWrite(t *testing.T) {
	h := setup(t)
	yb := wireYB(t, h)
	h.registerAgent(t, "edge-a", 1_000_000, 1_000_000)
	h.registerAgent(t, "edge-b", 1_000_000, 1_000_000)

	const id = model.PoolID(990200)
	_ = yb.Delete(h.ctx, id)
	t.Cleanup(func() { _ = yb.Delete(h.ctx, id) })

	// CreatePool (already migrated) seeds the pool with one member in Yugabyte.
	if _, err := h.o.CreatePool(h.ctx, ratePool(id, "192.0.2.1/32"), 100); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	memBefore := memberCount(t, yb, h.ctx, id)
	if memBefore != 1 {
		t.Fatalf("create must land 1 member in yb, got %d", memBefore)
	}
	etcdBefore := countBulkKeys(t, h)

	// PUT: add two members (keep the original). This is the load test's member-scaling
	// phase — the path that previously collapsed to ~200 req/s and never landed members.
	upd := ratePool(id, "192.0.2.1/32", "192.0.2.2/32", "192.0.2.3/32")
	if _, err := h.o.UpdatePool(h.ctx, upd, 100); err != nil {
		t.Fatalf("UpdatePool add: %v", err)
	}

	// (a) the new members landed in Yugabyte.
	if got := memberCount(t, yb, h.ctx, id); got != 3 {
		t.Fatalf("UpdatePool must land 3 members in yb, got %d", got)
	}
	if rec, ok, _ := yb.Get(h.ctx, id); !ok || len(rec.Pool.Members) != 3 {
		t.Fatalf("yb body must carry 3 members, got ok=%v rec=%+v", ok, rec)
	}
	// (b) ZERO etcd growth under the bulk-data keyspaces.
	if etcdAfter := countBulkKeys(t, h); etcdAfter != etcdBefore {
		t.Fatalf("UpdatePool grew etcd bulk keys: before=%d after=%d (must be flat)", etcdBefore, etcdAfter)
	}

	// RemoveMember: drop one — lands in Yugabyte, still no etcd bulk write.
	if _, err := h.o.RemoveMember(h.ctx, id, netip.MustParsePrefix("192.0.2.2/32")); err != nil {
		t.Fatalf("RemoveMember: %v", err)
	}
	if got := memberCount(t, yb, h.ctx, id); got != 2 {
		t.Fatalf("RemoveMember must leave 2 members in yb, got %d", got)
	}
	if etcdAfter := countBulkKeys(t, h); etcdAfter != etcdBefore {
		t.Fatalf("RemoveMember grew etcd bulk keys: before=%d after=%d (must be flat)", etcdBefore, etcdAfter)
	}
}

// memberCount returns how many members the yb store reports for a pool (via the
// CIDR-overlap query, which reads the members table).
func memberCount(t *testing.T, yb *ybstore.Store, ctx context.Context, id model.PoolID) int {
	t.Helper()
	rec, ok, err := yb.Get(ctx, id)
	if err != nil {
		t.Fatalf("yb.Get: %v", err)
	}
	if !ok {
		return 0
	}
	n := 0
	for range rec.Pool.Members {
		n++
	}
	// Cross-check against the members table itself: every member prefix must resolve to
	// THIS pool in MemberConflicts (excludePool=0 returns all holders).
	for _, m := range rec.Pool.Members {
		cs, err := yb.MemberConflicts(ctx, []netip.Prefix{m.Prefix}, 0)
		if err != nil {
			t.Fatalf("yb.MemberConflicts: %v", err)
		}
		found := false
		for _, c := range cs {
			if c.PoolID == id && c.Prefix == m.Prefix {
				found = true
			}
		}
		if !found {
			t.Fatalf("member %s in body but not in members table for pool %d", m.Prefix, id)
		}
	}
	return n
}
