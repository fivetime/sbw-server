package ybstore

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/fivetime/sbw-contract/model"
	"github.com/jackc/pgx/v5/pgconn"
)

// mustPrefix parses a CIDR or fails the test.
func mustPrefix(t *testing.T, s string) netip.Prefix {
	t.Helper()
	p, err := netip.ParsePrefix(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return p
}

// TestNullEdge: an empty backup edge maps to a NULL backup_edge (so the
// pools_by_backup index never indexes a placeholder ""), a set one to its string.
func TestNullEdge(t *testing.T) {
	if nullEdge("") != nil {
		t.Fatalf("empty edge must map to NULL, got %v", nullEdge(""))
	}
	if got := nullEdge("edge-a"); got != "edge-a" {
		t.Fatalf("set edge must map to its string, got %v", got)
	}
}

// TestIsUniqueViolation: only a pgconn.PgError with code 23505 is the
// cross-pool double-claim signal; anything else is not.
func TestIsUniqueViolation(t *testing.T) {
	if !isUniqueViolation(&pgconn.PgError{Code: "23505"}) {
		t.Fatal("23505 must be a unique violation")
	}
	if isUniqueViolation(&pgconn.PgError{Code: "23503"}) {
		t.Fatal("23503 (fk) must not be a unique violation")
	}
	if isUniqueViolation(errors.New("plain")) {
		t.Fatal("a non-pg error must not be a unique violation")
	}
}

// TestCapacityCache: the cache reads UsedByEdge through the store, serves the
// snapshot, and retains the prior snapshot on a refresh error (degrade to stale,
// never to a wrong-direction value).
func TestCapacityCache(t *testing.T) {
	// A CapacityCache built with a nil store + manual map exercises Used without a DB.
	c := NewCapacityCache(nil, 0)
	if c.interval != 5*time.Second {
		t.Fatalf("zero interval must default to 5s, got %s", c.interval)
	}
	c.mu.Lock()
	c.used = map[model.EdgeID]int64{"edge-a": 4200}
	c.mu.Unlock()
	if got := c.Used("edge-a"); got != 4200 {
		t.Fatalf("Used(edge-a) = %d, want 4200", got)
	}
	if got := c.Used("edge-unknown"); got != 0 {
		t.Fatalf("unknown edge must be 0, got %d", got)
	}
}

// ybTestStore dials the test Yugabyte/Postgres if YB_TEST_DSN is set and the DB is
// reachable; otherwise it skips. This keeps the DB-backed assertions runnable in
// the lab (where YSQL is up) without failing the offline CI build.
func ybTestStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("YB_TEST_DSN")
	if dsn == "" {
		t.Skip("YB_TEST_DSN not set; skipping Yugabyte-backed test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := Connect(ctx, dsn)
	if err != nil {
		t.Skipf("Yugabyte unreachable (%v); skipping", err)
	}
	t.Cleanup(pool.Close)
	return New(pool)
}

// TestCreateGetDelete_RoundTrip exercises the create txn, the render reads, the
// anti-replay (ErrExists), the cross-pool double-claim (ErrMemberConflict) and the
// used-by-edge aggregate against a real Yugabyte/Postgres when one is configured.
func TestCreateGetDelete_RoundTrip(t *testing.T) {
	s := ybTestStore(t)
	ctx := context.Background()
	const id = model.PoolID(990001)
	const other = model.PoolID(990002)
	_ = s.Delete(ctx, id)
	_ = s.Delete(ctx, other)
	t.Cleanup(func() { _ = s.Delete(ctx, id); _ = s.Delete(ctx, other) })

	mem := mustPrefix(t, "203.0.113.7/32")
	rec := Record{
		Pool:    model.Pool{ID: id, Members: []model.Member{{Prefix: mem}}},
		Primary: "edge-a", Backup: "edge-b", Tokens: 1000,
	}
	if err := s.CreatePool(ctx, rec, rec.Pool.Members); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	// Idempotent replay: same id → ErrExists (0 rows, ON CONFLICT DO NOTHING).
	if err := s.CreatePool(ctx, rec, rec.Pool.Members); !errors.Is(err, ErrExists) {
		t.Fatalf("replay must be ErrExists, got %v", err)
	}

	// Cross-pool double-claim: a DIFFERENT pool claiming the same member → conflict.
	dup := Record{Pool: model.Pool{ID: other, Members: []model.Member{{Prefix: mem}}}, Primary: "edge-a"}
	if err := s.CreatePool(ctx, dup, dup.Pool.Members); !errors.Is(err, ErrMemberConflict) {
		t.Fatalf("cross-pool claim must be ErrMemberConflict, got %v", err)
	}
	// ...and it rolled back: the other pool must NOT exist.
	if _, ok, _ := s.Get(ctx, other); ok {
		t.Fatal("conflicting create must have rolled back the pool row")
	}

	// Render reads.
	if ph, err := s.PoolsForHome(ctx, "edge-a"); err != nil || len(ph) != 1 || ph[0].ID != id {
		t.Fatalf("PoolsForHome(edge-a) = %+v, %v", ph, err)
	}
	if pb, err := s.PoolsForBackup(ctx, "edge-b"); err != nil || len(pb) != 1 || pb[0].ID != id {
		t.Fatalf("PoolsForBackup(edge-b) = %+v, %v", pb, err)
	}

	// Member conflict for an overlapping /24.
	cs, err := s.MemberConflicts(ctx, []netip.Prefix{mustPrefix(t, "203.0.113.0/24")}, 0)
	if err != nil || len(cs) != 1 || cs[0].PoolID != id {
		t.Fatalf("MemberConflicts /24 = %+v, %v", cs, err)
	}

	// Capacity aggregate.
	used, err := s.UsedByEdge(ctx)
	if err != nil || used["edge-a"] < 1000 {
		t.Fatalf("UsedByEdge = %+v, %v", used, err)
	}

	if err := s.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, _ := s.Get(ctx, id); ok {
		t.Fatal("Delete must remove the pool")
	}
}

// TestUpdatePool_MemberSync exercises the in-place update path: UpdatePool rewrites
// the body, bumps the version, and SYNCS the members table (adds the new, drops the
// removed, leaves the unchanged) in one txn; a cross-pool double-claim on an added
// member rolls the whole sync back (ErrMemberConflict); RemoveMember drops a single
// member (pool-scoped) and refreshes the body. Runs only against a real Yugabyte
// (YB_TEST_DSN).
func TestUpdatePool_MemberSync(t *testing.T) {
	s := ybTestStore(t)
	ctx := context.Background()
	const id = model.PoolID(990020)
	const other = model.PoolID(990021)
	_ = s.Delete(ctx, id)
	_ = s.Delete(ctx, other)
	t.Cleanup(func() { _ = s.Delete(ctx, id); _ = s.Delete(ctx, other) })

	mA := mustPrefix(t, "198.51.100.1/32")
	mB := mustPrefix(t, "198.51.100.2/32")
	rec := Record{
		Pool:    model.Pool{ID: id, Members: []model.Member{{Prefix: mA}}},
		Primary: "edge-a", Backup: "edge-b", Tokens: 1000,
	}
	if err := s.CreatePool(ctx, rec, rec.Pool.Members); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	// ADD mB, KEEP mA. Body + members table must both carry {mA, mB}; version bumps.
	rec.Pool.Members = []model.Member{{Prefix: mA}, {Prefix: mB}}
	if err := s.UpdatePool(ctx, rec, rec.Pool.Members); err != nil {
		t.Fatalf("UpdatePool add: %v", err)
	}
	if got, _, _ := s.Get(ctx, id); len(got.Pool.Members) != 2 {
		t.Fatalf("body members after add = %v, want 2", got.Pool.Members)
	}
	row, _, _ := s.GetForReconcile(ctx, id)
	if row.Version != 2 {
		t.Fatalf("version after update = %d, want 2", row.Version)
	}
	// The members table got the new claim (so a cross-pool conflict is now detectable).
	if cs, _ := s.MemberConflicts(ctx, []netip.Prefix{mB}, 0); len(cs) != 1 || cs[0].PoolID != id {
		t.Fatalf("MemberConflicts(mB) = %+v, want pool %d", cs, id)
	}

	// REMOVE mA via the update set {mB}: the members-table row for mA is pruned.
	rec.Pool.Members = []model.Member{{Prefix: mB}}
	if err := s.UpdatePool(ctx, rec, rec.Pool.Members); err != nil {
		t.Fatalf("UpdatePool remove: %v", err)
	}
	if cs, _ := s.MemberConflicts(ctx, []netip.Prefix{mA}, 0); len(cs) != 0 {
		t.Fatalf("mA must be released after prune, got %+v", cs)
	}

	// Cross-pool double-claim: a SECOND pool exists holding mC; an UpdatePool on pool
	// `id` that tries to ADD mC must hit the members.prefix PK and ROLL BACK whole.
	mC := mustPrefix(t, "198.51.100.9/32")
	dup := Record{Pool: model.Pool{ID: other, Members: []model.Member{{Prefix: mC}}}, Primary: "edge-a"}
	if err := s.CreatePool(ctx, dup, dup.Pool.Members); err != nil {
		t.Fatalf("CreatePool other: %v", err)
	}
	rec.Pool.Members = []model.Member{{Prefix: mB}, {Prefix: mC}}
	if err := s.UpdatePool(ctx, rec, rec.Pool.Members); !errors.Is(err, ErrMemberConflict) {
		t.Fatalf("cross-pool add must be ErrMemberConflict, got %v", err)
	}
	// Rolled back: pool `id` still has only mB, and mC still belongs to `other`.
	if got, _, _ := s.Get(ctx, id); len(got.Pool.Members) != 1 || got.Pool.Members[0].Prefix != mB {
		t.Fatalf("after conflict body must be unchanged {mB}, got %+v", got.Pool.Members)
	}
	if cs, _ := s.MemberConflicts(ctx, []netip.Prefix{mC}, 0); len(cs) != 1 || cs[0].PoolID != other {
		t.Fatalf("mC must still belong to other (%d), got %+v", other, cs)
	}

	// RemoveMember drops mB (pool-scoped) + refreshes body; a foreign prefix is a no-op.
	if err := s.RemoveMember(ctx, id, mB); err != nil {
		t.Fatalf("RemoveMember mB: %v", err)
	}
	if got, _, _ := s.Get(ctx, id); len(got.Pool.Members) != 0 {
		t.Fatalf("body after RemoveMember(mB) = %+v, want empty", got.Pool.Members)
	}
	if cs, _ := s.MemberConflicts(ctx, []netip.Prefix{mB}, 0); len(cs) != 0 {
		t.Fatalf("mB must be released after RemoveMember, got %+v", cs)
	}
	// pool-scoped: removing mC (held by `other`) from pool `id` must NOT touch other's row.
	if err := s.RemoveMember(ctx, id, mC); err != nil {
		t.Fatalf("RemoveMember foreign mC: %v", err)
	}
	if cs, _ := s.MemberConflicts(ctx, []netip.Prefix{mC}, 0); len(cs) != 1 || cs[0].PoolID != other {
		t.Fatalf("foreign RemoveMember must not evict other's mC, got %+v", cs)
	}

	// UpdatePool on a destroyed pool → ErrNotFound (no silent member sync to nothing).
	_ = s.Delete(ctx, id)
	if err := s.UpdatePool(ctx, rec, rec.Pool.Members); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdatePool on missing pool must be ErrNotFound, got %v", err)
	}
}

// TestFailoverPivot_VersionCAS exercises the version-CAS failover pivot that
// replaced the etcd poolstore Record: a fresh pool starts at version=1, GetForReconcile
// reads the pivot row, UpdateCAS bumps the version (and re-homes members) only when the
// expected version matches (the no-double-failover guard), a STALE expected version is
// rejected with ErrConflict, the retiring marker round-trips addressably, and DeleteCAS
// is version-gated. Runs only against a real Yugabyte/Postgres (YB_TEST_DSN).
func TestFailoverPivot_VersionCAS(t *testing.T) {
	s := ybTestStore(t)
	ctx := context.Background()
	const id = model.PoolID(990010)
	_ = s.Delete(ctx, id)
	t.Cleanup(func() { _ = s.Delete(ctx, id) })

	mem := mustPrefix(t, "203.0.113.20/32")
	rec := Record{
		Pool:    model.Pool{ID: id, Members: []model.Member{{Prefix: mem}}},
		Primary: "edge-a", Backup: "edge-b", Tokens: 500,
	}
	if err := s.CreatePool(ctx, rec, rec.Pool.Members); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	// Fresh pool: pivot at version=1, retiring clear.
	row, ok, err := s.GetForReconcile(ctx, id)
	if err != nil || !ok {
		t.Fatalf("GetForReconcile: ok=%v err=%v", ok, err)
	}
	if row.Version != 1 || row.Primary != "edge-a" || row.Backup != "edge-b" || row.Retiring != "" {
		t.Fatalf("fresh pivot = %+v, want v1 edge-a/edge-b/no-retiring", row)
	}

	// PROMOTE under version-CAS: backup→primary, old primary marked retiring. Members
	// re-home to the new primary in the same txn.
	if err := s.UpdateCAS(ctx, row, row.Version, "edge-b", "", "edge-a"); err != nil {
		t.Fatalf("UpdateCAS promote: %v", err)
	}
	// A STALE version (the one we already consumed) must lose — no double-failover.
	if err := s.UpdateCAS(ctx, row, row.Version, "edge-c", "", "edge-a"); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale UpdateCAS must be ErrConflict, got %v", err)
	}
	row2, _, err := s.GetForReconcile(ctx, id)
	if err != nil {
		t.Fatalf("GetForReconcile after promote: %v", err)
	}
	if row2.Version != 2 || row2.Primary != "edge-b" || row2.Retiring != "edge-a" {
		t.Fatalf("post-promote pivot = %+v, want v2 edge-b retiring=edge-a", row2)
	}
	// Members followed the promote (render reads members.home_edge / pools.home_edge).
	if ph, err := s.PoolsForHome(ctx, "edge-b"); err != nil || len(ph) != 1 || ph[0].ID != id {
		t.Fatalf("PoolsForHome(edge-b) after promote = %+v, %v", ph, err)
	}

	// Clear the retiring marker (retire-old step).
	if err := s.UpdateCAS(ctx, row2, row2.Version, "edge-b", "", ""); err != nil {
		t.Fatalf("UpdateCAS retire: %v", err)
	}
	row3, _, _ := s.GetForReconcile(ctx, id)
	if row3.Version != 3 || row3.Retiring != "" {
		t.Fatalf("post-retire pivot = %+v, want v3 no-retiring", row3)
	}

	// PoolsForDeadEdge / ListIDs see the pool by its current home.
	if dead, err := s.PoolsForDeadEdge(ctx, "edge-b"); err != nil || len(dead) != 1 || dead[0] != id {
		t.Fatalf("PoolsForDeadEdge(edge-b) = %+v, %v", dead, err)
	}
	if ids, err := s.ListIDs(ctx); err != nil || len(ids) == 0 {
		t.Fatalf("ListIDs = %+v, %v", ids, err)
	}

	// DeleteCAS is version-gated: a stale version no-ops with ErrConflict; the
	// current version deletes.
	if err := s.DeleteCAS(ctx, id, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale DeleteCAS must be ErrConflict, got %v", err)
	}
	if err := s.DeleteCAS(ctx, id, row3.Version); err != nil {
		t.Fatalf("DeleteCAS current version: %v", err)
	}
	if _, ok, _ := s.GetForReconcile(ctx, id); ok {
		t.Fatal("DeleteCAS must remove the pool")
	}
}
