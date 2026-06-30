package orchestrator

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fivetime/sbw-contract/model"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/fivetime/sbw-server/internal/edgever"
	"github.com/fivetime/sbw-server/internal/ledger"
	"github.com/fivetime/sbw-server/internal/poolstore"
	"github.com/fivetime/sbw-server/internal/registry"
)

// Local etcd for this package (distinct port so `go test ./...` parallelizes).
var testEtcdAddr string

func TestMain(m *testing.M) {
	bin, err := exec.LookPath("etcd")
	if err != nil {
		os.Exit(m.Run())
	}
	dir, _ := os.MkdirTemp("", "etcd-orch-*")
	addr := "127.0.0.1:23794"
	peer := "http://127.0.0.1:23795"
	cmd := exec.Command(bin, "--data-dir", dir,
		"--listen-client-urls", "http://"+addr, "--advertise-client-urls", "http://"+addr,
		"--listen-peer-urls", peer, "--initial-advertise-peer-urls", peer,
		"--initial-cluster", "default="+peer, "--log-level", "error")
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(dir)
		os.Exit(m.Run())
	}
	cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{addr}, DialTimeout: 2 * time.Second})
	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		_, e := cli.Get(ctx, "ping")
		cancel()
		if e == nil {
			testEtcdAddr = addr
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = cli.Close()
	code := m.Run()
	_ = cmd.Process.Kill()
	_, _ = cmd.Process.Wait()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// fakePusher records the last desired state pushed per edge, and can be armed to
// fail for a chosen edge (to exercise rollback).
// errFakeBackpressure is the fakePusher's stand-in for grpcsrv.ErrSlowConsumer: a
// transient buffer-full condition the orchestrator must treat as best-effort (the
// create still commits) rather than rolling back.
var errFakeBackpressure = errors.New("fake: agent push buffer full")

type fakePusher struct {
	mu      sync.Mutex
	last    map[model.EdgeID]model.EdgeDesiredState
	pushes  int
	failFor map[model.EdgeID]bool
	// backpressureFor marks edges whose push returns a non-fatal slow-consumer /
	// buffer-full condition (errFakeBackpressure). CreatePool must NOT roll back on
	// it — the agent converges via the coalescing buffer / resync.
	backpressureFor map[model.EdgeID]bool
	// notSub marks edges whose agent stream is NOT subscribed to this replica
	// (cross-shard, owned by a peer). Empty → every edge is local (default), so
	// existing tests keep pushing synchronously.
	notSub map[model.EdgeID]bool
	// blockUntil, if non-nil, makes every push BLOCK until the channel is closed —
	// used to prove CreatePool returns (after the Txn) without waiting on the push.
	blockUntil chan struct{}
}

func newFakePusher() *fakePusher {
	return &fakePusher{last: map[model.EdgeID]model.EdgeDesiredState{}, failFor: map[model.EdgeID]bool{}, backpressureFor: map[model.EdgeID]bool{}, notSub: map[model.EdgeID]bool{}}
}

func (f *fakePusher) PushDesired(edge model.EdgeID, st model.EdgeDesiredState) error {
	f.mu.Lock()
	block := f.blockUntil
	fail := f.failFor[edge]
	bp := f.backpressureFor[edge]
	f.pushes++
	f.mu.Unlock()
	if block != nil {
		<-block // hold the push open (does NOT hold f.mu, so get/state stays readable)
	}
	if fail {
		return errors.New("push failed")
	}
	if bp {
		// Record the snapshot too (the coalescing transport would still deliver the
		// latest) and return the non-fatal backpressure sentinel.
		f.mu.Lock()
		f.last[edge] = st
		f.mu.Unlock()
		return errFakeBackpressure
	}
	f.mu.Lock()
	f.last[edge] = st
	f.mu.Unlock()
	return nil
}

// IsBackpressure satisfies the orchestrator's backpressureClassifier capability
// (grpcsrv.Server in prod). Only errFakeBackpressure is non-fatal backpressure.
func (f *fakePusher) IsBackpressure(err error) bool { return errors.Is(err, errFakeBackpressure) }

// withAsyncDispatch restores the DEFAULT (goroutine) post-Txn dispatch, overriding
// the harness's WithSyncDispatch. Used by TestCreatePoolReturnsBeforePush to prove
// CreatePool returns the moment the Txn commits, without waiting on the push.
func withAsyncDispatch() Option {
	return func(o *Orchestrator) { o.dispatch = func(f func()) { go f() } }
}

// IsSubscribed satisfies the orchestrator's subChecker capability (grpcsrv.Server
// in prod). An edge marked notSub is treated as owned by a peer replica → the
// orchestrator bumps edgever instead of pushing here (L-08).
func (f *fakePusher) IsSubscribed(edge model.EdgeID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.notSub[edge]
}

// markRemote flags edges as NOT subscribed here (cross-shard).
func (f *fakePusher) markRemote(edges ...model.EdgeID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range edges {
		f.notSub[e] = true
	}
}

func (f *fakePusher) get(edge model.EdgeID) (model.EdgeDesiredState, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.last[edge]
	return st, ok
}

type harness struct {
	o    *Orchestrator
	led  *ledger.Ledger
	reg  *registry.Registry
	push *fakePusher
	ev   *edgever.Store
	yb   *fakeYB
	cli  *clientv3.Client
	pfx  string
	ctx  context.Context
}

func setup(t *testing.T, opts ...Option) *harness {
	t.Helper()
	if testEtcdAddr == "" {
		t.Skip("etcd binary not available")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{testEtcdAddr}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	prefix := "test/" + strings.ReplaceAll(t.Name(), "/", "_") + "/"
	ctx := context.Background()
	t.Cleanup(func() {
		_, _ = cli.Delete(ctx, prefix, clientv3.WithPrefix())
		_ = cli.Close()
	})
	reg := registry.New(cli, prefix)
	led := ledger.New(cli, prefix, time.Minute)
	ev := edgever.New(cli, prefix)
	push := newFakePusher()

	var gen uint64
	base := []Option{
		WithEdgeAddrs(map[model.EdgeID]netip.Addr{
			"edge-a": netip.MustParseAddr("10.0.1.1"),
			"edge-b": netip.MustParseAddr("10.0.2.1"),
			"edge-c": netip.MustParseAddr("10.0.3.1"),
		}),
		WithGenerator(func() uint64 { gen++; return gen }),
		WithEdgeVer(ev),
		// Run the post-Txn async create job (the hot-path delta push) SYNCHRONOUSLY so
		// these tests' post-condition assertions (state pushed) are deterministic.
		// Production uses the default `go f()` (create never blocks on the agent); the
		// async-return behavior is covered by TestCreatePoolReturnsBeforePush.
		WithSyncDispatch(),
	}
	o := New(reg, push, append(base, opts...)...)
	// Wire the MANDATORY in-memory ybStore (the test analog of the production *ybstore.Store):
	// the whole pool/member lifecycle + the version-CAS failover pivot run through it. It is
	// also the lens the harness observes pool records / member homes through (h.yb.getRec /
	// h.yb.smGet) and serves the optimistic-placement capacity.
	yb := newFakeYB()
	o.SetYBStore(yb, yb)
	return &harness{o: o, led: led, reg: reg, push: push, ev: ev, yb: yb, cli: cli, pfx: prefix, ctx: ctx}
}

func (h *harness) registerAgent(t *testing.T, edge model.EdgeID, capacity, tokens int64) {
	t.Helper()
	if err := h.reg.Register(h.ctx, edge, uint64(capacity)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.led.InitAgent(h.ctx, string(edge), tokens); err != nil {
		t.Fatal(err)
	}
}

func ratePool(id model.PoolID, members ...string) model.Pool {
	ms := make([]model.Member, len(members))
	for i, m := range members {
		ms[i] = model.Member{Prefix: netip.MustParsePrefix(m)}
	}
	return model.Pool{
		ID: id, Members: ms,
		Action:      model.ActionSpec{Kind: model.ActionRateLimit},
		IngressRate: model.RateSpec{Type: model.RateKbps, CIR: 1_000_000, CommittedBurstBytes: 12_500_000},
		EgressRate:  model.RateSpec{Type: model.RateKbps, CIR: 2_000_000, CommittedBurstBytes: 25_000_000},
	}
}

func TestCreatePoolHappyPath(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 60) // less free → becomes backup (worst-fit picks edge-a primary)

	rec, err := h.o.CreatePool(h.ctx, ratePool(200, "203.0.113.0/24"), 30)
	if err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	// Worst-fit: edge-a (100 free) is primary, edge-b (60) is backup.
	if rec.Primary != "edge-a" || rec.Backup != "edge-b" {
		t.Fatalf("placement = primary %s / backup %s, want edge-a/edge-b", rec.Primary, rec.Backup)
	}
	// HYBRID (optimistic create): the create path no longer reserves/commits in the
	// etcd ledger — placement is sourced from the (cached) UsedByEdge map and the
	// authoritative no-oversell counter lives in Yugabyte (TODO). So the ledger
	// balance is UNTOUCHED by a create and NO reservation key is written.
	if free, _ := h.led.Remaining(h.ctx, "edge-a"); free != 100 {
		t.Errorf("edge-a ledger must be untouched by an optimistic create = %d, want 100", free)
	}
	if free, _ := h.led.Remaining(h.ctx, "edge-b"); free != 60 {
		t.Errorf("edge-b ledger must be untouched by an optimistic create = %d, want 60", free)
	}
	// Primary got a FULL state (anchors + flow_redirects); backup got STANDBY
	// (policers/classify, but no anchors).
	pst, ok := h.push.get("edge-a")
	if !ok || len(pst.Anchors) != 1 || len(pst.FlowRedirects) != 1 || pst.RedirectNextHop != netip.MustParseAddr("10.0.1.1") {
		t.Errorf("primary state wrong: %+v", pst)
	}
	bst, ok := h.push.get("edge-b")
	if !ok || len(bst.Policers) != 2 || len(bst.Anchors) != 0 || len(bst.FlowRedirects) != 0 {
		t.Errorf("backup standby state wrong: %+v", bst)
	}
	if err := pst.Validate(); err != nil {
		t.Errorf("primary state invalid: %v", err)
	}
}

// TestCreatePoolCommitsDespitePushFailure asserts the converged ASYNC design:
// the create COMMITS the instant the atomic etcd Txn lands and NEVER blocks on (or
// rolls back for) the agent. A push that outright FAILS in the post-Txn async job
// is best-effort — the record stays, tokens stay committed, and the report-hash
// drift backstop + the agent's on-connect/periodic resync recover the lost push.
// (This replaces the old synchronous "a push failure rolls the whole create back".)
func TestCreatePoolCommitsDespitePushFailure(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 100)
	h.push.failFor["edge-b"] = true // backup push fails in the async job → swallowed

	rec, err := h.o.CreatePool(h.ctx, ratePool(201, "203.0.113.0/24"), 30)
	if err != nil {
		t.Fatalf("create must commit despite an async push failure, got %v", err)
	}
	// Record persisted; the create COMMITS regardless of the async push outcome.
	// The optimistic create writes no ledger reservation, so the ledger is untouched
	// (the failover/Yugabyte counter — not the create — accounts capacity).
	if _, ok := h.yb.getRec(201); !ok {
		t.Error("pool record must be persisted (create committed)")
	}
	if free, _ := h.led.Remaining(h.ctx, string(rec.Primary)); free != 100 {
		t.Errorf("optimistic create leaves the ledger untouched, free=%d want 100", free)
	}
}

// TestCreatePoolReturnsBeforePush asserts that CreatePool RETURNS as soon as the
// atomic Txn commits and does NOT wait on the agent: with the DEFAULT (goroutine)
// dispatch and a pusher whose push blocks, the create still returns promptly with
// the record, and the record is already persisted. The push lands later.
func TestCreatePoolReturnsBeforePush(t *testing.T) {
	h := setup(t, withAsyncDispatch()) // override the harness's sync dispatch
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 100)

	release := make(chan struct{})
	h.push.blockUntil = release // every push blocks until released

	done := make(chan poolstore.Record, 1)
	go func() {
		rec, err := h.o.CreatePool(h.ctx, ratePool(206, "203.0.113.0/24"), 30)
		if err != nil {
			t.Errorf("create: %v", err)
		}
		done <- rec
	}()

	select {
	case rec := <-done:
		// Returned WITHOUT the (still-blocked) push completing.
		if rec.Primary == "" {
			t.Error("create returned an empty record")
		}
		if _, ok := h.yb.getRec(206); !ok {
			t.Error("record must be persisted before the async push runs")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CreatePool blocked on the agent push (must return after the Txn)")
	}
	close(release) // let the background push proceed/finish
}

// TestCreatePoolCommitsOnPushBackpressure asserts the scalability fix #2 decouple:
// a push that can't be delivered immediately because the agent's downlink buffer is
// full is BEST-EFFORT — the create still COMMITS (record persisted, tokens
// allocated) rather than rolling back. The agent converges to the latest desired
// state via the coalescing buffer / periodic resync. This is the opposite of
// TestCreatePoolRollbackOnPushFailure (a GENUINE push error still rolls back).
func TestCreatePoolCommitsOnPushBackpressure(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 100)
	h.push.backpressureFor["edge-b"] = true // backup push hits a full buffer (non-fatal)

	rec, err := h.o.CreatePool(h.ctx, ratePool(205, "203.0.113.0/24"), 30)
	if err != nil {
		t.Fatalf("create must commit despite push backpressure, got %v", err)
	}
	// Record persisted; the create commits despite backpressure. Optimistic create
	// writes no ledger reservation (the ledger stays at the seed).
	if _, ok := h.yb.getRec(205); !ok {
		t.Error("pool record must be persisted (create committed)")
	}
	if free, _ := h.led.Remaining(h.ctx, string(rec.Primary)); free != 100 {
		t.Errorf("optimistic create leaves the ledger untouched, free=%d want 100", free)
	}
}

func TestCreatePoolInsufficientCapacity(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	// Optimistic placement caps an edge at sellable = NIC*90%. edge-b's NIC (10 →
	// sellable 9) is below the requested 50, so only edge-a can host the pool and a
	// 2-home placement is impossible → ErrNoPlacement. (The ledger seed no longer
	// constrains placement; only the NIC capacity does.)
	h.registerAgent(t, "edge-b", 10, 10) // sellable 9 < 50 → can't be a second home

	_, err := h.o.CreatePool(h.ctx, ratePool(202, "203.0.113.0/24"), 50)
	if !errors.Is(err, ErrNoPlacement) {
		t.Fatalf("want ErrNoPlacement, got %v", err)
	}
	// Optimistic create writes nothing to the etcd ledger (trivially untouched).
	if free, _ := h.led.Remaining(h.ctx, "edge-a"); free != 100 {
		t.Errorf("edge-a tokens touched on failed placement: %d", free)
	}
}

func TestDestroyPoolWithdrawsAndReturns(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 100)

	if _, err := h.o.CreatePool(h.ctx, ratePool(203, "203.0.113.0/24"), 40); err != nil {
		t.Fatal(err)
	}
	if err := h.o.DestroyPool(h.ctx, 203); err != nil {
		t.Fatalf("DestroyPool: %v", err)
	}
	// Record gone, tokens fully restored, edges re-rendered to empty.
	if _, ok := h.yb.getRec(203); ok {
		t.Error("record must be gone after destroy")
	}
	if free, _ := h.led.Remaining(h.ctx, "edge-a"); free != 100 {
		t.Errorf("edge-a tokens not returned: %d", free)
	}
	st, _ := h.push.get("edge-a")
	if len(st.Anchors) != 0 || len(st.Policers) != 0 {
		t.Errorf("edge-a should be withdrawn to empty, got %+v", st)
	}
	// Idempotent: destroying again is a no-op.
	if err := h.o.DestroyPool(h.ctx, 203); err != nil {
		t.Errorf("destroy idempotency: %v", err)
	}
}

// TestDestroyPoolKeepsPoolWhenWithdrawFails guards the #1 fix: DestroyPool must NOT
// tear the pool down while a withdraw push failed (the pool may still be enforced on
// that edge). On failure the record is RESTORED and a retriable error returned; a
// retry after the withdraw recovers destroys it exactly once. (The ledger-quota
// assertions of the pre-hybrid test are gone: an optimistic create writes no
// reservation, so there is no per-create ledger quota to free — the load-bearing
// invariant under test is the record restore/retry, not the ledger.)
func TestDestroyPoolKeepsPoolWhenWithdrawFails(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 100)
	if _, err := h.o.CreatePool(h.ctx, ratePool(210, "203.0.113.0/24"), 40); err != nil {
		t.Fatal(err)
	}
	rec, _ := h.yb.getRec(210)
	h.push.failFor[rec.Primary] = true // the primary's withdraw push will fail

	if err := h.o.DestroyPool(h.ctx, 210); err == nil {
		t.Fatal("destroy must fail when a withdraw push fails")
	}
	// Pool kept (record restored).
	if _, ok := h.yb.getRec(210); !ok {
		t.Error("pool record must be restored when withdraw fails")
	}
	// Withdraw recovers → retry destroys cleanly.
	h.push.failFor[rec.Primary] = false
	if err := h.o.DestroyPool(h.ctx, 210); err != nil {
		t.Fatalf("retry destroy: %v", err)
	}
	if _, ok := h.yb.getRec(210); ok {
		t.Error("record must be gone after a successful retry")
	}
}

// TestUpdatePoolRollsBackTokensOnClaimConflict guards the #2 fix: a rate change
// that ALSO adds a member already held by another pool must fail ATOMICALLY — the
// whole ybStore.UpdatePool txn rolls back (conflict detected before any mutation),
// leaving the stored record untouched (old tokens, old member set). UpdatePool no
// longer touches the etcd ledger (it is one atomic ybStore txn), so the load-bearing
// invariant is the record being unchanged, not a ledger balance.
func TestUpdatePoolRollsBackTokensOnClaimConflict(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 200, 200)
	h.registerAgent(t, "edge-b", 200, 200)
	// Pool 711 owns 198.51.100.0/24; pool 710 owns 203.0.113.0/24 at 30 tokens.
	if _, err := h.o.CreatePool(h.ctx, ratePool(711, "198.51.100.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	if _, err := h.o.CreatePool(h.ctx, ratePool(710, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}

	// Raise 710 to 50 tokens AND add 198.51.100.0/24 (owned by 711): the cross-pool
	// member claim fails → the atomic ybStore txn rolls back wholesale and the stored
	// record stays at the OLD amount (30) with its original single member.
	upd := ratePool(710, "203.0.113.0/24", "198.51.100.0/24")
	if _, err := h.o.UpdatePool(h.ctx, upd, 50); err == nil {
		t.Fatal("update must fail on cross-pool member conflict")
	}
	after, _ := h.yb.getRec(710)
	if after.Tokens != 30 {
		t.Errorf("record tokens must be unchanged (30), got %d", after.Tokens)
	}
	if len(after.Pool.Members) != 1 {
		t.Errorf("record members must be unchanged (1), got %d", len(after.Pool.Members))
	}
}

func TestCreatePoolDuplicateRejected(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 100)
	if _, err := h.o.CreatePool(h.ctx, ratePool(204, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	if _, err := h.o.CreatePool(h.ctx, ratePool(204, "198.51.100.0/24"), 30); err == nil {
		t.Error("duplicate pool id must be rejected")
	}
}

func TestCreatePoolSingleHome(t *testing.T) {
	h := setup(t, WithReplicas(1))
	h.registerAgent(t, "edge-a", 100, 100)

	rec, err := h.o.CreatePool(h.ctx, ratePool(205, "203.0.113.0/24"), 30)
	if err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if rec.Backup != "" {
		t.Errorf("single-home pool should have no backup, got %q", rec.Backup)
	}
	if _, ok := h.push.get("edge-a"); !ok {
		t.Error("primary should have been pushed")
	}
}

// --- C-02: src_ip→home unique record integration ---

func TestCreatePoolClaimsSourcesToPrimary(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)

	if _, err := h.o.CreatePool(h.ctx, ratePool(500, "203.0.113.0/24", "198.51.100.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	// Both member sources homed to the PRIMARY (edge-a); none to the backup
	// (backup does not send FlowSpec, §5.3).
	a := h.yb.smSourcesForHome("edge-a")
	if len(a) != 2 {
		t.Errorf("primary should home both sources, got %v", a)
	}
	if b := h.yb.smSourcesForHome("edge-b"); len(b) != 0 {
		t.Errorf("backup must NOT be an egress home, got %v", b)
	}
}

func TestCreatePoolRejectsCrossPoolDoubleHome(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)

	// Pool 600 homes 203.0.113.0/24.
	if _, err := h.o.CreatePool(h.ctx, ratePool(600, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	// Pool 601 tries to include the SAME source → rejected by the unique record.
	_, err := h.o.CreatePool(h.ctx, ratePool(601, "203.0.113.0/24"), 30)
	if err == nil {
		t.Fatal("cross-pool double-home must be rejected")
	}
	// Pool 601 fully rolled back: no record. (Optimistic create writes no ledger
	// reservation, so the ledger stays at the seed regardless — there is nothing to
	// "restore"; the rejection just leaves no record + no member claim.)
	if _, ok := h.yb.getRec(601); ok {
		t.Error("rejected pool must not persist")
	}
	if free, _ := h.led.Remaining(h.ctx, "edge-a"); free != 100 {
		t.Errorf("optimistic create never touches the ledger: edge-a free=%d, want 100", free)
	}
	// Pool 600's claim is intact.
	if rec, ok := h.yb.smGet(netip.MustParsePrefix("203.0.113.0/24")); !ok || rec.PoolID != 600 {
		t.Errorf("original claim must survive the rejected create: %+v", rec)
	}
}

func TestDestroyPoolReleasesSources(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)
	if _, err := h.o.CreatePool(h.ctx, ratePool(602, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	if err := h.o.DestroyPool(h.ctx, 602); err != nil {
		t.Fatal(err)
	}
	if all := h.yb.smList(); len(all) != 0 {
		t.Errorf("destroy must release all src records, got %v", all)
	}
	// The source is now free for another pool to claim.
	if _, err := h.o.CreatePool(h.ctx, ratePool(603, "203.0.113.0/24"), 30); err != nil {
		t.Errorf("source must be reclaimable after destroy: %v", err)
	}
}

func TestPromoteRehomesSources(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100) // primary
	h.registerAgent(t, "edge-b", 100, 90)  // backup
	if _, err := h.o.CreatePool(h.ctx, ratePool(604, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	if _, err := h.o.PromoteBackup(h.ctx, 604); err != nil {
		t.Fatal(err)
	}
	// The src_ip→home record now points at the promoted primary (edge-b) — the
	// failover pivot (§6.4). Exactly one record, never split.
	rec, ok := h.yb.smGet(netip.MustParsePrefix("203.0.113.0/24"))
	if !ok || rec.Home != "edge-b" {
		t.Errorf("failover must re-home the source to edge-b, got %+v", rec)
	}
	if a := h.yb.smSourcesForHome("edge-a"); len(a) != 0 {
		t.Errorf("old primary must no longer be the egress home, got %v", a)
	}
}

// --- T-607: advertise gate (RIB-survival guard suppression) ---

func TestCreatePoolAdvertiseGateSuppresses(t *testing.T) {
	gone := netip.MustParsePrefix("198.51.100.5/32")
	h := setup(t, WithAdvertiseGate(func(edge model.EdgeID, member netip.Prefix) bool {
		return edge == "edge-a" && member == gone // edge-a's guard says this host vanished
	}))
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)

	// Pool with a present member and a gone member.
	if _, err := h.o.CreatePool(h.ctx, ratePool(800, "203.0.113.7/32", "198.51.100.5/32"), 30); err != nil {
		t.Fatal(err)
	}
	st, _ := h.push.get("edge-a") // primary
	// Only the present member is advertised + redirected; the gone one is withheld.
	if len(st.Anchors) != 1 || st.Anchors[0].Prefix != netip.MustParsePrefix("203.0.113.7/32") {
		t.Errorf("gate must suppress the gone member's anchor, got %v", st.Anchors)
	}
	if len(st.FlowRedirects) != 1 {
		t.Errorf("gate must suppress the gone member's redirect, got %v", st.FlowRedirects)
	}
	// Both members keep their limiting machinery (resume cleanly on return).
	if len(st.Policers) != 2 || len(st.ClassifySessions) != 4 {
		t.Errorf("suppression must keep policer/classify: %d/%d", len(st.Policers), len(st.ClassifySessions))
	}
	if err := st.Validate(); err != nil {
		t.Errorf("gated state invalid: %v", err)
	}
}

func TestRerenderEdgeReflectsGateChange(t *testing.T) {
	gone := netip.MustParsePrefix("198.51.100.5/32")
	suppress := true
	h := setup(t, WithReplicas(1), WithAdvertiseGate(func(_ model.EdgeID, member netip.Prefix) bool {
		return suppress && member == gone
	}))
	h.registerAgent(t, "edge-a", 100, 100)
	if _, err := h.o.CreatePool(h.ctx, ratePool(801, "203.0.113.7/32", "198.51.100.5/32"), 30); err != nil {
		t.Fatal(err)
	}
	if st, _ := h.push.get("edge-a"); len(st.Anchors) != 1 {
		t.Fatalf("initially the gone member must be suppressed, got %d anchors", len(st.Anchors))
	}
	// The host route returns → gate stops suppressing → a re-render advertises it.
	suppress = false
	if err := h.o.RerenderEdge(h.ctx, "edge-a"); err != nil {
		t.Fatal(err)
	}
	if st, _ := h.push.get("edge-a"); len(st.Anchors) != 2 || len(st.FlowRedirects) != 2 {
		t.Errorf("after host returns, re-render must advertise both members, got %d anchors", len(h.lastAnchors("edge-a")))
	}
}

func (h *harness) lastAnchors(edge model.EdgeID) []model.Anchor {
	st, _ := h.push.get(edge)
	return st.Anchors
}

// TestGenerationPerEdgeMonotonic proves the generation is a PER-EDGE in-memory
// monotonic counter (the genseq global-etcd-CAS replacement): each re-render of an
// edge carries a strictly higher generation than the previous render of THAT edge,
// every push has a non-zero generation, and NO global etcd "gen" key is written
// (the create/render hot path is etcd-free for generation). Each edge advances its
// OWN counter independently — a render of edge-b does not consume edge-a's stream.
func TestGenerationPerEdgeMonotonic(t *testing.T) {
	if testEtcdAddr == "" {
		t.Skip("etcd binary not available")
	}
	h := setup(t, WithReplicas(1))
	h.registerAgent(t, "edge-a", 100, 100)
	if _, err := h.o.CreatePool(h.ctx, ratePool(900, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	a1 := mustPush(t, h, "edge-a").Generation
	if a1 == 0 {
		t.Fatalf("create generation must be non-zero, got %d", a1)
	}
	if err := h.o.RerenderEdge(h.ctx, "edge-a"); err != nil {
		t.Fatal(err)
	}
	a2 := mustPush(t, h, "edge-a").Generation
	if a2 <= a1 {
		t.Fatalf("re-render generation %d must exceed prior %d (per-edge monotonic)", a2, a1)
	}
	if a2 != a1+1 {
		t.Errorf("steady-path generation must be contiguous per edge: %d then %d", a1, a2)
	}

	// A second edge draws from its OWN counter, independent of edge-a's stream.
	h.registerAgent(t, "edge-b", 100, 100)
	if err := h.o.RerenderEdge(h.ctx, "edge-b"); err != nil {
		t.Fatal(err)
	}
	if err := h.o.RerenderEdge(h.ctx, "edge-b"); err != nil {
		t.Fatal(err)
	}
	b := mustPush(t, h, "edge-b").Generation
	if b == 0 {
		t.Fatalf("edge-b generation must be non-zero, got %d", b)
	}

	// No global generation key was ever written (genseq is gone): the only etcd keys
	// under the prefix are coordination state, never "<prefix>gen".
	resp, err := h.cli.Get(h.ctx, h.pfx+"gen")
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 0 {
		t.Errorf("render/create must write NO global etcd gen key, found %q", resp.Kvs[0].Value)
	}
}

// TestGenerationReseedNeverBackward proves a controller RESTART/handoff (a fresh
// genCtr map re-seeded from genSeed) never regresses a live edge's generation, and
// that the discontinuity is the kind the agent resolves with a full DESIRED_STATE
// resync: the new instance's first generation for the edge is STRICTLY ABOVE every
// value the prior instance issued, so a delta whose BaseGeneration no longer matches
// the agent's last-applied is detected as a gap (never a stale/duplicate that could
// be wrongly accepted). We simulate the restart by clearing the in-memory counter
// and re-seeding from a higher base.
func TestGenerationReseedNeverBackward(t *testing.T) {
	if testEtcdAddr == "" {
		t.Skip("etcd binary not available")
	}
	h := setup(t, WithReplicas(1))
	h.registerAgent(t, "edge-a", 100, 100)
	if _, err := h.o.CreatePool(h.ctx, ratePool(901, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	before := mustPush(t, h, "edge-a").Generation

	// Simulate a controller restart: the per-edge counter is in-memory, so a new
	// instance starts with an empty map and re-seeds from a HIGHER wall-clock base.
	h.o.genMu.Lock()
	h.o.genCtr = map[model.EdgeID]uint64{}
	h.o.genSeed = func() uint64 { return before + 1000 } // a strictly-higher instance seed
	h.o.genMu.Unlock()

	if err := h.o.RerenderEdge(h.ctx, "edge-a"); err != nil {
		t.Fatal(err)
	}
	after := mustPush(t, h, "edge-a").Generation
	if after <= before {
		t.Fatalf("post-restart generation %d must exceed pre-restart %d (never backward)", after, before)
	}
	// The jump (not before+1) is exactly the discontinuity the agent treats as a gap
	// → it requests a full DESIRED_STATE resync rather than wedging.
	if after == before+1 {
		t.Errorf("expected a re-seed JUMP (gap → resync), got contiguous %d→%d", before, after)
	}
}

// TestDeltaBaseGenerationChainAndGap proves the delta hot path chains
// BaseGeneration onto the last generation THIS replica delivered to the edge, and
// that a re-seed (restart) produces a delta whose BaseGeneration breaks the chain —
// the signal the agent uses to fall back to a full DESIRED_STATE resync.
func TestDeltaBaseGenerationChainAndGap(t *testing.T) {
	if testEtcdAddr == "" {
		t.Skip("etcd binary not available")
	}
	h := setup(t, WithReplicas(1))
	dp := &deltaCapture{}
	h.o.push = dp // a Pusher WITH PushDelta so the hot path ships incremental deltas

	h.registerAgent(t, "edge-a", 100, 100)
	if _, err := h.o.CreatePool(h.ctx, ratePool(902, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	// First delta on a cold edge bases off generation 0 (no prior delivery).
	d1 := dp.last()
	if d1.BaseGeneration != 0 {
		t.Fatalf("first delta on a cold edge must base off 0, got %d", d1.BaseGeneration)
	}
	g1 := d1.Generation

	// A second pool create chains: BaseGeneration == the generation the first delivered.
	if _, err := h.o.CreatePool(h.ctx, ratePool(903, "198.51.100.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	d2 := dp.last()
	if d2.BaseGeneration != g1 {
		t.Fatalf("steady delta must chain BaseGeneration onto %d, got %d", g1, d2.BaseGeneration)
	}
	if d2.Generation <= g1 {
		t.Fatalf("delta generation %d must exceed its base %d", d2.Generation, g1)
	}

	// Restart: clear the in-memory delivery + counter state, re-seed higher. The next
	// delta's BaseGeneration is now 0 again (this instance has delivered nothing to the
	// edge), which won't match the agent's last-applied generation → the agent gap-
	// detects and resyncs. Generation still moves strictly forward (never backward).
	h.o.genMu.Lock()
	h.o.lastGen = map[model.EdgeID]uint64{}
	h.o.genCtr = map[model.EdgeID]uint64{}
	h.o.genSeed = func() uint64 { return d2.Generation + 1000 }
	h.o.genMu.Unlock()

	if _, err := h.o.CreatePool(h.ctx, ratePool(904, "192.0.2.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	d3 := dp.last()
	if d3.BaseGeneration != 0 {
		t.Errorf("post-restart delta BaseGeneration should reset to 0 (gap → agent resync), got %d", d3.BaseGeneration)
	}
	if d3.Generation <= d2.Generation {
		t.Errorf("post-restart generation %d must still exceed prior %d", d3.Generation, d2.Generation)
	}
}

// deltaCapture is a Pusher WITH the optional PushDelta capability, recording the
// last delta the orchestrator shipped (for BaseGeneration/Generation assertions).
type deltaCapture struct {
	mu    sync.Mutex
	delta model.EdgeDesiredDelta
}

func (d *deltaCapture) PushDesired(model.EdgeID, model.EdgeDesiredState) error { return nil }
func (d *deltaCapture) PushDelta(_ model.EdgeID, delta model.EdgeDesiredDelta) error {
	d.mu.Lock()
	d.delta = delta
	d.mu.Unlock()
	return nil
}

func (d *deltaCapture) last() model.EdgeDesiredDelta {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.delta
}

func mustPush(t *testing.T, h *harness, edge model.EdgeID) model.EdgeDesiredState {
	t.Helper()
	st, ok := h.push.get(edge)
	if !ok {
		t.Fatalf("no push captured for %s", edge)
	}
	return st
}

// TestCreatePoolConcurrentSameID guards the create-if-not-exists CAS: N goroutines
// creating the SAME pool id concurrently (no LB serialization) yield EXACTLY ONE
// success + N-1 conflicts, and exactly ONE pool record exists afterward. (The
// pre-hybrid T-707 token/orphan accounting is gone: an optimistic create writes no
// ledger reservation, so there are no orphan reservations to leak and the ledger is
// untouched — the CAS-uniqueness invariant is what remains under test.)
func TestCreatePoolConcurrentSameID(t *testing.T) {
	h := setup(t)
	// THREE candidate edges (replicas=2): more homes than a pool needs, so losers can
	// place differently from the winner — still exactly one record wins.
	h.registerAgent(t, "edge-a", 1000, 1000)
	h.registerAgent(t, "edge-b", 1000, 1000)
	h.registerAgent(t, "edge-c", 1000, 1000)

	const N = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	var success, conflict, other int
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := h.o.CreatePool(h.ctx, ratePool(250, "203.0.113.0/24"), 30)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				success++
			case errors.Is(err, poolstore.ErrExists):
				conflict++
			default:
				other++
				t.Logf("unexpected create error: %v", err)
			}
		}()
	}
	wg.Wait()

	if success != 1 || conflict != N-1 || other != 0 {
		t.Fatalf("concurrent same-id: want 1 success + %d conflict + 0 other, got %d/%d/%d", N-1, success, conflict, other)
	}
	if _, ok := h.yb.getRec(250); !ok {
		t.Fatal("pool 250 must exist after concurrent creates")
	}
	// Optimistic create never debits the ledger, regardless of contention.
	for _, e := range []string{"edge-a", "edge-b", "edge-c"} {
		if free, _ := h.led.Remaining(h.ctx, e); free != 1000 {
			t.Errorf("%s ledger must be untouched by optimistic creates, got %d want 1000", e, free)
		}
	}
}
