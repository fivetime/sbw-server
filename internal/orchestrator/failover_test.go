package orchestrator

import (
	"context"
	"errors"
	"net/netip"
	"testing"

	"github.com/fivetime/sbw-contract/model"
)

func TestPromoteBackupSwapsAndWithdraws(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100) // primary (worst-fit, tie → edge-a)
	h.registerAgent(t, "edge-b", 100, 90)  // backup

	rec, err := h.o.CreatePool(h.ctx, ratePool(300, "203.0.113.0/24"), 30)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Primary != "edge-a" || rec.Backup != "edge-b" {
		t.Fatalf("placement %s/%s", rec.Primary, rec.Backup)
	}

	// Simulate edge-a hard death: its push will fail during withdraw.
	h.push.failFor["edge-a"] = true

	next, err := h.o.PromoteBackup(h.ctx, 300)
	// Withdraw of the dead old primary fails → non-fatal warning, next is valid.
	if err == nil {
		t.Log("note: withdraw succeeded (fake pusher); promote returned clean")
	}
	// PromoteBackup is now a pivot move: the old backup becomes primary, the backup is
	// dropped, and the old primary is marked Retiring (it keeps rendering FULL until the
	// new primary is delivered — no naked window). Delivery is via an edgever bump (the
	// async converge loop), NOT a synchronous push, so assert the PIVOT STATE, not
	// h.push.get.
	if next.Primary != "edge-b" || next.Backup != "" || next.Retiring != "edge-a" {
		t.Fatalf("after promote want primary edge-b/no backup/retiring edge-a, got %s/%s/%s", next.Primary, next.Backup, next.Retiring)
	}
	// The pool was created OPTIMISTICALLY (no ledger reservation), so promote has no
	// ledger key to transfer/return — the ledger balance is untouched.
	if free, _ := h.led.Remaining(h.ctx, "edge-a"); free != 100 {
		t.Errorf("edge-a tokens must be untouched: %d", free)
	}
	// Pivot persisted with the new primary, no backup, old primary retiring. Observed
	// through the store mirror (the egress home pivot is now edge-b).
	stored, _ := h.yb.getRec(300)
	if stored.Primary != "edge-b" || stored.Pool.HomeEdge != "edge-b" || stored.Backup != "" || stored.Retiring != "edge-a" {
		t.Errorf("stored record not updated: %+v", stored)
	}
}

// failoverNote records an autonomous-failover notification (test double for the
// control plane's emitFailover).
type failoverNote struct {
	id       model.PoolID
	from, to model.EdgeID
	gen      uint64
}

// TestFailoverEdgeNotifiesAutoPromote proves the synchronous node-failure drain
// (FailoverEdge) fires the autonomous-failover notification with the dead old primary
// and the promoted backup, and that a planned Decommission of the same shape does NOT.
func TestFailoverEdgeNotifiesAutoPromote(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100) // primary
	h.registerAgent(t, "edge-b", 100, 90)  // backup
	h.registerAgent(t, "edge-c", 100, 80)  // spare (reprovision target)

	var notes []failoverNote
	h.o.SetFailoverNotify(func(id model.PoolID, from, to model.EdgeID, gen uint64) {
		notes = append(notes, failoverNote{id, from, to, gen})
	})

	rec, err := h.o.CreatePool(h.ctx, ratePool(310, "203.0.113.0/24"), 30)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Primary != "edge-a" || rec.Backup != "edge-b" {
		t.Fatalf("placement %s/%s", rec.Primary, rec.Backup)
	}

	if err := h.o.FailoverEdge(h.ctx, "edge-a"); err != nil {
		t.Fatalf("FailoverEdge: %v", err)
	}
	if len(notes) != 1 {
		t.Fatalf("expected 1 failover notification, got %d (%+v)", len(notes), notes)
	}
	n := notes[0]
	if n.id != 310 || n.from != "edge-a" || n.to != "edge-b" {
		t.Fatalf("failover note mismatch: %+v (want pool 310 edge-a→edge-b)", n)
	}

	// A planned Decommission must NOT fire the unsolicited failover notification (it is
	// request-correlated). Drain edge-b (the now-primary) and assert no new note.
	before := len(notes)
	if err := h.o.Decommission(h.ctx, "edge-b"); err != nil {
		t.Fatalf("Decommission: %v", err)
	}
	if len(notes) != before {
		t.Fatalf("Decommission fired a failover notification (should be silent): %+v", notes[before:])
	}
}

func TestPromoteBackupNoBackup(t *testing.T) {
	h := setup(t, WithReplicas(1))
	h.registerAgent(t, "edge-a", 100, 100)
	if _, err := h.o.CreatePool(h.ctx, ratePool(301, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	if _, err := h.o.PromoteBackup(h.ctx, 301); !errors.Is(err, ErrNoBackup) {
		t.Errorf("single-home promote must return ErrNoBackup, got %v", err)
	}
}

func TestProvisionBackupRestoresRedundancy(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)
	h.registerAgent(t, "edge-c", 100, 80) // spare

	if _, err := h.o.CreatePool(h.ctx, ratePool(302, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	// Promote: edge-b becomes primary, no backup.
	if _, err := h.o.PromoteBackup(h.ctx, 302); err != nil {
		t.Fatal(err)
	}
	// Provision a fresh backup, excluding the dead edge-a.
	rec, err := h.o.ProvisionBackup(h.ctx, 302, "edge-a")
	if err != nil {
		t.Fatalf("ProvisionBackup: %v", err)
	}
	if rec.Backup != "edge-c" {
		t.Fatalf("new backup should be edge-c (only spare), got %q", rec.Backup)
	}
	// The fresh backup is placed on the PIVOT (delivery is via an edgever bump, not a
	// synchronous push), so assert the pivot state via the store mirror: edge-c is the
	// backup; the primary stayed edge-b. Optimistic placement writes nothing to the
	// etcd ledger, so edge-c's seeded balance is untouched.
	stored, _ := h.yb.getRec(302)
	if stored.Backup != "edge-c" || stored.Primary != "edge-b" {
		t.Errorf("pivot must place edge-c as backup behind primary edge-b, got %+v", stored)
	}
	if free, _ := h.led.Remaining(h.ctx, "edge-c"); free != 80 {
		t.Errorf("optimistic placement must leave edge-c ledger untouched = %d, want 80", free)
	}
	// Idempotent: provisioning again with a healthy backup is a no-op.
	again, err := h.o.ProvisionBackup(h.ctx, 302)
	if err != nil || again.Backup != "edge-c" {
		t.Errorf("provision with healthy backup should no-op: %+v err=%v", again, err)
	}
}

func TestProvisionBackupNoSpare(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)
	if _, err := h.o.CreatePool(h.ctx, ratePool(303, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	if _, err := h.o.PromoteBackup(h.ctx, 303); err != nil {
		t.Fatal(err)
	}
	// Only edge-a (excluded as dead) and edge-b (now primary) exist → no spare.
	_, err := h.o.ProvisionBackup(h.ctx, 303, "edge-a")
	if !errors.Is(err, ErrNoPlacement) {
		t.Errorf("want ErrNoPlacement when no spare, got %v", err)
	}
}

func TestCleanupRevivedIsNonPreemptive(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)
	if _, err := h.o.CreatePool(h.ctx, ratePool(304, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	if _, err := h.o.PromoteBackup(h.ctx, 304); err != nil {
		t.Fatal(err)
	}
	// edge-a revives. CleanupRevived pushes its CURRENT authoritative state, which
	// is empty (it holds no pools now) — the cleanup directive. It must NOT make
	// edge-a primary again.
	if err := h.o.CleanupRevived(h.ctx, "edge-a"); err != nil {
		t.Fatalf("CleanupRevived: %v", err)
	}
	ast, ok := h.push.get("edge-a")
	if !ok || len(ast.Anchors) != 0 || len(ast.Policers) != 0 || len(ast.FlowRedirects) != 0 {
		t.Errorf("revived edge-a must be told to clean up (empty state), got %+v", ast)
	}
	// edge-b is still the primary — no preemption.
	stored, _ := h.yb.getRec(304)
	if stored.Primary != "edge-b" {
		t.Errorf("revival must not reclaim primary: %+v", stored)
	}
}

func TestDecommissionPromotesHotBackup(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100) // primary of 305
	h.registerAgent(t, "edge-b", 100, 90)  // backup of 305
	h.registerAgent(t, "edge-c", 100, 80)  // spare for replacement

	if _, err := h.o.CreatePool(h.ctx, ratePool(305, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	// Decommission edge-a (the primary): its hot backup edge-b is promoted, and a
	// fresh backup is NOT auto-added by promote (that's a separate concern) — but
	// edge-a must end up drained + deregistered.
	if err := h.o.Decommission(h.ctx, "edge-a"); err != nil {
		t.Fatalf("Decommission: %v", err)
	}
	stored, _ := h.yb.getRec(305)
	if stored.Primary != "edge-b" {
		t.Errorf("decommission must promote backup edge-b to primary, got %s", stored.Primary)
	}
	// edge-a removed from scheduling. Its drain (withdraw) is delivered via an edgever
	// bump on the async converge loop, NOT a synchronous push — so assert the PIVOT
	// state (edge-a no longer primary/backup; it is at most Retiring) rather than the
	// now-stale fakePusher render.
	if _, ok, _ := h.reg.Get(h.ctx, "edge-a"); ok {
		t.Error("decommissioned edge-a must be deregistered")
	}
	if stored.Primary == "edge-a" || stored.Backup == "edge-a" {
		t.Errorf("decommissioned edge-a must not remain primary/backup, got %+v", stored)
	}
	// Optimistic create reserved nothing, so edge-a's ledger balance is untouched.
	if free, _ := h.led.Remaining(h.ctx, "edge-a"); free != 100 {
		t.Errorf("edge-a tokens not returned: %d", free)
	}
}

func TestDecommissionProvisionsWhenNoBackup(t *testing.T) {
	h := setup(t, WithReplicas(1))
	h.registerAgent(t, "edge-a", 100, 100) // single home of 306
	h.registerAgent(t, "edge-b", 100, 90)  // spare to migrate onto

	if _, err := h.o.CreatePool(h.ctx, ratePool(306, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	// edge-a is primary with NO backup → decommission must provision a backup
	// (edge-b) first, then promote it.
	if err := h.o.Decommission(h.ctx, "edge-a"); err != nil {
		t.Fatalf("Decommission: %v", err)
	}
	stored, _ := h.yb.getRec(306)
	// The pool migrates onto edge-b (provision-then-promote). Delivery is via an edgever
	// bump on the converge loop, not a synchronous push, so assert the PIVOT state: edge-b
	// is the new primary (egress home), edge-a is at most Retiring.
	if stored.Primary != "edge-b" || stored.Pool.HomeEdge != "edge-b" {
		t.Errorf("pool must migrate to edge-b as primary, got %+v", stored)
	}
	if stored.Backup == "edge-a" {
		t.Errorf("drained edge-a must not be a home, got %+v", stored)
	}
}

func TestFailoverEdgeRehomesAndKeepsRegistered(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100) // primary of 700
	h.registerAgent(t, "edge-b", 100, 90)  // backup of 700
	h.registerAgent(t, "edge-c", 100, 80)  // spare

	if _, err := h.o.CreatePool(h.ctx, ratePool(700, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	// edge-a dies hard: its push fails. FailoverEdge promotes edge-b.
	h.push.failFor["edge-a"] = true
	if err := h.o.FailoverEdge(h.ctx, "edge-a"); err != nil {
		t.Fatalf("FailoverEdge: %v", err)
	}
	stored, _ := h.yb.getRec(700)
	if stored.Primary != "edge-b" {
		t.Errorf("failover must promote edge-b, got primary %s", stored.Primary)
	}
	// Unlike Decommission, the dead edge stays REGISTERED (revival possible §5.8).
	if _, ok, _ := h.reg.Get(h.ctx, "edge-a"); !ok {
		t.Error("hard-dead edge must remain registered for possible revival")
	}
	// src_ip→home re-homed to edge-b (the egress pivot).
	if rec, ok := h.yb.smGet(netip.MustParsePrefix("203.0.113.0/24")); !ok || rec.Home != "edge-b" {
		t.Errorf("failover must re-home the source to edge-b, got %+v", rec)
	}
	// edge-a's tokens freed.
	if free, _ := h.led.Remaining(h.ctx, "edge-a"); free != 100 {
		t.Errorf("dead edge tokens not freed: %d", free)
	}
}

// --- C-04: double-death fail-open / fail-close ---

// withDeadSet wires a liveness oracle + double-death alarm onto a fresh harness.
func setupDD(t *testing.T, dead map[model.EdgeID]bool) (*harness, *[]ddEvent) {
	var events []ddEvent
	h := setup(t,
		WithLiveness(func(e model.EdgeID) bool { return !dead[e] }),
		WithDoubleDeathAlarm(func(id model.PoolID, failOpen bool) {
			events = append(events, ddEvent{id, failOpen})
		}),
	)
	return h, &events
}

type ddEvent struct {
	id       model.PoolID
	failOpen bool
}

func blackholePool(id model.PoolID, member string) model.Pool {
	rtbh := model.Community{ASN: 65000, Value: 666}
	return model.Pool{
		ID: id, Members: []model.Member{{Prefix: netip.MustParsePrefix(member)}},
		Action: model.ActionSpec{Kind: model.ActionBlackhole, RTBHCommunity: &rtbh},
	}
}

func TestDoubleDeathFailOpenTearsDownBillingPool(t *testing.T) {
	dead := map[model.EdgeID]bool{}
	h, events := setupDD(t, dead)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)
	// No spare: only edge-a, edge-b exist. BSS marks this billing pool fail-OPEN
	// (policy is now BSS-specified, not action-derived; default is fail-close).
	failOpenPool := ratePool(900, "203.0.113.0/24")
	failOpenPool.FailOpen = true
	if _, err := h.o.CreatePool(h.ctx, failOpenPool, 30); err != nil {
		t.Fatal(err)
	}
	// BOTH homes die; no spare to provision → double death on a billing pool. The
	// production hard-death path is the level-triggered reconciler (asyncDoubleDeath),
	// not the manual drain — drive ReconcilePool to the fail-open tear-down (DeleteCAS),
	// which fires the alarm exactly once.
	dead["edge-a"] = true
	dead["edge-b"] = true
	if st := h.o.mustReconcileDegraded(t, 900); st != StatusDegraded {
		t.Fatalf("double-death reconcile status=%d, want Degraded", st)
	}
	// Fail-OPEN: pool torn down via DeleteCAS — record gone, member srcs released.
	if _, ok := h.yb.getRec(900); ok {
		t.Error("billing pool must be torn down on double-death (fail-open)")
	}
	if rec, ok := h.yb.smGet(netip.MustParsePrefix("203.0.113.0/24")); ok {
		t.Errorf("src record must be released, got %+v", rec)
	}
	// Optimistic create reserved nothing, so both edges' ledger balances are untouched.
	if free, _ := h.led.Remaining(h.ctx, "edge-a"); free != 100 {
		t.Errorf("edge-a ledger must be untouched: %d", free)
	}
	if free, _ := h.led.Remaining(h.ctx, "edge-b"); free != 90 {
		t.Errorf("edge-b ledger must be untouched: %d", free)
	}
	if len(*events) != 1 || (*events)[0].id != 900 || !(*events)[0].failOpen {
		t.Errorf("expected one fail-OPEN alarm for pool 900, got %+v", *events)
	}
}

// mustReconcileDegraded runs one reconcile pass and fails the test on error.
func (o *Orchestrator) mustReconcileDegraded(t *testing.T, id model.PoolID) ReconcileStatus {
	t.Helper()
	st, err := o.ReconcilePool(context.Background(), id)
	if err != nil {
		t.Fatalf("ReconcilePool(%d): %v", id, err)
	}
	return st
}

func TestDoubleDeathFailCloseKeepsControlPool(t *testing.T) {
	dead := map[model.EdgeID]bool{}
	h, events := setupDD(t, dead)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)
	// Blackhole (control) pool.
	if _, err := h.o.CreatePool(h.ctx, blackholePool(901, "203.0.113.7/32"), 30); err != nil {
		t.Fatal(err)
	}
	dead["edge-a"] = true
	dead["edge-b"] = true
	// Drive the production hard-death path (the level-triggered reconciler). For a
	// fail-close pool, asyncDoubleDeath KEEPS the pool (no DeleteCAS) and fires the alarm
	// EXACTLY ONCE — unlike the manual drain, which could re-enter the double-death
	// branch (provision-then-promote-then-reprovision) and double-fire.
	if st := h.o.mustReconcileDegraded(t, 901); st != StatusDegraded {
		t.Fatalf("double-death reconcile status=%d, want Degraded", st)
	}
	// Fail-CLOSE: record KEPT (suppression intent preserved). The optimistic create
	// wrote no ledger reservation, so there is no committed quota to keep/return — the
	// load-bearing fail-close invariant is the RECORD being kept, not the ledger.
	if _, ok := h.yb.getRec(901); !ok {
		t.Error("control pool must be kept on double-death (fail-close)")
	}
	if free, _ := h.led.Remaining(h.ctx, "edge-a"); free != 100 {
		t.Errorf("optimistic create leaves the ledger untouched: edge-a free=%d, want 100", free)
	}
	if len(*events) != 1 || (*events)[0].id != 901 || (*events)[0].failOpen {
		t.Errorf("expected one fail-CLOSE alarm for pool 901, got %+v", *events)
	}
}

func TestFailoverSkipsDeadBackupProvisionsLiveSpare(t *testing.T) {
	dead := map[model.EdgeID]bool{}
	h, events := setupDD(t, dead)
	h.registerAgent(t, "edge-a", 100, 100) // primary
	h.registerAgent(t, "edge-b", 100, 90)  // backup
	h.registerAgent(t, "edge-c", 100, 80)  // live spare
	if _, err := h.o.CreatePool(h.ctx, ratePool(902, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	// Primary AND backup die, but edge-c is alive → NOT double death: provision
	// edge-c as a fresh backup, then promote it.
	dead["edge-a"] = true
	dead["edge-b"] = true
	if err := h.o.FailoverEdge(h.ctx, "edge-a"); err != nil {
		t.Fatalf("FailoverEdge: %v", err)
	}
	stored, ok := h.yb.getRec(902)
	if !ok || stored.Primary != "edge-c" {
		t.Errorf("must re-home onto the live spare edge-c, got %+v", stored)
	}
	if len(*events) != 0 {
		t.Errorf("a live spare means no double-death, got alarms %+v", *events)
	}
}

func TestCreatePoolSkipsDeadAgents(t *testing.T) {
	dead := map[model.EdgeID]bool{"edge-a": true}
	h, _ := setupDD(t, dead)
	h.registerAgent(t, "edge-a", 100, 100) // dead — must not be placed on
	h.registerAgent(t, "edge-b", 100, 90)  // live
	h.registerAgent(t, "edge-c", 100, 80)  // live
	rec, err := h.o.CreatePool(h.ctx, ratePool(903, "203.0.113.0/24"), 30)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Primary == "edge-a" || rec.Backup == "edge-a" {
		t.Errorf("dead agent must not be placed on: %+v", rec)
	}
}

// --- C-03: data-plane-ready gate before BGP switch ---
//
// The ready gate moved OFF the synchronous ProvisionBackup (which is now the async
// version-CAS pivot primitive) and ONTO the level-triggered reconciler: a freshly
// provisioned backup is StatusGated until its etcd applied-version catches up before a
// promote switches BGP onto it (先就位再切). That readiness sequencing is exercised by
// reconcile_test.go's TestReconcileProvisionGatePromoteRetire. Here we keep coverage
// that the manual-admin ProvisionBackup PLACES the fresh backup on the pivot.

func TestProvisionBackupPlacesFreshBackup(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)
	h.registerAgent(t, "edge-c", 100, 80) // spare
	if _, err := h.o.CreatePool(h.ctx, ratePool(1000, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	// Promote so the pool loses its backup, then provision a fresh one elsewhere
	// (excluding edge-a, the retired old primary) → edge-c is placed as the new backup.
	if _, err := h.o.PromoteBackup(h.ctx, 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := h.o.ProvisionBackup(h.ctx, 1000, "edge-a"); err != nil {
		t.Fatalf("ProvisionBackup: %v", err)
	}
	rec, _ := h.yb.getRec(1000)
	if rec.Backup != "edge-c" {
		t.Errorf("backup = %s, want edge-c", rec.Backup)
	}
	// Optimistic placement writes no etcd ledger reservation.
	if free, _ := h.led.Remaining(h.ctx, "edge-c"); free != 80 {
		t.Errorf("edge-c ledger must be untouched by optimistic placement, got %d want 80", free)
	}
}

func TestProvisionBackupNoSpareKeepsPrimaryOnly(t *testing.T) {
	h := setup(t)
	h.registerAgent(t, "edge-a", 100, 100)
	h.registerAgent(t, "edge-b", 100, 90)
	if _, err := h.o.CreatePool(h.ctx, ratePool(1001, "203.0.113.0/24"), 30); err != nil {
		t.Fatal(err)
	}
	if _, err := h.o.PromoteBackup(h.ctx, 1001); err != nil {
		t.Fatal(err)
	}
	// No spare left (edge-a retired/excluded, edge-b is now primary) → ErrNoPlacement,
	// the pool runs primary-only until capacity frees.
	if _, err := h.o.ProvisionBackup(h.ctx, 1001, "edge-a"); err == nil || !errors.Is(err, ErrNoPlacement) {
		t.Fatalf("expected ErrNoPlacement with no spare, got %v", err)
	}
	rec, _ := h.yb.getRec(1001)
	if rec.Backup != "" {
		t.Errorf("no spare must leave no backup, got %s", rec.Backup)
	}
}
