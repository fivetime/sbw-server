package liveness

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/fivetime/sbw-contract/model"
)

type recorder struct {
	mu      sync.Mutex
	failed  []model.EdgeID
	revived []model.EdgeID
}

func (r *recorder) onFailover(_ context.Context, e model.EdgeID) {
	r.mu.Lock()
	r.failed = append(r.failed, e)
	r.mu.Unlock()
}

func (r *recorder) onRevive(_ context.Context, e model.EdgeID) {
	r.mu.Lock()
	r.revived = append(r.revived, e)
	r.mu.Unlock()
}
func (r *recorder) failedCount() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.failed) }

type clk struct{ t time.Time }

func (c *clk) now() time.Time      { return c.t }
func (c *clk) add(d time.Duration) { c.t = c.t.Add(d) }

func newMon(grace time.Duration) (*Monitor, *recorder, *clk) {
	r := &recorder{}
	c := &clk{t: time.Unix(1_700_000_000, 0)}
	m := New(grace, r.onFailover, WithClock(c.now), WithRevive(r.onRevive))
	return m, r, c
}

func TestHeartbeatGraceFailover(t *testing.T) {
	m, r, c := newMon(30 * time.Second)
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a") // last seen now

	// Within grace: no failover.
	c.add(20 * time.Second)
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("within grace must not fail over, got %v", dead)
	}
	// A fresh heartbeat resets the clock.
	m.Heartbeat(ctx, "edge-a")
	c.add(20 * time.Second)
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("heartbeat should have reset grace, got %v", dead)
	}
	// Now go silent past grace.
	c.add(31 * time.Second)
	dead := m.Tick(ctx)
	if len(dead) != 1 || dead[0] != "edge-a" {
		t.Fatalf("stale edge must fail over, got %v", dead)
	}
	// Idempotent: does not re-fire while still failed.
	c.add(60 * time.Second)
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("must not re-fire an already-failed edge, got %v", dead)
	}
	if r.failedCount() != 1 {
		t.Errorf("failover fired %d times, want 1", r.failedCount())
	}
}

// TestMeteringStaleNotify proves the new pure NOTIFICATION reports each tracked edge's
// CURRENT heartbeat-stale level on every Tick (fresh while within grace, stale once
// past it) WITHOUT changing the failover behaviour — the consumer does the transition
// tracking, so the notify reports the raw level every tick.
func TestMeteringStaleNotify(t *testing.T) {
	r := &recorder{}
	c := &clk{t: time.Unix(1_700_000_000, 0)}
	type sample struct {
		edge  model.EdgeID
		stale bool
	}
	var mu sync.Mutex
	var samples []sample
	m := New(30*time.Second, r.onFailover, WithClock(c.now), WithRevive(r.onRevive),
		WithMeteringStaleNotify(func(e model.EdgeID, stale bool) {
			mu.Lock()
			samples = append(samples, sample{e, stale})
			mu.Unlock()
		}))
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")

	// Within grace → notify reports fresh (false).
	c.add(20 * time.Second)
	m.Tick(ctx)
	mu.Lock()
	last := samples[len(samples)-1]
	mu.Unlock()
	if last.edge != "edge-a" || last.stale {
		t.Fatalf("within grace notify should be fresh, got %+v", last)
	}

	// Past grace → notify reports stale (true) AND the edge fails over (logic unchanged).
	c.add(31 * time.Second)
	dead := m.Tick(ctx)
	if len(dead) != 1 || dead[0] != "edge-a" {
		t.Fatalf("stale edge must still fail over (logic unchanged), got %v", dead)
	}
	mu.Lock()
	last = samples[len(samples)-1]
	mu.Unlock()
	if !last.stale {
		t.Fatalf("past grace notify should be stale, got %+v", last)
	}
}

func TestHardDownImmediateNoGrace(t *testing.T) {
	m, _, _ := newMon(time.Hour) // huge grace — heartbeat path won't trigger
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")

	m.HardDown("edge-a") // tap PeerDown
	// No clock advance: hard-down fails over immediately on the next tick.
	dead := m.Tick(ctx)
	if len(dead) != 1 || dead[0] != "edge-a" {
		t.Fatalf("hard-down must fail over without grace, got %v", dead)
	}
}

// TestHardDebounceDampsFlap: with a hold-down, a tap PeerDown that clears (a
// recovering edge's flap) within the window does NOT fail over; one that persists
// past it does. Mirrors the soft-debounce, for the L-recovery flap.
func TestHardDebounceDampsFlap(t *testing.T) {
	r := &recorder{}
	c := &clk{t: time.Unix(1_700_000_000, 0)}
	m := New(time.Hour, r.onFailover, WithClock(c.now), WithRevive(r.onRevive),
		WithHardDebounce(3*time.Second))
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")

	// PeerDown, but the 3s hold-down hasn't elapsed: no failover.
	m.HardDown("edge-a")
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("within hard-debounce must not fail over, got %v", dead)
	}
	c.add(2 * time.Second)
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("still within hold-down must not fail over, got %v", dead)
	}
	// The tap recovers (flap) before the hold-down elapses → cancel.
	m.HardUp("edge-a")
	c.add(5 * time.Second)
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("recovered flap must not fail over, got %v", dead)
	}

	// A fresh PeerDown that PERSISTS past the hold-down fires.
	m.HardDown("edge-a")
	m.Tick(ctx) // arms the hold-down (hardSince = now)
	c.add(4 * time.Second)
	dead := m.Tick(ctx)
	if len(dead) != 1 || dead[0] != "edge-a" {
		t.Fatalf("persistent hard-down past hold-down must fail over, got %v", dead)
	}
	if r.failedCount() != 1 {
		t.Errorf("failover fired %d times, want 1 (flap damped)", r.failedCount())
	}
}

// L-03 churn fix: a (re)started replica's first-convergence grace must suppress
// hard-death — its tap is still establishing, so a quorum of "down" votes during
// the window is "not connected yet", not node death. The standing quorum fires
// only once past the grace.
func TestStartupGraceSuppressesHardDeath(t *testing.T) {
	r := &recorder{}
	c := &clk{t: time.Unix(1_700_000_000, 0)}
	m := New(time.Hour, r.onFailover, WithClock(c.now),
		WithQuorum(2), WithSelfID("ctrl-a"), WithStartupGrace(30*time.Second))
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")

	// Quorum reached: our own vote + a peer coverer's.
	m.HardDown("edge-a")
	m.Vote("edge-a", "ctrl-b", true)

	if m.Ready() {
		t.Fatal("must not be Ready within startup grace")
	}
	c.add(20 * time.Second) // still within the 30s grace
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("within startup grace, quorum votes must not fail over, got %v", dead)
	}
	if m.IsDead("edge-a") {
		t.Fatal("IsDead must be false within startup grace")
	}

	c.add(11 * time.Second) // total 31s > 30s grace
	if !m.Ready() {
		t.Fatal("must be Ready past startup grace")
	}
	dead := m.Tick(ctx)
	if len(dead) != 1 || dead[0] != "edge-a" {
		t.Fatalf("past startup grace, corroborated hard-death must fail over, got %v", dead)
	}
}

// A healthy edge that flapped during the grace (vote later cleared by PeerUp)
// must NOT fail over once the grace passes — the cleared vote leaves no quorum.
func TestStartupGraceClearedVoteDoesNotFire(t *testing.T) {
	r := &recorder{}
	c := &clk{t: time.Unix(1_700_000_000, 0)}
	m := New(time.Hour, r.onFailover, WithClock(c.now),
		WithQuorum(2), WithSelfID("ctrl-a"), WithStartupGrace(30*time.Second))
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")

	m.HardDown("edge-a")             // spurious during convergence
	m.Vote("edge-a", "ctrl-b", true) // transient peer flap
	c.add(10 * time.Second)
	// Tap converges: both votes clear (PeerUp / peer PeerUp).
	m.HardUp("edge-a")
	m.Vote("edge-a", "ctrl-b", false)

	c.add(30 * time.Second) // past grace
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("cleared votes must not fail over past grace, got %v", dead)
	}
}

// K=2 e2e fix: a coverer must not heartbeat-fail an edge whose agent reports to a
// PEER coverer (it's silent here by design). Once the agent homes here and then
// goes silent, the heartbeat-stale path fires normally.
func TestHeartbeatStaleGatedByReporting(t *testing.T) {
	r := &recorder{}
	c := &clk{t: time.Unix(1_700_000_000, 0)}
	reports := map[model.EdgeID]bool{}
	m := New(30*time.Second, r.onFailover, WithClock(c.now),
		WithReporting(func(e model.EdgeID) bool { return reports[e] }))
	ctx := context.Background()

	// Tap-covered here, but the agent homes on a peer coverer (not reporting to me).
	m.Alive(ctx, "edge-a")
	c.add(60 * time.Second) // far past grace, yet no heartbeat is expected here
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("non-reporting edge must not be heartbeat-failed, got %v", dead)
	}

	// It re-homes here (now reports to me) then goes silent → heartbeat-stale fires.
	reports["edge-a"] = true
	m.Heartbeat(ctx, "edge-a")
	c.add(31 * time.Second)
	dead := m.Tick(ctx)
	if len(dead) != 1 || dead[0] != "edge-a" {
		t.Fatalf("reporting edge gone silent must fail over, got %v", dead)
	}
}

func TestRevivalIsNonPreemptive(t *testing.T) {
	m, r, c := newMon(30 * time.Second)
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")
	c.add(31 * time.Second)
	if dead := m.Tick(ctx); len(dead) != 1 {
		t.Fatalf("expected failover, got %v", dead)
	}
	// Edge comes back: revive fires, NOT another failover.
	m.Alive(ctx, "edge-a")
	if len(r.revived) != 1 || r.revived[0] != "edge-a" {
		t.Fatalf("revival must fire onRevive, got %v", r.revived)
	}
	// Re-armed: a subsequent death fails over again.
	c.add(31 * time.Second)
	if dead := m.Tick(ctx); len(dead) != 1 {
		t.Fatalf("re-armed edge must fail over again, got %v", dead)
	}
	if r.failedCount() != 2 {
		t.Errorf("want 2 failovers across death→revive→death, got %d", r.failedCount())
	}
}

func TestHardUpClearsBeforeTick(t *testing.T) {
	m, r, _ := newMon(time.Hour)
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")
	m.HardDown("edge-a")
	m.HardUp("edge-a") // session flapped back before the tick
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("hard-up before tick must cancel failover, got %v", dead)
	}
	if r.failedCount() != 0 {
		t.Errorf("no failover should have fired, got %d", r.failedCount())
	}
}

func TestForgetStopsTracking(t *testing.T) {
	m, _, c := newMon(30 * time.Second)
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")
	m.Forget("edge-a") // planned decommission
	c.add(60 * time.Second)
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("forgotten edge must not fail over, got %v", dead)
	}
}

func TestGraceZeroDisablesHeartbeatPath(t *testing.T) {
	m, _, c := newMon(0) // only hard-down triggers
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")
	c.add(24 * time.Hour)
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("grace=0 must disable heartbeat-loss failover, got %v", dead)
	}
	m.HardDown("edge-a")
	if dead := m.Tick(ctx); len(dead) != 1 {
		t.Fatalf("hard-down must still trigger with grace=0, got %v", dead)
	}
}

func TestSoftDeathNeedsBothSignals(t *testing.T) {
	r := &recorder{}
	c := &clk{t: time.Unix(1_700_000_000, 0)}
	m := New(time.Hour, r.onFailover, WithClock(c.now), WithRevive(r.onRevive), WithSoftDebounce(10*time.Second))
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")

	// Canary anomaly ALONE — no failover even after a long time.
	m.CanaryDown("edge-a")
	c.add(time.Minute)
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("canary anomaly alone must not fail over, got %v", dead)
	}
	// Agent reports data-plane death too → conjunction starts; not yet past debounce.
	m.Health("edge-a", true)
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("conjunction must debounce before firing, got %v", dead)
	}
	// Sustained past the soft debounce → soft death → failover.
	c.add(11 * time.Second)
	if dead := m.Tick(ctx); len(dead) != 1 || dead[0] != "edge-a" {
		t.Fatalf("sustained soft-death conjunction must fail over, got %v", dead)
	}
}

func TestSoftDeathResetsWhenSignalClears(t *testing.T) {
	r := &recorder{}
	c := &clk{t: time.Unix(1_700_000_000, 0)}
	m := New(time.Hour, r.onFailover, WithClock(c.now), WithSoftDebounce(10*time.Second))
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")

	// Conjunction begins.
	m.CanaryDown("edge-a")
	m.Health("edge-a", true)
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatal("debounce not elapsed yet")
	}
	// Canary returns before the debounce elapses → conjunction broken → reset.
	c.add(5 * time.Second)
	m.CanaryUp("edge-a")
	c.add(20 * time.Second) // would have exceeded debounce had it not reset
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("a cleared signal must reset the debounce, got %v", dead)
	}
	if r.failedCount() != 0 {
		t.Errorf("no failover should have fired, got %d", r.failedCount())
	}
}

func TestSoftDeathHealthRecoveryClears(t *testing.T) {
	r := &recorder{}
	c := &clk{t: time.Unix(1_700_000_000, 0)}
	m := New(time.Hour, r.onFailover, WithClock(c.now), WithSoftDebounce(10*time.Second))
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")
	m.CanaryDown("edge-a")
	m.Health("edge-a", true)
	c.add(5 * time.Second)
	// Agent recovers (reports healthy) → conjunction broken.
	m.Health("edge-a", false)
	c.add(20 * time.Second)
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("health recovery must clear soft-death, got %v", dead)
	}
}

// newQuorumMon builds a monitor with a coverer quorum and a self id (L-03).
func newQuorumMon(quorum int, selfID string) (*Monitor, *recorder, *clk) {
	r := &recorder{}
	c := &clk{t: time.Unix(1_700_000_000, 0)}
	m := New(time.Hour, r.onFailover, WithClock(c.now), WithRevive(r.onRevive),
		WithQuorum(quorum), WithSelfID(selfID))
	return m, r, c
}

// Under quorum=2, a single coverer's PeerDown (our own HardDown) must NOT fail
// over — it needs a second coverer to corroborate (cross-path, L-03).
func TestQuorumSingleVoteDoesNotFail(t *testing.T) {
	m, r, _ := newQuorumMon(2, "ctrl-a")
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")

	m.HardDown("edge-a") // only ctrl-a sees it down (one path)
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("single coverer vote must not reach quorum 2, got %v", dead)
	}
	if r.failedCount() != 0 {
		t.Errorf("no failover expected on one vote, got %d", r.failedCount())
	}
}

// A second coverer's vote reaches the quorum and fires.
func TestQuorumSecondVoteFires(t *testing.T) {
	m, r, _ := newQuorumMon(2, "ctrl-a")
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")

	m.HardDown("edge-a")             // ctrl-a (local)
	m.Vote("edge-a", "ctrl-b", true) // ctrl-b corroborates
	dead := m.Tick(ctx)
	if len(dead) != 1 || dead[0] != "edge-a" {
		t.Fatalf("two coverers down must reach quorum, got %v", dead)
	}
	if r.failedCount() != 1 {
		t.Errorf("want one failover, got %d", r.failedCount())
	}
}

// Our own id arriving via Vote is ignored (HardDown owns the local vote), so it
// cannot double-count to fake a quorum.
func TestQuorumIgnoresSelfVote(t *testing.T) {
	m, _, _ := newQuorumMon(2, "ctrl-a")
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")

	m.HardDown("edge-a")             // ctrl-a local vote
	m.Vote("edge-a", "ctrl-a", true) // same id via Vote — must not count twice
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("self id via Vote must not double-count to quorum, got %v", dead)
	}
}

// A coverer clearing its vote (PeerUp) drops below quorum before the tick.
func TestQuorumVoteClearedDropsBelow(t *testing.T) {
	m, r, _ := newQuorumMon(2, "ctrl-a")
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")

	m.HardDown("edge-a")
	m.Vote("edge-a", "ctrl-b", true)
	m.Vote("edge-a", "ctrl-b", false) // ctrl-b's path recovered before tick
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("dropping below quorum must cancel failover, got %v", dead)
	}
	if r.failedCount() != 0 {
		t.Errorf("no failover expected, got %d", r.failedCount())
	}
}

// IsDead reflects the quorum too: dead only once corroborated.
func TestQuorumIsDead(t *testing.T) {
	m, _, _ := newQuorumMon(2, "ctrl-a")
	ctx := context.Background()
	m.Heartbeat(ctx, "edge-a")
	m.HardDown("edge-a")
	if m.IsDead("edge-a") {
		t.Error("one vote must not make IsDead true under quorum 2")
	}
	m.Vote("edge-a", "ctrl-b", true)
	if !m.IsDead("edge-a") {
		t.Error("two votes must make IsDead true")
	}
}

// TestDeathNotifyMethod proves WithDeathNotify reports the death METHOD that fired:
// heartbeat-stale on a silent agent, and hard-quorum when a tap PeerDown is present.
// It fires once per newly-dead edge, before onFailover, and never changes the dead set.
func TestDeathNotifyMethod(t *testing.T) {
	r := &recorder{}
	c := &clk{t: time.Unix(1_700_000_000, 0)}
	var methods []string
	var mu sync.Mutex
	m := New(30*time.Second, r.onFailover, WithClock(c.now), WithRevive(r.onRevive),
		WithDeathNotify(func(_ model.EdgeID, method string) {
			mu.Lock()
			methods = append(methods, method)
			mu.Unlock()
		}))
	ctx := context.Background()

	// Heartbeat-stale: an agent that reported then went silent past grace.
	m.Heartbeat(ctx, "edge-a")
	c.add(31 * time.Second)
	if dead := m.Tick(ctx); len(dead) != 1 {
		t.Fatalf("expected 1 dead, got %v", dead)
	}
	mu.Lock()
	if len(methods) != 1 || methods[0] != "heartbeat-stale" {
		mu.Unlock()
		t.Fatalf("expected heartbeat-stale, got %v", methods)
	}
	mu.Unlock()

	// Hard-quorum: a tap PeerDown on a fresh edge (quorum=1 default) → immediate.
	m.HardDown("edge-b")
	if dead := m.Tick(ctx); len(dead) != 1 || dead[0] != "edge-b" {
		t.Fatalf("expected edge-b dead, got %v", dead)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(methods) != 2 || methods[1] != "hard-quorum" {
		t.Fatalf("expected hard-quorum, got %v", methods)
	}
}

// TestHeartbeatStaleRequiresEverReported: an edge the monitor has observed (registration /
// PeerUp) but that has NEVER sent an EdgeReport must NOT be heartbeat-stale-failed-over just
// because the grace elapsed — that is the L-10 cold-restart false-death (a still-converging
// edge whose first report is late). Once it reports, normal heartbeat-staleness resumes.
// Hard-death is intentionally unaffected (a fresh-edge tap quorum is immediate — see
// TestDeathNotifyMethod).
func TestHeartbeatStaleRequiresEverReported(t *testing.T) {
	r := &recorder{}
	c := &clk{t: time.Unix(1_700_000_000, 0)}
	m := New(30*time.Second, r.onFailover, WithClock(c.now), WithRevive(r.onRevive))
	ctx := context.Background()

	// Observed via registration/PeerUp, never a heartbeat; advance past the grace.
	m.Alive(ctx, "edge-x")
	c.add(31 * time.Second)
	if dead := m.Tick(ctx); len(dead) != 0 {
		t.Fatalf("never-reported edge must NOT be heartbeat-staled, got %v", dead)
	}

	// After its first report, heartbeat-staleness applies normally.
	m.Heartbeat(ctx, "edge-x")
	c.add(31 * time.Second)
	if dead := m.Tick(ctx); len(dead) != 1 || dead[0] != "edge-x" {
		t.Fatalf("reported-then-silent edge must be heartbeat-staled, got %v", dead)
	}
}
