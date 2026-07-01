package lossmon

import (
	"context"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/fivetime/sbw-contract/model"
)

type clk struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clk) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clk) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type alertRec struct {
	edge     model.EdgeID
	member   netip.Prefix
	dir      model.Direction
	loss     uint16
	reason   string
	degraded bool
}

type recorder struct {
	mu       sync.Mutex
	alerts   []alertRec
	migrates []alertRec // reuse shape (loss + key) for migrate calls
}

func (r *recorder) onAlert(edge model.EdgeID, m netip.Prefix, dir model.Direction, loss uint16, reason string, degraded bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.alerts = append(r.alerts, alertRec{edge, m, dir, loss, reason, degraded})
}

func (r *recorder) onMigrate(_ context.Context, edge model.EdgeID, m netip.Prefix, dir model.Direction, loss uint16) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.migrates = append(r.migrates, alertRec{edge: edge, member: m, dir: dir, loss: loss})
}

func (r *recorder) alertCount() int   { r.mu.Lock(); defer r.mu.Unlock(); return len(r.alerts) }
func (r *recorder) migrateCount() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.migrates) }

var m1 = netip.MustParsePrefix("172.16.0.1/32")

// newMon builds a monitor: alert 15% (1500bps), migrate 30% (3000bps), sustain 5m.
func newMon() (*Monitor, *recorder, *clk) {
	r := &recorder{}
	c := &clk{t: time.Unix(1_700_000_000, 0)}
	m := New(1500, 3000, 5*time.Minute, r.onMigrate,
		WithClock(c.now), WithAlert(r.onAlert))
	return m, r, c
}

func s(member netip.Prefix, dir model.Direction, bps uint16, reason string) Sample {
	return Sample{Member: member, Dir: dir, LossBps: bps, Reason: reason}
}

// Loss between the alert and migrate thresholds → exactly one degraded alert, never a
// migrate no matter how long it persists.
func TestAlertNoMigrateBetweenThresholds(t *testing.T) {
	m, r, c := newMon()
	ctx := context.Background()

	m.Report("l1", []Sample{s(m1, model.DirectionIngress, 2000, "ip4-input")}) // 20%
	m.Tick(ctx)
	if r.alertCount() != 1 {
		t.Fatalf("crossing alert threshold must emit one degraded, got %d", r.alertCount())
	}
	if got := r.alerts[0]; !got.degraded || got.loss != 2000 || got.reason != "ip4-input" {
		t.Fatalf("bad alert %+v", got)
	}
	// Persist a long time — still below migrate threshold → no migrate, no re-alert.
	for i := 0; i < 5; i++ {
		c.add(2 * time.Minute)
		m.Report("l1", []Sample{s(m1, model.DirectionIngress, 2000, "ip4-input")})
		m.Tick(ctx)
	}
	if r.alertCount() != 1 {
		t.Fatalf("sustained same-level loss must not re-alert, got %d", r.alertCount())
	}
	if r.migrateCount() != 0 {
		t.Fatalf("loss under migrate threshold must never migrate, got %d", r.migrateCount())
	}
}

// Loss over the migrate threshold fires a migrate ONLY after the sustain window, and
// exactly once.
func TestMigrateAfterSustain(t *testing.T) {
	m, r, c := newMon()
	ctx := context.Background()

	m.Report("l1", []Sample{s(m1, model.DirectionEgress, 4000, "rx-miss")}) // 40%
	m.Tick(ctx)
	if r.migrateCount() != 0 {
		t.Fatalf("migrate must wait for the sustain window, got %d", r.migrateCount())
	}
	if r.alertCount() != 1 || !r.alerts[0].degraded {
		t.Fatalf("over-migrate loss must also alert degraded, got %+v", r.alerts)
	}
	// Just under the sustain window — still no migrate.
	c.add(4 * time.Minute)
	m.Report("l1", []Sample{s(m1, model.DirectionEgress, 4000, "rx-miss")})
	m.Tick(ctx)
	if r.migrateCount() != 0 {
		t.Fatalf("migrate fired before sustain elapsed, got %d", r.migrateCount())
	}
	// Past the sustain window → migrate, once.
	c.add(2 * time.Minute)
	m.Report("l1", []Sample{s(m1, model.DirectionEgress, 4000, "rx-miss")})
	m.Tick(ctx)
	if r.migrateCount() != 1 {
		t.Fatalf("sustained over-migrate loss must migrate once, got %d", r.migrateCount())
	}
	if got := r.migrates[0]; got.edge != "l1" || got.member != m1 || got.dir != model.DirectionEgress || got.loss != 4000 {
		t.Fatalf("bad migrate %+v", got)
	}
	// Keep reporting high — must NOT re-migrate (already fired, not yet recovered).
	c.add(10 * time.Minute)
	m.Report("l1", []Sample{s(m1, model.DirectionEgress, 4000, "rx-miss")})
	m.Tick(ctx)
	if r.migrateCount() != 1 {
		t.Fatalf("migrate must not re-fire without recovery, got %d", r.migrateCount())
	}
}

// A dip under the migrate threshold BEFORE the sustain window elapses resets the window
// — the spell must be CONTINUOUS.
func TestSustainResetsOnDip(t *testing.T) {
	m, r, c := newMon()
	ctx := context.Background()

	m.Report("l1", []Sample{s(m1, model.DirectionIngress, 4000, "")}) // 40%
	m.Tick(ctx)
	c.add(4 * time.Minute)
	// Dip to 20% (still degraded/alerting, but under migrate) → sustain resets.
	m.Report("l1", []Sample{s(m1, model.DirectionIngress, 2000, "")})
	m.Tick(ctx)
	c.add(2 * time.Minute) // 6m since first over-migrate, but the window restarted
	m.Report("l1", []Sample{s(m1, model.DirectionIngress, 4000, "")})
	m.Tick(ctx)
	if r.migrateCount() != 0 {
		t.Fatalf("a dip under migrate must reset the sustain window, got %d", r.migrateCount())
	}
}

// Recovery below the alert threshold emits a recovered alert and re-arms migrate.
func TestRecoveryEmitsAndReArms(t *testing.T) {
	m, r, c := newMon()
	ctx := context.Background()

	// Degrade + migrate.
	m.Report("l1", []Sample{s(m1, model.DirectionIngress, 5000, "")})
	m.Tick(ctx)
	c.add(6 * time.Minute)
	m.Report("l1", []Sample{s(m1, model.DirectionIngress, 5000, "")})
	m.Tick(ctx)
	if r.migrateCount() != 1 {
		t.Fatalf("setup: expected one migrate, got %d", r.migrateCount())
	}
	// Recover: member drops below the watermark → ABSENT from the snapshot.
	m.Report("l1", nil)
	m.Tick(ctx)
	// Last alert must be a recovery (degraded=false).
	last := r.alerts[len(r.alerts)-1]
	if last.degraded {
		t.Fatalf("recovery must emit a non-degraded alert, got %+v", last)
	}
	// Degrade again over migrate + sustain → a SECOND migrate (re-armed).
	c.add(1 * time.Minute)
	m.Report("l1", []Sample{s(m1, model.DirectionIngress, 5000, "")})
	m.Tick(ctx)
	c.add(6 * time.Minute)
	m.Report("l1", []Sample{s(m1, model.DirectionIngress, 5000, "")})
	m.Tick(ctx)
	if r.migrateCount() != 2 {
		t.Fatalf("a recovered-then-degraded member must migrate again, got %d", r.migrateCount())
	}
}

// Absence of a member from an edge's snapshot recovers ONLY that edge's members; a
// different edge's series is untouched (per-edge reconcile).
func TestPerEdgeReconcile(t *testing.T) {
	m, r, _ := newMon()
	ctx := context.Background()

	m.Report("l1", []Sample{s(m1, model.DirectionIngress, 2000, "")})
	m.Report("l2", []Sample{s(m1, model.DirectionIngress, 2000, "")})
	m.Tick(ctx)
	if r.alertCount() != 2 {
		t.Fatalf("two edges degraded → two alerts, got %d", r.alertCount())
	}
	// l1 recovers (empty snapshot); l2 unchanged (no new report).
	m.Report("l1", nil)
	m.Tick(ctx)
	// Exactly one recovery (l1); l2 stays degraded (no re-alert).
	recovered := 0
	for _, a := range r.alerts {
		if !a.degraded {
			recovered++
		}
	}
	if recovered != 1 {
		t.Fatalf("only l1 should recover, got %d recoveries", recovered)
	}
}

// Forget drops an edge's series so a stale over-migrate spell can't fire post-failover.
func TestForgetDropsEdge(t *testing.T) {
	m, r, c := newMon()
	ctx := context.Background()

	m.Report("l1", []Sample{s(m1, model.DirectionIngress, 5000, "")})
	m.Tick(ctx)
	c.add(4 * time.Minute)
	m.Forget("l1") // edge failed over
	c.add(4 * time.Minute)
	m.Tick(ctx) // no report, no series → nothing fires
	if r.migrateCount() != 0 {
		t.Fatalf("a forgotten edge must not migrate, got %d", r.migrateCount())
	}
}

// A recovered series is GC'd after the stale TTL (memory reclaim, no behavioural effect).
func TestRecoveredSeriesGCd(t *testing.T) {
	r := &recorder{}
	c := &clk{t: time.Unix(1_700_000_000, 0)}
	m := New(1500, 3000, 5*time.Minute, r.onMigrate,
		WithClock(c.now), WithAlert(r.onAlert), WithStaleTTL(time.Minute))
	ctx := context.Background()

	m.Report("l1", []Sample{s(m1, model.DirectionIngress, 2000, "")})
	m.Tick(ctx)
	m.Report("l1", nil) // recover
	c.add(2 * time.Minute)
	m.Tick(ctx) // emits recovery + GCs (stale past 1m)
	m.mu.Lock()
	n := len(m.series)
	m.mu.Unlock()
	if n != 0 {
		t.Fatalf("recovered+stale series must be GC'd, %d left", n)
	}
}
