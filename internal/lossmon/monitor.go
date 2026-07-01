// Package lossmon holds the SERVER side of the §4.2.5 per-member forwarding-loss
// policy. The agent measures each member's per-direction forwarding loss (flowprobe)
// and reports only the members over its watermark in HealthReport.MemberLoss; this
// monitor keeps the per-(edge,member,direction) sustain window and decides the two
// outcomes the agent deliberately does NOT: an ALERT on crossing the alert threshold
// (>15% by default → a Redpanda edge-forwarding-degraded event for human triage) and
// a per-pool MIGRATE when loss stays over the migrate threshold (>30%) for the sustain
// window (5m). Migration targets the affected POOL (resolve member→pool→promote its
// backup), not the whole edge — a lossy path to one member must not evacuate an edge
// that forwards everyone else fine.
//
// It is a pure state machine (clock + callbacks, no store/etcd), mirroring the
// liveness Monitor: Report folds an edge's latest loss snapshot into per-series state,
// and Tick fires the alert transitions + sustained migrates outside the lock. The
// callbacks carry the resolution (member→pool) and the side effects (emit / promote).
package lossmon

import (
	"context"
	"log/slog"
	"net/netip"
	"sync"
	"time"

	"github.com/fivetime/sbw-contract/model"
)

// Sample is one member's per-direction loss reading, taken from a
// HealthReport.MemberLoss entry. The agent emits only members above its report
// watermark, so a member ABSENT from an edge's snapshot is proven healthy (below
// watermark) — Report treats absence as recovery.
type Sample struct {
	Member  netip.Prefix
	Dir     model.Direction
	LossBps uint16 // basis points 0..10000 (network loss, policer-violate excluded)
	Reason  string // dominant VPP drop reason (top_drop_reason), for the alert
}

// key identifies one tracked (edge, member, direction) loss series.
type key struct {
	edge   model.EdgeID
	member netip.Prefix
	dir    model.Direction
}

type series struct {
	lossBps      uint16
	reason       string
	sustainSince time.Time // when loss first went >= migrateBps continuously; zero = below
	alerting     bool      // last-emitted alert level (>= alertBps) — edge-triggered transition
	migrated     bool      // migrate already fired; re-armed when loss recovers below alert
	lastSeen     time.Time // last time this member was in a snapshot (degraded); GC anchor
}

// MigrateFunc migrates the pool that owns a member whose loss stayed over the migrate
// threshold for the sustain window (resolve member→pool→promote its backup). Runs
// outside the monitor lock (it does a store lookup + a pivot CAS).
type MigrateFunc func(ctx context.Context, edge model.EdgeID, member netip.Prefix, dir model.Direction, lossBps uint16)

// AlertFunc fires on an alert-level TRANSITION for one member: degraded=true when loss
// first crosses the alert threshold, false when it recovers back under it. Edge-
// triggered (once per transition, not per tick). Runs outside the monitor lock.
type AlertFunc func(edge model.EdgeID, member netip.Prefix, dir model.Direction, lossBps uint16, reason string, degraded bool)

// Monitor tracks per-member loss and fires alert/migrate policy.
type Monitor struct {
	mu             sync.Mutex
	alertBps       uint16        // >= this (basis points) → degraded alert
	migrateBps     uint16        // >= this, sustained migrateSustain → per-pool migrate
	migrateSustain time.Duration // how long loss must stay >= migrateBps before migrating
	staleTTL       time.Duration // GC a recovered series this long after its last degraded snapshot
	now            func() time.Time
	series         map[key]*series
	onMigrate      MigrateFunc
	onAlert        AlertFunc
	log            *slog.Logger
}

// Option configures a Monitor.
type Option func(*Monitor)

// WithClock overrides the time source (tests).
func WithClock(now func() time.Time) Option { return func(m *Monitor) { m.now = now } }

// WithAlert sets the alert-transition callback (→ emit edge-forwarding-degraded).
func WithAlert(fn AlertFunc) Option { return func(m *Monitor) { m.onAlert = fn } }

// WithStaleTTL overrides how long a recovered series is retained before GC. 0 → the
// migrate sustain window.
func WithStaleTTL(d time.Duration) Option { return func(m *Monitor) { m.staleTTL = d } }

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option { return func(m *Monitor) { m.log = l } }

// New builds a loss Monitor. alertBps/migrateBps are basis points (0..10000);
// migrateSustain is how long loss must stay over migrateBps before a migrate fires.
// onMigrate is the per-pool migrate action (nil is tolerated for tests).
func New(alertBps, migrateBps uint16, migrateSustain time.Duration, onMigrate MigrateFunc, opts ...Option) *Monitor {
	m := &Monitor{
		alertBps:       alertBps,
		migrateBps:     migrateBps,
		migrateSustain: migrateSustain,
		now:            time.Now,
		series:         map[key]*series{},
		onMigrate:      onMigrate,
		log:            slog.Default(),
	}
	for _, o := range opts {
		o(m)
	}
	if m.staleTTL <= 0 {
		m.staleTTL = migrateSustain
	}
	return m
}

// Report folds one edge's latest loss snapshot into the per-series state. samples is
// the FULL set of that edge's members currently over the agent watermark; any member
// previously tracked for this edge but ABSENT from samples has dropped below the
// watermark → recovered (loss reset to 0, sustain window cleared). Firing happens on
// Tick, not here (keeps the report hot path lock-cheap and off the store).
func (m *Monitor) Report(edge model.EdgeID, samples []Sample) {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()

	seen := make(map[key]bool, len(samples))
	for _, sm := range samples {
		k := key{edge: edge, member: sm.Member, dir: sm.Dir}
		seen[k] = true
		s := m.series[k]
		if s == nil {
			s = &series{}
			m.series[k] = s
		}
		s.lossBps = sm.LossBps
		s.reason = sm.Reason
		s.lastSeen = now
		if sm.LossBps >= m.migrateBps {
			if s.sustainSince.IsZero() {
				s.sustainSince = now // start of a continuous over-migrate spell
			}
		} else {
			s.sustainSince = time.Time{} // dipped under migrate → the sustain window resets
		}
	}
	// Members tracked for THIS edge but absent from the snapshot are below the agent
	// watermark now → healthy. Zero their loss so Tick emits the recovery + GCs them;
	// leave lastSeen at its last degraded value so the staleTTL counts from there.
	for k, s := range m.series {
		if k.edge != edge || seen[k] {
			continue
		}
		s.lossBps = 0
		s.sustainSince = time.Time{}
	}
}

// Forget drops all series for an edge (called when the edge fails over / decommissions
// — its members moved, so a stale sustain window must not fire a migrate against an
// edge that no longer homes them).
func (m *Monitor) Forget(edge model.EdgeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.series {
		if k.edge == edge {
			delete(m.series, k)
		}
	}
}

type alertAction struct {
	k        key
	loss     uint16
	reason   string
	degraded bool
}

type migrateAction struct {
	k    key
	loss uint16
}

// Tick evaluates every tracked series: emits an alert on an alert-level transition,
// fires a per-pool migrate for members over the migrate threshold past the sustain
// window (once, until they recover), and GCs recovered/stale series. Callbacks run
// outside the lock.
func (m *Monitor) Tick(ctx context.Context) {
	now := m.now()
	m.mu.Lock()
	var alerts []alertAction
	var migrates []migrateAction
	for k, s := range m.series {
		degraded := s.lossBps >= m.alertBps
		if degraded != s.alerting {
			alerts = append(alerts, alertAction{k: k, loss: s.lossBps, reason: s.reason, degraded: degraded})
			s.alerting = degraded
		}
		if s.lossBps >= m.migrateBps && !s.sustainSince.IsZero() &&
			now.Sub(s.sustainSince) >= m.migrateSustain && !s.migrated {
			migrates = append(migrates, migrateAction{k: k, loss: s.lossBps})
			s.migrated = true // don't re-fire until it recovers below the alert threshold
		}
		if s.lossBps < m.alertBps {
			s.migrated = false // recovered → re-arm for a future degradation
			if !s.alerting && now.Sub(s.lastSeen) >= m.staleTTL {
				delete(m.series, k) // healthy and stale → reclaim
			}
		}
	}
	m.mu.Unlock()

	for _, a := range alerts {
		if m.onAlert != nil {
			m.onAlert(a.k.edge, a.k.member, a.k.dir, a.loss, a.reason, a.degraded)
		}
	}
	for _, mg := range migrates {
		if m.onMigrate != nil {
			m.onMigrate(ctx, mg.k.edge, mg.k.member, mg.k.dir, mg.loss)
		}
	}
}
