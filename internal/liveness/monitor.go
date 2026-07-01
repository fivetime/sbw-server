// Package liveness is the controller's failover TRIGGER (controller §5.9/§6.5):
// the component that decides WHEN an edge is dead and fires the re-home action.
// It separates detection (this package, signal fusion + grace) from action (the
// orchestrator's FailoverEdge), so the policy of "when to fail over" is one
// testable place.
//
// Detection division (§6.5):
//   - HARD death (tap PeerDown): the BGP session is gone, R has already done
//     sub-second hemostasis (回正路, §6.5) — the controller promotes the backup
//     IMMEDIATELY (no grace; the death is unambiguous). Under sharding with
//     multihop BFD (DESIGN-liveness §9, L-03), a single coverer's PeerDown can be
//     a PATH fault rather than node death, so hard-death is gated on a QUORUM of
//     the edge's K coverers voting down (cross-path corroboration). Quorum=1
//     (the default, single-controller / single-hop) keeps the immediate behaviour.
//   - SOFT death (tap CanaryDown ∧ agent healthDead): the session is UP but the
//     data plane is dead — neither signal alone is trusted, so failover fires
//     only when BOTH hold past the soft debounce (§4.7/6.13), never on the canary
//     withdrawal alone.
//   - HEARTBEAT loss (no EdgeReport within grace): the agent process may have
//     died while the data plane keeps forwarding (a soft condition, §5.9) — the
//     controller waits a GRACE period (k8s Node NotReady analogue) before failing
//     over, so a quick agent restart does not cause an unnecessary switch.
//
// Revival (§5.8) is non-preemptive: when a failed-over edge reports/peers again
// the monitor fires onRevive (→ CleanupRevived: the edge self-cleans residual
// state) and resets, but never fails the pools BACK to it.
//
// The monitor is pure (injectable clock, callbacks) so the whole policy is unit
// tested without etcd, gRPC, or a RIB tap. Tick is driven by a ticker in the
// control plane; the signal methods are driven by the report path and the tap.
package liveness

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/fivetime/sbw-contract/model"
)

// ActionFunc re-homes (onFailover) or cleans up (onRevive) an edge. It runs
// outside the monitor lock; it may block (the monitor calls it from Tick or a
// signal method, both tolerant of latency).
type ActionFunc func(ctx context.Context, edge model.EdgeID)

// Monitor tracks per-edge liveness and fires failover/revive actions.
type Monitor struct {
	mu           sync.Mutex
	grace        time.Duration
	softDebounce time.Duration
	hardDebounce time.Duration // hold-down: hard-death quorum must persist this long before failover (0 = immediate); damps a recovering edge's tap flap
	restartGrace time.Duration // §4.2.4 vpp-gone hold-down: a crashed VPP may be relaunched+self-heal in place, so wait this long before soft-death failover (0 → 5s)
	startupGrace time.Duration // first-convergence grace (L-03 churn fix); see readyAt
	readyAt      time.Time     // hard-death verdicts/votes trusted only at/after this
	quorum       int           // coverer votes needed to judge hard-death (L-03); >=1
	selfID       string        // this replica's coverer id for its own HardDown vote
	now          func() time.Time
	edges        map[model.EdgeID]*state
	onFailover   ActionFunc
	onRevive     ActionFunc
	reporting    func(model.EdgeID) bool // is the edge's agent reporting to THIS replica? nil = assume yes
	// meteringStale, if set, is fired once per tracked edge on EACH Tick with the edge's
	// CURRENT heartbeat-stale level (does NOT change failover LOGIC — it is a pure
	// notification mirroring onFailover/onRevive; the consumer does the transition
	// tracking). Used by the control plane to emit the unsolicited metering-stale/
	// metering-resumed observability events. Fired outside the monitor lock.
	meteringStale func(edge model.EdgeID, stale bool)
	// onDeath, if set, is fired once for each edge NEWLY judged dead on a Tick, with the
	// DEATH METHOD that fired ("hard-quorum" | "heartbeat-stale" | "soft-death"), BEFORE
	// onFailover for that edge. It does NOT change failover LOGIC — it only carries which
	// rule tripped so the control plane can stamp the unsolicited edge-down + per-pool
	// failover events with the real death method (instead of a hardcoded "node-failure").
	// Fired outside the monitor lock, in the same per-edge order as onFailover.
	onDeath func(edge model.EdgeID, method string)
	log     *slog.Logger
}

type state struct {
	lastSeen     time.Time       // last heartbeat (EdgeReport) or alive signal
	hardVotes    map[string]bool // coverer ids that observed PeerDown (HARD death votes, L-03)
	canaryDown   bool            // tap: canary withdrawn while session up (SOFT signal)
	healthDead   bool            // agent reported HealthDataPlaneDown (SOFT signal)
	faultKind    model.FaultKind // §4.2 agent-typed edge fault (from HealthReport.FaultKind); routes the soft-death speed
	softSince    time.Time       // when the soft-death predicate (dataDead) began; zero = not currently
	hardSince    time.Time       // when the hard-death quorum was first reached (hold-down debounce); zero = not currently
	failed       bool            // failover already fired (don't re-fire until revived)
	everReported bool            // has sent ≥1 EdgeReport to THIS controller (L-10: never fail over an edge we've never heard from)
}

// ready reports whether this replica is past its first-convergence grace, i.e.
// its tap has had time to establish sessions so a missing session means "dead"
// rather than "not connected yet". Before readyAt, hard-death verdicts are NOT
// trusted (DESIGN-liveness §9, L-03 churn fix): a freshly (re)started coverer's
// taps are still coming up — treating their initial down-state as node death,
// and publishing those votes, falsely tips healthy edges over the quorum.
func (m *Monitor) ready() bool { return !m.now().Before(m.readyAt) }

// Ready is the exported, locked form of ready() — the control plane consults it
// to gate publishing this replica's death votes during the startup grace (so a
// reviving replica never poisons peer coverers' quorum, L-03).
func (m *Monitor) Ready() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ready()
}

// hardDead reports whether the edge has reached the coverer quorum for hard
// death (L-03): at least `quorum` distinct coverers observed its session down.
// Gated on ready() — during the startup grace a reviving replica's not-yet-up
// taps must not be read as node death (L-03 churn fix).
func (m *Monitor) hardDead(s *state) bool { return m.ready() && len(s.hardVotes) >= m.quorum }

// softDead reports the §4.7 soft-death conjunction: the tap sees a canary
// anomaly AND the agent reports its data plane dead. Either alone is not enough
// (avoids a false withdrawal from a tap flap or a single bad report).
func (s *state) softDead() bool { return s.canaryDown && s.healthDead }

// dataDead is the §4.2 fault-aware soft-death predicate that supersedes softDead()
// on the failover path. For a DETERMINATE agent-typed fault (vpp-gone / link-down /
// forwarding-broken) the agent's direct healthDead report is trustworthy on its own —
// no canary conjunction. vpp-gone / link-down don't withdraw the tap canary (it rides
// the kernel ctap, which survives both); forwarding-broken is AGENT-PROBE-CONFIRMED
// (§4.2.7 — K consecutive black-holed rounds) and, crucially, a fabric-facing black-hole
// need not sever the canary's path, so requiring the canary would blind exactly the case
// the probe exists to catch. For an unclassified fault (a pre-§4.2 agent reporting
// FaultNone) it keeps the §4.7 canary∧health conjunction so one bad report alone never
// fires. Backward-compatible: FaultNone → softDead(), the original behaviour.
func (s *state) dataDead() bool {
	switch s.faultKind {
	case model.FaultVPPGone, model.FaultLinkDown, model.FaultForwardingBroken:
		return s.healthDead
	default:
		return s.softDead()
	}
}

// debounceFor is the §4.2.4 per-fault-kind hold-down before soft-death failover.
// Determinate faults fire fast — link-down immediately (a dead uplink won't heal
// itself), forwarding-broken immediately (the agent already spent §4.2.7's K probe
// rounds confirming it — a second server debounce would just re-create the 39s the
// whole design removes), vpp-gone after a short restart grace (a crashed VPP may be
// relaunched by kubelet/supervisor and self-heal in place). FaultNone from a pre-§4.2
// agent keeps the full softDebounce, so there is no regression for untyped faults.
func (m *Monitor) debounceFor(f model.FaultKind) time.Duration {
	switch f {
	case model.FaultLinkDown, model.FaultForwardingBroken:
		return 0
	case model.FaultVPPGone:
		return m.restartGrace
	default:
		return m.softDebounce
	}
}

// Option configures a Monitor.
type Option func(*Monitor)

// WithClock overrides the time source (tests).
func WithClock(now func() time.Time) Option { return func(m *Monitor) { m.now = now } }

// WithRevive sets the revival action (→ orchestrator.CleanupRevived).
func WithRevive(fn ActionFunc) Option { return func(m *Monitor) { m.onRevive = fn } }

// WithMeteringStaleNotify sets a pure NOTIFICATION fired once per tracked edge on each
// Tick with the edge's current heartbeat-stale level (stale=true when its agent has
// gone silent past the grace, same gate the heartbeat-stale failover path reads). It
// does NOT alter failover logic — it only surfaces the level so the control plane can
// emit the unsolicited metering-stale / metering-resumed observability events
// (transition tracking lives in the consumer). nil = no notification.
func WithMeteringStaleNotify(f func(edge model.EdgeID, stale bool)) Option {
	return func(m *Monitor) { m.meteringStale = f }
}

// WithDeathNotify sets a NOTIFICATION fired once for each edge NEWLY judged dead on a
// Tick, carrying the death METHOD that fired ("hard-quorum" | "heartbeat-stale" |
// "soft-death"), BEFORE onFailover for that edge. It does NOT alter failover logic —
// it only surfaces which rule tripped so the control plane can stamp the unsolicited
// edge-down event and the per-pool failover events with the real death method. nil =
// no notification.
func WithDeathNotify(f func(edge model.EdgeID, method string)) Option {
	return func(m *Monitor) { m.onDeath = f }
}

// WithSoftDebounce sets how long the soft-death conjunction must persist before
// failover (§4.7 超去抖). 0 → defaults to grace (or 5s if grace is unset).
func WithSoftDebounce(d time.Duration) Option { return func(m *Monitor) { m.softDebounce = d } }

// WithHardDebounce sets a hold-down the hard-death quorum must persist before
// failover fires (0 = immediate, the default). Damps a recovering edge whose tap
// flaps up→down→up during its own re-convergence from re-firing a failover.
func WithHardDebounce(d time.Duration) Option { return func(m *Monitor) { m.hardDebounce = d } }

// WithRestartGrace sets the §4.2.4 hold-down for a vpp-gone fault before soft-death
// failover — a crashed VPP may be relaunched (kubelet/supervisor) and self-heal in
// place, so a short grace avoids evacuating an edge that recovers on its own. 0 → 5s.
func WithRestartGrace(d time.Duration) Option { return func(m *Monitor) { m.restartGrace = d } }

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option { return func(m *Monitor) { m.log = l } }

// WithQuorum sets how many distinct coverers must vote a session down before
// hard-death fires (L-03 corroborated failover). <=1 → immediate (single vote),
// the single-controller / single-hop default. Use ceil((K+1)/2) or K under
// multihop BFD so a single bad path doesn't trigger a false failover.
func WithQuorum(q int) Option { return func(m *Monitor) { m.quorum = q } }

// WithSelfID sets this replica's coverer id, used as the key for its own
// HardDown vote so it counts as one distinct coverer in the quorum. Empty → the
// local vote uses a fixed key (fine when quorum=1).
func WithSelfID(id string) Option { return func(m *Monitor) { m.selfID = id } }

// WithReporting gates the HEARTBEAT-stale failover path on whether the edge's
// agent is currently reporting to THIS replica (homed here). Under K-coverage an
// edge is tap-covered by K replicas but heartbeats to only ONE; a non-reporting
// coverer must NOT fail it over for going heartbeat-silent (it reports elsewhere
// — the false-failover seen in the K=2 e2e). nil → assume reporting (single
// controller: every live agent reports to the one controller). Hard-death (tap)
// and soft-death are unaffected — those signals every coverer observes directly.
func WithReporting(fn func(model.EdgeID) bool) Option { return func(m *Monitor) { m.reporting = fn } }

// WithStartupGrace sets the first-convergence grace (L-03 churn fix): for this
// long after the monitor is built — i.e. after the controller (re)starts — its
// tap is still establishing sessions, so hard-death verdicts and outbound death
// votes are suppressed (ready()==false). Prevents a reviving replica from falsely
// failing healthy edges (its taps look "down" only because they haven't come up
// yet) and from poisoning peer coverers' quorum with premature votes. 0 disables
// (ready immediately) — the single-controller / single-hop default behaviour.
func WithStartupGrace(d time.Duration) Option { return func(m *Monitor) { m.startupGrace = d } }

// New builds a monitor. It fires onFailover when an edge is (a) hard-down
// (PeerDown, immediate), (b) heartbeat-silent past grace (§5.9 agent gone), or
// (c) soft-dead — canary anomaly AND agent-reported data-plane-death sustained
// past the soft debounce (§4.7). grace<=0 disables the heartbeat path.
func New(grace time.Duration, onFailover ActionFunc, opts ...Option) *Monitor {
	m := &Monitor{
		grace:      grace,
		now:        time.Now,
		edges:      map[model.EdgeID]*state{},
		onFailover: onFailover,
		log:        slog.Default(),
	}
	for _, o := range opts {
		o(m)
	}
	if m.softDebounce == 0 {
		if grace > 0 {
			m.softDebounce = grace
		} else {
			m.softDebounce = 5 * time.Second
		}
	}
	if m.restartGrace == 0 {
		m.restartGrace = 5 * time.Second
	}
	if m.quorum < 1 {
		m.quorum = 1
	}
	// readyAt is set relative to the clock AFTER options apply (WithClock honoured
	// in tests). startupGrace<=0 → readyAt in the past → ready immediately.
	m.readyAt = m.now().Add(m.startupGrace)
	return m
}

// localVoteKey is the coverer id this replica uses for its own HardDown vote.
func (m *Monitor) localVoteKey() string {
	if m.selfID != "" {
		return m.selfID
	}
	return "local"
}

func (m *Monitor) get(edge model.EdgeID) *state {
	s, ok := m.edges[edge]
	if !ok {
		s = &state{lastSeen: m.now(), hardVotes: map[string]bool{}}
		m.edges[edge] = s
	}
	return s
}

// Alive records that an edge is up (registration / PeerUp / a fresh heartbeat):
// it refreshes lastSeen and clears hard-down. If the edge had been failed over,
// this is a revival — onRevive fires and the edge is re-armed for a future death.
func (m *Monitor) Alive(ctx context.Context, edge model.EdgeID) {
	m.mu.Lock()
	s := m.get(edge)
	s.lastSeen = m.now()
	s.hardVotes = map[string]bool{}
	s.canaryDown = false
	s.healthDead = false
	s.faultKind = model.FaultNone
	s.softSince = time.Time{}
	revived := s.failed
	s.failed = false
	m.mu.Unlock()

	if revived && m.onRevive != nil {
		m.log.Info("edge revived after failover; cleaning up residual state", "edge", edge)
		m.onRevive(ctx, edge)
	}
}

// Heartbeat records an EdgeReport arrival (the agent process is alive): it
// refreshes lastSeen. If the edge had been failed over and now has no live death
// signal (not hard-down, not soft-dead), the resumed heartbeat is a revival.
func (m *Monitor) Heartbeat(ctx context.Context, edge model.EdgeID) {
	m.mu.Lock()
	s := m.get(edge)
	s.lastSeen = m.now()
	s.everReported = true // L-10: we've now heard from this edge — it becomes death-eligible
	revived := s.failed && !m.hardDead(s) && !s.dataDead()
	if revived {
		s.failed = false
	}
	m.mu.Unlock()

	if revived && m.onRevive != nil {
		m.log.Info("edge heartbeat resumed after failover; cleaning up", "edge", edge)
		m.onRevive(ctx, edge)
	}
}

// Health folds an agent's self-reported data-plane health (B-05): softDead=true
// when the agent says HealthDataPlaneDown, and fault is the agent-typed fault kind
// (§4.2.3, FaultNone from a pre-§4.2 agent). The death predicate it feeds is
// dataDead(): for an ambiguous/unclassified fault it is one half of the §4.7
// soft-death conjunction — on its own it never fails over (a single bad report could
// be a false positive), it must coincide with a tap canary anomaly; for an
// unambiguous typed fault (vpp-gone / link-down) the report is trusted on its own
// and routed to its own §4.2.4 debounce.
func (m *Monitor) Health(edge model.EdgeID, softDead bool, fault model.FaultKind) {
	m.mu.Lock()
	s := m.get(edge)
	s.healthDead = softDead
	s.faultKind = fault
	if !s.dataDead() {
		s.softSince = time.Time{} // predicate broken → reset the debounce
	}
	m.mu.Unlock()
}

// HardDown records THIS replica's tap PeerDown for the edge — one coverer vote.
// With quorum=1 (default) it makes the edge hard-dead immediately; under a higher
// quorum (L-03) it counts as one of the K coverers and failover waits for the
// rest to corroborate. Realized on Tick to keep callbacks off the tap's hot path.
func (m *Monitor) HardDown(edge model.EdgeID) {
	m.mu.Lock()
	m.get(edge).hardVotes[m.localVoteKey()] = true
	m.mu.Unlock()
}

// HardUp clears THIS replica's hard-down vote (PeerUp); a subsequent
// Heartbeat/Alive completes revival once no death signal remains.
func (m *Monitor) HardUp(edge model.EdgeID) {
	m.mu.Lock()
	delete(m.get(edge).hardVotes, m.localVoteKey())
	m.mu.Unlock()
}

// Vote records another coverer's hard-death observation for the edge (L-03): the
// death-vote watcher feeds peer replicas' PeerDown/PeerUp here so this replica
// can reach the corroboration quorum. down=true adds the vote, false clears it.
// The local replica uses HardDown/HardUp; this is strictly for remote coverers.
func (m *Monitor) Vote(edge model.EdgeID, coverer string, down bool) {
	if coverer == "" || coverer == m.localVoteKey() {
		return // ignore empty / our own id (HardDown owns the local vote)
	}
	m.mu.Lock()
	s := m.get(edge)
	if down {
		s.hardVotes[coverer] = true
	} else {
		delete(s.hardVotes, coverer)
	}
	m.mu.Unlock()
}

// CanaryDown records a tap canary withdrawal WHILE the session is up — a SOFT
// signal (the edge's RIB lost its canary but BGP is alive, §R-12). It only
// triggers failover in conjunction with an agent data-plane-death report.
func (m *Monitor) CanaryDown(edge model.EdgeID) {
	m.mu.Lock()
	m.get(edge).canaryDown = true
	m.mu.Unlock()
}

// CanaryUp records the canary reappearing — clears the soft canary signal and
// the soft-death debounce.
func (m *Monitor) CanaryUp(edge model.EdgeID) {
	m.mu.Lock()
	s := m.get(edge)
	s.canaryDown = false
	s.softSince = time.Time{}
	m.mu.Unlock()
}

// IsDead reports whether the edge is currently judged dead — hard-down, or
// already failed over. Used by placement to avoid homing onto a dead agent and
// by drain to tell a live backup (promotable) from a dead one (double death).
// An untracked edge is treated as alive (optimistic: it may simply not have
// signaled yet).
func (m *Monitor) IsDead(edge model.EdgeID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.edges[edge]
	if !ok {
		return false
	}
	return m.hardDead(s) || s.failed
}

// Forget drops an edge from tracking (deregistered / decommissioned), so it is
// not failed over for going silent after a planned exit.
func (m *Monitor) Forget(edge model.EdgeID) {
	m.mu.Lock()
	delete(m.edges, edge)
	m.mu.Unlock()
}

// Tick evaluates every tracked edge and fires onFailover for those newly judged
// dead, by the three §4.7/§5.9/§6.5 rules:
//   - HARD: coverer quorum of PeerDown votes (L-03) — immediate, no grace
//     (quorum=1 = a single PeerDown, the default).
//   - HEARTBEAT: silent past grace (§5.9 agent process gone) — promote backup.
//   - SOFT: canary anomaly AND agent data-plane-death, sustained past the soft
//     debounce (§4.7) — a single soft signal never fires.
//
// Returns the edges it fired on. Run from a ticker.
func (m *Monitor) Tick(ctx context.Context) []model.EdgeID {
	now := m.now()
	m.mu.Lock()
	var dead []model.EdgeID
	deadVotes := map[model.EdgeID][]string{} // debug: which coverers voted (L-03 churn diag)
	deathMethod := map[model.EdgeID]string{} // which §4.7/§5.9/§6.5 rule fired (TIER-3 reason)
	// staleLevels collects each non-failed edge's CURRENT heartbeat-stale level for the
	// metering-stale NOTIFICATION (fired after the lock; pure observability, no logic
	// change). nil unless a notify is wired.
	var staleLevels map[model.EdgeID]bool
	if m.meteringStale != nil {
		staleLevels = make(map[model.EdgeID]bool, len(m.edges))
	}
	for edge, s := range m.edges {
		if s.failed {
			continue
		}
		// Heartbeat-stale (§5.9 agent process gone) — but only for edges whose agent
		// is reporting to THIS replica. Under K-coverage a coverer that isn't the
		// agent's reporter is heartbeat-silent by design (the agent homes elsewhere);
		// treating that as death falsely fails healthy edges (K=2 e2e). The reporter
		// gate is nil in single-controller mode → unchanged there.
		stale := m.grace > 0 && now.Sub(s.lastSeen) > m.grace &&
			(m.reporting == nil || m.reporting(edge))
		if staleLevels != nil {
			staleLevels[edge] = stale
		}

		// Soft-death predicate with a per-fault-kind debounce (§4.2.4): track when it
		// began, fire once it has persisted past the kind's hold-down; reset the moment
		// it clears. debounceFor routes link-down→immediate, vpp-gone→restartGrace, and
		// everything else (incl. FaultNone) → the full softDebounce, so a pre-§4.2 agent
		// is unchanged.
		softFired := false
		if s.dataDead() {
			switch d := m.debounceFor(s.faultKind); {
			case d <= 0:
				softFired = true
			case s.softSince.IsZero():
				s.softSince = now
			case now.Sub(s.softSince) >= d:
				softFired = true
			}
		} else {
			s.softSince = time.Time{}
		}

		// everReported gate (L-10 heartbeat-stale false-death fix): heartbeat-staleness means
		// "reported, THEN went silent". An edge that has NEVER sent an EdgeReport to this
		// controller can't be stale — it was never fresh; a cold restart that just times out
		// its FIRST report past the grace must not be read as a death. Without this gate every
		// restart false-fails-over every still-converging edge (mass re-home skew, L-10).
		// HARD death stays UNGATED: a tap-PeerDown quorum is unambiguous and immediate BY
		// DESIGN (§6.5), and its own ready() startup-grace already covers the re-establishment
		// window. soft-death needs a Health report, so it is naturally gated too.
		staleEligible := stale && s.everReported

		// Hard-death (tap PeerDown quorum) hold-down: track when the quorum was first
		// reached, fire once it persists past hardDebounce; reset the moment it clears.
		// hardDebounce<=0 fires immediately (original behaviour). Damps a recovering
		// edge whose tap flaps up→down→up from re-firing a failover during re-convergence.
		hardFired := false
		if m.hardDead(s) {
			switch {
			case m.hardDebounce <= 0:
				hardFired = true
			case s.hardSince.IsZero():
				s.hardSince = now
			case now.Sub(s.hardSince) >= m.hardDebounce:
				hardFired = true
			}
		} else {
			s.hardSince = time.Time{}
		}

		if hardFired || staleEligible || softFired {
			s.failed = true
			dead = append(dead, edge)
			votes := make([]string, 0, len(s.hardVotes))
			for c := range s.hardVotes {
				votes = append(votes, c)
			}
			deadVotes[edge] = votes
			// Death METHOD precedence (TIER-3 reason; observability only — does NOT change
			// which edges fire): hard-quorum (unambiguous tap PeerDown quorum) dominates,
			// then the soft-death conjunction, then heartbeat-stale. This carries the rule
			// that tripped; the SET of dead edges is exactly as before.
			switch {
			case hardFired:
				deathMethod[edge] = "hard-quorum"
			case softFired:
				deathMethod[edge] = "soft-death"
			default:
				deathMethod[edge] = "heartbeat-stale"
			}
		}
	}
	m.mu.Unlock()

	// Metering-stale NOTIFICATION (pure observability; fired outside the lock). The
	// consumer converts the level into an edge-triggered metering-stale/resumed event.
	if m.meteringStale != nil {
		for edge, stale := range staleLevels {
			m.meteringStale(edge, stale)
		}
	}

	for _, edge := range dead {
		m.log.Warn("edge judged dead; triggering failover",
			"edge", edge, "quorum", m.quorum, "votes", deadVotes[edge], "method", deathMethod[edge])
		// Death-method NOTIFICATION first (pure observability; carries the rule that
		// tripped so the control plane stamps the edge-down + per-pool failover events
		// with the real method), THEN the failover action — both outside the lock.
		if m.onDeath != nil {
			m.onDeath(edge, deathMethod[edge])
		}
		m.onFailover(ctx, edge)
	}
	return dead
}
