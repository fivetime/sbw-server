package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"sync"
	"time"

	"github.com/fivetime/sbw-contract/metrics"
	"github.com/fivetime/sbw-contract/model"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/fivetime/sbw-server/internal/apiresult"
	"github.com/fivetime/sbw-server/internal/deathvote"
	"github.com/fivetime/sbw-server/internal/edgever"
	"github.com/fivetime/sbw-server/internal/ledger"
	"github.com/fivetime/sbw-server/internal/liveness"
	"github.com/fivetime/sbw-server/internal/lossmon"
	"github.com/fivetime/sbw-server/internal/orchestrator"
	"github.com/fivetime/sbw-server/internal/registry"
	"github.com/fivetime/sbw-server/internal/shard"
)

// sellableFracPercent is the share of an agent's NIC line rate the controller may
// sell as tokens (§4.1 Σhome ≤ NIC×90%) — headroom for bursts/overhead.
const sellableFracPercent = 90

// defaultDriftStreakThreshold is how many CONSECUTIVE expected≠reported sweeps an
// edge must show before the drift sweep fires a full resync — debouncing the
// benign window where the agent is still catching up to a burst of fresh creates.
const defaultDriftStreakThreshold = 2

// reportedHash records an edge's last-reported pool-set hash and when it arrived.
// seen flags that a report landed since the last sweep, so a silent edge is not
// re-evaluated (its O(N) expected set is not recomputed) until it reports again.
type reportedHash struct {
	hash uint64
	at   time.Time
	seen bool
}

// ControlPlane is the composed controller: it wires the etcd-backed stores
// (registry, ledger) and the MANDATORY Yugabyte-backed pool records (cp.YB) to
// the gRPC agent server and the pool orchestrator into one runnable unit. It is
// the seam the cmd/ binary and the
// e2e test both drive, so "does it all compose" is provable without a full
// deploy. Stateless: every replica builds the same ControlPlane over the shared
// etcd, no leader election (§1.2).
type ControlPlane struct {
	Registry *registry.Registry
	Ledger   *ledger.Ledger
	// YB is the MANDATORY YugabyteDB-backed bulk pool/member store. The cmd wires it via
	// SetYBStore before agents arrive (the controller EXITS at startup if Yugabyte is
	// unavailable — there is no all-etcd fallback). The create path's data write, the
	// render reads, the drift expected-set and the admin pool list/get all source from
	// here; etcd keeps only coordination. It is the orchestrator.YBStore interface so the
	// unit tests can inject an in-memory double (ybstore.Mem) without a live DB.
	YB       orchestrator.YBStore
	YBCap    orchestrator.CapacityProvider
	Orch     *orchestrator.Orchestrator
	Liveness *liveness.Monitor
	Loss     *lossmon.Monitor                              // §4.2.5 per-member forwarding-loss policy (alert / per-pool migrate)
	scvr     *scvrProvider                                 // server-half of the ServerCoverer contract (Report/Register); the gRPC handlers delegate to it
	onReport func(context.Context, model.EdgeReport) error // server-half report processing, invoked via the seam
	// onRegister / covererFunc are the server-half halves of registration the seam's
	// Register dispatches to: onRegister does the authoritative register (ledger init /
	// inventory), covererFunc computes the agent's coverer set (sharding; nil = off).
	onRegister  func(context.Context, model.EdgeID, uint64) error
	covererFunc func(context.Context, model.EdgeID) (model.CovererAssignment, bool, error)
	// fan is the SERVER-HALF desired-state fan (§8 step3 WATCH downlink): the
	// orchestrator's Pusher is now this, not grpcsrv directly. PushDesired/PushDelta/
	// PushRehome marshal the model into an rpc.Directive and emit Assignment{EDGE_DIRECTIVE}
	// into the coverer's Watch channel; the in-process consumer (runWatchConsumer) relays
	// each to the agent via the SAME grpcsrv.Push*, so the agent-facing path is byte-identical.
	fan *desiredFan
	// replicaID is this server replica's id (deathvote / liveness SelfID). It is NOT a
	// coverer id — coverer ids come over the wire on Watch (req.CovererId) and key the
	// fan's streams; this is the server replica's own identity for the etcd death-vote
	// bridge and the liveness monitor.
	replicaID string
	// coverageK is the HRW replication factor K applied over the CONNECTED coverer set
	// for BOTH edge→coverer routing (desiredFan.k) and COVERAGE (CoveredEdges). Sourced
	// from the one sh.K so routing and tap-assignment stay consistent.
	coverageK int
	// coverageRecalc coalesces COVERAGE recompute triggers (connect / evict / first
	// register). Buffered cap 1: a burst collapses to a single pending recompute that
	// RunCoverageRecompute debounces. Scheduled non-blocking via scheduleCoverageRecompute.
	coverageRecalc chan struct{}

	reports       *reportCache
	acctTolerance uint64
	onAcctDrift   func(AccountDrift)

	// progStreak counts an edge's CONSECUTIVE count-drift audits (B-02): an alarm
	// fires only after progThreshold in a row, debouncing the benign push→apply
	// window where a fresh push legitimately outpaces the agent's last report.
	progStreakMu  sync.Mutex
	progStreak    map[model.EdgeID]int
	progThreshold int
	onProgDrift   func(ProgramDrift)

	// onAnchorMismatch fires when an edge's anchor intent (Yugabyte) disagrees with
	// the host /32 it physically advertises (L-04). Alarm-only; suppression is the
	// T-607 render gate.
	onAnchorMismatch func(AnchorMismatch)

	// presence is the server-side member→edge PRESENCE/VERDICT map rebuilt from the
	// coverer MEMBER_EDGE report stream (memberedge.go). It replaces the monolith's
	// in-process RIB-survival guard as the source for the render-time anchor-suppression
	// gate (WithAdvertiseGate → memberSuppressed), the member-up/down emits, and the
	// anchor intent↔physical audit (anchoraudit.go). Always wired (newMemberPresence).
	presence *memberPresence

	// driftMu guards the per-edge report-hash state below. The report hot path
	// (driftCheck) only takes this lock to STORE the latest reported hash — it is
	// O(1) and never touches etcd. The periodic DriftSweep reads it to decide which
	// edges to recompute (≤ once per sweep per edge, NOT per report).
	driftMu sync.Mutex
	// driftReported is the agent's last-reported InstalledPoolHash per edge, plus
	// when it arrived. seen since the last sweep is whether a fresh report landed
	// (so the sweep skips edges that went silent).
	driftReported map[model.EdgeID]reportedHash
	// driftStreak counts an edge's CONSECUTIVE expected≠reported sweeps. A resync
	// fires only at driftStreakThreshold so transient agent catch-up lag (one sweep
	// behind a burst of creates) does not trigger a spurious O(N) resync.
	driftStreak          map[model.EdgeID]int
	driftStreakThreshold int

	// voter publishes this replica's tap PeerDown/PeerUp as etcd death votes so
	// peer coverers can reach the corroboration quorum (L-03). nil when sharding is
	// off — HardDown/HardUp then only drive the local monitor (quorum=1).
	voter *deathvote.Voter

	// edgever is the per-edge desired/applied version store (L-07). When set
	// (sharding), failover switches to the async evacuation reconciler: the
	// failover/revive triggers enqueue pool reconciles and deliver via the converge
	// loop, instead of the synchronous replica-local drain. nil → synchronous drain.
	edgever *edgever.Store
	// reconcileQ feeds RunPoolReconcile pool ids to re-evaluate (liveness events +
	// applied-version advances). Buffered + best-effort: a full queue drops the
	// enqueue, which the periodic sweep backstops.
	reconcileQ chan model.PoolID

	// createWindow is the ±skew the create anti-replay timestamp must fall within
	// (and the nonce key's TTL lease). 0 → defaultCreateWindow (5min). now is the
	// clock the handler uses to compute drift (overridable in tests).
	createWindow time.Duration
	now          func() time.Time

	// emitter ships API-result events (converged / failed) to Redpanda so the BSS can
	// correlate a synchronous create/update/destroy response (request_id + generation)
	// with the eventual DATA-PLANE realization. Defaults to apiresult.Noop (disabled)
	// — the control plane runs exactly as today until SetAPIResultEmitter wires a real
	// producer (only when brokers are configured). pendingMu guards pending.
	emitter   apiresult.Emitter
	pendingMu sync.Mutex
	pending   map[string]pendingResult

	// obsMu guards the per-edge transition state for the LEVEL→EDGE conversion of the
	// stateful unsolicited observability events (edge-dataplane-down/up and metering-
	// stale/resumed). Reports/Tick deliver a LEVEL ("is the edge data-plane-down right
	// now?", "is its metering stale right now?"); these maps hold the last-emitted level
	// so an event fires ONLY on a transition, not every report/tick. Absent key = the
	// benign default (up / fresh), so the first bad observation transitions.
	obsMu             sync.Mutex
	dpDownEdges       map[model.EdgeID]bool // edge → last-emitted data-plane-down level
	meteringStaleEdge map[model.EdgeID]bool // edge → last-emitted metering-stale level
	// deathMethodByEdge carries the most recent DEATH METHOD ("hard-quorum"|"heartbeat-
	// stale"|"soft-death") the liveness monitor reported for an edge, set by the death
	// notify (which also emits the one edge-down event) and read by emitFailover so the
	// per-pool failover events carry the SAME method instead of a hardcoded "node-failure".
	// Cleared on edge-up (revive). Absent ⇒ fall back to "node-failure" (e.g. a drain-path
	// failover with no monitor verdict). Guarded by obsMu.
	deathMethodByEdge map[model.EdgeID]string

	metrics    *metrics.Metrics
	log        *slog.Logger
	grpcServer *grpc.Server
}

// pendingResult is one in-flight API operation awaiting its data-plane realization.
// It is registered after a SUCCESSFUL synchronous create/update/destroy and resolved
// either by a home-edge report echoing an applied generation >= Generation (→
// converged) or by the timeout sweep (→ failed, reason=timeout).
type pendingResult struct {
	requestID   string
	op          string // "create" | "update" | "destroy" | "migrate" | "decommission"
	poolID      model.PoolID
	primaryEdge model.EdgeID
	generation  uint64
	createdAt   time.Time
	// --- enrichment carried onto the resolved "converged" event (TIER 4). All zero for a
	// plain RegisterPending (destroy/decommission); set by registerPendingEnriched.
	fromEdge enrich // migrate: the old primary the pool moved OFF (→ Event.FromEdge)
	rate     *rateBasis
}

// enrich is a string alias used only to make the pendingResult literal self-documenting.
type enrich = model.EdgeID

// rateBasis is the GRANTED rate/billing basis carried onto a create/update "converged"
// event so an event-only BSS can rate the pool without a second lookup. nil for ops that
// carry no rate (destroy/decommission/migrate).
type rateBasis struct {
	cirKbps        uint64
	ingressCIRKbps uint64
	tokens         int64
	unlimited      bool
}

// CPOptions parameterizes the control plane.
type CPOptions struct {
	// Prefix namespaces all etcd keys (e.g. "sbw/").
	Prefix string
	// ReservationTTL is the hung-token reclaim window (§4.3).
	ReservationTTL time.Duration
	// Replicas is homes per pool (default 2: primary + backup).
	Replicas int
	// EdgeAddrs / EdgeAddrs6 give each edge's v4 / v6 redirect next-hop (egress
	// FlowSpec render). May be nil/partial at start and refreshed as agents register
	// (V1: operator-set). EdgeAddrs6 is needed for rate-limit pools with v6 members.
	EdgeAddrs  map[model.EdgeID]netip.Addr
	EdgeAddrs6 map[model.EdgeID]netip.Addr
	// HomeMarker supplies the home large-community marker (§4.7).
	HomeMarker func(model.EdgeID) (model.LargeCommunity, bool)
	// OnReport handles an agent's uplink EdgeReport (health fusion / capacity).
	// The liveness monitor's Heartbeat and the report cache are always updated in
	// addition to this, before it.
	OnReport func(context.Context, model.EdgeReport) error
	// OnDoubleDeath, if set, fires when a pool loses all live homes (§4.7):
	// failOpen reports the per-pool policy applied (billing→open, control→close).
	// Wire to a Prometheus alert.
	OnDoubleDeath func(id model.PoolID, failOpen bool)
	// LivenessGrace is the heartbeat-loss grace before an edge is failed over
	// (§5.9). 0 → default 30s. Negative disables heartbeat-loss failover (only a
	// tap HardDown triggers).
	LivenessGrace time.Duration
	// LivenessSoftDebounce is how long the soft-death conjunction (tap canary
	// anomaly ∧ agent data-plane-death) must persist before failover (§4.7). 0 →
	// defaults to the grace.
	LivenessSoftDebounce time.Duration
	// LivenessHardDebounce is a hold-down the HARD-death quorum (tap PeerDown) must
	// persist before failover fires. 0 → default 3s. Damps a recovering edge whose
	// tap flaps up→down→up during its own re-convergence from re-firing a failover.
	LivenessHardDebounce time.Duration
	// LivenessVPPRestartGrace is the §4.2.4 hold-down for a vpp-gone typed fault
	// before soft-death failover — a crashed VPP may be relaunched and self-heal in
	// place. 0 → default 5s. Distinct from LivenessSoftDebounce (which stays the
	// hold-down for ambiguous/untyped soft death); a link-down typed fault fires
	// immediately with no configurable grace (a dead uplink won't heal itself).
	LivenessVPPRestartGrace time.Duration
	// LossAlertPct / LossMigratePct are the §4.2.5 per-member forwarding-loss policy
	// watermarks as PERCENTAGES (0..100). A member at/above LossAlertPct emits a
	// Redpanda edge-forwarding-degraded event (human triage); at/above LossMigratePct
	// sustained LossMigrateSustain, its POOL is migrated off the lossy home. 0 → 15 / 30.
	LossAlertPct   int
	LossMigratePct int
	// LossMigrateSustain is how long loss must stay at/above LossMigratePct before the
	// per-pool migrate fires (the transient-spike hold-down). 0 → 5m.
	LossMigrateSustain time.Duration
	// LivenessQuorum is how many distinct coverers must observe an edge's session
	// down before HARD-death failover fires (L-03 corroborated failover, multihop
	// BFD). 0/1 → immediate (single PeerDown), the single-controller/single-hop
	// default. Set to ceil((K+1)/2) or K under sharding+multihop.
	LivenessQuorum int
	// SelfID is this replica's coverer id (the death-vote key for its own
	// PeerDown). Empty → a fixed local key (fine when LivenessQuorum<=1).
	SelfID string
	// ReadyTimeout bounds how long a freshly provisioned backup may take to
	// confirm it applied its desired-state generation before a migration switches
	// onto it (§4.4①/§5.9②). 0 → default 10s.
	ReadyTimeout time.Duration
	// CanaryLC identifies canary routes in the RIB tap (per-edge loopback /32 +
	// this large community, §6.4/T-305). A canary's withdrawal is the edge's
	// hard-death signal; its presence is the alive signal. The zero value means
	// no canary is configured — the tap then drives liveness from PeerDown/PeerUp
	// only.
	CanaryLC model.LargeCommunity
	// AccountToleranceBps is the allowed ledger-vs-reported sold-bandwidth drift
	// before reconciliation alarms (§4.3). 0 → default 1 Mbit/s (absorbs rounding/
	// in-flight transactions).
	AccountToleranceBps uint64
	// OnAccountDrift, if set, is called for each agent whose ledger/report drift
	// exceeds the tolerance (wire to a Prometheus gauge / alert).
	OnAccountDrift func(AccountDrift)
	// ProgramDriftStreak is how many CONSECUTIVE count-drift audits an edge must
	// show before B-02 alarms + auto-repushes it (debounces the push→apply window).
	// 0 → default 2.
	ProgramDriftStreak int
	// DriftStreakThreshold is how many CONSECUTIVE report-hash drift SWEEPS an edge
	// must show (expected≠reported) before the sweep fires a full DESIRED_STATE
	// resync — debouncing transient agent catch-up lag behind a burst of creates.
	// 0 → default 2.
	DriftStreakThreshold int
	// MemberPresenceTTL is the lease window for a coverer MEMBER_EDGE present-assertion
	// (memberedge.go). It governs the chunked-snapshot drift reap: a member no longer
	// refreshed by the coverer's periodic snapshot lapses and is reaped after this
	// window. MUST exceed the coverer snapshot cadence (EOR + ReconcileTapView interval)
	// by a safety factor. 0 → default 5min.
	MemberPresenceTTL time.Duration
	// OnProgramDrift, if set, is called for each edge whose data-plane count drift
	// persisted past the streak (wire to a Prometheus alert).
	OnProgramDrift func(ProgramDrift)
	// OnAnchorMismatch, if set, is called for each edge whose anchor intent (Yugabyte)
	// disagrees with the host /32 it physically advertises (L-04 two-way reconcile).
	OnAnchorMismatch func(AnchorMismatch)
	// Metrics, if set, receives the control plane's alarm + inventory series
	// (T-1003). nil disables metrics. Serve its Handler() from the cmd.
	Metrics *metrics.Metrics
	// CreateWindow is the ±skew the create anti-replay timestamp must fall within
	// (and the nonce key's TTL lease). 0 → default 5min.
	CreateWindow time.Duration
	// CoverageK is the HRW replication factor K over the CONNECTED coverer set, applied
	// to BOTH desired-state routing and COVERAGE (tap-assignment). 0 → 1. In production
	// main.go passes the resolved sh.K so routing and coverage agree.
	CoverageK int
	// Logger; defaults to slog.Default().
	Logger *slog.Logger
}

// NewControlPlane wires the stores, gRPC server and orchestrator over one etcd
// client. The gRPC server is the orchestrator's Pusher (it owns the agent
// streams), and the registration handler composes registry.Register with
// ledger.InitAgent (capacity→tokens) so a registering agent becomes schedulable
// in one step.
func NewControlPlane(kv clientv3.KV, opt CPOptions) *ControlPlane {
	log := opt.Logger
	if log == nil {
		log = slog.Default()
	}
	if opt.Prefix == "" {
		opt.Prefix = "sbw/"
	}
	if opt.Replicas == 0 {
		opt.Replicas = 2
	}
	if opt.ReservationTTL == 0 {
		opt.ReservationTTL = 30 * time.Second
	}

	if opt.LivenessGrace == 0 {
		opt.LivenessGrace = 30 * time.Second
	}
	// §4.2.5 loss-policy defaults (a direct NewControlPlane caller / test may leave them
	// zero; the cmd passes LossConfig.WithDefaults, so these agree with config.go).
	if opt.LossAlertPct == 0 {
		opt.LossAlertPct = 15
	}
	if opt.LossMigratePct == 0 {
		opt.LossMigratePct = 30
	}
	if opt.LossMigrateSustain == 0 {
		opt.LossMigrateSustain = 5 * time.Minute
	}
	// LivenessHardDebounce defaults to 0 = immediate hard-death (DESIGN §6.5: a tap
	// PeerDown quorum is unambiguous). It is opt-in (sharding.hard_debounce) for the
	// K=2 recovery-flap case where a re-converging edge's tap briefly flaps.
	if opt.AccountToleranceBps == 0 {
		opt.AccountToleranceBps = 1_000_000 // 1 Mbit/s
	}
	if opt.ProgramDriftStreak == 0 {
		opt.ProgramDriftStreak = 2
	}
	if opt.DriftStreakThreshold == 0 {
		opt.DriftStreakThreshold = defaultDriftStreakThreshold
	}
	reports := newReportCache()

	if opt.CreateWindow == 0 {
		opt.CreateWindow = defaultCreateWindow
	}
	if opt.CoverageK == 0 {
		opt.CoverageK = 1
	}

	reg := registry.New(kv, opt.Prefix)
	led := ledger.New(kv, opt.Prefix, opt.ReservationTTL)

	// cp is built up front (empty of the later-constructed orch/agents/mon/guard) so
	// the closures below can capture it and branch on cp.edgever at CALL time — the
	// async evacuation deps are injected post-construction by SetEdgeVer (the cmd
	// builds edgever from the etcd *Client). Fields are filled in as each component
	// is constructed; cp is returned at the end.
	cp := &ControlPlane{
		Registry: reg, Ledger: led,
		replicaID:      opt.SelfID,
		coverageK:      opt.CoverageK,
		coverageRecalc: make(chan struct{}, 1),
		reports:        reports,
		acctTolerance:  opt.AccountToleranceBps,
		onAcctDrift:    opt.OnAccountDrift,
		progStreak:     make(map[model.EdgeID]int),
		progThreshold:  opt.ProgramDriftStreak,
		// onProgDrift / onAnchorMismatch are composed below (emit + user option) once cp
		// exists, so the dangling TIER-1 seams fire the unsolicited events without main.go
		// having to set the options. nil here; filled in after construction.
		driftReported:        make(map[model.EdgeID]reportedHash),
		driftStreak:          make(map[model.EdgeID]int),
		driftStreakThreshold: opt.DriftStreakThreshold,
		reconcileQ:           make(chan model.PoolID, 1024),
		createWindow:         opt.CreateWindow,
		now:                  time.Now,
		emitter:              apiresult.Noop{}, // disabled until SetAPIResultEmitter (no brokers ⇒ no behaviour change)
		pending:              make(map[string]pendingResult),
		dpDownEdges:          make(map[model.EdgeID]bool),
		meteringStaleEdge:    make(map[model.EdgeID]bool),
		deathMethodByEdge:    make(map[model.EdgeID]string),
		metrics:              opt.Metrics,
		log:                  log,
	}
	// The member→edge presence map: rebuilt from the coverer MEMBER_EDGE reports, it
	// feeds the advertise gate / member-up-down emits / anchor audit below. now is the
	// CP clock (overridable in tests); the lease TTL governs the chunked-snapshot drift
	// reap (memberedge.go), defaulting (0) to a safe multiple of the coverer snapshot
	// cadence.
	cp.presence = newMemberPresence(cp.now, opt.MemberPresenceTTL)

	// Wire the dangling TIER-1 observability seams (the OnProgramDrift / OnAnchorMismatch
	// callbacks the reconcilers already fire but main.go never set): compose the
	// unsolicited emit with any operator-supplied option so BOTH run. Inert under the Noop
	// emitter — no brokers ⇒ no events ⇒ no behaviour change.
	userProgDrift := opt.OnProgramDrift
	cp.onProgDrift = func(d ProgramDrift) {
		cp.emitProgramDrift(d)
		if userProgDrift != nil {
			userProgDrift(d)
		}
	}
	userAnchorMismatch := opt.OnAnchorMismatch
	cp.onAnchorMismatch = func(m AnchorMismatch) {
		cp.emitAnchorMismatch(m)
		if userAnchorMismatch != nil {
			userAnchorMismatch(m)
		}
	}

	// mon is created after the orchestrator (it calls orch.FailoverEdge); the
	// register/report closures capture the variable and only deref it once agents
	// start arriving, which is strictly after this function returns.
	var mon *liveness.Monitor

	onRegister := func(ctx context.Context, edge model.EdgeID, capacityBps uint64) error {
		// Read the prior registry state BEFORE Register overwrites it, so the TIER-3 edge-
		// inventory events can distinguish a FIRST register (no prior agent) from a restart
		// re-register, and a CAPACITY CHANGE (existing edge, different CapacityBps) from an
		// identical re-register. Best-effort: a read error just suppresses the inventory
		// event (the register itself still proceeds) — observability never blocks registration.
		prior, hadPrior, gerr := reg.Get(ctx, edge)
		if gerr != nil {
			hadPrior = false // treat as unknown; suppress the event rather than mis-signal
			log.Warn("edge-inventory: prior agent read failed (suppressing inventory event)", "edge", edge, "err", gerr)
		}
		if err := reg.Register(ctx, edge, capacityBps); err != nil {
			return err
		}
		tokens := int64(capacityBps) * sellableFracPercent / 100
		// InitAgent is first-registration-only: a restart re-registers (capacity
		// refreshed) but must NOT reset a balance with committed allocations.
		if _, err := led.InitAgent(ctx, string(edge), tokens); err != nil {
			return err
		}
		mon.Alive(ctx, edge) // (re-)register counts as an alive signal
		// TIER-3 edge inventory: a FIRST register joins a new schedulable node to the fleet
		// (edge-registered); a re-register only signals if the sellable capacity actually
		// moved (edge-capacity-changed) — an identical restart re-register is silent (no
		// spam). Async / Noop-safe; emitted only after the register+init succeeded.
		if gerr == nil {
			if !hadPrior {
				cp.emitEdgeRegistered(edge, int64(capacityBps))
				// A brand-new edge joins the universe and must be assigned to its
				// covering coverer's tap — recompute + re-emit COVERAGE (debounced). A
				// re-register (same edge) does not change the universe, so no recompute.
				cp.scheduleCoverageRecompute()
			} else if prior.CapacityBps != capacityBps {
				cp.emitEdgeCapacityChanged(edge, int64(capacityBps))
			}
		}
		log.Info("agent registered", "edge", edge, "capacity_bps", capacityBps, "sellable_tokens", tokens)
		return nil
	}

	userReport := opt.OnReport
	onReport := func(ctx context.Context, r model.EdgeReport) error {
		mon.Heartbeat(ctx, r.EdgeID)                                  // every report is a heartbeat (agent alive)
		mon.Health(r.EdgeID, r.Health.SoftDead(), r.Health.FaultKind) // self-reported data-plane death (§4.7 soft half) + §4.2.3 typed fault
		// TIER-1 edge-dataplane transition: r.Health.State==HealthDataPlaneDown means VPP
		// is dead while BGP/canary may still be up (billing while black-holing, invisible
		// to BGP). Convert the per-report LEVEL into an EDGE — emit only on up→down /
		// down→up, not every report. Async / Noop-safe.
		cp.emitEdgeDataplane(r.EdgeID, r.Health.State == model.HealthDataPlaneDown)
		// §4.2.5 per-member forwarding-loss: fold the edge's loss snapshot (members over
		// the agent watermark) into the loss monitor's sustain windows. RunLoss.Tick
		// fires the alert / per-pool migrate; absence of a member = recovered. Nil-safe
		// (empty MemberLoss = a clean snapshot that recovers any tracked member).
		cp.ingestMemberLoss(r)
		reports.put(r) // cache for account reconciliation (§4.3)
		// API-RESULT CONVERGENCE: the report echoes the applied desired-state generation
		// (r.Generation). Resolve any pending create/update/destroy whose home is this
		// edge and whose pushed generation the agent has now applied (>=) → emit
		// "converged". O(pending) under a brief lock; the emit is async (non-blocking).
		cp.resolvePending(ctx, r)
		// L-07: advance the edge's etcd applied-version from the agent's echo so the
		// async reconciler's ready gate (edgeReady) is observable cross-replica, and
		// re-evaluate the edge's pools — a newly-ready backup unblocks a gated promote
		// (or a now-live new primary unblocks the retire-old step). Best-effort.
		if cp.edgever != nil && r.Health.AppliedVersion > 0 {
			if err := cp.edgever.Advance(ctx, r.EdgeID, r.Health.AppliedVersion); err != nil {
				log.Warn("applied-version advance failed", "edge", r.EdgeID, "err", err)
			}
			cp.enqueuePoolsFor(ctx, r.EdgeID)
		}
		// REPORT-DRIVEN DRIFT BACKSTOP (O(1) hot path): the agent reports the hash of
		// the pool-set it has materialized; here we only RECORD it (in-memory, per
		// edge). The O(N) expected-set recompute + compare + full-resync decision is
		// deferred to the periodic DriftSweep (RunDriftSweep), which evaluates each
		// reporting edge at most once per sweep — so a high report rate no longer
		// drives O(N) Yugabyte work per report.
		cp.driftCheck(ctx, r)
		if userReport != nil {
			return userReport(ctx, r)
		}
		log.Debug("agent report", "edge", r.EdgeID, "schema", r.SchemaVersion)
		return nil
	}

	// orch is assigned just below; the failover/revive closures and the liveness
	// gate capture it and deref it only after this function returns.
	var orch *orchestrator.Orchestrator

	// The liveness oracle + double-death alarm capture mon (declared above);
	// isAlive is only called during operations, strictly after this returns.
	ddAlarm := func(id model.PoolID, failOpen bool) {
		opt.Metrics.DoubleDeath(failOpen)
		if failOpen {
			log.Warn("pool double-death → FAIL-OPEN (torn down; traffic returns to normal path)", "pool", id)
		} else {
			log.Warn("pool double-death → FAIL-CLOSE (kept; suppression preserved)", "pool", id)
		}
		// TIER-2: unsolicited "pool-double-death" event (the dangling OnDoubleDeath seam).
		// Async / Noop-safe; composes with any operator-supplied OnDoubleDeath option.
		cp.emitDoubleDeath(id, failOpen)
		if opt.OnDoubleDeath != nil {
			opt.OnDoubleDeath(id, failOpen)
		}
	}

	// SERVER-HALF fan (the ServerCoverer WATCH downlink): the orchestrator's Pusher is
	// this fan, not an agent transport. It implements Pusher+deltaPusher+subChecker, so
	// renderAndPushGen/pushPoolDeltaGen/locallyDeliverable are UNCHANGED — they now fan
	// into the per-coverer Watch channels, which the remote coverers drain over gRPC.
	// Deliverability is re-gated by HRW over the CONNECTED coverer set (the fan's own
	// stream map) — no etcd/ctrlreg on the routing path.
	fan := newDesiredFan(log, opt.CoverageK)

	orch = orchestrator.New(reg, fan,
		orchestrator.WithReplicas(opt.Replicas),
		orchestrator.WithEdgeAddrs(opt.EdgeAddrs),
		orchestrator.WithEdgeAddrs6(opt.EdgeAddrs6),
		orchestrator.WithHomeMarker(opt.HomeMarker),
		// T-607 advertise gate, RE-LIT off the seam: the RIB-survival guard is a COVERER
		// concern (tap-fed), but its withdraw VERDICT now arrives over CovererReport.
		// MEMBER_EDGE and is rebuilt into cp.presence; memberSuppressed answers the render
		// gate from that presence map exactly as the monolith's g.ShouldWithdraw did. Until
		// a coverer feeds a view the gate fails static (advertises) — never blackholes.
		orchestrator.WithAdvertiseGate(cp.memberSuppressed),
		// Generation is now a PER-EDGE in-memory monotonic counter (seeded wall-clock-ms
		// per instance): zero per-render etcd writes. The agent only needs per-edge
		// monotonicity for its BaseGeneration gap-detection; a seed jump across a
		// restart/handoff triggers a full DESIRED_STATE resync (the safe backstop).
		orchestrator.WithLiveness(func(e model.EdgeID) bool { return mon == nil || !mon.IsDead(e) }),
		orchestrator.WithDoubleDeathAlarm(ddAlarm),
		orchestrator.WithLogger(log),
	)

	// Failover trigger: a judged-dead edge re-homes its pools; a revived edge
	// cleans up residual state (non-preemptive, §5.8). Errors are logged — the
	// reconcile/reclaim backstops cover anything left half-done.
	//
	// Under sharding (cp.edgever wired, L-07) failover is ASYNC EVACUATION: the
	// dead edge's pools are enqueued for the level-triggered ReconcilePool, which
	// only mutates etcd; whichever replica holds each backup's stream realizes it
	// via the converge loop. This is the fix for the K=2 cross-replica gap — the
	// synchronous orch.FailoverEdge can only push to streams on THIS replica.
	failover := func(ctx context.Context, edge model.EdgeID) {
		opt.Metrics.Failover()
		// The dead edge's pools are moving off it; drop its per-member loss sustain
		// windows so a stale >30% spell can't later fire a redundant per-pool migrate
		// against an edge that no longer homes those members (§4.2.5). Idempotent even
		// without this (migrateMemberPool re-checks the home), but keeps state tidy.
		if cp.Loss != nil {
			cp.Loss.Forget(edge)
		}
		if cp.edgever != nil {
			// BULK fast path (L-07): re-home ALL the dead edge's pools in parallel +
			// ONE coalesced edgever bump per new-primary edge, instead of enqueuing each
			// for an independent ReconcilePool (per-pool same-key bump = CAS-conflict
			// storm → minutes-long blackhole). Leftovers (no live backup) fall back to
			// the per-pool path; redundancy is restored off the critical path.
			orch.FailoverEdgeBulk(ctx, edge, cp.enqueuePool)
			return
		}
		if err := orch.FailoverEdge(ctx, edge); err != nil {
			log.Warn("failover incomplete", "edge", edge, "err", err)
		}
	}
	revive := func(ctx context.Context, edge model.EdgeID) {
		// TIER-3 edge-up: the SLA outage window opened by edge-down closes. Revival is
		// non-preemptive (pools don't move back, §5.8) — this is edge-HEALTH only, not a
		// homing change. Async / Noop-safe. Also clears the stored death method.
		cp.emitEdgeUp(edge)
		if cp.edgever != nil {
			// Non-preemptive cleanup cross-replica: bump the revived edge's version so
			// its covering replica re-renders (withdrawing residual state), and re-
			// evaluate any pools still referencing it. It never reclaims a primary.
			cp.Orch.MarkEdge(ctx, edge)
			cp.enqueuePoolsFor(ctx, edge)
			return
		}
		if err := orch.CleanupRevived(ctx, edge); err != nil {
			log.Warn("revival cleanup incomplete", "edge", edge, "err", err)
		}
	}
	// The churn fixes below apply ONLY under sharding (K>1 coverers). Single
	// controller (quorum<=1): every live agent reports to the one controller and
	// there are no peers to poison, so keep the original immediate behaviour —
	// nil reporting gate + zero startup grace.
	var reporting func(model.EdgeID) bool
	var startupGrace time.Duration
	if opt.LivenessQuorum > 1 {
		// Heartbeat-stale only for edges whose covering coverer is connected to THIS
		// server replica: under K-coverage an edge taps to all its coverers but heartbeats
		// to one, so a replica whose coverer does not hold this edge must not fail it for
		// going silent. Re-gated on covering-coverer connectivity (fan.IsSubscribed).
		reporting = fan.IsSubscribed
		// First-convergence grace: for LivenessGrace after this replica (re)starts,
		// its tap is still establishing — suppress hard-death verdicts and outbound
		// votes so a reviving coverer doesn't falsely fail healthy edges or poison
		// peers' quorum.
		startupGrace = opt.LivenessGrace
	}
	mon = liveness.New(opt.LivenessGrace, failover,
		liveness.WithRevive(revive),
		// TIER-1 metering-stale notify: the monitor reports each edge's heartbeat-stale
		// LEVEL on every Tick; emitMeteringStale converts it to an edge-triggered
		// metering-stale/metering-resumed event. Pure observability (no liveness logic
		// change); inert under the Noop emitter.
		liveness.WithMeteringStaleNotify(cp.emitMeteringStale),
		// TIER-3 death-method notify: the monitor reports each NEWLY-dead edge's death
		// METHOD ("hard-quorum"|"heartbeat-stale"|"soft-death") BEFORE failover. The
		// handler emits the ONE unsolicited "edge-down" event (the node-death FACT, so the
		// BSS can tell "1 node died with N pools" from N independent moves) and records the
		// method so the per-pool emitFailover events carry it too. Pure observability (no
		// liveness logic change); inert under the Noop emitter.
		liveness.WithDeathNotify(cp.onEdgeDeath),
		liveness.WithSoftDebounce(opt.LivenessSoftDebounce),
		liveness.WithHardDebounce(opt.LivenessHardDebounce),
		liveness.WithRestartGrace(opt.LivenessVPPRestartGrace),
		liveness.WithQuorum(opt.LivenessQuorum),
		liveness.WithSelfID(opt.SelfID),
		liveness.WithReporting(reporting),
		liveness.WithStartupGrace(startupGrace),
		liveness.WithLogger(log))

	cp.Orch = orch
	cp.Liveness = mon
	// §4.2.5 per-member loss policy: pct→basis-points (×100). onMigrate resolves
	// member→pool and gracefully promotes its backup; onAlert emits the Redpanda
	// edge-forwarding-degraded/-recovered event. Inert until an agent reports MemberLoss.
	cp.Loss = lossmon.New(
		uint16(opt.LossAlertPct*100), uint16(opt.LossMigratePct*100), opt.LossMigrateSustain,
		cp.migrateMemberPool,
		lossmon.WithClock(cp.now),
		lossmon.WithAlert(cp.emitForwardingDegraded),
		lossmon.WithLogger(log))
	cp.fan = fan // server-half desired-state fan (the ServerCoverer WATCH downlink)
	// COVERER ASSIGNMENT default: the agent's coverer set is derived from the SAME
	// connected-coverer HRW the desired-state routing uses (fan.assignmentFor) — so an agent
	// is told to home to the EXACT coverer the server routes its EDGE_DIRECTIVE through, with
	// that coverer's advertised agent-endpoint. No store: the connected Watch streams are the
	// source of truth. (Replaces the step-11 ctrlreg/etcd-backed assigner placeholder.)
	cp.covererFunc = func(_ context.Context, edge model.EdgeID) (model.CovererAssignment, bool, error) {
		a, ok := fan.assignmentFor(edge)
		return a, ok, nil
	}
	cp.scvr = &scvrProvider{cp: cp} // the server-half of the ServerCoverer Report/Register
	cp.onReport = onReport          // the server-half report processing the gRPC Report dispatches to
	cp.onRegister = onRegister      // the server-half register processing the gRPC Register dispatches to
	// Wire the unsolicited failover notification: at a node-failure auto-promote (the
	// async reconciler's PROMOTE step / the synchronous FailoverEdge drain) the
	// orchestrator calls this with (pool, deadOldPrimary, promotedBackup, generation)
	// and the control plane emits an UNSOLICITED "failover" event so the BSS learns its
	// pool→edge home moved without having issued a request. With the Noop emitter (no
	// brokers) this is inert.
	orch.SetFailoverNotify(cp.emitFailover)
	// Wire the TIER-3 backup/capacity + decommission-rehome notifies: the orchestrator
	// fires them at the provision-backup / drain decision points and the control plane
	// emits the matching unsolicited events. Inert under the Noop emitter (no brokers).
	orch.SetRedundancyNotify(cp.onRedundancyNotify)
	orch.SetRehomeNotify(func(id model.PoolID, from, to model.EdgeID) {
		cp.emitRehome(id, from, to, "decommission")
	})

	// NO in-process Watch consumer: the COVERER processes are the real consumers now —
	// they Watch over gRPC (covererServer.Watch) and relay each directive to their
	// agents. The server's downlink stops at the per-coverer Watch channel.
	return cp
}

// connectedCovererIDs is the connected-coverer membership (sorted keys of the fan's
// stream map) — the single source of truth that REPLACES ctrlreg for coverage. Cheap,
// no etcd. Used off the hot path (coverage compute / recompute) only.
func (cp *ControlPlane) connectedCovererIDs() []string {
	cp.fan.mu.Lock()
	defer cp.fan.mu.Unlock()
	return cp.fan.connectedCovererIDsLocked()
}

// scheduleCoverageRecompute coalesces a COVERAGE recompute request into the cap-1
// channel without blocking: a burst (fleet reconnect) collapses to one pending signal
// that RunCoverageRecompute debounces.
func (cp *ControlPlane) scheduleCoverageRecompute() {
	select {
	case cp.coverageRecalc <- struct{}{}:
	default:
	}
}

// recomputeCoverageAll re-derives each connected coverer's COVERAGE by HRW over the
// connected set + the REGISTRY edge universe and re-emits it. A connect SHRINKS / an
// evict GROWS every other coverer's covered set, so all must be refreshed. Best-effort:
// an absent/superseded stream is skipped by emitCoverage; an edge-universe read error
// just skips this pass (the next trigger / DriftSweep backstops).
func (cp *ControlPlane) recomputeCoverageAll(ctx context.Context) {
	ids := cp.connectedCovererIDs()
	if len(ids) == 0 {
		return
	}
	es, err := cp.Registry.EdgeIDs(ctx)
	if err != nil {
		cp.log.Warn("coverage recompute: edge universe read failed", "err", err)
		return
	}
	strs := make([]string, len(es))
	for i, e := range es {
		strs[i] = string(e)
	}
	for _, cid := range ids {
		covered := shard.CoveredEdges(cid, strs, ids, cp.coverageK)
		edges := make([]model.EdgeID, len(covered))
		for i, s := range covered {
			edges[i] = model.EdgeID(s)
		}
		cp.fan.emitCoverage(cid, edges)
	}
	// COVERAGE drives the coverers' taps; this drives the AGENTS: REHOME each edge whose
	// primary coverer moved (a coverer recovery/join is the case COVERAGE alone leaves the
	// agent stranded on its fallback while desired-state routes to the recovered primary).
	if n := cp.fan.rehomeChangedPrimaries(es); n > 0 {
		cp.log.Info("coverage recompute: rehomed edges whose primary coverer changed", "count", n)
	}
}

// RunCoverageRecompute is the debounced COVERAGE recompute loop: it waits for a
// scheduled trigger, then coalesces a burst by resetting a debounce timer until the
// membership/edge-universe is quiet for `debounce`, and re-emits COVERAGE to every
// connected coverer. Blocks; run in a goroutine.
func (cp *ControlPlane) RunCoverageRecompute(ctx context.Context, debounce time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-cp.coverageRecalc:
			t := time.NewTimer(debounce)
		coalesce:
			for {
				select {
				case <-cp.coverageRecalc:
					if !t.Stop() {
						<-t.C
					}
					t.Reset(debounce)
				case <-t.C:
					break coalesce
				case <-ctx.Done():
					t.Stop()
					return
				}
			}
			cp.recomputeCoverageAll(ctx)
		}
	}
}

// SetGRPCServer hands the ServerCoverer gRPC server to the control plane so Stop can
// bound its graceful shutdown (long-lived Watch streams). Call once after building gs.
func (cp *ControlPlane) SetGRPCServer(gs *grpc.Server) { cp.grpcServer = gs }

// PushRehome fans a coverer-assignment REHOME directive down the covering coverer's
// Watch channel (same downlink as desired-state), for the remote coverer to relay to
// its agent. Returns ErrNotSubscribed (benign) when no covering coverer is connected
// here. Available for a server-driven rehome-on-membership-change path.
func (cp *ControlPlane) PushRehome(edge model.EdgeID, a model.CovererAssignment) error {
	return cp.fan.PushRehome(edge, a)
}

// SetCoverer installs the coverer-assignment function the scvr seam's Register uses to
// compute an agent's coverer set (sharding, L-05/L-06). Called post-construction once the
// coverage reconciler exists (cmd startSharding) — mirrors grpcsrv.SetCoverer but feeds the
// seam's server-half rather than the agent-facing transport. Set before Serve (the register
// handler reads it), so the assignment after the §8 step3 register re-route stays intact.
func (cp *ControlPlane) SetCoverer(fn func(context.Context, model.EdgeID) (model.CovererAssignment, bool, error)) {
	cp.covererFunc = fn
}

// onRedundancyNotify maps the orchestrator's backup/capacity notify (one callback, a
// kind discriminator) onto the TIER-3 emit helpers. "backup-changed" emits BOTH
// backup-changed (the standby moved) and redundancy-regained (the recovery twin);
// "redundancy-lost" and "capacity-exhausted" map 1:1. Async / Noop-safe.
func (cp *ControlPlane) onRedundancyNotify(kind string, id model.PoolID, toEdge model.EdgeID, reason string) {
	switch kind {
	case "backup-changed":
		cp.emitBackupChanged(id, toEdge)
		cp.emitRedundancyRegained(id)
	case "redundancy-lost":
		cp.emitRedundancyLost(id, reason)
	case "capacity-exhausted":
		cp.emitCapacityExhausted(id)
	}
}

// expectedPoolSet is the set of pool IDs THIS controller expects edge to hold in its
// data plane right now: every pool homed to edge (as PRIMARY or BACKUP), gated by the
// RIB-survival guard's view of each pool's HOST members (poolLiveOnEdge): a pool whose
// host members are ALL trustworthily absent is excluded; bare-metal-only pools are never
// host-gated. This is the controller's side of the report-hash compare — the agent hashes
// the SAME set it materialized, so a steady-state edge matches exactly.
//
// It deliberately does NOT zero out on edge-level death (liveness.IsDead). The resync this
// drift triggers (RerenderEdge → renderAndPush) pushes poolsForHome+Backup REGARDLESS of
// liveness, and an edge's death re-homes its PRIMARY pools to the backup via the pivot,
// which empties poolsForHome naturally. Gating the EXPECTED side on IsDead while the PUSHED
// side isn't made a route-withdrawn-but-still-reporting edge — a crashed-bird flap: agent
// alive, /32s momentarily gone, so route-death but heartbeat-alive — drift FOREVER:
// expected=∅ vs the agent's still-materialized set, re-firing a full resync every sweep
// (observed: endless "report-hash drift persisted → full resync", runaway controller
// CPU/mem, placement 503s). Tracking poolsForHome keeps EXPECTED == PUSHED so it converges.
func (cp *ControlPlane) expectedPoolSet(ctx context.Context, edge model.EdgeID) ([]model.PoolID, error) {
	primary, err := cp.poolsForHome(ctx, edge)
	if err != nil {
		return nil, err
	}
	backup, err := cp.poolsForBackup(ctx, edge)
	if err != nil {
		return nil, err
	}
	out := make([]model.PoolID, 0, len(primary)+len(backup))
	add := func(pools []model.Pool) {
		for _, p := range pools {
			if cp.poolLiveOnEdge(edge, p) {
				out = append(out, p.ID)
			}
		}
	}
	add(primary)
	add(backup)
	return out, nil
}

// poolLiveOnEdge reports whether pool's members are LIVE on edge. The RIB-survival
// guard (the host-presence source) is a COVERER concern — it is tap-fed and does not
// live on the server — so cp has no Guard and this returns true UNCONDITIONALLY: the
// server cannot host-gate a pool by physical /32 presence (a regression vs the
// monolith's guard until the coverer feeds member presence over the seam). Kept as a
// method so expectedPoolSet stays structurally identical to the monolith.
func (cp *ControlPlane) poolLiveOnEdge(_ model.EdgeID, _ model.Pool) bool {
	return true
}

// driftCheck is the report HOT PATH and is O(1): it just records the agent's
// reported InstalledPoolHash (and the arrival time) in an in-memory per-edge map,
// guarded by driftMu. It performs NO etcd reads and NO expected-set computation —
// the O(N) expected-set recompute moved off the per-report path entirely and is
// now bounded to the periodic DriftSweep (≤ once per sweep per edge). The actual
// expected-vs-reported compare + full-resync decision lives in DriftSweep.
func (cp *ControlPlane) driftCheck(_ context.Context, r model.EdgeReport) {
	cp.driftMu.Lock()
	cp.driftReported[r.EdgeID] = reportedHash{hash: r.InstalledPoolHash, at: cp.now(), seen: true}
	cp.driftMu.Unlock()
}

// DriftSweep is the bounded backstop for the report-hash drift check. For every
// edge that has REPORTED since the last sweep it computes expectedPoolSet(edge)
// ONCE — so the O(N) work happens at most once per sweep per edge (3 edges), never
// once per report — and compares model.PoolSetHash(expected) to the edge's last
// stored reported hash.
//
// PERSISTENCE: a per-edge consecutive-mismatch streak debounces transient agent
// catch-up lag. On expected==reported the streak resets to 0. On mismatch it
// increments; only when it reaches driftStreakThreshold (≥2 consecutive sweeps =
// the agent genuinely is not converging, not just briefly lagging a burst of
// creates) does it fire a full DESIRED_STATE resync (RerenderEdge) and reset the
// streak. Best-effort: an expected-set error just skips that edge this sweep.
func (cp *ControlPlane) DriftSweep(ctx context.Context) {
	// Snapshot the edges that reported since the last sweep and clear their seen
	// flags, holding driftMu only briefly (not across the O(N) Yugabyte reads below).
	cp.driftMu.Lock()
	pending := make(map[model.EdgeID]uint64, len(cp.driftReported))
	for edge, rh := range cp.driftReported {
		if rh.seen {
			pending[edge] = rh.hash
			rh.seen = false
			cp.driftReported[edge] = rh
		}
	}
	cp.driftMu.Unlock()

	for edge, reported := range pending {
		expected, err := cp.expectedPoolSet(ctx, edge)
		if err != nil {
			cp.log.Warn("drift sweep: expected pool set failed", "edge", edge, "err", err)
			continue // retry next sweep; streak untouched
		}
		want := model.PoolSetHash(expected)
		if want == reported {
			cp.resetDriftStreak(edge) // converged
			continue
		}
		streak := cp.bumpDriftStreak(edge)
		if cp.metrics != nil {
			cp.metrics.ProgramDrift(edge, "pool_set_hash", len(expected))
		}
		if streak < cp.driftStreakThreshold {
			cp.log.Info("report-hash drift (lagging) → deferring resync",
				"edge", edge, "expected_hash", want, "installed_hash", reported,
				"expected_pools", len(expected), "streak", streak, "threshold", cp.driftStreakThreshold)
			continue // transient catch-up lag; wait for persistence
		}
		cp.log.Info("report-hash drift persisted → full resync",
			"edge", edge, "expected_hash", want, "installed_hash", reported,
			"expected_pools", len(expected), "streak", streak)
		cp.resetDriftStreak(edge)
		if err := cp.Orch.RerenderEdge(ctx, edge); err != nil {
			cp.log.Warn("report-hash resync push failed", "edge", edge, "err", err)
		}
	}
}

func (cp *ControlPlane) bumpDriftStreak(edge model.EdgeID) int {
	cp.driftMu.Lock()
	defer cp.driftMu.Unlock()
	cp.driftStreak[edge]++
	return cp.driftStreak[edge]
}

func (cp *ControlPlane) resetDriftStreak(edge model.EdgeID) {
	cp.driftMu.Lock()
	delete(cp.driftStreak, edge)
	cp.driftMu.Unlock()
}

// RunDriftSweep runs DriftSweep every interval until ctx is cancelled — the bounded
// O(N) report-hash backstop (modeled on RunLiveness/RunReclaim). Blocks; run in a
// goroutine. Default interval ~30s keeps the per-edge expected-set recompute rare.
func (cp *ControlPlane) RunDriftSweep(ctx context.Context, interval time.Duration) {
	cp.runLoop(ctx, interval, cp.DriftSweep)
}

// RunResultTimeoutSweep emits a "failed"(reason=timeout) API-result event for every
// pending whose data-plane realization has not converged within `timeout`, then
// removes it — the backstop for an op the home edge never confirms. It runs every
// `interval` until ctx is cancelled (modeled on RunDriftSweep). It deliberately does
// NOT fire on a single delivery loss: a dropped delta is transient (the report-hash
// drift backstop + the agent's resync recover it), so only the elapsed timeout — not
// a missed report — resolves a pending as failed. With the Noop emitter the events
// are dropped, but the sweep still GC-s stale pendings (bounded map). Blocks; run in
// a goroutine.
func (cp *ControlPlane) RunResultTimeoutSweep(ctx context.Context, interval, timeout time.Duration) {
	cp.runLoop(ctx, interval, func(ctx context.Context) { cp.resultTimeoutSweep(ctx, timeout) })
}

// resultTimeoutSweep is one pass of RunResultTimeoutSweep: snapshot + delete the
// timed-out pendings under a brief lock, then emit OUTSIDE the lock (async produce).
func (cp *ControlPlane) resultTimeoutSweep(ctx context.Context, timeout time.Duration) {
	now := cp.now()
	var expired []pendingResult
	cp.pendingMu.Lock()
	for id, p := range cp.pending {
		if now.Sub(p.createdAt) >= timeout {
			expired = append(expired, p)
			delete(cp.pending, id)
		}
	}
	cp.pendingMu.Unlock()
	for _, p := range expired {
		cp.log.Warn("api-result pending timed out (data plane did not converge)",
			"request_id", p.requestID, "op", p.op, "pool", p.poolID, "edge", p.primaryEdge, "generation", p.generation)
		ev := apiresult.Event{
			RequestID:  p.requestID,
			Op:         p.op,
			PoolID:     uint64(p.poolID),
			Edge:       string(p.primaryEdge),
			Outcome:    apiresult.OutcomeFailed,
			Reason:     "timeout",
			Generation: p.generation,
			TSUnixMs:   now.UnixMilli(),
		}
		decoratePendingEvent(&ev, p) // carry the migrate from_edge / rate basis on a timeout too
		// context.Background(), NOT the caller ctx: the franz-go Produce is ASYNC, so by the
		// time it runs the onReport/sweep request ctx may be canceled → "context canceled"
		// and the converged/failed event is dropped. Mirrors emitFailover/member-up etc.
		cp.emitter.Emit(context.Background(), ev)
	}
}

// Stop evicts every connected coverer Watch stream (closing each done so a blocked
// emit releases with ErrNotSubscribed) and then bounds the ServerCoverer gRPC
// server's graceful shutdown: GracefulStop waits for ALL active streams, and the
// coverer Watch streams are long-lived, so an unbounded wait would deadlock shutdown
// forever. After the grace window the server is stopped hard; coverers reconnect to
// another replica and re-converge via initialCovererSync.
func (cp *ControlPlane) Stop() {
	// Evict all coverer streams so the pumps return and any blocked emit unblocks.
	if cp.fan != nil {
		cp.fan.mu.Lock()
		for id, s := range cp.fan.streams {
			close(s.done)
			delete(cp.fan.streams, id)
		}
		cp.fan.mu.Unlock()
	}
	if cp.grpcServer == nil {
		return
	}
	done := make(chan struct{})
	go func() { cp.grpcServer.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		cp.log.Warn("graceful stop timed out (long-lived coverer streams); forcing stop")
		cp.grpcServer.Stop()
	}
}

// RunReclaim runs the hung-token reclaim loop (§4.3) every interval until ctx is
// cancelled. Safe to run on every replica (each return is a revision-gated txn).
// Blocks; run in a goroutine.
// runLoop ticks fn every interval until ctx is cancelled — the shared skeleton
// for the control plane's periodic reconcilers (reclaim / liveness / account /
// program / tap-view / action-expiry / metrics). Blocks; run in a goroutine.
func (cp *ControlPlane) runLoop(ctx context.Context, interval time.Duration, fn func(context.Context)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn(ctx)
		}
	}
}

func (cp *ControlPlane) RunReclaim(ctx context.Context, interval time.Duration) {
	cp.runLoop(ctx, interval, func(ctx context.Context) {
		n, err := cp.Ledger.Reclaim(ctx)
		if err != nil {
			cp.log.Warn("token reclaim failed", "err", err)
			return
		}
		if n > 0 {
			cp.metrics.TokensReclaimed(n)
			cp.log.Info("reclaimed hung tokens", "count", n)
		}
	})
}

// RunLiveness drives the failover trigger: every interval it evaluates tracked
// edges and fails over those judged dead (hard-down, or heartbeat older than the
// grace, §5.9/§6.5). Blocks; run in a goroutine. The interval should be well
// below the grace so a dead edge is caught promptly.
func (cp *ControlPlane) RunLiveness(ctx context.Context, interval time.Duration) {
	cp.runLoop(ctx, interval, func(ctx context.Context) { cp.Liveness.Tick(ctx) })
}

// HardDown / HardUp feed tap-derived hard-death signals (PeerDown / canary
// withdrawal, §6.5) into the liveness monitor. The RIB-tap adapter calls these
// when wired; until then the heartbeat path drives failover on its own. Under
// sharding (a voter is set) they also publish/clear this replica's etcd death
// vote so peer coverers can corroborate (L-03); the publish is best-effort and
// async so it never blocks the tap's hot path.
func (cp *ControlPlane) HardDown(edge model.EdgeID) {
	cp.Liveness.HardDown(edge)
	// Startup grace (L-03 churn fix): record the local vote but do NOT publish it
	// while this replica's tap is still converging — a reviving coverer's premature
	// down-votes must not reach peer coverers' quorum (that falsely failed healthy
	// edges in the 2-replica e2e). hardDead is gated too, so we won't self-fire on
	// a still-converging tap either; once ready, a genuinely-down edge fires (the
	// recorded vote + a peer vote reach quorum).
	if cp.Liveness.Ready() {
		cp.publishVote(edge, true)
	} else {
		cp.log.Info("startup grace: holding tap death vote (tap converging, L-03)", "edge", edge)
	}
}

// HardUp clears a hard-down (PeerUp).
func (cp *ControlPlane) HardUp(edge model.EdgeID) {
	cp.Liveness.HardUp(edge)
	cp.publishVote(edge, false)
}

// SetVoter installs the death-vote publisher (sharding, L-03) — call before the
// tap starts. nil leaves the controller in single-vote mode.
func (cp *ControlPlane) SetVoter(v *deathvote.Voter) { cp.voter = v }

func (cp *ControlPlane) publishVote(edge model.EdgeID, down bool) {
	if cp.voter == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var err error
		if down {
			err = cp.voter.Down(ctx, edge)
		} else {
			err = cp.voter.Up(ctx, edge)
		}
		if err != nil {
			cp.log.Warn("death vote publish failed", "edge", edge, "down", down, "err", err)
		}
	}()
}

// RunDeathVotes consumes peer coverers' votes from the voter's Watch and feeds
// them into the liveness monitor so this replica reaches the corroboration quorum
// (L-03). Blocks until ctx is done; run in a goroutine when sharding is on.
func (cp *ControlPlane) RunDeathVotes(ctx context.Context) error {
	if cp.voter == nil {
		return nil
	}
	ch, err := cp.voter.Watch(ctx)
	if err != nil {
		return err
	}
	for ev := range ch {
		// Monitor.Vote ignores our own id, so peer votes are what move the quorum.
		cp.Liveness.Vote(ev.Edge, ev.Coverer, ev.Down)
	}
	return nil
}

// SetAPIResultEmitter wires the Redpanda API-result emitter (the BSS-facing async
// convergence stream). Call once at startup. nil or apiresult.Noop leaves the
// feature DISABLED — no events are produced and the control plane behaves exactly as
// it did before (the cmd passes Noop when no brokers are configured). Not concurrent
// with serving: set it before agents arrive.
func (cp *ControlPlane) SetAPIResultEmitter(e apiresult.Emitter) {
	if e == nil {
		e = apiresult.Noop{}
	}
	cp.emitter = e
}

// RegisterPending records an in-flight API operation so its eventual data-plane
// realization can be reported to the BSS (a "converged" event when the home edge's
// report echoes generation >= the pushed generation, or a "failed"/timeout event
// from the sweep). Called by the admin handler AFTER a successful synchronous
// create/update/destroy. A zero/empty edge or empty requestID is dropped (nothing to
// resolve against): with the Noop emitter this whole path is inert anyway, but the
// registry stays bounded and the sweep has nothing to chew on. generation 0 (a
// cross-shard withdraw with no local render) registers a pending that only the
// timeout sweep can resolve — the report-echo gate (>=0 is always true) would resolve
// it on the FIRST report from that edge, which is acceptable (the withdraw already
// landed synchronously for a local home; a cross-shard one is gated separately).
func (cp *ControlPlane) RegisterPending(requestID, op string, poolID model.PoolID, edge model.EdgeID, generation uint64) {
	if requestID == "" || edge == "" {
		return
	}
	cp.pendingMu.Lock()
	cp.pending[requestID] = pendingResult{
		requestID:   requestID,
		op:          op,
		poolID:      poolID,
		primaryEdge: edge,
		generation:  generation,
		createdAt:   cp.now(),
	}
	cp.pendingMu.Unlock()
}

// registerPendingEnriched is RegisterPending plus the TIER-4 enrichment carried onto the
// eventual "converged" event: fromEdge (the old primary, for a migrate's from_edge) and
// rate (the granted CIR/tokens/billing basis, for a create/update). Either may be the
// zero value / nil when not applicable. Same drop rules as RegisterPending (empty
// requestID/edge → dropped); inert under the Noop emitter.
func (cp *ControlPlane) registerPendingEnriched(requestID, op string, poolID model.PoolID, edge, fromEdge model.EdgeID, generation uint64, rate *rateBasis) {
	if requestID == "" || edge == "" {
		return
	}
	cp.pendingMu.Lock()
	cp.pending[requestID] = pendingResult{
		requestID:   requestID,
		op:          op,
		poolID:      poolID,
		primaryEdge: edge,
		generation:  generation,
		createdAt:   cp.now(),
		fromEdge:    fromEdge,
		rate:        rate,
	}
	cp.pendingMu.Unlock()
}

// rateBasisForPool derives the TIER-4 rate/billing basis from a granted pool + its token
// cost: the egress CIR (kbps) drives the cost; CIR==0 is the unlimited "95th-percentile"
// pool (~0 tokens + a 100G count-only placeholder policer — neither tokens nor the
// metered rate reveal the basis, so unlimited is flagged explicitly). Returns nil for a
// non-rate-limit (control/blackhole) pool — there is no committed rate to report.
func rateBasisForPool(p model.Pool, tokens int64) *rateBasis {
	if p.Action.Kind != model.ActionRateLimit {
		return nil
	}
	return &rateBasis{
		cirKbps:        p.EgressRate.CIR,
		ingressCIRKbps: p.IngressRate.CIR,
		tokens:         tokens,
		unlimited:      p.EgressRate.CIR == 0,
	}
}

// resolvePending is the report hot-path resolver: for every pending whose home edge
// is the reporting edge AND whose pushed generation the agent has now applied
// (r.Generation >= pending.generation), it emits a "converged" event and removes the
// pending. It snapshots the resolved set under a brief lock and emits OUTSIDE the
// lock; the emit itself is async (franz-go Produce returns immediately), so the
// report path never blocks on the broker. O(pending) per report — pending is the
// count of UNCONVERGED ops (steady state ~0), not all pools.
func (cp *ControlPlane) resolvePending(ctx context.Context, r model.EdgeReport) {
	var resolved []pendingResult
	cp.pendingMu.Lock()
	for id, p := range cp.pending {
		if p.primaryEdge == r.EdgeID && r.Generation >= p.generation {
			resolved = append(resolved, p)
			delete(cp.pending, id)
		}
	}
	cp.pendingMu.Unlock()
	for _, p := range resolved {
		ev := apiresult.Event{
			RequestID:  p.requestID,
			Op:         p.op,
			PoolID:     uint64(p.poolID),
			Edge:       string(p.primaryEdge),
			Outcome:    apiresult.OutcomeConverged,
			Generation: p.generation,
			TSUnixMs:   cp.now().UnixMilli(),
		}
		decoratePendingEvent(&ev, p) // TIER-4: migrate from_edge + create/update rate basis
		// context.Background(), NOT the caller ctx: the franz-go Produce is ASYNC, so by the
		// time it runs the onReport/sweep request ctx may be canceled → "context canceled"
		// and the converged/failed event is dropped. Mirrors emitFailover/member-up etc.
		cp.emitter.Emit(context.Background(), ev)
	}
}

// decoratePendingEvent stamps the TIER-4 enrichment a pendingResult carries onto its
// resolved api-result Event: the migrate's from_edge (the old primary the pool moved off,
// so the migrate event has From/To like a failover, not just Edge=newPrimary) and a
// create/update's granted rate basis (cir_kbps / ingress_cir_kbps / tokens / billing_mode
// / unlimited) so an event-only BSS can rate the pool. No-op when neither is set.
func decoratePendingEvent(ev *apiresult.Event, p pendingResult) {
	if p.fromEdge != "" {
		ev.FromEdge = string(p.fromEdge)
		ev.ToEdge = string(p.primaryEdge) // the migrate's new primary, mirroring failover From/To
	}
	if p.rate != nil {
		ev.CIRKbps = p.rate.cirKbps
		ev.IngressCIRKbps = p.rate.ingressCIRKbps
		ev.Tokens = p.rate.tokens
		ev.Unlimited = p.rate.unlimited
		if p.rate.unlimited {
			ev.BillingMode = "95th-pct"
		} else {
			ev.BillingMode = "cir"
		}
	}
}

// emitFailover ships an UNSOLICITED, controller-initiated "failover" event: a node
// failure auto-promoted a pool's backup, so the pool's home moved from→to WITHOUT any
// BSS request. The BSS discriminates this from a request-correlated API-result by Op
// ("failover") / RequestID (""): there is NO converged/timeout handshake — it is a
// NOTIFICATION emitted immediately at the auto-promote decision point so the BSS learns
// the home moved. The emit is async (franz-go Produce returns at once), so it NEVER
// blocks the failover path; with the Noop emitter (no brokers) it is a no-op. The
// orchestrator's onFailover calls this once per pool moved (keyed by pool_id).
func (cp *ControlPlane) emitFailover(id model.PoolID, from, to model.EdgeID, generation uint64) {
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:        "failover",
		Source:    "controller",
		RequestID: "", // unsolicited — no request to correlate
		PoolID:    uint64(id),
		FromEdge:  string(from),
		ToEdge:    string(to),
		// TIER-3: carry the real death method the liveness monitor reported for the dead
		// old primary ("hard-quorum"|"heartbeat-stale"|"soft-death") instead of a hardcoded
		// "node-failure", so the per-pool move and the edge-down event agree on the cause.
		// Falls back to "node-failure" when no monitor verdict was recorded for the edge
		// (e.g. a manual FailoverEdge drain with no preceding Tick death).
		Reason:     cp.deathReasonFor(from),
		Generation: generation,
		TSUnixMs:   cp.now().UnixMilli(),
	})
}

// deathReasonFor returns the death METHOD the liveness monitor most recently reported
// for edge (set by onEdgeDeath), or "node-failure" when none was recorded — so a
// failover triggered without a monitor verdict (a manual drain) still carries a
// meaningful reason. Read-only; guarded by obsMu.
func (cp *ControlPlane) deathReasonFor(edge model.EdgeID) string {
	cp.obsMu.Lock()
	m := cp.deathMethodByEdge[edge]
	cp.obsMu.Unlock()
	if m == "" {
		return "node-failure"
	}
	return m
}

// onEdgeDeath is the liveness monitor's death-method notify (WithDeathNotify): fired
// once per NEWLY-dead edge BEFORE failover, carrying the method that tripped. It (a)
// records the method so the subsequent per-pool emitFailover events stamp the SAME
// cause, and (b) emits the ONE unsolicited "edge-down" event — the node-death FACT as a
// single event, so the BSS can distinguish "1 node died with N pools" from N
// independent moves. Async / Noop-safe; never blocks the monitor's Tick.
func (cp *ControlPlane) onEdgeDeath(edge model.EdgeID, method string) {
	if method == "" {
		method = "node-failure"
	}
	cp.obsMu.Lock()
	cp.deathMethodByEdge[edge] = method
	cp.obsMu.Unlock()
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:        "edge-down",
		Source:    "controller",
		RequestID: "", // unsolicited — no request to correlate
		Edge:      string(edge),
		Reason:    method,
		TSUnixMs:  cp.now().UnixMilli(),
	})
}

// emitEdgeUp ships the TIER-3 "edge-up" event at the liveness revive (the dead edge
// reports/peers again): the SLA outage window opened by edge-down closes. Revival is
// non-preemptive (§5.8) — pools do NOT move back — so this is edge-HEALTH only, not a
// homing change (no from/to). It also clears the recorded death method. Async /
// Noop-safe; keyed by edge (via pool_id 0 → a per-edge stream).
func (cp *ControlPlane) emitEdgeUp(edge model.EdgeID) {
	cp.obsMu.Lock()
	delete(cp.deathMethodByEdge, edge)
	cp.obsMu.Unlock()
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:        "edge-up",
		Source:    "controller",
		RequestID: "",
		Edge:      string(edge),
		Reason:    "revived",
		TSUnixMs:  cp.now().UnixMilli(),
	})
}

// emitMemberDown ships an UNSOLICITED, controller-initiated "member-down" event: a
// pool MEMBER's host /32 (/128) left its home edge's physical RIB and the controller
// trustworthily confirms its absence — the authoritative member-liveness verdict
// (DESIGN-liveness: a member's host-route presence in the home edge's RIB via BGP IS
// its liveness; route-withdrawal is the veto). Like emitFailover it is a NOTIFICATION,
// not a request: Op="member-down", Source="controller", RequestID="", no
// converged/timeout handshake. The emit is async (franz-go Produce returns at once),
// so it NEVER blocks the tap/host-change path; with the Noop emitter (no brokers) it
// is a no-op. Keyed by member_prefix so a member's down/up events stay ordered.
//
// GRANULARITY NOTE: the K=2 death-vote quorum (deathvote / liveness.Monitor) is
// per-EDGE (a node), keyed by model.EdgeID — there is NO per-member /32 vote. So the
// member-presence signal used here is the host /32's withdrawal the controller
// observes via its OWN RIB tap, gated on the RIB-survival guard's trustworthy-absence
// verdict (view valid ∧ EOR ∧ host absent = ShouldWithdraw) — the same route-withdrawal
// veto the anchor-suppression path keys off. That is the closest correct member-level
// signal; reason is "route-withdrawal".
func (cp *ControlPlane) emitMemberDown(id model.PoolID, edge model.EdgeID, member netip.Prefix, reason string) {
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:           "member-down",
		Source:       "controller",
		RequestID:    "", // unsolicited — no request to correlate
		PoolID:       uint64(id),
		Edge:         string(edge),
		MemberPrefix: member.String(),
		Reason:       reason,
		TSUnixMs:     cp.now().UnixMilli(),
	})
}

// emitMemberUp is emitMemberDown's recovery twin: the member's host /32 (/128) is
// re-announced / re-confirmed present in its home edge's RIB. Op="member-up", same
// unsolicited shape (Source="controller", RequestID="", no handshake). Reason is left
// empty (a recovery has no death method). Keyed by member_prefix.
func (cp *ControlPlane) emitMemberUp(id model.PoolID, edge model.EdgeID, member netip.Prefix) {
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:           "member-up",
		Source:       "controller",
		RequestID:    "", // unsolicited — no request to correlate
		PoolID:       uint64(id),
		Edge:         string(edge),
		MemberPrefix: member.String(),
		TSUnixMs:     cp.now().UnixMilli(),
	})
}

// --- TIER 1 + TIER 2 unsolicited observability emits ---------------------------
//
// Every helper below mirrors emitFailover: it ships an UNSOLICITED, controller-
// initiated event (Source="controller", RequestID="" — no request to correlate, no
// converged/timeout handshake), the emit is async (franz-go Produce returns at once
// so it never blocks the reconciler/report path), and with the Noop emitter (no
// brokers) it is a no-op (zero behaviour change). They EXTEND the open op set.

// emitProgramDrift ships a TIER-1 data-plane drift event at the B-02 alarm point
// (ReconcileProgram, past the debounce streak): op is "delivery-loss" (a push never
// reached the agent → member billed-as-live but not enforced) or "program-drift"
// (VPP installed fewer policers/sessions than told). Gap carries the count magnitude.
// Keyed by pool_id (0 here → a single per-edge stream is acceptable; these are rare).
func (cp *ControlPlane) emitProgramDrift(d ProgramDrift) {
	op := d.Kind // "delivery-loss" | "program-drift"
	if op == "" {
		return
	}
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:        op,
		Source:    "controller",
		RequestID: "",
		Edge:      string(d.Edge),
		Reason:    op,
		Gap:       d.Gap,
		TSUnixMs:  cp.now().UnixMilli(),
	})
}

// emitAnchorMismatch ships TIER-1 per-member apply-failure events at the L-04 alarm
// point (ReconcileAnchors): one "anchor-unprovisioned" per Unprovisioned[] prefix (the
// member is assigned here but its /32 is not advertised → no data path) and one
// "anchor-rogue" per Rogue[] prefix (L advertises a /32 no pool homes here). Keyed by
// member_prefix so a member's events stay ordered.
func (cp *ControlPlane) emitAnchorMismatch(m AnchorMismatch) {
	now := cp.now().UnixMilli()
	for _, p := range m.Unprovisioned {
		cp.emitter.Emit(context.Background(), apiresult.Event{
			Op:           "anchor-unprovisioned",
			Source:       "controller",
			RequestID:    "",
			Edge:         string(m.Edge),
			MemberPrefix: p.String(),
			Reason:       "unprovisioned",
			TSUnixMs:     now,
		})
	}
	for _, p := range m.Rogue {
		cp.emitter.Emit(context.Background(), apiresult.Event{
			Op:           "anchor-rogue",
			Source:       "controller",
			RequestID:    "",
			Edge:         string(m.Edge),
			MemberPrefix: p.String(),
			Reason:       "rogue",
			TSUnixMs:     now,
		})
	}
}

// emitDoubleDeath ships the TIER-2 "pool-double-death" event at the C-04 ddAlarm: the
// pool lost all live homes. failOpen ⇒ "fail-open" (pool destroyed, billing stops);
// !failOpen ⇒ "fail-close" (degraded, homeless). Keyed by pool_id.
func (cp *ControlPlane) emitDoubleDeath(id model.PoolID, failOpen bool) {
	reason := "fail-close"
	if failOpen {
		reason = "fail-open"
	}
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:        "pool-double-death",
		Source:    "controller",
		RequestID: "",
		PoolID:    uint64(id),
		Reason:    reason,
		TSUnixMs:  cp.now().UnixMilli(),
	})
}

// emitPoolExpired ships the TIER-2 "pool-expired" event at the T-706 TTL auto-destroy
// (ExpireActions → DestroyPool) — the path that, unlike the admin destroy, produces no
// api-result today. action is the suppression action kind whose TTL elapsed. Keyed by
// pool_id.
func (cp *ControlPlane) emitPoolExpired(id model.PoolID, action string) {
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:        "pool-expired",
		Source:    "controller",
		RequestID: "",
		PoolID:    uint64(id),
		Reason:    "ttl-expired",
		Action:    action,
		TSUnixMs:  cp.now().UnixMilli(),
	})
}

// emitMemberEvicted ships the TIER-2 "member-evicted" event at the cross-pool CIDR-
// overlap replace path (evict → RemoveMember): a member was displaced out of pool
// `displacedPool` (A) by `byPool` (the displacing caller, which alone gets Replaced[]
// in its sync body). PoolID is the DISPLACED pool; DisplacedByPool the displacing one.
// Keyed by member_prefix.
func (cp *ControlPlane) emitMemberEvicted(displacedPool model.PoolID, member netip.Prefix, byPool model.PoolID) {
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:              "member-evicted",
		Source:          "controller",
		RequestID:       "",
		PoolID:          uint64(displacedPool),
		MemberPrefix:    member.String(),
		DisplacedByPool: uint64(byPool),
		Reason:          "cross-pool-replace",
		TSUnixMs:        cp.now().UnixMilli(),
	})
}

// emitEdgeDataplane is the LEVEL→EDGE converter for the TIER-1 edge-dataplane health
// transition: dpDown is the CURRENT level ("is VPP reported dead while BGP/canary is
// up?"). It compares against the last-emitted level under obsMu and emits exactly one
// "edge-dataplane-down" (up→down) or "edge-dataplane-up" (down→up) on a CHANGE, nothing
// on a repeat. Keyed by pool_id 0 → a per-edge alarm stream. Edge-triggered: a steady
// stream of down-reports yields ONE down event.
func (cp *ControlPlane) emitEdgeDataplane(edge model.EdgeID, dpDown bool) {
	cp.obsMu.Lock()
	prev := cp.dpDownEdges[edge]
	if prev == dpDown {
		cp.obsMu.Unlock()
		return // no transition
	}
	cp.dpDownEdges[edge] = dpDown
	cp.obsMu.Unlock()

	op := "edge-dataplane-up"
	reason := "dataplane-recovered"
	if dpDown {
		op = "edge-dataplane-down"
		reason = "dataplane-down"
	}
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:        op,
		Source:    "controller",
		RequestID: "",
		Edge:      string(edge),
		Reason:    reason,
		TSUnixMs:  cp.now().UnixMilli(),
	})
}

// emitMeteringStale is the LEVEL→EDGE converter for the TIER-1 metering-stale
// transition fed by the liveness heartbeat-stale gate: stale is the CURRENT level. It
// emits exactly one "metering-stale" (fresh→stale) or "metering-resumed" (stale→fresh)
// on a CHANGE. Keyed by pool_id 0 → a per-edge stream. Edge-triggered (one event per
// transition, not per tick).
func (cp *ControlPlane) emitMeteringStale(edge model.EdgeID, stale bool) {
	cp.obsMu.Lock()
	prev := cp.meteringStaleEdge[edge]
	if prev == stale {
		cp.obsMu.Unlock()
		return // no transition
	}
	cp.meteringStaleEdge[edge] = stale
	cp.obsMu.Unlock()

	op := "metering-resumed"
	reason := "metering-resumed"
	if stale {
		op = "metering-stale"
		reason = "heartbeat-stale"
	}
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:        op,
		Source:    "controller",
		RequestID: "",
		Edge:      string(edge),
		Reason:    reason,
		TSUnixMs:  cp.now().UnixMilli(),
	})
}

// ingestMemberLoss folds an edge report's per-member forwarding-loss vector (§4.2.5)
// into the loss monitor. The vector is the edge's FULL set of members over the agent
// watermark; RunLoss.Tick then fires the alert / per-pool migrate. A report with no
// MemberLoss is a clean snapshot that recovers any member the edge previously flagged.
func (cp *ControlPlane) ingestMemberLoss(r model.EdgeReport) {
	if cp.Loss == nil {
		return
	}
	samples := make([]lossmon.Sample, 0, len(r.Health.MemberLoss))
	for _, ml := range r.Health.MemberLoss {
		samples = append(samples, lossmon.Sample{
			Member:  ml.Prefix,
			Dir:     ml.Dir,
			LossBps: ml.LossBps,
			Reason:  ml.TopDropReason,
		})
	}
	cp.Loss.Report(r.EdgeID, samples)
}

// RunLoss drives the §4.2.5 loss-policy sweep: each tick evaluates the per-member
// sustain windows and fires alert transitions + per-pool migrates. Mirrors RunLiveness.
func (cp *ControlPlane) RunLoss(ctx context.Context, interval time.Duration) {
	cp.runLoop(ctx, interval, func(ctx context.Context) { cp.Loss.Tick(ctx) })
}

// emitForwardingDegraded is the loss monitor's ALERT callback: it ships the unsolicited
// edge-forwarding-degraded (crossed the alert watermark) / edge-forwarding-recovered
// (dropped back under it) event for one member. The loss monitor already tracks the
// transition (fires only on a change), so this just emits. Async / Noop-safe.
func (cp *ControlPlane) emitForwardingDegraded(edge model.EdgeID, member netip.Prefix, dir model.Direction, lossBps uint16, reason string, degraded bool) {
	op := "edge-forwarding-recovered"
	if degraded {
		op = "edge-forwarding-degraded"
	}
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:           op,
		Source:       "controller",
		RequestID:    "",
		Edge:         string(edge),
		MemberPrefix: member.String(),
		Reason:       fmt.Sprintf("%s %s", dir, reason),
		LossBps:      lossBps,
		TSUnixMs:     cp.now().UnixMilli(),
	})
}

// migrateMemberPool is the loss monitor's MIGRATE callback: a member's loss stayed over
// the migrate watermark for the sustain window, so move its POOL off the lossy home
// (§4.2.5 — the affected pool only, NOT the whole edge; other members on this edge may
// forward fine). Resolve member→pool, then gracefully promote the pool's backup (先发新
// 后撤旧, the same planned-migrate primitive the admin API uses) and restore redundancy.
// A pool with no backup cannot migrate — logged (the alert already flagged it for a
// human); a member no longer homed on the reporting edge is a mid-migration race → skip.
func (cp *ControlPlane) migrateMemberPool(ctx context.Context, edge model.EdgeID, member netip.Prefix, dir model.Direction, lossBps uint16) {
	home, pool, ok, err := cp.memberHome(ctx, member)
	if err != nil {
		cp.log.Warn("loss-migrate: member→pool lookup failed", "edge", edge, "member", member, "err", err)
		return
	}
	if !ok {
		cp.log.Warn("loss-migrate: member has no claiming pool (stale loss report?)", "edge", edge, "member", member)
		return
	}
	if home != edge {
		// The pool no longer homes on the edge that reported the loss (already migrated,
		// or a transient src→home skew). Idempotent skip — nothing to move.
		cp.log.Info("loss-migrate: member not homed on reporting edge; skip", "edge", edge, "member", member, "home", home, "pool", pool)
		return
	}
	_, newPrimary, oldPrimary, gen, err := cp.Orch.PromoteBackupGen(ctx, pool)
	switch {
	case errors.Is(err, orchestrator.ErrNoBackup):
		cp.log.Warn("loss-migrate deferred: pool has no backup to promote onto", "edge", edge, "member", member, "pool", pool, "loss_bps", lossBps)
		return
	case errors.Is(err, orchestrator.ErrWithdrawIncomplete):
		cp.log.Warn("loss-migrate: old-primary withdraw incomplete (new primary live)", "pool", pool, "err", err)
	case err != nil:
		cp.log.Warn("loss-migrate: promote failed", "edge", edge, "pool", pool, "err", err)
		return
	}
	// Restore N+1 on a spare edge (best-effort; a missing spare is not a migrate failure).
	if _, perr := cp.Orch.ProvisionBackup(ctx, pool); perr != nil {
		cp.log.Warn("loss-migrate: backup not reprovisioned (redundancy not restored)", "pool", pool, "err", perr)
	}
	cp.log.Warn("per-member sustained loss → migrated pool off lossy home",
		"edge", edge, "member", member, "dir", dir, "loss_bps", lossBps, "pool", pool, "from", oldPrimary, "to", newPrimary)
	cp.emitForwardingMigrate(edge, member, pool, oldPrimary, newPrimary, lossBps, gen)
}

// emitForwardingMigrate ships the unsolicited migrate event for a loss-triggered
// per-pool move (§4.2.5). Op "migrate" with Source="controller" + Reason="forwarding-
// loss" distinguishes it from a request-correlated admin migrate; from_edge→to_edge +
// member_prefix + loss_bps make it actionable. Async / Noop-safe.
func (cp *ControlPlane) emitForwardingMigrate(edge model.EdgeID, member netip.Prefix, pool model.PoolID, from, to model.EdgeID, lossBps uint16, gen uint64) {
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:           "migrate",
		Source:       "controller",
		RequestID:    "",
		PoolID:       uint64(pool),
		Edge:         string(to),
		FromEdge:     string(from),
		ToEdge:       string(to),
		MemberPrefix: member.String(),
		Reason:       "forwarding-loss",
		LossBps:      lossBps,
		Generation:   gen,
		TSUnixMs:     cp.now().UnixMilli(),
	})
}

// --- TIER 3 edge/fleet-availability emits -------------------------------------
//
// Same unsolicited shape as the TIER-1/2 helpers (Source="controller", RequestID="",
// async, Noop-safe). They surface FLEET state the BSS cannot see from per-pool moves:
// edge inventory (register/deregister/capacity), backup redundancy, and fleet-wide
// capacity exhaustion.

// emitEdgeRegistered ships the TIER-3 "edge-registered" event for an edge's FIRST
// registration (a brand-new schedulable node joins the fleet). A restart re-register
// does NOT fire this — onRegister distinguishes them via the registry's RegisteredAt
// (a new node has none yet). capacityBps is the sellable NIC line rate. Keyed by edge.
func (cp *ControlPlane) emitEdgeRegistered(edge model.EdgeID, capacityBps int64) {
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:          "edge-registered",
		Source:      "controller",
		RequestID:   "",
		Edge:        string(edge),
		CapacityBps: capacityBps,
		TSUnixMs:    cp.now().UnixMilli(),
	})
}

// emitEdgeCapacityChanged ships the TIER-3 "edge-capacity-changed" event when an
// EXISTING edge re-registers with a DIFFERENT CapacityBps (sellable capacity moved →
// billing/oversell input). Emitted ONLY on an actual change so an identical re-register
// is silent (no spam). capacityBps is the NEW sellable line rate. Keyed by edge.
func (cp *ControlPlane) emitEdgeCapacityChanged(edge model.EdgeID, capacityBps int64) {
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:          "edge-capacity-changed",
		Source:      "controller",
		RequestID:   "",
		Edge:        string(edge),
		CapacityBps: capacityBps,
		TSUnixMs:    cp.now().UnixMilli(),
	})
}

// emitEdgeDeregistered ships the TIER-3 "edge-deregistered" event at a planned
// Decommission (the edge is removed from the schedulable pool and will not return).
// Distinct from edge-down (a death the edge may revive from). Keyed by edge.
// cleanupEdge purges an edge's per-edge tracking state when it is permanently removed
// (decommission/deregister), so these maps do not accumulate stale entries over the
// controller's lifetime (edge churn). Each map is purged under its own guard.
func (cp *ControlPlane) cleanupEdge(edge model.EdgeID) {
	cp.progStreakMu.Lock()
	delete(cp.progStreak, edge)
	cp.progStreakMu.Unlock()
	cp.driftMu.Lock()
	delete(cp.driftReported, edge)
	delete(cp.driftStreak, edge)
	cp.driftMu.Unlock()
	cp.obsMu.Lock()
	delete(cp.dpDownEdges, edge)
	delete(cp.meteringStaleEdge, edge)
	delete(cp.deathMethodByEdge, edge)
	cp.obsMu.Unlock()
	if cp.reports != nil {
		cp.reports.delete(edge)
	}
	if cp.Orch != nil {
		cp.Orch.CleanupEdge(edge)
	}
}

func (cp *ControlPlane) emitEdgeDeregistered(edge model.EdgeID) {
	cp.cleanupEdge(edge)
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:        "edge-deregistered",
		Source:    "controller",
		RequestID: "",
		Edge:      string(edge),
		Reason:    "decommission",
		TSUnixMs:  cp.now().UnixMilli(),
	})
}

// emitBackupChanged ships the TIER-3 "backup-changed" event when a pool's BACKUP home
// is (re)provisioned onto a fresh edge (asyncProvisionBackup placed a new backup). The
// pool's redundancy moved to a new standby. toEdge is the new backup. Keyed by pool_id.
func (cp *ControlPlane) emitBackupChanged(id model.PoolID, toEdge model.EdgeID) {
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:        "backup-changed",
		Source:    "controller",
		RequestID: "",
		PoolID:    uint64(id),
		ToEdge:    string(toEdge),
		TSUnixMs:  cp.now().UnixMilli(),
	})
}

// emitRedundancyLost ships the TIER-3 "redundancy-lost" event when a pool that wants a
// backup cannot get one — no spare capacity (StatusDegraded / ErrNoPlacement). The pool
// runs PRIMARY-ONLY (still serving, but a single failure now loses it). reason is e.g.
// "no-spare-capacity". Keyed by pool_id. emitRedundancyRegained is the recovery twin.
func (cp *ControlPlane) emitRedundancyLost(id model.PoolID, reason string) {
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:        "redundancy-lost",
		Source:    "controller",
		RequestID: "",
		PoolID:    uint64(id),
		Reason:    reason,
		TSUnixMs:  cp.now().UnixMilli(),
	})
}

// emitRedundancyRegained ships the TIER-3 "redundancy-regained" event when a pool that
// had lost its backup gets one again (a backup was successfully (re)provisioned). It is
// the recovery twin of redundancy-lost. NOTE: this is emitted alongside backup-changed
// whenever a backup is placed — it is cheap (the placement already happened), but it
// does NOT track whether the pool was PREVIOUSLY degraded (the reconciler does not carry
// that level), so a BSS pairs it to the last redundancy-lost for the same pool_id rather
// than relying on a strict transition here. Keyed by pool_id.
func (cp *ControlPlane) emitRedundancyRegained(id model.PoolID) {
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:        "redundancy-regained",
		Source:    "controller",
		RequestID: "",
		PoolID:    uint64(id),
		TSUnixMs:  cp.now().UnixMilli(),
	})
}

// emitCapacityExhausted ships the TIER-3 "capacity-exhausted" event: the scheduler
// returned ErrInsufficientCapacity (no edge has spare tokens), so NEW placements/backups
// will fail FLEET-WIDE until capacity is added. Distinct from per-pool redundancy-lost:
// this is the fleet-level signal. id is the pool whose placement tripped it (0 if none).
// Keyed by pool_id. (See the orchestrator note: the scheduler does not cleanly separate
// "fleet exhausted" from "this one pool couldn't place", so this fires whenever placement
// returns ErrInsufficientCapacity, with reason "no-capacity".)
func (cp *ControlPlane) emitCapacityExhausted(id model.PoolID) {
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:        "capacity-exhausted",
		Source:    "controller",
		RequestID: "",
		PoolID:    uint64(id),
		Reason:    "no-capacity",
		TSUnixMs:  cp.now().UnixMilli(),
	})
}

// emitRehome ships the TIER-3 "rehome" event for a SINGLE pool re-homed during a planned
// DECOMMISSION drain (drainEdgeGen): the pool's primary moved from the drained edge onto
// a new primary. The bulk "decommission" api-result still correlates the request_id; this
// is the per-pool DETAIL for per-edge billing reconciliation. reason is "decommission"
// (deliberately NOT relabeled as a node-failure failover). Keyed by pool_id.
func (cp *ControlPlane) emitRehome(id model.PoolID, from, to model.EdgeID, reason string) {
	cp.emitter.Emit(context.Background(), apiresult.Event{
		Op:        "rehome",
		Source:    "controller",
		RequestID: "",
		PoolID:    uint64(id),
		FromEdge:  string(from),
		ToEdge:    string(to),
		Reason:    reason,
		TSUnixMs:  cp.now().UnixMilli(),
	})
}

// SetEdgeVer enables the async failover evacuation path (L-07): it wires the
// per-edge desired/applied version store into the control plane AND the
// orchestrator. With it set, the failover/revive triggers switch from the
// synchronous replica-local drain to the level-triggered ReconcilePool + converge
// loop (run RunConverge and RunPoolReconcile in goroutines). Call once at startup
// before agents arrive (the cmd builds edgever from the etcd *Client, which
// NewControlPlane does not hold). nil leaves the synchronous path in effect.
func (cp *ControlPlane) SetEdgeVer(ev *edgever.Store) {
	cp.edgever = ev
	cp.Orch.SetEdgeVer(ev)
}

// HasEdgeVer reports whether the per-edge desired/applied version store is wired
// (L-07). The sharded cmd asserts this is true before agents arrive: without it the
// async failover evacuation (decide on one replica, deliver on another via the
// converge loop) is silently dead for a backup subscribed to a peer replica.
func (cp *ControlPlane) HasEdgeVer() bool { return cp.edgever != nil }

// SetYBStore wires the MANDATORY YugabyteDB-backed bulk pool/member store (+ its
// used-capacity provider) into the control plane AND the orchestrator. With it set, the
// create path's data write collapses to one ybstore.CreatePool ACID txn, placement
// capacity is served from the cache, and the render/drift/admin reads source pools FROM
// Yugabyte. etcd keeps only coordination (leader election, sharding, liveness, edgever).
// Call once at startup before agents arrive (the cmd does — the controller exits if
// Yugabyte is unavailable). The args are interfaces so tests can inject an in-memory
// double (ybstore.Mem); production passes *ybstore.Store + *ybstore.CapacityCache.
func (cp *ControlPlane) SetYBStore(yb orchestrator.YBStore, cap orchestrator.CapacityProvider) {
	cp.YB = yb
	cp.YBCap = cap
	cp.Orch.SetYBStore(yb, cap)
}

// poolsForHome returns the pools edge is PRIMARY for, from the MANDATORY Yugabyte
// bulk store — the single seam the drift/enqueue reads go through. cp.YB is always
// wired (the controller exits at startup without a live Yugabyte), so there is NO
// etcd-poolstore fallback: the create path writes ZERO etcd, so the etcd poolstore
// is empty in production and reading it would enumerate nothing (silent drift).
func (cp *ControlPlane) poolsForHome(ctx context.Context, edge model.EdgeID) ([]model.Pool, error) {
	return cp.YB.PoolsForHome(ctx, edge)
}

// poolsForBackup is poolsForHome's BACKUP twin — Yugabyte only (cp.YB always set).
func (cp *ControlPlane) poolsForBackup(ctx context.Context, edge model.EdgeID) ([]model.Pool, error) {
	return cp.YB.PoolsForBackup(ctx, edge)
}

// enqueuePool requests a reconcile pass for one pool (best-effort; a full queue is
// backstopped by the periodic sweep).
func (cp *ControlPlane) enqueuePool(id model.PoolID) {
	if cp.reconcileQ == nil {
		return
	}
	select {
	case cp.reconcileQ <- id:
	default: // a pending request already covers this; the sweep backstops a drop
	}
}

// enqueuePoolsFor enqueues every pool the edge is a home (primary or backup) of —
// the pools a death / revival / readiness change on that edge may need to act on.
func (cp *ControlPlane) enqueuePoolsFor(ctx context.Context, edge model.EdgeID) {
	primary, err := cp.poolsForHome(ctx, edge)
	if err != nil {
		cp.log.Warn("enqueue: pools-for-home failed", "edge", edge, "err", err)
	}
	backup, err := cp.poolsForBackup(ctx, edge)
	if err != nil {
		cp.log.Warn("enqueue: pools-for-backup failed", "edge", edge, "err", err)
	}
	for _, p := range primary {
		cp.enqueuePool(p.ID)
	}
	for _, p := range backup {
		cp.enqueuePool(p.ID)
	}
}

// RunConverge is the cross-replica delivery loop (L-07, the Nova-compute "realize
// desired state" half): it watches the per-edge desired version and, for every
// edge whose Subscribe stream THIS replica holds (IsSubscribed), re-renders +
// pushes that edge's complete desired state. So when any replica's reconciler
// bumps an edge's version (a failover moved a pool on/off it), whichever replica
// the agent is actually subscribed to delivers it — closing the K=2 gap where the
// reconciling replica could not push to a backup subscribed on a peer. Idempotent
// (complete-state render). Blocks until ctx is done; run in a goroutine when
// sharding is on. No-op without an edgever store.
func (cp *ControlPlane) RunConverge(ctx context.Context) error {
	if cp.edgever == nil {
		return nil
	}
	ch, err := cp.edgever.WatchDesired(ctx)
	if err != nil {
		return err
	}
	for ev := range ch {
		if !cp.fan.IsSubscribed(ev.Edge) {
			continue // a covering coverer on a peer replica owns delivery for this edge
		}
		if err := cp.Orch.RerenderEdge(ctx, ev.Edge); err != nil {
			cp.log.Warn("converge render failed", "edge", ev.Edge, "version", ev.Version, "err", err)
		}
	}
	return nil
}

// RunPoolReconcile drives the async failover state machine (L-07): it consumes
// reconcile requests (liveness death/revival + applied-version advances feed
// cp.reconcileQ) and, on a periodic sweep, re-evaluates every pool as the
// level-triggered backstop. Each ReconcilePool call takes one idempotent step;
// StatusActed/Conflict re-enqueue to continue the migration, StatusGated waits for
// the next applied-advance or sweep, terminal states settle. Safe on every replica
// (CAS-serialized on the record revision). Blocks until ctx is done; run in a
// goroutine when sharding is on. No-op without an edgever store.
func (cp *ControlPlane) RunPoolReconcile(ctx context.Context, sweepInterval time.Duration) {
	if cp.edgever == nil {
		return
	}
	t := time.NewTicker(sweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-cp.reconcileQ:
			cp.reconcileOne(ctx, id)
		case <-t.C:
			cp.sweepPools(ctx)
		}
	}
}

// reconcileOne runs one ReconcilePool step and schedules the follow-up.
func (cp *ControlPlane) reconcileOne(ctx context.Context, id model.PoolID) {
	st, err := cp.Orch.ReconcilePool(ctx, id)
	if err != nil {
		cp.log.Warn("pool reconcile step failed", "pool", id, "err", err)
	}
	switch st {
	case orchestrator.StatusActed, orchestrator.StatusConflict:
		cp.enqueuePool(id) // continue the migration / re-read after a peer advanced it
		// StatusGated: an applied-version advance (onReport) or the sweep re-triggers.
		// StatusHealthy/StatusDegraded: settle; only the sweep retries.
	}
}

// sweepPools re-evaluates every pool — the level-triggered backstop that catches
// anything a dropped enqueue or a missed event left mid-migration. It enumerates the
// authoritative pool set from the MANDATORY Yugabyte store. There is NO etcd-poolstore
// fallback: the create writes ZERO etcd, so the etcd poolstore enumerates ZERO pools
// in production — reading it would silently skip every Yugabyte pool (no failover/drift).
// cp.YB is always wired (the controller exits at startup without a live Yugabyte).
func (cp *ControlPlane) sweepPools(ctx context.Context) {
	ids, err := cp.YB.ListIDs(ctx)
	if err != nil {
		cp.log.Warn("reconcile sweep: list pool ids failed", "err", err)
		return
	}
	for _, id := range ids {
		cp.reconcileOne(ctx, id)
	}
}
