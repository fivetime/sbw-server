// Package orchestrator is the controller's pool-create transaction (controller
// §4.3/§5.1) — the capstone that ties placement, persistence, rendering and
// distribution together:
//
//	select homes (cached capacity) → persist record (ONE Yugabyte txn) → render → push
//
// HYBRID ARCHITECTURE (2026-06, step 2): the create path writes ZERO etcd. It does
// NOT reserve/commit tokens in the etcd ledger — placement reads each edge's
// remaining capacity from the in-memory CapacityCache (sellable − UsedByEdge, both
// from memory) — and the authoritative pool/member data AND the failover pivot are
// ONE Yugabyte ACID txn (pools row with version=1/retiring=false + one member row
// each). The failover pivot that used to be the per-create etcd poolstore Record is
// now the pools.version + pools.retiring COLUMNS: version is the optimistic-CAS token
// (replacing etcd's ModRevision) the reconciler advances on each pivot move, and it
// PERSISTS, so a crashed controller rebuilds it from Yugabyte. With NO per-create
// etcd write of any kind, a million-scale create burst no longer pegs etcd; etcd is
// left to pure coordination (leader election, sharding, liveness, registry). (Strict
// production no-oversell, when an edge is run near its ceiling, is a future Yugabyte
// capacity counter checked in the same create txn — see the TODO in CreatePoolNonce.)
//
// The FAILOVER/UPDATE paths are OPTIMISTIC too (cached-capacity placement, no etcd
// ledger reserve/commit); edgever bumps drive prompt cross-replica delivery. The async
// failover reconciler (reconcile.go) reads/CAS-writes the Yugabyte version pivot
// (GetForReconcile / UpdateCAS / DeleteCAS), never the etcd poolstore Record. Yugabyte
// is MANDATORY — there is NO all-etcd retreat: the controller exits at startup without a
// live Yugabyte (main.go fatals on empty/failed DSN), so o.yb is ALWAYS non-nil; the
// unit tests wire an in-memory fake (ybstore.Mem) over the SAME Yugabyte code path.
//
// §5.1 双向归位 is preserved by construction: the PRIMARY home is rendered FULL
// (anchors advertised + egress FlowSpec redirects), the BACKUP home STANDBY
// (limiting machinery pre-built, nothing advertised, §5.3). A re-render reads the
// edge's WHOLE pool set from Yugabyte (o.yb.PoolsForHome/PoolsForBackup), so
// adding/removing one pool always yields that edge's complete, correct state —
// never a partial diff.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/netip"
	"strconv"
	"sync"
	"time"

	"github.com/fivetime/sbw-contract/model"
	"golang.org/x/sync/singleflight"

	"github.com/fivetime/sbw-server/internal/edgever"
	"github.com/fivetime/sbw-server/internal/poolstore"
	"github.com/fivetime/sbw-server/internal/registry"
	"github.com/fivetime/sbw-server/internal/render"
	"github.com/fivetime/sbw-server/internal/scheduler"
	"github.com/fivetime/sbw-server/internal/srcmap"
	"github.com/fivetime/sbw-server/internal/ybstore"
)

// Pusher ships an edge's FULL desired state downstream (grpcsrv.Server satisfies
// it). This is the declarative backstop — resync / failover / anti-drift.
type Pusher interface {
	PushDesired(edge model.EdgeID, state model.EdgeDesiredState) error
}

// deltaPusher is an OPTIONAL Pusher capability: ship an INCREMENTAL per-pool delta
// (the O(delta) hot path) instead of the whole edge. grpcsrv.Server satisfies it.
// A Pusher that does not (e.g. a test stub) makes the orchestrator fall back to a
// full PushDesired on every change — same behavior as before this scalability fix.
type deltaPusher interface {
	PushDelta(edge model.EdgeID, delta model.EdgeDesiredDelta) error
}

// ErrNoPlacement is returned when the scheduler cannot find enough homes.
var ErrNoPlacement = errors.New("orchestrator: no placement (insufficient agents/capacity)")

// sellableFracPercent is the share of an agent's NIC line rate the controller may
// sell as tokens (NIC×90%, §4.2). Mirrors controller.sellableFracPercent — kept
// local so the optimistic create path can compute an edge's sellable token budget
// straight from the registry capacity (cached) instead of reading the etcd ledger.
const sellableFracPercent = 90

// sellableTokens converts a registered NIC line rate (bps) to the per-home token
// budget the placement may fill: capacity×90%. cap.Used (the sum of committed
// per-home pool tokens from Yugabyte) subtracted from this yields the advisory free
// figure SelectHomes uses for optimistic placement.
func sellableTokens(capacityBps uint64) int64 {
	return int64(capacityBps) * sellableFracPercent / 100
}

// Orchestrator runs pool create/destroy transactions over the controller's
// stores. It holds no per-call state, so any stateless replica can run it.
type Orchestrator struct {
	reg  *registry.Registry
	push Pusher

	// renderSF coalesces concurrent render-and-push for the SAME edge into one (Fix 4).
	// The resync/subscribe path spawns `go renderAndPush(edge)` per overflow/connect; for
	// a backup-skew-inflated edge that can't be delivered the overflow→resync loop spawned
	// ~230 concurrent renders, each holding a multi-hundred-MB EdgeDesiredState → ctrl RSS
	// → 21GiB (heap: render.ForEdge cum 7.5GB). With singleflight only one render per edge
	// runs at a time; concurrent callers share its result. Zero value is ready to use.
	renderSF singleflight.Group

	// yb is the MANDATORY YugabyteDB-backed bulk pool/member store AND the version-CAS
	// FAILOVER PIVOT (one Yugabyte row is both). Every pool/member lifecycle operation
	// — create (ONE ybstore.CreatePool ACID txn: pools.id PK = anti-replay,
	// members.prefix PK = cross-pool double-claim guard), update, remove-member,
	// destroy — and the async failover reconciler (GetForReconcile/UpdateCAS/DeleteCAS,
	// the version column replacing the etcd ModRevision) run through it. The render
	// reads source pools FROM it (PoolsForHome/Backup) and placement capacity comes
	// from cap (the cached UsedByEdge). There is NO all-etcd fallback: the controller
	// exits at startup without a live Yugabyte, so yb is ALWAYS non-nil in production;
	// the unit tests wire an in-memory fake (the SAME path, no live DB). SetYBStore
	// wires it.
	yb  YBStore
	cap CapacityProvider

	replicas   int // homes per pool (2 = primary+backup, 1 = single home)
	edgeAddrs  map[model.EdgeID]netip.Addr
	edgeAddrs6 map[model.EdgeID]netip.Addr
	homeMarker func(model.EdgeID) (model.LargeCommunity, bool)
	suppress   func(model.EdgeID, netip.Prefix) bool // T-607 advertise gate; nil = advertise all
	isAlive    func(model.EdgeID) bool               // liveness oracle; nil = treat all alive
	sessBudget func(model.EdgeID) uint64             // §9.1 materialization budget (agent-reported); nil = session dim off
	onDblDeath func(id model.PoolID, failOpen bool)  // C-04 double-death alarm; nil = silent
	// onFailover, if set, is invoked at an AUTONOMOUS (node-failure-driven) auto-promote
	// decision point — the async reconciler's PROMOTE step and the synchronous
	// FailoverEdge drain — with the pool, the dead old primary, the promoted backup and
	// the new primary's render generation. It is the seam the control plane uses to emit
	// the unsolicited "failover" API-result notification so the BSS learns its pool→edge
	// home moved without having issued any request. NOT called on a planned migrate or
	// decommission (those are request-correlated / operator-initiated). nil = silent. It
	// MUST NOT block the failover path (the control plane's impl emits async).
	onFailover func(id model.PoolID, fromEdge, toEdge model.EdgeID, generation uint64)
	// onRedundancy, if set, is the TIER-3 backup/capacity observability notify fired at
	// the backup-provisioning decision points (asyncProvisionBackup): kind is
	// "backup-changed" (a fresh backup was placed; toEdge is it), "redundancy-lost" (no
	// spare capacity for a backup; reason set), or "capacity-exhausted" (the scheduler
	// reported fleet-wide ErrInsufficientCapacity). It is OBSERVABILITY only — it never
	// changes placement/backup LOGIC, and MUST be non-blocking (the control plane emits
	// async). nil = silent.
	onRedundancy func(kind string, id model.PoolID, toEdge model.EdgeID, reason string)
	// onRehome, if set, is the TIER-3 per-pool re-home notify fired by the DECOMMISSION
	// drain (drainEdgeGen, planned exit) for each pool whose primary moved off the drained
	// edge — the per-edge billing-reconciliation detail under the bulk "decommission"
	// api-result. NOT a node-failure failover (that fires onFailover). Non-blocking;
	// nil = silent.
	onRehome func(id model.PoolID, from, to model.EdgeID)
	// genSeed mints the per-edge generation SEED — the first value a freshly-seen
	// edge's counter starts ABOVE — on this controller instance. Default is wall-
	// clock-ms, so a restart / failover / cross-replica handoff always seeds an
	// edge ABOVE any value a prior instance could have issued (monotonicity never
	// regresses for a live edge). Tests override it (WithGenerator) with a fixed
	// counter for deterministic, contiguous assertions. The generation is NOT a
	// global etcd sequence anymore: the agent only needs per-edge monotonicity
	// (it gap-detects on BaseGeneration == its own last-applied), and a seed jump
	// across a handoff is exactly the discontinuity that triggers a full
	// DESIRED_STATE resync — the existing, safe backstop. No per-render etcd write.
	genSeed func() uint64
	edgever *edgever.Store // L-07 per-edge desired version; nil → DesiredVersion stays 0 (single-replica)
	// withdrawTimeout bounds the DestroyPool gate that waits for a CROSS-SHARD home
	// to confirm (applied-version) it withdrew the pool before the quota is freed
	// (L-08, the cross-replica analog of "withdraw push returned success"). 0 → default.
	withdrawTimeout time.Duration

	// genMu guards lastGen: the last generation THIS replica delivered to each
	// locally-owned edge (full snapshot or delta). It is the BaseGeneration the next
	// DESIRED_DELTA chains onto, so the agent can detect a gap. It is a delivery-path
	// optimization local to whoever owns the edge's stream — NOT authoritative
	// cross-replica state: an unknown edge (cold replica / just took over the stream)
	// falls back to a full snapshot, and the agent's generation-gap detection is the
	// backstop, so the orchestrator stays safely stateless for correctness.
	genMu   sync.Mutex
	lastGen map[model.EdgeID]uint64
	// genCtr is the PER-EDGE monotonic generation counter (replaces the global
	// etcd genseq). It is seeded lazily (genSeed, default wall-clock-ms) the first
	// time an edge is rendered on THIS instance, then incremented per render of
	// that edge, so the steady path is strictly-increasing AND contiguous per edge
	// — exactly what the agent's BaseGeneration gap-detection needs. It is in-memory
	// only: a restart re-seeds ABOVE the prior value (never backward) and a stale/
	// peer-owned chain simply fails the agent's gap check → full resync. No etcd.
	genCtr map[model.EdgeID]uint64

	// dispatch runs the post-Txn async create job (the hot-path per-pool delta push)
	// OFF the request path. Default `go f()`: CreatePool returns 201 the instant the
	// atomic Txn commits and never blocks on the agent. Tests inject a synchronous
	// runner (WithSyncDispatch) so post-condition assertions are deterministic without
	// racing the goroutine.
	dispatch func(func())

	// log surfaces best-effort async-push failures (deliverEdge / bump). These are
	// retried by the reconciler/drift backstop, but a silent drop is a blind spot —
	// log at WARN/ERROR with edge (+ pool where known) so the retry is visible.
	log *slog.Logger
}

// Option configures an Orchestrator.
type Option func(*Orchestrator)

// WithReplicas sets homes per pool (default 2: primary + backup). 1 = single home.
func WithReplicas(n int) Option { return func(o *Orchestrator) { o.replicas = n } }

// WithEdgeAddrs supplies each edge's redirect next-hop (its own address) for the
// egress FlowSpec render.
func WithEdgeAddrs(m map[model.EdgeID]netip.Addr) Option {
	return func(o *Orchestrator) { o.edgeAddrs = m }
}

// WithEdgeAddrs6 supplies each edge's IPv6 redirect next-hop for the egress flow6
// render (RFC 5701 redirect-to-IPv6). Required for rate-limit pools with v6 members.
func WithEdgeAddrs6(m map[model.EdgeID]netip.Addr) Option {
	return func(o *Orchestrator) { o.edgeAddrs6 = m }
}

// WithHomeMarker supplies the home large-community marker (§4.7 conflict win).
func WithHomeMarker(f func(model.EdgeID) (model.LargeCommunity, bool)) Option {
	return func(o *Orchestrator) { o.homeMarker = f }
}

// WithGenerator overrides the per-edge generation SEED with a deterministic
// in-process source (tests). Each call supplies the value a freshly-seen edge's
// counter starts ABOVE; the orchestrator then increments per-edge from there.
// Production leaves the default wall-clock-ms seed (every controller instance
// seeds ABOVE the previous one's values, so a restart/handoff never regresses an
// edge's generation; the agent's per-edge gap-detection resyncs across the jump).
// There is NO global etcd sequence: the generation only needs to be monotonic
// per edge, which an in-memory counter delivers with zero per-render etcd writes.
func WithGenerator(seed func() uint64) Option {
	return func(o *Orchestrator) { o.genSeed = seed }
}

// WithLiveness wires a liveness oracle (the liveness monitor's IsDead, negated):
// placement avoids dead agents, and drain tells a live backup (promotable) from
// a dead one (double death). nil treats every registered agent as alive.
func WithLiveness(isAlive func(model.EdgeID) bool) Option {
	return func(o *Orchestrator) { o.isAlive = isAlive }
}

// WithSessionBudget wires the §9.1 materialization admission dimension: sessBudget(edge)
// is the max members that edge can program (agent-reported CapacityReport.SessionBudget).
// When set, placement rejects a pool whose members would exceed an edge's remaining
// session budget (ErrNoPlacement / capacity-exhausted materialization), instead of
// silently homing intent the data plane cannot materialize. nil (or a budget of 0 per
// edge) leaves placement on bandwidth alone.
func WithSessionBudget(f func(model.EdgeID) uint64) Option {
	return func(o *Orchestrator) { o.sessBudget = f }
}

// WithDoubleDeathAlarm sets the callback fired when a pool loses ALL live homes
// (C-04 §4.7): failOpen reports the per-pool policy applied (billing → open /
// torn down; control → close / kept). Wire to a Prometheus alert.
func WithDoubleDeathAlarm(cb func(id model.PoolID, failOpen bool)) Option {
	return func(o *Orchestrator) { o.onDblDeath = cb }
}

// SetFailoverNotify wires the autonomous-failover notification callback after
// construction (the control plane builds the orchestrator before its emitter is set).
// It fires at a node-failure auto-promote (the async reconciler's PROMOTE step and the
// synchronous FailoverEdge drain) with (pool, deadOldPrimary, promotedBackup,
// newPrimaryGeneration) so the control plane can emit the unsolicited "failover"
// API-result event. It is NOT invoked on a planned migrate / decommission. The callback
// MUST be non-blocking (the control plane emits asynchronously). nil disables the
// notification.
func (o *Orchestrator) SetFailoverNotify(f func(id model.PoolID, fromEdge, toEdge model.EdgeID, generation uint64)) {
	o.onFailover = f
}

// notifyFailover fires the autonomous-failover callback if wired. Centralized so the
// two auto-promote decision points (ReconcilePool PROMOTE, FailoverEdge drain) emit an
// identically-shaped notification.
func (o *Orchestrator) notifyFailover(id model.PoolID, from, to model.EdgeID, gen uint64) {
	if o.onFailover != nil {
		o.onFailover(id, from, to, gen)
	}
}

// SetRedundancyNotify wires the TIER-3 backup/capacity observability notify (see the
// onRedundancy field). nil disables it. Call once at startup (the control plane builds
// the orchestrator before its emitter is set).
func (o *Orchestrator) SetRedundancyNotify(f func(kind string, id model.PoolID, toEdge model.EdgeID, reason string)) {
	o.onRedundancy = f
}

// SetRehomeNotify wires the TIER-3 per-pool decommission re-home notify (see the
// onRehome field). nil disables it.
func (o *Orchestrator) SetRehomeNotify(f func(id model.PoolID, from, to model.EdgeID)) {
	o.onRehome = f
}

// notifyRedundancy fires the backup/capacity notify if wired (centralized so the
// provision-backup decision points emit identically). kind is one of "backup-changed",
// "redundancy-lost", "capacity-exhausted".
func (o *Orchestrator) notifyRedundancy(kind string, id model.PoolID, toEdge model.EdgeID, reason string) {
	if o.onRedundancy != nil {
		o.onRedundancy(kind, id, toEdge, reason)
	}
}

// notifyRehome fires the per-pool decommission re-home notify if wired.
func (o *Orchestrator) notifyRehome(id model.PoolID, from, to model.EdgeID) {
	if o.onRehome != nil {
		o.onRehome(id, from, to)
	}
}

// WithAdvertiseGate wires the RIB-survival guard's withdraw decision (T-607,
// §6.4-1): suppress(edge, member) returns true when the member's host route is
// certainly gone, so its anchor + FlowSpec are withheld to avoid blackholing.
// nil advertises every member (no gating).
func WithAdvertiseGate(suppress func(model.EdgeID, netip.Prefix) bool) Option {
	return func(o *Orchestrator) { o.suppress = suppress }
}

// WithEdgeVer wires the per-edge desired-version store (L-07). When set, every
// render stamps EdgeDesiredState.DesiredVersion with the edge's current edgever,
// and the async reconcile path (reconcile.go) bumps it after each authoritative
// record write so the converge loop delivers and the applied-version readiness
// gate works cross-replica. nil leaves DesiredVersion at 0 (single-replica, the
// synchronous drain path needs no versioning).
func WithEdgeVer(ev *edgever.Store) Option { return func(o *Orchestrator) { o.edgever = ev } }

// SetEdgeVer wires the per-edge desired-version store after construction (the cmd
// builds it from the etcd *Client, which NewControlPlane does not hold). Enables
// the async reconcile path; nil leaves the synchronous drain in effect. Call once
// at startup, before agents arrive.
func (o *Orchestrator) SetEdgeVer(ev *edgever.Store) { o.edgever = ev }

// SetYBStore wires the MANDATORY YugabyteDB-backed bulk store + capacity cache after
// construction (the cmd builds the pgxpool, which NewControlPlane does not hold). The
// SAME Yugabyte row is both the bulk pool/member data and the version-CAS failover
// pivot, so this is the single store the entire pool lifecycle runs through. Call once
// at startup, before agents arrive — the controller exits if Yugabyte is unavailable.
func (o *Orchestrator) SetYBStore(store YBStore, cap CapacityProvider) {
	o.yb = store
	o.cap = cap
}

// WithWithdrawConfirmTimeout bounds the DestroyPool cross-shard withdraw gate
// (L-08). 0 keeps the default (30s). Mainly for tests that exercise the timeout.
func WithWithdrawConfirmTimeout(d time.Duration) Option {
	return func(o *Orchestrator) { o.withdrawTimeout = d }
}

// WithLogger sets the logger used to surface best-effort async-push failures
// (deliverEdge / bump). nil keeps slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *Orchestrator) {
		if l != nil {
			o.log = l
		}
	}
}

// WithSyncDispatch makes the post-Txn async create job (the hot-path per-pool delta
// push) run SYNCHRONOUSLY in CreatePool instead of a goroutine. CreatePool then only
// returns after that work completes. For tests that assert post-conditions (state
// pushed) deterministically; production uses the default `go f()` so the request never
// blocks on the agent.
func WithSyncDispatch() Option {
	return func(o *Orchestrator) { o.dispatch = func(f func()) { f() } }
}

// MarkEdge bumps an edge's desired version so the converge loop re-renders +
// delivers it cross-replica (L-07). Used by the revive path to push cleanup to a
// reviving edge without a replica-local push. No-op without an edgever store.
func (o *Orchestrator) MarkEdge(ctx context.Context, edge model.EdgeID) { o.bump(ctx, edge) }

// poolsForHome returns the pools edge is PRIMARY for, from the mandatory Yugabyte
// bulk store — the single seam the render/audit paths read through.
func (o *Orchestrator) poolsForHome(ctx context.Context, edge model.EdgeID) ([]model.Pool, error) {
	return o.yb.PoolsForHome(ctx, edge)
}

// poolsForBackup is poolsForHome's BACKUP-role twin.
func (o *Orchestrator) poolsForBackup(ctx context.Context, edge model.EdgeID) ([]model.Pool, error) {
	return o.yb.PoolsForBackup(ctx, edge)
}

// New builds an orchestrator. By default it places 2 homes and stamps each edge's
// generation from a PER-EDGE in-memory monotonic counter seeded from wall-clock-ms
// (no global etcd write per render). Override the seed with WithGenerator (tests).
// The MANDATORY Yugabyte bulk store is wired post-construction via SetYBStore (the
// cmd builds the pgxpool, which NewControlPlane does not hold).
func New(reg *registry.Registry, push Pusher, opts ...Option) *Orchestrator {
	o := &Orchestrator{
		reg: reg, push: push,
		replicas: 2,
		genSeed:  func() uint64 { return uint64(time.Now().UnixMilli()) },
		lastGen:  map[model.EdgeID]uint64{},
		genCtr:   map[model.EdgeID]uint64{},
		dispatch: func(f func()) { go f() },
		log:      slog.Default(),
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// nextGenForEdge returns the next strictly-increasing generation for edge. The
// first call for an edge seeds the counter from genSeed() (wall-clock-ms in
// production) so a fresh controller instance always starts ABOVE any value a prior
// instance issued for that edge — generations never regress for a live edge across
// a restart/failover/handoff. Subsequent calls increment, so the steady path is
// contiguous per edge (what the agent's BaseGeneration gap-detection wants). Purely
// in-memory: ZERO etcd writes. A discontinuity (re-seed jump, or a peer replica
// taking over the stream with a fresh chain) is harmless — it merely fails the
// agent's BaseGeneration == last-applied check, which triggers a full DESIRED_STATE
// resync (the existing, safe backstop), never a wedge.
func (o *Orchestrator) nextGenForEdge(edge model.EdgeID) uint64 {
	o.genMu.Lock()
	defer o.genMu.Unlock()
	cur, seen := o.genCtr[edge]
	if !seen {
		cur = o.genSeed() // seed ABOVE any prior instance's values; never backward
	}
	cur++
	o.genCtr[edge] = cur
	return cur
}

func (o *Orchestrator) renderOptions(edge model.EdgeID) render.Options {
	return render.Options{Generation: o.nextGenForEdge(edge), EdgeAddrs: o.edgeAddrs, EdgeAddrs6: o.edgeAddrs6, HomeMarker: o.homeMarker, Suppress: o.suppress}
}

// RerenderEdge recomputes and pushes one edge's complete desired state from the
// current poolstore + advertise gate. The tap fusion calls it when the guard's
// view of a member's host route changes (host appeared → advertise; host
// certainly gone → withdraw), so anchor advertisement tracks fabric reality
// without a pool event (T-607). A no-op-safe wrapper over renderAndPush.
func (o *Orchestrator) RerenderEdge(ctx context.Context, edge model.EdgeID) error {
	return o.renderAndPush(ctx, edge)
}

// ExpectedCounts renders edge's complete desired state from the edge's Yugabyte
// pool set (o.poolsForHome/poolsForBackup) and advertise gate and returns how many policers and classify sessions that
// state contains — the controller's "expected" side of the B-02 three-number
// reconciliation. It renders with a ZERO generation deliberately: the counts do
// not depend on the generation value, and minting a real one (renderOptions) per
// audit would advance the edge's generation counter and could race the apply path.
// Read-only: it pushes nothing.
func (o *Orchestrator) ExpectedCounts(ctx context.Context, edge model.EdgeID) (policers, sessions int, err error) {
	primary, err := o.poolsForHome(ctx, edge)
	if err != nil {
		return 0, 0, err
	}
	backup, err := o.poolsForBackup(ctx, edge)
	if err != nil {
		return 0, 0, err
	}
	opt := render.Options{Generation: 0, EdgeAddrs: o.edgeAddrs, EdgeAddrs6: o.edgeAddrs6, HomeMarker: o.homeMarker, Suppress: o.suppress}
	st, err := render.ForEdge(edge, primary, backup, opt)
	if err != nil {
		return 0, 0, err
	}
	return len(st.Policers), len(st.ClassifySessions), nil
}

// DesiredVersion is the edge's current desired content version (edgever), or 0 when no
// edgever store is wired. The B-02 audit uses it to tell a report that PREDATES the
// latest push (still converging / resyncing — the count gap is delivery-in-flight, not
// a loss) from one that is caught up (where a gap IS a real drift). Without it a slow
// huge edge gets a phantom delivery-loss every audit cycle → a redundant full-snapshot
// re-push that re-feeds the converge (and the push-queue memory).
func (o *Orchestrator) DesiredVersion(ctx context.Context, edge model.EdgeID) (uint64, error) {
	if o.edgever == nil {
		return 0, nil
	}
	return o.edgever.Desired(ctx, edge)
}

// CleanupEdge drops an edge's per-edge generation state when it is permanently removed
// (decommission/deregister), so lastGen/genCtr do not accumulate over the controller's
// lifetime. Safe to call for an unknown edge. A later re-registration starts fresh (a
// decommissioned edge has no surviving pools, so a reset counter is correct).
func (o *Orchestrator) CleanupEdge(edge model.EdgeID) {
	o.genMu.Lock()
	delete(o.lastGen, edge)
	delete(o.genCtr, edge)
	o.genMu.Unlock()
}

// alive reports whether edge is usable as a home (no oracle → optimistic yes).
func (o *Orchestrator) alive(edge model.EdgeID) bool {
	return o.isAlive == nil || o.isAlive(edge)
}

// liveCandidatesCap lists registered agents that are alive and not excluded — the
// schedulable set for placement (CreatePool) and re-homing (ProvisionBackup) — plus
// each surviving agent's registered NIC capacity (bps), gathered from the SAME single
// registry.List. The create path's
// optimistic placement uses the capacity map to compute sellable(edge) − cap.Used(edge)
// entirely from memory — no per-candidate etcd ledger read.
func (o *Orchestrator) liveCandidatesCap(ctx context.Context, exclude map[model.EdgeID]bool) ([]model.EdgeID, map[model.EdgeID]uint64, error) {
	agents, err := o.reg.List(ctx)
	if err != nil {
		return nil, nil, err
	}
	out := make([]model.EdgeID, 0, len(agents))
	capBps := make(map[model.EdgeID]uint64, len(agents))
	for _, a := range agents {
		if exclude[a.EdgeID] || !o.alive(a.EdgeID) {
			continue
		}
		out = append(out, a.EdgeID)
		capBps[a.EdgeID] = a.CapacityBps
	}
	return out, capBps, nil
}

// CapacityProvider supplies each edge's committed usage for optimistic placement:
// Used = Σ sold bandwidth (bps token cost), Members = Σ materialized member COUNT (the
// §9.1 session dimension). *ybstore.CapacityCache satisfies it (a periodically-refreshed
// snapshot of Yugabyte's UsedByEdge/MembersByEdge); the unit tests inject a trivial
// in-memory provider.
type CapacityProvider interface {
	Used(model.EdgeID) int64
	Members(model.EdgeID) int64
}

// sessRemaining is the per-edge MATERIALIZATION-budget closure the SelectHomes session
// dimension uses (§9.1): reported SessionBudget(edge) − cached materialized Members(edge).
// An edge that reports NO budget (pre-§9.1 agent, sessBudget 0) is UNCONSTRAINED
// (math.MaxInt64 free) so a mixed fleet still places — only edges that advertise a limit
// are bounded by it. Returns nil when the session dimension is not wired at all.
func (o *Orchestrator) sessRemaining() scheduler.Remaining {
	if o.sessBudget == nil {
		return nil
	}
	return func(_ context.Context, e model.EdgeID) (int64, error) {
		budget := o.sessBudget(e)
		if budget == 0 {
			return math.MaxInt64, nil // unreported → unconstrained
		}
		return int64(budget) - o.cap.Members(e), nil
	}
}

// sessionConstraint returns the (remSess, needSess) pair to pass to SelectHomes for a pool
// with memberCount members: the materialization closure + the member count when the session
// dimension is wired, or (nil, 0) to select on bandwidth alone.
func (o *Orchestrator) sessionConstraint(memberCount int) (scheduler.Remaining, int64) {
	rs := o.sessRemaining()
	if rs == nil {
		return nil, 0
	}
	return rs, int64(memberCount)
}

// remaining returns the optimistic per-edge token-budget closure SelectHomes uses on
// EVERY placement path (create + reconcile-driven backup provision): remaining =
// sellable(edge) − cap.Used(edge), BOTH from memory, so placement writes NEITHER etcd
// nor Yugabyte per candidate. capBps is the per-candidate registered NIC capacity
// gathered alongside the live-candidate list (liveCandidatesCap). Centralizing it here
// keeps the create path and the failover reconcile path on the SAME optimistic source.
func (o *Orchestrator) remaining(capBps map[model.EdgeID]uint64) func(context.Context, model.EdgeID) (int64, error) {
	return func(ctx context.Context, e model.EdgeID) (int64, error) {
		return sellableTokens(capBps[e]) - o.cap.Used(e), nil
	}
}

// resvID keys a reservation by (pool, edge) — NOT by role — so the handle stays
// attached to its edge across a primary↔backup swap (failover). It is retained as the
// deterministic placement-handle id stamped on the returned record for the admin/HTTP
// layer (the create path is optimistic and writes no etcd ledger reservation).
func resvID(id model.PoolID, edge model.EdgeID) string {
	return "pool-" + strconv.FormatUint(uint64(id), 10) + "-" + string(edge)
}

// CreatePool is the nonce-less create (tests / internal callers). It delegates to
// CreatePoolNonce with no anti-replay nonce, so the persist Txn is a plain
// create-if-not-exists. The HTTP create path uses CreatePoolNonce with the
// request's nonce so a replayed request is rejected (ErrReplay).
func (o *Orchestrator) CreatePool(ctx context.Context, pool model.Pool, tokens int64) (poolstore.Record, error) {
	return o.CreatePoolNonce(ctx, pool, tokens, "")
}

// CreatePoolNonce is the ASYNC, OPTIMISTIC create (converged design: controller =
// brain, agent = hands, nothing on the request path blocks on the agent). It does
// placement from CACHED capacity (no ledger reserve/commit — hybrid architecture)
// + the data write (ONE Yugabyte ACID txn when ybstore is wired) + ONE atomic etcd
// poolstore Txn (anti-replay nonce claim + create-if-not-exists, the failover pivot)
// SYNCHRONOUSLY, then RETURNS the record the instant that Txn commits — it never
// waits on the agent. The only post-Txn work — the hot-path per-pool delta push — is
// fired OFF the request path (o.dispatch, default `go`). A failed async push is
// acceptable: the report-hash drift backstop + the agent's on-connect/periodic
// resync recover it. There is NO per-create etcd ledger write and NO per-create
// edgever bump.
//
// HYBRID STEP 2 (2026-06): the create writes ZERO etcd. The failover pivot (home /
// backup / version / retiring) is now a Yugabyte ROW (ybstore.CreatePool inserts it
// version=1, retiring=false), not an etcd poolstore Record. The pools.id PRIMARY KEY
// is the anti-replay key (a replayed create, same id, is the idempotent ON CONFLICT
// no-op → ErrExists), so the separate etcd create-nonce is gone; `nonce` is accepted
// for API compatibility but no longer drives an etcd write (the admin-layer ±window
// timestamp check still bounds replay). ybstore is MANDATORY (o.yb is always non-nil —
// the controller exits at startup without it), so there is NO etcd create-with-nonce
// fallback. `tokens` is the per-home quota cost (stamped on the returned record + the pool
// row, used by the failover paths).
func (o *Orchestrator) CreatePoolNonce(ctx context.Context, pool model.Pool, tokens int64, nonce string) (poolstore.Record, error) {
	rec, _, _, err := o.CreatePoolNonceGen(ctx, pool, tokens, nonce)
	return rec, err
}

// CreatePoolNonceGen is CreatePoolNonce additionally surfacing the PRIMARY home
// edge and the per-edge desired-state Generation the primary's pushed delta carries
// — the (edge, generation) the API-result pending registry needs to resolve
// convergence (the agent's report echoes this Generation once it has applied the
// op). The generation is PRE-MINTED synchronously (before the async dispatch) from
// the primary edge's monotonic counter and threaded into the primary's delta push,
// so the value returned to the admin handler is exactly the one the agent echoes
// back. Returns generation 0 when there is no primary (never, post-placement).
func (o *Orchestrator) CreatePoolNonceGen(ctx context.Context, pool model.Pool, tokens int64, nonce string) (poolstore.Record, model.EdgeID, uint64, error) {
	// tokens==0 is the UNLIMITED pool (95th-percentile billing): no committed
	// quota is reserved, so it fits any live edge — only negative is invalid.
	if tokens < 0 {
		return poolstore.Record{}, "", 0, fmt.Errorf("orchestrator: pool %d: tokens must be non-negative", pool.ID)
	}
	// Pre-existence check from Yugabyte (the authoritative pool row). The ON CONFLICT
	// create-CAS below is the authoritative race guard; this is the fast clear-error path.
	if _, ok, err := o.yb.Get(ctx, pool.ID); err != nil {
		return poolstore.Record{}, "", 0, err
	} else if ok {
		return poolstore.Record{}, "", 0, fmt.Errorf("orchestrator: pool %d already exists", pool.ID)
	}

	// Placement: pick distinct LIVE homes with enough remaining tokens (never
	// place a new pool on a dead agent). liveCandidatesCap returns the schedulable
	// set AND each agent's registered NIC capacity from one registry.List, so the
	// cached-capacity rem closure below needs no per-candidate etcd read.
	candidates, capBps, err := o.liveCandidatesCap(ctx, nil)
	if err != nil {
		return poolstore.Record{}, "", 0, err
	}
	// OPTIMISTIC placement: the create reserves nothing in the etcd ledger. remaining =
	// sellable(edge) − cap.Used(edge), BOTH from memory: sellable = registered NIC×90%
	// (registry, already fetched into capBps), used = the cached Yugabyte UsedByEdge sum
	// (refreshed every few seconds). So SelectHomes touches NEITHER etcd nor Yugabyte per
	// candidate per create. A few-seconds-stale "used" only ever causes a benign
	// re-placement (the lab runs huge per-edge capacity, so contention never oversells).
	//
	// TODO(no-oversell): production strict no-oversell (when an edge is run near its
	// sellable ceiling) needs an authoritative capacity gate — e.g. a Yugabyte per-edge
	// used-tokens counter incremented in the SAME CreatePool txn with a CHECK
	// (used+tokens ≤ sellable), rejecting the create on violation. That keeps the create
	// to ONE (Yugabyte) write while restoring a hard gate. Not built now: the lab
	// over-provisions capacity, so optimistic placement never oversells.
	remSess, needSess := o.sessionConstraint(len(pool.Members))
	homes, err := scheduler.SelectHomes(ctx, candidates, o.remaining(capBps), tokens, remSess, needSess, o.replicas)
	if err != nil {
		// Both "out of bandwidth" and "out of materialization budget" (§9.1) mean the pool
		// cannot be placed → 503 (ErrNoPlacement). Preserve the sentinel in the wrap so the
		// requester's 503 body / logs distinguish materialization from bandwidth exhaustion.
		if errors.Is(err, scheduler.ErrInsufficientCapacity) || errors.Is(err, scheduler.ErrInsufficientSessions) {
			return poolstore.Record{}, "", 0, fmt.Errorf("%w: %v", ErrNoPlacement, err)
		}
		return poolstore.Record{}, "", 0, err
	}

	rec := poolstore.Record{Pool: pool, Primary: homes[0], Tokens: tokens, PrimaryResvID: resvID(pool.ID, homes[0])}
	rec.Pool.HomeEdge = rec.Primary
	if o.replicas >= 2 {
		rec.Backup = homes[1]
		rec.BackupResvID = resvID(pool.ID, homes[1])
	}
	// NO ledger reserve/commit here: the create is OPTIMISTIC (see the placement
	// comment). The PrimaryResvID/BackupResvID are still stamped on the returned record
	// so the FAILOVER/UPDATE paths (which DO use the ledger) keep their deterministic
	// (pool,edge)-keyed handles — but on the create path no etcd ledger key is written.

	// PERSIST — ZERO etcd. The authoritative pool/member rows AND the failover pivot
	// are ONE Yugabyte ACID txn:
	//   - INSERT pools(... version=1, retiring=false) ON CONFLICT (id) DO NOTHING.
	//     The pools.id PRIMARY KEY is the anti-replay key (a replayed create, same id,
	//     is the idempotent no-op → ErrExists), so NO etcd create-nonce is written.
	//     version=1 seeds the optimistic-CAS token the reconciler advances on each
	//     pivot move (it persists, so a crashed controller rebuilds it from Yugabyte).
	//   - one INSERT per member (members.prefix PK). A unique_violation (23505) is the
	//     cross-pool double-claim → ErrMemberConflict and the whole txn ROLLS BACK
	//     (no partial claim) — the cross-pool guard, enforced by the PK.
	// ErrExists is an idempotent id replay (return the conflict). On any other yb error
	// we best-effort withdraw the half-pushed homes. The create writes NOTHING to etcd:
	// etcd is left to pure coordination (leader election, sharding, liveness, registry).
	// `nonce` is accepted for API compat but drives no write (the admin-layer ±window
	// timestamp check bounds replay).
	ybRec := ybstore.Record{Pool: pool, Primary: rec.Primary, Backup: rec.Backup, Tokens: tokens}
	if err := o.yb.CreatePool(ctx, ybRec, pool.Members); err != nil {
		if errors.Is(err, ybstore.ErrExists) {
			return poolstore.Record{}, "", 0, fmt.Errorf("pool %d: %w", pool.ID, poolstore.ErrExists)
		}
		o.rollbackCreate(ctx, rec)
		if errors.Is(err, ybstore.ErrMemberConflict) {
			return poolstore.Record{}, "", 0, fmt.Errorf("orchestrator: %w", err)
		}
		return poolstore.Record{}, "", 0, fmt.Errorf("orchestrator: yb create: %w", err)
	}

	// HOT PATH: the atomic Txn is committed → RETURN 201 to the handler immediately.
	// The only remaining work touches the AGENT — the controller-driven per-pool DELTA
	// push — and is fired OFF the request path (o.dispatch, default `go`) so the create
	// NEVER blocks on the agent:
	//   - per-pool DELTA push (render.ForPool + PushDelta): the O(delta) hot path. We
	//     never re-render/re-push the whole edge here; a drop under buffer overflow is
	//     recovered by the report-hash drift backstop + the agent's resync.
	// There is NO ledger Commit (the create reserves nothing — optimistic placement)
	// and NO per-create edgever bump (cross-shard convergence is driven by the agent's
	// report → the owning replica's DriftSweep, see pushPoolDelta). A failed async push
	// is acceptable and never rolls the create back. SYNC dispatch (tests) makes this
	// complete before CreatePoolNonce returns.
	// Pre-mint the PRIMARY edge's generation synchronously so the (primary, gen) the
	// admin handler registers as a pending is exactly what the primary's delta carries
	// (and thus what the agent echoes back in EdgeReport.Generation on convergence).
	// gen==0 only if there is no primary (never post-placement); then the API-result
	// pending is not registered and the timeout sweep is the sole resolver.
	var primaryGen uint64
	if rec.Primary != "" {
		primaryGen = o.nextGenForEdge(rec.Primary)
	}
	o.dispatch(func() {
		jobCtx := context.WithoutCancel(ctx)
		for i, edge := range rec.Homes() {
			standby := i > 0 // homes[0]=primary (full), homes[1]=backup (standby)
			gen := uint64(0)
			if i == 0 {
				gen = primaryGen // exactly the generation surfaced to the pending registry
			}
			_ = o.pushPoolDeltaGen(jobCtx, edge, pool, standby, gen)
		}
	})

	return rec, rec.Primary, primaryGen, nil
}

// pushPoolDeltaGen renders ONE pool's contribution to edge (the O(delta) hot path,
// render.ForPool) and ships it as an incremental EdgeDesiredDelta chained onto the
// last generation THIS replica delivered to the edge (BaseGeneration → gap
// detection). It is the controller-driven hot path that replaces re-rendering the
// whole edge on a create. Falls back when:
//   - the edge is NOT locally deliverable (a peer replica owns the stream): do
//     NOTHING here. The authoritative pool+members are already in Yugabyte (written
//     synchronously in the create Txn above, readable by ANY replica), so the owning
//     replica converges the edge WITHOUT a per-create etcd edgever bump: its agent's
//     periodic report carries an InstalledPoolHash, the owning replica's DriftSweep
//     recomputes expectedPoolSet (from Yugabyte) — now including this pool — sees the
//     mismatch and pushes a full DESIRED_STATE resync (RerenderEdge). This trades a
//     few seconds of cross-replica delivery latency for ZERO per-create etcd writes,
//     which is the whole point of the hybrid (etcd was pegged on the create rate).
//     The hot-path per-pool delta on the OWNING replica is the immediate channel;
//     the report→DriftSweep recompute is the cross-shard backstop. (Failover/destroy
//     still bump edgever — those are low-rate and need prompt cross-replica delivery.)
//   - the transport has no PushDelta capability (a non-grpcsrv Pusher / test stub):
//     a full PushDesired, identical to pre-fix behaviour.
//
// A backpressure / not-subscribed push error is swallowed by the caller as best-
// effort; the report-hash drift backstop recovers it.
//
// It takes an OPTIONALLY pre-minted generation: gen>0
// uses exactly that per-edge generation for the delta (so a caller — the API-result
// pending registry — can know, synchronously and BEFORE this async push runs, the
// generation the home edge will echo back on convergence); gen==0 mints a fresh one
// as before. The pre-minted generation MUST come from nextGenForEdge(edge) so it
// stays on the edge's monotonic chain (the agent's BaseGeneration gap-detection and
// the report's Generation echo both rely on it).
func (o *Orchestrator) pushPoolDeltaGen(ctx context.Context, edge model.EdgeID, pool model.Pool, standby bool, gen uint64) error {
	if !o.locallyDeliverable(edge) {
		return nil // peer owns the stream → its report→DriftSweep converges (no etcd write)
	}
	dp, ok := o.push.(deltaPusher)
	if !ok {
		return o.renderAndPush(ctx, edge) // no delta capability → full snapshot (no regression)
	}
	opt := o.renderOptions(edge)
	if gen > 0 {
		opt.Generation = gen
	}
	delta, err := render.ForPool(edge, pool, standby, opt)
	if err != nil {
		return err
	}
	var ver uint64
	if o.edgever != nil {
		if ver, err = o.edgever.Desired(ctx, edge); err != nil {
			return err
		}
	}
	o.genMu.Lock()
	base := o.lastGen[edge]
	o.lastGen[edge] = opt.Generation
	o.genMu.Unlock()
	return dp.PushDelta(edge, model.EdgeDesiredDelta{
		SchemaVersion:     model.SchemaVersion,
		EdgeID:            edge,
		Generation:        opt.Generation,
		BaseGeneration:    base,
		GeneratedAtUnixMs: time.Now().UnixMilli(),
		DesiredVersion:    ver,
		Upserts:           []model.PoolDelta{delta},
	})
}

// UpdatePool changes an existing pool's members and/or rates IN PLACE — the BSS
// update path (add/remove members, modify bandwidth). The home PLACEMENT is
// unchanged (no failover/migration). It is ONE ybstore.UpdatePool ACID txn (rewrite
// pools.body + bump version, sync the members table with the cross-pool double-claim
// guard via the members.prefix PK) — ZERO etcd. Like the create path it is OPTIMISTIC:
// no etcd ledger reserve and no srcmap claim/release (capacity is the cached
// UsedByEdge). The async per-pool delta push lets the edge agents reconcile only the
// diff — classify sessions for member changes, policer CIR for a rate change — so
// unaffected members keep flowing undisturbed. newTokens is the new per-home quota
// cost (caller derives it from the new rate).
func (o *Orchestrator) UpdatePool(ctx context.Context, newPool model.Pool, newTokens int64) (poolstore.Record, error) {
	rec, _, _, err := o.UpdatePoolGen(ctx, newPool, newTokens)
	return rec, err
}

// UpdatePoolGen is UpdatePool additionally surfacing the PRIMARY home edge and the
// per-edge desired-state Generation the primary's pushed delta carries — the
// (edge, generation) the API-result pending registry resolves convergence against
// (the agent's report echoes this Generation once it has applied the update). The
// generation is PRE-MINTED synchronously from the primary's monotonic counter and
// threaded into the primary's delta push, so it is exactly what the agent echoes.
func (o *Orchestrator) UpdatePoolGen(ctx context.Context, newPool model.Pool, newTokens int64) (poolstore.Record, model.EdgeID, uint64, error) {
	// tokens==0 = UNLIMITED (95th-percentile): no committed quota. Negative invalid.
	if newTokens < 0 {
		return poolstore.Record{}, "", 0, fmt.Errorf("orchestrator: pool %d: tokens must be non-negative", newPool.ID)
	}
	ybr, ok, err := o.yb.Get(ctx, newPool.ID)
	if err != nil {
		return poolstore.Record{}, "", 0, err
	}
	if !ok {
		return poolstore.Record{}, "", 0, fmt.Errorf("orchestrator: pool %d does not exist", newPool.ID)
	}
	ybr.Pool = newPool
	ybr.Tokens = newTokens
	if err := o.yb.UpdatePool(ctx, ybr, newPool.Members); err != nil {
		if errors.Is(err, ybstore.ErrMemberConflict) {
			return poolstore.Record{}, "", 0, fmt.Errorf("orchestrator: %w", err)
		}
		if errors.Is(err, ybstore.ErrNotFound) {
			return poolstore.Record{}, "", 0, fmt.Errorf("orchestrator: pool %d does not exist", newPool.ID)
		}
		return poolstore.Record{}, "", 0, fmt.Errorf("orchestrator: yb update: %w", err)
	}
	homes := []model.EdgeID{ybr.Primary}
	if ybr.Backup != "" {
		homes = append(homes, ybr.Backup)
	}
	// Pre-mint the primary's generation synchronously (see CreatePoolNonceGen).
	var primaryGen uint64
	if ybr.Primary != "" {
		primaryGen = o.nextGenForEdge(ybr.Primary)
	}
	o.dispatch(func() {
		jobCtx := context.WithoutCancel(ctx)
		for i, edge := range homes {
			gen := uint64(0)
			if i == 0 {
				gen = primaryGen
			}
			_ = o.pushPoolDeltaGen(jobCtx, edge, newPool, i > 0, gen)
		}
	})
	r, _, err := o.yb.Get(ctx, newPool.ID)
	if err != nil {
		// Persisted; the post-read failed. Return the record we just wrote.
		return poolstore.Record{Pool: newPool, Primary: ybr.Primary, Backup: ybr.Backup, Tokens: newTokens}, ybr.Primary, primaryGen, nil
	}
	return poolstore.Record{Pool: r.Pool, Primary: r.Primary, Backup: r.Backup, Tokens: r.Tokens}, ybr.Primary, primaryGen, nil
}

// MemberConflicts returns, for the given member prefixes, every existing member
// record that OVERLAPS one of them (CIDR containment either way / equal) and belongs
// to a pool OTHER than excludePool — the CIDR-aware cross-pool overlap set, deduped by
// source. Empty means the members can be claimed without displacing any other pool.
// The members table (prefix PK) is the authoritative claim set, so the overlap check
// runs there (CIDR-aware ANY/LIKE) and its (prefix,pool) conflicts are adapted to
// srcmap.Record so the admin handler's conflict/replace response is unchanged. home is
// left empty (the eviction path uses pool+prefix, not home).
func (o *Orchestrator) MemberConflicts(ctx context.Context, members []model.Member, excludePool model.PoolID) ([]srcmap.Record, error) {
	prefixes := make([]netip.Prefix, 0, len(members))
	for _, m := range members {
		prefixes = append(prefixes, m.Prefix)
	}
	cs, err := o.yb.MemberConflicts(ctx, prefixes, excludePool)
	if err != nil {
		return nil, err
	}
	out := make([]srcmap.Record, 0, len(cs))
	for _, c := range cs {
		out = append(out, srcmap.Record{Src: c.Prefix, PoolID: c.PoolID})
	}
	return out, nil
}

// RemoveMember drops a single member prefix from a pool: the member row lives in the
// Yugabyte bulk store, so it DELETEs that row (gated on pool_id, so a member that moved
// away is a no-op) and bumps the pool version in ONE txn — ZERO etcd, no srcmap release
// (the members.prefix PK was the claim) — then re-renders the homes so the evicted
// member's anchor/classify is withdrawn. Tokens and home are UNTOUCHED — member count
// does not affect the bandwidth quota. The pool is KEPT even if this empties it:
// removing a member and destroying a pool are separate BSS operations, and an empty
// pool is a dormant pool (no anchors, quota still reserved) until BSS adds members or
// destroys it explicitly. Used by the CIDR-overlap replace path to evict a conflicting
// member from the pool that holds it. No-op if the member is not in the pool.
func (o *Orchestrator) RemoveMember(ctx context.Context, poolID model.PoolID, prefix netip.Prefix) (poolstore.Record, error) {
	ybr, ok, err := o.yb.Get(ctx, poolID)
	if err != nil {
		return poolstore.Record{}, err
	}
	if !ok {
		return poolstore.Record{}, fmt.Errorf("orchestrator: pool %d does not exist", poolID)
	}
	kept := make([]model.Member, 0, len(ybr.Pool.Members))
	found := false
	for _, m := range ybr.Pool.Members {
		if m.Prefix == prefix {
			found = true
			continue
		}
		kept = append(kept, m)
	}
	if !found {
		return poolstore.Record{Pool: ybr.Pool, Primary: ybr.Primary, Backup: ybr.Backup, Tokens: ybr.Tokens}, nil
	}
	if err := o.yb.RemoveMember(ctx, poolID, prefix); err != nil {
		return poolstore.Record{}, fmt.Errorf("orchestrator: yb remove-member: %w", err)
	}
	ybr.Pool.Members = kept
	homes := []model.EdgeID{ybr.Primary}
	if ybr.Backup != "" {
		homes = append(homes, ybr.Backup)
	}
	for _, e := range homes {
		if err := o.deliverEdge(ctx, e); err != nil {
			return poolstore.Record{}, fmt.Errorf("orchestrator: remove-member push %s: %w", e, err)
		}
	}
	return poolstore.Record{Pool: ybr.Pool, Primary: ybr.Primary, Backup: ybr.Backup, Tokens: ybr.Tokens}, nil
}

// DestroyPool withdraws a pool: delete the record, re-render the homes (so the
// pool's advertisement/redirects are withdrawn), then — ONLY once every withdraw
// has landed — drop the src claims and return the tokens. Idempotent: an unknown
// pool is a no-op.
//
// Order is load-bearing: the quota MUST NOT be freed while the pool may still be
// enforced in the data plane. A returned token is immediately re-sellable, so
// freeing it before the withdraw lands would let a new pool be funded on that
// edge while the old pool's policer/anchors still linger → transient oversell.
// If ANY withdraw push fails we therefore keep the quota and RESTORE the record
// (re-render the homes so the pool stays fully advertised) and return a retriable
// error — leaving the pool intact rather than half-destroyed. DestroyPool is
// idempotent, so the caller (or the action-expiry sweep) retries until every
// withdraw lands and the quota is freed exactly once.
func (o *Orchestrator) DestroyPool(ctx context.Context, id model.PoolID) error {
	_, _, err := o.DestroyPoolGen(ctx, id)
	return err
}

// DestroyPoolGen is DestroyPool additionally surfacing the PRIMARY home edge and the
// per-edge desired-state Generation the primary's withdrawal render stamped — the
// (edge, generation) the API-result pending registry resolves convergence against.
// For a LOCAL primary the generation is the one renderAndPushGen minted for the
// pool-less snapshot (the agent echoes it once it has dropped the pool); a
// CROSS-SHARD primary is bumped (no local render) so the generation is 0 and the
// pending falls back to the timeout sweep. An unknown pool returns ("", 0, nil).
func (o *Orchestrator) DestroyPoolGen(ctx context.Context, id model.PoolID) (model.EdgeID, uint64, error) {
	// The authoritative pool/member rows live in Yugabyte. Read the record, delete the
	// pool+members in ONE yb txn, then withdraw every home AFTER the delete (render
	// reads pools FROM yb, so an absent row is what makes the re-render withdraw the
	// pool). Free NOTHING in the etcd ledger/srcmap: the members table WAS the claim and
	// the optimistic create reserved nothing. ZERO etcd pool/member writes; etcd keeps
	// only the coordination bumps (edgever) the cross-shard withdraw needs.
	ybr, ok, err := o.yb.Get(ctx, id)
	if err != nil {
		return "", 0, err
	}
	if !ok {
		return "", 0, nil // idempotent: unknown pool
	}
	rec := poolstore.Record{Pool: ybr.Pool, Primary: ybr.Primary, Backup: ybr.Backup, Tokens: ybr.Tokens}
	rec.Pool.HomeEdge = ybr.Primary
	// Tear the pool down in Yugabyte FIRST so a re-render (which reads pools FROM yb)
	// omits — withdraws — the pool.
	if err := o.yb.DestroyPool(ctx, id); err != nil {
		return "", 0, err
	}
	// Local homes push the withdraw synchronously; a CROSS-SHARD home is bumped and its
	// withdraw confirmed via applied-version below (L-08). Withdraw failing on a LOCAL
	// home keeps the pool (it may still be enforced there) — restore and retry.
	var crossShard []model.EdgeID
	var primaryGen uint64
	for _, edge := range rec.Homes() {
		if o.locallyDeliverable(edge) {
			gen, _, perr := o.renderAndPushGen(ctx, edge)
			if perr != nil {
				return "", 0, o.restoreDestroyed(ctx, rec, edge, perr)
			}
			if edge == rec.Primary {
				primaryGen = gen
			}
			continue
		}
		o.bump(ctx, edge)
		crossShard = append(crossShard, edge)
	}
	// Gate: a cross-shard home's withdraw is only "landed" once its owning replica
	// delivered the bumped (pool-less) state and the agent applied it. On timeout keep
	// the pool and retry, same as a failed local push.
	for _, edge := range crossShard {
		if err := o.waitEdgeApplied(ctx, edge); err != nil {
			return "", 0, o.restoreDestroyed(ctx, rec, edge, err)
		}
	}
	return rec.Primary, primaryGen, nil
}

// restoreDestroyed re-instates a pool whose withdraw could not be completed on
// `edge` (a local push failed, or a cross-shard withdraw was not confirmed before
// the gate timeout): put the record back and re-advertise every home so the pool
// stays fully intact rather than half-destroyed with freed quota. Returns a
// retriable error; DestroyPool is idempotent so the caller / action-expiry sweep
// retries until every withdraw lands and the quota is freed exactly once.
func (o *Orchestrator) restoreDestroyed(ctx context.Context, rec poolstore.Record, edge model.EdgeID, cause error) error {
	// Re-insert the authoritative Yugabyte pool+members row (version reset to 1 — the
	// create-path pivot). ZERO etcd pool/member write.
	ybRec := ybstore.Record{Pool: rec.Pool, Primary: rec.Primary, Backup: rec.Backup, Tokens: rec.Tokens}
	if err := o.yb.CreatePool(ctx, ybRec, rec.Pool.Members); err != nil && !errors.Is(err, ybstore.ErrExists) {
		o.log.Error("orchestrator: restore-destroyed re-insert failed", "pool", rec.Pool.ID, "err", err)
	}
	for _, e := range rec.Homes() {
		if err := o.deliverEdge(ctx, e); err != nil { // best-effort re-advertise (local push / cross-shard bump)
			o.log.Warn("orchestrator: restore-destroyed re-advertise failed (will retry)", "pool", rec.Pool.ID, "edge", e, "err", err)
		}
	}
	return fmt.Errorf("orchestrator: withdraw %s (pool kept, retry destroy): %w", edge, cause)
}

// renderAndPush rebuilds one edge's complete desired state from Yugabyte
// (o.poolsForHome/poolsForBackup: primary pools full, backup pools standby) and pushes it.
func (o *Orchestrator) renderAndPush(ctx context.Context, edge model.EdgeID) error {
	// Coalesce concurrent renders for this edge (Fix 4): a slow undeliverable edge's
	// overflow→resync loop otherwise spawns hundreds of parallel renders, each pinning a
	// full EdgeDesiredState. singleflight keeps exactly one in flight per edge.
	//
	// DoChan + WithoutCancel decouple the SHARED render from any single caller's
	// cancellation: a request-scoped caller (create/update → deliverEdge → renderAndPush with
	// the HTTP request ctx) that times out must NOT abort the render and cascade "context
	// canceled" to the background converge/reconcile callers coalesced onto it. The render runs
	// detached (delivery is a background obligation that should complete regardless of who
	// triggered it — matching the deadline-less background callers); each caller still bails on
	// its OWN ctx, so a request caller never blocks past its deadline.
	ch := o.renderSF.DoChan(string(edge), func() (interface{}, error) {
		_, _, e := o.renderAndPushGen(context.WithoutCancel(ctx), edge)
		return nil, e
	})
	select {
	case res := <-ch:
		return res.Err
	case <-ctx.Done():
		return ctx.Err() // caller bails; the detached render still completes for the others
	}
}

// renderAndPushGen is renderAndPush returning the generation AND the per-edge
// desired version (L-07) it stamped. The generation lets a caller wait for the
// agent to confirm it applied that generation before switching (§4.4 ① data-plane
// ready before BGP switch). The desired version is the edge's current edgever
// (0 when no edgever store is wired): it is carried in EdgeDesiredState so the
// agent echoes it back (HealthReport.AppliedVersion), letting the converge loop
// advance the etcd applied-version that the async reconciler's ready gate reads.
func (o *Orchestrator) renderAndPushGen(ctx context.Context, edge model.EdgeID) (uint64, uint64, error) {
	primary, err := o.poolsForHome(ctx, edge)
	if err != nil {
		return 0, 0, err
	}
	backup, err := o.poolsForBackup(ctx, edge)
	if err != nil {
		return 0, 0, err
	}
	opt := o.renderOptions(edge)
	st, err := render.ForEdge(edge, primary, backup, opt)
	if err != nil {
		return 0, 0, err
	}
	var ver uint64
	if o.edgever != nil {
		// Stamp the edge's current content version so the agent echoes it back.
		// Read here (not the render) keeps render pure; a concurrent later bump
		// only yields a fresher (higher) version, which is still sound (monotonic).
		if ver, err = o.edgever.Desired(ctx, edge); err != nil {
			return 0, 0, err
		}
		st.DesiredVersion = ver
	}
	if err := o.push.PushDesired(edge, st); err != nil {
		return opt.Generation, ver, err
	}
	// Record the generation this full snapshot delivered so the NEXT per-pool delta
	// (pushPoolDelta) chains its BaseGeneration onto it — a full push and a delta
	// share one generation chain, so the agent's gap detection stays sound.
	o.genMu.Lock()
	o.lastGen[edge] = opt.Generation
	o.genMu.Unlock()
	return opt.Generation, ver, nil
}

// subChecker is the optional Pusher capability to report whether an edge's agent
// stream is subscribed to THIS replica (grpcsrv.Server provides it).
type subChecker interface{ IsSubscribed(model.EdgeID) bool }

// locallyDeliverable reports whether THIS replica can push edge's desired state
// directly. Single-replica (no edgever) always can — one controller owns every
// edge. Sharded: only when the edge's agent stream is subscribed here; otherwise a
// peer replica owns delivery (L-08). A Pusher that can't report subscription is
// assumed local (no regression).
func (o *Orchestrator) locallyDeliverable(edge model.EdgeID) bool {
	if o.edgever == nil {
		return true
	}
	if sc, ok := o.push.(subChecker); ok {
		return sc.IsSubscribed(edge)
	}
	return true
}

// deliverEdge realizes edge's current desired state from the (already-persisted)
// poolstore. Local home → render+push synchronously (single-replica zero
// regression). CROSS-SHARD home (subscribed on a peer replica) → bump the edge's
// desired version so the OWNING replica's converge loop (controller.RunConverge)
// delivers it — closing L-08, where the synchronous fan-out failed with "edge not
// subscribed" for a home on a peer replica. PRECONDITION: the poolstore record
// reflecting the intended outcome (Create / Put / Delete) is already written, so a
// peer converge renders the right state.
func (o *Orchestrator) deliverEdge(ctx context.Context, edge model.EdgeID) error {
	if o.locallyDeliverable(edge) {
		return o.renderAndPush(ctx, edge)
	}
	o.bump(ctx, edge)
	return nil
}

// defaultWithdrawConfirmTimeout bounds the DestroyPool cross-shard withdraw gate.
const defaultWithdrawConfirmTimeout = 30 * time.Second

// waitEdgeApplied blocks until a CROSS-SHARD edge has APPLIED its current desired
// version (the converge loop on the owning replica delivered the bumped state and
// the agent echoed applied>=desired) or the bounded timeout elapses. Used by
// DestroyPool as the cross-replica equivalent of "withdraw push returned success":
// the quota stays reserved until the peer confirms the pool was withdrawn, so a
// freed token can't fund a new pool while the old one still lingers on that edge.
// No edgever (single-replica) → nothing to wait for.
func (o *Orchestrator) waitEdgeApplied(ctx context.Context, edge model.EdgeID) error {
	if o.edgever == nil {
		return nil
	}
	timeout := o.withdrawTimeout
	if timeout <= 0 {
		timeout = defaultWithdrawConfirmTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		if o.edgeReady(ctx, edge) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("orchestrator: edge %s withdraw not confirmed (applied<desired): %w", edge, ctx.Err())
		case <-t.C:
		}
	}
}

// rollbackCreate undoes a partially-applied create: delete the bulk yb pool/members
// row (the create wrote ONLY that row — ZERO etcd, the members table was the claim)
// and re-render the homes (withdrawing the half-pushed pool). The create reserved
// NOTHING in the etcd ledger (optimistic placement), so there are no reservations to
// return here. Best-effort.
func (o *Orchestrator) rollbackCreate(ctx context.Context, rec poolstore.Record) {
	if err := o.yb.Delete(ctx, rec.Pool.ID); err != nil { // drop the bulk pool+members (idempotent)
		o.log.Error("orchestrator: rollback-create delete failed", "pool", rec.Pool.ID, "err", err)
	}
	for _, edge := range rec.Homes() {
		if err := o.deliverEdge(ctx, edge); err != nil { // restore prior state best-effort (local push / cross-shard bump)
			o.log.Warn("orchestrator: rollback-create re-advertise failed (will retry)", "pool", rec.Pool.ID, "edge", edge, "err", err)
		}
	}
}
