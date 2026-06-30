package server

import (
	"context"
	"net/netip"
	"sort"
	"sync"
	"time"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-contract/rpc"
)

// memberedge.go is the SERVER-SIDE consumption of CovererReport_MEMBER_EDGE — the
// re-implementation of the in-process halves the coverer's tap split routed up
// (DESIGN-server-coverer-split §8 / tap.go onHostChange). The coverer owns the
// RIB-survival guard (the tap-fed /32 mirror) and ships only its VERDICT over the
// seam; the server rebuilds, off that verdict stream, the global member→edge
// presence map the monolith read straight out of its in-process guard, and drives
// the same four consumers:
//
//   - the render-time anchor-suppression gate (T-607, orchestrator WithAdvertiseGate)
//   - the unsolicited member-up / member-down BSS emits (emitMemberUp/Down)
//   - the affected home's re-render (markOrRerender via Orch/edgever)
//   - the anchor intent↔physical audit (anchoraudit.go ReconcileAnchors)
//
// It also supplies the global member→edge view DESIGN placement-locality-gap wants.

// memberPresence is the server-side member→edge PRESENCE/VERDICT map, the standin for
// the monolith's in-process guard. It is rebuilt purely from the MEMBER_EDGE report
// stream (LIVE single-host verdicts + the EOR/drift full snapshots), so the server
// never needs a tap of its own.
//
// Keying: (edge, member) → the SET of coverer ids currently asserting the member
// PRESENT on that edge, each stamped with a lease deadline. `edge` is the report's
// EdgeId — the tap edge that originated the /32, which for a correctly-placed member
// is its home (so the suppression gate, queried by HOME at render time, hits the same
// key; under the placement-locality gap they differ and the gate fails static, never
// blackholes). The per-coverer set IS the K-coverer dedup: K coverers each tap the
// edge and each asserts presence independently; the AGGREGATE verdict is
//
//	present  ⇔ at least one covering coverer asserts the member present
//	absent   ⇔ no coverer asserts it AND the family's view is trustworthy (validView)
//
// i.e. exactly guard.HasHost / guard.ShouldWithdraw, lifted across coverers and made
// fail-static: absence is honored (suppress / member-down) only once some coverer has
// delivered a snapshot for the family (validView), the server-side analog of the
// guard's view-valid (EOR-seen) gate. Without a snapshot the server has no trustworthy
// view and advertises — never withholds — so a cold/replaying server cannot blackhole.
//
// LEASE / TTL (the chunked-snapshot handling): the MEMBER_EDGE proto carries no
// last-chunk marker and no family field, so a multi-batch snapshot (memberEdgeChunk =
// 50_000) cannot be reconciled atomically (a documented gap). Rather than destructively
// replace a present-set from a single, possibly-partial report, each present-assertion
// REFRESHES a lease on its members; the periodic sweep (sweepExpired, run from
// ReconcileAnchors) reaps members whose lease lapsed — i.e. members the coverer's
// periodic snapshot (EOR + ReconcileTapView re-emit) stopped refreshing because they
// were withdrawn (a missed Withdrawal the LIVE Down=true path never delivered). Chunk
// ordering/atomicity is irrelevant: every chunk refreshes its own members' leases. The
// only cost is that a drift-only loss lingers up to one lease window before it suppresses
// (fail-static, safe) — eager (LIVE Down=true) withdrawals are instant.
type memberPresence struct {
	mu  sync.Mutex
	now func() time.Time
	ttl time.Duration

	// present[edge][member][coverer] = lease deadline. A non-empty inner map ⇒ the
	// member is present on the edge (per ≥1 coverer). Absent/empty ⇒ not present.
	present map[model.EdgeID]map[netip.Prefix]map[string]time.Time
	// validView[viewKey][coverer] = lease deadline; a coverer is recorded here once it
	// has delivered a present-assertion for (edge, family) — proof its view is live for
	// absence checks. Any non-expired coverer ⇒ the family's view is trustworthy.
	validView map[viewKey]map[string]time.Time
}

// viewKey identifies the trustworthiness of an (edge, family) view.
type viewKey struct {
	edge   model.EdgeID
	family model.Family
}

// defaultMemberPresenceTTL is the lease window for a present-assertion. It MUST exceed
// the coverer's snapshot cadence (EOR on (re)connect + RunReconcileTapView interval) by
// a safety factor so a still-present member is always re-refreshed before its lease
// lapses; a missed-withdrawal member, no longer in any snapshot, lapses and is reaped.
const defaultMemberPresenceTTL = 5 * time.Minute

func newMemberPresence(now func() time.Time, ttl time.Duration) *memberPresence {
	if now == nil {
		now = time.Now
	}
	if ttl <= 0 {
		ttl = defaultMemberPresenceTTL
	}
	return &memberPresence{
		now:       now,
		ttl:       ttl,
		present:   map[model.EdgeID]map[netip.Prefix]map[string]time.Time{},
		validView: map[viewKey]map[string]time.Time{},
	}
}

// markPresent records (edge, member) present per coverer and refreshes its lease +
// the (edge, family) view validity. It returns wasAbsent=true iff the member was
// absent across ALL coverers before this call — the absent→present transition the
// caller turns into a member-up emit (the K-coverer dedup: only the FIRST coverer's
// assertion transitions; the rest just refresh).
func (m *memberPresence) markPresent(edge model.EdgeID, coverer string, member netip.Prefix) (wasAbsent bool) {
	deadline := m.now().Add(m.ttl)
	m.mu.Lock()
	defer m.mu.Unlock()
	byMember := m.present[edge]
	if byMember == nil {
		byMember = map[netip.Prefix]map[string]time.Time{}
		m.present[edge] = byMember
	}
	covs := byMember[member]
	wasAbsent = !anyLive(covs, m.now())
	if covs == nil {
		covs = map[string]time.Time{}
		byMember[member] = covs
	}
	covs[coverer] = deadline
	m.refreshViewLocked(edge, model.FamilyOf(member), coverer, deadline)
	return wasAbsent
}

// markAbsent drops a coverer's presence assertion for (edge, member) — the LIVE
// Down=true verdict (the coverer's guard already passed ShouldWithdraw) or a lease
// reap. It returns nowAbsent=true iff the member is now absent across ALL coverers AND
// the family's view is trustworthy — the present→absent transition the caller turns
// into a member-down emit + suppression re-light.
func (m *memberPresence) markAbsent(edge model.EdgeID, coverer string, member netip.Prefix) (nowAbsent bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	byMember := m.present[edge]
	if byMember == nil {
		return false
	}
	covs := byMember[member]
	if !anyLive(covs, m.now()) {
		return false // already absent — no transition (dedup)
	}
	delete(covs, coverer)
	if len(covs) == 0 {
		delete(byMember, member)
	}
	if anyLive(byMember[member], m.now()) {
		return false // another coverer still asserts presence
	}
	return m.viewValidLocked(edge, model.FamilyOf(member))
}

// refreshViewLocked records/extends a coverer's view validity for (edge, family).
func (m *memberPresence) refreshViewLocked(edge model.EdgeID, fam model.Family, coverer string, deadline time.Time) {
	k := viewKey{edge, fam}
	cv := m.validView[k]
	if cv == nil {
		cv = map[string]time.Time{}
		m.validView[k] = cv
	}
	cv[coverer] = deadline
}

// viewValidLocked reports whether (edge, family) has a trustworthy view — any coverer
// with a non-expired snapshot lease. Caller holds m.mu.
func (m *memberPresence) viewValidLocked(edge model.EdgeID, fam model.Family) bool {
	return anyLive(m.validView[viewKey{edge, fam}], m.now())
}

// shouldWithdraw is the server-side guard.ShouldWithdraw: the member is a HOST whose
// (edge, family) view is trustworthy and which no coverer asserts present → its anchor
// must be withheld (T-607). Drives both the render suppression gate and the audit.
func (m *memberPresence) shouldWithdraw(edge model.EdgeID, member netip.Prefix) bool {
	if !model.IsHost(member) {
		return false // /24 bare-metal blocks are not host-presence signals — never gated
	}
	fam := model.FamilyOf(member)
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.viewValidLocked(edge, fam) {
		return false // no trustworthy view → fail-static (advertise, never blackhole)
	}
	return !anyLive(m.present[edge][member], m.now())
}

// hostsByFamily returns the host prefixes of `family` currently present on `edge`
// (per any live coverer) — the server-side guard.HostsByFamily the anchor audit reads
// for the ROGUE direction. Returns ok=false when the view is not trustworthy, so the
// audit skips the edge (mirrors the monolith's ViewValid guard).
func (m *memberPresence) hostsByFamily(edge model.EdgeID, family model.Family) (hosts []netip.Prefix, ok bool) {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.viewValidLocked(edge, family) {
		return nil, false
	}
	for member, covs := range m.present[edge] {
		if model.FamilyOf(member) == family && anyLive(covs, now) {
			hosts = append(hosts, member)
		}
	}
	return hosts, true
}

// sweepExpired reaps every lapsed presence/view lease and returns the (edge, member)
// pairs that became absent-across-all-coverers under a still-trustworthy view — the
// drift-repair member-down transitions the caller emits + re-renders for. Run from the
// periodic ReconcileAnchors sweep, NOT the report hot path.
func (m *memberPresence) sweepExpired() []memberLoss {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	var losses []memberLoss
	for edge, byMember := range m.present {
		for member, covs := range byMember {
			// A member lapses when EVERY coverer's lease has expired with no refresh (a
			// missed Withdrawal the periodic snapshot stopped carrying). `had` captures
			// "was tracked" — NOT anyLive(now), which is already false for the just-expired
			// leases we are about to reap (that bug made this reaper dead code: deleting
			// non-live entries can't change anyLive, so before==after always).
			had := len(covs) > 0
			for c, dl := range covs {
				if !dl.After(now) {
					delete(covs, c)
				}
			}
			if len(covs) == 0 {
				delete(byMember, member)
			}
			if had && !anyLive(byMember[member], now) && m.viewValidLocked(edge, model.FamilyOf(member)) {
				losses = append(losses, memberLoss{edge: edge, member: member})
			}
		}
		if len(byMember) == 0 {
			delete(m.present, edge)
		}
	}
	for k, cv := range m.validView {
		for c, dl := range cv {
			if !dl.After(now) {
				delete(cv, c)
			}
		}
		if len(cv) == 0 {
			delete(m.validView, k)
		}
	}
	return losses
}

// memberLoss is one (edge, member) that lapsed to absent in the sweep.
type memberLoss struct {
	edge   model.EdgeID
	member netip.Prefix
}

// anyLive reports whether any lease in the set is still in the future.
func anyLive(covs map[string]time.Time, now time.Time) bool {
	for _, dl := range covs {
		if dl.After(now) {
			return true
		}
	}
	return false
}

// ---- the host-change pipeline (server re-implementation of tap.go onHostChange) ----

// onMemberEdge ingests one MEMBER_EDGE report. Two shapes (coverer.go reportMemberEdge):
//
//   - Down=true, one member: a LIVE post-EOR WITHDRAWAL whose ShouldWithdraw verdict the
//     coverer already passed → drive the member-down / suppression transition.
//   - Down=false: a present-assertion. A SNAPSHOT batch (EOR / drift re-emit, ≥2 members
//     since a live change is always single-host) is the authoritative reconcile point:
//     refresh every member's lease + the family view, then re-render the edge ONCE to
//     re-light suppression — NO per-member emits (mirrors the monolith's single EOR
//     markOrRerender, which never floods member-up per replayed host). A SINGLE-member
//     Down=false is a LIVE add → the member-up transition.
//
// An empty-Members report is an empty-family snapshot whose family the proto cannot
// convey (no prefix to infer from, no family field) — nothing to reconcile; skipped
// (documented gap, benign: an empty family homes no host members to suppress).
//
// The map mutation is synchronous (ordering + dedup correctness); the YugabyteDB
// memberHome lookup + emit + re-render run in a goroutine so the gRPC Report stream is
// never blocked on a DB round-trip (exactly as the monolith's onHostChange did).
func (cp *ControlPlane) onMemberEdge(ctx context.Context, r *rpc.CovererReport) {
	if cp.presence == nil {
		return
	}
	edge := model.EdgeID(r.EdgeId)
	coverer := r.CovererId
	if coverer == "" {
		coverer = "?" // K-dedup degrades to a single anonymous coverer; still correct
	}
	members := parseMembers(r.Members)

	if r.Down {
		for _, mbr := range members { // normally exactly one
			cp.applyMemberAbsent(edge, coverer, mbr)
		}
		return
	}
	if len(members) == 0 {
		return // empty-family snapshot (family unconvertible) — documented gap
	}
	if len(members) >= 2 {
		// SNAPSHOT reconcile: bulk-refresh leases + view validity, then re-light
		// suppression for the tap edge in one shot. No per-member member-up flood.
		for _, mbr := range members {
			cp.presence.markPresent(edge, coverer, mbr)
		}
		go cp.markOrRerender(context.Background(), edge)
		return
	}
	// len == 1: LIVE add (or a one-host edge's snapshot) — member-up on a real transition.
	cp.applyMemberPresent(edge, coverer, members[0])
}

// applyMemberPresent records a LIVE host appearance and, on the absent→present
// transition, emits member-up to the member's HOME + re-renders it (so the now-present
// anchor is advertised). The home lookup + emit run async (DB round-trip off the hot path).
func (cp *ControlPlane) applyMemberPresent(edge model.EdgeID, coverer string, member netip.Prefix) {
	wasAbsent := cp.presence.markPresent(edge, coverer, member)
	if !wasAbsent {
		return // already present per another coverer — no transition (K-dedup)
	}
	go func() {
		ctx := context.Background()
		home, poolID, ok, err := cp.memberHome(ctx, member)
		if err != nil || !ok || home == "" {
			return // not a pool member, or no current home — ignore (matches monolith)
		}
		// Cross-replica dedup (the fan's IsSubscribed is the server analog of the
		// monolith's Agents.IsSubscribed): emit + re-render from exactly the replica
		// whose coverer holds the home's stream.
		if cp.fan == nil || cp.fan.IsSubscribed(home) {
			cp.emitMemberUp(poolID, home, member)
		}
		cp.markOrRerender(ctx, home)
	}()
}

// applyMemberAbsent records a host disappearance (a LIVE Down=true verdict or a lease
// reap) and, on the present→absent transition under a trustworthy view, emits
// member-down + re-renders the home (so the anchor is suppressed, T-607).
func (cp *ControlPlane) applyMemberAbsent(edge model.EdgeID, coverer string, member netip.Prefix) {
	nowAbsent := cp.presence.markAbsent(edge, coverer, member)
	if !nowAbsent {
		return // still present per another coverer, or view untrustworthy — no transition
	}
	go func() {
		ctx := context.Background()
		home, poolID, ok, err := cp.memberHome(ctx, member)
		if err != nil || !ok || home == "" {
			return
		}
		if cp.fan == nil || cp.fan.IsSubscribed(home) {
			cp.emitMemberDown(poolID, home, member, "route-withdrawal")
		}
		cp.markOrRerender(ctx, home)
	}()
}

// markOrRerender applies an edge's current desired state, mirroring the monolith
// (tap.go): under sharding (edgever wired) route through edgever — bump the edge's
// desired version so whichever replica holds its coverer stream re-renders + delivers
// (the cross-replica path, avoiding the replica-local "not subscribed" noise);
// single-replica falls back to the direct RerenderEdge.
func (cp *ControlPlane) markOrRerender(ctx context.Context, edge model.EdgeID) {
	if edge == "" || cp.Orch == nil {
		return
	}
	if cp.edgever != nil {
		cp.Orch.MarkEdge(ctx, edge)
		return
	}
	if err := cp.Orch.RerenderEdge(ctx, edge); err != nil {
		cp.log.Warn("member-edge re-render failed", "edge", edge, "err", err)
	}
}

// memberSuppressed is the orchestrator WithAdvertiseGate (T-607): withhold a HOST
// member's ingress anchor + egress redirect on `edge` exactly when the presence map
// trustworthily confirms its absence there (view valid ∧ host absent). Non-host members
// and untrustworthy/unknown views are never suppressed (fail-static). This is the render
// gate the monolith fed from g.ShouldWithdraw, re-lit off the report-built presence map.
func (cp *ControlPlane) memberSuppressed(edge model.EdgeID, member netip.Prefix) bool {
	if cp.presence == nil {
		return false
	}
	return cp.presence.shouldWithdraw(edge, member)
}

// memberHome resolves a host /32 (/128) member prefix to its claiming pool and that
// pool's current PRIMARY (home) edge — the host→home lookup the member-up/down pipeline
// needs (mirrors the monolith's memberHome). The member→pool claim and the pool→home
// assignment both live in the MANDATORY YugabyteDB bulk store (cp.YB always wired): a
// create writes ZERO etcd src→home, so YB is the only source. Not-found ⇒ ok=false.
func (cp *ControlPlane) memberHome(ctx context.Context, host netip.Prefix) (model.EdgeID, model.PoolID, bool, error) {
	cs, err := cp.YB.MemberConflicts(ctx, []netip.Prefix{host}, 0)
	if err != nil {
		return "", 0, false, err
	}
	for _, c := range cs {
		if c.Prefix != host {
			continue // exact member claim only, not a broader/narrower overlap
		}
		rec, ok, err := cp.YB.Get(ctx, c.PoolID)
		if err != nil || !ok {
			return "", 0, false, err
		}
		return rec.Primary, c.PoolID, rec.Primary != "", nil
	}
	return "", 0, false, nil
}

// parseMembers parses the MEMBER_EDGE CIDR wire form, skipping malformed entries.
func parseMembers(cidrs []string) []netip.Prefix {
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, s := range cidrs {
		if p, err := netip.ParsePrefix(s); err == nil {
			out = append(out, p)
		}
	}
	return out
}

// prefixSet / sortPrefixes are the small set helpers the anchor audit shares.
func prefixSet(ps []netip.Prefix) map[netip.Prefix]struct{} {
	out := make(map[netip.Prefix]struct{}, len(ps))
	for _, p := range ps {
		out[p] = struct{}{}
	}
	return out
}

func sortPrefixes(ps []netip.Prefix) {
	sort.Slice(ps, func(i, j int) bool {
		if ps[i].Addr() != ps[j].Addr() {
			return ps[i].Addr().Less(ps[j].Addr())
		}
		return ps[i].Bits() < ps[j].Bits()
	})
}
