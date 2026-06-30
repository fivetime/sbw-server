// Package render materializes the controller's authoritative model — pools with
// their home-edge assignments (§3.4) — into the per-edge EdgeDesiredState the
// distribution layer ships and agents reconcile (T-703). It is a pure,
// deterministic function of (pools, options): no I/O, no clock.
//
// It enforces §5.1 双向归位 BY CONSTRUCTION: a rate-limit pool's home edge
// receives both its members' /32 anchors (ingress homing) AND their source
// FlowSpec redirects pointing at that same edge (egress homing) — so ingress and
// egress home are the same edge, the controller's core consistency invariant
// (topology §2.3), never split.
package render

import (
	"fmt"
	"net/netip"
	"sort"

	"github.com/fivetime/sbw-contract/model"
)

// Options parameterizes a render.
type Options struct {
	// Generation stamps every produced EdgeDesiredState (idempotent apply /
	// reconcile ordering, §7).
	Generation uint64

	// EdgeAddrs / EdgeAddrs6 give each edge's v4 / v6 redirect next-hop — its own
	// address that R redirects egress-homed traffic to (FlowRedirect target).
	// EdgeAddrs is required for a rate-limit pool with IPv4 members homed to that
	// edge; EdgeAddrs6 for IPv6 members (RFC 5701 redirect-to-IPv6).
	EdgeAddrs  map[model.EdgeID]netip.Addr
	EdgeAddrs6 map[model.EdgeID]netip.Addr

	// HomeMarker, if set, returns the marker large community added to a home
	// edge's rate-limit anchors so MX204 raises local-pref and locks the home
	// winner (§4.7 / T-703, the resolution for multi-source conflicts). ok=false
	// skips the marker for that edge.
	HomeMarker func(model.EdgeID) (model.LargeCommunity, bool)

	// Suppress, if set, gates ADVERTISEMENT of a member on an edge (T-607,
	// §6.4-1 / §7): when it returns true, the member's ingress /32 anchor AND its
	// egress FlowSpec redirect are omitted — the controller has trustworthy
	// evidence (RIB-survival guard: host route absent + view valid) that the
	// member is gone, so advertising would blackhole. The limiting machinery
	// (policer/classify) is kept, so the member resumes cleanly if its host
	// returns. Fail-static: the guard returns true ONLY on certain absence, so an
	// uncertain/frozen view keeps advertising (never yanked on a tap flap).
	Suppress func(edge model.EdgeID, member netip.Prefix) bool
}

// DesiredStates renders one EdgeDesiredState per home edge. Only edges that are
// home to at least one pool appear in the result; the caller adds empty states
// for idle edges if it wants to prune them. The output slices are sorted, so
// equal inputs yield equal states (deterministic distribution / diffing).
func DesiredStates(pools []model.Pool, opt Options) (map[model.EdgeID]model.EdgeDesiredState, error) {
	states := map[model.EdgeID]*model.EdgeDesiredState{}

	get := func(edge model.EdgeID) *model.EdgeDesiredState {
		s, ok := states[edge]
		if !ok {
			s = &model.EdgeDesiredState{SchemaVersion: model.SchemaVersion, EdgeID: edge, Generation: opt.Generation}
			states[edge] = s
		}
		return s
	}

	// Deterministic order: pools by id, members by prefix.
	sorted := append([]model.Pool(nil), pools...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	for _, p := range sorted {
		if p.HomeEdge == "" {
			return nil, fmt.Errorf("render: pool %d has no home edge", p.ID)
		}
		members := append([]model.Member(nil), p.Members...)
		sort.Slice(members, func(i, j int) bool { return members[i].Prefix.String() < members[j].Prefix.String() })

		st := get(p.HomeEdge)
		switch p.Action.Kind {
		case model.ActionRateLimit:
			if err := renderRateLimit(st, p, members, p.HomeEdge, opt); err != nil {
				return nil, err
			}
		case model.ActionBlackhole:
			renderBlackhole(st, p, members, p.HomeEdge, opt)
		default:
			return nil, fmt.Errorf("render: pool %d: unsupported action %v (scrub is V2)", p.ID, p.Action.Kind)
		}
	}

	out := make(map[model.EdgeID]model.EdgeDesiredState, len(states))
	for e, s := range states {
		out[e] = *s
	}
	return out, nil
}

// ForEdge renders ONE edge's complete EdgeDesiredState from the pools homed to
// it: its primary pools (full — limiting machinery + advertised anchors + egress
// FlowSpec redirects, §5.1 双向归位) plus its backup pools (standby pre-build —
// policer/classify only, never advertised, §5.3). This is the per-edge view the
// pool-create transaction ships: the same render math as DesiredStates, but
// scoped to one edge and aware of its primary-vs-backup role for each pool.
//
// All pools in primary/backup are assumed homed to edge (the caller — the
// orchestrator that just placed them — guarantees this); HomeEdge is not
// re-read, so a pool's record need only name THIS edge as the relevant home.
// Outputs are sorted for determinism.
func ForEdge(edge model.EdgeID, primary, backup []model.Pool, opt Options) (model.EdgeDesiredState, error) {
	st := model.EdgeDesiredState{SchemaVersion: model.SchemaVersion, EdgeID: edge, Generation: opt.Generation}

	render := func(pools []model.Pool, standby bool) error {
		sorted := append([]model.Pool(nil), pools...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
		for _, p := range sorted {
			members := append([]model.Member(nil), p.Members...)
			sort.Slice(members, func(i, j int) bool { return members[i].Prefix.String() < members[j].Prefix.String() })
			if standby {
				if err := renderStandby(&st, p, members); err != nil {
					return err
				}
				continue
			}
			switch p.Action.Kind {
			case model.ActionRateLimit:
				if err := renderRateLimit(&st, p, members, edge, opt); err != nil {
					return err
				}
			case model.ActionBlackhole:
				renderBlackhole(&st, p, members, edge, opt)
			default:
				return fmt.Errorf("render: pool %d: unsupported action %v (scrub is V2)", p.ID, p.Action.Kind)
			}
		}
		return nil
	}

	if err := render(primary, false); err != nil {
		return model.EdgeDesiredState{}, err
	}
	if err := render(backup, true); err != nil {
		return model.EdgeDesiredState{}, err
	}
	return st, nil
}

// ForPool renders ONE pool's contribution to an edge's desired state (the O(1)
// incremental hot path, scalability fix). It is the per-pool analog of ForEdge:
// the same render math, but scoped to a single pool playing one role on the edge —
// PRIMARY (standby=false: limiting machinery + advertised anchors + egress FlowSpec
// redirects, §5.1 双向归位) or BACKUP (standby=true: policer/classify pre-built,
// nothing advertised, §5.3). The result is a model.PoolDelta the controller ships
// in an EdgeDesiredDelta and the agent applies WITHOUT re-diffing the whole edge.
//
// It renders into a scratch EdgeDesiredState and projects that single pool's
// resources out, so the per-pool and whole-edge paths stay byte-for-byte
// consistent by construction (no second renderer to drift). pool is assumed homed
// to edge in the given role (the caller — the orchestrator that placed it —
// guarantees this).
func ForPool(edge model.EdgeID, pool model.Pool, standby bool, opt Options) (model.PoolDelta, error) {
	st := model.EdgeDesiredState{SchemaVersion: model.SchemaVersion, EdgeID: edge, Generation: opt.Generation}
	members := append([]model.Member(nil), pool.Members...)
	sort.Slice(members, func(i, j int) bool { return members[i].Prefix.String() < members[j].Prefix.String() })

	if standby {
		if err := renderStandby(&st, pool, members); err != nil {
			return model.PoolDelta{}, err
		}
	} else {
		switch pool.Action.Kind {
		case model.ActionRateLimit:
			if err := renderRateLimit(&st, pool, members, edge, opt); err != nil {
				return model.PoolDelta{}, err
			}
		case model.ActionBlackhole:
			renderBlackhole(&st, pool, members, edge, opt)
		default:
			return model.PoolDelta{}, fmt.Errorf("render: pool %d: unsupported action %v (scrub is V2)", pool.ID, pool.Action.Kind)
		}
	}

	return model.PoolDelta{
		PoolID:            pool.ID,
		Policers:          st.Policers,
		ClassifySessions:  st.ClassifySessions,
		Anchors:           st.Anchors,
		FlowRedirects:     st.FlowRedirects,
		RedirectNextHop:   st.RedirectNextHop,
		RedirectNextHopV6: st.RedirectNextHopV6,
	}, nil
}

// addPolicersClassify adds the pool's two shared policers + per-member classify
// sessions (the limiting machinery). Shared by a PRIMARY home (full) and a
// BACKUP home (standby pre-build, §5.3 — 备预建 policer/classify 空跑).
func addPolicersClassify(st *model.EdgeDesiredState, p model.Pool, members []model.Member) error {
	st.Policers = append(st.Policers,
		policerSpec(p, model.DirectionIngress, p.IngressRate),
		policerSpec(p, model.DirectionEgress, p.EgressRate),
	)
	for _, m := range members {
		if err := m.Validate(); err != nil {
			return fmt.Errorf("render: pool %d member %s: %w", p.ID, m.Prefix, err)
		}
		in, err := classifySession(p, m, model.DirectionIngress)
		if err != nil {
			return err
		}
		eg, err := classifySession(p, m, model.DirectionEgress)
		if err != nil {
			return err
		}
		st.ClassifySessions = append(st.ClassifySessions, in, eg)
	}
	return nil
}

// renderStandby pre-builds the limiting machinery WITHOUT advertising (§5.3 —
// 备 L 预建 policer/classify 空跑,绝不预通告 /32 / 预发 FlowSpec). Only
// rate-limit pools pre-build; a blackhole backup has nothing to pre-build.
func renderStandby(st *model.EdgeDesiredState, p model.Pool, members []model.Member) error {
	if p.Action.Kind != model.ActionRateLimit {
		return nil
	}
	return addPolicersClassify(st, p, members)
}

func renderRateLimit(st *model.EdgeDesiredState, p model.Pool, members []model.Member, home model.EdgeID, opt Options) error {
	if err := addPolicersClassify(st, p, members); err != nil {
		return err
	}

	var marker *model.LargeCommunity
	if opt.HomeMarker != nil {
		if lc, ok := opt.HomeMarker(home); ok {
			marker = &lc
		}
	}

	var haveV4, haveV6 bool
	for _, m := range members {
		// T-607: skip advertising a member the guard says is certainly gone — both
		// its ingress anchor and egress redirect — to avoid blackholing. The
		// policer/classify added above stay, so it resumes if the host returns.
		if opt.Suppress != nil && opt.Suppress(home, m.Prefix) {
			continue
		}
		// Ingress homing: advertise the member /32 (with the home marker).
		a := model.Anchor{Prefix: m.Prefix}
		if marker != nil {
			a.LargeCommunities = []model.LargeCommunity{*marker}
		}
		st.Anchors = append(st.Anchors, a)

		// Egress homing: source FlowSpec → redirect to this edge. v4 carries the
		// RFC 8955 type-0x81 EC (RedirectNextHop); v6 the RFC 5701 IPv6-Address-
		// Specific redirect EC (RedirectNextHopV6). Same FlowRedirect entry either way.
		st.FlowRedirects = append(st.FlowRedirects, model.FlowRedirect{SrcPrefix: m.Prefix})
		if m.Prefix.Addr().Is6() {
			haveV6 = true
		} else {
			haveV4 = true
		}
	}

	if haveV4 {
		addr, ok := opt.EdgeAddrs[home]
		if !ok || !addr.IsValid() || !addr.Is4() {
			return fmt.Errorf("render: edge %s home to rate-limit pool %d needs a valid IPv4 redirect next-hop", home, p.ID)
		}
		st.RedirectNextHop = addr
	}
	if haveV6 {
		addr, ok := opt.EdgeAddrs6[home]
		if !ok || !addr.IsValid() || !addr.Is6() {
			return fmt.Errorf("render: edge %s home to rate-limit pool %d needs a valid IPv6 redirect next-hop", home, p.ID)
		}
		st.RedirectNextHopV6 = addr
	}
	return nil
}

func renderBlackhole(st *model.EdgeDesiredState, p model.Pool, members []model.Member, home model.EdgeID, opt Options) {
	// Drop happens in the upstream's network via the RTBH community; no policer,
	// no classify, no redirect — just the advertised /32 carrier.
	for _, m := range members {
		if opt.Suppress != nil && opt.Suppress(home, m.Prefix) {
			continue // host gone → don't advertise (T-607)
		}
		a := model.Anchor{Prefix: m.Prefix}
		if p.Action.RTBHCommunity != nil {
			a.Communities = []model.Community{*p.Action.RTBHCommunity}
		}
		if p.Action.RTBHLargeCommunity != nil {
			a.LargeCommunities = []model.LargeCommunity{*p.Action.RTBHLargeCommunity}
		}
		st.Anchors = append(st.Anchors, a)
	}
}

// unlimitedCIRKbps is the policer rate for an UNLIMITED pool (CIR==0 = 95th-
// percentile billing, "无限带宽"): effectively line-rate, and with exceed=transmit
// it never drops — the policer is there only to COUNT bytes (for 95th-pct billing),
// not to limit. 100 Gbps is well above any member's real traffic.
const unlimitedCIRKbps = 100_000_000

func policerSpec(p model.Pool, dir model.Direction, r model.RateSpec) model.PolicerSpec {
	// CIR==0 → unlimited (95th-percentile): count, never drop.
	cir, rt, cb, exceed := r.CIR, r.Type, r.CommittedBurstBytes, model.PolicerDrop
	if r.CIR == 0 {
		cir, rt, exceed = unlimitedCIRKbps, model.RateKbps, model.PolicerTransmit
		if cb == 0 {
			cb = 12_500_000
		}
	}
	return model.PolicerSpec{
		Name:                  model.PolicerName(p.ID, dir),
		PoolID:                p.ID,
		Direction:             dir,
		Type:                  model.Policer1R2C,
		RateType:              rt,
		CIR:                   cir,
		CommittedBurstBytes:   cb,
		CommittedBurstPackets: r.CommittedBurstPackets,
		ConformAction:         model.PolicerTransmit,
		ExceedAction:          exceed,
		BindWorker:            p.BindWorker,
	}
}

func classifySession(p model.Pool, m model.Member, dir model.Direction) (model.ClassifySession, error) {
	mask, err := m.MaskKind(dir)
	if err != nil {
		return model.ClassifySession{}, fmt.Errorf("render: pool %d member %s (%v): %w", p.ID, m.Prefix, dir, err)
	}
	return model.ClassifySession{
		PoolID:      p.ID,
		Prefix:      m.Prefix,
		Direction:   dir,
		Mask:        mask,
		PolicerName: model.PolicerName(p.ID, dir),
	}, nil
}
