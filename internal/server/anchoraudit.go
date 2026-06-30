package server

import (
	"context"
	"net/netip"
	"time"

	"github.com/fivetime/sbw-contract/model"
)

// Anchor intent↔physical reconciliation (DESIGN-liveness §11/§2, L-04). The physical
// side of this audit was the RIB-survival guard's tap view, a COVERER concern. The
// server has no tap; it rebuilds the physical member view from the coverer's
// CovererReport_MEMBER_EDGE stream (memberedge.go memberPresence) and reconciles the
// Yugabyte anchor INTENT against it — the server-side re-light of the monolith's
// in-process ReconcileAnchors. It also reaps lapsed presence leases (the chunked-
// snapshot drift-repair, see memberPresence) and emits the resulting member-down
// transitions, so a missed Withdrawal that no LIVE Down=true delivered is still caught.

// AnchorMismatch is one edge's anchor intent↔physical disagreement.
type AnchorMismatch struct {
	Edge model.EdgeID
	// Unprovisioned: host members Yugabyte homes here whose /32 the presence map
	// trustworthily reports absent (view valid) — the anchor is suppressed (T-607),
	// blackhole averted; this is the audited alarm for that suppression.
	Unprovisioned []netip.Prefix
	// Rogue: host /32 a coverer reports present on the edge that no pool homes here.
	Rogue []netip.Prefix
}

// ReconcileAnchors audits each live edge's anchor INTENT (Yugabyte primary-pool host
// members) against the PHYSICAL member view (the coverer-fed presence map). It first
// reaps lapsed presence leases — emitting the drift-repair member-down transitions —
// then, per (edge, family) with a trustworthy view, flags Unprovisioned (intended but
// physically absent → anchor suppressed) and Rogue (physically present but unintended).
// Inert until the first MEMBER_EDGE report establishes a view; until then every family
// is untrustworthy and skipped (no false alarms, no false suppression).
func (cp *ControlPlane) ReconcileAnchors(ctx context.Context) ([]AnchorMismatch, error) {
	if cp.presence == nil {
		return nil, nil
	}
	// Drift-repair: reap lapsed leases and fire member-down for members that lapsed to
	// absent-across-all under a still-trustworthy view (a missed Withdrawal the LIVE
	// path never delivered). Deduped re-render per home.
	cp.reapMemberLeases(ctx)

	agents, err := cp.Registry.List(ctx)
	if err != nil {
		return nil, err
	}
	var out []AnchorMismatch
	for _, a := range agents {
		edge := a.EdgeID
		if cp.Liveness != nil && cp.Liveness.IsDead(edge) {
			continue // dead edge: liveness/failover owns it, not this audit
		}
		intent, err := cp.intentHosts(ctx, edge)
		if err != nil {
			return nil, err
		}
		m := AnchorMismatch{Edge: edge}
		for _, fam := range []model.Family{model.FamilyIPv4, model.FamilyIPv6} {
			phys, ok := cp.presence.hostsByFamily(edge, fam)
			if !ok {
				continue // untrustworthy view: skip (no false unprovisioned/rogue)
			}
			physical := prefixSet(phys)
			for p := range intent {
				if model.FamilyOf(p) != fam {
					continue
				}
				if _, ok := physical[p]; !ok {
					m.Unprovisioned = append(m.Unprovisioned, p)
				}
			}
			for p := range physical {
				if _, ok := intent[p]; !ok {
					m.Rogue = append(m.Rogue, p)
				}
			}
		}
		if len(m.Unprovisioned) == 0 && len(m.Rogue) == 0 {
			if cp.metrics != nil {
				cp.metrics.AnchorMismatch(edge, 0, 0) // clear a previously-raised gauge
			}
			continue
		}
		sortPrefixes(m.Unprovisioned)
		sortPrefixes(m.Rogue)
		if cp.metrics != nil {
			cp.metrics.AnchorMismatch(edge, len(m.Unprovisioned), len(m.Rogue))
		}
		cp.log.Warn("anchor intent↔physical mismatch (L-04)",
			"edge", edge, "unprovisioned", m.Unprovisioned, "rogue", m.Rogue)
		if cp.onAnchorMismatch != nil {
			cp.onAnchorMismatch(m)
		}
		out = append(out, m)
	}
	return out, nil
}

// reapMemberLeases sweeps lapsed presence leases and emits a member-down for each
// (edge, member) that lapsed to absent-across-all under a trustworthy view, re-rendering
// each affected home once. This is the chunked-snapshot drift backstop: a withdrawal the
// coverer's snapshot stopped refreshing (but no LIVE Down=true carried) is realized here.
func (cp *ControlPlane) reapMemberLeases(ctx context.Context) {
	losses := cp.presence.sweepExpired()
	if len(losses) == 0 {
		return
	}
	rerendered := map[model.EdgeID]struct{}{}
	for _, l := range losses {
		home, poolID, ok, err := cp.memberHome(ctx, l.member)
		if err != nil || !ok || home == "" {
			continue
		}
		if cp.fan == nil || cp.fan.IsSubscribed(home) {
			cp.emitMemberDown(poolID, home, l.member, "route-withdrawal")
		}
		if _, done := rerendered[home]; !done {
			rerendered[home] = struct{}{}
			cp.markOrRerender(ctx, home)
		}
	}
}

// intentHosts is the set of HOST members (/32, /128) the controller intends this edge to
// anchor — the union of host members across the edge's primary pools (Yugabyte). Non-host
// (/24) members are not anchored and never gated, so they are excluded.
func (cp *ControlPlane) intentHosts(ctx context.Context, edge model.EdgeID) (map[netip.Prefix]struct{}, error) {
	pools, err := cp.poolsForHome(ctx, edge)
	if err != nil {
		return nil, err
	}
	out := make(map[netip.Prefix]struct{})
	for _, p := range pools {
		for _, mem := range p.Members {
			if model.IsHost(mem.Prefix) {
				out[mem.Prefix] = struct{}{}
			}
		}
	}
	return out, nil
}

// RunReconcileAnchors runs the anchor intent↔physical audit (+ the presence-lease
// drift sweep) on a timer (L-04). Live once the coverer feeds MEMBER_EDGE reports.
func (cp *ControlPlane) RunReconcileAnchors(ctx context.Context, interval time.Duration) {
	cp.runLoop(ctx, interval, func(ctx context.Context) {
		if _, err := cp.ReconcileAnchors(ctx); err != nil {
			cp.log.Warn("anchor reconciliation failed", "err", err)
		}
	})
}
