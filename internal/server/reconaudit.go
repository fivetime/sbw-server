package server

import (
	"context"
	"time"

	"github.com/fivetime/sbw-contract/model"
)

// Program reconciliation (controller §6 / limiter §6, B-02 "L 专属对账"): the
// data-plane fidelity counterpart to the §4.3 account reconciliation. Three
// numbers should agree for every live edge:
//
//   - EXPECTED: what the controller renders for the edge from the authoritative
//     Yugabyte store (PoolsForHome/Backup — policers + classify sessions it
//     intends the edge to program).
//   - DESIRED:  what the agent says it was TOLD to program (its held
//     EdgeDesiredState, EdgeReport.Health.Policers/SessionsDesired).
//   - ACTUAL:   what VPP is REALLY running (EdgeReport.Health.Policers/SessionsActual).
//
// EXPECTED ≠ DESIRED is a LOST PUSH (下发漏失): a desired-state update never
// reached the agent (dropped stream, skipped generation), so the agent is
// converging to a stale intent. ACTUAL ≠ DESIRED is PROGRAM DRIFT: the push
// landed but VPP did not end up matching it (a reconcile that could not apply).
//
// Unlike account reconciliation (which is read-only — a wrong ledger write could
// strand committed allocations), the cure here is SAFE and idempotent: re-render
// the edge and push it. A lost push is re-delivered; program drift triggers a
// fresh reconcile that self-heals VPP. So this audit auto-repushes the drifted
// edge after the drift persists past a debounce streak.

// ProgramDrift is one edge's expected-vs-desired-vs-actual count mismatch.
type ProgramDrift struct {
	Edge model.EdgeID
	// Kind is "delivery-loss" (expected ≠ desired) or "program-drift" (desired ≠
	// actual). When both are present, delivery-loss is reported (it is the upstream
	// cause — fixing it re-pushes desired, which then re-drives actual).
	Kind string

	ExpectedPolicers int
	DesiredPolicers  int
	ActualPolicers   int
	ExpectedSessions int
	DesiredSessions  int
	ActualSessions   int

	// Gap is the magnitude that tripped the alarm (Σ|count differences| for Kind).
	Gap int
	// Repushed is whether ReconcileProgram re-rendered+pushed the edge this cycle.
	Repushed bool
}

// ReconcileProgram audits every registered, live edge's data-plane program
// fidelity (B-02). For each edge with a trustworthy report it compares the
// controller's rendered EXPECTED counts against the agent's reported DESIRED and
// ACTUAL counts. A mismatch increments the edge's consecutive-drift streak; once
// the streak reaches the threshold the drift is alarmed, the edge is
// auto-repushed (re-render + push), and the streak resets. A clean cycle resets
// the streak immediately. Returns the drifts that alarmed this cycle.
//
// Trust gate: an edge is audited only when it is registered, not judged dead, has
// a cached report, and that report says VPP is connected and not data-plane-down.
// A soft-dead or booting edge is skipped — its counts are not yet meaningful and
// the liveness/failover path owns it, not this audit.
func (cp *ControlPlane) ReconcileProgram(ctx context.Context) ([]ProgramDrift, error) {
	agents, err := cp.Registry.List(ctx)
	if err != nil {
		return nil, err
	}
	var drifts []ProgramDrift
	for _, a := range agents {
		edge := a.EdgeID
		if cp.Liveness != nil && cp.Liveness.IsDead(edge) {
			cp.resetProgStreak(edge) // dead edge: liveness owns it, not this audit
			continue
		}
		rep, ok := cp.reports.get(edge)
		if !ok {
			continue // no report yet — nothing to compare
		}
		// Only trust the counts when the agent says VPP is up and reconciling.
		if !rep.Health.VPPConnected || rep.Health.SoftDead() {
			cp.resetProgStreak(edge)
			continue
		}

		// Phase gate (DESIGN-liveness §4.1): only a Ready edge — engine alive AND
		// caught up — can show a REAL program drift. While Reconciling (busy applying:
		// desired>actual is the EXPECTED catch-up, not a loss), Pending (starting), or
		// Degraded/Dead (the liveness path owns those), a gap is not a drift to repush.
		// Skipping here is what stops the false-drift edgever storm at its ROOT — and it
		// also spares the expensive ExpectedCounts render/query below for an edge that
		// isn't even in steady state. A pre-phase agent reports an empty Phase and falls
		// through to the legacy VPPConnected gate above.
		if p := rep.Health.Phase; p != "" && p != model.PhaseReady {
			cp.resetProgStreak(edge)
			continue
		}

		// Report-freshness gate (B-02 false-drift fix): if the agent has not yet applied
		// the controller's LATEST desired version, its reported counts reflect an OLDER
		// state. Comparing those to the freshly-rendered EXPECTED yields a PHANTOM
		// delivery-loss (the push is still in flight / resyncing, not lost). Auditing here
		// would auto-repush the full snapshot every cycle — for a backup-skewed huge edge
		// that re-feeds the slow converge and the push-queue (the 86GiB-leak driver). Skip
		// until applied == desired: a genuinely lost push is recovered by the resync path,
		// and a real drift still surfaces once the agent is caught up. AppliedVersion is a
		// stable content version the agent echoes (L-07); 0 desired = no edgever wired.
		if dv, err := cp.Orch.DesiredVersion(ctx, edge); err == nil && dv > 0 && rep.Health.AppliedVersion < dv {
			cp.resetProgStreak(edge)
			continue
		}

		expPol, expSess, err := cp.Orch.ExpectedCounts(ctx, edge)
		if err != nil {
			cp.log.Warn("program audit: render expected failed", "edge", edge, "err", err)
			continue
		}

		d := ProgramDrift{
			Edge:             edge,
			ExpectedPolicers: expPol, DesiredPolicers: rep.Health.PolicersDesired, ActualPolicers: rep.Health.PolicersActual,
			ExpectedSessions: expSess, DesiredSessions: rep.Health.SessionsDesired, ActualSessions: rep.Health.SessionsActual,
		}
		deliveryGap := abs(expPol-rep.Health.PolicersDesired) + abs(expSess-rep.Health.SessionsDesired)
		programGap := abs(rep.Health.PolicersDesired-rep.Health.PolicersActual) + abs(rep.Health.SessionsDesired-rep.Health.SessionsActual)

		switch {
		case deliveryGap > 0:
			d.Kind, d.Gap = "delivery-loss", deliveryGap
		case programGap > 0:
			d.Kind, d.Gap = "program-drift", programGap
		default:
			cp.resetProgStreak(edge) // all three agree — clean
			continue
		}

		// Drift this cycle: bump the streak; only act once it persists, to ride out
		// the benign window where a just-pushed update has not yet been reported.
		if cp.bumpProgStreak(edge) < cp.progThreshold {
			cp.log.Debug("program drift observed (within debounce)", "edge", edge, "kind", d.Kind, "gap", d.Gap)
			continue
		}
		cp.resetProgStreak(edge) // alarm fired; give the repush a cycle to land

		cp.metrics.ProgramDrift(edge, d.Kind, d.Gap)
		cp.log.Warn("data-plane program drift (B-02)", "edge", edge, "kind", d.Kind, "gap", d.Gap,
			"expected_policers", expPol, "desired_policers", rep.Health.PolicersDesired, "actual_policers", rep.Health.PolicersActual,
			"expected_sessions", expSess, "desired_sessions", rep.Health.SessionsDesired, "actual_sessions", rep.Health.SessionsActual)

		// Auto-repush: re-render + push the edge. Idempotent; re-delivers a lost
		// desired state and re-drives a reconcile that self-heals VPP. Skip it for an
		// UNSUBSCRIBED edge: the push fails "not subscribed" anyway, and RerenderEdge's
		// edgever bump only feeds the CAS write-conflict storm that wedged the controller
		// at 350K members (scale post-mortem — a VPP health-probe flap drops the gRPC
		// subscription, then this audit storms against the absent edge and starves
		// ingestion). The edge gets a full resync when it re-subscribes; the drift above
		// is still logged + metered for observability.
		switch {
		case !cp.fan.IsSubscribed(edge):
			cp.log.Info("program drift: edge's coverer not connected here, deferring repush to re-connect resync", "edge", edge, "kind", d.Kind)
		default:
			if err := cp.Orch.RerenderEdge(ctx, edge); err != nil {
				cp.log.Warn("program drift auto-repush failed", "edge", edge, "err", err)
			} else {
				d.Repushed = true
				cp.log.Info("program drift auto-repushed edge", "edge", edge, "kind", d.Kind)
			}
		}
		if cp.onProgDrift != nil {
			cp.onProgDrift(d)
		}
		drifts = append(drifts, d)
	}
	return drifts, nil
}

// RunReconcileProgram audits data-plane program fidelity every interval until ctx
// is cancelled. Blocks; run in a goroutine.
func (cp *ControlPlane) RunReconcileProgram(ctx context.Context, interval time.Duration) {
	cp.runLoop(ctx, interval, func(ctx context.Context) {
		if _, err := cp.ReconcileProgram(ctx); err != nil {
			cp.log.Warn("program reconciliation failed", "err", err)
		}
	})
}

func (cp *ControlPlane) bumpProgStreak(edge model.EdgeID) int {
	cp.progStreakMu.Lock()
	defer cp.progStreakMu.Unlock()
	cp.progStreak[edge]++
	return cp.progStreak[edge]
}

func (cp *ControlPlane) resetProgStreak(edge model.EdgeID) {
	cp.progStreakMu.Lock()
	delete(cp.progStreak, edge)
	cp.progStreakMu.Unlock()
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
