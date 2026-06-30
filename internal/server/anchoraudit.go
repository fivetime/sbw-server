package server

import (
	"context"
	"net/netip"
	"time"

	"github.com/fivetime/sbw-contract/model"
)

// Anchor intent↔physical reconciliation (DESIGN-liveness §11/§2, L-04). The
// physical side of this audit is the RIB-survival guard's tap view — a COVERER
// concern (the guard is tap-fed and lives on the coverer, not the server). With no
// guard on the server, ReconcileAnchors is INERT: it short-circuits and emits
// nothing. It becomes live again only once the coverer feeds a physical member view
// over the seam (CovererReport.MEMBER_EDGE is the stub today). Flagged risk: until
// then the server cannot reconcile anchor intent against L-physical /32 advertisement.

// AnchorMismatch is one edge's anchor intent↔physical disagreement. Retained so the
// emitAnchorMismatch composition (controlplane.go) still type-checks; it is never
// populated on the server until MEMBER_EDGE feeds a physical view.
type AnchorMismatch struct {
	Edge model.EdgeID
	// Unprovisioned: members Yugabyte homes here that L has not advertised (view
	// trustworthy) — anchor suppressed, blackhole averted.
	Unprovisioned []netip.Prefix
	// Rogue: host /32 L advertises that no pool homes here.
	Rogue []netip.Prefix
}

// ReconcileAnchors is INERT on the server: with no tap-fed guard there is no
// physical view to reconcile intent against, so it returns no mismatches. Kept (with
// RunReconcileAnchors) so the reconciler wiring is identical to the monolith and the
// audit lights up automatically once a physical member view arrives over the seam.
func (cp *ControlPlane) ReconcileAnchors(_ context.Context) ([]AnchorMismatch, error) {
	return nil, nil // no tap/guard on the server → no physical view to reconcile against
}

// RunReconcileAnchors runs the (inert) anchor intent↔physical audit on a timer
// (L-04). It is a no-op on the server until the coverer feeds member presence.
func (cp *ControlPlane) RunReconcileAnchors(ctx context.Context, interval time.Duration) {
	cp.runLoop(ctx, interval, func(ctx context.Context) {
		if _, err := cp.ReconcileAnchors(ctx); err != nil {
			cp.log.Warn("anchor reconciliation failed", "err", err)
		}
	})
}
