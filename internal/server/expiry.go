package server

import (
	"context"
	"time"

	"github.com/fivetime/sbw-contract/model"
)

// actionExpired reports whether an action's TTL has elapsed: it carries an
// absolute expiry (ExpiryUnixMs > 0; 0 = no expiry) and now is at/past it.
// Pool membership is long-lived, so rate-limit pools normally leave this unset
// and are never touched; suppression actions (blackhole/scrub) are REQUIRED to
// carry a TTL at ingest (admin.go), so a forgotten blackhole auto-lifts when its
// TTL elapses instead of black-holing the victim forever (§8/T-706).
func actionExpired(a model.ActionSpec, nowMs int64) bool {
	return a.ExpiryUnixMs > 0 && nowMs >= a.ExpiryUnixMs
}

// ExpireActions sweeps every stored pool once and destroys any whose action TTL
// has elapsed. Destroy is the orchestrator's normal teardown (delete the pool+members
// row in ONE Yugabyte txn + withdraw both homes; the members row WAS the claim and the
// optimistic create reserved nothing, so there is nothing to free in the etcd
// ledger/srcmap), so an expired blackhole's /32 anchor is withdrawn and the victim IP
// recovers automatically.
// Returns the number removed. Safe under multi-replica (DestroyPool is idempotent
// via a Yugabyte Get — an unknown pool returns a no-op, and the delete itself is a
// single Yugabyte txn whose absent-pool case is a no-op; no etcd CAS is involved).
func (cp *ControlPlane) ExpireActions(ctx context.Context) (int, error) {
	// Pools live in the MANDATORY Yugabyte bulk store (the create writes ZERO etcd, so
	// the etcd poolstore is empty in production). Enumerate the action+id pairs from there.
	type act struct {
		id     model.PoolID
		action model.ActionSpec
	}
	var pools []act
	yrs, err := cp.YB.List(ctx)
	if err != nil {
		return 0, err
	}
	for _, yr := range yrs {
		pools = append(pools, act{id: yr.Pool.ID, action: yr.Pool.Action})
	}
	now := time.Now().UnixMilli()
	n := 0
	for _, rec := range pools {
		if !actionExpired(rec.action, now) {
			continue
		}
		if err := cp.Orch.DestroyPool(ctx, rec.id); err != nil {
			cp.log.Warn("action expiry: destroy failed", "pool", rec.id, "err", err)
			continue
		}
		cp.log.Info("action TTL expired; pool auto-removed",
			"pool", rec.id, "action", rec.action.Kind, "expiry_ms", rec.action.ExpiryUnixMs)
		// TIER-2: unsolicited "pool-expired" event — the TTL auto-destroy path produces no
		// api-result today (it is not the admin destroy). Async / Noop-safe.
		cp.emitPoolExpired(rec.id, rec.action.Kind.String())
		n++
	}
	if n > 0 {
		cp.metrics.ActionsExpired(n)
	}
	return n, nil
}

// RunActionExpiry sweeps expired-TTL pools every interval until ctx is cancelled
// (T-706). Mirrors RunReclaim's ticker discipline.
func (cp *ControlPlane) RunActionExpiry(ctx context.Context, interval time.Duration) {
	cp.runLoop(ctx, interval, func(ctx context.Context) {
		if _, err := cp.ExpireActions(ctx); err != nil {
			cp.log.Warn("action expiry sweep failed", "err", err)
		}
	})
}
