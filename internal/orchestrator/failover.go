// Failover orchestration (controller §5.3/§5.8/§5.9, C-05) — the dynamic
// counterpart to CreatePool. It moves a pool's home(s) after a node dies, comes
// back, or is decommissioned, always preserving the two invariants:
//
//   - 双向归位: a pool's ingress home (/32 advertise) and egress home (FlowSpec)
//     are the SAME edge — every operation re-renders an edge's COMPLETE state from
//     Yugabyte (o.yb.PoolsForHome/PoolsForBackup), so the two never split.
//   - 先发新后撤旧 (§4.4 / §6.2): the new home is rendered FULL and pushed BEFORE
//     the old home is withdrawn, so the transition window always has a live home
//     (R's RA_OPTIMAL picks the one best path; no naked window).
//
// These run on the Yugabyte version-CAS failover pivot (the same row that holds the
// pool/member data) and deliver via edgever bumps, so a promote keeps the surviving
// home and re-homes the members atomically with the pivot move — ZERO etcd writes.
package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"github.com/fivetime/sbw-contract/model"

	"github.com/fivetime/sbw-server/internal/poolstore"
)

// ErrNoBackup is returned when a promote is requested for a pool that has no
// standby home to promote (single-home pool, or backup already consumed).
var ErrNoBackup = errors.New("orchestrator: pool has no backup to promote")

// ErrWithdrawIncomplete wraps a promote where the NEW primary is live and serving
// but the OLD primary's withdraw push failed — the normal hard-death case (the
// old node is unreachable). It is non-fatal: callers (failover/decommission)
// treat it as success, since R's RA_OPTIMAL keeps the transition safe and the
// agent self-cleans on revival (§5.8).
var ErrWithdrawIncomplete = errors.New("orchestrator: promoted but old primary withdraw incomplete")

// PromoteBackup runs the failover primitive 提备为主 (§5.3): the pool's BACKUP
// becomes the PRIMARY. The new primary was pre-built (policer/classify) and, for
// ingress, pre-announced (§5.2) — promotion just makes it render FULL (advertise
// + send FlowSpec). Order (§4.4): render the new primary FULL first, THEN
// withdraw the old primary, THEN free the old primary's tokens. After this the
// pool has NO backup (redundancy lost with the dead node) — call ProvisionBackup
// to restore it. Returns the updated record.
//
// Idempotent-friendly: if the pool already lost its backup (e.g. a retried
// promote), it returns ErrNoBackup — the caller treats a missing backup on an
// already-correct primary as success.
func (o *Orchestrator) PromoteBackup(ctx context.Context, id model.PoolID) (poolstore.Record, error) {
	rec, _, _, _, err := o.PromoteBackupGen(ctx, id)
	return rec, err
}

// PromoteBackupGen is PromoteBackup additionally surfacing the NEW PRIMARY edge and the
// per-edge desired-state GENERATION its FULL render carries — the (newPrimary, gen) the
// API-result pending registry resolves a planned migration's convergence against (the
// promoted agent echoes this generation in EdgeReport.Generation once it renders FULL),
// and the (from→to, gen) an unsolicited failover notification reports. The generation
// is captured from the new primary's delivery (bumpGen): in the single-replica direct-
// push path it is the render generation; in the sharded path the owning replica renders
// on the converge loop, so it is 0 (the migrate pending then resolves via the timeout
// sweep, the cross-shard analog DestroyPoolGen already uses for a non-local primary).
// Returns the updated record + new primary + OLD primary (the edge the pool moved OFF,
// for the migrate api-result's from_edge enrichment) + generation. ErrNoBackup (no
// standby to promote) is returned with the unchanged record and empty new/old primaries.
func (o *Orchestrator) PromoteBackupGen(ctx context.Context, id model.PoolID) (poolstore.Record, model.EdgeID, model.EdgeID, uint64, error) {
	// The failover pivot is the Yugabyte version-CAS row. Apply the promote there
	// (UpdateCAS re-homes the members table atomically with the pivot move) and drive
	// delivery via edgever bumps — ZERO etcd pool/member/srcmap/ledger writes. This is
	// the manual-admin (planned migrate) analog of the async reconciler's PROMOTE step;
	// ReconcilePool only promotes a DEAD primary, so a planned migration of a HEALTHY
	// primary rides this synchronous-on-the-pivot path instead.
	pv := o.pivot()
	raw, ok, err := pv.GetForReconcile(ctx, id)
	if err != nil {
		return poolstore.Record{}, "", "", 0, err
	}
	if !ok {
		return poolstore.Record{}, "", "", 0, fmt.Errorf("orchestrator: promote pool %d: not found", id)
	}
	rec := toPivotRow(raw)
	if rec.Backup == "" {
		return o.pivotRecord(rec), "", "", 0, ErrNoBackup
	}
	oldPrimary := rec.Primary
	newPrimary := rec.Backup
	// 先发新后撤旧: promote backup→primary, drop the backup, and mark the old primary
	// Retiring so it keeps rendering FULL until the new primary is delivered (no naked
	// window). version-CAS serializes against a concurrent reconcile (no double move).
	if err := pv.UpdateCAS(ctx, raw, rec.Version, newPrimary, "", oldPrimary); err != nil {
		return poolstore.Record{}, "", "", 0, fmt.Errorf("orchestrator: promote pool %d: %w", id, err)
	}
	gen := o.bumpGen(ctx, newPrimary) // deliver FULL to the new primary (capture its render generation)
	// 后撤旧: deliver empty state to the old primary so it withdraws. A hard-dead old
	// primary cannot receive it — EXPECTED and non-fatal (the retire-old reconcile
	// step / agent self-clean on revival converges it). Free nothing in the etcd
	// ledger (optimistic — there is no reservation on the yb path).
	o.bump(ctx, oldPrimary)
	next := o.pivotRecord(rec)
	next.Primary = newPrimary
	next.Backup = ""
	next.Retiring = oldPrimary
	return next, newPrimary, oldPrimary, gen, nil
}

// pivotRecord adapts a pivotRow back to the poolstore.Record shape the admin/HTTP
// layer consumes (it only reads Pool / Primary / Backup / Tokens off the result).
// The pivot-path failover writes ZERO etcd, so this is a pure in-memory projection
// for the response — not a store read.
func (o *Orchestrator) pivotRecord(r pivotRow) poolstore.Record {
	out := poolstore.Record{
		Pool:          r.Pool,
		Primary:       r.Primary,
		Backup:        r.Backup,
		Tokens:        r.Tokens,
		Retiring:      r.Retiring,
		PrimaryResvID: resvID(r.PoolID, r.Primary),
	}
	if r.Backup != "" {
		out.BackupResvID = resvID(r.PoolID, r.Backup)
	}
	return out
}

// ProvisionBackup restores a pool's standby home (§5.8 复活当新备 / §5.9 现选新备):
// it selects a fresh edge (distinct from the current primary and any `exclude` edges,
// e.g. a known-dead node), renders it STANDBY (machinery pre-built, nothing
// advertised), and records it as the new backup. No-op if the pool already has a
// healthy backup not in `exclude`. It routes to the SAME async pivot primitive the
// reconciler uses (asyncProvisionBackup): version-CAS the new backup_edge onto the
// Yugabyte pivot row + bump edgever for cross-replica delivery, with OPTIMISTIC
// cached-capacity placement (no etcd ledger reserve/commit). ZERO etcd
// pool/member/ledger writes. The returned record projects the new pivot state.
func (o *Orchestrator) ProvisionBackup(ctx context.Context, id model.PoolID, exclude ...model.EdgeID) (poolstore.Record, error) {
	pv := o.pivot()
	raw, ok, err := pv.GetForReconcile(ctx, id)
	if err != nil {
		return poolstore.Record{}, err
	}
	if !ok {
		return poolstore.Record{}, fmt.Errorf("orchestrator: provision backup pool %d: not found", id)
	}
	rec := toPivotRow(raw)
	dead := map[model.EdgeID]bool{rec.Primary: true}
	for _, e := range exclude {
		dead[e] = true
	}
	// Healthy existing backup (not excluded, not dead) → nothing to do.
	if rec.Backup != "" && !dead[rec.Backup] && o.alive(rec.Backup) {
		return o.pivotRecord(rec), nil
	}
	// When an excluded edge IS the current backup, clear it on the working row so the
	// pivot primitive treats it as replaceable (otherwise it would keep the excluded
	// backup). asyncProvisionBackup honors the exclude set for placement.
	for _, e := range exclude {
		if rec.Backup == e {
			rec.Backup = ""
			raw.Backup = ""
		}
	}
	st, err := o.asyncProvisionBackup(ctx, raw, rec, exclude...)
	if err != nil {
		if errors.Is(err, ErrNoPlacement) {
			return o.pivotRecord(rec), fmt.Errorf("%w: no spare home for backup of pool %d", ErrNoPlacement, id)
		}
		return poolstore.Record{}, err
	}
	_ = st
	// Re-read the pivot so the returned record reflects the freshly placed backup.
	if raw2, ok2, err2 := pv.GetForReconcile(ctx, id); err2 == nil && ok2 {
		return o.pivotRecord(toPivotRow(raw2)), nil
	}
	return o.pivotRecord(rec), nil
}

// CleanupRevived handles 原主复活 (§5.8): a previously-replaced edge comes back.
// Its records were overwritten during failover, so the poolstore says it should
// hold whatever it currently is (usually nothing). Pushing that state is the
// cleanup directive — the agent reconciles away any residual /32/policer. This
// is NON-PREEMPTIVE: it never reclaims a primary, it only tells the revived edge
// its current (authoritative) desired state.
func (o *Orchestrator) CleanupRevived(ctx context.Context, edge model.EdgeID) error {
	return o.renderAndPush(ctx, edge)
}

// Decommission gracefully drains an edge (§5.9 退役迁移) and removes it from the
// schedulable pool. For every pool the edge is PRIMARY of, it promotes the hot
// backup (优先提热备); if such a pool has no backup, it first provisions one,
// then promotes. For every pool the edge is only BACKUP of, it provisions a
// replacement backup elsewhere (which drops this edge). Finally the edge is
// withdrawn and deregistered. Best-effort per pool: the first error is returned
// but draining continues so a single stuck pool does not strand the rest.
func (o *Orchestrator) Decommission(ctx context.Context, edge model.EdgeID) error {
	_, err := o.DecommissionGen(ctx, edge)
	return err
}

// DecommissionGen is Decommission additionally surfacing the per-edge desired-state
// GENERATION the DRAINED edge's final withdrawal render carries — the API-result
// correlation handle. A decommission re-homes the edge's pools onto MANY other edges
// (one promoted new primary per pool), so there is no single primary to correlate
// against; the one render that DOES uniquely belong to this operation is the drained
// edge's OWN terminal withdrawal (drainEdge's closing bump), which the (still-live,
// graceful exit) agent echoes back. So ONE request_id is registered against
// (drainedEdge, thatGeneration): the BSS gets ONE "converged" when the decommissioned
// edge confirms it has shed its pools (or one "failed"/timeout). The per-pool re-homes
// onto other edges are driven by the same idempotent pivot machinery and need no
// separate correlation. Returns the drained edge's withdrawal generation (0 in the
// sharded path — the owning replica renders the withdrawal on the converge loop — so
// the pending then resolves via the timeout sweep, like a cross-shard destroy).
func (o *Orchestrator) DecommissionGen(ctx context.Context, edge model.EdgeID) (uint64, error) {
	gen, err := o.drainEdgeGen(ctx, edge, false) // planned exit → no failover notification
	// Remove from the schedulable pool (planned exit — it will not come back).
	if derr := o.reg.Deregister(ctx, edge); derr != nil && err == nil {
		err = derr
	}
	return gen, err
}

// FailoverEdge re-homes every pool off a DEAD edge (§5.3/§6.5, the hard-death
// reaction): promote each primary pool's hot backup (provision-then-promote if
// it had none), replace the edge wherever it was a backup, and withdraw its
// residual state — but leave it REGISTERED, because a hard-dead node may revive
// (§5.8) and self-clean rather than being permanently removed. This is the
// action the liveness monitor fires on PeerDown / heartbeat-grace expiry. It is
// the same drain as Decommission minus the deregister.
func (o *Orchestrator) FailoverEdge(ctx context.Context, edge model.EdgeID) error {
	// notify=true: this drain is the NODE-FAILURE reaction, so each pool's auto-promote
	// fires the unsolicited "failover" notification (the control plane emits it). The
	// Decommission drain passes false (planned/operator-initiated, not a node failure).
	_, err := o.drainEdgeGen(ctx, edge, true)
	return err
}

// drainEdgeGen moves every pool off edge: primaries → promote hot backup (provision
// first if none), backups → provision a replacement elsewhere, then withdraw the
// edge's residual state. Best-effort per pool: the first error is returned but
// draining continues so one stuck pool does not strand the rest. Shared by
// Decommission (planned) and FailoverEdge (hard death). It additionally returns the
// GENERATION the drained edge's
// closing withdrawal render carries (the API-result correlation handle for a planned
// decommission). 0 means no withdrawal generation was minted on this replica (sharded:
// the owning replica renders the withdrawal on the converge loop) → the pending resolves
// via the timeout sweep.
//
// notify gates the AUTONOMOUS-failover notification: when true (FailoverEdge, the
// node-failure reaction) each primary pool's auto-promote fires onFailover with the
// dead old primary, the promoted backup and its render generation; when false
// (Decommission, a planned operator exit) it stays silent — a decommission is
// request-correlated (it returns a request_id), not an unsolicited node-failure event.
func (o *Orchestrator) drainEdgeGen(ctx context.Context, edge model.EdgeID, notify bool) (uint64, error) {
	var firstErr error
	note := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// The pool/member data + failover pivot live in Yugabyte. Drain every pool the edge
	// homes through the SAME pivot-routed primitives (PromoteBackup / ProvisionBackup) —
	// version-CAS on the Yugabyte row + edgever bumps, ZERO etcd pool/member/srcmap/ledger
	// writes. This is the manual-admin (decommission) analog; the liveness hard-death path
	// enqueues ReconcilePool instead (controlplane failover closure). For a pool the edge
	// is PRIMARY of, promote its backup (provision one first if missing/dead); for a pool
	// it is only BACKUP of, provision a replacement elsewhere. Double death (no live home
	// + no spare) surfaces as ProvisionBackup's ErrNoPlacement and is handled by the async
	// reconcile sweep / fail policy (asyncDoubleDeath), not here.
	pv := o.pivot()
	ids, err := pv.PoolsForDeadEdge(ctx, edge)
	if err != nil {
		return 0, err
	}
	for _, id := range ids {
		raw, ok, err := pv.GetForReconcile(ctx, id)
		if err != nil || !ok {
			note(err)
			continue
		}
		rec := toPivotRow(raw)
		if rec.Primary == edge {
			// Need a LIVE backup to promote onto; provision a fresh one first if missing
			// or itself dead. ErrNoPlacement here = double death (handled by the async
			// reconcile sweep / fail policy; not a drain failure).
			if rec.Backup == "" || !o.alive(rec.Backup) {
				if _, perr := o.ProvisionBackup(ctx, id, edge); perr != nil {
					if errors.Is(perr, ErrNoPlacement) {
						continue
					}
					note(fmt.Errorf("drain %s: pool %d provision-before-promote: %w", edge, id, perr))
					continue
				}
			}
			_, newPrimary, _, gen, perr := o.PromoteBackupGen(ctx, id)
			if perr != nil && !errors.Is(perr, ErrNoBackup) && !errors.Is(perr, ErrWithdrawIncomplete) {
				note(fmt.Errorf("drain %s: pool %d promote: %w", edge, id, perr))
				continue
			}
			// AUTONOMOUS FAILOVER (sync path): the dead edge's primary pool just auto-
			// promoted its backup. On the node-failure drain (notify), tell the BSS its
			// home moved edge→newPrimary — it issued no request. ErrNoBackup means nothing
			// was promoted (no move), so skip. Non-blocking; never gates the drain.
			if newPrimary != "" && !errors.Is(perr, ErrNoBackup) {
				if notify {
					o.notifyFailover(id, edge, newPrimary, gen)
				} else {
					// PLANNED DECOMMISSION drain (notify=false): the per-pool move is NOT a
					// node-failure failover. Emit a "rehome" detail event (edge→newPrimary,
					// reason "decommission") for per-edge billing reconciliation — the bulk
					// "decommission" api-result still carries the request_id correlation.
					o.notifyRehome(id, edge, newPrimary)
				}
			}
			// Restore N+1 elsewhere (excluding the drained edge). ErrNoPlacement = run
			// primary-only until capacity frees.
			if _, perr := o.ProvisionBackup(ctx, id, edge); perr != nil && !errors.Is(perr, ErrNoPlacement) {
				note(fmt.Errorf("drain %s: pool %d reprovision-backup: %w", edge, id, perr))
			}
			continue
		}
		// Edge is only BACKUP of this pool: replace it elsewhere.
		if _, perr := o.ProvisionBackup(ctx, id, edge); perr != nil && !errors.Is(perr, ErrNoPlacement) {
			note(fmt.Errorf("drain %s: pool %d replace-backup: %w", edge, id, perr))
		}
	}
	// Withdraw anything residual on the drained edge via an edgever bump (its owning
	// replica re-renders empty). Best-effort. Capture the withdrawal generation — the
	// drained edge's terminal render — as the decommission's API-result correlation
	// handle (the graceful, still-live agent echoes it back on convergence).
	gen := o.bumpGen(ctx, edge)
	return gen, firstErr
}
