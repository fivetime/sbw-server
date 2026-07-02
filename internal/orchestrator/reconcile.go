// Async failover reconciler (DESIGN-liveness L-07) — the Nova-evacuate model for
// per-pool egress failover under K=2 sharding. The synchronous drainEdge
// (failover.go) pushes desired state replica-locally and waits on an in-memory
// report cache, so it cannot drive a backup whose Subscribe stream is on a PEER
// replica. ReconcilePool replaces that for the sharded case: it is LEVEL-TRIGGERED
// and mutates only the FAILOVER PIVOT (the Yugabyte version-CAS row) +
// edgever. It never pushes to an agent — whichever replica holds an edge's stream
// realizes the change via the converge loop (controller.RunConverge), and readiness
// is observed through the etcd applied-version (edgever), not replica-local memory.
//
// HYBRID STEP 2 (2026-06): the pivot moved off etcd. Every pivot mutation goes
// through pivotStore.UpdateCAS / DeleteCAS gated on the Yugabyte VERSION column read
// at the top (replacing the etcd ModRevision CAS), so two coverers reconciling the
// same pool concurrently can never lost-update the pivot — the loser gets ErrConflict
// and re-reads next pass. The members re-home runs in the SAME Yugabyte txn as the
// pivot move (UpdateCAS), keeping the data-plane src→home truth atomic with the
// promote. The migration is a multi-pass sequence:
//
//	provision backup → (gate: backup applied>=desired) → promote (defer old
//	withdraw) → (gate: new primary applied>=desired) → retire old → reprovision
//
// reproducing the synchronous "先就位再切 / 先发新后撤旧" happens-before ACROSS
// replicas via the etcd-observable applied-version.
package orchestrator

import (
	"context"
	"errors"
	"sync"

	"github.com/fivetime/sbw-contract/model"

	"github.com/fivetime/sbw-server/internal/scheduler"
	"github.com/fivetime/sbw-server/internal/ybstore"
)

// ReconcileStatus tells RunPoolReconcile how to schedule a pool's next pass.
type ReconcileStatus int

const (
	// StatusHealthy: at the redundancy target (N+1, or N+0 by design); idle.
	StatusHealthy ReconcileStatus = iota
	// StatusActed: took a state-changing step; re-enqueue promptly to continue the
	// migration sequence.
	StatusActed
	// StatusGated: waiting on an edge's applied-version to catch up (先就位再切);
	// re-check on the slow sweep / an applied-version advance.
	StatusGated
	// StatusDegraded: stable but degraded (no spare capacity for a backup, or a
	// fail-close pool kept after double death); settle — only the slow sweep retries.
	StatusDegraded
	// StatusConflict: a peer reconciler advanced the pivot (version-CAS lost); re-enqueue
	// promptly to re-read and continue from the new state.
	StatusConflict
)

// ReconcilePool drives one level-triggered failover step for pool id. See the
// package doc for the multi-pass migration sequence. Safe to call concurrently on
// multiple replicas (version-CAS-serialized on the pivot row). The pivot is the
// MANDATORY Yugabyte store (always wired); the nil guard is pure defensiveness for a
// half-built orchestrator in a test, never a production fallback.
func (o *Orchestrator) ReconcilePool(ctx context.Context, id model.PoolID) (ReconcileStatus, error) {
	pv := o.pivot()
	if pv == nil {
		return StatusHealthy, nil // defensive: no pivot store (half-built test orchestrator)
	}
	raw, ok, err := pv.GetForReconcile(ctx, id)
	if err != nil {
		return StatusHealthy, err
	}
	if !ok {
		return StatusHealthy, nil // pool gone (destroyed / torn down)
	}
	rec := toPivotRow(raw)

	// STEP retire-old (highest priority — finish an in-flight migration first).
	// A promote left an old primary to withdraw once the new primary is confirmed
	// live. Until then the old primary still renders FULL (it remains announced),
	// so there is no naked window even though it is no longer the pivot's primary.
	if rec.Retiring != "" {
		if !o.edgeReady(ctx, rec.Primary) || !o.alive(rec.Primary) {
			return StatusGated, nil // new primary not applied OR died before retire → keep old announced (no naked window)
		}
		old := rec.Retiring
		// Clear the retiring marker under version-CAS (primary/backup unchanged).
		if err := pv.UpdateCAS(ctx, raw, rec.Version, rec.Primary, rec.Backup, ""); err != nil {
			if errors.Is(err, ybstore.ErrConflict) {
				return StatusConflict, nil
			}
			return StatusActed, err
		}
		o.bump(ctx, old) // 后撤旧: deliver empty state → withdraw on old's replica. The
		// optimistic create reserved no etcd ledger quota, so there is nothing to return.
		return StatusActed, nil
	}

	primaryDead := !o.alive(rec.Primary)

	if primaryDead {
		// Need a LIVE, READY backup to promote onto.
		if rec.Backup == "" || !o.alive(rec.Backup) {
			return o.asyncProvisionBackup(ctx, raw, rec) // provision-before-promote
		}
		if !o.edgeReady(ctx, rec.Backup) {
			return StatusGated, nil // 先就位再切: backup not done building its data plane
		}
		// PROMOTE: backup → primary. Mark the old primary Retiring (withdraw deferred
		// to the retire-old step, gated on the new primary going live). The surviving
		// home (the old backup) needs no ledger handoff — the create path is optimistic
		// and reserves no etcd ledger quota, so promoting it to primary is a pure pivot
		// move. UpdateCAS re-homes members to the new primary in the same txn.
		oldPrimary := rec.Primary
		newPrimary := rec.Backup
		// Re-check liveness immediately before the CAS: the alive() check above and this
		// promote are not atomic, so a backup that died in between must NOT be promoted to a
		// dead primary (black-hole). Now double-dead → provision-before-promote instead.
		if !o.alive(newPrimary) {
			return o.asyncProvisionBackup(ctx, raw, rec)
		}
		if err := pv.UpdateCAS(ctx, raw, rec.Version, newPrimary, "", oldPrimary); err != nil {
			if errors.Is(err, ybstore.ErrConflict) {
				return StatusConflict, nil
			}
			return StatusActed, err
		}
		// Egress src→home pivot: UpdateCAS ALREADY re-homed the members table (the
		// authoritative src→home truth) atomically with the pivot move, so there is no
		// separate claim to write.
		gen := o.bumpGen(ctx, newPrimary) // 先发新: deliver FULL to the new primary
		// AUTONOMOUS FAILOVER: this is the node-failure auto-promote decision point (the
		// primary was judged DEAD, primaryDead above). The BSS issued no request yet the
		// pool's home moved oldPrimary→newPrimary, so notify it (the control plane emits
		// the unsolicited "failover" event). Non-blocking; never gates the promote.
		o.notifyFailover(rec.PoolID, oldPrimary, newPrimary, gen)
		return StatusActed, nil
	}

	// Primary alive — restore N+1 redundancy if the pool wants a backup.
	if o.replicas >= 2 && (rec.Backup == "" || !o.alive(rec.Backup)) {
		st, perr := o.asyncProvisionBackup(ctx, raw, rec)
		if errors.Is(perr, ErrNoPlacement) {
			return StatusDegraded, nil // no spare → run primary-only (stable), slow sweep retries
		}
		return st, perr
	}
	return StatusHealthy, nil
}

// FailoverEdgeBulk is the edge-DEATH fast path (sharded). Instead of enqueuing each of
// the dead edge's pools for an independent level-triggered ReconcilePool — per-pool
// UpdateCAS + a per-pool edgever bump on the SAME new-primary key, which collides into a
// CAS-conflict storm (~2s/pool, MINUTES for a busy edge → a prolonged ingress/egress
// blackhole) — it re-homes them ALL in parallel (reusing the per-pool UpdateCAS so the
// body JSONB stays consistent) and COALESCES the edgever bump to ONE per new-primary
// edge: the converge loop re-derives that edge's full (now-grown) pool set in a single
// render+deliver, so the warm-standby backup announces all the re-homed anchors+FlowSpec
// at once. Pools with a dead/not-ready backup fall back to the per-pool path (provision-
// before-promote); re-homed pools are enqueued to restore N+1 redundancy OFF the critical
// path. Idempotent under K=2: a peer coverer that lost the version-CAS just enqueues a
// no-op reconcile (the pool's primary is already alive).
func (o *Orchestrator) FailoverEdgeBulk(ctx context.Context, deadEdge model.EdgeID, enqueue func(model.PoolID)) {
	rows, err := o.pivot().ListPivotsByPrimary(ctx, deadEdge)
	if err != nil {
		o.log.Warn("bulk failover: list pivots failed; the per-pool sweep still covers it", "edge", deadEdge, "err", err)
		return
	}
	type moved struct {
		id model.PoolID
		to model.EdgeID
	}
	var (
		mu      sync.Mutex
		movedTo []moved
		wg      sync.WaitGroup
		sem     = make(chan struct{}, 16)
	)
	readyCache := map[model.EdgeID]bool{}
	for _, row := range rows {
		if row.Primary != deadEdge {
			continue
		}
		b := row.Backup
		// A LIVE backup is enough — do NOT gate on edgeReady here. The primary is DEAD, so
		// the pool is ALREADY naked (the dead primary stopped announcing on death); the
		// backup is a WARM standby whose policer/classify the standby render pre-applied, so
		// it can forward the instant it advertises. Gating on edgeReady is exactly what
		// serializes the per-pool path into the gradual evacuation: each promote bumps the
		// backup's desired, flipping edgeReady false, so the next pool waits a whole
		// apply+report cycle. Promoting all live-backup pools at once + ONE coalesced bump
		// (below) lets the backup advertise the WHOLE re-homed set in a single converge —
		// ending the blackhole in one cycle instead of trickling at ~one pool per cycle.
		ready, seen := readyCache[b]
		if !seen {
			ready = b != "" && o.alive(b)
			readyCache[b] = ready
		}
		if !ready {
			enqueue(row.PoolID) // no LIVE backup (double death) → per-pool provision-before-promote
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(r ybstore.PivotRow) {
			defer wg.Done()
			defer func() { <-sem }()
			// Reuse per-pool UpdateCAS: re-home members + pivot atomically, rebuild the
			// body JSONB consistently. retiring="" — a DEAD primary needs no graceful
			// withdraw overlap (it already stopped announcing on death).
			if !o.alive(r.Backup) { // re-check before promote: a backup dead since readyCache must not become a dead primary
				enqueue(r.PoolID)
				return
			}
			if err := o.pivot().UpdateCAS(ctx, r, r.Version, r.Backup, "", ""); err != nil {
				enqueue(r.PoolID) // version-CAS lost (peer coverer won) / race → per-pool no-op
				return
			}
			mu.Lock()
			movedTo = append(movedTo, moved{r.PoolID, r.Backup})
			mu.Unlock()
		}(row)
	}
	wg.Wait()

	// ONE bump per new-primary edge → its converge delivers ALL the re-homed pools in a
	// single render (kills the per-pool same-key CAS contention).
	genByEdge := map[model.EdgeID]uint64{}
	for _, m := range movedTo {
		if _, ok := genByEdge[m.to]; !ok {
			genByEdge[m.to] = o.bumpGen(ctx, m.to)
		}
	}
	// Notify (coalesced gen) + restore N+1 redundancy (re-homed pools now have backup="")
	// off the critical path — the data plane already serves from the new primary.
	for _, m := range movedTo {
		o.notifyFailover(m.id, deadEdge, m.to, genByEdge[m.to])
		enqueue(m.id)
	}
}

// asyncProvisionBackup selects a fresh live backup for rec (excluding the current
// primary and any dead/absent backup it replaces) using OPTIMISTIC cached capacity
// (no etcd ledger reserve/commit — hybrid step 3, mirroring CreatePoolNonce), then
// persists the new backup via UpdateCAS (version-CAS) and bumps its edgever so the
// converge loop builds it. It does NOT wait for readiness — that is a later
// level-triggered pass (StatusGated at the promote gate). On a version-CAS conflict
// it simply re-reads next pass (no reservation to unwind). Returns ErrNoPlacement
// when no spare exists; if the primary is ALSO dead that is double death.
//
// HYBRID STEP 3 (2026-06): the backup-provision path that the reconcile pass drives
// after a create no longer writes the etcd ledger (sbw/resv + sbw/tok). With ybstore
// wired, the backup placement reads remaining = sellable(edge) − cap.Used(edge) from
// the in-memory CapacityCache (refreshed every few seconds) — the SAME optimistic
// source the create path uses — and the pivot move (UpdateCAS) records backup_edge in
// the Yugabyte row (used by the next UsedByEdge refresh). So a create whose reconcile
// pass provisions/replaces a backup writes ZERO etcd: the only authoritative write is
// the Yugabyte UpdateCAS, and edgever bumps drive cross-replica delivery.
//
// TODO(no-oversell): production strict no-oversell on the backup-provision path needs
// the SAME future authoritative Yugabyte per-edge used-tokens counter the create path
// notes (a CHECK in the pivot txn), not the etcd ledger. Not built now (the lab
// over-provisions per-edge capacity, so optimistic placement never oversells).
func (o *Orchestrator) asyncProvisionBackup(ctx context.Context, raw ybstore.PivotRow, rec pivotRow, exclude ...model.EdgeID) (ReconcileStatus, error) {
	dead := map[model.EdgeID]bool{rec.Primary: true}
	if rec.Backup != "" {
		dead[rec.Backup] = true // replacing a dead/absent backup
	}
	// Operator-supplied exclusions (manual ProvisionBackup / drainEdge pass the
	// drained edge): never re-place the backup onto one of them. The reconciler calls
	// with no exclude (death is observed via the liveness oracle, not an explicit set).
	for _, e := range exclude {
		dead[e] = true
	}
	candidates, capBps, err := o.liveCandidatesCap(ctx, dead)
	if err != nil {
		return StatusActed, err
	}
	remSess, needSess := o.sessionConstraint(len(rec.Pool.Members))
	homes, err := scheduler.SelectHomes(ctx, candidates, o.remaining(capBps), rec.Tokens, remSess, needSess, 1)
	if err != nil {
		// No spare on EITHER dimension (bandwidth ErrInsufficientCapacity OR §9.1
		// materialization ErrInsufficientSessions) means this pool gets no backup.
		if errors.Is(err, scheduler.ErrInsufficientCapacity) || errors.Is(err, scheduler.ErrInsufficientSessions) {
			if !o.alive(rec.Primary) {
				return o.asyncDoubleDeath(ctx, raw, rec) // primary dead + no spare = double death
			}
			// TIER-3: this pool can't get a backup (runs primary-only) AND the scheduler
			// reported fleet-wide no-spare. Emit BOTH the per-pool redundancy-lost and the
			// fleet-level capacity-exhausted (the control plane maps each to its event); the
			// reason distinguishes bandwidth from materialization exhaustion (§9.1).
			reason := "no-capacity"
			if errors.Is(err, scheduler.ErrInsufficientSessions) {
				reason = "no-materialization-budget"
			}
			o.notifyRedundancy("redundancy-lost", rec.PoolID, "", "no-spare-capacity")
			o.notifyRedundancy("capacity-exhausted", rec.PoolID, "", reason)
			return StatusDegraded, ErrNoPlacement
		}
		return StatusActed, err
	}
	newBackup := homes[0]
	oldBackup := rec.Backup
	// Persist the new backup under version-CAS (primary + retiring unchanged). NO
	// ledger reserve/commit — optimistic placement (see the doc comment). The pivot's
	// backup_edge column is the authoritative record of the placement.
	if err := o.pivot().UpdateCAS(ctx, raw, rec.Version, rec.Primary, newBackup, rec.Retiring); err != nil {
		if errors.Is(err, ybstore.ErrConflict) {
			return StatusConflict, nil
		}
		return StatusActed, err
	}
	if oldBackup != "" && oldBackup != newBackup {
		o.bump(ctx, oldBackup) // withdraw its residual standby (best-effort)
	}
	o.bump(ctx, newBackup) // deliver STANDBY to the new backup
	// TIER-3: a fresh backup was placed → the pool's standby moved (backup-changed) and
	// its redundancy is restored (redundancy-regained, the recovery twin of a prior
	// redundancy-lost). Observability only; non-blocking; after the authoritative write.
	o.notifyRedundancy("backup-changed", rec.PoolID, newBackup, "")
	return StatusActed, nil
}

// asyncDoubleDeath applies the per-pool policy when a pool has lost all live homes
// (§4.7), version-CAS-gated so a concurrent successful re-home wins (hole 6):
// fail-OPEN tears the pool down (DeleteCAS + withdraw), fail-CLOSE keeps it
// (suppression preserved; revives via the normal §5.8 path). Either way the alarm
// fires. Returns StatusDegraded (stable terminal) or StatusConflict if a peer
// advanced the pivot first. DeleteCAS already removed the pool+members rows (the
// members table WAS the claim) and the optimistic reconcile reserved NOTHING in the
// etcd ledger — so there is nothing to release or return. ZERO etcd pool/member writes.
func (o *Orchestrator) asyncDoubleDeath(ctx context.Context, raw ybstore.PivotRow, rec pivotRow) (ReconcileStatus, error) {
	failOpen := rec.Pool.FailOpenOnDoubleDeath()
	if failOpen {
		if err := o.pivot().DeleteCAS(ctx, rec.PoolID, rec.Version); err != nil {
			if errors.Is(err, ybstore.ErrConflict) {
				return StatusConflict, nil // a peer re-homed it; that placement wins
			}
			return StatusDegraded, err
		}
		for _, edge := range rec.homes() {
			o.bump(ctx, edge) // withdraw (best-effort on dead homes)
		}
	}
	if o.onDblDeath != nil {
		o.onDblDeath(rec.PoolID, failOpen)
	}
	return StatusDegraded, nil
}

// edgeReady reports whether edge has APPLIED its current desired state (L-07
// readiness gate): the etcd applied-version >= the desired-version, with a
// non-zero desired (the edge has actually been versioned). Conservative — it waits
// for the edge to be FULLY caught up, which always implies it built the specific
// pool's machinery (the desired render that set the pool's role bumped the
// version, and applied>=desired means the agent echoed that version or newer). No
// edgever wired (single-replica) → always ready (the synchronous path gates via
// readyWait instead).
func (o *Orchestrator) edgeReady(ctx context.Context, edge model.EdgeID) bool {
	if o.edgever == nil {
		return true
	}
	des, err := o.edgever.Desired(ctx, edge)
	if err != nil {
		return false
	}
	if des == 0 {
		// Never async-versioned → a pre-existing/sync-managed home (e.g. a CreatePool
		// hot standby already delivered + applied). Assume ready, matching the V1
		// hot-backup semantics (WithReadyWait is also nil-skips for that case). Only
		// an async-PROVISIONED home (which always bumps to des>=1) is truly gated.
		return true
	}
	app, err := o.edgever.Applied(ctx, edge)
	if err != nil {
		return false
	}
	return app >= des
}

// bump delivers edge's current desired state. With an edgever store wired (sharding,
// L-07) it advances the edge's desired version so the converge loop on whichever
// replica holds its stream re-renders + delivers — the failover primitives never push
// directly, so a backup subscribed on a peer replica is still driven. WITHOUT an
// edgever store (single-replica deploy) there is no converge loop, so it falls back to
// a direct local render+push — the same delivery the synchronous drain used to do —
// otherwise the failover/promote would mutate the Yugabyte pivot but never tell the
// agent. Best-effort: a failed bump/push is retried by the next reconcile pass (or the
// drift backstop).
func (o *Orchestrator) bump(ctx context.Context, edge model.EdgeID) {
	if edge == "" {
		return
	}
	if o.edgever == nil {
		// single-replica: deliver directly (no converge loop). A failed push is retried
		// by the next reconcile pass / drift backstop, but log it — a silent drop hides
		// a stuck delivery.
		if err := o.renderAndPush(ctx, edge); err != nil {
			o.log.Warn("orchestrator: best-effort deliver failed (will retry)", "edge", edge, "err", err)
		}
		return
	}
	if _, err := o.edgever.Bump(ctx, edge); err != nil {
		// The level-triggered loop re-derives and re-bumps next pass — but log it so the
		// retry is visible (a silently-failing bump stalls cross-replica delivery).
		o.log.Warn("orchestrator: edgever bump failed (will retry)", "edge", edge, "err", err)
	}
}

// bumpGen is bump that additionally surfaces the per-edge render GENERATION it
// delivered — the value the agent echoes back in EdgeReport.Generation, used by the
// API-result pending registry (planned migrate) and the failover notification. Only
// the single-replica direct-push path can surface a concrete generation (it renders
// synchronously here, capturing renderAndPushGen's value). The edgever (sharded) path
// bumps a desired-version and the OWNING replica renders later on the converge loop,
// so this replica never mints that generation — it returns 0, which the migrate
// pending then resolves via the timeout sweep (the cross-shard analog DestroyPoolGen
// already uses for a non-local primary), and which the failover event simply reports
// as 0 (a notification, not a handshake). Best-effort: a failed push logs and yields 0.
func (o *Orchestrator) bumpGen(ctx context.Context, edge model.EdgeID) uint64 {
	if edge == "" {
		return 0
	}
	if o.edgever == nil {
		gen, _, err := o.renderAndPushGen(ctx, edge)
		if err != nil {
			o.log.Warn("orchestrator: best-effort deliver failed (will retry)", "edge", edge, "err", err)
			return 0
		}
		return gen
	}
	if _, err := o.edgever.Bump(ctx, edge); err != nil {
		o.log.Warn("orchestrator: edgever bump failed (will retry)", "edge", edge, "err", err)
	}
	return 0 // sharded: the owning replica mints the generation on its converge render
}
