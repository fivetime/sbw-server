package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-server/internal/orchestrator"
	"github.com/fivetime/sbw-server/internal/poolstore"
	"github.com/fivetime/sbw-server/internal/srcmap"
	"github.com/fivetime/sbw-server/internal/ybstore"
)

// The admin API is the controller's management/ingestion surface (T-701): BSS
// product data (pool id / rates / members / action / bind flag) enters here and
// is turned into a placed, distributed pool via the orchestrator; ops drives
// destroy and agent decommission. It is the only externally-driven write path,
// so per §1.2 it relies on the LB serializing a given request to one replica +
// the orchestrator's Yugabyte uniqueness (pools.id / members.prefix PK in one ACID
// txn) underneath — no lock here.
//
//	POST   /v1/pools                       create (body = pool product data)
//	GET    /v1/pools                       list records
//	GET    /v1/pools/{id}                  get one record
//	DELETE /v1/pools/{id}                  destroy
//	POST   /v1/pools/{id}/migrate          planned migration: promote backup + reprovision (§4.4/§5.9)
//	GET    /v1/agents                      list registered agents
//	POST   /v1/agents/{id}/decommission    drain + remove an agent (§5.9)

// defaultCreateWindow is the ±skew the create anti-replay timestamp must fall
// within (and the nonce key's TTL). A request older/newer than this is rejected
// before any work; the nonce key then only has to cover the live window.
const defaultCreateWindow = 5 * time.Minute

// PoolRequest is the BSS product-data ingestion body: the pool definition plus
// an optional explicit token cost. When Tokens is 0 the cost is derived from the
// egress rate (the scarce DC-uplink resource).
type PoolRequest struct {
	model.Pool
	// Tokens overrides the derived per-home quota cost (bits/s). 0 → derive.
	Tokens int64 `json:"tokens,omitempty"`
	// Replace controls CIDR-overlap policy (§6.4). Default false: a member that
	// overlaps a member already in ANOTHER pool (CIDR containment either way, not
	// just exact) is REJECTED with 409 + the conflict list. true: the overlapping
	// members are EVICTED from the pools that hold them and the new pool takes
	// over; the response reports what was displaced. Evicting a member never
	// destroys its (now possibly empty) pool — that is a separate BSS decision.
	Replace bool `json:"replace,omitempty"`

	// Timestamp (unix ms) + Nonce form the create request's ANTI-REPLAY envelope.
	// The handler rejects a Timestamp outside ±createWindow (default 5min) of now,
	// and the persist Txn rejects a replayed Nonce (a key with a TTL lease = the
	// window). Together they make a captured-and-resent create harmless: a stale
	// timestamp is refused outright, a replay inside the window hits the nonce key,
	// and the idempotent create-if-not-exists CAS makes anything that slips through
	// a no-op. Both empty (zero) → anti-replay is not enforced (internal/test calls).
	Timestamp int64  `json:"timestamp,omitempty"`
	Nonce     string `json:"nonce,omitempty"`
}

// memberRef identifies a member by its pool and CIDR (a member has no id of its
// own — its prefix IS its identity). Used in overlap conflict/replacement bodies.
type memberRef struct {
	PoolID model.PoolID `json:"pool_id"`
	CIDR   string       `json:"cidr"`
	Home   model.EdgeID `json:"home,omitempty"`
}

func refsOf(recs []srcmap.Record) []memberRef {
	out := make([]memberRef, 0, len(recs))
	for _, r := range recs {
		out = append(out, memberRef{PoolID: r.PoolID, CIDR: r.Src.String(), Home: r.Home})
	}
	return out
}

// conflictResponse is the 409 body when replace=false and members overlap.
type conflictResponse struct {
	Error     string      `json:"error"`
	Conflicts []memberRef `json:"conflicts"`
}

// poolResponse is the success body; Replaced lists members displaced from other
// pools when replace=true (omitted when nothing was displaced).
//
// RequestID + Generation are the ASYNC API-result correlation handles (the control
// plane accepts SYNCHRONOUSLY here, but the agent realizes the data plane — VPP
// policer + /32 anchor + FlowSpec — off the request path). The BSS keeps the
// RequestID and matches it to the "converged"/"failed" event the controller later
// emits to the Redpanda API-results topic once the primary home edge's report echoes
// an applied generation >= Generation (or the timeout sweep fires). For CREATE the
// RequestID reuses the request's anti-replay Nonce (already unique per request);
// UPDATE has no nonce, so a random RequestID is minted.
type poolResponse struct {
	poolstore.Record
	Replaced   []memberRef `json:"replaced,omitempty"`
	RequestID  string      `json:"request_id,omitempty"`
	Generation uint64      `json:"generation,omitempty"`
}

// destroyResponse is the 202 Accepted body for DELETE: the control plane has removed
// the pool synchronously, but the agent withdraws the data plane off the request
// path, so the BSS correlates RequestID to the later "converged"/"failed" event.
type destroyResponse struct {
	RequestID  string       `json:"request_id"`
	PoolID     model.PoolID `json:"pool_id"`
	Edge       model.EdgeID `json:"edge,omitempty"`
	Generation uint64       `json:"generation,omitempty"`
}

// newRequestID mints a random opaque request id for ops with no anti-replay nonce
// (update/destroy). 16 bytes of crypto-random hex — collision-free for correlation.
func newRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// resolveOverlaps applies the CIDR-overlap policy: it returns the cross-pool
// conflicts for the request's members. If there are conflicts and replace is
// false it writes the 409 conflict body and returns handled=true so the caller
// stops. With replace=true (or no conflicts) it returns the conflicts for the
// caller to evict AFTER the create/update commits.
func (cp *ControlPlane) resolveOverlaps(ctx context.Context, w http.ResponseWriter, p model.Pool, replace bool) (conflicts []srcmap.Record, handled bool) {
	conflicts, err := cp.Orch.MemberConflicts(ctx, p.Members, p.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "overlap check failed: %v", err)
		return nil, true
	}
	if len(conflicts) > 0 && !replace {
		writeJSON(w, http.StatusConflict, conflictResponse{
			Error:     "CIDR overlap with existing pool(s); set replace=true to displace",
			Conflicts: refsOf(conflicts),
		})
		return nil, true
	}
	return conflicts, false
}

// evict removes each conflicting member from the pool that holds it (the
// replace=true path), after the new pool has committed. byPool is the DISPLACING
// pool (the create/update caller). Returns the displaced set for the response.
//
// TIER-2: only the displacing caller gets Replaced[] in its sync body; the displaced
// pool's owner gets NOTHING today, so per displaced member we emit an unsolicited
// "member-evicted" event (PoolID = the displaced pool A, DisplacedByPool = byPool).
// Async / Noop-safe.
func (cp *ControlPlane) evict(ctx context.Context, conflicts []srcmap.Record, byPool model.PoolID) ([]memberRef, error) {
	for _, c := range conflicts {
		if _, err := cp.Orch.RemoveMember(ctx, c.PoolID, c.Src); err != nil {
			return nil, fmt.Errorf("evict %s from pool %d: %w", c.Src, c.PoolID, err)
		}
		cp.emitMemberEvicted(c.PoolID, c.Src, byPool)
	}
	return refsOf(conflicts), nil
}

// AdminHandler returns the management HTTP mux. Serve it from the cmd on a
// separate, access-controlled listener.
func (cp *ControlPlane) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/pools", cp.handleCreatePool)
	mux.HandleFunc("PUT /v1/pools/{id}", cp.handleUpdatePool)
	mux.HandleFunc("GET /v1/pools", cp.handleListPools)
	mux.HandleFunc("GET /v1/pools/{id}", cp.handleGetPool)
	mux.HandleFunc("DELETE /v1/pools/{id}", cp.handleDestroyPool)
	mux.HandleFunc("POST /v1/pools/{id}/migrate", cp.handleMigratePool)
	mux.HandleFunc("GET /v1/agents", cp.handleListAgents)
	mux.HandleFunc("POST /v1/agents/{id}/decommission", cp.handleDecommission)
	return mux
}

func (cp *ControlPlane) handleCreatePool(w http.ResponseWriter, r *http.Request) {
	var req PoolRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: %v", err)
		return
	}
	if err := ingestValidate(req.Pool); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid pool: %v", err)
		return
	}
	// ANTI-REPLAY window: reject a stale/future timestamp before doing any work, so
	// a captured request resent long after capture is refused outright (the nonce
	// key only needs to cover the live ±window). A zero timestamp + empty nonce
	// means the caller opted out (internal callers); a non-zero timestamp is always
	// window-checked. The nonce itself is verified atomically in the persist Txn.
	if req.Timestamp != 0 {
		skew := cp.createWindow
		if skew <= 0 {
			skew = defaultCreateWindow
		}
		drift := time.Duration(cp.now().UnixMilli()-req.Timestamp) * time.Millisecond
		if drift < -skew || drift > skew {
			writeErr(w, http.StatusBadRequest, "stale or future request timestamp (drift %s exceeds ±%s)", drift, skew)
			return
		}
	}
	normalizeUnlimited(&req)
	tokens := req.Tokens
	if tokens == 0 {
		var err error
		if tokens, err = tokensForPool(req.Pool); err != nil {
			writeErr(w, http.StatusBadRequest, "cannot derive token cost: %v", err)
			return
		}
	}
	// CIDR-overlap policy (§6.4): reject (409 + conflict list) unless replace=true,
	// in which case the overlapping members are evicted AFTER this pool commits.
	conflicts, handled := cp.resolveOverlaps(r.Context(), w, req.Pool, req.Replace)
	if handled {
		return
	}
	rec, primaryEdge, generation, err := cp.Orch.CreatePoolNonceGen(r.Context(), req.Pool, tokens, req.Nonce)
	if err != nil {
		switch {
		case errors.Is(err, orchestrator.ErrNoPlacement):
			writeErr(w, http.StatusServiceUnavailable, "no placement: %v", err)
		case errors.Is(err, poolstore.ErrReplay):
			// LEGACY: no production path returns ErrReplay post-Yugabyte migration (the
			// anti-replay etcd nonce is gone). A replayed create id now surfaces as
			// poolstore.ErrExists and falls through to the default branch below.
			writeErr(w, http.StatusConflict, "replayed create request: %v", err)
		default:
			// Duplicate id (ErrExists) / member CIDR conflict / etc. — a client/conflict error.
			writeErr(w, http.StatusConflict, "create failed: %v", err)
		}
		return
	}
	replaced, err := cp.evict(r.Context(), conflicts, req.Pool.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "pool created but eviction failed: %v", err)
		return
	}
	// ASYNC API-result correlation: reuse the anti-replay Nonce as the request_id (it
	// is already unique per request); mint a random one when the caller opted out of
	// the nonce (internal/test). Register the pending so the primary edge's report
	// (echoing this generation) resolves it to a "converged" event — or the timeout
	// sweep fires "failed". Unchanged when no brokers are configured (Noop emitter).
	requestID := req.Nonce
	if requestID == "" {
		requestID = newRequestID()
	}
	// TIER-4: carry the GRANTED rate basis (cir_kbps / ingress / tokens / billing_mode /
	// unlimited) onto the eventual "converged" event so an event-only BSS can rate the pool.
	cp.registerPendingEnriched(requestID, "create", req.Pool.ID, primaryEdge, "", generation, rateBasisForPool(req.Pool, tokens))
	writeJSON(w, http.StatusCreated, poolResponse{Record: rec, Replaced: replaced, RequestID: requestID, Generation: generation})
}

// handleUpdatePool changes an existing pool's members/rates in place (BSS update:
// add/remove members, modify bandwidth). The {id} in the path must match the body.
func (cp *ControlPlane) handleUpdatePool(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePoolID(w, r)
	if !ok {
		return
	}
	var req PoolRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: %v", err)
		return
	}
	if req.Pool.ID != id {
		writeErr(w, http.StatusBadRequest, "path id %d != body id %d", id, req.Pool.ID)
		return
	}
	if err := ingestValidate(req.Pool); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid pool: %v", err)
		return
	}
	normalizeUnlimited(&req)
	tokens := req.Tokens
	if tokens == 0 {
		var err error
		if tokens, err = tokensForPool(req.Pool); err != nil {
			writeErr(w, http.StatusBadRequest, "cannot derive token cost: %v", err)
			return
		}
	}
	conflicts, handled := cp.resolveOverlaps(r.Context(), w, req.Pool, req.Replace)
	if handled {
		return
	}
	rec, primaryEdge, generation, err := cp.Orch.UpdatePoolGen(r.Context(), req.Pool, tokens)
	if err != nil {
		switch {
		case errors.Is(err, orchestrator.ErrNoPlacement):
			writeErr(w, http.StatusServiceUnavailable, "no capacity: %v", err)
		default:
			writeErr(w, http.StatusConflict, "update failed: %v", err)
		}
		return
	}
	replaced, err := cp.evict(r.Context(), conflicts, req.Pool.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "pool updated but eviction failed: %v", err)
		return
	}
	// ASYNC API-result correlation: UPDATE carries no anti-replay nonce, so mint a
	// random request_id. Register the pending against the primary edge + the pushed
	// generation (resolved to "converged" by the edge's report, or "failed" by the
	// timeout sweep). Unchanged when no brokers are configured (Noop emitter).
	requestID := newRequestID()
	// TIER-4: carry the granted rate basis onto the converged event (same as create).
	cp.registerPendingEnriched(requestID, "update", req.Pool.ID, primaryEdge, "", generation, rateBasisForPool(req.Pool, tokens))
	writeJSON(w, http.StatusOK, poolResponse{Record: rec, Replaced: replaced, RequestID: requestID, Generation: generation})
}

// poolExists reports whether a pool id is present in the MANDATORY Yugabyte store
// (the hybrid migration moved pool/member rows off etcd; the create writes ZERO
// etcd, so the etcd poolstore is empty in production). cp.YB is always wired (the
// controller exits at startup without a live Yugabyte). A read-only existence probe.
func (cp *ControlPlane) poolExists(ctx context.Context, id model.PoolID) (bool, error) {
	_, ok, err := cp.YB.Get(ctx, id)
	return ok, err
}

func (cp *ControlPlane) handleGetPool(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePoolID(w, r)
	if !ok {
		return
	}
	// Pools live in the MANDATORY Yugabyte bulk store (the create writes ZERO etcd, so
	// the etcd poolstore is empty in production). Read from there, projecting to the
	// poolstore.Record shape the admin response uses.
	yr, found, err := cp.YB.Get(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "get: %v", err)
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, "pool %d not found", id)
		return
	}
	writeJSON(w, http.StatusOK, ybToRecord(yr))
}

func (cp *ControlPlane) handleListPools(w http.ResponseWriter, r *http.Request) {
	yrs, err := cp.YB.List(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list: %v", err)
		return
	}
	recs := make([]poolstore.Record, 0, len(yrs))
	for _, yr := range yrs {
		recs = append(recs, ybToRecord(yr))
	}
	writeJSON(w, http.StatusOK, recs)
}

// ybToRecord projects a ybstore.Record (the authoritative bulk-store row) to the
// poolstore.Record shape the admin GET/LIST responses use.
func ybToRecord(yr ybstore.Record) poolstore.Record {
	rec := poolstore.Record{Pool: yr.Pool, Primary: yr.Primary, Backup: yr.Backup, Tokens: yr.Tokens, Retiring: yr.RetiringEdge}
	rec.Pool.HomeEdge = yr.Primary
	return rec
}

func (cp *ControlPlane) handleDestroyPool(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePoolID(w, r)
	if !ok {
		return
	}
	primaryEdge, generation, err := cp.Orch.DestroyPoolGen(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "destroy: %v", err)
		return
	}
	// ASYNC API-result correlation: DESTROY carries no anti-replay nonce, so mint a
	// random request_id and register the pending against the primary edge + the
	// withdrawal-render generation. The control-plane removal already succeeded
	// synchronously; the agent's data-plane withdrawal is confirmed asynchronously
	// (report echo → "converged", or the timeout sweep → "failed"). Response changes
	// from 204 No Content to 202 Accepted carrying the request_id so the BSS can
	// correlate the eventual event. Unchanged when no brokers are configured (Noop).
	requestID := newRequestID()
	cp.RegisterPending(requestID, "destroy", id, primaryEdge, generation)
	writeJSON(w, http.StatusAccepted, destroyResponse{
		RequestID:  requestID,
		PoolID:     id,
		Edge:       primaryEdge,
		Generation: generation,
	})
}

func (cp *ControlPlane) handleListAgents(w http.ResponseWriter, r *http.Request) {
	agents, err := cp.Registry.List(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list agents: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, agents)
}

func (cp *ControlPlane) handleDecommission(w http.ResponseWriter, r *http.Request) {
	edge := model.EdgeID(r.PathValue("id"))
	if edge == "" {
		writeErr(w, http.StatusBadRequest, "missing agent id")
		return
	}
	generation, err := cp.Orch.DecommissionGen(r.Context(), edge)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "decommission: %v", err)
		return
	}
	cp.Liveness.Forget(edge) // stop tracking a deliberately-removed agent
	// TIER-3 edge inventory: the edge has been drained + deregistered (a planned, permanent
	// exit — distinct from edge-down, a death it may revive from). Async / Noop-safe; the
	// per-pool re-homes already emitted their own "rehome" detail events during the drain.
	cp.emitEdgeDeregistered(edge)
	// ASYNC API-result correlation. A decommission re-homes the edge's pools onto MANY
	// other edges (one promoted new primary per pool), so there is no single primary to
	// correlate against. CORRELATION CHOICE: ONE request_id, registered against the
	// DRAINED edge itself + its terminal WITHDRAWAL generation (the one render uniquely
	// belonging to THIS operation). Rationale: the decommission is a GRACEFUL, planned
	// exit against a still-LIVE agent, so its final empty-state render IS echoed back;
	// the IDENTICAL EdgeReport.Generation>=pending.generation test then fires one
	// "converged" the instant the decommissioned edge confirms it has shed its state
	// (or the timeout sweep fires "failed"). This is deliberately one event per operation
	// (not per re-homed edge): the per-pool re-homes ride the same idempotent pivot
	// machinery and need no separate handshake, and one event matches the BSS's "is the
	// agent drained yet" question. A generation of 0 (sharded: the owning replica renders
	// the withdrawal on the converge loop) leaves the timeout sweep as the sole resolver,
	// like a cross-shard destroy. The 400/500 sync paths and the 200 status are unchanged;
	// the request_id is ADDED to the existing body. Inert with no brokers (Noop emitter).
	requestID := newRequestID()
	cp.RegisterPending(requestID, "decommission", 0, edge, generation)
	writeJSON(w, http.StatusOK, map[string]any{
		"decommissioned": string(edge),
		"request_id":     requestID,
		"generation":     generation,
	})
}

// handleMigratePool is the PLANNED pool-migration entry (C-03, §3.3/§4.4): it
// moves a pool off its current primary onto its pre-built backup using the same
// ordered, ready-gated machinery as a failover — 先发新 (promote the standby
// backup to FULL) 后撤旧 (withdraw the old primary) — then reprovisions a fresh
// backup so redundancy is restored. Unlike a failover (triggered by liveness on a
// dead edge), this is operator-initiated against a HEALTHY primary (maintenance,
// rebalancing), so the old primary's withdrawal is expected to complete.
func (cp *ControlPlane) handleMigratePool(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePoolID(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	// Existence pre-check sourced from the authoritative store: the MANDATORY Yugabyte
	// store (the pool/member rows moved off etcd in the hybrid migration; cp.YB is always
	// wired, the controller exits at startup without a live Yugabyte). PromoteBackup
	// itself re-checks, so this is purely the clear-404 fast path.
	found, err := cp.poolExists(ctx, id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "migrate: get pool: %v", err)
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, "pool %d not found", id)
		return
	}

	// PromoteBackupGen surfaces the NEW primary + the generation its FULL render carries
	// (the API-result correlation handle), additionally to the record PromoteBackup
	// returns. The migrate is otherwise UNCHANGED.
	rec, newPrimary, oldPrimary, generation, err := cp.Orch.PromoteBackupGen(ctx, id)
	switch {
	case errors.Is(err, orchestrator.ErrNoBackup):
		// Nothing to promote — a single-home pool has no pre-built target. The
		// operator must provision a backup first (planned migration rides the backup).
		writeErr(w, http.StatusConflict, "pool %d has no backup to migrate onto", id)
		return
	case errors.Is(err, orchestrator.ErrWithdrawIncomplete):
		// New primary is live and serving; only the old primary's withdrawal lagged
		// (rare for a healthy primary). Functionally migrated — log and restore backup.
		cp.log.Warn("planned migration: old-primary withdraw incomplete (new primary live)", "pool", id, "err", err)
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "migrate (promote): %v", err)
		return
	}

	// Restore redundancy: provision a fresh backup on a spare edge (ready-gated).
	// A missing spare is not a migration failure — the move succeeded, the pool is
	// just single-homed until capacity frees up.
	backupRestored := true
	if rec2, perr := cp.Orch.ProvisionBackup(ctx, id); perr != nil {
		backupRestored = false
		cp.log.Warn("planned migration: backup not reprovisioned (redundancy not restored)", "pool", id, "err", perr)
	} else {
		rec = rec2
	}

	// ASYNC API-result correlation (same machinery as create/update/destroy): MIGRATE
	// carries no anti-replay nonce, so mint a random request_id. Register the pending
	// against the NEW primary edge + the generation its FULL render carries — the
	// promoted agent's report echoes that generation once it has rendered FULL, which
	// the IDENTICAL EdgeReport.Generation>=pending.generation test resolves to
	// "converged" (or the timeout sweep fires "failed"). A generation of 0 (a sharded
	// new primary the owning replica renders on the converge loop) registers a pending
	// only the timeout sweep can resolve — the same fallback DestroyPoolGen uses for a
	// cross-shard primary. Unchanged when no brokers are configured (Noop emitter); the
	// 404/409/500 sync paths above are untouched and the status code stays 200.
	requestID := newRequestID()
	// TIER-4: thread the OLD primary out (PromoteBackupGen) so the migrate's converged
	// event carries from_edge→to_edge (oldPrimary→newPrimary), like a failover, instead of
	// only Edge=newPrimary. A backup_restored=false migrate (no spare for a fresh backup)
	// already surfaced redundancy-lost via the orchestrator's provision-backup notify, so
	// it is NOT re-emitted here.
	cp.registerPendingEnriched(requestID, "migrate", id, newPrimary, oldPrimary, generation, nil)
	writeJSON(w, http.StatusOK, map[string]any{
		"migrated":        id,
		"new_primary":     rec.Primary,
		"new_backup":      rec.Backup,
		"backup_restored": backupRestored,
		"request_id":      requestID,
		"generation":      generation,
	})
}

// ingestValidate checks the parts of a BSS pool that are known at ingestion —
// members and action — but NOT HomeEdge, which the controller assigns by
// placement (so the contract's full Pool.Validate, which requires a home, is too
// strict here).
func ingestValidate(p model.Pool) error {
	if len(p.Members) == 0 {
		return fmt.Errorf("pool %d has no members", p.ID)
	}
	// Within one pool, members must not OVERLAP (CIDR containment either way, or
	// exact duplicate) — a /24 and a /32 inside it in the same pool is redundant
	// and muddies render/accounting. Cross-pool overlap is handled separately by
	// the MemberConflicts check against the Yugabyte members table (replace policy).
	// O(n²), n small.
	for i, m := range p.Members {
		if err := m.Validate(); err != nil {
			return fmt.Errorf("member %s: %w", m.Prefix, err)
		}
		for j := 0; j < i; j++ {
			if p.Members[j].Prefix.Overlaps(m.Prefix) {
				return fmt.Errorf("member %s overlaps member %s in pool %d", m.Prefix, p.Members[j].Prefix, p.ID)
			}
		}
	}
	switch p.Action.Kind {
	case model.ActionRateLimit:
		return nil
	case model.ActionBlackhole:
		// Suppression actions are forced to carry a TTL so a forgotten blackhole
		// auto-lifts when it expires instead of suppressing the victim IP forever
		// (§8/T-706); the controller's expiry sweep removes it on elapse.
		if p.Action.ExpiryUnixMs <= 0 {
			return fmt.Errorf("blackhole action requires a TTL (action.expiry_unix_ms > 0)")
		}
		return nil
	default:
		return fmt.Errorf("unsupported action %v (scrub is V2)", p.Action.Kind)
	}
}

// normalizeUnlimited maps the BSS "无限带宽/95峰值计费" convention (tokens = -1)
// to the canonical unlimited representation: rate CIR = 0 and 0 reserved tokens.
// A CIR==0 rate-limit pool renders an unlimited policer (count, never drop).
func normalizeUnlimited(req *PoolRequest) {
	if req.Tokens < 0 {
		req.Pool.EgressRate.CIR = 0
		req.Pool.IngressRate.CIR = 0
		req.Tokens = 0
	}
}

// tokensForPool derives a pool's per-home quota cost in bits/s from its egress
// rate (the scarce DC uplink, §5.2). Control pools (blackhole/scrub) consume ~no
// bandwidth but still occupy a home slot, so they cost a nominal 1.
func tokensForPool(p model.Pool) (int64, error) {
	if p.Action.Kind != model.ActionRateLimit {
		return 1, nil
	}
	r := p.EgressRate
	switch r.Type {
	case model.RateKbps:
		return int64(r.CIR) * 1000, nil // kbps → bits/s
	default:
		return 0, fmt.Errorf("pps-rated pool needs an explicit token cost (bits/s)")
	}
}

func parsePoolID(w http.ResponseWriter, r *http.Request) (model.PoolID, bool) {
	n, err := strconv.ParseUint(r.PathValue("id"), 10, 32)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid pool id: %v", err)
		return 0, false
	}
	return model.PoolID(n), true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, format string, args ...any) {
	writeJSON(w, status, map[string]string{"error": fmt.Sprintf(format, args...)})
}
