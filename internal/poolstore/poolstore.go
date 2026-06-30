// Package poolstore holds the controller's pool RECORD DTO and the create-path
// sentinel errors. The authoritative pool store is now Yugabyte-backed (see
// internal/ybstore); the former etcd-backed Store has been removed. What remains
// is the shared record shape the orchestrator/admin pass around plus the two
// create errors they branch on:
//
//   - Record: a stored pool's definition and both home assignments (PRIMARY edge
//     advertises + settles, BACKUP edge stands by) plus the deterministic
//     (pool,edge)-keyed placement-handle ids stamped on each home (no etcd
//     ledger reservation backs them — the create path is optimistic).
//   - ErrExists / ErrReplay: the create-if-not-exists and anti-replay outcomes
//     the orchestrator (ErrExists, T-707) and admin (ErrReplay) classify.
package poolstore

import (
	"errors"

	"github.com/fivetime/sbw-contract/model"
)

// ErrExists is returned when a record with that pool id already exists (the
// create-if-not-exists CAS lost). The orchestrator treats it specially: a
// CONCURRENT identical create that loses this race (the Yugabyte ON CONFLICT
// (id) DO NOTHING no-op) is an idempotent replay, not a failure — the winner
// owns the pool and the loser writes nothing to roll back (T-707).
var ErrExists = errors.New("poolstore: pool already exists")

// ErrReplay was returned when a create request's separate anti-replay nonce key
// already existed within the replay window. That dedicated etcd create-nonce was
// removed in the Yugabyte migration: the pools.id PRIMARY KEY is now the
// anti-replay key, so a replayed create (same id) resolves to ErrExists via the
// ON CONFLICT no-op and no path returns ErrReplay. The error is retained only for
// the admin layer's classification branch and API compatibility.
var ErrReplay = errors.New("poolstore: replayed create nonce")

// Record is a stored pool: its definition and both home assignments. The Pool's
// own HomeEdge always equals Primary (the render's notion of "home"); Backup is
// the standby edge ("" if the pool has no backup, e.g. blackhole or single-home).
type Record struct {
	Pool          model.Pool   `json:"pool"`
	Primary       model.EdgeID `json:"primary"`
	Backup        model.EdgeID `json:"backup,omitempty"`
	PrimaryResvID string       `json:"primary_resv_id"`
	BackupResvID  string       `json:"backup_resv_id,omitempty"`
	// Tokens is the per-home quota cost. Stored so failover (promote/provision) can
	// re-place on a new home without the caller re-supplying it. Placement is
	// OPTIMISTIC cached-capacity (scheduler.SelectHomes / o.remaining) — no etcd
	// ledger debit/reserve — and the value is persisted in the Yugabyte pivot row.
	Tokens    int64 `json:"tokens"`
	CreatedAt int64 `json:"created_at_ms"`

	// Retiring + RetiringResvID mark an OLD primary pending withdrawal during an
	// async promote (L-07, hole 3 "先发新后撤旧" across replicas). The async
	// reconciler sets Primary=newPrimary and Retiring=oldPrimary in the promote
	// step but does NOT withdraw the old primary yet; a later step bumps the old
	// primary's edgever (withdraw) ONLY after the new primary's applied-version
	// confirms it is live — reproducing the synchronous
	// happens-before cross-replica so a Decommission never opens a naked window.
	// Empty in the steady state. Retiring is NOT a home (not indexed, renders
	// empty → withdrawn); it is purely the "still owe this edge a withdraw" marker.
	Retiring       model.EdgeID `json:"retiring,omitempty"`
	RetiringResvID string       `json:"retiring_resv_id,omitempty"`
}

// Homes returns the record's home edges (primary first, then backup if set) —
// the edges whose desired state a create/destroy of this pool re-renders.
func (r Record) Homes() []model.EdgeID {
	if r.Backup == "" {
		return []model.EdgeID{r.Primary}
	}
	return []model.EdgeID{r.Primary, r.Backup}
}
