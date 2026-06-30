// Failover-pivot abstraction (hybrid-architecture step 2, 2026-06). The async
// reconciler (reconcile.go) and the synchronous drain (failover.go) used to read
// and CAS-write the etcd poolstore.Record (GetRev/PutCAS/DeleteCAS gated on the
// etcd ModRevision). That last per-create etcd write + the per-pool reconcile churn
// scaled super-linearly with concurrency and pegged etcd. This seam moves the pivot
// to a Yugabyte OPTIMISTIC-VERSION column: the version replaces ModRevision and the
// retiring boolean replaces the Record's Retiring marker.
//
// pivotStore is the interface the lifecycle paths talk to. *ybstore.Store satisfies
// it in production (a version column CAS, zero etcd); an in-memory fake satisfies it
// in the orchestrator tests (so the failover-semantics tests exercise the SAME
// version-CAS code path without a live Yugabyte). The per-(pool,edge) reservation
// handle ids the failover paths use are DERIVED from (pool,edge) — deterministic —
// so the pivot store carries no resv-id state; reconcile.go recomputes them.
package orchestrator

import (
	"context"
	"net/netip"

	"github.com/fivetime/sbw-contract/model"

	"github.com/fivetime/sbw-server/internal/ybstore"
)

// pivotRow is the failover-pivot state the reconciler reads and CAS-writes: a pool's
// home/backup/retiring assignment plus its optimistic-CAS Version (the Yugabyte
// version column, replacing the etcd ModRevision). It mirrors ybstore.PivotRow and
// is the orchestrator-local view so the package does not leak ybstore types into the
// reconcile logic (and the in-memory test fake can produce the same shape).
type pivotRow struct {
	Pool     model.Pool
	PoolID   model.PoolID
	Primary  model.EdgeID
	Backup   model.EdgeID
	Retiring model.EdgeID
	Tokens   int64
	Version  int64
}

// homes returns the pivot's home edges (primary first, then backup if set) — the
// edges a double-death tear-down withdraws.
func (r pivotRow) homes() []model.EdgeID {
	if r.Backup == "" {
		return []model.EdgeID{r.Primary}
	}
	return []model.EdgeID{r.Primary, r.Backup}
}

// pivotStore is the version-CAS failover pivot the lifecycle paths read and write.
// *ybstore.Store satisfies it (Yugabyte version column); the test fake satisfies it
// in-memory. ErrConflict from UpdateCAS/DeleteCAS is the lost-CAS signal (re-read +
// retry next pass) — the no-double-failover guard.
type pivotStore interface {
	GetForReconcile(ctx context.Context, id model.PoolID) (ybstore.PivotRow, bool, error)
	UpdateCAS(ctx context.Context, row ybstore.PivotRow, expectVersion int64, newPrimary, newBackup, newRetiring model.EdgeID) error
	DeleteCAS(ctx context.Context, id model.PoolID, expectVersion int64) error
	PoolsForDeadEdge(ctx context.Context, edge model.EdgeID) ([]model.PoolID, error)
	ListPivotsByPrimary(ctx context.Context, edge model.EdgeID) ([]ybstore.PivotRow, error)
	ListIDs(ctx context.Context) ([]model.PoolID, error)
}

// YBStore is the MANDATORY YugabyteDB-backed bulk pool/member store the orchestrator
// runs every pool/member lifecycle operation through. *ybstore.Store satisfies it in
// production; an in-memory fake satisfies it in the unit tests so they exercise the
// SAME Yugabyte path without a live DB. It subsumes pivotStore (the same Yugabyte row
// is both the bulk data and the failover pivot), so the create/update/destroy data
// writes and the version-CAS failover pivot share one backend — there is NO all-etcd
// fallback (the controller exits at startup without a live Yugabyte).
type YBStore interface {
	pivotStore
	Get(ctx context.Context, id model.PoolID) (ybstore.Record, bool, error)
	CreatePool(ctx context.Context, rec ybstore.Record, members []model.Member) error
	UpdatePool(ctx context.Context, rec ybstore.Record, members []model.Member) error
	RemoveMember(ctx context.Context, poolID model.PoolID, prefix netip.Prefix) error
	DestroyPool(ctx context.Context, id model.PoolID) error
	Delete(ctx context.Context, id model.PoolID) error
	PoolsForHome(ctx context.Context, edge model.EdgeID) ([]model.Pool, error)
	PoolsForBackup(ctx context.Context, edge model.EdgeID) ([]model.Pool, error)
	MemberConflicts(ctx context.Context, prefixes []netip.Prefix, excludePool model.PoolID) ([]ybstore.Conflict, error)
	List(ctx context.Context) ([]ybstore.Record, error)
}

// pivot returns the version-CAS failover pivot store. Yugabyte is mandatory, so it is
// always the wired YBStore (production *ybstore.Store or the test fake) — never nil.
func (o *Orchestrator) pivot() pivotStore { return o.yb }

// toPivotRow adapts a ybstore.PivotRow to the orchestrator-local pivotRow.
func toPivotRow(r ybstore.PivotRow) pivotRow {
	return pivotRow{
		Pool: r.Pool, PoolID: r.PoolID, Primary: r.Primary, Backup: r.Backup,
		Retiring: r.Retiring, Tokens: r.Tokens, Version: r.Version,
	}
}
