package orchestrator

import (
	"context"
	"fmt"
	"net/netip"
	"sort"
	"sync"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-server/internal/poolstore"
	"github.com/fivetime/sbw-server/internal/srcmap"
	"github.com/fivetime/sbw-server/internal/ybstore"
)

// fakeYB is an in-memory implementation of the orchestrator's MANDATORY ybStore
// interface. Production wires *ybstore.Store; the unit tests wire this so they exercise
// the SAME Yugabyte code path (CreatePool/UpdatePool/RemoveMember/DestroyPool + the
// version-CAS failover pivot: GetForReconcile/UpdateCAS/DeleteCAS) WITHOUT a live DB.
//
// It is the test analog of the production seam. It is also the SINGLE source the harness
// observes through: getRec/getRev expose a pool's authoritative record as the surviving
// poolstore.Record DTO, and smGet/smSourcesForHome/smList expose the member src→home
// claims as the surviving srcmap.Record DTO. The CAS token is the Yugabyte-style monotonic
// Version (replacing the old etcd ModRevision), so the migrated failover state machine is
// asserted through the same record/member lenses, now backed by this in-memory store.
type fakeYB struct {
	mu      sync.Mutex
	pools   map[model.PoolID]ybstore.Record
	version map[model.PoolID]int64
	// homes is the member src→home claim set the harness observes via smGet/etc.,
	// kept in lockstep with the pools' primary on every mutation. Maps the member
	// prefix's string to its claim record.
	homes map[string]srcmap.Record
}

func newFakeYB() *fakeYB {
	return &fakeYB{
		pools:   map[model.PoolID]ybstore.Record{},
		version: map[model.PoolID]int64{},
		homes:   map[string]srcmap.Record{},
	}
}

// --- observation lenses (replace the former etcd poolstore/srcmap mirror) ---

// recOf renders a pool's authoritative state as the surviving poolstore.Record DTO.
// Called under f.mu.
func (f *fakeYB) recOf(rec ybstore.Record) poolstore.Record {
	pr := poolstore.Record{
		Pool:          rec.Pool,
		Primary:       rec.Primary,
		Backup:        rec.Backup,
		Tokens:        rec.Tokens,
		Retiring:      rec.RetiringEdge,
		PrimaryResvID: resvID(rec.Pool.ID, rec.Primary),
	}
	pr.Pool.HomeEdge = rec.Primary
	if rec.Backup != "" {
		pr.BackupResvID = resvID(rec.Pool.ID, rec.Backup)
	}
	if rec.RetiringEdge != "" {
		pr.RetiringResvID = resvID(rec.Pool.ID, rec.RetiringEdge)
	}
	return pr
}

// getRec returns a pool's record (replaces the former h.store.Get).
func (f *fakeYB) getRec(id model.PoolID) (poolstore.Record, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.pools[id]
	if !ok {
		return poolstore.Record{}, false
	}
	return f.recOf(r), true
}

// getRev returns a pool's record plus its monotonic version (replaces the former
// h.store.GetRev; the version stands in for the old etcd ModRevision).
func (f *fakeYB) getRev(id model.PoolID) (poolstore.Record, int64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.pools[id]
	if !ok {
		return poolstore.Record{}, 0, false
	}
	return f.recOf(r), f.version[id], true
}

// claim records a member's src→home claim. Called under f.mu.
func (f *fakeYB) claim(prefix netip.Prefix, home model.EdgeID, pool model.PoolID) {
	f.homes[prefix.String()] = srcmap.Record{Src: prefix, Home: home, PoolID: pool}
}

// release drops a member's src→home claim iff it belongs to pool. Called under f.mu.
func (f *fakeYB) release(prefix netip.Prefix, pool model.PoolID) {
	if r, ok := f.homes[prefix.String()]; ok && r.PoolID == pool {
		delete(f.homes, prefix.String())
	}
}

// smGet returns a member's src→home claim (replaces the former h.sm.Get).
func (f *fakeYB) smGet(prefix netip.Prefix) (srcmap.Record, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.homes[prefix.String()]
	return r, ok
}

// smSourcesForHome returns every source homed to edge (replaces h.sm.SourcesForHome).
func (f *fakeYB) smSourcesForHome(edge model.EdgeID) []srcmap.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []srcmap.Record
	for _, r := range f.homes {
		if r.Home == edge {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Src.String() < out[j].Src.String() })
	return out
}

// smList returns all src→home claims (replaces h.sm.List).
func (f *fakeYB) smList() []srcmap.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]srcmap.Record, 0, len(f.homes))
	for _, r := range f.homes {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Src.String() < out[j].Src.String() })
	return out
}

// Used reports an edge's committed used capacity — the SUM of cost over the pools it is
// PRIMARY (home) for — so the harness can drive the orchestrator's optimistic placement
// (capacityProvider) off the SAME authoritative pool set, no live CapacityCache needed.
func (f *fakeYB) Used(edge model.EdgeID) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var used int64
	for _, r := range f.pools {
		if r.Primary == edge {
			used += r.Tokens
		}
	}
	return used
}

func (f *fakeYB) Get(ctx context.Context, id model.PoolID) (ybstore.Record, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.pools[id]
	return r, ok, nil
}

func (f *fakeYB) CreatePool(ctx context.Context, rec ybstore.Record, members []model.Member) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.pools[rec.Pool.ID]; ok {
		return fmt.Errorf("pool %d: %w", rec.Pool.ID, ybstore.ErrExists)
	}
	// Cross-pool double-claim guard (members.prefix PK): reject a member already held by
	// a DIFFERENT pool.
	held := f.memberOwners()
	for _, m := range members {
		if owner, ok := held[m.Prefix.String()]; ok && owner != rec.Pool.ID {
			return fmt.Errorf("member %s: %w", m.Prefix, ybstore.ErrMemberConflict)
		}
	}
	rec.Pool.HomeEdge = rec.Primary
	rec.Pool.Members = members
	f.pools[rec.Pool.ID] = rec
	f.version[rec.Pool.ID] = 1
	for _, m := range members {
		f.claim(m.Prefix, rec.Primary, rec.Pool.ID)
	}
	return nil
}

func (f *fakeYB) UpdatePool(ctx context.Context, rec ybstore.Record, members []model.Member) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.pools[rec.Pool.ID]; !ok {
		return fmt.Errorf("pool %d: %w", rec.Pool.ID, ybstore.ErrNotFound)
	}
	held := f.memberOwners()
	for _, m := range members {
		if owner, ok := held[m.Prefix.String()]; ok && owner != rec.Pool.ID {
			return fmt.Errorf("member %s: %w", m.Prefix, ybstore.ErrMemberConflict)
		}
	}
	// Release the src→home claims for members removed by this update.
	old := f.pools[rec.Pool.ID]
	newSet := map[string]bool{}
	for _, m := range members {
		newSet[m.Prefix.String()] = true
	}
	for _, m := range old.Pool.Members {
		if !newSet[m.Prefix.String()] {
			f.release(m.Prefix, rec.Pool.ID)
		}
	}
	rec.Pool.HomeEdge = rec.Primary
	rec.Pool.Members = members
	f.pools[rec.Pool.ID] = rec
	f.version[rec.Pool.ID]++
	for _, m := range members {
		f.claim(m.Prefix, rec.Primary, rec.Pool.ID)
	}
	return nil
}

func (f *fakeYB) RemoveMember(ctx context.Context, poolID model.PoolID, prefix netip.Prefix) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.pools[poolID]
	if !ok {
		return fmt.Errorf("pool %d: %w", poolID, ybstore.ErrNotFound)
	}
	kept := rec.Pool.Members[:0:0]
	for _, m := range rec.Pool.Members {
		if m.Prefix == prefix {
			continue
		}
		kept = append(kept, m)
	}
	rec.Pool.Members = kept
	f.pools[poolID] = rec
	f.version[poolID]++
	f.release(prefix, poolID)
	for _, m := range kept {
		f.claim(m.Prefix, rec.Primary, poolID)
	}
	return nil
}

func (f *fakeYB) DestroyPool(ctx context.Context, id model.PoolID) error { return f.Delete(ctx, id) }

func (f *fakeYB) Delete(ctx context.Context, id model.PoolID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.pools[id]
	if ok {
		for _, m := range rec.Pool.Members {
			f.release(m.Prefix, id)
		}
	}
	delete(f.pools, id)
	delete(f.version, id)
	return nil
}

func (f *fakeYB) PoolsForHome(ctx context.Context, edge model.EdgeID) ([]model.Pool, error) {
	return f.poolsBy(edge, true)
}

func (f *fakeYB) PoolsForBackup(ctx context.Context, edge model.EdgeID) ([]model.Pool, error) {
	return f.poolsBy(edge, false)
}

func (f *fakeYB) poolsBy(edge model.EdgeID, primary bool) ([]model.Pool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []model.Pool
	for _, r := range f.pools {
		if (primary && r.Primary == edge) || (!primary && r.Backup == edge) {
			p := r.Pool
			p.HomeEdge = r.Primary
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (f *fakeYB) GetForReconcile(ctx context.Context, id model.PoolID) (ybstore.PivotRow, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.pools[id]
	if !ok {
		return ybstore.PivotRow{}, false, nil
	}
	return ybstore.PivotRow{
		Pool: r.Pool, PoolID: id, Primary: r.Primary, Backup: r.Backup,
		Retiring: r.RetiringEdge, Tokens: r.Tokens, Version: f.version[id],
	}, true, nil
}

func (f *fakeYB) ListPivotsByPrimary(ctx context.Context, edge model.EdgeID) ([]ybstore.PivotRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []ybstore.PivotRow
	for id, r := range f.pools {
		if r.Primary != edge {
			continue
		}
		out = append(out, ybstore.PivotRow{
			Pool: r.Pool, PoolID: id, Primary: r.Primary, Backup: r.Backup,
			Retiring: r.RetiringEdge, Tokens: r.Tokens, Version: f.version[id],
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PoolID < out[j].PoolID })
	return out, nil
}

func (f *fakeYB) UpdateCAS(ctx context.Context, row ybstore.PivotRow, expectVersion int64, newPrimary, newBackup, newRetiring model.EdgeID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.version[row.PoolID] != expectVersion {
		return fmt.Errorf("pool %d: %w", row.PoolID, ybstore.ErrConflict)
	}
	rec, ok := f.pools[row.PoolID]
	if !ok {
		return fmt.Errorf("pool %d: %w", row.PoolID, ybstore.ErrConflict)
	}
	rec.Primary = newPrimary
	rec.Backup = newBackup
	rec.RetiringEdge = newRetiring
	rec.Pool.HomeEdge = newPrimary
	f.pools[row.PoolID] = rec
	f.version[row.PoolID] = expectVersion + 1
	// Re-home the members' src→home claim to the new primary (atomic-with-pivot in prod).
	if newPrimary != row.Primary && newPrimary != "" {
		for _, m := range rec.Pool.Members {
			f.claim(m.Prefix, newPrimary, row.PoolID)
		}
	}
	return nil
}

func (f *fakeYB) DeleteCAS(ctx context.Context, id model.PoolID, expectVersion int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.version[id] != expectVersion {
		return fmt.Errorf("pool %d: %w", id, ybstore.ErrConflict)
	}
	rec, ok := f.pools[id]
	if ok {
		for _, m := range rec.Pool.Members {
			f.release(m.Prefix, id)
		}
	}
	delete(f.pools, id)
	delete(f.version, id)
	return nil
}

func (f *fakeYB) PoolsForDeadEdge(ctx context.Context, edge model.EdgeID) ([]model.PoolID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []model.PoolID
	for id, r := range f.pools {
		if r.Primary == edge || r.Backup == edge {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (f *fakeYB) List(ctx context.Context) ([]ybstore.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ybstore.Record, 0, len(f.pools))
	for _, r := range f.pools {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Pool.ID < out[j].Pool.ID })
	return out, nil
}

func (f *fakeYB) ListIDs(ctx context.Context) ([]model.PoolID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]model.PoolID, 0, len(f.pools))
	for id := range f.pools {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (f *fakeYB) MemberConflicts(ctx context.Context, prefixes []netip.Prefix, excludePool model.PoolID) ([]ybstore.Conflict, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := map[string]bool{}
	var out []ybstore.Conflict
	for _, r := range f.pools {
		if r.Pool.ID == excludePool {
			continue
		}
		for _, m := range r.Pool.Members {
			for _, p := range prefixes {
				if m.Prefix.Overlaps(p) && !seen[m.Prefix.String()] {
					seen[m.Prefix.String()] = true
					out = append(out, ybstore.Conflict{Prefix: m.Prefix, PoolID: r.Pool.ID})
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Prefix.String() < out[j].Prefix.String() })
	return out, nil
}

// memberOwners maps each currently-claimed member prefix to its owning pool (for the
// cross-pool double-claim guard). Called under f.mu.
func (f *fakeYB) memberOwners() map[string]model.PoolID {
	owners := map[string]model.PoolID{}
	for id, r := range f.pools {
		for _, m := range r.Pool.Members {
			owners[m.Prefix.String()] = id
		}
	}
	return owners
}
