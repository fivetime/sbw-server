package ybstore

import (
	"context"
	"fmt"
	"net/netip"
	"sort"
	"sync"

	"github.com/fivetime/sbw-contract/model"
)

// Mem is an in-memory implementation of the pool/member bulk store + version-CAS
// failover pivot — the same method set *Store exposes over Yugabyte, backed by a Go
// map. It exists so unit/integration tests (and local dev) can exercise the MANDATORY
// Yugabyte code path WITHOUT a live DB: YugabyteDB is required in production (the
// controller exits at startup without it), so there is no longer an all-etcd fallback
// for tests to lean on. Mem reproduces the production semantics the orchestrator relies
// on — the pools.id anti-replay (ErrExists), the members.prefix cross-pool double-claim
// guard (ErrMemberConflict), and the optimistic version-CAS (ErrConflict) — so the seam
// is faithful, not a stub.
type Mem struct {
	mu      sync.Mutex
	pools   map[model.PoolID]Record
	version map[model.PoolID]int64
}

// NewMem builds an empty in-memory store.
func NewMem() *Mem {
	return &Mem{pools: map[model.PoolID]Record{}, version: map[model.PoolID]int64{}}
}

// Used reports an edge's committed used capacity — the SUM of cost over the pools it is
// PRIMARY (home) for — so it doubles as the orchestrator's CapacityProvider for
// optimistic placement (no separate CapacityCache needed in tests).
func (m *Mem) Used(edge model.EdgeID) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var used int64
	for _, r := range m.pools {
		if r.Primary == edge {
			used += r.Tokens
		}
	}
	return used
}

// memberOwners maps each claimed member prefix to its owning pool (the members.prefix PK
// analog). Called under m.mu.
func (m *Mem) memberOwners() map[string]model.PoolID {
	owners := map[string]model.PoolID{}
	for id, r := range m.pools {
		for _, mem := range r.Pool.Members {
			owners[mem.Prefix.String()] = id
		}
	}
	return owners
}

func (m *Mem) conflictsForeign(members []model.Member, pool model.PoolID) error {
	owners := m.memberOwners()
	for _, mem := range members {
		if owner, ok := owners[mem.Prefix.String()]; ok && owner != pool {
			return fmt.Errorf("member %s: %w", mem.Prefix, ErrMemberConflict)
		}
	}
	return nil
}

func (m *Mem) Get(ctx context.Context, id model.PoolID) (Record, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.pools[id]
	return r, ok, nil
}

func (m *Mem) CreatePool(ctx context.Context, rec Record, members []model.Member) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.pools[rec.Pool.ID]; ok {
		return fmt.Errorf("pool %d: %w", rec.Pool.ID, ErrExists)
	}
	if err := m.conflictsForeign(members, rec.Pool.ID); err != nil {
		return err
	}
	rec.Pool.HomeEdge = rec.Primary
	rec.Pool.Members = members
	m.pools[rec.Pool.ID] = rec
	m.version[rec.Pool.ID] = 1
	return nil
}

func (m *Mem) UpdatePool(ctx context.Context, rec Record, members []model.Member) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.pools[rec.Pool.ID]; !ok {
		return fmt.Errorf("pool %d: %w", rec.Pool.ID, ErrNotFound)
	}
	if err := m.conflictsForeign(members, rec.Pool.ID); err != nil {
		return err
	}
	rec.Pool.HomeEdge = rec.Primary
	rec.Pool.Members = members
	m.pools[rec.Pool.ID] = rec
	m.version[rec.Pool.ID]++
	return nil
}

func (m *Mem) RemoveMember(ctx context.Context, poolID model.PoolID, prefix netip.Prefix) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.pools[poolID]
	if !ok {
		return fmt.Errorf("pool %d: %w", poolID, ErrNotFound)
	}
	kept := rec.Pool.Members[:0:0]
	for _, mem := range rec.Pool.Members {
		if mem.Prefix == prefix {
			continue
		}
		kept = append(kept, mem)
	}
	rec.Pool.Members = kept
	m.pools[poolID] = rec
	m.version[poolID]++
	return nil
}

func (m *Mem) DestroyPool(ctx context.Context, id model.PoolID) error { return m.Delete(ctx, id) }

func (m *Mem) Delete(ctx context.Context, id model.PoolID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pools, id)
	delete(m.version, id)
	return nil
}

func (m *Mem) PoolsForHome(ctx context.Context, edge model.EdgeID) ([]model.Pool, error) {
	return m.poolsBy(edge, true), nil
}

func (m *Mem) PoolsForBackup(ctx context.Context, edge model.EdgeID) ([]model.Pool, error) {
	return m.poolsBy(edge, false), nil
}

func (m *Mem) poolsBy(edge model.EdgeID, primary bool) []model.Pool {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []model.Pool
	for _, r := range m.pools {
		if (primary && r.Primary == edge) || (!primary && r.Backup == edge) {
			p := r.Pool
			p.HomeEdge = r.Primary
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (m *Mem) GetForReconcile(ctx context.Context, id model.PoolID) (PivotRow, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.pools[id]
	if !ok {
		return PivotRow{}, false, nil
	}
	return PivotRow{
		Pool: r.Pool, PoolID: id, Primary: r.Primary, Backup: r.Backup,
		Retiring: r.RetiringEdge, Tokens: r.Tokens, Version: m.version[id],
	}, true, nil
}

func (m *Mem) UpdateCAS(ctx context.Context, row PivotRow, expectVersion int64, newPrimary, newBackup, newRetiring model.EdgeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.version[row.PoolID] != expectVersion {
		return fmt.Errorf("pool %d: %w", row.PoolID, ErrConflict)
	}
	rec, ok := m.pools[row.PoolID]
	if !ok {
		return fmt.Errorf("pool %d: %w", row.PoolID, ErrConflict)
	}
	rec.Primary = newPrimary
	rec.Backup = newBackup
	rec.RetiringEdge = newRetiring
	rec.Pool.HomeEdge = newPrimary
	m.pools[row.PoolID] = rec
	m.version[row.PoolID] = expectVersion + 1
	return nil
}

func (m *Mem) DeleteCAS(ctx context.Context, id model.PoolID, expectVersion int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.version[id] != expectVersion {
		return fmt.Errorf("pool %d: %w", id, ErrConflict)
	}
	delete(m.pools, id)
	delete(m.version, id)
	return nil
}

func (m *Mem) PoolsForDeadEdge(ctx context.Context, edge model.EdgeID) ([]model.PoolID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []model.PoolID
	for id, r := range m.pools {
		if r.Primary == edge || r.Backup == edge {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// ListPivotsByPrimary mirrors Store.ListPivotsByPrimary: the full PivotRow for every
// pool whose PRIMARY is edge (the bulk read the edge-death fast failover uses).
func (m *Mem) ListPivotsByPrimary(ctx context.Context, edge model.EdgeID) ([]PivotRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []PivotRow
	for id, r := range m.pools {
		if r.Primary != edge {
			continue
		}
		out = append(out, PivotRow{
			Pool: r.Pool, PoolID: id, Primary: r.Primary, Backup: r.Backup,
			Retiring: r.RetiringEdge, Tokens: r.Tokens, Version: m.version[id],
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PoolID < out[j].PoolID })
	return out, nil
}

func (m *Mem) List(ctx context.Context) ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Record, 0, len(m.pools))
	for _, r := range m.pools {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Pool.ID < out[j].Pool.ID })
	return out, nil
}

func (m *Mem) ListIDs(ctx context.Context) ([]model.PoolID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]model.PoolID, 0, len(m.pools))
	for id := range m.pools {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (m *Mem) MemberConflicts(ctx context.Context, prefixes []netip.Prefix, excludePool model.PoolID) ([]Conflict, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := map[string]bool{}
	var out []Conflict
	for _, r := range m.pools {
		if r.Pool.ID == excludePool {
			continue
		}
		for _, mem := range r.Pool.Members {
			for _, p := range prefixes {
				if mem.Prefix.Overlaps(p) && !seen[mem.Prefix.String()] {
					seen[mem.Prefix.String()] = true
					out = append(out, Conflict{Prefix: mem.Prefix, PoolID: r.Pool.ID})
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Prefix.String() < out[j].Prefix.String() })
	return out, nil
}
