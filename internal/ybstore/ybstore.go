// Package ybstore is the YugabyteDB-backed pool/member store: the BULK data that
// the hybrid-architecture decision (2026-06) moved OUT of etcd and into a
// horizontally-scalable, PostgreSQL-compatible store. A million-scale load test
// proved etcd chokes (~12 cores) on the create-path WRITE AMPLIFICATION — every
// pool create fanned ~10-20 etcd writes across nonce/index/ledger/srcmap/edgever.
// Yugabyte's sharded-raft scales those writes horizontally, and SQL collapses the
// amplification to ONE ACID transaction: a single INSERT per pool plus one INSERT
// per member, with the schema's PK and UNIQUE constraints doing the anti-replay
// and cross-pool double-claim guards that the etcd nonce key + srcmap CAS used to.
//
// Only the pool/member DATA lives here. COORDINATION stays on etcd: leader
// election, K=2 sharding/coverage, liveness (guard/monitor/ribtap/edgever). The
// admin-layer Timestamp ±window check stays too; ybstore's anti-replay is the
// pools.id PRIMARY KEY (INSERT … ON CONFLICT (id) DO NOTHING) — the SAME create
// (same id) replayed is a 0-row no-op, idempotent by construction.
//
// The schema (already deployed in the lab) is:
//
//	pools(id BIGINT PK, body JSONB, home_edge TEXT, backup_edge TEXT,
//	      cost BIGINT, created_at TIMESTAMPTZ,
//	      version BIGINT NOT NULL DEFAULT 1, retiring BOOLEAN NOT NULL DEFAULT false)
//	  -- pools_by_home(home_edge), pools_by_backup(backup_edge)
//	members(prefix TEXT PK, pool_id BIGINT, home_edge TEXT)
//	  -- members_by_pool(pool_id), members_by_home(home_edge)
//
// The version + retiring columns are the FAILOVER PIVOT that moved off etcd (the
// 2026-06 second hybrid step): version is the optimistic-CAS token that replaces
// etcd's ModRevision (GetForReconcile reads it, UpdateCAS/DeleteCAS bump it under a
// WHERE version=$expect guard so two concurrent coverers can never lost-update the
// pivot — the no-double-failover guarantee), and retiring marks an old primary
// pending withdrawal during an async promote (L-07 "先发新后撤旧"). With these the
// create path writes ZERO etcd and the async reconciler reads/writes the pivot in
// Yugabyte; etcd is left to pure coordination (leader election, sharding, liveness,
// registry). The per-(pool,edge) reservation handle ids the failover paths use are
// DERIVED from (pool_id, edge) — deterministic — so they need no column.
//
// The method set mirrors what the etcd poolstore + srcmap exposed on the create /
// render / drift paths so callers swap cleanly: CreatePool, PoolsForHome,
// PoolsForBackup, Get, Delete, List, MemberConflicts, UsedByEdge — plus the
// version-CAS failover pivot: GetForReconcile, UpdateCAS, DeleteCAS, PoolsForDeadEdge.
package ybstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"sort"
	"strings"

	"github.com/fivetime/sbw-contract/model"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrExists is returned by CreatePool when a pool with that id already exists —
	// the INSERT … ON CONFLICT (id) DO NOTHING affected 0 rows. The pools.id PK IS
	// the anti-replay key: a replayed create (same id) is this idempotent no-op,
	// distinct from a member double-claim. Mirrors poolstore.ErrExists so the
	// orchestrator's existing branch (return the conflict, don't roll back shared
	// reservations) is unchanged.
	ErrExists = errors.New("ybstore: pool already exists")

	// ErrMemberConflict is returned by CreatePool when a member prefix is already
	// claimed by a DIFFERENT pool — a members.prefix UNIQUE (PK) violation
	// (pgerrcode 23505). The whole create transaction rolls back, so a double-claim
	// is rejected at write time, never half-applied. The cross-pool uniqueness that
	// the etcd srcmap CAS used to enforce now lives in the members-table PK.
	ErrMemberConflict = errors.New("ybstore: member already claimed by another pool")

	// ErrNotFound is returned by UpdatePool when the UPDATE … WHERE id=$ matched 0
	// rows — the pool was destroyed between the caller's Get and this write (a lost
	// race). Surfaced (rather than silently no-op'ing the member sync) so the caller
	// can report the pool as gone instead of half-applying an update to nothing.
	ErrNotFound = errors.New("ybstore: pool not found")

	// ErrConflict is returned by UpdateCAS/DeleteCAS when the pool's version column no
	// longer matches the one GetForReconcile returned — another coverer advanced the
	// failover pivot first (the UPDATE/DELETE … WHERE version=$expect matched 0 rows).
	// The async failover reconciler (L-07) treats it as "re-read and retry next pass":
	// the state machine is level-triggered, so losing a version-CAS just means another
	// coverer already took this step. This is the optimistic-concurrency control that
	// replaces the etcd ModRevision CAS — the no-double-failover guard. The
	// version-CAS conflict error now lives here (poolstore no longer exports one).
	ErrConflict = errors.New("ybstore: pool version changed (CAS conflict)")
)

// Record mirrors poolstore.Record's create-path subset: the pool definition plus
// its primary (home) / backup edge assignment and the per-home quota cost. The
// failover pivot lives in THIS file's Yugabyte store too — the RetiringEdge body
// field (below) plus the version/retiring columns drive the async reconciler via
// GetForReconcile/UpdateCAS/DeleteCAS; poolstore is now a DTO/sentinel-only
// package. The JSON of THIS record is what lands in pools.body.
type Record struct {
	Pool    model.Pool   `json:"pool"`
	Primary model.EdgeID `json:"primary"`
	Backup  model.EdgeID `json:"backup,omitempty"`
	Tokens  int64        `json:"tokens"`
	// RetiringEdge mirrors the pools.retiring=true marker's addressable target: the
	// OLD primary an async promote left pending withdrawal (L-07 "先发新后撤旧").
	// Stored in body (not just the retiring boolean column) so a crashed controller
	// rebuilds WHICH edge still owes a withdraw, not merely that one does. Empty in
	// the steady state; retiring is purely the "still owe this edge a withdraw"
	// marker and is NOT a home (renders empty → withdrawn).
	RetiringEdge model.EdgeID `json:"retiring_edge,omitempty"`
}

// PivotRow is a pool's FAILOVER-PIVOT state read from its dedicated columns (NOT
// the JSONB body): the home (primary) and backup edge, the per-home quota cost, the
// retiring marker, and the optimistic-CAS Version. It is what GetForReconcile
// returns and what the async reconciler mutates via UpdateCAS — the Yugabyte
// equivalent of the etcd poolstore.Record + its ModRevision. The pool definition
// (Pool) is unmarshalled from body so the reconciler still has the member list (for
// the egress src claim) without a second read. The reservation-handle ids the
// failover paths use are derived from (PoolID, edge), so they are not stored here.
type PivotRow struct {
	Pool     model.Pool
	PoolID   model.PoolID
	Primary  model.EdgeID
	Backup   model.EdgeID
	Retiring model.EdgeID
	Tokens   int64
	Version  int64
}

// Conflict is one cross-pool member overlap (MemberConflicts result): the
// overlapping member prefix and the pool that holds it. Mirrors the (src,pool)
// shape of srcmap.Record that the admin overlap path consumes.
type Conflict struct {
	Prefix netip.Prefix
	PoolID model.PoolID
}

// Store is the YugabyteDB-backed pool/member store over a pgxpool.Pool.
type Store struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// New builds a Store over an already-constructed pgxpool.Pool (the cmd/controlplane
// owns the pool's lifecycle and Close).
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool, log: slog.Default()} }

// WithLogger sets the logger used to surface malformed-row skips (a dropped pool is
// a blind spot, so it is logged at ERROR with the pool id). nil keeps slog.Default().
func (s *Store) WithLogger(l *slog.Logger) *Store {
	if l != nil {
		s.log = l
	}
	return s
}

// Connect dials Yugabyte at dsn and returns a ready pool (ping-checked). The caller
// owns Close. There is NO hardcoded-DSN fallback: an empty DSN must NOT silently
// connect to a lab DB — main.go FATALS on an empty DSN before reaching here, and
// callers must pass an explicit DSN (yugabyte.dsn / YB_DSN).
// defaultMaxConns caps the pgxpool size when the DSN does not set pool_max_conns.
// pgxpool defaults MaxConns to runtime.NumCPU(); on a many-core host (the lab box has
// 160 cores) each replica then opens ~NumCPU connections and N replicas blow past
// YugabyteDB's ~300-connection ceiling → "too many clients already". 25/replica is
// plenty of write concurrency while staying well under the ceiling for a dozen replicas.
const defaultMaxConns = 25

func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("ybstore: parse dsn %q: %w", dsn, err)
	}
	// Honor an explicit pool_max_conns in the DSN (production tuning); otherwise cap the
	// NumCPU-derived default so a many-core host doesn't exhaust Yugabyte's connections.
	if !strings.Contains(dsn, "pool_max_conns") {
		cfg.MaxConns = defaultMaxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("ybstore: connect %q: %w", dsn, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ybstore: ping %q: %w", dsn, err)
	}
	return pool, nil
}

// CreatePool persists a pool + its members in ONE ACID transaction — the
// create-path write-amplification collapse that motivated the move off etcd:
//
//   - INSERT INTO pools … ON CONFLICT (id) DO NOTHING. If it affects 0 rows the
//     id already exists → ErrExists. The pools.id PRIMARY KEY is the anti-replay
//     key (a replayed create, same id, is this idempotent no-op), replacing the
//     separate etcd nonce key.
//   - For each member, INSERT INTO members(prefix, pool_id, home_edge). A
//     unique_violation (pgerrcode 23505) on prefix means a DIFFERENT pool already
//     claims that source → ErrMemberConflict and the txn ROLLS BACK (no partial
//     claim). This is the srcmap cross-pool double-home guard, enforced by the
//     members.prefix PK instead of an etcd CAS.
//
// Commit only if every statement succeeds. body is the JSON of rec.
func (s *Store) CreatePool(ctx context.Context, rec Record, members []model.Member) error {
	rec.Pool.HomeEdge = rec.Primary // render consistency: HomeEdge == Primary
	body, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("ybstore: marshal pool %d: %w", rec.Pool.ID, err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ybstore: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after a successful Commit

	// version=1, retiring=false: a freshly created pool is at the steady state, its
	// failover pivot un-touched. The async reconciler advances version on every
	// pivot mutation (UpdateCAS), so a crashed controller rebuilds the exact
	// optimistic-CAS token from this persisted column (no etcd ModRevision needed).
	ct, err := tx.Exec(ctx,
		`INSERT INTO pools(id, body, home_edge, backup_edge, cost, version, retiring)
		 VALUES($1, $2, $3, $4, $5, 1, false)
		 ON CONFLICT (id) DO NOTHING`,
		int64(rec.Pool.ID), body, string(rec.Primary), nullEdge(rec.Backup), rec.Tokens,
	)
	if err != nil {
		return fmt.Errorf("ybstore: insert pool %d: %w", rec.Pool.ID, err)
	}
	if ct.RowsAffected() == 0 {
		// ON CONFLICT DO NOTHING fired: the id is taken (idempotent replay).
		return fmt.Errorf("pool %d: %w", rec.Pool.ID, ErrExists)
	}

	for _, m := range members {
		_, err := tx.Exec(ctx,
			`INSERT INTO members(prefix, pool_id, home_edge) VALUES($1, $2, $3)`,
			m.Prefix.String(), int64(rec.Pool.ID), string(rec.Primary),
		)
		if err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("member %s: %w", m.Prefix, ErrMemberConflict)
			}
			return fmt.Errorf("ybstore: insert member %s (pool %d): %w", m.Prefix, rec.Pool.ID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("ybstore: commit pool %d: %w", rec.Pool.ID, err)
	}
	return nil
}

// UpdatePool persists an IN-PLACE pool change (the BSS update path: add/remove
// members, modify rates/action) in ONE ACID transaction — the member-mutation
// analog of CreatePool's write-amplification collapse. The home PLACEMENT is
// unchanged (failover/migration is the pivot path, not this), so home_edge /
// backup_edge / cost are NOT touched here; only the JSONB body and the members
// table move, and the optimistic-CAS version is bumped so a concurrent reconcile
// re-reads:
//
//   - UPDATE pools SET body=$(new), version=version+1 WHERE id=$id. 0 rows ⇒ the
//     pool was destroyed out from under the update → ErrExists-free "not found"
//     is reported by the caller's prior Get; here a 0-row update is a lost race we
//     surface as ErrNotFound so the caller does not silently no-op a member sync.
//   - SYNC the members table to the new set: DELETE this pool's members whose
//     prefix is NOT in the new set, then INSERT the added ones. An INSERT that hits
//     the members.prefix PRIMARY KEY held by a DIFFERENT pool is a cross-pool
//     double-claim → ErrMemberConflict and the WHOLE txn ROLLS BACK (mirrors
//     CreatePool's member guard; no partial member sync). members.home_edge is the
//     pool's current primary (rec.Primary), keeping render's src→home truth aligned.
//
// rec carries the UPDATED pool definition + its (unchanged) primary/backup/tokens;
// its JSON becomes the new pools.body. Commit only if every statement succeeds.
func (s *Store) UpdatePool(ctx context.Context, rec Record, members []model.Member) error {
	rec.Pool.HomeEdge = rec.Primary // render consistency: HomeEdge == Primary
	body, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("ybstore: marshal pool %d: %w", rec.Pool.ID, err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ybstore: begin update: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after a successful Commit

	// Bump version in the SAME statement that rewrites body so an in-flight reconcile
	// CAS (which reads version) is forced to re-read, never lost-updating the body.
	// home_edge/backup_edge/cost stay put: an in-place update does not move placement.
	ct, err := tx.Exec(ctx,
		`UPDATE pools SET body=$1, version=version+1 WHERE id=$2`,
		body, int64(rec.Pool.ID),
	)
	if err != nil {
		return fmt.Errorf("ybstore: update pool %d: %w", rec.Pool.ID, err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("pool %d: %w", rec.Pool.ID, ErrNotFound)
	}

	// Sync members to the new set via an INCREMENTAL DIFF. The old path did a prune
	// DELETE plus a per-member INSERT+recheck loop = ~2K round-trips for a K-member
	// pool every PUT (the member-scaling rate sank as pools grew). Now: read this pool's
	// current prefixes once, diff against the new set in Go, then DELETE only the removed
	// and batch-INSERT only the added — O(added+removed) work, a fixed statement count.
	rows, err := tx.Query(ctx, `SELECT prefix FROM members WHERE pool_id=$1`, int64(rec.Pool.ID))
	if err != nil {
		return fmt.Errorf("ybstore: read members pool %d: %w", rec.Pool.ID, err)
	}
	cur := make(map[string]struct{})
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return fmt.Errorf("ybstore: scan member pool %d: %w", rec.Pool.ID, err)
		}
		cur[p] = struct{}{}
	}
	rows.Close() // free the tx connection before the next Exec
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ybstore: read members pool %d: %w", rec.Pool.ID, err)
	}
	newSet := make(map[string]struct{}, len(members))
	for _, m := range members {
		newSet[m.Prefix.String()] = struct{}{}
	}
	added := make([]string, 0)
	for p := range newSet { // from the deduped set, so `added` has no duplicates
		if _, ok := cur[p]; !ok {
			added = append(added, p)
		}
	}
	removed := make([]string, 0)
	for p := range cur {
		if _, ok := newSet[p]; !ok {
			removed = append(removed, p)
		}
	}
	// DELETE only the removed; unchanged members stay so their data-plane claim never blips.
	if len(removed) > 0 {
		if _, err := tx.Exec(ctx,
			`DELETE FROM members WHERE pool_id=$1 AND prefix = ANY($2)`,
			int64(rec.Pool.ID), removed,
		); err != nil {
			return fmt.Errorf("ybstore: prune members pool %d: %w", rec.Pool.ID, err)
		}
	}
	// Batch-INSERT the added in ONE statement. Every `added` prefix is absent from THIS
	// pool's current set, so an ON CONFLICT (DO NOTHING) can only mean the prefix is held
	// by ANOTHER pool — a cross-pool double-claim. Hence rows-inserted < len(added) ⇒ a
	// foreign collision ⇒ ErrMemberConflict and the whole txn rolls back (the srcmap guard).
	if len(added) > 0 {
		ct, err := tx.Exec(ctx,
			`INSERT INTO members(prefix, pool_id, home_edge)
			 SELECT unnest($1::text[]), $2, $3
			 ON CONFLICT (prefix) DO NOTHING`,
			added, int64(rec.Pool.ID), string(rec.Primary),
		)
		if err != nil {
			return fmt.Errorf("ybstore: insert members pool %d: %w", rec.Pool.ID, err)
		}
		if int(ct.RowsAffected()) < len(added) {
			return fmt.Errorf("pool %d members: %w", rec.Pool.ID, ErrMemberConflict)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("ybstore: commit update pool %d: %w", rec.Pool.ID, err)
	}
	return nil
}

// RemoveMember drops a SINGLE member prefix from a pool and bumps the pool version,
// in ONE transaction — the eviction/replace path's data write. The members-table
// DELETE is gated on pool_id=$poolID so it removes the row ONLY if THIS pool holds
// it (a member that moved to another pool, or was never here, is left untouched —
// idempotent no-op). The pools.body member list is rewritten in the SAME txn (the
// prefix is filtered out of body.pool.members) so a render — which reads body — no
// longer carries the evicted member. The pool row itself is KEPT even if this empties
// it (an empty pool is dormant, not destroyed). home/backup/cost are untouched —
// member count does not affect the quota.
func (s *Store) RemoveMember(ctx context.Context, poolID model.PoolID, prefix netip.Prefix) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ybstore: begin remove-member: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx,
		`DELETE FROM members WHERE prefix=$1 AND pool_id=$2`,
		prefix.String(), int64(poolID),
	); err != nil {
		return fmt.Errorf("ybstore: remove member %s (pool %d): %w", prefix, poolID, err)
	}

	// Rewrite body.pool.members with the prefix filtered out, and bump version, in one
	// UPDATE. Read-modify-write the JSONB in Go (the member element shape is the
	// model.Member JSON, not a flat string) so render reads stay exact. A 0-row pool
	// (destroyed concurrently) is reported as ErrNotFound.
	var body []byte
	if err := tx.QueryRow(ctx, `SELECT body FROM pools WHERE id=$1`, int64(poolID)).Scan(&body); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("pool %d: %w", poolID, ErrNotFound)
		}
		return fmt.Errorf("ybstore: read body pool %d: %w", poolID, err)
	}
	var rec Record
	if err := json.Unmarshal(body, &rec); err != nil {
		return fmt.Errorf("ybstore: unmarshal body pool %d: %w", poolID, err)
	}
	kept := rec.Pool.Members[:0:0]
	for _, m := range rec.Pool.Members {
		if m.Prefix == prefix {
			continue
		}
		kept = append(kept, m)
	}
	rec.Pool.Members = kept
	newBody, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("ybstore: marshal body pool %d: %w", poolID, err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE pools SET body=$1, version=version+1 WHERE id=$2`, newBody, int64(poolID),
	); err != nil {
		return fmt.Errorf("ybstore: update body pool %d: %w", poolID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("ybstore: commit remove-member pool %d: %w", poolID, err)
	}
	return nil
}

// PoolsForHome returns the pools edge is PRIMARY for (rendered FULL), unmarshalled
// from pools.body. Served by the pools_by_home secondary index — O(pools on edge),
// not a full scan. Replaces the etcd poolstore's per-home lookup.
func (s *Store) PoolsForHome(ctx context.Context, edge model.EdgeID) ([]model.Pool, error) {
	return s.poolsBy(ctx, "home_edge", edge)
}

// PoolsForBackup returns the pools edge is BACKUP for (rendered STANDBY), via the
// pools_by_backup index. Replaces the etcd poolstore's per-backup lookup.
func (s *Store) PoolsForBackup(ctx context.Context, edge model.EdgeID) ([]model.Pool, error) {
	return s.poolsBy(ctx, "backup_edge", edge)
}

// ListPivotsByPrimary returns the full PivotRow for every pool whose PRIMARY
// (home_edge) is edge — the BULK read the edge-death fast failover uses to re-home
// all of a dead edge's pools at once (parallel UpdateCAS + a coalesced edgever bump),
// instead of one level-triggered ReconcilePool per pool (per-pool UpdateCAS + a
// per-pool bump on the SAME new-primary key → CAS-conflict storm). One query.
func (s *Store) ListPivotsByPrimary(ctx context.Context, edge model.EdgeID) ([]PivotRow, error) {
	return followerRead(ctx, s, "list-pivots-by-primary", func(tx pgx.Tx) ([]PivotRow, error) {
		rows, err := tx.Query(ctx,
			`SELECT id, body, home_edge, backup_edge, cost, version FROM pools WHERE home_edge = $1`,
			string(edge))
		if err != nil {
			return nil, fmt.Errorf("ybstore: list-pivots-by-primary %s: %w", edge, err)
		}
		defer rows.Close()
		var out []PivotRow
		for rows.Next() {
			var (
				id      int64
				body    []byte
				home    string
				backup  *string
				cost    int64
				version int64
			)
			if err := rows.Scan(&id, &body, &home, &backup, &cost, &version); err != nil {
				return nil, fmt.Errorf("ybstore: scan pivot: %w", err)
			}
			var rec Record
			if err := json.Unmarshal(body, &rec); err != nil {
				s.log.Error("ybstore: skipping malformed pivot row", "pool", id, "err", err)
				continue
			}
			row := PivotRow{Pool: rec.Pool, PoolID: model.PoolID(id), Primary: model.EdgeID(home), Tokens: cost, Version: version}
			if backup != nil {
				row.Backup = model.EdgeID(*backup)
			}
			out = append(out, row)
		}
		return out, rows.Err()
	})
}

// poolsBy runs SELECT body FROM pools WHERE <col>=$1 and unmarshals each row's stored
// Record body to its model.Pool. col is a fixed identifier (home_edge / backup_edge),
// never user input. Runs as a follower read: at 5.9M members this per-edge scan tripped
// read-restart on every retry and starved the render (DESIGN §9.1).
func (s *Store) poolsBy(ctx context.Context, col string, edge model.EdgeID) ([]model.Pool, error) {
	return followerRead(ctx, s, "pools by "+col, func(tx pgx.Tx) ([]model.Pool, error) {
		rows, err := tx.Query(ctx, `SELECT id, body FROM pools WHERE `+col+` = $1`, string(edge))
		if err != nil {
			return nil, fmt.Errorf("ybstore: pools by %s=%s: %w", col, edge, err)
		}
		defer rows.Close()

		var out []model.Pool
		for rows.Next() {
			var id int64
			var body []byte
			if err := rows.Scan(&id, &body); err != nil {
				return nil, fmt.Errorf("ybstore: scan pool body: %w", err)
			}
			var rec Record
			if err := json.Unmarshal(body, &rec); err != nil {
				// A dropped pool is a blind spot (it silently vanishes from the render);
				// log LOUD with the pool id before skipping rather than fail the whole render.
				s.log.Error("ybstore: skipping malformed pool row", "pool", id, "by", col, "err", err)
				continue
			}
			out = append(out, rec.Pool)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("ybstore: pools by %s rows: %w", col, err)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
		return out, nil
	})
}

// Get returns a pool's Record; ok=false if no such pool. Keeps the (rec, ok, err)
// shape (over the create-path subset) that the etcd poolstore's Get used to have.
func (s *Store) Get(ctx context.Context, id model.PoolID) (Record, bool, error) {
	var body []byte
	err := s.pool.QueryRow(ctx, `SELECT body FROM pools WHERE id = $1`, int64(id)).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, fmt.Errorf("ybstore: get pool %d: %w", id, err)
	}
	var rec Record
	if err := json.Unmarshal(body, &rec); err != nil {
		return Record{}, false, fmt.Errorf("ybstore: unmarshal pool %d: %w", id, err)
	}
	return rec, true, nil
}

// GetForReconcile returns a pool's FAILOVER-PIVOT row (home/backup/retiring + its
// optimistic-CAS Version) plus the pool definition — the Yugabyte equivalent of the
// etcd poolstore.GetRev the async reconciler (L-07) used. ok=false if no such pool
// (destroyed / torn down). The caller decides ONE state transition from the row and
// writes it back with UpdateCAS/DeleteCAS gated on Version, so two coverers
// reconciling the same pool serialize on the version column (no double-failover).
func (s *Store) GetForReconcile(ctx context.Context, id model.PoolID) (PivotRow, bool, error) {
	var (
		body    []byte
		home    string
		backup  *string
		cost    int64
		version int64
		retire  bool
	)
	err := s.pool.QueryRow(ctx,
		`SELECT body, home_edge, backup_edge, cost, version, retiring FROM pools WHERE id = $1`,
		int64(id),
	).Scan(&body, &home, &backup, &cost, &version, &retire)
	if errors.Is(err, pgx.ErrNoRows) {
		return PivotRow{}, false, nil
	}
	if err != nil {
		return PivotRow{}, false, fmt.Errorf("ybstore: get-for-reconcile pool %d: %w", id, err)
	}
	var rec Record
	if err := json.Unmarshal(body, &rec); err != nil {
		return PivotRow{}, false, fmt.Errorf("ybstore: unmarshal pivot pool %d: %w", id, err)
	}
	row := PivotRow{
		Pool:    rec.Pool,
		PoolID:  id,
		Primary: model.EdgeID(home),
		Tokens:  cost,
		Version: version,
	}
	if backup != nil {
		row.Backup = model.EdgeID(*backup)
	}
	// retiring=true encodes "an old primary is pending withdrawal". The retiring EDGE
	// itself is a real top-level body field, Record.RetiringEdge (json "retiring_edge"),
	// set by UpdateCAS at promote time and stored in body alongside the pool so it stays
	// addressable across a crash — it is read directly here from rec.RetiringEdge, NOT
	// re-derived by the reconciler.
	if retire {
		row.Retiring = rec.RetiringEdge
	}
	return row, true, nil
}

// UpdateCAS is the version-CAS failover-pivot write — the Yugabyte equivalent of the
// etcd ModRevision CAS the removed poolstore used. It applies ONE reconcile step (new home/backup/retiring)
// ONLY if the pool's version is still expectVersion, bumping version by 1 in the
// SAME statement:
//
//	UPDATE pools SET home_edge=$, backup_edge=$, retiring=$, version=version+1,
//	                 body=$(updated)
//	  WHERE id=$id AND version=$expectVersion
//
// 0 rows affected ⇒ another coverer advanced the pivot first ⇒ ErrConflict (the
// reconciler re-reads + retries next pass). This is the no-double-failover guard:
// two concurrent coverers can never both promote, because only one matches the
// version. The members.home_edge re-home runs in the SAME transaction (so a render
// of the new home from the members table is consistent with the pool row), keeping
// the data-plane source-of-truth atomic with the pivot move. The retiring EDGE is
// carried in body (RetiringEdge) so it survives a crash addressably; an empty
// newRetiring clears it. reHomeMembers re-homes members.home_edge to newPrimary when
// the primary actually changed (a promote), else leaves them (a backup-only change).
func (s *Store) UpdateCAS(ctx context.Context, row PivotRow, expectVersion int64, newPrimary, newBackup, newRetiring model.EdgeID) error {
	// Rebuild the JSONB body so its Pool.HomeEdge, Backup and RetiringEdge track the
	// pivot move (the render reads home_edge/backup_edge columns, but body is the
	// authoritative Record and GetForReconcile/Get read RetiringEdge from it).
	rec := Record{Pool: row.Pool, Primary: newPrimary, Backup: newBackup, Tokens: row.Tokens, RetiringEdge: newRetiring}
	rec.Pool.HomeEdge = newPrimary
	body, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("ybstore: marshal pivot pool %d: %w", row.PoolID, err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ybstore: begin update-cas: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	ct, err := tx.Exec(ctx,
		`UPDATE pools
		    SET home_edge=$1, backup_edge=$2, retiring=$3, body=$4, version=version+1
		  WHERE id=$5 AND version=$6`,
		string(newPrimary), nullEdge(newBackup), newRetiring != "", body, int64(row.PoolID), expectVersion,
	)
	if err != nil {
		return fmt.Errorf("ybstore: update-cas pool %d: %w", row.PoolID, err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("pool %d: %w", row.PoolID, ErrConflict)
	}
	// Re-home members to the new primary IN THE SAME TXN when the primary changed (a
	// promote). The members table is the data-plane src→home truth; keeping it atomic
	// with the pivot move means a render of either home is never split.
	if newPrimary != row.Primary && newPrimary != "" {
		if _, err := tx.Exec(ctx,
			`UPDATE members SET home_edge=$1 WHERE pool_id=$2`,
			string(newPrimary), int64(row.PoolID),
		); err != nil {
			return fmt.Errorf("ybstore: re-home members pool %d: %w", row.PoolID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("ybstore: commit update-cas pool %d: %w", row.PoolID, err)
	}
	return nil
}

// DeleteCAS removes a pool + its members ONLY if its version is still expectVersion,
// returning ErrConflict otherwise — the Yugabyte equivalent of the etcd
// ModRevision-gated delete the removed poolstore used.
// The async double-death tear-down uses it so a concurrent successful re-home (which
// advanced the version) wins and the destructive delete no-ops (L-07 hole 6).
func (s *Store) DeleteCAS(ctx context.Context, id model.PoolID, expectVersion int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ybstore: begin delete-cas: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	ct, err := tx.Exec(ctx, `DELETE FROM pools WHERE id=$1 AND version=$2`, int64(id), expectVersion)
	if err != nil {
		return fmt.Errorf("ybstore: delete-cas pool %d: %w", id, err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("pool %d: %w", id, ErrConflict)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM members WHERE pool_id=$1`, int64(id)); err != nil {
		return fmt.Errorf("ybstore: delete-cas members pool %d: %w", id, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("ybstore: commit delete-cas pool %d: %w", id, err)
	}
	return nil
}

// PoolsForDeadEdge returns the ids of pools whose HOME (primary) or BACKUP edge is
// `edge` — the pools to enqueue for the level-triggered reconciler when that edge
// dies/decommissions. Served by the pools_by_home / pools_by_backup indexes.
func (s *Store) PoolsForDeadEdge(ctx context.Context, edge model.EdgeID) ([]model.PoolID, error) {
	out, err := followerRead(ctx, s, "pools-for-dead-edge", func(tx pgx.Tx) ([]model.PoolID, error) {
		rows, err := tx.Query(ctx,
			`SELECT id FROM pools WHERE home_edge=$1 OR backup_edge=$1`, string(edge))
		if err != nil {
			return nil, fmt.Errorf("ybstore: pools-for-dead-edge %s: %w", edge, err)
		}
		defer rows.Close()
		var out []model.PoolID
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return nil, fmt.Errorf("ybstore: scan dead-edge id: %w", err)
			}
			out = append(out, model.PoolID(id))
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("ybstore: pools-for-dead-edge rows: %w", err)
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// ListIDs returns every pool id, sorted — the level-triggered reconcile sweep's
// backstop enumeration (replaces the etcd poolstore.List the sweep used).
func (s *Store) ListIDs(ctx context.Context) ([]model.PoolID, error) {
	out, err := followerRead(ctx, s, "list-ids", func(tx pgx.Tx) ([]model.PoolID, error) {
		rows, err := tx.Query(ctx, `SELECT id FROM pools`)
		if err != nil {
			return nil, fmt.Errorf("ybstore: list ids: %w", err)
		}
		defer rows.Close()
		var out []model.PoolID
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return nil, fmt.Errorf("ybstore: scan id: %w", err)
			}
			out = append(out, model.PoolID(id))
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("ybstore: list ids rows: %w", err)
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// DestroyPool tears a pool down — deletes the pool row AND all of its members in
// ONE ACID transaction — the admin DELETE /v1/pools/{id} data write on the hybrid
// (yb-wired) path. It is the destroy-lifecycle analog of CreatePool's one-txn
// collapse: the members are the srcmap claims' analog, so dropping the pool drops
// every claim atomically with it, replacing the etcd poolstore.Delete + per-member
// srcmap.Release the legacy path issued. Idempotent: an absent pool (already
// destroyed, or a retried destroy) is a 0-row no-op, not an error — the orchestrator's
// destroy is retried until every home's withdraw lands, so the data delete must be
// safe to repeat. Unlike DeleteCAS it is UNCONDITIONAL (no version gate): the admin
// destroy is the authoritative operator intent, not a failover-race step.
func (s *Store) DestroyPool(ctx context.Context, id model.PoolID) error {
	return s.Delete(ctx, id)
}

// Delete removes a pool and its members in ONE transaction (members first, then
// the pool — the inverse of CreatePool). Idempotent: an absent pool is a no-op.
func (s *Store) Delete(ctx context.Context, id model.PoolID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ybstore: begin delete: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, `DELETE FROM members WHERE pool_id = $1`, int64(id)); err != nil {
		return fmt.Errorf("ybstore: delete members of pool %d: %w", id, err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM pools WHERE id = $1`, int64(id)); err != nil {
		return fmt.Errorf("ybstore: delete pool %d: %w", id, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("ybstore: commit delete pool %d: %w", id, err)
	}
	return nil
}

// List returns every pool Record, sorted by id. Replaces the etcd poolstore's List.
func (s *Store) List(ctx context.Context) ([]Record, error) {
	return followerRead(ctx, s, "list", func(tx pgx.Tx) ([]Record, error) {
		rows, err := tx.Query(ctx, `SELECT id, body FROM pools`)
		if err != nil {
			return nil, fmt.Errorf("ybstore: list: %w", err)
		}
		defer rows.Close()

		var out []Record
		for rows.Next() {
			var id int64
			var body []byte
			if err := rows.Scan(&id, &body); err != nil {
				return nil, fmt.Errorf("ybstore: scan list body: %w", err)
			}
			var rec Record
			if err := json.Unmarshal(body, &rec); err != nil {
				// A dropped pool is a blind spot; log LOUD with the pool id before skipping.
				s.log.Error("ybstore: skipping malformed pool row in List", "pool", id, "err", err)
				continue
			}
			out = append(out, rec)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("ybstore: list rows: %w", err)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Pool.ID < out[j].Pool.ID })
		return out, nil
	})
}

// MemberConflicts returns, for the given prefixes, the existing member records
// that OVERLAP one of them (CIDR containment either way / equal) and belong to a
// pool OTHER than excludePool — the CIDR-aware cross-pool overlap set, mirroring
// srcmap.Conflicts semantics. Member granularity is fixed to {v4 /32, v4 /24,
// v6 /128}, so the complete overlap set is reachable from a few CANDIDATE keys:
//
//   - /32 → its exact prefix + its single covering /24
//   - /24 → itself + every /32 inside it (the /24's keyspace)
//   - /128 → its exact prefix (v6 has no sub-/128 granularity)
//
// The candidate set is collected, then matched in ONE round trip with
// `prefix = ANY($1)` (members.prefix is the PK, so each is an index point-get).
// For a /24 the contained /32s are not enumerable as exact keys, so that case adds
// a bounded LIKE scan of the "a.b.c." keyspace. Each returned row is re-checked
// with Overlaps so the set is provably identical to a containment scan.
func (s *Store) MemberConflicts(ctx context.Context, prefixes []netip.Prefix, excludePool model.PoolID) ([]Conflict, error) {
	cand := make(map[string]struct{})
	var likePrefixes []string // /24 keyspaces ("a.b.c.") to LIKE-scan
	for _, p := range prefixes {
		cand[p.String()] = struct{}{}
		switch {
		case p.Addr().Is4() && p.Bits() == 32:
			cand[netip.PrefixFrom(p.Addr(), 24).Masked().String()] = struct{}{}
		case p.Addr().Is4() && p.Bits() == 24:
			a := p.Addr().As4()
			likePrefixes = append(likePrefixes, fmt.Sprintf("%d.%d.%d.", a[0], a[1], a[2]))
		}
	}
	keys := make([]string, 0, len(cand))
	for k := range cand {
		keys = append(keys, k)
	}

	seen := make(map[string]struct{})
	var out []Conflict
	add := func(prefixStr string, poolID model.PoolID) error {
		if poolID == excludePool {
			return nil
		}
		if _, dup := seen[prefixStr]; dup {
			return nil
		}
		mp, err := netip.ParsePrefix(prefixStr)
		if err != nil {
			return nil // malformed stored prefix: skip
		}
		// Re-verify CIDR overlap against the candidate inputs (proves equivalence to
		// a full containment scan).
		overlaps := false
		for _, p := range prefixes {
			if mp.Overlaps(p) {
				overlaps = true
				break
			}
		}
		if !overlaps {
			return nil
		}
		seen[prefixStr] = struct{}{}
		out = append(out, Conflict{Prefix: mp, PoolID: poolID})
		return nil
	}

	if len(keys) > 0 {
		rows, err := s.pool.Query(ctx,
			`SELECT prefix, pool_id FROM members WHERE prefix = ANY($1)`, keys)
		if err != nil {
			return nil, fmt.Errorf("ybstore: member conflicts ANY: %w", err)
		}
		if err := scanConflicts(rows, add); err != nil {
			return nil, err
		}
	}

	for _, lp := range likePrefixes {
		// A new /24 overlaps any /32 inside it; LIKE-scan that /24's keyspace only.
		// The '_' / '%' wildcards in the literal are escaped (prefixes are numeric
		// dotted-quad text, so this only ever matches "a.b.c.<host>/32").
		rows, err := s.pool.Query(ctx,
			`SELECT prefix, pool_id FROM members WHERE prefix LIKE $1`, lp+"%")
		if err != nil {
			return nil, fmt.Errorf("ybstore: member conflicts LIKE: %w", err)
		}
		if err := scanConflicts(rows, add); err != nil {
			return nil, err
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Prefix.String() < out[j].Prefix.String() })
	return out, nil
}

func scanConflicts(rows pgx.Rows, add func(string, model.PoolID) error) error {
	defer rows.Close()
	for rows.Next() {
		var prefix string
		var poolID int64
		if err := rows.Scan(&prefix, &poolID); err != nil {
			return fmt.Errorf("ybstore: scan conflict: %w", err)
		}
		if err := add(prefix, model.PoolID(poolID)); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ybstore: conflict rows: %w", err)
	}
	return nil
}

// UsedByEdge returns each edge's committed per-home capacity in use — the SUM of
// cost over the pools it is PRIMARY (home) for — for placement capacity. One
// grouped aggregate, so the placement cache can refresh the whole map in a single
// query instead of probing the ledger per edge.
func (s *Store) UsedByEdge(ctx context.Context) (map[model.EdgeID]int64, error) {
	return followerRead(ctx, s, "used-by-edge", func(tx pgx.Tx) (map[model.EdgeID]int64, error) {
		rows, err := tx.Query(ctx,
			`SELECT home_edge, COALESCE(SUM(cost), 0) FROM pools GROUP BY home_edge`)
		if err != nil {
			return nil, fmt.Errorf("ybstore: used-by-edge: %w", err)
		}
		defer rows.Close()

		out := make(map[model.EdgeID]int64)
		for rows.Next() {
			var edge string
			var used int64
			if err := rows.Scan(&edge, &used); err != nil {
				return nil, fmt.Errorf("ybstore: scan used-by-edge: %w", err)
			}
			out[model.EdgeID(edge)] = used
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("ybstore: used-by-edge rows: %w", err)
		}
		return out, nil
	})
}

// nullEdge maps an empty backup edge to a NULL backup_edge (so the
// pools_by_backup index never indexes a placeholder ""), else the string.
func nullEdge(e model.EdgeID) any {
	if e == "" {
		return nil
	}
	return string(e)
}

// isUniqueViolation reports whether err is a PostgreSQL unique_violation (23505) —
// a members.prefix PK collision, i.e. a cross-pool double-claim.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation
}

// maxReadRetries bounds the re-reads on a Yugabyte read-restart (a rare backstop now
// that the bulk scans run as follower reads — see followerRead).
const maxReadRetries = 5

// followerStaleMs is the follower-read staleness for the bulk scans: the read is pinned
// to a snapshot this many ms in the PAST, so it cannot conflict with concurrent writes
// and a large scan never trips "Restart read required". Must exceed the cluster max clock
// skew (default ~500ms). The staleness is fine for the render/reconcile/metrics bulk
// reads — they are level-triggered and re-run; the failover PIVOT uses the fresh
// single-row GetForReconcile, NOT these scans.
const followerStaleMs = 10000

// isReadRestart reports whether err is a YugabyteDB/Postgres serialization failure
// (40001, "Restart read required"). A long index scan can hit it under concurrent
// writes, and once rows have streamed the query layer can't retry mid-flight ("query
// layer retry isn't possible because data was already transferred"), so the caller
// must re-run the WHOLE read. Seen at 350K: PoolsForBackup(l2) failed 122× during the
// converge write storm; at 5.9M it failed ALL retries and starved the render, cascading
// into a false-failover storm (DESIGN §9.1). followerRead is the fix; this stays as a
// cheap backstop for the (now rare) restart.
func isReadRestart(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.SerializationFailure
}

// followerRead runs fn inside a READ ONLY transaction pinned to a stale follower-read
// snapshot (followerStaleMs). The fixed past read point cannot conflict with concurrent
// writes, so a large per-edge / full-table scan never trips YugabyteDB "Restart read
// required" (40001) — the failure that starved the render and cascaded into a
// false-failover storm at 5.9M members (DESIGN §9.1). isReadRestart is kept as a cheap
// backstop. Use ONLY for bulk, staleness-tolerant reads; single-row pivot reads
// (GetForReconcile) must stay fresh/leader (never wrap those).
func followerRead[T any](ctx context.Context, s *Store, what string, fn func(pgx.Tx) (T, error)) (T, error) {
	var out T
	var err error
	for attempt := 0; attempt <= maxReadRetries; attempt++ {
		out, err = followerReadOnce(ctx, s, fn)
		if err == nil || !isReadRestart(err) {
			return out, err
		}
		s.log.Warn("ybstore: read restart under follower read, retrying", "query", what, "attempt", attempt+1)
	}
	return out, err
}

// followerReadOnce is one attempt of followerRead: open a READ ONLY txn, enable follower
// reads with the staleness bound, run fn, commit.
func followerReadOnce[T any](ctx context.Context, s *Store, fn func(pgx.Tx) (T, error)) (T, error) {
	var zero T
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return zero, fmt.Errorf("ybstore: begin follower read: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `SET LOCAL yb_read_from_followers = true`); err != nil {
		return zero, fmt.Errorf("ybstore: enable follower read: %w", err)
	}
	// followerStaleMs is a compile-time int const → safe to inline (SET LOCAL takes no $-params).
	if _, err := tx.Exec(ctx, fmt.Sprintf(`SET LOCAL yb_follower_read_staleness_ms = %d`, followerStaleMs)); err != nil {
		return zero, fmt.Errorf("ybstore: set follower staleness: %w", err)
	}
	out, err := fn(tx)
	if err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, fmt.Errorf("ybstore: commit follower read: %w", err)
	}
	return out, nil
}
