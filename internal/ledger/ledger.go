// Package ledger is the controller's etcd 配额令牌账本 (controller §4.3): the
// fast decision ledger for bin-packing. Each agent has a token balance (its
// sellable bandwidth, capacity×90%), seeded on first registration (InitAgent) and
// read back as Remaining for account reconciliation.
//
// The HUNG TOKEN problem it was built for — controller crashing between reserve
// and commit, debiting tokens for a pool that never materializes — can no longer
// occur through this package: pool-create is now a single Yugabyte ACID txn
// (orchestrator.CreatePool -> ybstore.Store.CreatePool) that reserves nothing in
// the etcd ledger, and the Reserve/Commit/Return methods that produced reservation
// records were deleted in the Yugabyte migration. The reservation record type and
// the background reclaim sweep (Reclaim: PREFIX-SCAN the resv/ records, return the
// expired-still-reserved ones — the Kubernetes reconcile-loop pattern, "expired"
// meaning "reclaimable" not "deleted") remain, but no production writer creates a
// reservation under resv/ anymore, so the sweep finds nothing to reclaim.
//
// What this package still does in production is the token-balance accounting:
// InitAgent / SetTokens / Remaining (and the Reclaim sweep over the now-unwritten
// reservation records).
//
// Token mutations are etcd transactions with version CAS (read-modify-write, retry
// on conflict): atomic and idempotent (state-checked, so retries and the
// stateless-multi-replica reclaim job never double-return).
package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// State is a reservation's lifecycle state.
type State string

const (
	StateReserved State = "reserved"
)

// tokenShards is the number of shards each agent's token balance is split across
// (the hot-counter pattern: §4.3). The balance is split K ways so the seed/read
// fan out across keys instead of serializing on one hot counter. The agent's
// available balance is the SUM over its shards. K is a power of two. The original
// justification — concurrent reserves picking different shards and CAS-ing in
// parallel — no longer applies (Reserve was deleted in the Yugabyte migration; the
// remaining writers are InitAgent's one-time seed, SetTokens, and the Reclaim
// refund); the split is now retained for the seed/read fan-out only.
const tokenShards = 64

// Reservation is one reserve record (persisted as JSON, no lease).
type Reservation struct {
	Agent     string `json:"agent"`
	Amount    int64  `json:"amount"`
	State     State  `json:"state"`
	Pool      string `json:"pool"`
	CreatedAt int64  `json:"created_at_ms"`
	ExpireAt  int64  `json:"expire_at_ms"`
	// Shard is the token shard this reservation debited (so Reclaim refunds the
	// SAME shard the amount was taken from — keeping every shard's running total =
	// its seeded slice − its outstanding reservations).
	Shard int `json:"shard"`
}

// Ledger is the token/reservation store over an etcd KV.
type Ledger struct {
	kv     clientv3.KV
	prefix string
	ttl    time.Duration
	now    func() time.Time
}

// Option configures a Ledger.
type Option func(*Ledger)

// WithClock overrides the time source (tests).
func WithClock(now func() time.Time) Option { return func(l *Ledger) { l.now = now } }

// New builds a ledger over kv, keying everything under prefix (e.g. "sbw/").
// ttl is how long a reservation may sit uncommitted before the reclaim job
// returns it.
func New(kv clientv3.KV, prefix string, ttl time.Duration, opts ...Option) *Ledger {
	l := &Ledger{kv: kv, prefix: prefix, ttl: ttl, now: time.Now}
	for _, o := range opts {
		o(l)
	}
	return l
}

// tokPrefix is the prefix under which all of agent's shard counters live:
// "<prefix>tok/<agent>/". The shard keys are tokPrefix + shardIndex.
func (l *Ledger) tokPrefix(agent string) string { return l.prefix + "tok/" + agent + "/" }
func (l *Ledger) tokShard(agent string, s int) string {
	return l.tokPrefix(agent) + strconv.Itoa(s)
}
func (l *Ledger) resvPrefix() string { return l.prefix + "resv/" }

// splitTokens divides tokens evenly across the K shards, putting the remainder on
// shard 0 so the slices always sum back to exactly tokens.
func splitTokens(tokens int64) [tokenShards]int64 {
	var shards [tokenShards]int64
	base := tokens / tokenShards
	rem := tokens % tokenShards
	for i := range shards {
		shards[i] = base
	}
	shards[0] += rem
	return shards
}

// putShardsOps builds the Put ops that seed agent's K shard counters from an
// even split of tokens.
func (l *Ledger) putShardsOps(agent string, tokens int64) []clientv3.Op {
	shards := splitTokens(tokens)
	ops := make([]clientv3.Op, tokenShards)
	for i := 0; i < tokenShards; i++ {
		ops[i] = clientv3.OpPut(l.tokShard(agent, i), strconv.FormatInt(shards[i], 10))
	}
	return ops
}

// InitAgent sets an agent's balance to tokens on FIRST registration only (a
// re-register/restart must not reset a balance with committed allocations).
// Returns true if it initialized, false if a balance already existed. The
// balance is split across K shards; shard 0's existence flags "initialized".
func (l *Ledger) InitAgent(ctx context.Context, agent string, tokens int64) (bool, error) {
	tr, err := l.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(l.tokShard(agent, 0)), "=", 0)).
		Then(l.putShardsOps(agent, tokens)...).
		Commit()
	if err != nil {
		return false, err
	}
	return tr.Succeeded, nil
}

// SetTokens force-sets an agent's balance (account reconciliation, §4.3), split
// evenly across the K shards.
func (l *Ledger) SetTokens(ctx context.Context, agent string, tokens int64) error {
	_, err := l.kv.Txn(ctx).Then(l.putShardsOps(agent, tokens)...).Commit()
	return err
}

// Remaining returns an agent's current token balance: the SUM of its shard
// counters (0 if unknown). A single prefix Get reads every shard in one round
// trip.
func (l *Ledger) Remaining(ctx context.Context, agent string) (int64, error) {
	resp, err := l.kv.Get(ctx, l.tokPrefix(agent), clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	var total int64
	for _, kv := range resp.Kvs {
		v, err := strconv.ParseInt(string(kv.Value), 10, 64)
		if err != nil {
			return 0, err
		}
		total += v
	}
	return total, nil
}

// returnOne credits r.Amount back to the SAME shard it was debited from (r.Shard)
// and deletes the reservation, in one txn gated on both the reservation and that
// shard's revisions (so a racing commit/return is detected and this no-ops).
// Crediting the original shard keeps every shard's total = its seeded slice −
// its outstanding reservations. Returns false on CAS conflict.
func (l *Ledger) returnOne(ctx context.Context, resvKey string, resvRev int64, r Reservation) (bool, error) {
	shardKey := l.tokShard(r.Agent, r.Shard)
	tr, err := l.kv.Get(ctx, shardKey)
	if err != nil {
		return false, err
	}
	var bal, shardRev int64
	if len(tr.Kvs) > 0 {
		if bal, err = strconv.ParseInt(string(tr.Kvs[0].Value), 10, 64); err != nil {
			return false, err
		}
		shardRev = tr.Kvs[0].ModRevision
	}
	txn, err := l.kv.Txn(ctx).
		If(
			clientv3.Compare(clientv3.ModRevision(resvKey), "=", resvRev),
			clientv3.Compare(clientv3.ModRevision(shardKey), "=", shardRev),
		).
		Then(
			clientv3.OpPut(shardKey, strconv.FormatInt(bal+r.Amount, 10)),
			clientv3.OpDelete(resvKey),
		).Commit()
	if err != nil {
		return false, err
	}
	return txn.Succeeded, nil
}

// Reclaim returns the tokens of every expired, still-reserved reservation (hung
// tokens from a controller crash between reserve and commit) and reports how
// many it reclaimed. Prefix-scans the reservations (no expiry queue) and filters
// reclaimable ones — the reconcile-loop pattern. Safe to run from every replica
// concurrently (each return is a revision-gated txn; a racing commit bumps the
// revision → that reservation is skipped, never returning a live pool's tokens).
// Run on a timer (e.g. 10s).
func (l *Ledger) Reclaim(ctx context.Context) (int, error) {
	resp, err := l.kv.Get(ctx, l.resvPrefix(), clientv3.WithPrefix())
	if err != nil {
		return 0, fmt.Errorf("ledger: scan reservations: %w", err)
	}
	nowMs := l.now().UnixMilli()
	n := 0
	for _, kv := range resp.Kvs {
		var r Reservation
		if err := json.Unmarshal(kv.Value, &r); err != nil {
			continue
		}
		if r.State != StateReserved || nowMs <= r.ExpireAt {
			continue // committed, or not yet expired → not reclaimable
		}
		ok, err := l.returnOne(ctx, string(kv.Key), kv.ModRevision, r)
		if err != nil {
			return n, fmt.Errorf("ledger: reclaim %s: %w", kv.Key, err)
		}
		if ok {
			n++ // reclaimed (CAS won): tokens returned, reservation deleted. A
			// CAS loss (ok=false, a commit raced in) is silently skipped — the
			// next cycle re-evaluates.
		}
	}
	return n, nil
}
