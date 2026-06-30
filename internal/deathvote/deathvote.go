// Package deathvote is the etcd side of corroborated failover (DESIGN-liveness
// §9, TODO-liveness L-03): under sharding with multihop BFD, a single coverer's
// PeerDown can be a PATH fault rather than node death, so the liveness Monitor
// fires hard-death only when a QUORUM of an edge's K coverers agree. This package
// carries those votes across replicas:
//
//   - Down/Up publish THIS replica's vote for an edge (its own PeerDown/PeerUp)
//     under a lease, so a crashed coverer's stale votes auto-expire — a dead
//     coverer must stop corroborating (it may have died, not the edge).
//   - Watch streams every replica's votes (initial snapshot + live changes) so
//     each replica can feed peers' votes into Monitor.Vote and reach quorum.
//
// Keys: <prefix>deathvotes/<edge>/<coverer>. A present key = that coverer votes
// the edge down; its deletion (explicit Up or lease expiry) = the vote cleared.
package deathvote

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/fivetime/sbw-contract/model"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Voter publishes this replica's death votes and watches all replicas'.
type Voter struct {
	cli     *clientv3.Client
	prefix  string
	self    string // this replica's coverer id (the vote key suffix)
	ttl     time.Duration
	leaseID clientv3.LeaseID
}

// VoteEvent is one coverer's vote change for an edge (Down false = cleared).
type VoteEvent struct {
	Edge    model.EdgeID
	Coverer string
	Down    bool
}

func (v *Voter) votesPrefix() string { return v.prefix + "deathvotes/" }
func (v *Voter) key(edge model.EdgeID) string {
	return v.votesPrefix() + string(edge) + "/" + v.self
}

// New builds a Voter for replica `self`. ttl bounds how long a crashed replica's
// votes linger (its lease keepalive stops, the votes expire after ~ttl).
func New(cli *clientv3.Client, prefix, self string, ttl time.Duration) *Voter {
	return &Voter{cli: cli, prefix: prefix, self: self, ttl: ttl}
}

// ensureLease grants the vote lease on first use and keeps it alive in the
// background, so all of this replica's votes vanish together if it dies.
func (v *Voter) ensureLease(ctx context.Context) error {
	if v.leaseID != 0 {
		return nil
	}
	ttlSec := int64(v.ttl.Seconds())
	if ttlSec < 1 {
		ttlSec = 1
	}
	lease, err := v.cli.Grant(ctx, ttlSec)
	if err != nil {
		return fmt.Errorf("deathvote: grant lease: %w", err)
	}
	keepCh, err := v.cli.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		return fmt.Errorf("deathvote: keepalive: %w", err)
	}
	// Drain the keepalive responses. An UNREAD KeepAlive channel fills the etcd
	// client's fixed-size response buffer, which then logs "lease keepalive response
	// queue is full; dropping response send" on every beat (observed flooding ctrl
	// logs). We don't need the responses — just keep the lease alive; the drain
	// goroutine exits when the channel closes (lease lost / client closed).
	go func() {
		for range keepCh {
		}
	}()
	v.leaseID = lease.ID
	return nil
}

// Down publishes this replica's vote that edge's session is down.
func (v *Voter) Down(ctx context.Context, edge model.EdgeID) error {
	if err := v.ensureLease(ctx); err != nil {
		return err
	}
	if _, err := v.cli.Put(ctx, v.key(edge), "down", clientv3.WithLease(v.leaseID)); err != nil {
		return fmt.Errorf("deathvote: put %s: %w", edge, err)
	}
	return nil
}

// Up clears this replica's vote for edge (its session recovered / PeerUp).
func (v *Voter) Up(ctx context.Context, edge model.EdgeID) error {
	if _, err := v.cli.Delete(ctx, v.key(edge)); err != nil {
		return fmt.Errorf("deathvote: delete %s: %w", edge, err)
	}
	return nil
}

// Watch streams vote changes: the current snapshot first (each present vote as a
// Down=true event), then live PUT (Down=true) / DELETE (Down=false) events until
// ctx is done. The consumer feeds these into Monitor.Vote (skipping its own id,
// which Monitor ignores anyway).
func (v *Voter) Watch(ctx context.Context) (<-chan VoteEvent, error) {
	out := make(chan VoteEvent, 64)
	// Snapshot at a fixed revision, then watch from the next one (no gap/dupe).
	resp, err := v.cli.Get(ctx, v.votesPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("deathvote: snapshot: %w", err)
	}
	go func() {
		defer close(out)
		for _, kv := range resp.Kvs {
			if ev, ok := v.parse(string(kv.Key), true); ok {
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
		wch := v.cli.Watch(ctx, v.votesPrefix(), clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision+1))
		for wr := range wch {
			for _, e := range wr.Events {
				down := e.Type == clientv3.EventTypePut
				if ev, ok := v.parse(string(e.Kv.Key), down); ok {
					select {
					case out <- ev:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out, nil
}

// parse splits <prefix>deathvotes/<edge>/<coverer> into a VoteEvent.
func (v *Voter) parse(key string, down bool) (VoteEvent, bool) {
	rest := strings.TrimPrefix(key, v.votesPrefix())
	if rest == key {
		return VoteEvent{}, false // not under our prefix
	}
	i := strings.LastIndex(rest, "/")
	if i <= 0 || i == len(rest)-1 {
		return VoteEvent{}, false // malformed (no edge or no coverer)
	}
	return VoteEvent{Edge: model.EdgeID(rest[:i]), Coverer: rest[i+1:], Down: down}, true
}
