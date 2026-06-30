// Package ctrlreg is the controller-replica membership registry (DESIGN-liveness
// §8): each replica Joins under an etcd LEASE and keeps it alive, so a dead
// replica's entry auto-expires after the TTL and the shard coverage (package
// shard) reshuffles without any inter-controller protocol. Unlike the agent
// registry (persistent, explicit deregister), controller membership is
// lease-backed because a crashed controller cannot deregister itself.
package ctrlreg

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Controller is a registered controller replica.
type Controller struct {
	ID           string `json:"id"`            // stable replica id (shard hash key)
	GRPCEndpoint string `json:"grpc_endpoint"` // where agents connect (told to agents as coverer)
	RegisteredAt int64  `json:"registered_ms"`
}

// Registry reads/writes controller membership under prefix+"controllers/".
type Registry struct {
	cli    *clientv3.Client
	prefix string
	ttl    time.Duration
	now    func() time.Time
}

// Option configures a Registry.
type Option func(*Registry)

// New builds a registry; ttl is the membership lease TTL (a dead replica drops
// out after ~ttl). Needs the full client for Lease/Watch (not just KV).
func New(cli *clientv3.Client, prefix string, ttl time.Duration, opts ...Option) *Registry {
	r := &Registry{cli: cli, prefix: prefix, ttl: ttl, now: time.Now}
	for _, o := range opts {
		o(r)
	}
	return r
}

func (r *Registry) listPrefix() string   { return r.prefix + "controllers/" }
func (r *Registry) key(id string) string { return r.prefix + "controllers/" + id }

// Join registers self under a fresh lease and starts background keep-alive,
// returning a Membership handle (Close it to leave gracefully). If the process
// dies the lease expires after the TTL and the entry vanishes.
func (r *Registry) Join(ctx context.Context, self Controller) (*Membership, error) {
	ttlSec := int64(r.ttl.Seconds())
	if ttlSec < 1 {
		ttlSec = 1
	}
	lease, err := r.cli.Grant(ctx, ttlSec)
	if err != nil {
		return nil, fmt.Errorf("ctrlreg: grant lease: %w", err)
	}
	self.RegisteredAt = r.now().UnixMilli()
	val, _ := json.Marshal(self)
	if _, err := r.cli.Put(ctx, r.key(self.ID), string(val), clientv3.WithLease(lease.ID)); err != nil {
		_, _ = r.cli.Revoke(context.Background(), lease.ID)
		return nil, fmt.Errorf("ctrlreg: put %s: %w", self.ID, err)
	}
	// Keep-alive on a background context so it survives the Join ctx; Close stops it.
	keepCh, err := r.cli.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		_, _ = r.cli.Revoke(context.Background(), lease.ID)
		return nil, fmt.Errorf("ctrlreg: keepalive: %w", err)
	}
	m := &Membership{cli: r.cli, leaseID: lease.ID, key: r.key(self.ID), done: make(chan struct{})}
	go m.keepalive(keepCh)
	return m, nil
}

// List returns the currently-live controllers (lease-backed; expired replicas
// are absent), sorted by id for a stable shard input.
func (r *Registry) List(ctx context.Context) ([]Controller, error) {
	resp, err := r.cli.Get(ctx, r.listPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("ctrlreg: list: %w", err)
	}
	out := make([]Controller, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var c Controller
		if err := json.Unmarshal(kv.Value, &c); err != nil {
			continue
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// IDs returns just the live controller ids (the shard-hash input).
func (r *Registry) IDs(ctx context.Context) ([]string, error) {
	cs, err := r.List(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(cs))
	for i, c := range cs {
		ids[i] = c.ID
	}
	return ids, nil
}

// Endpoints returns the live controllers' id→agent-facing gRPC endpoint map —
// what the coverage assigner needs to turn coverer ids into dial targets the
// agent is told (L-06). Satisfies coverage.EndpointMap.
func (r *Registry) Endpoints(ctx context.Context) (map[string]string, error) {
	cs, err := r.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(cs))
	for _, c := range cs {
		out[c.ID] = c.GRPCEndpoint
	}
	return out, nil
}

// Watch streams the live controller set: the current snapshot immediately, then
// a fresh snapshot on every membership change (join/leave/expiry), until ctx is
// done. Each controller drives a coverage recompute off this.
func (r *Registry) Watch(ctx context.Context) (<-chan []Controller, error) {
	snap, err := r.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make(chan []Controller, 1)
	out <- snap
	wch := r.cli.Watch(ctx, r.listPrefix(), clientv3.WithPrefix())
	go func() {
		defer close(out)
		for range wch {
			cs, err := r.List(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			select {
			case out <- cs:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// Membership keeps a replica's lease alive; Close leaves gracefully (delete +
// revoke), or just stop the process and let the lease expire.
type Membership struct {
	cli     *clientv3.Client
	leaseID clientv3.LeaseID
	key     string
	done    chan struct{}
}

func (m *Membership) keepalive(ch <-chan *clientv3.LeaseKeepAliveResponse) {
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // lease lost / client closed
			}
		case <-m.done:
			return
		}
	}
}

// Close leaves the membership: stops keep-alive, deletes the entry and revokes
// the lease so coverage reshuffles immediately (no TTL wait).
func (m *Membership) Close(ctx context.Context) error {
	select {
	case <-m.done:
		return nil // already closed
	default:
		close(m.done)
	}
	_, _ = m.cli.Delete(ctx, m.key)
	_, err := m.cli.Revoke(ctx, m.leaseID)
	return err
}
