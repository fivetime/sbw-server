// Package registry is the controller's schedulable-node registry (controller
// §3.2/§4.2): the set of edge agents that have registered (announced their NIC
// capacity) and may receive pool placements. It is the kubelet-style Node
// inventory — etcd-backed so any stateless controller replica sees it.
//
// Registration is idempotent by edge id (a restart re-registers the same node).
// Token initialization (capacity×90%) is done by the caller composing this with
// the ledger, so the two concerns stay separate.
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/fivetime/sbw-contract/model"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Agent is a registered edge node.
type Agent struct {
	EdgeID       model.EdgeID `json:"edge_id"`
	CapacityBps  uint64       `json:"capacity_bps"`  // NIC line rate
	RegisteredAt int64        `json:"registered_ms"` // first-seen wall clock (ms)
}

// Registry stores schedulable agents in etcd under prefix+"agents/".
type Registry struct {
	kv     clientv3.KV
	prefix string
	now    func() time.Time
}

// Option configures a Registry.
type Option func(*Registry)

// WithClock overrides the time source (tests).
func WithClock(now func() time.Time) Option { return func(r *Registry) { r.now = now } }

// New builds a registry keyed under prefix (e.g. "sbw/").
func New(kv clientv3.KV, prefix string, opts ...Option) *Registry {
	r := &Registry{kv: kv, prefix: prefix, now: time.Now}
	for _, o := range opts {
		o(r)
	}
	return r
}

func (r *Registry) key(edge model.EdgeID) string { return r.prefix + "agents/" + string(edge) }
func (r *Registry) listPrefix() string           { return r.prefix + "agents/" }

// Register records (or refreshes) an agent and its capacity. Idempotent: a
// re-register keeps the original RegisteredAt (restart is the same node, not a
// new one), only refreshing capacity.
func (r *Registry) Register(ctx context.Context, edge model.EdgeID, capacityBps uint64) error {
	existing, ok, err := r.Get(ctx, edge)
	if err != nil {
		return err
	}
	a := Agent{EdgeID: edge, CapacityBps: capacityBps, RegisteredAt: r.now().UnixMilli()}
	if ok {
		a.RegisteredAt = existing.RegisteredAt // preserve first-seen
	}
	val, _ := json.Marshal(a)
	_, err = r.kv.Put(ctx, r.key(edge), string(val))
	return err
}

// Deregister removes an agent from the schedulable pool (after drain, §5.9).
func (r *Registry) Deregister(ctx context.Context, edge model.EdgeID) error {
	_, err := r.kv.Delete(ctx, r.key(edge))
	return err
}

// Get returns an agent; ok=false if not registered.
func (r *Registry) Get(ctx context.Context, edge model.EdgeID) (Agent, bool, error) {
	resp, err := r.kv.Get(ctx, r.key(edge))
	if err != nil {
		return Agent{}, false, err
	}
	if len(resp.Kvs) == 0 {
		return Agent{}, false, nil
	}
	var a Agent
	if err := json.Unmarshal(resp.Kvs[0].Value, &a); err != nil {
		return Agent{}, false, err
	}
	return a, true, nil
}

// EdgeIDs returns just the registered edge ids, sorted — the coverage layer's
// Edges source (it shards over the edge set, not the capacities). Satisfies
// coverage.Edges.
func (r *Registry) EdgeIDs(ctx context.Context) ([]model.EdgeID, error) {
	as, err := r.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]model.EdgeID, len(as))
	for i, a := range as {
		out[i] = a.EdgeID
	}
	return out, nil
}

// List returns all schedulable agents, sorted by edge id (deterministic).
func (r *Registry) List(ctx context.Context) ([]Agent, error) {
	resp, err := r.kv.Get(ctx, r.listPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("registry: list: %w", err)
	}
	out := make([]Agent, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var a Agent
		if err := json.Unmarshal(kv.Value, &a); err != nil {
			continue
		}
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EdgeID < out[j].EdgeID })
	return out, nil
}
