// Package edgever is the per-edge desired/applied version projections that make
// failover delivery + readiness OBSERVABLE ACROSS REPLICAS (DESIGN-liveness
// L-07). Under K=2 sharding an edge-agent opens its Subscribe stream + reports to
// only ONE coverer, so the legacy replica-local push + in-memory ready-wait can
// never reach an edge subscribed to a peer replica. The Nova-evacuate redesign
// splits "decide" from "deliver": the reconciler only mutates etcd; whichever
// replica holds the edge's stream realizes it; readiness is read from etcd. This
// package is the two etcd signals that decoupling needs:
//
//   - DESIRED version  <prefix>edgever/<edge>      — a per-edge monotonic counter
//     bumped (strictly AFTER the authoritative Yugabyte ybstore write) whenever WHAT
//     an edge should render changes. Every replica Watches it; the one that locally
//     holds the edge's stream re-renders + pushes. This is the cross-replica
//     delivery trigger (it replaces the failover path's direct replica-local push).
//
//   - APPLIED version  <prefix>edgeapplied/<edge>  — advanced (monotonically) by
//     the replica holding the edge's stream once the agent echoes (HealthReport.
//     AppliedVersion) that it applied a desired state of at-least that version.
//     The reconciler on ANY replica reads it to gate promotion ("先就位再切")
//     without needing to be the replica the agent talks to.
//
// DESIRED is tied to record CONTENT (bumped after the record write), not to a
// render call, so "applied >= V" soundly implies "the agent built the data plane
// for a record version >= V" — unlike the per-edge in-memory per-render Generation,
// which cannot be correlated to content across concurrent re-renders.
package edgever

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/fivetime/sbw-contract/model"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ErrConflict is returned when too many concurrent CAS increments collide; retry.
var ErrConflict = errors.New("edgever: write conflict, retry")

const maxRetries = 64

// Store holds the per-edge desired + applied version keys under a prefix.
type Store struct {
	cli    *clientv3.Client // needs Watch (and KV); same dependency shape as deathvote
	prefix string
}

// New builds a version store keyed under prefix (e.g. "sbw/").
func New(cli *clientv3.Client, prefix string) *Store {
	return &Store{cli: cli, prefix: prefix}
}

func (s *Store) desiredPrefix() string            { return s.prefix + "edgever/" }
func (s *Store) desiredKey(e model.EdgeID) string { return s.desiredPrefix() + string(e) }
func (s *Store) appliedKey(e model.EdgeID) string { return s.prefix + "edgeapplied/" + string(e) }

// Event is one edge's desired-version change (WatchDesired output).
type Event struct {
	Edge    model.EdgeID
	Version uint64
}

// Bump atomically increments and returns edge's DESIRED version (>=1). The read +
// CAS-increment is one txn gated on the key's revision, so concurrent callers each
// get a distinct strictly-increasing value. Call it AFTER the Yugabyte ybstore
// record write so the version causally follows the content change it signals.
func (s *Store) Bump(ctx context.Context, edge model.EdgeID) (uint64, error) {
	k := s.desiredKey(edge)
	for i := 0; i < maxRetries; i++ {
		cur, rev, err := s.read(ctx, k)
		if err != nil {
			return 0, err
		}
		next := cur + 1
		tr, err := s.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(k), "=", rev)).
			Then(clientv3.OpPut(k, strconv.FormatUint(next, 10))).
			Commit()
		if err != nil {
			return 0, err
		}
		if tr.Succeeded {
			return next, nil
		}
	}
	return 0, ErrConflict
}

// Desired returns edge's current desired version (0 if never bumped).
func (s *Store) Desired(ctx context.Context, edge model.EdgeID) (uint64, error) {
	v, _, err := s.read(ctx, s.desiredKey(edge))
	return v, err
}

// Applied returns edge's current applied version (0 if never advanced).
func (s *Store) Applied(ctx context.Context, edge model.EdgeID) (uint64, error) {
	v, _, err := s.read(ctx, s.appliedKey(edge))
	return v, err
}

// Advance raises edge's APPLIED version to v if v is greater than the current
// value (monotonic, idempotent). A stale/duplicate report (v <= current) is a
// no-op. Called by the replica holding the edge's stream when the agent echoes it
// applied desired-version >= v.
func (s *Store) Advance(ctx context.Context, edge model.EdgeID, v uint64) error {
	k := s.appliedKey(edge)
	for i := 0; i < maxRetries; i++ {
		cur, rev, err := s.read(ctx, k)
		if err != nil {
			return err
		}
		if cur >= v {
			return nil // already at/above v
		}
		tr, err := s.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(k), "=", rev)).
			Then(clientv3.OpPut(k, strconv.FormatUint(v, 10))).
			Commit()
		if err != nil {
			return err
		}
		if tr.Succeeded {
			return nil
		}
	}
	return ErrConflict
}

// WatchDesired streams desired-version changes: the current snapshot first (each
// present key as an Event), then live PUTs, until ctx is done. Every replica runs
// it; the converge loop filters to edges it locally serves (grpcsrv.IsSubscribed)
// and re-renders them. Snapshot-at-revision then watch-from-next (no gap/dupe),
// mirroring deathvote.Watch.
func (s *Store) WatchDesired(ctx context.Context) (<-chan Event, error) {
	out := make(chan Event, 64)
	resp, err := s.cli.Get(ctx, s.desiredPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(out)
		emit := func(key, val string) bool {
			edge, ok := s.parseDesired(key)
			if !ok {
				return true
			}
			v, perr := strconv.ParseUint(val, 10, 64)
			if perr != nil {
				return true
			}
			select {
			case out <- Event{Edge: edge, Version: v}:
				return true
			case <-ctx.Done():
				return false
			}
		}
		for _, kv := range resp.Kvs {
			if !emit(string(kv.Key), string(kv.Value)) {
				return
			}
		}
		wch := s.cli.Watch(ctx, s.desiredPrefix(), clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision+1))
		for wr := range wch {
			for _, e := range wr.Events {
				if e.Type != clientv3.EventTypePut {
					continue // desired versions are never deleted
				}
				if !emit(string(e.Kv.Key), string(e.Kv.Value)) {
					return
				}
			}
		}
	}()
	return out, nil
}

func (s *Store) parseDesired(key string) (model.EdgeID, bool) {
	rest := strings.TrimPrefix(key, s.desiredPrefix())
	if rest == key || rest == "" {
		return "", false
	}
	return model.EdgeID(rest), true
}

// read returns the uint64 value of key and its ModRevision (0/0 if absent).
func (s *Store) read(ctx context.Context, key string) (uint64, int64, error) {
	resp, err := s.cli.Get(ctx, key)
	if err != nil {
		return 0, 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, 0, nil
	}
	v, err := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return v, resp.Kvs[0].ModRevision, nil
}
