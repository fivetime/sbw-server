package server

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-contract/rpc"
	"github.com/fivetime/sbw-server/internal/shard"
)

// covererSendDeadline bounds a single stream.Send to one coverer. If Send wedges
// past it the coverer is evicted (its bounded buffer is released, a blocked emit
// unblocks with ErrNotSubscribed) and the RPC tears down — so ONE wedged coverer
// cannot pin the 4096 buffer and back-pressure the orchestrator. On reconnect,
// initialCovererSync's per-edge RerenderEdge is the recovery.
const covererSendDeadline = 10 * time.Second

// errSendTimeout is returned by sendWithDeadline when a Send wedges past the deadline.
var errSendTimeout = errors.New("coverer send deadline exceeded")

// covererServer implements rpc.ServerCovererServer (the coverers are its clients).
// It is the agent-facing AgentService's server-side counterpart's SIBLING: this is
// the ONLY service the sbw-server registers (AgentService lives on the coverer).
type covererServer struct {
	rpc.UnimplementedServerCovererServer
	cp *ControlPlane
}

// NewCovererServer builds the ServerCoverer gRPC service over a control plane.
func NewCovererServer(cp *ControlPlane) *covererServer { return &covererServer{cp: cp} }

var _ rpc.ServerCovererServer = (*covererServer)(nil)

// Watch is the desired-state PUMP (server→coverer downlink). A coverer opens it
// once with its stable id; the server registers a fresh buffered stream, kicks the
// (re)connect re-sync asynchronously, and ranges the buffer to the wire.
func (s *covererServer) Watch(req *rpc.WatchRequest, stream rpc.ServerCoverer_WatchServer) error {
	if req.SchemaVersion != 0 && int(req.SchemaVersion) != model.SchemaVersion {
		return errors.New("coverer schema version mismatch")
	}
	covererID := req.CovererId
	if covererID == "" {
		return errors.New("watch: empty coverer_id")
	}
	cp := s.cp
	cs := cp.connectCoverer(covererID)
	ctx := stream.Context()
	// Cleanup on disconnect: unregister this stream (if still current) so a stale
	// entry never wedges emit.
	go func() {
		<-ctx.Done()
		cp.evictCoverer(covererID, cs)
	}()
	// Initial sync IN A GOROUTINE (never synchronously — else the COVERAGE +
	// per-edge RerenderEdge fan would fill cs.ch before this pump drains it →
	// deadlock, the monolith happens-before rule).
	go cp.initialCovererSync(ctx, covererID)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case a, ok := <-cs.ch:
			if !ok {
				return nil
			}
			if err := cp.sendWithDeadline(stream, a); err != nil {
				cp.evictCoverer(covererID, cs)
				return err
			}
		}
	}
}

// sendWithDeadline runs one stream.Send under covererSendDeadline. The Send runs in
// a goroutine so a wedged transport cannot block the pump forever; on timeout the
// caller evicts + tears the RPC down (the wedged Send goroutine unblocks when gRPC
// closes the stream). This is the only release valve for the blocks-never-drops
// emit path.
func (cp *ControlPlane) sendWithDeadline(stream rpc.ServerCoverer_WatchServer, a *rpc.Assignment) error {
	done := make(chan error, 1)
	go func() { done <- stream.Send(a) }()
	t := time.NewTimer(covererSendDeadline)
	defer t.Stop()
	select {
	case err := <-done:
		return err
	case <-t.C:
		return errSendTimeout
	}
}

// Report is the coverer uplink DRAIN: it consumes CovererReports until EOF and
// dispatches each to the server-half scvr.Report. One bad report is logged, NOT
// fatal to the stream (a coverer keeps its single long-lived Report stream).
func (s *covererServer) Report(stream rpc.ServerCoverer_ReportServer) error {
	ctx := stream.Context()
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&rpc.ReportAck{})
		}
		if err != nil {
			return err
		}
		if err := s.cp.scvr.Report(ctx, r); err != nil {
			s.cp.log.Warn("coverer report", "kind", r.Kind, "err", err)
		}
	}
}

// Register is the unary agent-registration relay: straight delegate to the
// server-half Register (schema gate + onRegister + covererFunc → coverers).
func (s *covererServer) Register(ctx context.Context, req *rpc.RegisterRequest) (*rpc.RegisterResponse, error) {
	return s.cp.scvr.Register(ctx, req)
}

// connectCoverer registers a FRESH Watch stream for covererID, superseding any
// prior one (close its done so a blocked emit/pump releases — mirrors the agent
// transport's Subscribe reconnect). Cross-process the server tolerates "orchestrator
// emits for a not-yet-connected coverer" (emit drops with ErrNotSubscribed per the
// connectivity gate); initialCovererSync on every (re)connect is the recovery.
func (cp *ControlPlane) connectCoverer(covererID string) *covererStream {
	f := cp.fan
	f.mu.Lock()
	if old := f.streams[covererID]; old != nil {
		close(old.done) // supersede the prior stream
	}
	s := &covererStream{id: covererID, ch: make(chan *rpc.Assignment, seamBuffer), done: make(chan struct{})}
	f.streams[covererID] = s
	f.mu.Unlock()
	// A new coverer SHRINKS every other coverer's covered set (HRW reshuffle), so all
	// must be re-emitted (debounced). The new coverer's own COVERAGE also arrives
	// immediately via initialCovererSync (idempotent overlap is harmless).
	cp.scheduleCoverageRecompute()
	return s
}

// evictCoverer unregisters s (only if it is still the current stream for its id) and
// closes its done so any blocked emit unblocks with ErrNotSubscribed. Idempotent:
// a second call (ctx.Done goroutine + pump send-error both evict) is a no-op once
// the entry is gone, and a superseded stream's done was already closed by connect.
func (cp *ControlPlane) evictCoverer(covererID string, s *covererStream) {
	f := cp.fan
	f.mu.Lock()
	removed := false
	if cur := f.streams[covererID]; cur == s {
		delete(f.streams, covererID)
		close(s.done)
		removed = true
	}
	f.mu.Unlock()
	if removed {
		// Departure GROWS the remaining coverers' covered sets — recompute + re-emit
		// COVERAGE to all (debounced).
		cp.scheduleCoverageRecompute()
	}
}

// initialCovererSync is the (re)connect re-sync for a coverer: emit one COVERAGE
// assignment for its covered-edge set (so its ribtap (de)taps to match), then
// mon.Alive + RerenderEdge each covered edge so the coverer recovers state from the
// store-backed render path. This folds in the monolith grpcsrv onSubscribe/onResync
// job (which is gone with the agent transport). RerenderEdge per covered edge on
// every (re)connect is the recovery for any directive dropped while the coverer was
// disconnected.
func (cp *ControlPlane) initialCovererSync(ctx context.Context, covererID string) {
	edges := cp.coveredEdgesFor(ctx, covererID)
	cp.fan.emitCoverage(covererID, edges)
	for _, e := range edges {
		if cp.Liveness != nil {
			cp.Liveness.Alive(ctx, e) // a live covering stream is an alive signal
		}
		if err := cp.Orch.RerenderEdge(ctx, e); err != nil {
			cp.log.Warn("coverer initial sync rerender failed", "coverer", covererID, "edge", e, "err", err)
		}
	}
}

// coveredEdgesFor returns the edges the given coverer id covers — computed by HRW
// (shard.CoveredEdges) over the CONNECTED coverer set and the REGISTRY edge universe.
// No ctrlreg/etcd is consulted for membership; only the edge-universe read hits etcd,
// and that is OFF the emit/routing hot path (this runs on (re)connect only). An
// edge-universe read error yields no edges this sync (the next sync / DriftSweep
// backstops).
func (cp *ControlPlane) coveredEdgesFor(ctx context.Context, covererID string) []model.EdgeID {
	ids := cp.connectedCovererIDs() // connected membership, no etcd
	es, err := cp.Registry.EdgeIDs(ctx)
	if err != nil {
		cp.log.Warn("covered-edges: edge universe read failed", "coverer", covererID, "err", err)
		return nil
	}
	strs := make([]string, len(es))
	for i, e := range es {
		strs[i] = string(e)
	}
	covered := shard.CoveredEdges(covererID, strs, ids, cp.coverageK)
	out := make([]model.EdgeID, len(covered))
	for i, s := range covered {
		out[i] = model.EdgeID(s)
	}
	return out
}
