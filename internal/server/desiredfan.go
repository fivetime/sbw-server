package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-contract/rpc"
	"github.com/fivetime/sbw-server/internal/orchestrator"
)

// seamBuffer is the per-coverer Watch channel depth. The seam NEVER drops (the
// producer blocks on a full channel, see emit); this depth only bounds how much
// rides in the channel before the producer blocks. The pump's send-deadline+evict
// (servercoverer.go) is the sole release valve when one coverer wedges.
const seamBuffer = 4096

// ErrNotSubscribed is the deliverability error emit returns when no connected
// coverer covers an edge (the cross-process analog of grpcsrv.ErrNotSubscribed,
// which is gone with the agent transport). The orchestrator treats a Pusher error
// as "not locally deliverable": the edge is bumped via edgever and delivered by
// whichever server replica's coverer holds it, through RunConverge→RerenderEdge.
var ErrNotSubscribed = errors.New("coverer not subscribed")

// covererStream is one coverer's open Watch downlink (the server→coverer half of
// the ServerCoverer gRPC contract). ch carries the Assignments the server fans to
// this coverer; done is closed to supersede/evict it.
type covererStream struct {
	id   string
	ch   chan *rpc.Assignment
	done chan struct{}
}

// desiredFan is the SERVER-HALF of the ServerCoverer Watch downlink. It satisfies
// the orchestrator's Pusher (+ the optional deltaPusher / subChecker capabilities)
// so it drops in as o.push, but instead of touching an agent transport it MARSHALS
// the typed model into an *rpc.Directive and emits an Assignment{EDGE_DIRECTIVE}
// into the Watch channel of the COVERER covering that edge. The remote coverer
// (over gRPC) drains the channel and relays each directive to its agents.
//
// Re-gating (the load-bearing split fix): the server can no longer ask an agent
// transport "is this agent here?". Deliverability is re-gated on whether a coverer
// Watch stream covering the edge is connected HERE: streamFor(edge) resolves the
// covering coverer ids via coverersOf (coverage.Reconciler.CoverersOf) and returns
// the first one with a live entry in streams.
type desiredFan struct {
	mu      sync.Mutex
	streams map[string]*covererStream
	// coverersOf resolves an edge to the controller/coverer ids covering it,
	// primary-first (wired post-construction to coverage.Reconciler.CoverersOf via
	// cp.SetCoverersOf). nil until wired → nothing is locally deliverable.
	coverersOf func(context.Context, model.EdgeID) ([]string, error)
	log        *slog.Logger
}

// Compile-time proof the fan satisfies the orchestrator's Pusher and the optional
// capabilities it type-asserts at runtime (deltaPusher / subChecker).
var _ orchestrator.Pusher = (*desiredFan)(nil)

type fanDeltaPusher interface {
	PushDelta(model.EdgeID, model.EdgeDesiredDelta) error
}
type fanSubChecker interface {
	IsSubscribed(model.EdgeID) bool
}

var (
	_ fanDeltaPusher = (*desiredFan)(nil)
	_ fanSubChecker  = (*desiredFan)(nil)
)

// newDesiredFan builds the server-half fan. It pre-registers NO stream: a coverer's
// stream is created fresh on its Watch connect (connectCoverer). coverersOf is
// injected later via SetCoverersOf.
func newDesiredFan(log *slog.Logger) *desiredFan {
	return &desiredFan{streams: map[string]*covererStream{}, log: log}
}

// PushDesired marshals the FULL edge snapshot into a DESIRED_STATE directive and
// emits it. Generation is carried verbatim.
func (f *desiredFan) PushDesired(edge model.EdgeID, st model.EdgeDesiredState) error {
	p, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return f.emit(edge, &rpc.Directive{Kind: rpc.Directive_DESIRED_STATE, Generation: st.Generation, Payload: p})
}

// PushDelta marshals an INCREMENTAL per-pool delta into a DESIRED_DELTA directive
// and emits it.
func (f *desiredFan) PushDelta(edge model.EdgeID, dl model.EdgeDesiredDelta) error {
	p, err := json.Marshal(dl)
	if err != nil {
		return err
	}
	return f.emit(edge, &rpc.Directive{Kind: rpc.Directive_DESIRED_DELTA, Generation: dl.Generation, Payload: p})
}

// PushRehome marshals a coverer assignment into a REHOME directive and emits it.
func (f *desiredFan) PushRehome(edge model.EdgeID, a model.CovererAssignment) error {
	p, err := json.Marshal(a)
	if err != nil {
		return err
	}
	return f.emit(edge, &rpc.Directive{Kind: rpc.Directive_REHOME, Payload: p})
}

// streamFor returns the connected coverer stream serving edge: the FIRST of the
// edge's coverers (primary-first) that has a live Watch stream registered here.
// nil when coverersOf is unwired or no covering coverer is connected. Caller holds
// f.mu.
// coversIDs resolves the coverer ids covering edge (primary-first). The wired coverersOf
// hits ctrlreg/etcd, so this MUST be called WITHOUT holding f.mu — holding the global fan
// lock across a store round-trip would serialize every emit behind etcd and stall on a slow
// store. (TODO step10/11: route over the CONNECTED coverer set f.streams via HRW so the hot
// path needs no store I/O at all, and keep COVERAGE computed from the same set.)
func (f *desiredFan) coversIDs(edge model.EdgeID) []string {
	if f.coverersOf == nil {
		return nil
	}
	ids, err := f.coverersOf(context.Background(), edge)
	if err != nil {
		if f.log != nil {
			f.log.Warn("desiredfan: coverersOf failed", "edge", edge, "err", err)
		}
		return nil
	}
	return ids
}

// streamForIDs returns the first of ids that has a live Watch stream registered here, or
// nil if none is connected. Caller holds f.mu (map read only — no store I/O).
func (f *desiredFan) streamForIDs(ids []string) *covererStream {
	for _, id := range ids {
		if s := f.streams[id]; s != nil {
			return s
		}
	}
	return nil
}

// IsSubscribed reports whether a covering coverer's Watch stream is connected here.
// This is the subChecker the orchestrator's locallyDeliverable consults, preserving
// L-08 peer-ownership gating: an edge whose coverer is NOT connected to this server
// replica is not pushed locally; it is bumped via edgever and delivered by the
// replica whose coverer holds it, through RunConverge→RerenderEdge→fan.
func (f *desiredFan) IsSubscribed(edge model.EdgeID) bool {
	ids := f.coversIDs(edge) // store I/O OUTSIDE the lock
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.streamForIDs(ids) != nil
}

// emit routes one directive to the covering coverer's Watch channel as an
// Assignment{EDGE_DIRECTIVE}. It returns ErrNotSubscribed when no covering coverer
// is connected (the synchronous deliverability gate the best-effort/rollback
// callers branch on). The channel BLOCKS, NEVER DROPS into the bounded buffer; the
// pump's send-deadline+evict is the only release valve. done releases the producer
// if the stream is superseded/closed.
func (f *desiredFan) emit(edge model.EdgeID, d *rpc.Directive) error {
	ids := f.coversIDs(edge) // store I/O OUTSIDE the lock — never hold f.mu across etcd
	f.mu.Lock()
	s := f.streamForIDs(ids)
	f.mu.Unlock()
	if s == nil {
		return ErrNotSubscribed
	}
	a := &rpc.Assignment{Kind: rpc.Assignment_EDGE_DIRECTIVE, EdgeId: string(edge), Directive: d}
	select {
	case s.ch <- a:
		return nil
	case <-s.done:
		return ErrNotSubscribed
	}
}

// emitCoverage pushes a COVERAGE assignment (the FULL covered-edge set) into a
// coverer's Watch channel, keyed by coverer id. The remote coverer's ribtap
// (de)taps to match. Best-effort: a superseded/absent stream is skipped.
func (f *desiredFan) emitCoverage(covererID string, edges []model.EdgeID) {
	f.mu.Lock()
	s := f.streams[covererID]
	f.mu.Unlock()
	if s == nil {
		return
	}
	ce := make([]string, len(edges))
	for i, e := range edges {
		ce[i] = string(e)
	}
	a := &rpc.Assignment{Kind: rpc.Assignment_COVERAGE, CoveredEdges: ce}
	select {
	case s.ch <- a:
	case <-s.done:
	}
}
