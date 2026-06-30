package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"sync"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-contract/rpc"

	"github.com/fivetime/sbw-server/internal/orchestrator"
	"github.com/fivetime/sbw-server/internal/shard"
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
	id string
	// endpoint is the coverer's externally-routable agent-facing address
	// (WatchRequest.agent_endpoint), handed back to agents in their coverer-assignment so
	// they home to their PRIMARY coverer. May be "" (coverer advertised none — K=1 ok).
	endpoint string
	ch       chan *rpc.Assignment
	done     chan struct{}
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
// Watch stream covering the edge is connected HERE: streamForLocked(edge) computes
// the edge's covering coverer ids by HRW over the CONNECTED coverer set (the keys of
// streams) and returns the first one with a live entry — no store I/O at all.
type desiredFan struct {
	mu      sync.Mutex
	streams map[string]*covererStream
	// k is the HRW replication factor over the CONNECTED coverer set: an edge routes
	// to the top-k of the connected coverers, primary-first. Wired at construction
	// from CPOptions.CoverageK (== the server's sharding K).
	k   int
	log *slog.Logger
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
// stream is created fresh on its Watch connect (connectCoverer). k is the HRW
// replication factor over the connected coverer set.
func newDesiredFan(log *slog.Logger, k int) *desiredFan {
	return &desiredFan{streams: map[string]*covererStream{}, k: k, log: log}
}

// connectedCovererIDsLocked is the SINGLE read point of the connected-coverer
// membership: the sorted keys of f.streams. This is the source of truth that REPLACES
// ctrlreg/etcd for routing + coverage — cheap, no I/O. Caller holds f.mu.
func (f *desiredFan) connectedCovererIDsLocked() []string {
	ids := make([]string, 0, len(f.streams))
	for id := range f.streams {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
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

// streamForLocked returns the connected coverer stream serving edge: it resolves the
// edge's covering coverer ids by HRW over the CONNECTED coverer set, then picks the
// first (highest-HRW) — which is necessarily connected because the candidate set IS
// the connected set. nil when no coverer is connected. Pure CPU, no store I/O. Caller
// holds f.mu.
func (f *desiredFan) streamForLocked(edge model.EdgeID) *covererStream {
	ids := f.connectedCovererIDsLocked()
	covers := shard.Coverers(string(edge), ids, f.k) // top-k of the connected set, pure CPU
	return f.streamForIDs(covers)                    // first with a live stream == covers[0]
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

// assignmentFor computes the agent's coverer-assignment over the SAME connected-coverer
// HRW the desired-state routing uses (streamForLocked) — so the coverer the agent is told
// to home to is exactly the one the server actually routes EDGE_DIRECTIVE through. It maps
// the top-k covering coverer ids to model.Coverer{id, agent-endpoint, primary}, primary =
// covers[0]. Returns false when no coverer is connected (registration must not fail on it;
// the agent stays put and is re-homed by the next REHOME). Every id in covers is from the
// connected set read under the same lock, so each has a live stream.
func (f *desiredFan) assignmentFor(edge model.EdgeID) (model.CovererAssignment, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	covers := shard.Coverers(string(edge), f.connectedCovererIDsLocked(), f.k)
	if len(covers) == 0 {
		return model.CovererAssignment{}, false
	}
	out := model.CovererAssignment{EdgeID: edge, Coverers: make([]model.Coverer, 0, len(covers))}
	for i, id := range covers {
		s := f.streams[id]
		if s == nil {
			continue // belt-and-braces; covers ⊆ connected set under this lock
		}
		out.Coverers = append(out.Coverers, model.Coverer{
			ControllerID: id,
			GRPCEndpoint: s.endpoint,
			Primary:      i == 0,
		})
	}
	if len(out.Coverers) == 0 {
		return model.CovererAssignment{}, false
	}
	return out, true
}

// IsSubscribed reports whether a covering coverer's Watch stream is connected here.
// This is the subChecker the orchestrator's locallyDeliverable consults, preserving
// L-08 peer-ownership gating: an edge whose coverer is NOT connected to this server
// replica is not pushed locally; it is bumped via edgever and delivered by the
// replica whose coverer holds it, through RunConverge→RerenderEdge→fan.
func (f *desiredFan) IsSubscribed(edge model.EdgeID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.streamForLocked(edge) != nil
}

// emit routes one directive to the covering coverer's Watch channel as an
// Assignment{EDGE_DIRECTIVE}. It returns ErrNotSubscribed when no covering coverer
// is connected (the synchronous deliverability gate the best-effort/rollback
// callers branch on). The channel BLOCKS, NEVER DROPS into the bounded buffer; the
// pump's send-deadline+evict is the only release valve. done releases the producer
// if the stream is superseded/closed.
func (f *desiredFan) emit(edge model.EdgeID, d *rpc.Directive) error {
	f.mu.Lock()
	s := f.streamForLocked(edge) // HRW over the connected set, pure CPU under a short hold
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
