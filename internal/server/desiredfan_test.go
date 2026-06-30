package server

import (
	"log/slog"
	"testing"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-contract/rpc"
)

// addStream registers a fake connected coverer (id + advertised agent endpoint) on the fan,
// the same shape connectCoverer builds. Test-only — no pump, the channel is never drained.
func addStream(f *desiredFan, id, endpoint string) {
	f.mu.Lock()
	f.streams[id] = &covererStream{id: id, endpoint: endpoint, ch: make(chan *rpc.Assignment, 1), done: make(chan struct{})}
	f.mu.Unlock()
}

// TestAssignmentForMatchesRouting is the load-bearing invariant of the K>1 coverer
// assignment (A): the coverer an agent is TOLD to home to (assignmentFor primary) must be
// the EXACT coverer the server ROUTES its desired-state through (streamForLocked) — both
// derive from the same connected-coverer HRW, so a mis-home that silently black-holes
// desired-state cannot happen. It also proves endpoints are carried and exactly one primary
// is marked.
func TestAssignmentForMatchesRouting(t *testing.T) {
	f := newDesiredFan(slog.New(slog.DiscardHandler), 2)
	addStream(f, "10.99.0.10", "coverer-0.sbw-system:1791")
	addStream(f, "10.99.0.11", "coverer-1.sbw-system:1791")

	// Sweep enough edges that HRW lands a primary on each coverer at least once (so we test
	// both, not just one half of the hash space).
	sawPrimary := map[string]bool{}
	for _, edge := range []string{"l1", "l2", "l3", "l4", "l5", "l6", "l7", "l8"} {
		a, ok := f.assignmentFor(model.EdgeID(edge))
		if !ok {
			t.Fatalf("edge %s: assignmentFor returned not-ok with 2 coverers connected", edge)
		}
		if len(a.Coverers) != 2 {
			t.Fatalf("edge %s: want 2 coverers (k=2), got %d", edge, len(a.Coverers))
		}
		// Exactly one primary, and it is covers[0].
		primaries := 0
		var primaryID, primaryEndpoint string
		for _, c := range a.Coverers {
			if c.Primary {
				primaries++
				primaryID, primaryEndpoint = c.ControllerID, c.GRPCEndpoint
			}
			if c.GRPCEndpoint == "" {
				t.Fatalf("edge %s: coverer %s has empty endpoint (agent could not re-home)", edge, c.ControllerID)
			}
		}
		if primaries != 1 {
			t.Fatalf("edge %s: want exactly 1 primary, got %d", edge, primaries)
		}
		if a.Coverers[0].Primary != true || a.Coverers[0].ControllerID != primaryID {
			t.Fatalf("edge %s: primary must be covers[0], got primary=%s covers[0]=%s", edge, primaryID, a.Coverers[0].ControllerID)
		}
		// THE invariant: assignment primary == routing target.
		f.mu.Lock()
		route := f.streamForLocked(model.EdgeID(edge))
		f.mu.Unlock()
		if route == nil {
			t.Fatalf("edge %s: streamForLocked nil but assignment ok", edge)
		}
		if route.id != primaryID {
			t.Fatalf("edge %s: routing target %s != assignment primary %s (mis-home would black-hole desired-state)", edge, route.id, primaryID)
		}
		if route.endpoint != primaryEndpoint {
			t.Fatalf("edge %s: endpoint mismatch route=%s assign=%s", edge, route.endpoint, primaryEndpoint)
		}
		sawPrimary[primaryID] = true
	}
	if len(sawPrimary) != 2 {
		t.Fatalf("HRW never spread primaries across both coverers (saw %v) — test is not exercising both", sawPrimary)
	}
}

// TestAssignmentForK1 proves a single connected coverer yields a one-coverer primary-only
// assignment (the lab K=1 path) carrying its endpoint.
func TestAssignmentForK1(t *testing.T) {
	f := newDesiredFan(slog.New(slog.DiscardHandler), 1)
	addStream(f, "10.99.0.10", "coverer-0.sbw-system:1791")
	a, ok := f.assignmentFor(model.EdgeID("l1"))
	if !ok || len(a.Coverers) != 1 || !a.Coverers[0].Primary {
		t.Fatalf("K=1: want one primary coverer, got ok=%v %+v", ok, a.Coverers)
	}
	if a.Coverers[0].GRPCEndpoint != "coverer-0.sbw-system:1791" {
		t.Fatalf("K=1: wrong endpoint %q", a.Coverers[0].GRPCEndpoint)
	}
}

// TestAssignmentForNoCoverers proves an edge with NO connected coverer returns not-ok (the
// Register reply omits coverers; the agent stays put and is re-homed by the next REHOME)
// rather than fabricating an assignment.
func TestAssignmentForNoCoverers(t *testing.T) {
	f := newDesiredFan(slog.New(slog.DiscardHandler), 2)
	if a, ok := f.assignmentFor(model.EdgeID("l1")); ok {
		t.Fatalf("no coverers connected: want not-ok, got %+v", a)
	}
}

// delStream removes a coverer stream (simulating a coverer death/evict). Test-only.
func delStream(f *desiredFan, id string) {
	f.mu.Lock()
	delete(f.streams, id)
	f.mu.Unlock()
}

// drainRehome non-blockingly pops one Assignment and reports whether it is a REHOME.
func drainRehome(s *covererStream) bool {
	select {
	case a := <-s.ch:
		return a.Kind == rpc.Assignment_EDGE_DIRECTIVE && a.Directive != nil && a.Directive.Kind == rpc.Directive_REHOME
	default:
		return false
	}
}

// TestRehomeOnPrimaryFlip proves the coverage-recompute REHOME path: a steady recompute
// pushes nothing, a primary's DEATH rehomes the edge onto the survivor, and the primary's
// RECOVERY rehomes the edge BACK (broadcast to all covers so the agent — sitting on its
// fallback — is told to migrate to the recovered primary). An unregistered edge is never
// rehomed on first observation.
func TestRehomeOnPrimaryFlip(t *testing.T) {
	f := newDesiredFan(slog.New(slog.DiscardHandler), 2)
	addStream(f, "10.99.0.10", "coverer-0.sbw-system:1791")
	addStream(f, "10.99.0.11", "coverer-1.sbw-system:1791")
	edge := model.EdgeID("l1")

	a, ok := f.assignmentFor(edge)
	if !ok {
		t.Fatal("assignmentFor not ok with both coverers")
	}
	primary0 := a.Coverers[0].ControllerID
	other := "10.99.0.11"
	if primary0 == "10.99.0.11" {
		other = "10.99.0.10"
	}
	f.notePrimary(edge, primary0) // agent registered + homed to its primary

	// Steady recompute: no flip -> no REHOME.
	if n := f.rehomeChangedPrimaries([]model.EdgeID{edge}); n != 0 {
		t.Fatalf("steady recompute should rehome nothing, got %d", n)
	}

	// Primary DEATH: edge's only remaining coverer is `other`; it must be rehomed there.
	delStream(f, primary0)
	if n := f.rehomeChangedPrimaries([]model.EdgeID{edge}); n != 1 {
		t.Fatalf("primary death should rehome 1, got %d", n)
	}
	if !drainRehome(f.streams[other]) {
		t.Fatalf("survivor %s got no REHOME after primary death", other)
	}

	// Primary RECOVERY: flips back -> REHOME broadcast reaches the recovered primary (where
	// the agent must migrate from its fallback).
	addStream(f, primary0, "recovered.sbw-system:1791")
	if n := f.rehomeChangedPrimaries([]model.EdgeID{edge}); n != 1 {
		t.Fatalf("primary recovery should rehome 1, got %d", n)
	}
	if !drainRehome(f.streams[primary0]) {
		t.Fatalf("recovered primary %s got no REHOME (agent on fallback would be black-holed)", primary0)
	}

	// First observation of an UNREGISTERED edge (no notePrimary): record baseline, no REHOME.
	if n := f.rehomeChangedPrimaries([]model.EdgeID{model.EdgeID("neveredge")}); n != 0 {
		t.Fatalf("unregistered edge first recompute should rehome nothing, got %d", n)
	}
}
