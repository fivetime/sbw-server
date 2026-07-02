package server

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-server/internal/apiresult"
)

// recEmitter records emitted events for assertions (a test-only apiresult.Emitter).
type recEmitter struct {
	mu     sync.Mutex
	events []apiresult.Event
}

func (r *recEmitter) Emit(_ context.Context, ev apiresult.Event) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
}

func (r *recEmitter) ops() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.events))
	for i, e := range r.events {
		out[i] = e.Op
	}
	return out
}

// newFIBTestCP builds the minimal ControlPlane the emitFIBDrift level→edge converter
// touches (no etcd/store): the transition map, a recording emitter, a fixed clock.
func newFIBTestCP(rec *recEmitter) *ControlPlane {
	return &ControlPlane{
		fibDriftEdges: make(map[model.EdgeID]bool),
		emitter:       rec,
		now:           func() time.Time { return time.Unix(1, 0) },
		log:           slog.New(slog.DiscardHandler),
	}
}

// TestEmitFIBDriftEdgeTriggered proves the FIB-drift signal (previously received-and-
// dropped, DESIGN §6.5) is now surfaced to BSS, and edge-triggered: exactly one event
// per baseline↔drifted transition — NOT one per report — carrying the drift magnitude.
func TestEmitFIBDriftEdgeTriggered(t *testing.T) {
	rec := &recEmitter{}
	cp := newFIBTestCP(rec)

	cp.emitFIBDrift("l1", 0)  // baseline → baseline: no event
	cp.emitFIBDrift("l1", 5)  // baseline → drifted: fib-drift (Gap=5)
	cp.emitFIBDrift("l1", 3)  // drifted → drifted: no event (already drifted)
	cp.emitFIBDrift("l1", -8) // still drifted: no event
	cp.emitFIBDrift("l1", 0)  // drifted → baseline: fib-drift-cleared
	cp.emitFIBDrift("l1", 0)  // baseline → baseline: no event

	got := rec.ops()
	want := []string{"fib-drift", "fib-drift-cleared"}
	if len(got) != len(want) {
		t.Fatalf("emitted ops = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("op[%d] = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}

	// The onset event carries the drift magnitude (unsigned).
	rec.mu.Lock()
	onset := rec.events[0]
	rec.mu.Unlock()
	if onset.Gap != 5 || onset.Edge != "l1" || onset.Reason != "bird-vpp-fib-mismatch" {
		t.Fatalf("onset event = %+v, want Gap=5 Edge=l1 Reason=bird-vpp-fib-mismatch", onset)
	}
}

// TestEmitFIBDriftPerEdge proves the transition state is keyed per edge — a drift on
// one edge does not suppress another edge's first drift event.
func TestEmitFIBDriftPerEdge(t *testing.T) {
	rec := &recEmitter{}
	cp := newFIBTestCP(rec)

	cp.emitFIBDrift("l1", 4) // fib-drift (l1)
	cp.emitFIBDrift("l2", 7) // fib-drift (l2) — independent edge, must fire
	if got := rec.ops(); len(got) != 2 {
		t.Fatalf("want 2 independent fib-drift events, got %v", got)
	}
}
