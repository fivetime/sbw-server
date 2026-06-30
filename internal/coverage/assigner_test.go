package coverage

import (
	"context"
	"testing"

	"github.com/fivetime/sbw-server/internal/shard"
)

type fakeEndpoints struct{ m map[string]string }

func (f *fakeEndpoints) Endpoints(context.Context) (map[string]string, error) { return f.m, nil }

// Assign returns the edge's HRW coverers resolved to endpoints, primary first.
func TestAssignPrimaryAndFallback(t *testing.T) {
	ctrls := []string{"ctrl-a", "ctrl-b", "ctrl-c"}
	rec := New("ctrl-a", 2, &fakeMembers{ids: ctrls}, &fakeEdges{}, &fakeTap{})
	ep := &fakeEndpoints{m: map[string]string{"ctrl-a": "a:1791", "ctrl-b": "b:1791", "ctrl-c": "c:1791"}}
	a := NewAssigner(rec, ep)

	for _, e := range edges(10) {
		got, err := a.Assign(context.Background(), e)
		if err != nil {
			t.Fatal(err)
		}
		want := shard.Coverers(string(e), ctrls, 2)
		if len(got.Coverers) != 2 {
			t.Fatalf("Assign(%s) got %d coverers, want 2", e, len(got.Coverers))
		}
		// primary == HRW rank 0, with the right endpoint.
		p, ok := got.Primary()
		if !ok || p.ControllerID != want[0] || p.GRPCEndpoint != ep.m[want[0]] {
			t.Errorf("Assign(%s) primary = %+v, want %s", e, p, want[0])
		}
		if got.EdgeID != e {
			t.Errorf("Assign edge id = %s, want %s", got.EdgeID, e)
		}
	}
}

// An unknown endpoint (raced membership) still lists the id, with empty endpoint.
func TestAssignUnknownEndpoint(t *testing.T) {
	ctrls := []string{"ctrl-a", "ctrl-b"}
	rec := New("ctrl-a", 2, &fakeMembers{ids: ctrls}, &fakeEdges{}, &fakeTap{})
	ep := &fakeEndpoints{m: map[string]string{"ctrl-a": "a:1791"}} // ctrl-b missing
	a := NewAssigner(rec, ep)
	got, err := a.Assign(context.Background(), "edge-x")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range got.Coverers {
		if c.ControllerID == "ctrl-b" && c.GRPCEndpoint != "" {
			t.Errorf("ctrl-b endpoint should be empty (unknown), got %q", c.GRPCEndpoint)
		}
	}
}
