package coverage

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-server/internal/shard"
)

// fakeMembers / fakeEdges / fakeTap are in-memory stand-ins so the coverage
// logic is testable without etcd or GoBGP.
type fakeMembers struct{ ids []string }

func (f *fakeMembers) IDs(context.Context) ([]string, error) { return f.ids, nil }

type fakeEdges struct{ ids []model.EdgeID }

func (f *fakeEdges) EdgeIDs(context.Context) ([]model.EdgeID, error) { return f.ids, nil }

type fakeTap struct{ last []model.EdgeID }

func (f *fakeTap) Ensure(_ context.Context, e []model.EdgeID) error {
	f.last = append([]model.EdgeID(nil), e...)
	return nil
}

func edges(n int) []model.EdgeID {
	out := make([]model.EdgeID, n)
	for i := range out {
		out[i] = model.EdgeID(string(rune('A'+i)) + "-edge")
	}
	return out
}

func strs(es []model.EdgeID) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = string(e)
	}
	sort.Strings(out)
	return out
}

// Reconcile must drive the tap with EXACTLY the edges shard says this replica
// covers — no more, no less — cross-checked against the shard primitive.
func TestReconcileEnsuresCoveredSubset(t *testing.T) {
	ctrls := []string{"ctrl-a", "ctrl-b", "ctrl-c"}
	es := edges(12)
	for _, self := range ctrls {
		tap := &fakeTap{}
		r := New(self, 2, &fakeMembers{ids: ctrls}, &fakeEdges{ids: es}, tap)
		if err := r.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		// Expected = shard.CoveredEdges for this replica.
		want := shard.CoveredEdges(self, strsRaw(es), ctrls, 2)
		got := strs(tap.last)
		sort.Strings(want)
		if len(got) != len(want) {
			t.Fatalf("%s covered %v, want %v", self, got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("%s covered %v, want %v", self, got, want)
			}
		}
	}
}

// Every edge must be covered by exactly K replicas across the fleet (K=2): the
// union of all replicas' Ensure sets, counted per edge, equals K.
func TestKRedundantAcrossFleet(t *testing.T) {
	ctrls := []string{"ctrl-a", "ctrl-b", "ctrl-c", "ctrl-d"}
	es := edges(20)
	count := map[model.EdgeID]int{}
	for _, self := range ctrls {
		tap := &fakeTap{}
		r := New(self, 2, &fakeMembers{ids: ctrls}, &fakeEdges{ids: es}, tap)
		if err := r.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, e := range tap.last {
			count[e]++
		}
	}
	for _, e := range es {
		if count[e] != 2 {
			t.Errorf("edge %s covered by %d replicas, want K=2", e, count[e])
		}
	}
}

// CoverersOf returns primary (index 0) + fallback, matching shard.Coverers —
// this is what an agent is told as {primary, fallback...}.
func TestCoverersOfMatchesShard(t *testing.T) {
	ctrls := []string{"ctrl-a", "ctrl-b", "ctrl-c"}
	r := New("ctrl-a", 2, &fakeMembers{ids: ctrls}, &fakeEdges{}, &fakeTap{})
	for _, e := range edges(10) {
		got, err := r.CoverersOf(context.Background(), e)
		if err != nil {
			t.Fatal(err)
		}
		want := shard.Coverers(string(e), ctrls, 2)
		if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
			t.Errorf("CoverersOf(%s)=%v, want %v", e, got, want)
		}
		// Covers is consistent: self covers iff self is in CoverersOf.
		cov, _ := r.Covers(context.Background(), e)
		inSet := got[0] == "ctrl-a" || got[1] == "ctrl-a"
		if cov != inSet {
			t.Errorf("Covers(%s)=%v but CoverersOf=%v", e, cov, got)
		}
	}
}

// A membership change (one replica added) must reshuffle the covered set the
// reconciler drives — coverage is recomputed live from members each call.
func TestMembershipChangeReshuffles(t *testing.T) {
	es := edges(20)
	members := &fakeMembers{ids: []string{"ctrl-a", "ctrl-b"}}
	tap := &fakeTap{}
	r := New("ctrl-a", 2, members, &fakeEdges{ids: es}, tap)

	_ = r.Reconcile(context.Background())
	before := strs(tap.last)
	// With 2 replicas and K=2, ctrl-a covers ALL edges (both cover everything).
	if len(before) != len(es) {
		t.Fatalf("with 2 replicas K=2, ctrl-a should cover all %d, got %d", len(es), len(before))
	}

	members.ids = []string{"ctrl-a", "ctrl-b", "ctrl-c", "ctrl-d"}
	_ = r.Reconcile(context.Background())
	after := strs(tap.last)
	// Now ctrl-a only covers ~half; the set must shrink.
	if len(after) >= len(before) {
		t.Errorf("after adding replicas ctrl-a coverage should shrink: %d → %d", len(before), len(after))
	}
}

// countingTap counts Ensure calls concurrency-safely for the Run loop test.
type countingTap struct{ n atomic.Int64 }

func (c *countingTap) Ensure(context.Context, []model.EdgeID) error {
	c.n.Add(1)
	return nil
}

// Run reconciles once at startup, then on each trigger, until ctx is done.
func TestRunReconcilesOnStartupAndTrigger(t *testing.T) {
	tap := &countingTap{}
	r := New("ctrl-a", 2, &fakeMembers{ids: []string{"ctrl-a"}}, &fakeEdges{ids: edges(3)}, tap)
	trigger := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); r.Run(ctx, trigger, 0, nil) }()

	// startup reconcile should land quickly.
	waitFor(t, func() bool { return tap.n.Load() >= 1 }, "startup reconcile")
	trigger <- struct{}{}
	trigger <- struct{}{}
	waitFor(t, func() bool { return tap.n.Load() >= 3 }, "reconcile per trigger")

	cancel()
	wg.Wait()
}

// A closed trigger channel stops triggering but the loop keeps running on its
// timer and exits cleanly on ctx cancel.
func TestRunSurvivesClosedTrigger(t *testing.T) {
	tap := &countingTap{}
	r := New("ctrl-a", 2, &fakeMembers{ids: []string{"ctrl-a"}}, &fakeEdges{ids: edges(2)}, tap)
	trigger := make(chan struct{})
	close(trigger)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() { r.Run(ctx, trigger, 20*time.Millisecond, nil); close(done) }()

	waitFor(t, func() bool { return tap.n.Load() >= 2 }, "timer keeps reconciling after closed trigger")
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit on ctx cancel")
	}
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func strsRaw(es []model.EdgeID) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = string(e)
	}
	return out
}
