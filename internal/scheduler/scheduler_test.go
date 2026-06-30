package scheduler

import (
	"context"
	"errors"
	"testing"

	"github.com/fivetime/sbw-contract/model"
)

func remFn(m map[model.EdgeID]int64) Remaining {
	return func(_ context.Context, e model.EdgeID) (int64, error) { return m[e], nil }
}

func TestSelectHomesDistinctWithCapacity(t *testing.T) {
	cands := []model.EdgeID{"e2", "e5", "e9"}
	got, err := SelectHomes(context.Background(), cands,
		remFn(map[model.EdgeID]int64{"e2": 500, "e5": 800, "e9": 300}), 400, 2)
	if err != nil {
		t.Fatal(err)
	}
	// worst-fit: most free first → e5(800), e2(500). e9(300<400) excluded.
	if len(got) != 2 || got[0] != "e5" || got[1] != "e2" {
		t.Errorf("got %v, want [e5 e2]", got)
	}
	if got[0] == got[1] {
		t.Error("primary and backup must be distinct")
	}
}

func TestSelectHomesInsufficient(t *testing.T) {
	// Only one agent has >= need → can't place a primary+backup pair.
	_, err := SelectHomes(context.Background(), []model.EdgeID{"e2", "e5"},
		remFn(map[model.EdgeID]int64{"e2": 1000, "e5": 100}), 500, 2)
	if !errors.Is(err, ErrInsufficientCapacity) {
		t.Errorf("want ErrInsufficientCapacity, got %v", err)
	}
}

func TestSelectHomesDedupCandidates(t *testing.T) {
	got, err := SelectHomes(context.Background(), []model.EdgeID{"e2", "e2", "e5"},
		remFn(map[model.EdgeID]int64{"e2": 900, "e5": 600}), 100, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] == got[1] {
		t.Errorf("duplicate candidate must not yield duplicate homes: %v", got)
	}
}

func TestSelectHomesTieBreakSpreads(t *testing.T) {
	// Equal free → RANDOM tie-break (not edge id), so the primary spreads across the tied
	// candidates instead of piling onto the lowest id (the create-burst skew this fixes).
	const calls = 600
	primary := map[model.EdgeID]int{}
	for i := 0; i < calls; i++ {
		got, err := SelectHomes(context.Background(), []model.EdgeID{"e9", "e2", "e5"},
			remFn(map[model.EdgeID]int64{"e2": 500, "e5": 500, "e9": 500}), 100, 2)
		if err != nil {
			t.Fatal(err)
		}
		if got[0] == got[1] {
			t.Fatalf("homes must be distinct: %v", got)
		}
		primary[got[0]]++
	}
	// Uniform is ~200 each; require >=100 (>8σ below the mean, so no flake) — a regression
	// to a fixed tie-break (one edge primary all 600 times, the others 0) fails loudly.
	for _, e := range []model.EdgeID{"e2", "e5", "e9"} {
		if primary[e] < 100 {
			t.Errorf("tie-break not spreading: %s was primary only %d/%d times (want >=100)", e, primary[e], calls)
		}
	}
}

func TestSelectHomesErrorPropagates(t *testing.T) {
	boom := errors.New("etcd down")
	_, err := SelectHomes(context.Background(), []model.EdgeID{"e2"},
		func(_ context.Context, _ model.EdgeID) (int64, error) { return 0, boom }, 1, 1)
	if !errors.Is(err, boom) {
		t.Errorf("rem error must propagate, got %v", err)
	}
}
