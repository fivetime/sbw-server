package shard

import (
	"fmt"
	"testing"
)

func ids(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return out
}

func TestCoverersDeterministicAndSized(t *testing.T) {
	ctrls := ids("ctrl-", 5)
	a := Coverers("edge-7", ctrls, 2)
	b := Coverers("edge-7", ctrls, 2)
	if len(a) != 2 {
		t.Fatalf("want 2 coverers, got %v", a)
	}
	if a[0] != b[0] || a[1] != b[1] {
		t.Errorf("non-deterministic: %v vs %v", a, b)
	}
	// k larger than the set → all controllers, no dup.
	all := Coverers("edge-7", ctrls, 99)
	if len(all) != len(ctrls) {
		t.Errorf("k>set should return all %d, got %d", len(ctrls), len(all))
	}
	seen := map[string]bool{}
	for _, c := range all {
		if seen[c] {
			t.Errorf("duplicate coverer %s", c)
		}
		seen[c] = true
	}
	// degenerate inputs.
	if Coverers("e", ctrls, 0) != nil || Coverers("e", nil, 2) != nil {
		t.Error("k<=0 or empty set must yield nil")
	}
}

func TestCoversConsistentWithCoverers(t *testing.T) {
	ctrls := ids("ctrl-", 4)
	for i := 0; i < 50; i++ {
		e := fmt.Sprintf("edge-%d", i)
		cov := Coverers(e, ctrls, 2)
		inSet := map[string]bool{cov[0]: true, cov[1]: true}
		for _, c := range ctrls {
			if Covers(c, e, ctrls, 2) != inSet[c] {
				t.Errorf("Covers(%s,%s) disagrees with Coverers %v", c, e, cov)
			}
		}
	}
}

func TestBalancedDistribution(t *testing.T) {
	const M, K, N = 5, 2, 5000
	ctrls := ids("ctrl-", M)
	edges := ids("e-", N)
	load := map[string]int{}
	for _, e := range edges {
		for _, c := range Coverers(e, ctrls, K) {
			load[c]++
		}
	}
	want := K * N / M // 2000
	for _, c := range ctrls {
		got := load[c]
		// within ±20% of the ideal even split.
		if got < want*8/10 || got > want*12/10 {
			t.Errorf("controller %s load %d not within ±20%% of %d", c, got, want)
		}
	}
}

func TestMinimalReshuffleOnRemoval(t *testing.T) {
	const K, N = 2, 3000
	ctrls := ids("ctrl-", 6)
	edges := ids("e-", N)
	removed := ctrls[3]
	after := append(append([]string{}, ctrls[:3]...), ctrls[4:]...)

	moved, unaffectedStable := 0, 0
	for _, e := range edges {
		before := Coverers(e, ctrls, K)
		coveredByRemoved := before[0] == removed || before[1] == removed
		now := Coverers(e, after, K)
		if coveredByRemoved {
			moved++
		} else {
			// HRW monotonicity: an edge NOT covered by the removed controller
			// must keep the exact same coverers.
			if now[0] != before[0] || now[1] != before[1] {
				t.Errorf("edge %s not covered by removed but coverers changed: %v → %v", e, before, now)
			} else {
				unaffectedStable++
			}
		}
	}
	// Only edges the removed controller covered should reshuffle (~K*N/M of them).
	if moved == 0 || moved > N/2 {
		t.Errorf("unexpected reshuffle count: %d of %d", moved, N)
	}
	if unaffectedStable == 0 {
		t.Error("expected many edges unaffected by the removal")
	}
}

func TestCoveredEdgesAndCoverage(t *testing.T) {
	ctrls := ids("ctrl-", 3)
	edges := ids("e-", 30)
	cov := Coverage(edges, ctrls, 2)
	// Every controller's CoveredEdges = exactly the edges where it's a coverer.
	for _, c := range ctrls {
		ce := map[string]bool{}
		for _, e := range CoveredEdges(c, edges, ctrls, 2) {
			ce[e] = true
		}
		for _, e := range edges {
			want := cov[e][0] == c || cov[e][1] == c
			if ce[e] != want {
				t.Errorf("CoveredEdges(%s) mismatch for %s", c, e)
			}
		}
	}
}
