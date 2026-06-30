// Package shard assigns each edge to a set of covering controllers via
// rendezvous (highest-random-weight, HRW) hashing — the "领任务" of the
// controller-sharding HA model (DESIGN-liveness §8/§10).
//
// Every controller replica holds the same view of the live controller set (from
// the etcd membership registry) and computes the SAME assignment locally, with no
// coordination: for each edge, the K controllers with the highest hash of
// (edge, controller) cover it. Two properties make this the right primitive:
//
//   - **Deterministic & coordination-free**: same inputs → same map on every
//     replica, so each replica independently knows which edges it covers and
//     which controllers cover any given edge (to tell an agent its coverers).
//   - **Minimal reshuffle**: adding/removing one controller only moves the edges
//     whose top-K membership actually changes — unlike modulo hashing, the rest
//     stay put (HRW's monotonicity).
//
// Coverage is K-redundant: each edge is covered by K controllers at once (K=2 in
// the design), so a controller death leaves the edge still covered with zero
// gap; actions are deduped via the Yugabyte version-CAS (ybstore.UpdateCAS/DeleteCAS gated on the pool's version column), so two coverers acting is harmless.
package shard

import (
	"hash/fnv"
	"sort"
)

// score is the HRW weight of (edge, controller). Higher score = preferred. The
// NUL separator keeps "ab"+"c" distinct from "a"+"bc". FNV-1a alone has weak
// avalanche (the trailing input byte biases the result, skewing the balance), so
// the FNV sum is run through the splitmix64 finalizer to uniformize it.
func score(edge, controller string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(edge))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(controller))
	return mix64(h.Sum64())
}

// mix64 is the splitmix64 finalizer: a strong bijective bit-mixer that gives the
// near-uniform avalanche HRW needs (FNV-1a's own avalanche is too weak).
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// Coverers returns the K controllers that cover edge, highest HRW score first.
// Returns min(k, len(controllers)) ids (k<=0 → none). Ties (equal score) break
// by controller id so the result is fully deterministic. The input slice is not
// modified.
func Coverers(edge string, controllers []string, k int) []string {
	if k <= 0 || len(controllers) == 0 {
		return nil
	}
	type cs struct {
		id string
		sc uint64
	}
	ranked := make([]cs, len(controllers))
	for i, c := range controllers {
		ranked[i] = cs{id: c, sc: score(edge, c)}
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].sc != ranked[j].sc {
			return ranked[i].sc > ranked[j].sc // higher score first
		}
		return ranked[i].id < ranked[j].id // tie-break deterministically
	})
	if k > len(ranked) {
		k = len(ranked)
	}
	out := make([]string, k)
	for i := 0; i < k; i++ {
		out[i] = ranked[i].id
	}
	return out
}

// Covers reports whether self is one of edge's K coverers given the controller
// set — i.e. whether this replica should tap/act on edge.
func Covers(self, edge string, controllers []string, k int) bool {
	for _, c := range Coverers(edge, controllers, k) {
		if c == self {
			return true
		}
	}
	return false
}

// CoveredEdges returns the subset of edges that self covers (the edges this
// replica should tap), preserving the input order.
func CoveredEdges(self string, edges, controllers []string, k int) []string {
	var out []string
	for _, e := range edges {
		if Covers(self, e, controllers, k) {
			out = append(out, e)
		}
	}
	return out
}

// Coverage returns the full edge→coverers map (each value highest-score first).
// Used by the assigner / to look up an agent's coverers (primary = index 0).
func Coverage(edges, controllers []string, k int) map[string][]string {
	out := make(map[string][]string, len(edges))
	for _, e := range edges {
		out[e] = Coverers(e, controllers, k)
	}
	return out
}
