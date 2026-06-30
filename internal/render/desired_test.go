package render

import (
	"math/rand"
	"net/netip"
	"reflect"
	"testing"

	"github.com/fivetime/sbw-contract/model"
)

func mem(p string) model.Member { return model.Member{Prefix: netip.MustParsePrefix(p)} }

func rateLimitPool(id model.PoolID, home model.EdgeID, members ...model.Member) model.Pool {
	return model.Pool{
		ID: id, HomeEdge: home, Members: members,
		Action:      model.ActionSpec{Kind: model.ActionRateLimit},
		IngressRate: model.RateSpec{Type: model.RateKbps, CIR: 1_000_000, CommittedBurstBytes: 12_500_000},
		EgressRate:  model.RateSpec{Type: model.RateKbps, CIR: 2_000_000, CommittedBurstBytes: 25_000_000},
	}
}

func edgeAddrs() map[model.EdgeID]netip.Addr {
	return map[model.EdgeID]netip.Addr{
		"edge-2": netip.MustParseAddr("10.0.2.1"),
		"edge-5": netip.MustParseAddr("10.0.5.1"),
	}
}

func edgeAddrs6() map[model.EdgeID]netip.Addr {
	return map[model.EdgeID]netip.Addr{
		"edge-2": netip.MustParseAddr("2001:db8:2::1"),
		"edge-5": netip.MustParseAddr("2001:db8:5::1"),
	}
}

func TestRenderRateLimitDualHoming(t *testing.T) {
	pools := []model.Pool{rateLimitPool(200, "edge-2", mem("203.0.113.0/24"), mem("198.51.100.5/32"))}
	states, err := DesiredStates(pools, Options{Generation: 7, EdgeAddrs: edgeAddrs()})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	st, ok := states["edge-2"]
	if !ok || len(states) != 1 {
		t.Fatalf("want only edge-2 state, got %v", keys(states))
	}
	if st.Generation != 7 || st.SchemaVersion != model.SchemaVersion {
		t.Errorf("envelope wrong: %+v", st)
	}
	if len(st.Policers) != 2 {
		t.Errorf("want 2 policers (in+eg), got %d", len(st.Policers))
	}
	if len(st.ClassifySessions) != 4 { // 2 members × 2 directions
		t.Errorf("want 4 classify sessions, got %d", len(st.ClassifySessions))
	}
	// §5.1 dual homing: same members in anchors (ingress) AND flow_redirects (egress).
	if len(st.Anchors) != 2 {
		t.Errorf("want 2 anchors, got %d", len(st.Anchors))
	}
	if len(st.FlowRedirects) != 2 {
		t.Errorf("want 2 flow_redirects, got %d", len(st.FlowRedirects))
	}
	if st.RedirectNextHop != netip.MustParseAddr("10.0.2.1") {
		t.Errorf("redirect next-hop = %v, want 10.0.2.1", st.RedirectNextHop)
	}
	anchorSet := prefixSet(anchorPrefixes(st))
	flowSet := prefixSet(flowPrefixes(st))
	if !reflect.DeepEqual(anchorSet, flowSet) {
		t.Errorf("ingress anchors %v and egress flow_redirects %v must cover the same members", anchorSet, flowSet)
	}
	// The output must satisfy the frozen contract (classify→policer refs, etc.).
	if err := st.Validate(); err != nil {
		t.Errorf("rendered state fails contract validation: %v", err)
	}
}

func TestRenderSeparateHomeEdges(t *testing.T) {
	pools := []model.Pool{
		rateLimitPool(200, "edge-2", mem("203.0.113.0/24")),
		rateLimitPool(300, "edge-5", mem("198.51.100.0/24")),
	}
	states, err := DesiredStates(pools, Options{Generation: 1, EdgeAddrs: edgeAddrs()})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("want 2 edge states, got %v", keys(states))
	}
	if states["edge-2"].RedirectNextHop != netip.MustParseAddr("10.0.2.1") ||
		states["edge-5"].RedirectNextHop != netip.MustParseAddr("10.0.5.1") {
		t.Error("each edge must redirect to its own address")
	}
}

func TestRenderMultiplePoolsSameEdgeAggregate(t *testing.T) {
	pools := []model.Pool{
		rateLimitPool(200, "edge-2", mem("203.0.113.0/24")),
		rateLimitPool(300, "edge-2", mem("198.51.100.0/24")),
	}
	states, _ := DesiredStates(pools, Options{EdgeAddrs: edgeAddrs()})
	st := states["edge-2"]
	if len(st.Policers) != 4 || len(st.Anchors) != 2 || len(st.FlowRedirects) != 2 {
		t.Errorf("two pools should aggregate: %d policers, %d anchors, %d flows", len(st.Policers), len(st.Anchors), len(st.FlowRedirects))
	}
	if err := st.Validate(); err != nil {
		t.Errorf("aggregate state invalid: %v", err)
	}
}

func TestRenderBlackhole(t *testing.T) {
	rtbh := model.Community{ASN: 65000, Value: 666}
	pools := []model.Pool{{
		ID: 400, HomeEdge: "edge-2", Members: []model.Member{mem("203.0.113.7/32")},
		Action: model.ActionSpec{Kind: model.ActionBlackhole, RTBHCommunity: &rtbh},
	}}
	states, err := DesiredStates(pools, Options{})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	st := states["edge-2"]
	if len(st.Policers) != 0 || len(st.ClassifySessions) != 0 || len(st.FlowRedirects) != 0 {
		t.Errorf("blackhole has no policer/classify/redirect: %+v", st)
	}
	if len(st.Anchors) != 1 || len(st.Anchors[0].Communities) != 1 || st.Anchors[0].Communities[0] != rtbh {
		t.Errorf("blackhole anchor must carry the RTBH community: %+v", st.Anchors)
	}
}

// TestRenderBlackholeLargeCommunity covers a 32-bit-ASN carrier: the RTBH signal
// must ride as a large community since the ASN does not fit a standard community.
func TestRenderBlackholeLargeCommunity(t *testing.T) {
	// carrier AS 4231457290 (32-bit) RTBH: <asn>:666:0
	lc := model.LargeCommunity{GlobalAdmin: 4231457290, LocalData1: 666, LocalData2: 0}
	pools := []model.Pool{{
		ID: 401, HomeEdge: "edge-2", Members: []model.Member{mem("203.0.113.8/32")},
		Action: model.ActionSpec{Kind: model.ActionBlackhole, RTBHLargeCommunity: &lc},
	}}
	states, err := DesiredStates(pools, Options{})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	st := states["edge-2"]
	if len(st.Policers) != 0 || len(st.ClassifySessions) != 0 || len(st.FlowRedirects) != 0 {
		t.Errorf("blackhole has no policer/classify/redirect: %+v", st)
	}
	if len(st.Anchors) != 1 {
		t.Fatalf("want 1 anchor, got %+v", st.Anchors)
	}
	if len(st.Anchors[0].Communities) != 0 {
		t.Errorf("32-bit-ASN RTBH must not emit a standard community: %+v", st.Anchors[0].Communities)
	}
	if len(st.Anchors[0].LargeCommunities) != 1 || st.Anchors[0].LargeCommunities[0] != lc {
		t.Errorf("blackhole anchor must carry the RTBH large community: %+v", st.Anchors)
	}
}

// TestRenderBlackholeBothCommunities: standard + large may coexist (e.g. a
// 16-bit lab upstream and a 32-bit production carrier both peered off the MX).
func TestRenderBlackholeBothCommunities(t *testing.T) {
	rtbh := model.Community{ASN: 65000, Value: 666}
	lc := model.LargeCommunity{GlobalAdmin: 4231457290, LocalData1: 666, LocalData2: 0}
	pools := []model.Pool{{
		ID: 402, HomeEdge: "edge-2", Members: []model.Member{mem("203.0.113.9/32")},
		Action: model.ActionSpec{Kind: model.ActionBlackhole, RTBHCommunity: &rtbh, RTBHLargeCommunity: &lc},
	}}
	states, err := DesiredStates(pools, Options{})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	a := states["edge-2"].Anchors
	if len(a) != 1 || len(a[0].Communities) != 1 || a[0].Communities[0] != rtbh ||
		len(a[0].LargeCommunities) != 1 || a[0].LargeCommunities[0] != lc {
		t.Errorf("blackhole anchor must carry both communities: %+v", a)
	}
}

func TestRenderIPv6MemberFlowRedirect(t *testing.T) {
	pools := []model.Pool{rateLimitPool(200, "edge-2", mem("2001:db8::5/128"))}
	states, err := DesiredStates(pools, Options{EdgeAddrs: edgeAddrs(), EdgeAddrs6: edgeAddrs6()})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	st := states["edge-2"]
	if len(st.Anchors) != 1 || len(st.ClassifySessions) != 2 {
		t.Errorf("v6 member should get anchor + classify: %+v", st)
	}
	// v6 egress homing: a flow_redirect + the v6 redirect next-hop (RFC 5701), and
	// NO v4 next-hop (pool has no v4 members).
	if len(st.FlowRedirects) != 1 || st.FlowRedirects[0].SrcPrefix != netip.MustParsePrefix("2001:db8::5/128") {
		t.Errorf("v6 member should get a flow_redirect: %+v", st.FlowRedirects)
	}
	if st.RedirectNextHopV6 != netip.MustParseAddr("2001:db8:2::1") {
		t.Errorf("v6 redirect next-hop = %v, want 2001:db8:2::1", st.RedirectNextHopV6)
	}
	if st.RedirectNextHop.IsValid() {
		t.Errorf("no v4 members → v4 redirect next-hop must be empty, got %v", st.RedirectNextHop)
	}
	if err := st.Validate(); err != nil {
		t.Errorf("v6 state invalid: %v", err)
	}
}

// A pool mixing v4 and v6 members emits BOTH families' redirects + next-hops.
func TestRenderMixedV4V6FlowRedirect(t *testing.T) {
	pools := []model.Pool{rateLimitPool(200, "edge-2", mem("203.0.113.5/32"), mem("2001:db8::5/128"))}
	states, err := DesiredStates(pools, Options{EdgeAddrs: edgeAddrs(), EdgeAddrs6: edgeAddrs6()})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	st := states["edge-2"]
	if len(st.FlowRedirects) != 2 {
		t.Fatalf("mixed pool should emit 2 flow_redirects, got %d", len(st.FlowRedirects))
	}
	if !st.RedirectNextHop.Is4() || !st.RedirectNextHopV6.Is6() {
		t.Errorf("mixed pool needs both next-hops: v4=%v v6=%v", st.RedirectNextHop, st.RedirectNextHopV6)
	}
	if err := st.Validate(); err != nil {
		t.Errorf("mixed state invalid: %v", err)
	}
}

// A v6 member homed to an edge without an EdgeAddrs6 entry is a render error.
func TestRenderIPv6MissingNextHopErrors(t *testing.T) {
	pools := []model.Pool{rateLimitPool(200, "edge-2", mem("2001:db8::5/128"))}
	if _, err := DesiredStates(pools, Options{EdgeAddrs: edgeAddrs()}); err == nil {
		t.Fatal("v6 member with no EdgeAddrs6 should error")
	}
}

func TestRenderHomeMarkerOnAnchors(t *testing.T) {
	marker := model.LargeCommunity{GlobalAdmin: 65010, LocalData1: 101, LocalData2: 2}
	pools := []model.Pool{rateLimitPool(200, "edge-2", mem("203.0.113.0/24"))}
	states, _ := DesiredStates(pools, Options{
		EdgeAddrs:  edgeAddrs(),
		HomeMarker: func(e model.EdgeID) (model.LargeCommunity, bool) { return marker, e == "edge-2" },
	})
	a := states["edge-2"].Anchors[0]
	if len(a.LargeCommunities) != 1 || a.LargeCommunities[0] != marker {
		t.Errorf("rate-limit anchor must carry the home marker, got %+v", a)
	}
}

func TestRenderDeterministicUnderShuffle(t *testing.T) {
	pools := []model.Pool{
		rateLimitPool(200, "edge-2", mem("203.0.113.0/24"), mem("198.51.100.0/24"), mem("192.0.2.0/24")),
		rateLimitPool(300, "edge-5", mem("10.1.0.0/24")),
	}
	want, _ := DesiredStates(pools, Options{Generation: 1, EdgeAddrs: edgeAddrs()})
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 20; i++ {
		sh := append([]model.Pool(nil), pools...)
		r.Shuffle(len(sh), func(a, b int) { sh[a], sh[b] = sh[b], sh[a] })
		// shuffle members too
		for pi := range sh {
			ms := append([]model.Member(nil), sh[pi].Members...)
			r.Shuffle(len(ms), func(a, b int) { ms[a], ms[b] = ms[b], ms[a] })
			sh[pi].Members = ms
		}
		got, _ := DesiredStates(sh, Options{Generation: 1, EdgeAddrs: edgeAddrs()})
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("non-deterministic output for shuffle %d", i)
		}
	}
}

func TestForEdgePrimaryFullBackupStandby(t *testing.T) {
	primary := []model.Pool{rateLimitPool(200, "edge-2", mem("203.0.113.0/24"), mem("198.51.100.5/32"))}
	// A pool whose PRIMARY home is edge-5, but for which edge-2 is the BACKUP:
	// edge-2 pre-builds its limiting machinery but must NOT advertise anything.
	backup := []model.Pool{rateLimitPool(300, "edge-5", mem("192.0.2.0/24"))}

	st, err := ForEdge("edge-2", primary, backup, Options{Generation: 9, EdgeAddrs: edgeAddrs()})
	if err != nil {
		t.Fatalf("ForEdge: %v", err)
	}
	if st.EdgeID != "edge-2" || st.Generation != 9 {
		t.Errorf("envelope wrong: %+v", st)
	}
	// Primary pool 200: 2 policers + 4 classify + 2 anchors + 2 flow_redirects.
	// Backup pool 300: 2 policers + 2 classify, and NOTHING advertised.
	if len(st.Policers) != 4 {
		t.Errorf("want 4 policers (primary 2 + backup 2), got %d", len(st.Policers))
	}
	if len(st.ClassifySessions) != 6 { // primary 2×2 + backup 1×2
		t.Errorf("want 6 classify sessions, got %d", len(st.ClassifySessions))
	}
	// §5.3: backup pre-builds machinery but never advertises — so anchors and
	// flow_redirects come from the PRIMARY pool only.
	if len(st.Anchors) != 2 {
		t.Errorf("want 2 anchors (primary only), got %d", len(st.Anchors))
	}
	if len(st.FlowRedirects) != 2 {
		t.Errorf("want 2 flow_redirects (primary only), got %d", len(st.FlowRedirects))
	}
	// None of the backup member's prefixes may be advertised.
	for _, a := range st.Anchors {
		if a.Prefix == netip.MustParsePrefix("192.0.2.0/24") {
			t.Error("backup member 192.0.2.0/24 must NOT be advertised as an anchor")
		}
	}
	if st.RedirectNextHop != netip.MustParseAddr("10.0.2.1") {
		t.Errorf("redirect next-hop = %v, want 10.0.2.1", st.RedirectNextHop)
	}
	if err := st.Validate(); err != nil {
		t.Errorf("ForEdge state fails contract validation: %v", err)
	}
}

func TestForEdgeBlackholeBackupNoop(t *testing.T) {
	rtbh := model.Community{ASN: 65000, Value: 666}
	backup := []model.Pool{{
		ID: 400, HomeEdge: "edge-5", Members: []model.Member{mem("203.0.113.7/32")},
		Action: model.ActionSpec{Kind: model.ActionBlackhole, RTBHCommunity: &rtbh},
	}}
	st, err := ForEdge("edge-2", nil, backup, Options{})
	if err != nil {
		t.Fatalf("ForEdge: %v", err)
	}
	// A blackhole backup has nothing to pre-build (no policer/classify) and must
	// never advertise the /32 — the standby edge is completely passive.
	if len(st.Policers) != 0 || len(st.ClassifySessions) != 0 || len(st.Anchors) != 0 || len(st.FlowRedirects) != 0 {
		t.Errorf("blackhole backup must be a no-op, got %+v", st)
	}
}

func TestRenderErrors(t *testing.T) {
	// no home edge
	if _, err := DesiredStates([]model.Pool{{ID: 1, Action: model.ActionSpec{Kind: model.ActionRateLimit}}}, Options{}); err == nil {
		t.Error("missing home edge should error")
	}
	// rate-limit v4 member but no edge addr
	if _, err := DesiredStates([]model.Pool{rateLimitPool(200, "edge-9", mem("203.0.113.0/24"))}, Options{EdgeAddrs: edgeAddrs()}); err == nil {
		t.Error("missing redirect next-hop should error")
	}
	// scrub is V2
	scrub := []model.Pool{{ID: 5, HomeEdge: "edge-2", Members: []model.Member{mem("203.0.113.0/24")}, Action: model.ActionSpec{Kind: model.ActionScrub}}}
	if _, err := DesiredStates(scrub, Options{EdgeAddrs: edgeAddrs()}); err == nil {
		t.Error("scrub action should error (V2)")
	}
}

// helpers
func keys(m map[model.EdgeID]model.EdgeDesiredState) []model.EdgeID {
	var out []model.EdgeID
	for k := range m {
		out = append(out, k)
	}
	return out
}

func anchorPrefixes(s model.EdgeDesiredState) []netip.Prefix {
	var out []netip.Prefix
	for _, a := range s.Anchors {
		out = append(out, a.Prefix)
	}
	return out
}

func flowPrefixes(s model.EdgeDesiredState) []netip.Prefix {
	var out []netip.Prefix
	for _, f := range s.FlowRedirects {
		out = append(out, f.SrcPrefix)
	}
	return out
}

func prefixSet(ps []netip.Prefix) map[netip.Prefix]bool {
	m := map[netip.Prefix]bool{}
	for _, p := range ps {
		m[p] = true
	}
	return m
}

func TestRenderSuppressGate(t *testing.T) {
	// Two members; suppress the second (its host route vanished).
	gone := netip.MustParsePrefix("198.51.100.5/32")
	pools := []model.Pool{rateLimitPool(200, "edge-2", mem("203.0.113.7/32"), model.Member{Prefix: gone})}
	opt := Options{
		Generation: 1,
		EdgeAddrs:  edgeAddrs(),
		Suppress: func(edge model.EdgeID, member netip.Prefix) bool {
			return edge == "edge-2" && member == gone
		},
	}
	states, err := DesiredStates(pools, opt)
	if err != nil {
		t.Fatal(err)
	}
	st := states["edge-2"]
	// Anchor + flow_redirect only for the surviving member; suppressed one omitted.
	if len(st.Anchors) != 1 || st.Anchors[0].Prefix != netip.MustParsePrefix("203.0.113.7/32") {
		t.Errorf("suppressed member must not be advertised, anchors=%v", anchorPrefixes(st))
	}
	if len(st.FlowRedirects) != 1 || st.FlowRedirects[0].SrcPrefix != netip.MustParsePrefix("203.0.113.7/32") {
		t.Errorf("suppressed member must not be redirected, flows=%v", flowPrefixes(st))
	}
	// But the limiting machinery for BOTH members stays (resume cleanly on return).
	if len(st.Policers) != 2 || len(st.ClassifySessions) != 4 {
		t.Errorf("suppression must keep policer/classify, got %d policers %d classify", len(st.Policers), len(st.ClassifySessions))
	}
	if err := st.Validate(); err != nil {
		t.Errorf("suppressed state invalid: %v", err)
	}
}

func TestForEdgeSuppressGate(t *testing.T) {
	gone := netip.MustParsePrefix("198.51.100.5/32")
	primary := []model.Pool{rateLimitPool(200, "edge-2", mem("203.0.113.7/32"), model.Member{Prefix: gone})}
	opt := Options{
		Generation: 1, EdgeAddrs: edgeAddrs(),
		Suppress: func(_ model.EdgeID, member netip.Prefix) bool { return member == gone },
	}
	st, err := ForEdge("edge-2", primary, nil, opt)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Anchors) != 1 || len(st.FlowRedirects) != 1 {
		t.Errorf("ForEdge must suppress the gone member: anchors=%v flows=%v", anchorPrefixes(st), flowPrefixes(st))
	}
}
