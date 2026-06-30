package server

import (
	"net/netip"
	"testing"
	"time"

	"github.com/fivetime/sbw-contract/model"
)

// TestMemberPresenceReaperDetectsLapse proves the sweepExpired reaper fires when a member's
// lease lapses (a missed Withdrawal the LIVE Down=true path never delivered) WHILE the
// family view stays trustworthy via a sibling refresh — the case the `before := anyLive`
// bug made undetectable (the reaper was dead code).
func TestMemberPresenceReaperDetectsLapse(t *testing.T) {
	now := time.Unix(1000, 0)
	mp := newMemberPresence(func() time.Time { return now }, time.Minute)
	edge := model.EdgeID("l1")
	m1 := netip.MustParsePrefix("172.16.0.7/32")
	m2 := netip.MustParsePrefix("172.16.0.8/32") // sibling keeps the (edge,family) view fresh

	mp.markPresent(edge, "covA", m1)
	mp.markPresent(edge, "covA", m2)

	// Past m1's lease; refresh ONLY m2 (so the view stays valid via covA) — m1 stops
	// being refreshed, i.e. it was withdrawn but no LIVE Down=true arrived.
	now = now.Add(2 * time.Minute)
	mp.markPresent(edge, "covA", m2)

	losses := mp.sweepExpired()
	if len(losses) != 1 || losses[0].member != m1 || losses[0].edge != edge {
		t.Fatalf("expected exactly m1 to lapse, got %v", losses)
	}
	// m1 is now reaped; a second sweep must NOT re-report it.
	if l := mp.sweepExpired(); len(l) != 0 {
		t.Fatalf("lapse must fire once, second sweep got %v", l)
	}
}

// TestMemberPresenceKCovererDedup proves a member stays present (no lapse) while ANY of its
// K coverers keeps refreshing — only the loss of the LAST coverer transitions it absent.
func TestMemberPresenceKCovererDedup(t *testing.T) {
	now := time.Unix(1000, 0)
	mp := newMemberPresence(func() time.Time { return now }, time.Minute)
	edge := model.EdgeID("l1")
	m := netip.MustParsePrefix("172.16.0.7/32")

	if !mp.markPresent(edge, "covA", m) {
		t.Fatal("first coverer's assertion must be the absent→present transition")
	}
	if mp.markPresent(edge, "covB", m) {
		t.Fatal("second coverer asserting an already-present member must NOT re-transition")
	}

	// covA lapses but covB keeps refreshing → member still present, no loss.
	now = now.Add(2 * time.Minute)
	mp.markPresent(edge, "covB", m)
	if l := mp.sweepExpired(); len(l) != 0 {
		t.Fatalf("member must stay present while covB refreshes, got loss %v", l)
	}
}
