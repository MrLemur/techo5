package nearest

import (
	"testing"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
)

// A committed membership of this device alone is a majority of itself: it answers with no peers and
// no clock sync, and the release fence still applies.
func TestSoleMemberAnswersWithoutPeersOrClock(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office"})
	h.n.peers = func() []announce.Peer { return nil }
	h.n.clockNow = func(time.Time) (time.Time, bool) { return time.Time{}, false }
	if reason := h.n.notAnsweringReason(); reason != "" {
		t.Fatalf("sole member reports %q", reason)
	}
	h.n.Woke(0)
	if len(h.opened) != 1 || len(h.sent) != 0 {
		t.Fatalf("sole member opened %d times, sent %d claims", len(h.opened), len(h.sent))
	}
	// A fence in progress drops the wake.
	h.n.membership.invalidate("fencing")
	h.n.Woke(0)
	if len(h.opened) != 1 {
		t.Fatal("sole member answered through a closed fence")
	}
	if reason := h.n.notAnsweringReason(); reason != "membership change in progress" {
		t.Fatalf("reason %q", reason)
	}
}

// A sole member still refuses when a visible device is in another mode.
func TestSoleMemberDropsOnModeMismatch(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office"})
	peer := announce.Peer{Name: "Kitchen", Address: "192.0.2.1", Port: 8181}
	h.n.peers = func() []announce.Peer { return []announce.Peer{peer} }
	h.n.peerStatuses.observe(word, peer, peerStatus{Mode: modeExisting, Enabled: true}, time.Now())
	h.n.Woke(0)
	if len(h.opened) != 0 || h.at0 != 0 {
		t.Fatal("sole member answered beside a device in another mode")
	}
	if reason := h.n.notAnsweringReason(); reason != "mode mismatch" {
		t.Fatalf("reason %q", reason)
	}
}

// Two committed members with the other down do not answer: only a membership of one is its own majority.
func TestTwoMembersWithOnePeerDownDoNotAnswer(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen"})
	h.n.peers = func() []announce.Peer { return nil }
	if reason := h.n.notAnsweringReason(); reason != "no majority reachable" {
		t.Fatalf("reason %q", reason)
	}
	h.n.Woke(0)
	h.wait(time.Second)
	if h.at0 != 0 || len(h.opened) != 0 {
		t.Fatal("two-member group answered with its peer down")
	}
}

func TestNotAnsweringReasons(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen", "Den"})
	h.n.clockNow = func(time.Time) (time.Time, bool) { return time.Time{}, false }
	if reason := h.n.notAnsweringReason(); reason != "waiting for clock sync" {
		t.Fatalf("reason %q", reason)
	}
	h.n.clockNow = func(at time.Time) (time.Time, bool) { return at, true }
	if reason := h.n.notAnsweringReason(); reason != "waiting for a wake coordinator" {
		t.Fatalf("reason %q", reason)
	}
	h.n.membership.addConflict(membershipInfo{Cluster: "other"})
	if reason := h.n.notAnsweringReason(); reason != "group conflict" {
		t.Fatalf("reason %q", reason)
	}
	if got := h.n.MembershipView().NotAnswering; got != "group conflict" {
		t.Fatalf("view reason %q", got)
	}
}

// A committed membership of this device alone does not answer while another device with the house word is
// visible, even one in the same mode: it is not in the group, so this device is not its own majority.
func TestSoleMemberDropsWhenAnotherDeviceQuorumPeerIsVisible(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office"})
	peer := announce.Peer{Name: "Kitchen", Address: "192.0.2.1", Port: 8181}
	h.n.peers = func() []announce.Peer { return []announce.Peer{peer} }
	h.n.peerStatuses.observe(word, peer, peerStatus{Mode: modeElected, Enabled: true}, time.Now())
	h.n.Woke(0)
	h.wait(time.Second)
	if len(h.opened) != 0 || h.at0 != 0 {
		t.Fatal("singleton answered beside a visible device quorum peer")
	}
	if reason := h.n.notAnsweringReason(); reason != "other devices visible but not in group" {
		t.Fatalf("reason %q", reason)
	}
}
