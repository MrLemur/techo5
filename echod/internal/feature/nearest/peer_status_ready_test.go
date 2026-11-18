package nearest

import (
	"testing"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
)

// A member that last reported the house's mode and then went quiet does not stop the others answering;
// one never heard from, one in another mode, and one at a new address still do.
func TestPeerModesQuietMemberDoesNotBlock(t *testing.T) {
	const mode = modeElected
	now := time.Now()
	peer := announce.Peer{Name: "Dot 1", Address: "10.0.0.1", Port: 6053}
	var c peerStatusCache

	if c.ready(word, mode, []announce.Peer{peer}, now) {
		t.Fatal("a peer that never reported should block")
	}
	c.observe(word, peer, peerStatus{Name: "Dot 1", Mode: mode, Enabled: true}, now.Add(-time.Hour))
	if !c.ready(word, mode, []announce.Peer{peer}, now) {
		t.Fatal("a member gone quiet after reporting the mode should not block")
	}
	moved := peer
	moved.Address = "10.0.0.2"
	if c.ready(word, mode, []announce.Peer{moved}, now) {
		t.Fatal("a peer at an address it never reported from should block")
	}
	c.observe(word, peer, peerStatus{Name: "Dot 1", Mode: modeExisting, Enabled: true}, now.Add(-time.Hour))
	if c.ready(word, mode, []announce.Peer{peer}, now) {
		t.Fatal("a peer whose last report was another mode should block")
	}
}
