package nearest

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
)

// In classic mode a busy device sends nothing, as before.
func TestBusyDeviceInOtherModesSendsNoBusyClaim(t *testing.T) {
	h := newHouse(t)
	h.n.busy = func() bool { return true }
	config.Started(config.Device{Name: "Office"})
	var sent atomic.Int64
	h.n.peers = func() []announce.Peer { return nil }
	h.n.woke = func(int, time.Time) { sent.Add(1) }
	h.n.Woke(0)
	time.Sleep(100 * time.Millisecond)
	if sent.Load() != 0 {
		t.Fatalf("a busy device in classic mode opened a turn %d times", sent.Load())
	}
}

// In Device quorum a busy device that is itself the elected leader adds its own claim to the event it
// collects, so that when it heard the word best the others are told it won and stay quiet.
func TestElectedBusyLeaderCollectsItsOwnClaim(t *testing.T) {
	h := newHouse(t)
	h.now = time.Now()
	h.n.now = func() time.Time { return h.now }
	enableCoordinatorRoster(t, h, "Kitchen", []string{"Kitchen", "Office", "Den"})
	leadAs(h, "kitchen", 7)
	h.n.busy = func() bool { return true }
	h.n.score = func() float64 { return 20 }
	var opened atomic.Int64
	h.n.woke = func(int, time.Time) { opened.Add(1) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.n.coordinatorReceive(w, r, true)
	}))
	defer server.Close()
	addr := server.Listener.Addr().(*net.TCPAddr)
	peer := announce.Peer{Name: "Kitchen", Address: addr.IP.String(), Port: addr.Port}
	members := []string{"den", "kitchen", "office"}
	office := claim{From: "Office", Turn: "one", Word: "okay_nabu", Score: 10,
		Protocol: coordinationProtocolVersion, HeardAt: h.now.UnixNano(), Members: members}
	var response coordinatorResponse
	var ok bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		response, ok = h.n.coordinatorPost(peer, coordinatorWakePath, coordinatorRequest{
			From: "Office", Term: 7, Members: members, Candidate: h.stamp(office),
			Cluster: h.token.cluster, Epoch: h.token.epoch, Fingerprint: h.token.fingerprint,
		})
	}()
	h.n.Woke(0)
	<-done
	if !ok || !response.Granted || response.Winner.From != "Kitchen" || !response.Winner.Busy {
		t.Fatalf("response = %+v, accepted %v; want the busy leader to win", response, ok)
	}
	if opened.Load() != 0 {
		t.Fatalf("a busy device opened a turn %d times", opened.Load())
	}
}

// A busy device that is not the leader sends its claim to the leader, bound to the committed membership.
func TestElectedBusyDeviceSendsItsClaimToTheLeader(t *testing.T) {
	h := newHouse(t)
	h.now = time.Now()
	h.n.now = func() time.Time { return h.now }
	enableCoordinatorRoster(t, h, "Office", []string{"Kitchen", "Office", "Den"})
	got := make(chan coordinatorRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var request coordinatorRequest
		if json.Unmarshal(body, &request) == nil {
			got <- request
		}
		http.Error(w, "stub", http.StatusConflict)
	}))
	defer server.Close()
	addr := server.Listener.Addr().(*net.TCPAddr)
	h.n.peers = func() []announce.Peer {
		return []announce.Peer{
			{Name: "Kitchen", Address: addr.IP.String(), Port: addr.Port},
			{Name: "Den", Address: "192.0.2.1", Port: 8181},
		}
	}
	h.peersReport(modeElected)
	leadAs(h, "kitchen", 7)
	h.n.busy = func() bool { return true }
	var opened atomic.Int64
	h.n.woke = func(int, time.Time) { opened.Add(1) }
	h.n.Woke(0)
	select {
	case request := <-got:
		c := request.Candidate
		if !c.Busy || c.From != "Office" || request.Term != 7 || request.Cluster != h.token.cluster ||
			request.Epoch != h.token.epoch || request.Fingerprint != h.token.fingerprint || c.Cluster != h.token.cluster {
			t.Fatalf("request = %+v, want a busy claim bound to the committed membership", request)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a busy device sent no claim to the elected leader")
	}
	if opened.Load() != 0 {
		t.Fatalf("a busy device opened a turn %d times", opened.Load())
	}
}

// leadAs makes name the elected leader for the next couple of seconds.
func leadAs(h *house, name string, term uint64) {
	h.n.coordinator.mu.Lock()
	defer h.n.coordinator.mu.Unlock()
	h.n.coordinator.leader = name
	h.n.coordinator.leaderTerm = term
	h.n.coordinator.grantUntil = time.Now().Add(2 * time.Second)
	h.n.coordinator.leaderUntil = time.Now().Add(2 * time.Second)
}

// A busy device whose claim another device beat ends its own turn, since the person said the wake word
// again nearer that one; winning, a played claim, no winner or no turn open leaves things as they are.
func TestBusyLostEndsTheTurnOnlyWhenAnotherWon(t *testing.T) {
	mine := claim{From: "Office", Word: word, Busy: true}
	played := mine
	played.Played = true
	for _, tc := range []struct {
		name   string
		mine   claim
		winner claim
		busy   bool
		ends   bool
	}{
		{"another won", mine, claim{From: "Kitchen"}, true, true},
		{"this device won", mine, claim{From: "office"}, true, false},
		{"no winner", mine, claim{}, true, false},
		{"played claim", played, claim{From: "Kitchen"}, true, false},
		{"turn already over", mine, claim{From: "Kitchen"}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHouse(t)
			ended := false
			h.n.busy = func() bool { return tc.busy }
			h.n.endTurn = func() { ended = true }
			h.n.busyLost(tc.mine, tc.winner)
			if ended != tc.ends {
				t.Fatalf("turn ended = %v, want %v", ended, tc.ends)
			}
		})
	}
}
