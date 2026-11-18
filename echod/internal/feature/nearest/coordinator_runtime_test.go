package nearest

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
)

// A quorum must still fail closed when the aggregate median's uncertainty
// exceeds the bound, even if every member is reachable.
func TestSimulationUncertainMedianRejectsWakeDespiteQuorum(t *testing.T) {
	h := newHouse(t)
	names := []string{"a", "b", "c", "d", "e", "f"}
	enableCoordinatorRoster(t, h, "a", names)
	now := time.Now()
	for _, peer := range names[1:] {
		h.n.clock.offer(peer, 0, clockMaxUncertainty, now.Add(-time.Second))
		h.n.clock.contact(peer, now, "")
	}
	if !coordinatorQuorum(names, []string{"a", "b", "c", "d"}) {
		t.Fatal("fixture lacks a majority")
	}
	h.n.clockNow = func(at time.Time) (time.Time, bool) {
		return h.n.clock.corrected(at, names, "a")
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, coordinatorWakePath, nil)
	h.n.coordinatorReceive(recorder, request, true)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("wake status = %d (%s), want 503", recorder.Code, fmt.Sprint(recorder.Body))
	}
}

func TestCoordinatorRuntimeVoteAndRestartQuarantine(t *testing.T) {
	now := time.Now()
	var c coordinatorRuntime
	if _, yes := c.vote("a", 1, now); yes {
		t.Fatal("restarted voter granted a vote during its quarantine")
	}
	if _, yes := c.vote("a", 1, now.Add(coordinatorGrant)); !yes {
		t.Fatal("voter did not grant after its quarantine")
	}
	if _, yes := c.vote("b", 2, now.Add(coordinatorGrant+time.Second)); yes {
		t.Fatal("voter granted a competing coordinator during an active grant")
	}
	if _, yes := c.vote("b", 2, now.Add(2*coordinatorGrant)); !yes {
		t.Fatal("voter did not allow a new coordinator after its grant expired")
	}
}

func TestSimulationCoordinatorFailoverHasNoOverlappingLeases(t *testing.T) {
	start := time.Now()
	names := []string{"a", "b", "c", "d", "e", "f"}
	nodes := make([]*coordinatorRuntime, len(names))
	for i := range nodes {
		nodes[i] = &coordinatorRuntime{boot: start.Add(-coordinatorGrant)}
	}
	oldVotes := []string{}
	for i := 0; i < 4; i++ {
		if _, yes := nodes[i].vote("a", 1, start); yes {
			oldVotes = append(oldVotes, names[i])
		}
	}
	if !coordinatorQuorum(names, oldVotes) {
		t.Fatal("old coordinator did not acquire a majority")
	}
	nextVotes := []string{}
	for i := 2; i < 6; i++ {
		if _, yes := nodes[i].vote("c", 2, start.Add(coordinatorLease)); yes {
			nextVotes = append(nextVotes, names[i])
		}
	}
	if coordinatorQuorum(names, nextVotes) {
		t.Fatal("competing coordinator acquired a majority during the old grant")
	}
	nextVotes = nil
	for i := 2; i < 6; i++ {
		if _, yes := nodes[i].vote("c", 2, start.Add(coordinatorGrant)); yes {
			nextVotes = append(nextVotes, names[i])
		}
	}
	if !coordinatorQuorum(names, nextVotes) {
		t.Fatal("new coordinator did not acquire a majority after grants expired")
	}
}

func TestSimulationEqualPartitionCannotElectCoordinator(t *testing.T) {
	roster := []string{"a", "b", "c", "d", "e", "f"}
	if coordinatorQuorum(roster, []string{"a", "b", "c"}) ||
		coordinatorQuorum(roster, []string{"d", "e", "f"}) {
		t.Fatal("a three-device partition elected a coordinator")
	}
}

func TestCoordinatorQuorumRejectsDuplicateAndUnknownVoters(t *testing.T) {
	roster := []string{"a", "b", "c", "d", "e", "f"}
	for _, votes := range [][]string{{"a", "a", "b", "c"}, {"a", "b", "c", "unknown"}} {
		if coordinatorQuorum(roster, votes) {
			t.Fatalf("accepted invalid voters: %v", votes)
		}
	}
}

func TestSimulationRestartedVoterWaitsOutOldGrant(t *testing.T) {
	start := time.Now()
	old := &coordinatorRuntime{boot: start.Add(-coordinatorGrant)}
	if _, ok := old.vote("a", 10, start); !ok {
		t.Fatal("initial vote failed")
	}
	restarted := &coordinatorRuntime{}
	if _, ok := restarted.vote("b", 1, start.Add(time.Second)); ok {
		t.Fatal("restarted voter voted while an old grant could still be live")
	}
	if _, ok := restarted.vote("b", 1, start.Add(coordinatorGrant+time.Second)); !ok {
		t.Fatal("restarted voter failed to resume after quarantine")
	}
}

// Claims from different devices are the required nearest-device election case. With only
// word/timestamp/turn metadata, this same shape is also compatible with two close, separate
// same-word utterances; the protocol cannot identify that ambiguity without shared event data.
func TestCoordinatorCollectsTwoConcurrentWakeClaims(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Kitchen", []string{"Kitchen", "Office", "Den"})
	h.n.coordinator.mu.Lock()
	h.n.coordinator.leader = "kitchen"
	h.n.coordinator.leaderTerm = 7
	h.n.coordinator.leaderUntil = time.Now().Add(2 * time.Second)
	h.n.coordinator.mu.Unlock()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.n.coordinatorReceive(w, r, true)
	}))
	defer server.Close()
	addr := server.Listener.Addr().(*net.TCPAddr)
	peer := announce.Peer{Name: "Kitchen", Address: addr.IP.String(), Port: addr.Port}
	members := []string{"den", "kitchen", "office"}
	at := time.Now().UnixNano()
	claims := []claim{
		{From: "Office", Turn: "one", Word: "okay_nabu", Score: 10,
			Protocol: coordinationProtocolVersion, HeardAt: at, Members: members},
		{From: "Den", Turn: "two", Word: "okay_nabu", Score: 20,
			Protocol: coordinationProtocolVersion, HeardAt: at, Members: members},
	}
	results := make([]coordinatorResponse, len(claims))
	ok := make([]bool, len(claims))
	var wg sync.WaitGroup
	for i, candidate := range claims {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], ok[i] = h.n.coordinatorPost(peer, coordinatorWakePath, coordinatorRequest{
				From: candidate.From, Term: 7, Members: members, Candidate: h.stamp(candidate),
				Cluster: h.token.cluster, Epoch: h.token.epoch, Fingerprint: h.token.fingerprint,
			})
		}()
	}
	wg.Wait()
	for i := range claims {
		if !ok[i] || !results[i].Granted || results[i].Winner.From != "Den" {
			t.Fatalf("claim %d got %+v, accepted %v", i, results[i], ok[i])
		}
	}
}

func TestCoordinatorFailsClosedWhenOneDeviceReportsTwoTurnsInOneEvent(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Kitchen", []string{"Kitchen", "Office", "Den"})
	h.n.coordinator.mu.Lock()
	h.n.coordinator.leader = "kitchen"
	h.n.coordinator.leaderTerm = 7
	h.n.coordinator.leaderUntil = time.Now().Add(2 * time.Second)
	h.n.coordinator.mu.Unlock()
	at := time.Now().UnixNano()
	members := []string{"den", "kitchen", "office"}
	first := claim{From: "Office", Turn: "first", Word: "okay_nabu", Score: 10,
		Protocol: coordinationProtocolVersion, HeardAt: at, Members: members}
	second := first
	second.Turn, second.Score = "second", 99
	result := make(chan bool, 2)
	go func() { _, ok := h.n.coordinatorCollect(h.stamp(first), 7, h.token); result <- ok }()
	time.Sleep(20 * time.Millisecond)
	go func() { _, ok := h.n.coordinatorCollect(h.stamp(second), 7, h.token); result <- ok }()
	firstResult, secondResult := <-result, <-result
	if firstResult || secondResult {
		t.Fatal("coordinator chose a turn despite same-device event ambiguity")
	}
}

func TestCoordinatorRejectsClaimAfterEventDeadline(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Kitchen", []string{"Kitchen", "Office", "Den"})
	h.n.coordinator.mu.Lock()
	h.n.coordinator.leader = "kitchen"
	h.n.coordinator.leaderTerm = 7
	h.n.coordinator.leaderUntil = time.Now().Add(2 * time.Second)
	h.n.coordinator.mu.Unlock()
	at := time.Now().UnixNano()
	members := []string{"den", "kitchen", "office"}
	first := claim{From: "Office", Turn: "first", Word: "okay_nabu", Score: 10,
		Protocol: coordinationProtocolVersion, HeardAt: at, Members: members}
	result := make(chan bool, 1)
	go func() { _, ok := h.n.coordinatorCollect(h.stamp(first), 7, h.token); result <- ok }()
	var event *coordinatorEvent
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		h.n.coordinator.mu.Lock()
		if len(h.n.coordinator.events) > 0 {
			event = h.n.coordinator.events[0]
			event.deadline = time.Now().Add(-time.Millisecond)
		}
		h.n.coordinator.mu.Unlock()
		if event != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if event == nil {
		t.Fatal("coordinator did not start an event")
	}
	late := first
	late.From, late.Turn, late.Score = "Den", "late", 99
	if _, ok := h.n.coordinatorCollect(h.stamp(late), 7, h.token); ok {
		t.Fatal("coordinator admitted a claim after the event deadline")
	}
	select {
	case ok := <-result:
		if !ok {
			t.Fatal("timely first claim did not receive its decision")
		}
	case <-time.After(time.Second):
		t.Fatal("event was not finalized")
	}
}

func TestCoordinatorDoesNotDecideAfterLosingLease(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Kitchen", []string{"Kitchen", "Office", "Den"})
	h.n.coordinator.mu.Lock()
	h.n.coordinator.leader = "kitchen"
	h.n.coordinator.leaderTerm = 3
	h.n.coordinator.leaderUntil = time.Now().Add(eventCollectionWindow / 3)
	h.n.coordinator.mu.Unlock()
	c := claim{From: "Office", Turn: "wake", Word: "okay_nabu", Score: 10,
		Protocol: coordinationProtocolVersion, HeardAt: time.Now().UnixNano(), Members: []string{"den", "kitchen", "office"}}
	if _, ok := h.n.coordinatorCollect(h.stamp(c), 3, h.token); ok {
		t.Fatal("coordinator made a decision after its lease expired")
	}
}

func TestCoordinatorRejectsLateClaimForCompletedEvent(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Kitchen", []string{"Kitchen", "Office", "Den"})
	h.n.coordinator.mu.Lock()
	h.n.coordinator.leader = "kitchen"
	h.n.coordinator.leaderTerm = 3
	h.n.coordinator.leaderUntil = time.Now().Add(2 * time.Second)
	h.n.coordinator.mu.Unlock()
	c := claim{From: "Office", Turn: "first", Word: "okay_nabu", Score: 10,
		Protocol: coordinationProtocolVersion, HeardAt: time.Now().UnixNano(), Members: []string{"den", "kitchen", "office"}}
	if _, ok := h.n.coordinatorCollect(h.stamp(c), 3, h.token); !ok {
		t.Fatal("coordinator did not decide the initial claim")
	}
	c.From, c.Turn = "Den", "late"
	if _, ok := h.n.coordinatorCollect(h.stamp(c), 3, h.token); ok {
		t.Fatal("late claim reopened an already decided event")
	}
}

func TestCoordinatorDoesNotAnswerWhenCoordinatorUnavailable(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen"})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	addr := server.Listener.Addr().(*net.TCPAddr)
	peer := announce.Peer{Name: "Kitchen", Address: addr.IP.String(), Port: addr.Port}
	h.n.coordinator.mu.Lock()
	h.n.coordinator.leader = "kitchen"
	h.n.coordinator.leaderTerm = 3
	h.n.coordinator.grantUntil = time.Now().Add(time.Second)
	h.n.coordinator.mu.Unlock()
	mine := &pending{claim: claim{From: "Office", Turn: "wake", Word: "okay_nabu", Score: 10,
		Protocol: coordinationProtocolVersion, HeardAt: time.Now().UnixNano(), Members: []string{"kitchen", "office"}}}
	opened := false
	h.n.coordinatorWake([]announce.Peer{peer}, mine, func() bool { return true }, func(time.Time, float64) {
		opened = true
	})
	if opened || mine.readinessComplete {
		t.Fatal("unavailable coordinator caused a response")
	}
}

// In Device quorum, claims for one word inside the event tolerance are one event with one winner;
// beyond it they are separate events; and one device reporting two turns is dropped, never guessed.
func TestElectedEventGroupingBoundaries(t *testing.T) {
	setup := func() *house {
		h := newHouse(t)
		enableCoordinatorRoster(t, h, "Kitchen", []string{"Kitchen", "Office", "Den"})
		h.n.coordinator.mu.Lock()
		h.n.coordinator.leader, h.n.coordinator.leaderTerm = "kitchen", 7
		h.n.coordinator.leaderUntil = time.Now().Add(10 * time.Second)
		h.n.coordinator.mu.Unlock()
		return h
	}
	members := []string{"den", "kitchen", "office"}
	mk := func(from, turn string, at time.Time, score float64) claim {
		return claim{From: from, Turn: turn, Word: "okay_nabu", Score: score, Protocol: coordinationProtocolVersion, HeardAt: at.UnixNano(), Members: members}
	}
	t.Run("inside the tolerance is one event", func(t *testing.T) {
		h := setup()
		at := time.Now()
		type res struct {
			w  claim
			ok bool
		}
		out := make(chan res, 2)
		for _, c := range []claim{mk("Office", "a", at, 5), mk("Den", "b", at.Add(120*time.Millisecond), 9)} {
			go func() { w, ok := h.n.coordinatorCollect(h.stamp(c), 7, h.token); out <- res{w, ok} }()
		}
		for range 2 {
			if r := <-out; !r.ok || r.w.From != "Den" {
				t.Fatalf("grouped result = %+v", r)
			}
		}
	})
	t.Run("beyond the tolerance is two events", func(t *testing.T) {
		h := setup()
		at := time.Now()
		first, ok1 := h.n.coordinatorCollect(h.stamp(mk("Office", "a", at, 5)), 7, h.token)
		second, ok2 := h.n.coordinatorCollect(h.stamp(mk("Den", "b", at.Add(300*time.Millisecond), 9)), 7, h.token)
		if !ok1 || !ok2 || first.From != "Office" || second.From != "Den" {
			t.Fatalf("separate events merged: %+v %v / %+v %v", first, ok1, second, ok2)
		}
	})
	t.Run("a skewed clock still joins the open event", func(t *testing.T) {
		h := setup()
		at := time.Now()
		out := make(chan claim, 2)
		for _, c := range []claim{mk("Office", "a", at, 5), mk("Den", "b", at.Add(600*time.Millisecond), 0)} {
			go func() { w, _ := h.n.coordinatorCollect(h.stamp(c), 7, h.token); out <- w }()
		}
		for range 2 {
			if w := <-out; w.From != "Office" {
				t.Fatalf("winner = %q, want the best claim for both devices", w.From)
			}
		}
	})
	t.Run("claims further apart than the wider tolerance are two events", func(t *testing.T) {
		h := setup()
		at := time.Now()
		out := make(chan claim, 2)
		for _, c := range []claim{mk("Office", "a", at.Add(-700*time.Millisecond), 5), mk("Den", "b", at.Add(700*time.Millisecond), 9)} {
			go func() { w, _ := h.n.coordinatorCollect(h.stamp(c), 7, h.token); out <- w }()
		}
		got := map[string]bool{(<-out).From: true, (<-out).From: true}
		if !got["Office"] || !got["Den"] {
			t.Fatalf("separate events merged: %v", got)
		}
	})
	t.Run("a different word is a different event", func(t *testing.T) {
		h := setup()
		at := time.Now()
		other := mk("Den", "b", at, 99)
		other.Word = "hey_jarvis"
		out := make(chan claim, 2)
		go func() { w, _ := h.n.coordinatorCollect(h.stamp(mk("Office", "a", at, 5)), 7, h.token); out <- w }()
		go func() { w, _ := h.n.coordinatorCollect(h.stamp(other), 7, h.token); out <- w }()
		got := map[string]bool{(<-out).From: true, (<-out).From: true}
		if !got["Office"] || !got["Den"] {
			t.Fatalf("words grouped together: %v", got)
		}
	})
}
