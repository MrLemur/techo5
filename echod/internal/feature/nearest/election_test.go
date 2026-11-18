package nearest

import (
	"testing"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
)

func TestThreeDeviceReadinessGate(t *testing.T) {
	at := time.Date(2026, 11, 13, 8, 0, 0, 0, time.UTC)
	claims := []claim{
		{From: "A", Turn: "a", Word: "okay_nabu", Score: 20, Protocol: coordinationProtocolVersion, HeardAt: at.UnixNano()},
		{From: "B", Turn: "b", Word: "okay_nabu", Score: 27, Protocol: coordinationProtocolVersion, HeardAt: at.Add(30 * time.Millisecond).UnixNano()},
		{From: "C", Turn: "c", Word: "okay_nabu", Score: 22, Protocol: coordinationProtocolVersion, HeardAt: at.Add(-20 * time.Millisecond).UnixNano()},
	}
	members := []string{"a", "b", "c"}
	reports := []readiness{
		readyReport("A", members, claims, at),
		readyReport("B", members, claims, at),
		readyReport("C", members, claims, at),
	}
	for _, local := range claims {
		peers := make([]readiness, 0, 2)
		for _, r := range reports {
			if r.From != local.From {
				peers = append(peers, r)
			}
		}
		winner, ok := earlyWinner(local, members, peers)
		if !ok || winner.From != "B" {
			t.Errorf("device %s: early result %+v, ready %v; want B", local.From, winner, ok)
		}
	}
}

func TestMemberWithoutReadinessDoesNotReleaseEarly(t *testing.T) {
	at := time.Date(2026, 11, 13, 8, 0, 0, 0, time.UTC)
	mine := claim{From: "Echo1", Turn: "a", Word: "okay_nabu", Score: 20, Protocol: coordinationProtocolVersion, HeardAt: at.UnixNano()}
	allMembers := []string{"echo1", "echo2", "echo3", "echo4", "echo5", "mi clock"}
	var reports []readiness
	for _, name := range allMembers[1:5] {
		reports = append(reports, readyReport(name, allMembers, []claim{mine}, at))
	}
	if _, ready := earlyWinner(mine, allMembers, reports); ready {
		t.Fatal("idle legacy member was silently excluded from authenticated membership")
	}
}

func TestReadinessFallbacks(t *testing.T) {
	at := time.Date(2026, 11, 13, 8, 0, 0, 0, time.UTC)
	mine := claim{From: "A", Turn: "a", Word: "okay_nabu", Score: 20, Protocol: coordinationProtocolVersion, HeardAt: at.UnixNano()}
	members := []string{"a", "b", "c"}
	claims := []claim{mine, {From: "B", Turn: "b", Word: mine.Word, Score: 21, Protocol: coordinationProtocolVersion, HeardAt: mine.HeardAt}}
	good := []readiness{readyReport("B", members, claims, at), readyReport("C", members, claims, at)}

	if _, ok := earlyWinner(mine, members, good[:1]); ok {
		t.Error("missing member acknowledgement released early")
	}
	diverged := append([]readiness(nil), good...)
	diverged[1].Candidates = []claim{mine}
	if _, ok := earlyWinner(mine, members, diverged); ok {
		t.Error("divergent candidate views released early")
	}
	changedMembers := append([]readiness(nil), good...)
	changedMembers[1].Members = []string{"a", "b"}
	if _, ok := earlyWinner(mine, members, changedMembers); ok {
		t.Error("changed membership released early")
	}
	badClock := append([]readiness(nil), good...)
	badClock[0].ReceivedAt += int64(250 * time.Millisecond)
	badClock[0].SentAt += int64(250 * time.Millisecond)
	if _, ok := earlyWinner(mine, members, badClock); ok {
		t.Error("clock offset outside the configured bound released early")
	}
}

func TestMemberSnapshotIsSortedUniqueAndEligible(t *testing.T) {
	got := members("Office", []announce.Peer{
		{Name: "Kitchen"},
		{Name: "Office"},
		{Name: "Skylight", NoMicrophone: true},
		{Name: "Den"},
	})
	want := []string{"den", "kitchen", "office"}
	if !sameMembers(got, want) {
		t.Fatalf("members %v; want frozen eligible membership %v", got, want)
	}
}

func TestRosterExcludesIdleLegacyPeer(t *testing.T) {
	roster := []string{"echo dot 1", "echo dot 2", "echo dot 3", "echo dot 4", "echo spot 1"}
	peers := []announce.Peer{{Name: "Echo Dot 2"}, {Name: "Echo Dot 3"}, {Name: "Echo Dot 4"}, {Name: "Echo Spot 1"}, {Name: "Mi Clock 1"}}
	selected, complete := rosterPeers(peers, roster, "Echo Dot 1")
	if !complete || len(selected) != 4 {
		t.Fatalf("selected %d peers, complete=%v", len(selected), complete)
	}
	if _, complete = rosterPeers(peers[:3], roster, "Echo Dot 1"); complete {
		t.Fatal("missing roster member must prevent early release")
	}
}

func TestLocalCandidateMissingFromPeerReportsForcesFallback(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen", "Den"})
	h.n.readinessFanout = func([]announce.Peer, *pending, func() bool, func(time.Time, float64)) {}
	h.n.Woke(0)
	mine := h.mine.claim
	kitchen := claim{From: "Kitchen", Turn: "k", Word: mine.Word, Score: 23,
		Protocol: coordinationProtocolVersion, HeardAt: mine.HeardAt + int64(10*time.Millisecond)}
	den := claim{From: "Den", Turn: "d", Word: mine.Word, Score: 30,
		Protocol: coordinationProtocolVersion, HeardAt: mine.HeardAt + int64(20*time.Millisecond)}
	h.n.took(den)
	reports := []readiness{
		readyReport("Kitchen", mine.Members, []claim{mine, kitchen}, h.now),
		readyReport("Den", mine.Members, []claim{mine, kitchen}, h.now),
	}
	if _, ok := earlyWinner(mine, mine.Members, reports); !ok {
		t.Fatal("setup must have mutually agreeing peer views")
	}
	if h.n.acceptEarly(h.mine, kitchen, reports) {
		t.Fatal("early release ignored a local candidate absent from peer views")
	}
	if h.mine.readinessComplete {
		t.Fatal("inconsistent local view marked this claim ready")
	}
}

func TestUnacknowledgedClaimAfterAcceptanceCannotChangeEarlyWinner(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen"})
	h.n.Woke(0)
	mine := h.mine
	reports := []readiness{readyReport("Kitchen", mine.Members, []claim{mine.claim}, h.now)}
	winner, ready := earlyWinner(mine.claim, mine.Members, reports)
	if !ready || !h.n.acceptEarly(mine, winner, reports) {
		t.Fatal("setup did not accept the peer snapshot")
	}
	unacknowledged := claim{From: "Den", Turn: "late", Word: mine.Word, Score: 99,
		Protocol: coordinationProtocolVersion, HeardAt: mine.HeardAt + int64(10*time.Millisecond)}
	h.n.took(unacknowledged)
	h.n.decide(mine.Word, mine, func() bool { return true }, func(time.Time, float64) { h.n.woke(0, mine.at) })
	if len(h.opened) != 1 {
		t.Errorf("opened %d times; expected the acknowledged snapshot to decide once", len(h.opened))
	}
}

func TestLegacyCompetingClaimBlocksCoordinatedEarlyRelease(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen"})
	h.n.Woke(0)
	mine := h.mine.claim
	legacy := claim{From: "Mi Clock", Turn: "legacy-turn", Word: mine.Word, Score: mine.Score + 5}
	h.n.mu.Lock()
	h.n.others = append(h.n.others, arrived{claim: legacy, at: h.now.Add(10 * time.Millisecond)})
	h.n.mu.Unlock()
	reports := []readiness{readyReport("Kitchen", mine.Members, []claim{mine}, h.now)}
	if h.n.acceptEarly(h.mine, mine, reports) {
		t.Fatal("legacy participant's competing claim did not force fallback")
	}
	if h.mine.readinessComplete {
		t.Fatal("legacy competing claim marked the coordinated event ready")
	}
}

func TestBestEventHonorsLegacyCompetingClaim(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen"})
	h.n.Woke(0)
	mine := h.mine.claim
	legacy := claim{From: "Mi Clock", Turn: "legacy-turn", Word: mine.Word, Score: mine.Score + 5}
	h.n.mu.Lock()
	h.n.others = append(h.n.others, arrived{claim: legacy, at: h.now.Add(10 * time.Millisecond)})
	best, beaten := h.n.bestCoordinatedEvent(mine)
	h.n.mu.Unlock()
	if !beaten || best.From != legacy.From {
		t.Fatalf("best event claim %+v, beaten %v; want legacy competitor to suppress weaker coordinated candidate", best, beaten)
	}
}

func TestFixedGroupingWindowIsBoundedAndWordScoped(t *testing.T) {
	at := time.Now()
	a := claim{From: "A", Turn: "a", Word: "okay_nabu", Protocol: coordinationProtocolVersion, HeardAt: at.UnixNano()}
	close := claim{From: "B", Turn: "b", Word: a.Word, Protocol: coordinationProtocolVersion,
		HeardAt: at.Add(eventCollectionWindow + maximumClockSkew).UnixNano()}
	far := close
	far.HeardAt = at.Add(eventCollectionWindow + maximumClockSkew + time.Nanosecond).UnixNano()
	otherWord := close
	otherWord.Word = "hey_jarvis"
	if !withinEvent(a, close) {
		t.Error("candidate at the fixed grouping boundary did not group")
	}
	if withinEvent(a, far) || withinEvent(a, otherWord) {
		t.Error("candidate outside the fixed event group was included")
	}
}

func readyReport(from string, members []string, claims []claim, at time.Time) readiness {
	return readiness{
		Version: coordinationProtocolVersion, From: from, Members: members, Candidates: claims, Ready: true,
		ReceivedAt: at.Add(5 * time.Millisecond).UnixNano(), SentAt: at.Add(15 * time.Millisecond).UnixNano(),
		localSent: at, localReceived: at.Add(20 * time.Millisecond),
	}
}
