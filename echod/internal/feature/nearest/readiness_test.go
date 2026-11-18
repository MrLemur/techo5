package nearest

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
)

func TestPostReadinessAcceptsOnlySignedReadiness(t *testing.T) {
	newHouse(t)
	for _, proven := range []bool{true, false} {
		t.Run(map[bool]string{true: "signed", false: "tampered"}[proven], func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read request body: %v", err)
				}
				if err := verify(word, "Kitchen", r.Header.Get(authHeader), body, time.Now()); err != nil {
					t.Errorf("verify versioned claim: %v", err)
					w.WriteHeader(http.StatusForbidden)
					return
				}
				var sent claim
				if err := json.Unmarshal(body, &sent); err != nil || sent.Protocol != coordinationProtocolVersion {
					t.Errorf("request claim: %+v, err %v", sent, err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				now := time.Now()
				report := readiness{
					Version: coordinationProtocolVersion, From: "Kitchen", Members: sent.Members,
					Candidates: []claim{sent}, Ready: true,
					ReceivedAt: now.Add(5 * time.Millisecond).UnixNano(),
					SentAt:     now.Add(15 * time.Millisecond).UnixNano(),
				}
				response, _ := json.Marshal(report)
				proof := readinessMAC(word, nonceOf(r.Header.Get(authHeader)), response)
				if !proven {
					proof = "tampered"
				}
				w.Header().Set(readinessHeader, proof)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(response)
			}))
			defer srv.Close()
			addr := srv.Listener.Addr().(*net.TCPAddr)
			got, ok := postReadiness(announce.Peer{Name: "Kitchen", Address: addr.IP.String(), Port: addr.Port}, claim{
				From: "Office", Turn: "turn", Word: "okay_nabu", Score: 20,
				Protocol: coordinationProtocolVersion, HeardAt: time.Now().UnixNano(), Members: []string{"kitchen", "office"},
			})
			if ok != proven {
				t.Fatalf("readiness proof valid %v: accepted %v", proven, ok)
			}
			if proven && (!got.Ready || got.From != "Kitchen") {
				t.Fatalf("readiness report %+v", got)
			}
		})
	}
}

func TestHealthyReadinessExchangeIncludesCollectionButMeetsClockBound(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen"})
	claimAt := time.Now()
	mine := h.stamp(claim{From: "Kitchen", Turn: "turn", Word: "okay_nabu", Score: 20,
		Protocol: coordinationProtocolVersion, HeardAt: claimAt.UnixNano(), Members: []string{"kitchen", "office"}})
	srv := httptest.NewServer(http.HandlerFunc(h.n.receiveReadiness))
	defer srv.Close()
	addr := srv.Listener.Addr().(*net.TCPAddr)
	start := time.Now()
	report, ok := postReadiness(announce.Peer{Name: "Office", Address: addr.IP.String(), Port: addr.Port}, mine)
	elapsed := time.Since(start)
	if !ok || !report.Ready {
		t.Fatalf("healthy exchange report %+v, accepted %v", report, ok)
	}
	if elapsed < eventCollectionWindow || elapsed > readinessDeadline {
		t.Fatalf("healthy exchange took %v; collection is %v and fast deadline is %v", elapsed, eventCollectionWindow, readinessDeadline)
	}
	if !clockInterval(report.localSent, report.localReceived, report) {
		t.Fatalf("collection delay was misclassified as clock uncertainty: %+v", report)
	}
	winner, ready := earlyWinner(mine, mine.Members, []readiness{report})
	if !ready || winner.From != mine.From {
		t.Fatalf("healthy exchange did not authorize early release: winner %+v ready %v", winner, ready)
	}
}

func TestReceiveReadinessRejectsSameSenderTurnConflictsBeforeCompaction(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen", "Den"})
	h.n.wait = func(time.Duration) {}
	at := time.Now()
	members := []string{"den", "kitchen", "office"}
	first := claim{From: "Den", Turn: "first", Word: "okay_nabu", Score: 10,
		Protocol: coordinationProtocolVersion, HeardAt: at.UnixNano(), Members: members}
	second := first
	second.Turn, second.Score = "second", 99
	unrelated := second
	unrelated.From, unrelated.Turn = "Kitchen", "separate-event"
	unrelated.HeardAt = at.Add(time.Second).UnixNano()
	h.n.took(first)
	h.n.took(second)
	h.n.took(unrelated)
	incoming := claim{From: "Kitchen", Turn: "incoming", Word: first.Word, Score: 20,
		Protocol: coordinationProtocolVersion, HeardAt: at.UnixNano(), Members: members}
	body, err := json.Marshal(incoming)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, v3Path, bytes.NewReader(body))
	req.RemoteAddr = "192.168.1.5:1234"
	req.Header.Set(authHeader, sign(word, "Office", body, time.Now()))
	response := httptest.NewRecorder()
	h.n.receiveReadiness(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("response %d: %s", response.Code, response.Body.String())
	}
	var report readiness
	if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Ready {
		t.Fatal("conflicting same-sender turns were accepted after candidate compaction")
	}
	if len(report.Candidates) != 2 || !containsClaim(report.Candidates, incoming) || !containsClaim(report.Candidates, second) {
		t.Fatalf("compacted current-event candidates %+v; unrelated separate event affected the group", report.Candidates)
	}
}

func TestReadinessUsesElectionClockForBothServerTimestamps(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen"})
	clock := func(at time.Time) (time.Time, bool) { return at.Add(2 * time.Second), true }
	h.n.clockNow = clock
	now, _ := clock(time.Now())
	mine := claim{From: "Kitchen", Turn: "shifted", Word: "okay_nabu", Score: 20,
		Protocol: coordinationProtocolVersion, HeardAt: now.UnixNano(), Members: []string{"kitchen", "office"}}
	server := httptest.NewServer(http.HandlerFunc(h.n.receiveReadiness))
	defer server.Close()
	addr := server.Listener.Addr().(*net.TCPAddr)
	report, ok := postReadinessWithClock(announce.Peer{Name: "Office", Address: addr.IP.String(), Port: addr.Port}, mine, clock)
	if !ok || !clockInterval(report.localSent, report.localReceived, report) {
		t.Fatalf("shifted election clock produced invalid readiness: %+v, accepted %v", report, ok)
	}
}

func TestReadinessExchangeRejectsClockSkewOutsideBound(t *testing.T) {
	newHouse(t)
	mine := claim{From: "Kitchen", Turn: "turn", Word: "okay_nabu", Score: 20,
		Protocol: coordinationProtocolVersion, HeardAt: time.Now().UnixNano(), Members: []string{"kitchen", "office"}}
	const skew = 100 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read claim: %v", err)
			return
		}
		var incoming claim
		if err := json.Unmarshal(body, &incoming); err != nil {
			t.Errorf("decode claim: %v", err)
			return
		}
		received := time.Now().Add(skew)
		time.Sleep(eventCollectionWindow)
		report := readiness{Version: coordinationProtocolVersion, From: "Office", Members: incoming.Members,
			Candidates: []claim{incoming}, Ready: true, ReceivedAt: received.UnixNano(),
			SentAt: received.Add(eventCollectionWindow).UnixNano()}
		response, _ := json.Marshal(report)
		w.Header().Set(readinessHeader, readinessMAC(word, nonceOf(r.Header.Get(authHeader)), response))
		_, _ = w.Write(response)
	}))
	defer srv.Close()
	addr := srv.Listener.Addr().(*net.TCPAddr)
	report, ok := postReadiness(announce.Peer{Name: "Office", Address: addr.IP.String(), Port: addr.Port}, mine)
	if !ok || !report.Ready {
		t.Fatalf("signed skew test response was not parsed: %+v, accepted %v", report, ok)
	}
	if clockInterval(report.localSent, report.localReceived, report) {
		t.Fatal("clock offset outside the 50 ms bound allowed early release")
	}
	if _, ready := earlyWinner(mine, mine.Members, []readiness{report}); ready {
		t.Fatal("readiness gate accepted the out-of-bound clock interval")
	}
}

func TestReceiveReadinessIncludesClaimsArrivingDuringCollection(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen", "Den"})
	waiting := make(chan time.Duration, 1)
	release := make(chan struct{})
	h.n.wait = func(d time.Duration) {
		waiting <- d
		<-release
	}
	me := config.Get().Device.Name
	at := time.Now()
	incoming := claim{From: "Kitchen", Turn: "k", Word: "okay_nabu", Score: 21,
		Protocol: coordinationProtocolVersion, HeardAt: at.UnixNano(), Members: members(me, h.n.peers())}
	body, err := json.Marshal(incoming)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, v3Path, bytes.NewReader(body))
	req.RemoteAddr = "192.168.1.5:1234"
	req.Header.Set(authHeader, sign(word, me, body, time.Now()))
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.n.receiveReadiness(response, req)
		close(done)
	}()
	select {
	case got := <-waiting:
		if got != eventCollectionWindow {
			close(release)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
			}
			t.Fatalf("collection wait %v; want %v", got, eventCollectionWindow)
		}
	case <-done:
		t.Fatalf("handler returned before collection: status %d body %s", response.Code, response.Body.String())
	case <-time.After(2 * time.Second):
		close(release)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
		t.Fatal("handler did not enter fixed-window collection")
	}
	later := claim{From: "Den", Turn: "d", Word: incoming.Word, Score: 25,
		Protocol: coordinationProtocolVersion, HeardAt: at.Add(40 * time.Millisecond).UnixNano(), Members: incoming.Members}
	h.n.took(later)
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not finish after collection released")
	}
	if response.Code != http.StatusOK {
		t.Fatalf("response %d: %s", response.Code, response.Body.String())
	}
	var report readiness
	if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Candidates) != 2 || !containsClaim(report.Candidates, later) {
		t.Errorf("readiness candidates %+v; want both claimants collected during the fixed interval", report.Candidates)
	}
}

// postReadiness is postReadinessWithClock with a clock that is always ready.
func postReadiness(p announce.Peer, c claim) (readiness, bool) {
	return postReadinessWithClock(p, c, func(t time.Time) (time.Time, bool) { return t, true })
}
