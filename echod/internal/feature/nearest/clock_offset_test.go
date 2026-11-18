package nearest

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
)

func TestClockSamplingRetriesAfterTransientSlowExchange(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Kitchen", "Office"})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || r.Header.Get(readinessHeader) != clockMAC(word, "request", body) {
			http.Error(w, "invalid clock request", http.StatusForbidden)
			return
		}
		var request clockMessage
		if json.Unmarshal(body, &request) != nil {
			http.Error(w, "invalid clock request", http.StatusBadRequest)
			return
		}
		if requests.Add(1) == 1 {
			time.Sleep(60 * time.Millisecond)
		}
		now := time.Now().UnixNano()
		response, _ := json.Marshal(clockMessage{Nonce: request.Nonce, Received: now, Sent: now})
		w.Header().Set(readinessHeader, clockMAC(word, "response", response))
		_, _ = w.Write(response)
	}))
	defer server.Close()
	addr := server.Listener.Addr().(*net.TCPAddr)
	h.n.sampleClock(announce.Peer{Name: "Kitchen", Address: addr.IP.String(), Port: addr.Port}, "kitchen")
	if requests.Load() != 2 {
		t.Fatalf("got %d clock requests, want one retry", requests.Load())
	}
	if _, ok := h.n.clock.correctedFrom(time.Now(), "Kitchen"); !ok {
		t.Fatal("a good retry failed to establish clock readiness")
	}
}

func TestMeasureClockOffset(t *testing.T) {
	start := time.Unix(1700000000, 0)
	end := start.Add(30 * time.Millisecond)
	// Peer is two seconds fast; outgoing and return delays are 5 and 15 ms.
	remoteIn := start.Add(2*time.Second + 5*time.Millisecond)
	remoteOut := remoteIn.Add(10 * time.Millisecond)
	got, ok := measureClockOffset(start, end, remoteIn.UnixNano(), remoteOut.UnixNano())
	if !ok || got.lower != 1985*time.Millisecond || got.upper != 2005*time.Millisecond {
		t.Fatalf("offset %+v, valid %v", got, ok)
	}
}

func TestMedianClockAcceptsWideNoncentralPeer(t *testing.T) {
	now := time.Now()
	var clock electionClock
	names := []string{"a", "b", "c", "d", "e", "f"}
	offsets := []time.Duration{0, time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second, 5 * time.Second}
	for i, peer := range names[1:] {
		uncertainty := time.Millisecond
		if peer == "f" {
			uncertainty = 100 * time.Millisecond
		}
		clock.offer(peer, offsets[i+1], uncertainty, now)
	}
	got, ok := clock.corrected(now, names, "a")
	if !ok || got.Sub(now) != 2500*time.Millisecond {
		t.Fatalf("wide noncentral sample: offset %v, ready %v", got.Sub(now), ok)
	}
}

func TestMedianClockRejectsWideCentralPeer(t *testing.T) {
	now := time.Now()
	var clock electionClock
	names := []string{"a", "b", "c", "d", "e", "f"}
	offsets := []time.Duration{-time.Second, -900 * time.Millisecond, -800 * time.Millisecond, 800 * time.Millisecond, 900 * time.Millisecond, time.Second}
	clock.source = func(at time.Time) time.Time { return at.Add(offsets[0]) }
	for i, peer := range names[1:] {
		uncertainty := time.Duration(0)
		if peer == "d" {
			uncertainty = 60 * time.Millisecond
		}
		clock.offer(peer, offsets[i+1]-offsets[0], uncertainty, now)
	}
	if _, ok := clock.corrected(now, names, "a"); ok {
		t.Fatal("accepted a median wider than the clock uncertainty bound")
	}
}

func TestMedianClockUncertaintyBoundary(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name        string
		uncertainty time.Duration
		wantReady   bool
	}{
		{"at bound", clockMaxUncertainty, true},
		{"over bound", clockMaxUncertainty + time.Nanosecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var clock electionClock
			clock.offer("b", 0, tc.uncertainty, now)
			clock.offer("c", 0, tc.uncertainty, now)
			_, ok := clock.corrected(now, []string{"a", "b", "c"}, "a")
			if ok != tc.wantReady {
				t.Fatalf("ready = %v, want %v", ok, tc.wantReady)
			}
		})
	}
}

func TestMedianClockWideSampleAcrossIndependentFrames(t *testing.T) {
	now := time.Now()
	names := []string{"a", "b", "c", "d", "e", "f"}
	skews := []time.Duration{-7 * time.Second, -3 * time.Second, -time.Second, 2 * time.Second, 4 * time.Second, 30 * time.Second}
	for i, me := range names {
		var clock electionClock
		clock.source = func(at time.Time) time.Time { return at.Add(skews[i]) }
		for j, peer := range names {
			if i == j {
				continue
			}
			uncertainty := time.Millisecond
			if peer == "f" {
				uncertainty = 60 * time.Millisecond
			}
			clock.offer(peer, skews[j]-skews[i], uncertainty, now)
		}
		got, ok := clock.corrected(now, names, me)
		want := now.Add((skews[2] + skews[3]) / 2)
		if !ok || absDuration(got.Sub(want)) > clockMaxUncertainty {
			t.Fatalf("%s: time %v, want within bound of %v; ready %v", me, got, want, ok)
		}
	}
}

func TestClockSamplingAndExpiry(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Kitchen", "Office"})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || r.Header.Get(readinessHeader) != clockMAC(word, "request", body) {
			t.Error("invalid clock request")
			return
		}
		var request clockMessage
		if json.Unmarshal(body, &request) != nil {
			t.Error("invalid clock request body")
			return
		}
		at := time.Now().Add(2 * time.Second).UnixNano()
		body, _ = json.Marshal(clockMessage{Nonce: request.Nonce, Received: at, Sent: at})
		w.Header().Set(readinessHeader, clockMAC(word, "response", body))
		_, _ = w.Write(body)
	}))
	defer server.Close()
	addr := server.Listener.Addr().(*net.TCPAddr)
	h.n.sampleClock(announce.Peer{Name: "Kitchen", Address: addr.IP.String(), Port: addr.Port}, "kitchen")
	now := time.Now()
	corrected, ok := h.n.electionTime(now)
	if !ok || absDuration(corrected.Sub(now)-time.Second) > clockMaxUncertainty {
		t.Fatalf("corrected time offset %v, ready %v", corrected.Sub(now), ok)
	}
	if _, ok := h.n.electionTime(now.Add(clockSampleLifetime + time.Second)); ok {
		t.Fatal("accepted an expired sample")
	}
	if _, ok := h.n.clock.corrected(now, []string{"different peer", "office"}, "Office"); ok {
		t.Fatal("accepted a sample from a different peer")
	}
}

func TestClockRejectsUntrustedResponse(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Kitchen", "Office"})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(readinessHeader, "untrusted")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	addr := server.Listener.Addr().(*net.TCPAddr)
	h.n.sampleClock(announce.Peer{Name: "Kitchen", Address: addr.IP.String(), Port: addr.Port}, "kitchen")
	if _, ok := h.n.electionTime(time.Now()); ok {
		t.Fatal("accepted an unauthenticated clock sample")
	}
}

func TestUnavailableElectionClockDoesNotAnswer(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Kitchen", "Office"})
	h.n.clockNow = func(time.Time) (time.Time, bool) { return time.Time{}, false }
	h.n.Woke(0)
	h.wait(time.Second)
	if h.mine != nil || h.at0 != 0 || len(h.opened) != 0 {
		t.Fatal("unavailable election clock allowed an answer")
	}
}

func TestSimulationKeepsBetterSampleAcrossJitter(t *testing.T) {
	start := time.Now()
	var clock electionClock
	clock.offer("kitchen", 0, time.Millisecond, start)
	// Every second subsequent response is near the maximum uncertainty.
	// Replacing the good sample unconditionally would make the clock expire
	// after only 250 ms, even though the earlier sample remains precise.
	for i := 0; i < 80; i++ {
		now := start.Add(time.Duration(i) * 100 * time.Millisecond)
		if i > 0 && i%20 == 0 {
			clock.offer("kitchen", 0, 19500*time.Microsecond, now)
		}
		if _, ok := clock.corrected(now, []string{"kitchen", "office"}, "office"); !ok {
			t.Fatalf("clock unavailable at %v despite a better retained sample", now.Sub(start))
		}
	}
}

func TestClockReplacesExpiredOrLessPreciseSample(t *testing.T) {
	now := time.Now()
	var clock electionClock
	clock.offer("kitchen", time.Second, 10*time.Millisecond, now)
	clock.offer("kitchen", 2*time.Second, 19*time.Millisecond, now.Add(time.Second))
	if clock.samples["kitchen"].offset != time.Second {
		t.Fatal("worse sample replaced a still more precise one")
	}
	clock.offer("kitchen", 2*time.Second, 19*time.Millisecond, now.Add(6*time.Second))
	if clock.samples["kitchen"].offset != 2*time.Second {
		t.Fatal("better sample did not replace an aged sample")
	}
}

func TestSimulationPeerMedianWithoutAnchor(t *testing.T) {
	start := time.Now()
	names := []string{"a", "b", "c", "d", "e", "f"}
	skews := []time.Duration{-7 * time.Second, -3 * time.Second, -time.Second, 2 * time.Second, 4 * time.Second, 30 * time.Second}
	for _, members := range [][]string{names, names[1:]} {
		var reference time.Time
		for i, me := range names {
			if !containsString(members, me) {
				continue
			}
			now := start.Add(skews[i])
			var clock electionClock
			for j, peer := range names {
				if peer != me && containsString(members, peer) {
					clock.offer(peer, skews[j]-skews[i], time.Millisecond, now)
				}
			}
			corrected, ok := clock.corrected(now, members, me)
			if !ok {
				t.Fatalf("%s: clock not ready", me)
			}
			// Compare wall values: each simulated device has an independent clock.
			if reference.IsZero() {
				reference = corrected.Round(0)
			} else if !corrected.Round(0).Equal(reference) {
				t.Fatalf("%s: %v differs from cohort median %v", me, corrected, reference)
			}
		}
	}
}

func TestPeerMedianRequiresEveryRosterSample(t *testing.T) {
	now := time.Now()
	var clock electionClock
	clock.offer("b", time.Second, time.Millisecond, now)
	if _, ok := clock.corrected(now, []string{"a", "b", "c"}, "a"); ok {
		t.Fatal("missing peer sample allowed an election clock")
	}
}

func TestOfflinePeerIsRemovedAndRejoins(t *testing.T) {
	start := time.Now()
	roster := []string{"a", "b", "c"}
	var clock electionClock
	if active := clock.active(roster, "a", start); !slices.Equal(active, roster) {
		t.Fatalf("initial membership %v", active)
	}
	clock.contact("b", start, "")
	clock.contact("c", start, "")
	clock.contact("b", start.Add(clockOfflineAfter), "")
	active := clock.active(roster, "a", start.Add(clockOfflineAfter))
	if !slices.Equal(active, []string{"a", "b"}) {
		t.Fatalf("offline peer remained in membership: %v", active)
	}
	clock.contact("c", start.Add(clockOfflineAfter+time.Second), "")
	active = clock.active(roster, "a", start.Add(clockOfflineAfter+time.Second))
	if !slices.Equal(active, roster) {
		t.Fatalf("returning peer did not rejoin: %v", active)
	}
}

func TestSimulationPartitionCannotFormTwoMajorities(t *testing.T) {
	roster := []string{"a", "b", "c", "d", "e", "f"}
	start := time.Now()
	for _, side := range [][]string{{"a", "b", "c"}, {"d", "e", "f"}} {
		var clock electionClock
		clock.active(roster, side[0], start)
		for _, peer := range side[1:] {
			clock.contact(peer, start.Add(clockOfflineAfter), "")
		}
		active := clock.active(roster, side[0], start.Add(clockOfflineAfter))
		if len(active) > len(roster)/2 {
			t.Fatalf("partition unexpectedly formed a majority: %v", active)
		}
	}
}

func TestMeasureClockOffsetRejectsImpossibleExchange(t *testing.T) {
	start := time.Unix(1700000000, 0)
	end := start.Add(10 * time.Millisecond)
	if _, ok := measureClockOffset(start, end, start.UnixNano(), start.Add(20*time.Millisecond).UnixNano()); ok {
		t.Fatal("accepted remote processing longer than the local round trip")
	}
	if _, ok := measureClockOffset(start, end, start.Add(time.Millisecond).UnixNano(), start.UnixNano()); ok {
		t.Fatal("accepted reversed remote timestamps")
	}
}
