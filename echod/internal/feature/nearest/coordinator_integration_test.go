package nearest

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
)

type simulationStatus struct {
	Leader  string   `json:"leader"`
	Term    uint64   `json:"term"`
	Quorum  bool     `json:"quorum"`
	Clock   bool     `json:"clock"`
	Wide    bool     `json:"wide"`
	Members []string `json:"members"`
}

// TestCoordinatorProcess is a subprocess fixture for the six-device test. Each
// process has its own real config singleton, HTTP server and coordinator state.
func TestCoordinatorProcess(t *testing.T) {
	name := os.Getenv("TECHO5_SIM_NODE")
	if name == "" {
		return
	}
	var ports map[string]int
	if err := json.Unmarshal([]byte(os.Getenv("TECHO5_SIM_PORTS")), &ports); err != nil {
		t.Fatal(err)
	}
	names := []string{"a", "b", "c", "d", "e", "f"}
	h := newHouse(t)
	enableCoordinatorRoster(t, h, name, names)
	// Give each subprocess an independent synthetic clock offset and exercise
	// the real peer calibration path rather than assuming equal wall clocks.
	skew := time.Duration(int(name[0]-'a')-3) * time.Second
	h.n.clock.source = func(at time.Time) time.Time { return at.Add(skew) }
	h.n.clockNow = h.n.electionTime
	var peers []announce.Peer
	for _, other := range names {
		if other != name {
			peers = append(peers, announce.Peer{Name: other, Address: "127.0.0.1", Port: ports[other]})
		}
	}
	h.n.peers = func() []announce.Peer { return peers }
	h.n.now = time.Now
	h.n.score = func() float64 { return float64(name[0] - 'a' + 1) }
	var opened atomic.Int64
	var delayMillis atomic.Int64
	var clockDelayMillis atomic.Int64
	h.n.woke = func(int, time.Time) { opened.Add(1) }
	mux := http.NewServeMux()
	delay := func(handler http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(time.Duration(delayMillis.Load()) * time.Millisecond)
			handler(w, r)
		}
	}
	mux.HandleFunc(clockPath, delay(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Duration(clockDelayMillis.Load()) * time.Millisecond)
		h.n.receiveClock(w, r)
	}))
	mux.HandleFunc(coordinatorVotePath, delay(func(w http.ResponseWriter, r *http.Request) {
		h.n.coordinatorReceive(w, r, false)
	}))
	mux.HandleFunc(coordinatorWakePath, delay(func(w http.ResponseWriter, r *http.Request) {
		h.n.coordinatorReceive(w, r, true)
	}))
	mux.HandleFunc("/simulation/delay", func(w http.ResponseWriter, r *http.Request) {
		value, err := strconv.ParseInt(r.URL.Query().Get("ms"), 10, 64)
		if err != nil || value < 0 || value > 1000 {
			http.Error(w, "invalid delay", http.StatusBadRequest)
			return
		}
		delayMillis.Store(value)
	})
	mux.HandleFunc("/simulation/clock-delay", func(w http.ResponseWriter, r *http.Request) {
		value, err := strconv.ParseInt(r.URL.Query().Get("ms"), 10, 64)
		if err != nil || value < 0 || value > 1000 {
			http.Error(w, "invalid delay", http.StatusBadRequest)
			return
		}
		clockDelayMillis.Store(value)
	})
	mux.HandleFunc("/simulation/status", func(w http.ResponseWriter, _ *http.Request) {
		now := time.Now()
		leader, term := h.n.coordinator.leaderFor(now)
		_, clockReady := h.n.clockNow(now)
		h.n.clock.mu.Lock()
		wide := h.n.clock.samples["f"].uncertainty > clockMaxUncertainty
		h.n.clock.mu.Unlock()
		status := simulationStatus{
			Leader: leader, Term: term,
			Quorum:  h.n.coordinator.authorised(name, term, now),
			Clock:   clockReady,
			Wide:    wide,
			Members: h.n.clock.active(names, name, now),
		}
		_ = json.NewEncoder(w).Encode(status)
	})
	mux.HandleFunc("/simulation/wake", func(w http.ResponseWriter, _ *http.Request) {
		before := opened.Load()
		h.n.Woke(0)
		time.Sleep(800 * time.Millisecond)
		_, _ = fmt.Fprint(w, opened.Load()-before)
	})
	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports[name])))
	if err != nil {
		t.Fatal(err)
	}
	go h.n.clockLoop()
	go h.n.coordinatorLoop()
	if err := http.Serve(listener, mux); err != nil {
		t.Fatal(err)
	}
}

func TestSixNodeCoordinatorSimulation(t *testing.T) {
	if os.Getenv("TECHO5_COORDINATOR_SIMULATION") != "1" {
		t.Skip("set TECHO5_COORDINATOR_SIMULATION=1 for the subprocess simulation")
	}
	names := []string{"a", "b", "c", "d", "e", "f"}
	ports := make(map[string]int)
	for _, name := range names {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ports[name] = listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()
	}
	portJSON, _ := json.Marshal(ports)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	processes := make(map[string]*exec.Cmd)
	start := func(name string) {
		t.Helper()
		cmd := exec.Command(binary, "-test.run=^TestCoordinatorProcess$", "-test.timeout=180s")
		cmd.Env = append(os.Environ(), "TECHO5_SIM_NODE="+name, "TECHO5_SIM_PORTS="+string(portJSON),
			"GORACE=halt_on_error=1")
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		processes[name] = cmd
	}
	stop := func(name string) {
		t.Helper()
		if cmd := processes[name]; cmd != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			delete(processes, name)
		}
	}
	defer func() {
		for name := range processes {
			stop(name)
		}
	}()
	for _, name := range names {
		start(name)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	status := func(name string) (simulationStatus, bool) {
		var s simulationStatus
		resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/simulation/status", ports[name]))
		if err != nil {
			return s, false
		}
		defer resp.Body.Close()
		return s, json.NewDecoder(resp.Body).Decode(&s) == nil
	}
	awaitLeader := func(want string, members int, timeout time.Duration) {
		t.Helper()
		until := time.Now().Add(timeout)
		for time.Now().Before(until) {
			s, ok := status(want)
			if ok && s.Leader == want && s.Quorum && len(s.Members) == members {
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
		for _, name := range names {
			if s, ok := status(name); ok {
				t.Logf("%s: %+v", name, s)
			}
		}
		t.Fatalf("coordinator %s did not obtain quorum with %d members", want, members)
	}
	awaitLeader("a", 6, 20*time.Second)
	// Allow heartbeats to reach the two claimants, then send the same wake to
	// both at once; only c, the higher-scoring claimant, should open.
	time.Sleep(2 * time.Second)
	wakes := make([]string, 2)
	var wg sync.WaitGroup
	for i, name := range []string{"b", "c"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/simulation/wake", ports[name]))
			if err != nil {
				wakes[i] = "error: " + err.Error()
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			wakes[i] = strings.TrimSpace(string(body))
		}()
	}
	wg.Wait()
	if wakes[0] != "0" || wakes[1] != "1" {
		t.Fatalf("simultaneous wake results b=%q c=%q", wakes[0], wakes[1])
	}
	// Exercise all participants, including the coordinator itself, with
	// staggered detections inside the same collection window.
	allWakes := make([]string, len(names))
	for i, name := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(i*12) * time.Millisecond)
			resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/simulation/wake", ports[name]))
			if err != nil {
				allWakes[i] = "error: " + err.Error()
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			allWakes[i] = strings.TrimSpace(string(body))
		}()
	}
	wg.Wait()
	for i, result := range allWakes {
		want := "0"
		if names[i] == "f" {
			want = "1"
		}
		if result != want {
			t.Fatalf("staggered wake results = %v; want only f to answer", allWakes)
		}
	}
	// A paused leader retains its old term, unlike a crashed/restarted process.
	// It must be able to rejoin after the replacement has used that same term.
	if err := processes["a"].Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	awaitLeader("b", 5, 40*time.Second)
	if err := processes["a"].Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	awaitLeader("a", 6, 25*time.Second)
	// One active noncentral peer has sustained 60 ms clock exchanges. Wait
	// until the leader has actually replaced the old narrow sample, then check
	// that the bounded cohort median remains usable and a wake can answer.
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/simulation/clock-delay?ms=60", ports["f"]))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	until := time.Now().Add(20 * time.Second)
	observed := false
	for time.Now().Before(until) {
		s, ok := status("a")
		if ok && len(s.Members) == 6 && s.Wide && s.Clock && s.Quorum {
			observed = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !observed {
		t.Fatal("leader failed to maintain a bounded median with a wide noncentral peer sample")
	}
	resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/simulation/wake", ports["b"]))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if strings.TrimSpace(string(body)) != "1" {
		t.Fatalf("wake failed with usable median and one wide sample: %s", body)
	}
	resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/simulation/clock-delay?ms=0", ports["f"]))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	awaitLeader("a", 6, 20*time.Second)
	// Delayed coordinator traffic must time out without an answer, not fall
	// through to the older 500 ms election.
	resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/simulation/delay?ms=600", ports["a"]))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/simulation/wake", ports["b"]))
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if strings.TrimSpace(string(body)) != "0" {
		t.Fatalf("delayed coordinator caused an answer: %q", body)
	}
	resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/simulation/delay?ms=0", ports["a"]))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	awaitLeader("a", 6, 15*time.Second)
	// Crash the coordinator while the collection window is open. With strict
	// elected mode has no legacy timer that may answer the unfinished wake.
	crashWakes := make([]string, 2)
	for i, name := range []string{"b", "c"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/simulation/wake", ports[name]))
			if err != nil {
				crashWakes[i] = "error: " + err.Error()
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			crashWakes[i] = strings.TrimSpace(string(body))
		}()
	}
	time.Sleep(40 * time.Millisecond)
	stop("a")
	wg.Wait()
	if crashWakes[0] != "0" || crashWakes[1] != "0" {
		t.Fatalf("wake during coordinator crash answered: %v", crashWakes)
	}
	awaitLeader("b", 5, 40*time.Second)
	start("a")
	awaitLeader("a", 6, 25*time.Second)
	// Three stopped nodes leave no majority; the surviving half must not answer.
	for _, name := range []string{"a", "b", "c"} {
		stop(name)
	}
	time.Sleep(4 * time.Second)
	for _, name := range []string{"d", "e", "f"} {
		if s, ok := status(name); ok && s.Quorum {
			t.Fatalf("minority device %s retained coordinator quorum: %+v", name, s)
		}
	}
	resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/simulation/wake", ports["d"]))
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if strings.TrimSpace(string(body)) != "0" {
		t.Fatalf("minority partition answered: %q", body)
	}
	for _, name := range []string{"a", "b", "c"} {
		start(name)
	}
	awaitLeader("a", 6, 30*time.Second)
}
