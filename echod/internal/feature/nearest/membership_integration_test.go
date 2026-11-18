package nearest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
)

type joinStatus struct {
	Info   membershipInfo `json:"info"`
	Leader string         `json:"leader"`
	Quorum bool           `json:"quorum"`
	Clock  bool           `json:"clock"`
	Ready  bool           `json:"ready"`
	View   MembershipView `json:"view"`
}

// TestJoinProcess is a subprocess fixture: a device with its own config singleton, state directory and
// HTTP server, no roster and no mocked membership. It runs the production loops.
func TestJoinProcess(t *testing.T) {
	name := os.Getenv("TECHO5_JOIN_NODE")
	if name == "" {
		return
	}
	var ports map[string]int
	if err := json.Unmarshal([]byte(os.Getenv("TECHO5_JOIN_PORTS")), &ports); err != nil {
		t.Fatal(err)
	}
	bootstrapSettle = 2 * time.Second
	membershipDrain = 800 * time.Millisecond
	h := newHouse(t)
	state := os.Getenv("TECHO5_JOIN_STATE")
	if state == "" {
		state = t.TempDir()
	}
	config.Started(config.Device{Name: name})
	if err := config.Set().Nearest().Mode(modeElected); err != nil {
		t.Fatal(err)
	}
	n := h.n
	n.membership = newMembershipRuntime(state)
	n.membership.onGate = n.electionReset
	n.clockNow = n.electionTime
	n.now = time.Now
	var visible atomic.Pointer[[]announce.Peer]
	setVisible := func(names []string) {
		var peers []announce.Peer
		for _, other := range names {
			if port, ok := ports[other]; ok && other != name {
				peers = append(peers, announce.Peer{Name: other, Address: "127.0.0.1", Port: port})
			}
		}
		visible.Store(&peers)
	}
	initial := os.Getenv("TECHO5_JOIN_VISIBLE")
	if initial == "" {
		for other := range ports {
			initial += other + ","
		}
	}
	setVisible(strings.Split(strings.Trim(initial, ","), ","))
	n.peers = func() []announce.Peer { return *visible.Load() }
	n.score = func() float64 { return float64(name[0]-'a') + 1 }
	var opened atomic.Int64
	n.woke = func(int, time.Time) { opened.Add(1) }
	mux := http.NewServeMux()
	for path, handler := range n.membershipRoutes() {
		mux.HandleFunc(path, handler)
	}
	mux.HandleFunc(peerStatusPath, n.receivePeerStatus)
	mux.HandleFunc(clockPath, n.receiveClock)
	mux.HandleFunc(v3Path, n.receiveReadiness)
	mux.HandleFunc(coordinatorVotePath, func(w http.ResponseWriter, r *http.Request) { n.coordinatorReceive(w, r, false) })
	mux.HandleFunc(coordinatorWakePath, func(w http.ResponseWriter, r *http.Request) { n.coordinatorReceive(w, r, true) })
	mux.HandleFunc("/simulation/status", func(w http.ResponseWriter, _ *http.Request) {
		now := time.Now()
		leader, term := n.coordinator.leaderFor(now)
		_, clockReady := n.clockNow(now)
		_ = json.NewEncoder(w).Encode(joinStatus{Info: n.membershipInfo(), Leader: leader,
			Quorum: n.coordinator.authorised(strings.ToLower(name), term, now), Clock: clockReady, Ready: n.coordinationOpen(),
			View: n.MembershipView()})
	})
	mux.HandleFunc("/simulation/peers", func(w http.ResponseWriter, r *http.Request) {
		setVisible(strings.Split(r.URL.Query().Get("set"), ","))
	})
	// The operator controls, called as the setup page's forms call them.
	mux.HandleFunc("/simulation/op", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		epoch, _ := strconv.ParseUint(q.Get("epoch"), 10, 64)
		var err error
		switch q.Get("op") {
		case "authority":
			err = n.DoReplaceAuthority(q.Get("cluster"), epoch, q.Get("device"))
		case "remove":
			err = n.DoRemoveDevice(q.Get("cluster"), epoch, q.Get("device"))
		case "keep":
			err = n.DoKeepCluster(q.Get("cluster"), epoch)
		case "retry":
			err = n.DoRetryPending(q.Get("tx"))
		case "reenrol":
			err = n.DoReEnrol()
		default:
			err = fmt.Errorf("unknown op")
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
		}
	})
	mux.HandleFunc("/simulation/wake", func(w http.ResponseWriter, _ *http.Request) {
		before := opened.Load()
		n.Woke(0)
		time.Sleep(800 * time.Millisecond)
		_, _ = fmt.Fprint(w, opened.Load()-before)
	})
	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports[name])))
	if err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	n.enabled = true
	n.mu.Unlock()
	go http.Serve(listener, mux)
	// Start discovery only once every device is listening: a device that starts looking while the others
	// are still booting would, correctly, bootstrap a cluster of its own, and this test is about
	// devices that can see one another.
	waitFor := os.Getenv("TECHO5_JOIN_WAIT")
	for other, port := range ports {
		if other == name || (waitFor != "" && !strings.Contains(","+waitFor+",", ","+other+",")) {
			continue
		}
		for until := time.Now().Add(90 * time.Second); time.Now().Before(until); time.Sleep(100 * time.Millisecond) {
			c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second)
			if err == nil {
				_ = c.Close()
				break
			}
		}
		_ = other
	}
	go n.peerStatusLoop()
	go n.membershipLoop()
	go n.clockLoop()
	go n.coordinatorLoop()
	select {}
}

// TestThreeDevicesJoinAutomatically is the vertical slice: three separate processes with no roster
// configuration find each other over loopback HTTP, form one cluster at epoch 3, elect a wake
// coordinator and route a wake through the production gate.
func TestThreeDevicesJoinAutomatically(t *testing.T) {
	if os.Getenv("TECHO5_MEMBERSHIP_SIMULATION") != "1" {
		t.Skip("set TECHO5_MEMBERSHIP_SIMULATION=1 for the subprocess simulation")
	}
	names := []string{"a", "b", "c"}
	ports := map[string]int{}
	for _, name := range names {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ports[name] = l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
	}
	portJSON, _ := json.Marshal(ports)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var cmds []*exec.Cmd
	logs := map[string]*bytes.Buffer{}
	t.Cleanup(func() {
		if t.Failed() {
			for _, name := range names {
				lines := strings.Split(logs[name].String(), "\n")
				if len(lines) > 60 {
					lines = lines[len(lines)-60:]
				}
				t.Logf("--- %s log tail\n%s", name, strings.Join(lines, "\n"))
			}
		}
	})
	for _, name := range names {
		cmd := exec.Command(binary, "-test.run=^TestJoinProcess$", "-test.timeout=240s")
		cmd.Env = append(os.Environ(), "TECHO5_JOIN_NODE="+name, "TECHO5_JOIN_PORTS="+string(portJSON))
		logs[name] = &bytes.Buffer{}
		cmd.Stdout, cmd.Stderr = io.Discard, logs[name]
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		cmds = append(cmds, cmd)
	}
	defer func() {
		for _, cmd := range cmds {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	httpClient := &http.Client{Timeout: 2 * time.Second}
	status := func(name string) (joinStatus, bool) {
		var s joinStatus
		resp, err := httpClient.Get(fmt.Sprintf("http://127.0.0.1:%d/simulation/status", ports[name]))
		if err != nil {
			return s, false
		}
		defer resp.Body.Close()
		return s, json.NewDecoder(resp.Body).Decode(&s) == nil
	}
	until := time.Now().Add(90 * time.Second)
	for {
		all := true
		for _, name := range names {
			s, ok := status(name)
			if !ok || s.Info.Lifecycle != "ready" || s.Info.Epoch != 3 || len(s.Info.Members) != 3 {
				all = false
			}
		}
		if all {
			break
		}
		if time.Now().After(until) {
			for _, name := range names {
				s, ok := status(name)
				t.Logf("%s: %v %+v", name, ok, s)
			}
			t.Fatal("devices did not form one ready three-member cluster")
		}
		time.Sleep(500 * time.Millisecond)
	}
	first, _ := status("a")
	for _, name := range names {
		s, _ := status(name)
		if s.Info.Cluster != first.Info.Cluster || s.Info.Fingerprint != first.Info.Fingerprint {
			t.Fatalf("%s is in a different cluster or roster: %+v vs %+v", name, s.Info, first.Info)
		}
	}
	// Wait for an elected coordinator with quorum, then send the same wake to all three.
	deadline := time.Now().Add(40 * time.Second)
	for {
		leaders := 0
		for _, name := range names {
			if s, ok := status(name); ok && s.Quorum && s.Clock {
				leaders++
			}
		}
		if leaders == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no wake coordinator obtained quorum")
		}
		time.Sleep(250 * time.Millisecond)
	}
	results := make([]string, len(names))
	done := make(chan int, len(names))
	for i, name := range names {
		go func() {
			resp, err := httpClient.Get(fmt.Sprintf("http://127.0.0.1:%d/simulation/wake", ports[name]))
			if err != nil {
				results[i] = "error: " + err.Error()
			} else {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				results[i] = strings.TrimSpace(string(body))
			}
			done <- i
		}()
	}
	for range names {
		<-done
	}
	if results[0] != "0" || results[1] != "0" || results[2] != "1" {
		t.Fatalf("wake results = %v; only c, the best score, may open", results)
	}
}
