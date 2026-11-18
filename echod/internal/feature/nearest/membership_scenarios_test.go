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
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// lockedBuffer collects a child's log while the test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// joinHarness runs real device processes (see TestJoinProcess), each with its own config singleton,
// state directory and HTTP server. Scenarios kill, pause, restart and re-wire them.
type joinHarness struct {
	t      *testing.T
	ports  map[string]int
	state  map[string]string
	cmds   map[string]*exec.Cmd
	logs   map[string]*lockedBuffer
	client *http.Client
}

func newJoinHarness(t *testing.T, names ...string) *joinHarness {
	t.Helper()
	if os.Getenv("TECHO5_MEMBERSHIP_SIMULATION") != "1" {
		t.Skip("set TECHO5_MEMBERSHIP_SIMULATION=1 for the subprocess simulation")
	}
	h := &joinHarness{t: t, ports: map[string]int{}, state: map[string]string{}, cmds: map[string]*exec.Cmd{}, logs: map[string]*lockedBuffer{},
		client: &http.Client{Timeout: 3 * time.Second}}
	for _, name := range names {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		h.ports[name] = l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		h.state[name] = t.TempDir()
	}
	t.Cleanup(func() {
		for name := range h.cmds {
			h.kill(name)
		}
	})
	return h
}

// start launches a device. visible is the peers it can see; wait is the peers it waits to hear
// listening before it starts discovery ("-" for none).
func (h *joinHarness) start(name, visible, wait string) {
	h.t.Helper()
	ports, _ := json.Marshal(h.ports)
	binary, err := os.Executable()
	if err != nil {
		h.t.Fatal(err)
	}
	cmd := exec.Command(binary, "-test.run=^TestJoinProcess$", "-test.timeout=600s")
	cmd.Env = append(os.Environ(), "TECHO5_JOIN_NODE="+name, "TECHO5_JOIN_PORTS="+string(ports),
		"TECHO5_JOIN_STATE="+h.state[name], "TECHO5_JOIN_VISIBLE="+visible, "TECHO5_JOIN_WAIT="+wait)
	h.logs[name] = &lockedBuffer{}
	cmd.Stdout, cmd.Stderr = io.Discard, h.logs[name]
	if err := cmd.Start(); err != nil {
		h.t.Fatal(err)
	}
	h.cmds[name] = cmd
}

func (h *joinHarness) kill(name string) {
	if cmd := h.cmds[name]; cmd != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		delete(h.cmds, name)
	}
}

func (h *joinHarness) pause(name string)  { _ = h.cmds[name].Process.Signal(syscall.SIGSTOP) }
func (h *joinHarness) resume(name string) { _ = h.cmds[name].Process.Signal(syscall.SIGCONT) }

func (h *joinHarness) get(name, path string) (string, int, error) {
	resp, err := h.client.Get(fmt.Sprintf("http://127.0.0.1:%d%s", h.ports[name], path))
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return strings.TrimSpace(string(body)), resp.StatusCode, nil
}

func (h *joinHarness) status(name string) (joinStatus, bool) {
	var s joinStatus
	body, code, err := h.get(name, "/simulation/status")
	return s, err == nil && code == http.StatusOK && json.Unmarshal([]byte(body), &s) == nil
}

func (h *joinHarness) wake(name string) string {
	body, _, err := h.get(name, "/simulation/wake")
	if err != nil {
		return "error: " + err.Error()
	}
	return body
}

func (h *joinHarness) op(name, query string) (string, bool) {
	body, code, err := h.get(name, "/simulation/op?"+query)
	return body, err == nil && code == http.StatusOK
}

// until polls cond every 250 ms and fails the test with a dump of every device if it never holds.
func (h *joinHarness) until(what string, timeout time.Duration, cond func() bool) {
	h.t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
		if cond() {
			return
		}
	}
	for name := range h.ports {
		if s, ok := h.status(name); ok {
			h.t.Logf("%s: %s epoch=%d members=%d authority=%.6s blocked=%q quorum=%v op=%+v", name, s.Info.Lifecycle,
				s.Info.Epoch, len(s.Info.Members), s.Info.Authority, s.Info.Blocked, s.Quorum, s.View.Operation)
		}
	}
	for name, buf := range h.logs {
		var keep []string
		for _, line := range strings.Split(buf.String(), "\n") {
			if strings.Contains(line, "membership") || strings.Contains(line, "retire") {
				keep = append(keep, line)
			}
		}
		if len(keep) > 40 {
			keep = keep[len(keep)-40:]
		}
		h.t.Logf("--- %s log\n%s", name, strings.Join(keep, "\n"))
	}
	h.t.Fatalf("timed out waiting for %s", what)
}

// ready is a device that is ready at epoch with that many members.
func (h *joinHarness) ready(name string, epoch uint64, members int) bool {
	s, ok := h.status(name)
	return ok && s.Info.Lifecycle == "ready" && s.Info.Epoch == epoch && len(s.Info.Members) == members
}

// coordinator is the name of the device that currently holds a wake-coordinator lease among names.
func (h *joinHarness) coordinator(names ...string) string {
	for _, name := range names {
		if s, ok := h.status(name); ok && s.Quorum && s.Clock {
			return name
		}
	}
	return ""
}

func (h *joinHarness) authorityName(asked string, names ...string) string {
	s, _ := h.status(asked)
	for _, name := range names {
		if o, ok := h.status(name); ok && o.Info.DeviceID == s.Info.Authority {
			return name
		}
	}
	return ""
}

func (h *joinHarness) formThree() {
	for _, name := range []string{"a", "b", "c"} {
		h.start(name, "a,b,c,d,e", "a,b,c")
	}
	h.until("three devices ready at epoch 3", 90*time.Second, func() bool {
		return h.ready("a", 3, 3) && h.ready("b", 3, 3) && h.ready("c", 3, 3)
	})
}

// Fourth device joins while an old member is offline: the parent majority commits, the offline member
// still counts, and on its return it replays history before voting.
func TestFourthDeviceJoinsWithOneMemberOfflineThenReplay(t *testing.T) {
	h := newJoinHarness(t, "a", "b", "c", "d")
	h.formThree()
	h.kill("c")
	h.start("d", "a,b,c,d", "a,b")
	h.until("d joined at epoch 4 with c still a member", 90*time.Second, func() bool {
		return h.ready("a", 4, 4) && h.ready("b", 4, 4) && h.ready("d", 4, 4)
	})
	h.start("c", "a,b,c,d", "a,b,d")
	h.until("returning c replays history to epoch 4", 90*time.Second, func() bool { return h.ready("c", 4, 4) })
	a, _ := h.status("a")
	c, _ := h.status("c")
	if a.Info.Fingerprint != c.Info.Fingerprint {
		t.Fatalf("c installed a different roster: %+v vs %+v", c.Info, a.Info)
	}
}

// Authority offline: the existing majority still elects a wake coordinator, a new device waits, and
// replacing the authority from a survivor lets the join finish.
func TestAuthorityOfflineWakesContinueAndReplacementAdmitsNewDevice(t *testing.T) {
	h := newJoinHarness(t, "a", "b", "c", "d")
	h.formThree()
	authority := h.authorityName("a", "a", "b", "c")
	var survivors []string
	for _, n := range []string{"a", "b", "c"} {
		if n != authority {
			survivors = append(survivors, n)
		}
	}
	h.kill(authority)
	h.until("survivors elect a coordinator with the denominator still three", 60*time.Second, func() bool {
		return h.coordinator(survivors...) != ""
	})
	h.start("d", "a,b,c,d", "-")
	time.Sleep(15 * time.Second)
	if s, _ := h.status("d"); s.Info.Epoch != 0 || s.Info.Blocked == "" && s.Info.Lifecycle == "ready" {
		t.Fatalf("d joined without an authority: %+v", s.Info)
	}
	view, _ := h.status(survivors[0])
	newAuthority := ""
	for _, m := range view.View.Own.Members {
		if m.Name == survivors[1] {
			newAuthority = m.ID
		}
	}
	query := fmt.Sprintf("op=authority&cluster=%s&epoch=3&device=%s", view.Info.Cluster, newAuthority)
	if msg, ok := h.op(survivors[0], query); !ok {
		t.Fatalf("replace authority refused: %s", msg)
	}
	h.until("authority replaced (epoch 4) and d admitted (epoch 5)", 120*time.Second, func() bool {
		return h.ready(survivors[0], 5, 4) && h.ready(survivors[1], 5, 4) && h.ready("d", 5, 4)
	})
}

// A 3/2 partition of five: only the three-member side keeps a coordinator, the denominator stays five,
// and when the majority shrinks to two nothing answers.
func TestFiveMemberPartitionOnlyMajorityRenewsQuorum(t *testing.T) {
	h := newJoinHarness(t, "a", "b", "c", "d", "e")
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		h.start(name, "a,b,c,d,e", "a,b,c,d,e")
	}
	h.until("five devices ready", 240*time.Second, func() bool {
		for _, n := range []string{"a", "b", "c", "d", "e"} {
			s, ok := h.status(n)
			if !ok || s.Info.Lifecycle != "ready" || len(s.Info.Members) != 5 {
				return false
			}
		}
		return true
	})
	h.pause("d")
	h.pause("e")
	h.until("the three-member side renews a coordinator", 60*time.Second, func() bool { return h.coordinator("a", "b", "c") != "" })
	if s, _ := h.status("a"); len(s.Info.Members) != 5 {
		t.Fatalf("the denominator changed with the partition: %d members", len(s.Info.Members))
	}
	h.pause("c")
	time.Sleep(25 * time.Second)
	if got := h.coordinator("a", "b"); got != "" {
		t.Fatalf("two of five kept a coordinator: %s", got)
	}
	if r := h.wake("b"); r != "0" {
		t.Fatalf("a minority answered a wake: %q", r)
	}
}

// Two groups form apart, meet, drop wakes, and the kept group retires the other through that group's own
// majority; the retired devices then re-enrol one by one, and the conflict clears only afterwards.
func TestConflictingClustersFailClosedThenKeepAndReenrol(t *testing.T) {
	h := newJoinHarness(t, "a", "b", "c", "d")
	h.start("a", "a,b", "-")
	h.start("b", "a,b", "-")
	h.start("c", "c,d", "-")
	h.start("d", "c,d", "-")
	h.until("two separate groups of two", 120*time.Second, func() bool {
		return h.ready("a", 2, 2) && h.ready("b", 2, 2) && h.ready("c", 2, 2) && h.ready("d", 2, 2)
	})
	for _, name := range []string{"a", "b", "c", "d"} {
		h.get(name, "/simulation/peers?set=a,b,c,d")
	}
	h.until("every device reports the conflict", 60*time.Second, func() bool {
		for _, n := range []string{"a", "b", "c", "d"} {
			if s, ok := h.status(n); !ok || s.Info.Lifecycle != "conflict" {
				return false
			}
		}
		return true
	})
	for _, name := range []string{"a", "b", "c", "d"} {
		if r := h.wake(name); r != "0" {
			t.Fatalf("%s answered a wake during a conflict: %q", name, r)
		}
	}
	a, _ := h.status("a")
	if len(a.View.Conflicts) != 1 {
		t.Fatalf("the page would list %d other clusters, want 1", len(a.View.Conflicts))
	}
	if msg, ok := h.op("a", fmt.Sprintf("op=keep&cluster=%s&epoch=%d", a.Info.Cluster, a.Info.Epoch+7)); ok {
		t.Fatalf("a stale keep was accepted: %s", msg)
	}
	// Keeping is a control the person can press again; a first round that did not finish is retried
	// against the same retirement transaction.
	for attempt := 0; ; attempt++ {
		if msg, ok := h.op("a", fmt.Sprintf("op=keep&cluster=%s&epoch=%d", a.Info.Cluster, a.Info.Epoch)); !ok {
			t.Fatalf("keep refused: %s", msg)
		}
		h.until("the keep operation ends", 120*time.Second, func() bool {
			s, ok := h.status("a")
			return ok && !s.View.Operation.Running
		})
		if s, _ := h.status("a"); s.View.Operation.Error == "" {
			break
		} else if attempt == 3 {
			t.Fatalf("keep never completed: %s", s.View.Operation.Error)
		}
	}
	h.until("the losing group is retired", 120*time.Second, func() bool {
		c, okc := h.status("c")
		d, okd := h.status("d")
		return okc && okd && c.Info.Retired && d.Info.Retired
	})
	h.until("the winners clear the conflict only after retirement", 60*time.Second, func() bool {
		s, ok := h.status("a")
		return ok && len(s.View.Conflicts) == 0 && s.Info.Lifecycle == "ready"
	})
	for _, name := range []string{"c", "d"} {
		if msg, ok := h.op(name, "op=reenrol"); !ok {
			t.Fatalf("%s re-enrol refused: %s", name, msg)
		}
		h.until(name+" joined the kept group", 120*time.Second, func() bool { return h.ready(name, 2+uint64(strings.Index("cd", name))+1, 3+strings.Index("cd", name)) })
	}
}

// A losing group without a reachable majority is never reported retired, and the page says who is missing.
func TestLosingClusterWithoutMajorityBlocksRecovery(t *testing.T) {
	h := newJoinHarness(t, "a", "b", "c", "d", "e")
	h.start("a", "a,b", "-")
	h.start("b", "a,b", "-")
	for _, name := range []string{"c", "d", "e"} {
		h.start(name, "c,d,e", "-")
	}
	h.until("a group of two and a group of three", 180*time.Second, func() bool {
		return h.ready("a", 2, 2) && h.ready("b", 2, 2) && h.ready("c", 3, 3) && h.ready("d", 3, 3) && h.ready("e", 3, 3)
	})
	h.kill("d")
	h.kill("e")
	h.get("a", "/simulation/peers?set=a,b,c")
	h.get("b", "/simulation/peers?set=a,b,c")
	h.get("c", "/simulation/peers?set=a,b,c")
	h.until("a notices the other group", 60*time.Second, func() bool {
		s, ok := h.status("a")
		return ok && len(s.View.Conflicts) == 1
	})
	a, _ := h.status("a")
	h.op("a", fmt.Sprintf("op=keep&cluster=%s&epoch=%d", a.Info.Cluster, a.Info.Epoch))
	h.until("the operation reports it is blocked", 60*time.Second, func() bool {
		s, ok := h.status("a")
		return ok && !s.View.Operation.Running && strings.Contains(s.View.Operation.Error, "no reachable majority")
	})
	if c, _ := h.status("c"); c.Info.Retired {
		t.Fatal("a cluster without a reachable majority was retired")
	}
	if s, _ := h.status("a"); len(s.View.Conflicts) != 1 {
		t.Fatal("the conflict was cleared although retirement was not installed")
	}
}
