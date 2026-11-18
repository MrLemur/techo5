package nearest

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
	"github.com/HuskerMinion/techo5/echod/internal/feature/nearest/membership"
)

// A fence must wait for a release already reserved on the old generation, and no new release may start
// on it afterwards: the voice callback can never run once the fence has been acknowledged.
func TestFenceWaitsForReservedReleaseAndBlocksNewOnes(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen", "Den"})
	rt := h.n.membership
	started, finish := make(chan struct{}), make(chan struct{})
	released := make(chan bool, 1)
	go func() {
		released <- rt.releaseIfCurrent(h.token, func() { close(started); <-finish })
	}()
	<-started
	current, _, _ := rt.rosterStore().View()
	change, err := membership.NewRosterChange(current, "t1", "add", "zed")
	if err != nil {
		t.Fatal(err)
	}
	fenced := make(chan error, 1)
	go func() { fenced <- rt.closeForFence(change) }()
	select {
	case <-fenced:
		t.Fatal("fence returned while a reserved release was still running")
	case <-time.After(100 * time.Millisecond):
	}
	if rt.releaseIfCurrent(h.token, func() { t.Error("callback ran after the fence") }) {
		t.Fatal("a new release started on the old generation during the fence")
	}
	close(finish)
	if err := <-fenced; err != nil {
		t.Fatal(err)
	}
	if !<-released {
		t.Fatal("the already reserved release was not allowed to finish")
	}
	if rt.valid(h.token) {
		t.Fatal("old token still valid after fence")
	}
}

// Whichever code path changes the house word, mode or enablement, the old generation stops working and
// the roster itself is kept.
func TestSettingsChangeInvalidatesGenerationButKeepsRoster(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen", "Den"})
	rt := h.n.membership
	if !rt.valid(h.token) {
		t.Fatal("seeded membership should start ready")
	}
	if err := config.Set().Home().HouseWord("a different house word"); err != nil {
		t.Fatal(err)
	}
	defer config.Set().Home().HouseWord(word)
	if rt.valid(h.token) || rt.snapshot().Blocked != "settings_changed" {
		t.Fatalf("settings change left the generation usable: %+v", rt.snapshot())
	}
	if !rt.refreshConfig() {
		t.Fatal("refresh did not notice the change")
	}
	if got := rt.committedMembers(); len(got) != 3 {
		t.Fatalf("roster lost on settings change: %v", got)
	}
	if rt.snapshot().Blocked != "quiet_period" {
		t.Fatalf("new generation should wait out the old vote grant: %+v", rt.snapshot())
	}
}

func TestMembershipEndpointRejectsBadRequests(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen", "Den"})
	h.n.enabled = true
	status := h.n.membershipRoutes()[membershipPrefix+"status"]
	srv := httptest.NewServer(status)
	defer srv.Close()
	post := func(body []byte, mac string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader(body))
		req.Header.Set(readinessHeader, mac)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	good, _ := json.Marshal(membershipRequest{Version: membershipVersion, ID: newRequestID(), From: "kitchen"})
	mac := membershipMAC(word, "request:status", good)
	if resp := post(good, "bad"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("unauthenticated request = %d", resp.StatusCode)
	}
	big := bytes.Repeat([]byte("x"), maxMembershipBody+1)
	if resp := post(big, membershipMAC(word, "request:status", big)); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("oversized request = %d", resp.StatusCode)
	}
	old, _ := json.Marshal(membershipRequest{Version: 99, ID: newRequestID(), From: "kitchen"})
	if resp := post(old, membershipMAC(word, "request:status", old)); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unsupported version = %d", resp.StatusCode)
	}
	wrong, _ := json.Marshal(membershipRequest{Version: membershipVersion, ID: newRequestID(), From: "kitchen", To: "someone-else"})
	if resp := post(wrong, membershipMAC(word, "request:status", wrong)); resp.StatusCode != http.StatusConflict {
		t.Fatalf("wrong recipient = %d", resp.StatusCode)
	}
	resp := post(good, mac)
	defer resp.Body.Close()
	var out bytes.Buffer
	_, _ = out.ReadFrom(resp.Body)
	if resp.StatusCode != http.StatusOK || strings.Contains(out.String(), word) {
		t.Fatalf("valid status = %d, body %s", resp.StatusCode, out.String())
	}
	var parsed membershipResponse
	if json.Unmarshal(out.Bytes(), &parsed) != nil || parsed.Info == nil || parsed.To != "kitchen" || parsed.Info.Cluster != "test" {
		t.Fatalf("unexpected status body: %s", out.String())
	}
}

// A retransmitted commit is idempotent and does not close the gate or start a new generation.
func TestInstallRetransmitDoesNotDisturbTheGate(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen", "Den"})
	rt := h.n.membership
	rt.rosterStore().SetTiming(nil, 20*time.Millisecond)
	current, _, _ := rt.rosterStore().View()
	change, err := membership.NewRosterChange(current, "t1", "add", "zed")
	if err != nil {
		t.Fatal(err)
	}
	decision := membership.RosterDecision{Change: change, JoiningDevice: "zed", Acknowledgments: []membership.RosterAck{
		{Change: change, Voter: "den"}, {Change: change, Voter: "office"}}}
	var installed error
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if installed = rt.install(decision); installed == nil {
			break
		}
	}
	if installed != nil {
		t.Fatalf("install: %v", installed)
	}
	before := rt.snapshot()
	if err := rt.install(decision); err != nil {
		t.Fatalf("retransmit: %v", err)
	}
	if rt.snapshot().Token != before.Token {
		t.Fatal("a retransmitted commit changed the membership generation")
	}
	if len(rt.committedMembers()) != 4 {
		t.Fatalf("members = %v", rt.committedMembers())
	}
}

// Votes are refused for a full grant after a generation change, as after a restart, and the old
// leader and grants are gone.
func TestElectionResetQuarantinesVotes(t *testing.T) {
	var c coordinatorRuntime
	now := time.Now()
	c.vote("a", 1, now) // a first call starts the restart quarantine
	if _, yes := c.vote("a", 1, now.Add(coordinatorGrant)); !yes {
		t.Fatal("setup vote refused")
	}
	reset := now.Add(coordinatorGrant + time.Second)
	c.reset(reset)
	if leader, _ := c.leaderFor(reset); leader != "" {
		t.Fatalf("leader %q survived the reset", leader)
	}
	if _, yes := c.vote("b", 2, reset.Add(time.Second)); yes {
		t.Fatal("voted inside the quarantine after a reset")
	}
	if _, yes := c.vote("b", 2, reset.Add(coordinatorGrant)); !yes {
		t.Fatal("never voted again after the quarantine")
	}
}

// Switching away from Device quorum and back keeps the committed membership; it never bootstraps anew.
func TestModeRoundTripKeepsMembership(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen", "Den"})
	rt := h.n.membership
	fingerprint := rt.info("Office", modeElected, true).Fingerprint
	if err := h.n.SetMode(modeExisting); err != nil {
		t.Fatal(err)
	}
	if got := rt.committedMembers(); len(got) != 3 {
		t.Fatalf("members lost when leaving Device quorum: %v", got)
	}
	if err := h.n.SetMode(modeElected); err != nil {
		t.Fatal(err)
	}
	info := rt.info("Office", modeElected, true)
	if info.Fingerprint != fingerprint || info.Epoch != 1 || len(info.Members) != 3 {
		t.Fatalf("membership changed across a mode round trip: %+v", info)
	}
	if rt.wasEnrolled() == false && rt.rosterStore() == nil {
		t.Fatal("device forgot it was enrolled")
	}
}

// Discovery: two peers claiming one stable ID are both ignored, and a peer answering under another
// display name than it announced is ignored. Neither can become a voter.
func TestDirectoryIgnoresDuplicateIDsAndNameMismatch(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen", "Den"})
	fake := func(id, name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw := new(bytes.Buffer)
			_, _ = raw.ReadFrom(r.Body)
			var req membershipRequest
			_ = json.Unmarshal(raw.Bytes(), &req)
			out, _ := json.Marshal(membershipResponse{Version: membershipVersion, ID: req.ID, From: id, To: req.From,
				Status: statusOK, Info: &membershipInfo{DeviceID: id, Name: name, Mode: modeElected, Enabled: true}})
			w.Header().Set(readinessHeader, membershipMAC(word, "response:status", out))
			_, _ = w.Write(out)
		}))
	}
	one, two, liar := fake("same-id", "P1"), fake("same-id", "P2"), fake("honest-id", "NotWhatIAnnounced")
	defer one.Close()
	defer two.Close()
	defer liar.Close()
	peerFor := func(name string, s *httptest.Server) announce.Peer {
		addr := s.Listener.Addr().(*net.TCPAddr)
		return announce.Peer{Name: name, Address: addr.IP.String(), Port: addr.Port}
	}
	h.n.peers = func() []announce.Peer {
		return []announce.Peer{peerFor("P1", one), peerFor("P2", two), peerFor("Liar", liar)}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir, ambiguous := h.n.membershipDirectory(ctx)
	if len(dir) != 0 {
		t.Fatalf("directory trusted ambiguous or mismatched peers: %v", dir)
	}
	if len(ambiguous) == 0 {
		t.Fatal("duplicate stable IDs were not reported")
	}
}

// A member that is offline after a restart keeps its last announced name, and a rename keeps its ID in
// the roster while restarting the election generation.
func TestAnnouncedNamesSurviveRestartAndRenameKeepsID(t *testing.T) {
	dir := t.TempDir()
	rt := newMembershipRuntime(dir)
	rt.deviceID = "self"
	if err := os.MkdirAll(filepath.Join(dir, "nearest-membership"), 0o700); err != nil {
		t.Fatal(err)
	}
	rt.rememberName("kitchen-id", "Kitchen")
	again := newMembershipRuntime(dir)
	again.deviceID = "self"
	again.loadNames()
	names, ok := again.namesFor([]string{"kitchen-id", "self"}, "Office")
	if !ok || len(names) != 2 || names[0] != "kitchen" || names[1] != "office" {
		t.Fatalf("names after restart = %v ok=%v", names, ok)
	}
	generation := rt.generation
	rt.rememberName("kitchen-id", "Galley")
	if rt.generation == generation {
		t.Fatal("a rename did not restart the election generation")
	}
	if got, _ := rt.namesFor([]string{"kitchen-id"}, "Office"); got[0] != "galley" {
		t.Fatalf("rename not applied: %v", got)
	}
}
