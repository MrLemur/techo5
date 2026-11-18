package nearest

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
	"github.com/HuskerMinion/techo5/echod/internal/feature/nearest/membership"
	"github.com/HuskerMinion/techo5/echod/internal/feature/web"
)

// Membership messages are JSON over HTTP on the local network, signed with the house word like the other
// Nearest messages. Nothing is encrypted, and a device that has the house word can claim to be another.
const (
	membershipPrefix  = "/nearest/membership/v1/"
	membershipVersion = 1
	maxMembershipBody = 64 << 10
	maxHistoryPage    = 8
)

// membershipDrain is how long a fenced device waits before acknowledging: the longest vote grant a
// device of the old generation may hold, plus the longest a request can still be in flight. A variable
// so the tests can shorten it.
var membershipDrain = coordinatorGrant + bodyWithin + time.Duration(config.MaxNearestWindowMs)*time.Millisecond + readinessDeadline + 100*time.Millisecond

// membershipInfo is what a device says about itself before and after enrolment. It never contains the
// house word, HA tokens or other configuration.
type membershipInfo struct {
	DeviceID    string   `json:"device_id"`
	Name        string   `json:"name"`
	Mode        string   `json:"mode"`
	Enabled     bool     `json:"enabled"`
	Lifecycle   string   `json:"lifecycle"`
	Blocked     string   `json:"blocked,omitempty"`
	Cluster     string   `json:"cluster,omitempty"`
	Epoch       uint64   `json:"epoch,omitempty"`
	Fingerprint string   `json:"fingerprint,omitempty"`
	Authority   string   `json:"authority,omitempty"`
	Members     []string `json:"members,omitempty"`
	Pending     string   `json:"pending,omitempty"`
	Retired     bool     `json:"retired,omitempty"`
	// RetiredClusters are clusters this device belonged to and left by retirement. It stays evidence that
	// the cluster was retired here after the device has re-enrolled elsewhere.
	RetiredClusters []string `json:"retired_clusters,omitempty"`
}

type membershipRequest struct {
	Version  int                        `json:"version"`
	ID       string                     `json:"id"`
	From     string                     `json:"from"`
	To       string                     `json:"to,omitempty"`
	Name     string                     `json:"name,omitempty"`
	Change   *membership.RosterChange   `json:"change,omitempty"`
	Decision *membership.RosterDecision `json:"decision,omitempty"`
	After    uint64                     `json:"after,omitempty"`
}

type membershipResponse struct {
	Version int                         `json:"version"`
	ID      string                      `json:"id"`
	From    string                      `json:"from"`
	To      string                      `json:"to"`
	Status  string                      `json:"status"`
	Info    *membershipInfo             `json:"info,omitempty"`
	Ack     *membership.RosterAck       `json:"ack,omitempty"`
	History []membership.RosterDecision `json:"history,omitempty"`
	More    bool                        `json:"more,omitempty"`
}

// Refusal statuses, so the caller can tell why.
const (
	statusOK       = "ok"
	statusDraining = "draining"
	statusBehind   = "behind"
	statusConflict = "conflict"
	statusStorage  = "storage_error"
	statusRefused  = "refused"
	statusRedirect = "redirect"
	statusNoMajor  = "no_majority"
)

func newRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func membershipMAC(word, label string, body []byte) string {
	return clockMAC(word, "membership-"+label, body)
}

var membershipClient = &http.Client{
	Timeout:       3 * time.Second,
	Transport:     client.Transport,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// memCall sends one request to the peer contacted and validates the response
// against that connection: request ID, sender and recipient must all match.
// wantID may be empty for the first status exchange, when the peer's stable ID
// is not yet known.
func (n *Nearest) memCall(ctx context.Context, peer announce.Peer, wantID, endpoint string, request membershipRequest) (membershipResponse, error) {
	var out membershipResponse
	word := config.Get().Home.HouseWord
	if word == "" || n.membership.deviceIDValue() == "" {
		return out, errors.New("nearest membership: not ready to talk")
	}
	request.Version, request.ID, request.From, request.To = membershipVersion, newRequestID(), n.membership.deviceIDValue(), wantID
	body, err := json.Marshal(request)
	if err != nil {
		return out, err
	}
	url := "http://" + net.JoinHostPort(peer.Address, fmt.Sprint(peer.Port)) + membershipPrefix + endpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return out, err
	}
	req.Header.Set(readinessHeader, membershipMAC(word, "request:"+endpoint, body))
	resp, err := membershipClient.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxMembershipBody+1))
	if err != nil || len(raw) > maxMembershipBody || resp.StatusCode != http.StatusOK ||
		!hmac.Equal([]byte(resp.Header.Get(readinessHeader)), []byte(membershipMAC(word, "response:"+endpoint, raw))) {
		return out, fmt.Errorf("nearest membership: %s unavailable (http %d)", endpoint, resp.StatusCode)
	}
	if json.Unmarshal(raw, &out) != nil || out.Version != membershipVersion || out.ID != request.ID ||
		out.To != request.From || out.From == "" || (wantID != "" && out.From != wantID) {
		return membershipResponse{}, errors.New("nearest membership: response does not match request")
	}
	return out, nil
}

// membershipHandler wraps one endpoint: POST only, house-local, authenticated, bounded.
func (n *Nearest) membershipHandler(endpoint string, handle func(membershipRequest) (membershipResponse, bool)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || web.AwayRequest(r) || !n.open() {
			http.NotFound(w, r)
			return
		}
		_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(bodyWithin))
		raw, err := io.ReadAll(io.LimitReader(r.Body, maxMembershipBody+1))
		cfg := config.Get()
		if err != nil || len(raw) > maxMembershipBody ||
			!hmac.Equal([]byte(r.Header.Get(readinessHeader)), []byte(membershipMAC(cfg.Home.HouseWord, "request:"+endpoint, raw))) {
			http.Error(w, "invalid request", http.StatusForbidden)
			return
		}
		var request membershipRequest
		if json.Unmarshal(raw, &request) != nil || request.Version != membershipVersion || len(request.ID) != 32 || request.From == "" {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		me := n.membership.deviceIDValue()
		if request.To != "" && request.To != me {
			http.Error(w, "wrong recipient", http.StatusConflict)
			return
		}
		response, _ := handle(request)
		response.Version, response.ID, response.From, response.To = membershipVersion, request.ID, me, request.From
		out, err := json.Marshal(response)
		if err != nil {
			http.Error(w, "invalid response", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(readinessHeader, membershipMAC(cfg.Home.HouseWord, "response:"+endpoint, out))
		_, _ = w.Write(out)
	}
}

// membershipControlOpen is whether membership messages are answered: whenever Device quorum is selected,
// even while wakes are dropped, since a device joining has to get through before it can answer wakes.
func (n *Nearest) membershipControlOpen() bool {
	return n.open() && config.Get().Nearest.EffectiveMode() == modeElected
}

// membershipRoutes are the membership endpoints, served by the web server and by the tests.
func (n *Nearest) membershipRoutes() map[string]http.HandlerFunc {
	routes := map[string]func(membershipRequest) (membershipResponse, bool){
		"status": func(membershipRequest) (membershipResponse, bool) {
			info := n.membershipInfo()
			return membershipResponse{Status: statusOK, Info: &info}, true
		},
		"join":    n.handleJoin,
		"prepare": n.handlePrepare,
		"fence":   n.handleFence,
		"commit":  n.handleCommit,
		"history": n.handleHistory,
		"retire":  n.handleRetire,
	}
	out := make(map[string]http.HandlerFunc, len(routes))
	for endpoint, fn := range routes {
		out[membershipPrefix+endpoint] = n.membershipHandler(endpoint, fn)
	}
	return out
}

func (n *Nearest) registerMembership() {
	for path, handler := range n.membershipRoutes() {
		web.Handle(path, "", n.open, handler)
	}
}

func (n *Nearest) membershipInfo() membershipInfo {
	cfg := config.Get()
	return n.membership.info(cfg.Device.Name, cfg.Nearest.EffectiveMode(), cfg.Nearest.Enabled())
}

func statusFor(err error) string {
	switch {
	case err == nil:
		return statusOK
	case errors.Is(err, membership.ErrDraining):
		return statusDraining
	case errors.Is(err, membership.ErrBehind):
		return statusBehind
	case errors.Is(err, membership.ErrConflict), errors.Is(err, membership.ErrCorrupt):
		return statusConflict
	default:
		return statusStorage
	}
}

func (n *Nearest) handleJoin(req membershipRequest) (membershipResponse, bool) {
	info := n.membershipInfo()
	if !n.membershipControlOpen() {
		return membershipResponse{Status: statusRefused, Info: &info}, false
	}
	if info.Cluster == "" || info.Authority != info.DeviceID || info.Lifecycle == "retired" {
		return membershipResponse{Status: statusRedirect, Info: &info}, false
	}
	n.membership.queueJoin(req.From, req.Name)
	return membershipResponse{Status: statusOK, Info: &info}, true
}

func (n *Nearest) handlePrepare(req membershipRequest) (membershipResponse, bool) {
	if !n.membershipControlOpen() || req.Change == nil {
		return membershipResponse{Status: statusRefused}, false
	}
	err := n.membership.prepare(*req.Change)
	return membershipResponse{Status: statusFor(err)}, err == nil
}

func (n *Nearest) handleFence(req membershipRequest) (membershipResponse, bool) {
	if !n.membershipControlOpen() || req.Change == nil {
		return membershipResponse{Status: statusRefused}, false
	}
	ack, err := n.membership.fence(*req.Change)
	if err != nil {
		return membershipResponse{Status: statusFor(err)}, false
	}
	return membershipResponse{Status: statusOK, Ack: &ack}, true
}

func (n *Nearest) handleCommit(req membershipRequest) (membershipResponse, bool) {
	if !n.membershipControlOpen() || req.Decision == nil || req.Decision.Change.Kind == "retire" {
		return membershipResponse{Status: statusRefused}, false
	}
	err := n.membership.install(*req.Decision)
	return membershipResponse{Status: statusFor(err)}, err == nil
}

func (n *Nearest) handleHistory(req membershipRequest) (membershipResponse, bool) {
	history, more, err := n.membership.historyAfter(req.After)
	if err != nil {
		return membershipResponse{Status: statusFor(err)}, false
	}
	return membershipResponse{Status: statusOK, History: history, More: more}, true
}

// remoteParticipant is one real contacted device. Every response is validated
// against the peer and ID it was sent to, in memCall.
type remoteParticipant struct {
	n    *Nearest
	peer announce.Peer
	id   string
}

// pollStatus repeats a request while the peer reports draining, up to the context's deadline.
func (p remoteParticipant) poll(ctx context.Context, endpoint string, request membershipRequest) (membershipResponse, error) {
	for {
		resp, err := p.n.memCall(ctx, p.peer, p.id, endpoint, request)
		if err != nil {
			return resp, err
		}
		if resp.Status != statusDraining {
			if resp.Status != statusOK {
				return resp, fmt.Errorf("nearest membership: %s refused: %s", p.id, resp.Status)
			}
			return resp, nil
		}
		select {
		case <-ctx.Done():
			return resp, membership.ErrDraining
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (p remoteParticipant) PrepareJoin(ctx context.Context, change membership.RosterChange) error {
	_, err := p.poll(ctx, "prepare", membershipRequest{Change: &change})
	return err
}

func (p remoteParticipant) FenceAndDrain(ctx context.Context, change membership.RosterChange) (membership.RosterAck, error) {
	resp, err := p.poll(ctx, "fence", membershipRequest{Change: &change})
	if err != nil {
		return membership.RosterAck{}, err
	}
	if resp.Ack == nil || resp.Ack.Voter != p.id {
		return membership.RosterAck{}, errors.New("nearest membership: acknowledgement not from the device contacted")
	}
	return *resp.Ack, nil
}

func (p remoteParticipant) CommitRoster(ctx context.Context, decision membership.RosterDecision) error {
	endpoint := "commit"
	if decision.Change.Kind == "retire" {
		endpoint = "retire"
	}
	_, err := p.poll(ctx, endpoint, membershipRequest{Decision: &decision})
	return err
}

// localParticipant is this device taking part in its own round.
type localParticipant struct{ n *Nearest }

func (p localParticipant) PrepareJoin(context.Context, membership.RosterChange) error {
	return errors.New("nearest membership: a device does not join itself")
}

func (p localParticipant) FenceAndDrain(ctx context.Context, change membership.RosterChange) (membership.RosterAck, error) {
	for {
		ack, err := p.n.membership.fence(change)
		if !errors.Is(err, membership.ErrDraining) {
			return ack, err
		}
		select {
		case <-ctx.Done():
			return ack, membership.ErrDraining
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (p localParticipant) CommitRoster(ctx context.Context, decision membership.RosterDecision) error {
	for {
		err := p.n.membership.install(decision)
		if !errors.Is(err, membership.ErrDraining) {
			return err
		}
		select {
		case <-ctx.Done():
			return membership.ErrDraining
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func lowerTrim(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
