package nearest

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
	"github.com/HuskerMinion/techo5/echod/internal/feature/web"
)

const readinessHeader = "X-Techo5-Nearest-Ready"

// acceptEarly is the last check before a coordinated event is settled early: every peer must have
// reported the same claims this device has seen for it. A claim seen here that a peer did not report
// means the views differ, and the event is not settled early.
func (n *Nearest) acceptEarly(mine *pending, best claim, reports []readiness) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.pendingSettingsCurrentLocked(mine) || mine.decided || len(reports) == 0 {
		return false
	}
	var local []claim
	for _, a := range n.others {
		if withinEvent(mine.claim, a.claim) {
			local = appendUniqueClaim(local, a.claim)
		}
		if classicClaimCompetes(mine.claim, mine.at, a) {
			return false
		}
	}
	for _, p := range n.claims {
		if withinEvent(mine.claim, p.claim) {
			local = appendUniqueClaim(local, p.claim)
		}
	}
	if hasConflictingTurns(mine.claim, local) {
		return false
	}
	local = compactEventClaims(mine.claim, local)
	if !sameCandidates(mine.claim, reports[0].Candidates, local) {
		return false
	}
	winner := best
	mine.earlyWinner = &winner
	mine.readinessComplete = true
	return true
}

func (n *Nearest) receiveReadiness(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		web.Refuse(w, "post a nearest coordinator claim", http.StatusMethodNotAllowed)
		return
	}
	if web.AwayRequest(r) {
		http.NotFound(w, r)
		return
	}
	if !n.coordinationOpen() {
		http.NotFound(w, r)
		return
	}
	now, clockReady := n.clockNow(time.Now())
	if !clockReady {
		web.Refuse(w, "election clock not ready", http.StatusServiceUnavailable)
		return
	}
	header := r.Header.Get(authHeader)
	if err := looksSigned(header, now); err != nil {
		web.Refuse(w, "not this house", http.StatusForbidden)
		return
	}
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(bodyWithin))
	body, err := readBody(r)
	if err != nil {
		web.Refuse(w, "not a claim", http.StatusBadRequest)
		return
	}
	c := config.Get()
	now, clockReady = n.clockNow(time.Now())
	if !clockReady || verify(c.Home.HouseWord, c.Device.Name, header, body, now) != nil {
		web.Refuse(w, "not this house", http.StatusForbidden)
		return
	}
	var incoming claim
	if err := json.Unmarshal(body, &incoming); err != nil || incoming.From == "" || incoming.Protocol != coordinationProtocolVersion ||
		incoming.HeardAt == 0 || !finiteClaim(incoming) || len(incoming.Members) == 0 || len(incoming.Members) > maxPeers+1 {
		web.Refuse(w, "not a nearest coordinator claim", http.StatusBadRequest)
		return
	}
	if !slicesAreSortedUnique(incoming.Members) || !containsString(incoming.Members, strings.ToLower(incoming.From)) {
		web.Refuse(w, "invalid election membership", http.StatusBadRequest)
		return
	}
	eventAt := time.Unix(0, incoming.HeardAt)
	if absDuration(now.Sub(eventAt)) > eventTolerance+client.Timeout {
		web.Refuse(w, "coordinator claim outside its event bound", http.StatusBadRequest)
		return
	}
	requestReceivedAt := now
	requestReceivedLocal := time.Now()
	answered := n.took(incoming)
	localMembers, token, rosterValid := n.electedRoster()
	configuredCount := len(localMembers)
	localMembers = n.clock.active(localMembers, c.Device.Name, time.Now())
	_, rosterComplete := rosterPeers(limitPeers(hearing(n.peers())), localMembers, c.Device.Name)
	membershipOK := rosterValid && rosterComplete && len(localMembers) > configuredCount/2 && sameMembers(localMembers, incoming.Members) &&
		incoming.Cluster == token.cluster && incoming.Epoch == token.epoch && incoming.Fingerprint == token.fingerprint
	n.wait(eventCollectionWindow)
	report := readiness{
		Version: coordinationProtocolVersion, From: c.Device.Name, Members: localMembers,
		Candidates: n.eventCandidates(incoming), Ready: membershipOK && !answered && !n.hasClassicCompeting(incoming, requestReceivedLocal),
		ReceivedAt: requestReceivedAt.UnixNano(),
	}
	// Include the receiving device's own candidate (if it heard this event) in its shared view.
	for _, local := range n.localEventCandidates(incoming) {
		report.Candidates = appendUniqueClaim(report.Candidates, local)
	}
	if hasConflictingTurns(incoming, report.Candidates) {
		report.Ready = false
	}
	report.Candidates = compactEventClaims(incoming, report.Candidates)
	if report.Ready && (!sameMembers(report.Members, incoming.Members) || !n.membership.valid(token)) {
		report.Ready = false
	}
	sentAt, clockReady := n.clockNow(time.Now())
	if !clockReady {
		web.Refuse(w, "election clock expired", http.StatusServiceUnavailable)
		return
	}
	report.SentAt = sentAt.UnixNano()
	responseBody, err := json.Marshal(report)
	if err != nil {
		web.Refuse(w, "could not write readiness", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(readinessHeader, readinessMAC(c.Home.HouseWord, nonceOf(header), responseBody))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(responseBody)
}

func (n *Nearest) hasClassicCompeting(anchor claim, receivedAt time.Time) bool {
	eventAt := time.Unix(0, anchor.HeardAt)
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, a := range n.others {
		if a.Protocol != coordinationProtocolVersion && a.Word == anchor.Word {
			corrected, ok := n.clockNow(a.at)
			if !ok || absDuration(corrected.Sub(eventAt)) <= eventTolerance || !a.at.Before(receivedAt) {
				return true
			}
		}
	}
	return false
}

func (n *Nearest) eventCandidates(anchor claim) []claim {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []claim
	for _, a := range n.others {
		if withinEvent(anchor, a.claim) {
			out = appendUniqueClaim(out, a.claim)
		}
	}
	return out
}

func (n *Nearest) localEventCandidates(anchor claim) []claim {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []claim
	for _, p := range n.claims {
		if withinEvent(anchor, p.claim) && strings.EqualFold(p.From, config.Get().Device.Name) {
			out = appendUniqueClaim(out, p.claim)
		}
	}
	return out
}

// hasConflictingTurns is whether one device sent two turns for one event: they may be two wake words,
// so the event is not settled. It runs before compactEventClaims, which would merge them.
func hasConflictingTurns(anchor claim, in []claim) bool {
	turns := make(map[string]string)
	for _, c := range in {
		if c.From == "" || !withinEvent(anchor, c) {
			continue
		}
		sender := strings.ToLower(c.From)
		if turn, found := turns[sender]; found && turn != c.Turn {
			return true
		}
		turns[sender] = c.Turn
	}
	return false
}

func compactEventClaims(anchor claim, in []claim) []claim {
	out := make([]claim, 0, len(in))
	for _, c := range in {
		if c.From == "" || !withinEvent(anchor, c) {
			continue
		}
		found := -1
		for i := range out {
			if strings.EqualFold(out[i].From, c.From) {
				found = i
				break
			}
		}
		if found < 0 {
			out = append(out, c)
		} else if better(c, out[found]) {
			out[found] = c
		}
	}
	return out
}

func appendUniqueClaim(in []claim, c claim) []claim {
	if c.From == "" || containsClaim(in, c) {
		return in
	}
	return append(in, c)
}

func limitPeers(peers []announce.Peer) []announce.Peer {
	if len(peers) > maxPeers {
		return peers[:maxPeers]
	}
	return peers
}

func slicesAreSortedUnique(in []string) bool {
	for i, s := range in {
		if s == "" || (i > 0 && in[i-1] >= s) {
			return false
		}
	}
	return true
}

func containsString(in []string, want string) bool {
	for _, s := range in {
		if s == want {
			return true
		}
	}
	return false
}

func readinessMAC(word, nonce string, body []byte) string {
	sum := sha256.Sum256(body)
	m := hmac.New(sha256.New, key(word))
	m.Write([]byte("nearest-v7-ready\n" + nonce + "\n" + hex.EncodeToString(sum[:])))
	return hex.EncodeToString(m.Sum(nil))
}

func postReadinessWithClock(p announce.Peer, c claim, clock func(time.Time) (time.Time, bool)) (readiness, bool) {
	body, err := json.Marshal(c)
	if err != nil {
		return readiness{}, false
	}
	word := config.Get().Home.HouseWord
	ctx, cancel := context.WithTimeout(context.Background(), readinessDeadline)
	defer cancel()
	url := "http://" + net.JoinHostPort(p.Address, fmt.Sprint(p.Port)) + v3Path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return readiness{}, false
	}
	localSent, ok := clock(time.Now())
	if !ok {
		return readiness{}, false
	}
	header := sign(word, p.Name, body, localSent)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(authHeader, header)
	resp, err := client.Do(req)
	localReceived, clockReady := clock(time.Now())
	if !clockReady {
		if resp != nil {
			resp.Body.Close()
		}
		return readiness{}, false
	}
	if err != nil {
		slog.Info("nearest readiness request failed", "peer", p.Name, "reason", "transport", "elapsed_ms", localReceived.Sub(localSent).Milliseconds(), "error", err)
		return readiness{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slog.Info("nearest readiness request failed", "peer", p.Name, "reason", "http", "status", resp.StatusCode, "elapsed_ms", localReceived.Sub(localSent).Milliseconds())
		return readiness{}, false
	}
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil || len(responseBody) > maxBody ||
		!hmac.Equal([]byte(resp.Header.Get(readinessHeader)), []byte(readinessMAC(word, nonceOf(header), responseBody))) {
		slog.Info("nearest readiness request failed", "peer", p.Name, "reason", "invalid response or signature")
		return readiness{}, false
	}
	var r readiness
	if err := json.Unmarshal(responseBody, &r); err != nil || r.Version != coordinationProtocolVersion || !strings.EqualFold(r.From, p.Name) {
		slog.Info("nearest readiness request failed", "peer", p.Name, "reason", "invalid report")
		return readiness{}, false
	}
	r.localSent, r.localReceived = localSent, localReceived
	return r, true
}
