package nearest

import (
	"bytes"
	"context"
	"crypto/hmac"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
	"github.com/HuskerMinion/techo5/echod/internal/feature/web"
)

const (
	coordinatorVotePath  = "/nearest/v7/vote"
	coordinatorWakePath  = "/nearest/v7/wake"
	coordinatorGrant     = 3 * time.Second
	coordinatorLease     = time.Second
	coordinatorTick      = 500 * time.Millisecond
	coordinatorClockJump = 100 * time.Millisecond
)

type coordinatorRuntime struct {
	mu          sync.Mutex
	boot        time.Time
	checkedAt   time.Time
	term        uint64
	voted       string
	grantUntil  time.Time
	leader      string
	leaderTerm  uint64
	leaderUntil time.Time
	events      []*coordinatorEvent
}

// checkClock invalidates leases after a wall/monotonic discontinuity, including
// a system suspend on platforms whose monotonic clock stops while suspended.
// The caller holds c.mu.
func (c *coordinatorRuntime) checkClock(now time.Time) {
	if !c.checkedAt.IsZero() {
		monotonic := now.Sub(c.checkedAt)
		wall := now.Round(0).Sub(c.checkedAt.Round(0))
		if absDuration(monotonic-wall) > coordinatorClockJump {
			c.boot = now
			c.voted, c.leader = "", ""
			c.grantUntil, c.leaderUntil = time.Time{}, time.Time{}
			c.leaderTerm = 0
			slog.Warn("nearest coordinator clock discontinuity; leases invalidated")
		}
	}
	c.checkedAt = now
}

// reset drops every grant, lease and event of a previous membership generation and refuses votes for a
// full grant, exactly as a restart does. The term is kept so it never goes backwards.
func (c *coordinatorRuntime) reset(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.boot, c.checkedAt = now, now
	c.voted, c.leader, c.leaderTerm = "", "", 0
	c.grantUntil, c.leaderUntil = time.Time{}, time.Time{}
	for _, e := range c.events {
		e.closed, e.reset = true, true
	}
}

func timeNow() time.Time { return time.Now() }

type coordinatorEvent struct {
	token      membershipToken
	word       string
	heardAt    int64
	term       uint64
	members    []string
	deadline   time.Time
	closed     bool
	reset      bool // discarded by a membership generation change
	candidates []claim
	ambiguous  bool
	done       chan struct{}
	winner     claim
	valid      bool
}

type coordinatorRequest struct {
	From      string   `json:"from"`
	Term      uint64   `json:"term"`
	Members   []string `json:"members"`
	Candidate claim    `json:"candidate,omitempty"`
	// Cluster, Epoch and Fingerprint bind an elected message to one committed membership, and every
	// message must carry them.
	Cluster     string `json:"cluster,omitempty"`
	Epoch       uint64 `json:"epoch,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
}

type coordinatorResponse struct {
	From    string `json:"from"`
	Term    uint64 `json:"term"`
	Granted bool   `json:"granted"`
	Winner  claim  `json:"winner,omitempty"`

	Cluster     string `json:"cluster,omitempty"`
	Epoch       uint64 `json:"epoch,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
}

func (t membershipToken) matches(cluster string, epoch uint64, fingerprint string) bool {
	return cluster == t.cluster && epoch == t.epoch && fingerprint == t.fingerprint && cluster != ""
}

// coordinatorActive snapshots the active roster and only accepts a strict
// majority of the configured roster. It never changes the configured quorum.
func (n *Nearest) coordinatorActive() ([]string, []announce.Peer, bool) {
	active, peers, _, ok := n.coordinatorActiveToken()
	return active, peers, ok
}

func (n *Nearest) coordinatorActiveToken() ([]string, []announce.Peer, membershipToken, bool) {
	cfg := config.Get()
	roster, token, ok := n.electedRoster()
	if !ok {
		return nil, nil, token, false
	}
	active := n.clock.active(roster, cfg.Device.Name, time.Now())
	peers, complete := rosterPeers(limitPeers(hearing(n.peers())), active, cfg.Device.Name)
	return active, peers, token, complete && len(active) > len(roster)/2
}

// vote refuses a different candidate until the previous grant has expired.
// A process returning from a restart waits out the longest possible old grant.
func (c *coordinatorRuntime) vote(candidate string, term uint64, now time.Time) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkClock(now)
	if c.boot.IsZero() {
		c.boot = now
	}
	if now.Sub(c.boot) < coordinatorGrant || candidate == "" || term == 0 {
		return c.term, false
	}
	if now.Before(c.grantUntil) && c.voted != candidate {
		return c.term, false
	}
	if term < c.term || (term == c.term && c.voted != "" && c.voted != candidate) {
		return c.term, false
	}
	if c.voted != candidate || c.term != term {
		slog.Info("nearest coordinator vote", "candidate", candidate, "term", term)
	}
	c.term, c.voted, c.grantUntil = term, candidate, now.Add(coordinatorGrant)
	c.leader, c.leaderTerm = candidate, term
	return c.term, true
}

func (c *coordinatorRuntime) leaderFor(now time.Time) (string, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkClock(now)
	if now.After(c.grantUntil) {
		return "", 0
	}
	return c.leader, c.leaderTerm
}

func (c *coordinatorRuntime) authorised(me string, term uint64, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkClock(now)
	return c.leader == me && c.leaderTerm == term && now.Before(c.leaderUntil)
}

func (n *Nearest) coordinatorLoop() {
	var hadQuorum bool
	for {
		if n.coordinationOpen() {
			n.coordinatorRound()
			me := strings.ToLower(config.Get().Device.Name)
			leader, term := n.coordinator.leaderFor(time.Now())
			hasQuorum := leader == me && n.coordinator.authorised(me, term, time.Now())
			if hasQuorum != hadQuorum {
				slog.Info("nearest coordinator quorum", "leader", me, "term", term, "ready", hasQuorum)
				hadQuorum = hasQuorum
			}
		} else {
			hadQuorum = false
		}
		time.Sleep(coordinatorTick)
	}
}

func (n *Nearest) coordinatorRound() {
	cfg := config.Get()
	active, peers, token, ok := n.coordinatorActiveToken()
	if !ok || len(active) == 0 || active[0] != strings.ToLower(cfg.Device.Name) {
		return
	}
	if _, ok := n.clockNow(time.Now()); !ok {
		return
	}
	c := &n.coordinator
	now := time.Now()
	c.mu.Lock()
	term := c.term
	if c.voted != active[0] || !now.Before(c.grantUntil) {
		term++
	}
	c.mu.Unlock()
	_, self := c.vote(active[0], term, now)
	if !self {
		return
	}
	type result struct {
		response coordinatorResponse
		ok       bool
	}
	results := make(chan result, len(peers))
	for _, peer := range peers {
		go func() {
			r, ok := n.coordinatorPost(peer, coordinatorVotePath, coordinatorRequest{
				From: cfg.Device.Name, Term: term, Members: active,
				Cluster: token.cluster, Epoch: token.epoch, Fingerprint: token.fingerprint,
			})
			results <- result{r, ok && token.matches(r.Cluster, r.Epoch, r.Fingerprint)}
		}()
	}
	roster, _, _ := n.electedRoster()
	votes := []string{strings.ToLower(cfg.Device.Name)}
	sameTermRefused := false
	for range peers {
		result := <-results
		if result.ok && result.response.Term > term {
			c.mu.Lock()
			if result.response.Term > c.term {
				c.term = result.response.Term
				c.voted = ""
			}
			c.mu.Unlock()
		}
		if result.ok && result.response.Granted && result.response.Term == term {
			votes = append(votes, strings.ToLower(result.response.From))
		} else if result.ok && !result.response.Granted && result.response.Term == term {
			sameTermRefused = true
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.term == term && c.voted == active[0] && coordinatorQuorum(roster, votes) &&
		time.Since(now) < coordinatorLease && n.membership.valid(token) {
		c.leader, c.leaderTerm, c.leaderUntil = active[0], term, now.Add(coordinatorLease)
	} else if c.term == term && c.voted == active[0] && sameTermRefused && term < ^uint64(0) {
		// A paused leader can return with the same term that its replacement
		// used. Voters must not grant that term to a different candidate even
		// after their old leases expire, so retry in a newer term. Invalidate
		// our old coordinator lease before doing so.
		c.term++
		c.leader, c.leaderTerm, c.leaderUntil = "", 0, time.Time{}
	}
}

func (n *Nearest) coordinatorReceive(w http.ResponseWriter, r *http.Request, wake bool) {
	if r.Method != http.MethodPost || web.AwayRequest(r) || !n.coordinationOpen() {
		http.NotFound(w, r)
		return
	}
	now, ready := n.clockNow(time.Now())
	if !ready {
		http.Error(w, "clock unavailable", http.StatusServiceUnavailable)
		return
	}
	header := r.Header.Get(authHeader)
	if looksSigned(header, now) != nil {
		http.Error(w, "invalid signature", http.StatusForbidden)
		return
	}
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(bodyWithin))
	body, err := readBody(r)
	cfg := config.Get()
	if err != nil || verify(cfg.Home.HouseWord, cfg.Device.Name, header, body, now) != nil {
		http.Error(w, "invalid request", http.StatusForbidden)
		return
	}
	var req coordinatorRequest
	active, _, token, ok := n.coordinatorActiveToken()
	if json.Unmarshal(body, &req) != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	switch {
	case !ok:
		http.Error(w, "membership mismatch: not ready", http.StatusConflict)
		return
	case !token.matches(req.Cluster, req.Epoch, req.Fingerprint):
		http.Error(w, "membership mismatch: other cluster or epoch", http.StatusConflict)
		return
	case !sameMembers(req.Members, active) || !slices.Contains(active, strings.ToLower(req.From)):
		http.Error(w, "membership mismatch: active set differs", http.StatusConflict)
		return
	}
	resp := coordinatorResponse{From: cfg.Device.Name, Term: req.Term,
		Cluster: token.cluster, Epoch: token.epoch, Fingerprint: token.fingerprint}
	if !wake {
		candidate := strings.ToLower(req.From)
		if candidate != active[0] {
			http.Error(w, "invalid coordinator candidate", http.StatusConflict)
			return
		}
		resp.Term, resp.Granted = n.coordinator.vote(candidate, req.Term, time.Now())
	} else {
		if !strings.EqualFold(req.Candidate.From, req.From) || req.Candidate.Protocol != coordinationProtocolVersion ||
			!sameMembers(req.Candidate.Members, active) || req.Candidate.Turn == "" ||
			!token.matches(req.Candidate.Cluster, req.Candidate.Epoch, req.Candidate.Fingerprint) ||
			!finiteClaim(req.Candidate) || req.Candidate.HeardAt == 0 ||
			absDuration(now.Sub(time.Unix(0, req.Candidate.HeardAt))) > eventTolerance+client.Timeout {
			http.Error(w, "invalid claim", http.StatusBadRequest)
			return
		}
		winner, ok := n.coordinatorCollect(req.Candidate, req.Term, token)
		resp.Granted, resp.Winner = ok && n.membership.valid(token), winner
	}
	out, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "invalid response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(readinessHeader, readinessMAC(cfg.Home.HouseWord, nonceOf(header), out))
	_, _ = w.Write(out)
}

func (n *Nearest) coordinatorCollect(incoming claim, term uint64, token membershipToken) (claim, bool) {
	me := strings.ToLower(config.Get().Device.Name)
	c := &n.coordinator
	if !c.authorised(me, term, time.Now()) || !n.membership.valid(token) {
		return claim{}, false
	}
	c.mu.Lock()
	var event *coordinatorEvent
	for _, e := range c.events {
		if e.token == token && e.term == term && e.word == incoming.Word &&
			absDuration(time.Duration(e.heardAt-incoming.HeardAt)) <= eventTolerance {
			event = e
			break
		}
	}
	if event == nil {
		now := time.Now()
		for _, e := range c.events {
			if e.token == token && e.term == term && e.word == incoming.Word && !e.closed && now.Before(e.deadline) &&
				absDuration(time.Duration(e.heardAt-incoming.HeardAt)) <= concurrentEventTolerance {
				event = e
				break
			}
		}
	}
	if event == nil {
		event = &coordinatorEvent{token: token, word: incoming.Word, heardAt: incoming.HeardAt, term: term,
			members: slices.Clone(incoming.Members), deadline: time.Now().Add(eventCollectionWindow), done: make(chan struct{})}
		c.events = append(c.events, event)
		go n.coordinatorFinish(event)
	}
	select {
	case <-event.done:
		c.mu.Unlock()
		return claim{}, false
	default:
		if event.closed || !time.Now().Before(event.deadline) {
			event.closed = true
			n.coordinator.mu.Unlock()
			return claim{}, false
		}
		for _, previous := range event.candidates {
			if strings.EqualFold(previous.From, incoming.From) && previous.Turn != incoming.Turn {
				event.ambiguous = true
			}
		}
		event.candidates = appendUniqueClaim(event.candidates, incoming)
	}
	c.mu.Unlock()
	select {
	case <-event.done:
		return event.winner, event.valid
	case <-time.After(readinessDeadline):
		return claim{}, false
	}
}

func (n *Nearest) coordinatorFinish(event *coordinatorEvent) {
	time.Sleep(eventCollectionWindow)
	c := &n.coordinator
	active, _, token, complete := n.coordinatorActiveToken()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkClock(time.Now())
	me := strings.ToLower(config.Get().Device.Name)
	if complete && token == event.token && !event.reset && sameMembers(active, event.members) && c.leader == me && c.leaderTerm == event.term &&
		time.Now().Before(c.leaderUntil) && len(event.candidates) > 0 && !event.ambiguous {
		best := event.candidates[0]
		for _, candidate := range event.candidates[1:] {
			if better(candidate, best) {
				best = candidate
			}
		}
		event.winner, event.valid = best, true
	}
	close(event.done)
	time.AfterFunc(kept, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.events = slices.DeleteFunc(c.events, func(e *coordinatorEvent) bool { return e == event })
	})
}

func (n *Nearest) coordinatorPost(peer announce.Peer, path string, request coordinatorRequest) (coordinatorResponse, bool) {
	var result coordinatorResponse
	body, err := json.Marshal(request)
	if err != nil {
		return result, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), readinessDeadline)
	defer cancel()
	url := "http://" + net.JoinHostPort(peer.Address, fmt.Sprint(peer.Port)) + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return result, false
	}
	at, ok := n.clockNow(time.Now())
	if !ok {
		return result, false
	}
	word := config.Get().Home.HouseWord
	header := sign(word, peer.Name, body, at)
	req.Header.Set(authHeader, header)
	resp, err := client.Do(req)
	if err != nil {
		slog.Info("nearest coordinator request failed", "peer", peer.Name, "path", path, "error", err)
		return result, false
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil || len(out) > maxBody || resp.StatusCode != http.StatusOK ||
		!hmac.Equal([]byte(resp.Header.Get(readinessHeader)), []byte(readinessMAC(word, nonceOf(header), out))) ||
		json.Unmarshal(out, &result) != nil || !strings.EqualFold(result.From, peer.Name) {
		slog.Info("nearest coordinator response rejected", "peer", peer.Name, "path", path, "status", resp.StatusCode, "reason", strings.TrimSpace(string(out)))
		return coordinatorResponse{}, false
	}
	return result, true
}

func (n *Nearest) coordinatorWake(peers []announce.Peer, mine *pending, can func() bool, open func(time.Time, float64)) {
	me := config.Get().Device.Name
	leader, term := n.coordinator.leaderFor(time.Now())
	if leader == "" {
		slog.Info("nearest coordinator unavailable; not answering")
		return
	}
	var response coordinatorResponse
	var ok bool
	if strings.EqualFold(leader, me) {
		response.Winner, ok = n.coordinatorCollect(mine.claim, term, mine.membership)
		response.Granted, response.Term = ok, term
	} else {
		for _, peer := range peers {
			if strings.EqualFold(peer.Name, leader) {
				response, ok = n.coordinatorPost(peer, coordinatorWakePath, coordinatorRequest{
					From: me, Term: term, Members: mine.Members, Candidate: mine.claim,
					Cluster: mine.membership.cluster, Epoch: mine.membership.epoch, Fingerprint: mine.membership.fingerprint,
				})
				ok = ok && mine.membership.matches(response.Cluster, response.Epoch, response.Fingerprint)
				break
			}
		}
	}
	current, currentTerm := n.coordinator.leaderFor(time.Now())
	if !ok || !response.Granted || response.Term != term || current != leader || currentTerm != term {
		slog.Info("nearest coordinator election incomplete; not answering")
		return
	}
	n.mu.Lock()
	if !n.pendingSettingsCurrentLocked(mine) {
		n.mu.Unlock()
		slog.Info("nearest coordinator result superseded; not answering")
		return
	}
	mine.readinessComplete = true
	winner := response.Winner
	mine.earlyWinner = &winner
	n.mu.Unlock()
	slog.Info("nearest coordinator decided", "coordinator", leader, "term", term, "winner", winner.From,
		"word", mine.Word, "waited_ms", n.now().Sub(mine.at).Milliseconds())
	n.decide(mine.Word, mine, can, open)
}
