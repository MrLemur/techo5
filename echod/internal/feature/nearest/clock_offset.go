package nearest

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
	"github.com/HuskerMinion/techo5/echod/internal/feature/web"
)

const (
	clockPath           = "/nearest/v7/clock"
	clockSampleInterval = 2 * time.Second
	clockRetryInterval  = time.Second
	// clockWarmupInterval is the poll interval until the first usable clock bound. Extra samples only ever
	// tighten the same authenticated bound; they never loosen it.
	clockWarmupInterval = 250 * time.Millisecond
	clockSampleLifetime = 15 * time.Second
	clockOfflineAfter   = 20 * time.Second
	// A sample's uncertainty grows with age at this rate. A device whose clock drifts faster gets
	// samples that are wrong by more than they say.
	clockDriftAllowance = 2 * time.Millisecond
	clockMaxUncertainty = 20 * time.Millisecond
)

type electionClock struct {
	mu      sync.Mutex
	base    time.Time
	samples map[string]clockSample
	members map[string]clockMember
	source  func(time.Time) time.Time
}

type clockMember struct {
	since    time.Time
	lastSeen time.Time
	failure  string
}

type clockSample struct {
	offset      time.Duration
	uncertainty time.Duration
	sampled     time.Time
}

func (c *electionClock) active(roster []string, me string, now time.Time) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.members == nil {
		c.members = make(map[string]clockMember)
	}
	active := make([]string, 0, len(roster))
	for _, name := range roster {
		if strings.EqualFold(name, me) {
			active = append(active, name)
			continue
		}
		state := c.members[name]
		if state.since.IsZero() {
			state.since = now
			c.members[name] = state
		}
		last := state.lastSeen
		if last.IsZero() {
			last = state.since
		}
		age := now.Sub(last)
		if age < 0 || age < clockOfflineAfter {
			active = append(active, name)
		}
	}
	return active
}

// resetSamples forgets everything learned under a previous membership generation.
func (c *electionClock) resetSamples() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples, c.members = nil, nil
}

func (c *electionClock) contact(peer string, now time.Time, failure string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.members == nil {
		c.members = make(map[string]clockMember)
	}
	peer = strings.ToLower(peer)
	state := c.members[peer]
	if state.since.IsZero() {
		state.since = now
	}
	if failure == "" {
		state.lastSeen = now
	}
	state.failure = failure
	c.members[peer] = state
}

func (c *electionClock) diagnostics(roster []string, me string, now time.Time) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, name := range roster {
		if strings.EqualFold(name, me) {
			continue
		}
		state := c.members[name]
		sample, ok := c.samples[name]
		if !ok {
			reason := state.failure
			if reason == "" {
				reason = "not discovered or no response"
			}
			out = append(out, name+": no sample; "+reason)
			continue
		}
		age := now.Sub(sample.sampled)
		uncertainty := sample.uncertainty + time.Duration(int64(age)*int64(clockDriftAllowance)/int64(time.Second))
		if age < 0 || age > clockSampleLifetime || uncertainty > clockMaxUncertainty {
			out = append(out, name+": sample_age="+age.Round(time.Millisecond).String()+
				" uncertainty="+uncertainty.Round(time.Millisecond).String()+"; "+state.failure)
		}
	}
	return out
}

func (c *electionClock) offer(peer string, offset, uncertainty time.Duration, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.samples == nil {
		c.samples = make(map[string]clockSample)
	}
	peer = strings.ToLower(peer)
	old := c.samples[peer]
	age := now.Sub(old.sampled)
	if !old.sampled.IsZero() && age >= 0 && age <= clockSampleLifetime {
		oldUncertainty := old.uncertainty + time.Duration(int64(age)*int64(clockDriftAllowance)/int64(time.Second))
		if oldUncertainty <= uncertainty {
			return
		}
	}
	c.samples[peer] = clockSample{offset: offset, uncertainty: uncertainty, sampled: now}
}

func (c *electionClock) local(now time.Time) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.base.IsZero() {
		c.base = now
	}
	local := c.base.Add(now.Sub(c.base))
	if c.source != nil {
		return c.source(local)
	}
	return local
}

func (c *electionClock) correctedFrom(now time.Time, peer string) (time.Time, bool) {
	local := c.local(now)
	c.mu.Lock()
	defer c.mu.Unlock()
	sample, found := c.samples[strings.ToLower(peer)]
	age := now.Sub(sample.sampled)
	if age < 0 {
		age = -age
	}
	if !found || sample.sampled.IsZero() || age > clockSampleLifetime ||
		sample.uncertainty+time.Duration(int64(age)*int64(clockDriftAllowance)/int64(time.Second)) > clockMaxUncertainty {
		return time.Time{}, false
	}
	return local.Add(sample.offset), true
}

func (c *electionClock) corrected(now time.Time, members []string, me string) (time.Time, bool) {
	local := c.local(now)
	c.mu.Lock()
	defer c.mu.Unlock()
	lower := make([]time.Duration, 0, len(members))
	upper := make([]time.Duration, 0, len(members))
	for _, member := range members {
		if strings.EqualFold(member, me) {
			lower = append(lower, 0)
			upper = append(upper, 0)
			continue
		}
		sample, found := c.samples[strings.ToLower(member)]
		age := now.Sub(sample.sampled)
		if age < 0 {
			age = -age
		}
		if !found || sample.sampled.IsZero() || age > clockSampleLifetime {
			return time.Time{}, false
		}
		uncertainty := sample.uncertainty + time.Duration(int64(age)*int64(clockDriftAllowance)/int64(time.Second))
		lower = append(lower, sample.offset-uncertainty)
		upper = append(upper, sample.offset+uncertainty)
	}
	if len(lower) < 2 {
		return time.Time{}, false
	}
	slices.Sort(lower)
	slices.Sort(upper)
	median := func(values []time.Duration, roundUp bool) time.Duration {
		middle := len(values) / 2
		if len(values)%2 == 0 {
			delta := values[middle] - values[middle-1]
			if roundUp {
				delta++
			}
			return values[middle-1] + delta/2
		}
		return values[middle]
	}
	lo, hi := median(lower, false), median(upper, true)
	if hi < lo || hi-lo > 2*clockMaxUncertainty {
		return time.Time{}, false
	}
	return local.Add(lo + (hi-lo)/2), true
}

func (n *Nearest) electionTime(now time.Time) (time.Time, bool) {
	cfg := config.Get()
	members, _, ok := n.electedRoster()
	if !ok || cfg.Nearest.EffectiveMode() != modeElected {
		return time.Time{}, false
	}
	active := n.clock.active(members, cfg.Device.Name, time.Now())
	if len(active) <= len(members)/2 {
		return time.Time{}, false
	}
	return n.clock.corrected(now, active, cfg.Device.Name)
}

type clockMessage struct {
	Nonce    string `json:"nonce"`
	Received int64  `json:"received,omitempty"`
	Sent     int64  `json:"sent,omitempty"`
}

func clockMAC(word, label string, body []byte) string {
	m := hmac.New(sha256.New, key(word))
	m.Write([]byte("nearest-clock-v7:" + label + "\n"))
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

func (n *Nearest) receiveClock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || web.AwayRequest(r) || !n.clockOpen() {
		http.NotFound(w, r)
		return
	}
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(bodyWithin))
	body, err := readBody(r)
	cfg := config.Get()
	if err != nil || !hmac.Equal([]byte(r.Header.Get(readinessHeader)), []byte(clockMAC(cfg.Home.HouseWord, "request", body))) {
		http.Error(w, "invalid request", http.StatusForbidden)
		return
	}
	var req clockMessage
	_, ok := n.clockRoster()
	if json.Unmarshal(body, &req) != nil || len(req.Nonce) != 32 || !ok {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	received := n.clock.local(time.Now())
	report := clockMessage{Nonce: req.Nonce, Received: received.UnixNano()}
	report.Sent = n.clock.local(time.Now()).UnixNano()
	response, err := json.Marshal(report)
	if err != nil {
		http.Error(w, "invalid response", http.StatusInternalServerError)
		return
	}
	w.Header().Set(readinessHeader, clockMAC(cfg.Home.HouseWord, "response", response))
	_, _ = w.Write(response)
}

func (n *Nearest) sampleClock(p announce.Peer, peer string) {
	n.sampleClockOnce(p, peer)
	// Retry straight away when the best sample will not last until the next poll, so one slow Wi-Fi
	// exchange does not leave every device waiting a second for the clock.
	if _, ok := n.clock.correctedFrom(time.Now().Add(clockSampleInterval), peer); !ok {
		time.Sleep(30 * time.Millisecond)
		n.sampleClockOnce(p, peer)
	}
}

func (n *Nearest) sampleClockOnce(p announce.Peer, peer string) {
	failure := "request failed"
	defer func() {
		if failure != "" {
			n.clock.contact(peer, time.Now(), failure)
		}
	}()
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return
	}
	body, err := json.Marshal(clockMessage{Nonce: hex.EncodeToString(nonce[:])})
	if err != nil {
		failure = "encoding: " + err.Error()
		return
	}
	cfg := config.Get()
	url := "http://" + net.JoinHostPort(p.Address, strconv.Itoa(p.Port)) + clockPath
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set(readinessHeader, clockMAC(cfg.Home.HouseWord, "request", body))
	sent := n.clock.local(time.Now())
	resp, err := client.Do(req)
	received := n.clock.local(time.Now())
	if err != nil {
		failure = "transport: " + err.Error()
		return
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil || len(response) > maxBody || resp.StatusCode != http.StatusOK ||
		!hmac.Equal([]byte(resp.Header.Get(readinessHeader)), []byte(clockMAC(cfg.Home.HouseWord, "response", response))) {
		failure = "invalid response or authentication; http=" + strconv.Itoa(resp.StatusCode)
		return
	}
	var report clockMessage
	if json.Unmarshal(response, &report) != nil || report.Nonce != hex.EncodeToString(nonce[:]) {
		failure = "invalid response nonce"
		return
	}
	n.clock.contact(peer, time.Now(), "")
	failure = ""
	bounds, ok := measureClockOffset(sent, received, report.Received, report.Sent)
	if !ok {
		n.clock.contact(peer, time.Now(), "clock uncertainty too high; rtt="+received.Sub(sent).Round(time.Millisecond).String())
		return
	}
	// Allow for drift during the exchange as well as since. A wide sample still helps when the median
	// across the group is narrow.
	drift := time.Duration(int64(received.Sub(sent)) * int64(clockDriftAllowance) / int64(time.Second))
	bounds.lower -= drift
	bounds.upper += drift
	n.clock.offer(peer, bounds.lower+(bounds.upper-bounds.lower)/2,
		(bounds.upper-bounds.lower+1)/2, time.Now())
}

func (n *Nearest) clockLoop() {
	var wasReady, checked bool
	var previousMembers string
	var lastDiagnostic time.Time
	for {
		delay := clockSampleInterval
		cfg := config.Get()
		if n.clockOpen() {
			members, ok := n.clockRoster()
			if ok {
				var wg sync.WaitGroup
				for _, peer := range limitPeers(hearing(n.peers())) {
					if !containsString(members, strings.ToLower(strings.TrimSpace(peer.Name))) {
						continue
					}
					wg.Add(1)
					go func() {
						defer wg.Done()
						n.sampleClock(peer, peer.Name)
					}()
				}
				wg.Wait()
				now := time.Now()
				active := n.clock.active(members, cfg.Device.Name, now)
				if joined := strings.Join(active, ","); joined != previousMembers {
					slog.Info("nearest coordinator membership", "members", active)
					previousMembers = joined
				}
				_, ready := n.clock.corrected(now, active, cfg.Device.Name)
				if !checked || ready != wasReady {
					slog.Info("nearest coordinator clock", "members", len(active), "ready", ready)
					checked, wasReady = true, ready
				}
				if !ready && (lastDiagnostic.IsZero() || now.Sub(lastDiagnostic) >= 30*time.Second) {
					slog.Info("nearest coordinator clock unavailable", "peers", n.clock.diagnostics(active, cfg.Device.Name, now))
					lastDiagnostic = now
				}
				// A near-limit sample can expire before the normal next poll.
				// Retry sooner unless the current bound covers that whole interval.
				if _, ready := n.clock.corrected(time.Now().Add(clockSampleInterval), active, cfg.Device.Name); !ready {
					delay = clockRetryInterval
				}
				if !wasReady {
					delay = clockWarmupInterval
				}
			}
		}
		time.Sleep(delay)
	}
}

// clockOffset bounds the remote clock minus the local clock for one exchange.
// The remote timestamps use that peer's clock; sent and received are local.
type clockOffset struct {
	lower time.Duration
	upper time.Duration
}

// measureClockOffset uses the four timestamps of a request and response.
// It does not assume that the outgoing and return network delays are equal.
func measureClockOffset(sent, received time.Time, remoteReceived, remoteSent int64) (clockOffset, bool) {
	if remoteReceived == 0 || remoteSent == 0 || remoteSent < remoteReceived || !received.After(sent) {
		return clockOffset{}, false
	}
	// Compare monotonic and wall elapsed time when available. A local wall-clock
	// step during the exchange invalidates the measured offset.
	elapsed := received.Sub(sent)
	wallElapsed := received.Round(0).Sub(sent.Round(0))
	if absDuration(elapsed-wallElapsed) > time.Millisecond {
		return clockOffset{}, false
	}
	remoteIn := time.Unix(0, remoteReceived)
	remoteOut := time.Unix(0, remoteSent)
	offset := clockOffset{
		lower: remoteOut.Sub(received),
		upper: remoteIn.Sub(sent),
	}
	// Bound arithmetic as well as implausible samples before calculating width.
	const maxOffset = 24 * time.Hour
	return offset, offset.lower <= offset.upper && offset.lower >= -maxOffset && offset.upper <= maxOffset
}
