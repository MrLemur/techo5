package nearest

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
	"github.com/HuskerMinion/techo5/echod/internal/feature/web"
)

const peerStatusPath = "/nearest/v7/status"

type observedPeerStatus struct {
	status  peerStatus
	address string
	port    int
	seen    time.Time
}

type peerStatusCache struct {
	mu       sync.Mutex
	wordHash [sha256.Size]byte
	records  map[string]observedPeerStatus
}

func (c *peerStatusCache) selectWord(word string) {
	hash := sha256.Sum256([]byte(word))
	if c.records == nil || c.wordHash != hash {
		c.wordHash = hash
		c.records = make(map[string]observedPeerStatus)
	}
}

func (c *peerStatusCache) observe(word string, peer announce.Peer, status peerStatus, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.selectWord(word)
	c.records[strings.ToLower(strings.TrimSpace(peer.Name))] = observedPeerStatus{
		status: status, address: peer.Address, port: peer.Port, seen: now,
	}
}

// ready checks every currently discovered hearing peer and remembers an
// authenticated mismatch even if that peer subsequently disappears. A peer
// must report the selected mode again before it stops blocking coordination.
//
// A peer that stops answering after it last reported the selected mode, at the
// address it still has, does not block: it is a member gone quiet (unplugged,
// cut off), and the majority and the leader's lease already decide without
// it. Blocking on it took every wake word in the house away for as long as its
// discovery record outlived it. A peer seen that never
// reported, or that reported another mode, still blocks.
func (c *peerStatusCache) ready(word, mode string, peers []announce.Peer, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.selectWord(word)
	for _, record := range c.records {
		if record.status.Enabled && record.status.Mode != mode {
			return false
		}
	}
	if len(peers) > maxPeers {
		return false
	}
	seen := make(map[string]bool, len(peers))
	for _, peer := range peers {
		name := strings.ToLower(strings.TrimSpace(peer.Name))
		record, ok := c.records[name]
		if name == "" || seen[name] || !ok || record.address != peer.Address || record.port != peer.Port ||
			now.Before(record.seen) || !record.status.Enabled || record.status.Mode != mode {
			return false
		}
		// Stale but last reporting this mode: gone quiet, which does not block (above).
		seen[name] = true
	}
	return true
}

func (n *Nearest) peerStatusLoop() {
	for {
		cfg := config.Get()
		if n.open() && cfg.Nearest.EffectiveMode() != modeExisting {
			word := cfg.Home.HouseWord
			for _, peer := range limitPeers(hearing(n.peers())) {
				go func(peer announce.Peer) {
					if status, err := requestPeerStatus(peer, word); err == nil {
						// Do not let an in-flight request restore a previous house's cache.
						if config.Get().Home.HouseWord == word {
							n.peerStatuses.observe(word, peer, status, time.Now())
						}
					}
				}(peer)
			}
		}
		time.Sleep(clockSampleInterval)
	}
}

func (n *Nearest) peerModesReady(word, mode string) bool {
	return n.peerStatuses.ready(word, mode, hearing(n.peers()), time.Now())
}

// peerStatus is what a peer last reported about itself. It never changes membership: a peer that is no
// longer discovered stays a member.
type peerStatus struct {
	Nonce   string `json:"nonce"`
	Name    string `json:"name,omitempty"`
	Mode    string `json:"mode,omitempty"`
	Enabled bool   `json:"enabled"`
}

func (n *Nearest) receivePeerStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || web.AwayRequest(r) || !n.open() {
		http.NotFound(w, r)
		return
	}
	_ = http.NewResponseController(w).SetReadDeadline(n.now().Add(bodyWithin))
	body, err := readBody(r)
	cfg := config.Get()
	if err != nil || !hmac.Equal([]byte(r.Header.Get(readinessHeader)),
		[]byte(clockMAC(cfg.Home.HouseWord, "peer-status-request", body))) {
		http.Error(w, "invalid request", http.StatusForbidden)
		return
	}
	var request peerStatus
	if json.Unmarshal(body, &request) != nil || len(request.Nonce) != 32 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if _, err := hex.DecodeString(request.Nonce); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	response, err := json.Marshal(peerStatus{
		Nonce: request.Nonce, Name: cfg.Device.Name, Mode: cfg.Nearest.EffectiveMode(),
		Enabled: cfg.Nearest.Enabled(),
	})
	if err != nil {
		http.Error(w, "invalid response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(readinessHeader, clockMAC(cfg.Home.HouseWord, "peer-status-response", response))
	_, _ = w.Write(response)
}

// requestPeerStatus only accepts a response bound to this request and the
// announced peer name. The house word authenticates the exchange; HTTP remains
// unencrypted, like the other Nearest device messages.
func requestPeerStatus(peer announce.Peer, word string) (peerStatus, error) {
	if word == "" || strings.TrimSpace(peer.Name) == "" {
		return peerStatus{}, fmt.Errorf("nearest: peer or house word unavailable")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return peerStatus{}, err
	}
	request := peerStatus{Nonce: hex.EncodeToString(nonce[:])}
	body, err := json.Marshal(request)
	if err != nil {
		return peerStatus{}, err
	}
	url := "http://" + net.JoinHostPort(peer.Address, fmt.Sprint(peer.Port)) + peerStatusPath
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return peerStatus{}, err
	}
	req.Header.Set(readinessHeader, clockMAC(word, "peer-status-request", body))
	resp, err := client.Do(req)
	if err != nil {
		return peerStatus{}, err
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil || len(response) > maxBody || resp.StatusCode != http.StatusOK ||
		!hmac.Equal([]byte(resp.Header.Get(readinessHeader)),
			[]byte(clockMAC(word, "peer-status-response", response))) {
		return peerStatus{}, fmt.Errorf("nearest: peer status unavailable")
	}
	var status peerStatus
	if json.Unmarshal(response, &status) != nil || status.Nonce != request.Nonce ||
		!strings.EqualFold(strings.TrimSpace(status.Name), strings.TrimSpace(peer.Name)) ||
		!validMode(status.Mode) {
		return peerStatus{}, fmt.Errorf("nearest: invalid peer status")
	}
	return status, nil
}
