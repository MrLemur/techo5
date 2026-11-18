package nearest

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/nearest/membership"
	"github.com/HuskerMinion/techo5/echod/internal/layout"
)

type membershipToken struct {
	cluster     string
	epoch       uint64
	fingerprint string
	generation  uint64
}

type membershipSnapshot struct {
	DeviceID  string
	Token     membershipToken
	Members   []string
	Authority string
	Blocked   string
	Ready     bool
}

func (s membershipSnapshot) majority() int {
	return len(s.Members)/2 + 1
}

// membershipRuntime is the single source of elected membership state. Its
// gate lock is always taken before mu. No callback or network operation runs
// while either lock is held.
type membershipRuntime struct {
	mu          sync.Mutex
	gate        sync.Mutex
	releases    sync.WaitGroup
	stateDir    string
	deviceID    string
	store       *membership.RosterStore
	generation  uint64
	closed      bool
	blocked     string
	phase       string                     // worker view: discovering, joining, catching_up, conflict
	retiredSeen map[string]map[string]bool // cluster -> members seen retired; sticky, as re-enrolling hides it
	conflicts   map[string]membershipInfo  // other clusters met while a roster exists, until explicitly resolved
	note        string                     // worker explanation shown while not ready (for example authority_offline)
	noBootstrap bool                       // a re-enrolling device only ever joins, it never bootstraps
	quietUntil  time.Time                  // restart protection: wait out the longest old vote grant
	opened      bool
	names       map[string]string         // stable ID -> last announced display name
	cfgKey      string                    // settings the current generation was opened under
	onGate      func()                    // discard election state when the generation changes
	seen        map[string]membershipInfo // last discovery result, for the setup page
	seenAt      time.Time
	tx          sync.Mutex // serialises transitions; taken before gate and mu
	op          operationState
	joins       map[string]string // stable ID -> display name, queued at the authority
	settling    time.Time         // when discovery of a never-enrolled device began
}

func newMembershipRuntime(stateDir string) *membershipRuntime {
	return &membershipRuntime{stateDir: stateDir, closed: true, blocked: "uninitialised"}
}

func (m *membershipRuntime) paths() (string, string) {
	dir := filepath.Join(m.stateDir, "nearest-membership")
	return filepath.Join(dir, "identity"), filepath.Join(dir, "roster.json")
}

func (m *membershipRuntime) enrolledPath() string {
	return filepath.Join(m.stateDir, "nearest-membership", "enrolled")
}

// markEnrolled records, separately from the roster, that this device has taken
// part in a cluster, so missing roster state can never turn it into a new
// bootstrap candidate.
func (m *membershipRuntime) markEnrolled() error {
	return writeMembershipState(m.enrolledPath(), []byte("1\n"))
}

func (m *membershipRuntime) wasEnrolled() bool {
	_, err := os.Stat(m.enrolledPath())
	return err == nil
}

func (m *membershipRuntime) open() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stateDir == "" {
		m.stateDir = layout.StateDir
	}
	identity, roster := m.paths()
	if err := os.MkdirAll(filepath.Dir(identity), 0o700); err != nil {
		m.blocked = "storage_error"
		return err
	}
	if m.deviceID == "" {
		raw, err := os.ReadFile(identity)
		switch {
		case err == nil:
			m.deviceID = strings.TrimSpace(string(raw))
			if bytes, decodeErr := hex.DecodeString(m.deviceID); decodeErr != nil || len(bytes) != 16 {
				m.blocked = "storage_error"
				return membership.ErrCorrupt
			}
		case errors.Is(err, os.ErrNotExist):
			var id [16]byte
			if _, err := rand.Read(id[:]); err != nil {
				m.blocked = "storage_error"
				return err
			}
			m.deviceID = hex.EncodeToString(id[:])
			if err := writeMembershipState(identity, []byte(m.deviceID+"\n")); err != nil {
				m.blocked = "storage_error"
				return err
			}
		default:
			m.blocked = "storage_error"
			return err
		}
	}
	store, err := membership.OpenRoster(roster)
	if errors.Is(err, membership.ErrUninitialized) {
		m.store, m.closed, m.blocked = nil, true, "uninitialised"
		if m.wasEnrolled() {
			m.blocked = "state_missing"
		}
		m.opened = true
		return nil
	}
	if err != nil {
		m.store, m.closed, m.blocked = nil, true, "storage_error"
		return err
	}
	store.SetTiming(nil, membershipDrain)
	if err := m.markEnrolled(); err != nil {
		m.store, m.closed, m.blocked = nil, true, "storage_error"
		return err
	}
	m.loadNames()
	m.store = store
	m.opened = true
	m.cfgKey = currentConfigKey()
	m.quietUntil = time.Now().Add(coordinatorGrant)
	current, pending, uncertain := store.View()
	m.closed = pending != nil || uncertain
	if current.Cluster == "" {
		m.closed, m.blocked = true, "joining"
		return nil
	}
	if uncertain {
		m.blocked = "storage_error"
	} else if pending != nil {
		m.blocked = "fencing"
	} else {
		m.closed, m.blocked = false, ""
	}
	if store.Retired() {
		m.closed, m.blocked = true, "retired"
	} else if !slices.Contains(current.Members, m.deviceID) {
		m.closed, m.blocked = true, "removed"
	}
	return nil
}

// bootstrap creates a group of one, only on a device with no saved membership. Deciding which device
// does it is discoverAndJoin's.
func (m *membershipRuntime) bootstrap(cluster string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deviceID == "" || m.store != nil || !m.closed || m.blocked != "uninitialised" {
		return membership.ErrConflict
	}
	_, rosterPath := m.paths()
	store, err := membership.CreateRoster(rosterPath, membership.Roster{
		Cluster: cluster, Epoch: 1, Authority: m.deviceID, Members: []string{m.deviceID},
	})
	if err != nil {
		m.blocked = "storage_error"
		return err
	}
	if err := m.markEnrolled(); err != nil {
		m.blocked = "storage_error"
		return err
	}
	store.SetTiming(nil, membershipDrain)
	m.store, m.closed, m.blocked = store, false, ""
	m.cfgKey = currentConfigKey()
	m.quietUntil = time.Now().Add(coordinatorGrant)
	m.generation++
	return nil
}

func (m *membershipRuntime) memberNames() ([]string, bool) {
	snapshot := m.snapshot()
	return snapshot.Members, snapshot.Ready
}

func (m *membershipRuntime) authority() (string, bool) {
	snapshot := m.snapshot()
	return snapshot.Authority, snapshot.Ready
}

// closeForFence prevents a new release from reserving the old generation,
// waits for releases already reserved by it, then durably records the exact
// successor. A failed write leaves the gate closed.
func (m *membershipRuntime) closeForFence(change membership.RosterChange) error {
	m.gate.Lock()
	m.mu.Lock()
	m.closed = true
	m.blocked = "fencing"
	m.generation++
	store := m.store
	m.mu.Unlock()
	m.gate.Unlock()
	m.releases.Wait()
	m.gateChanged()
	if store == nil {
		return membership.ErrUninitialized
	}
	if err := store.Fence(change); err != nil {
		// Only a failed write is a storage fault; a refused or out-of-order change leaves the gate
		// closed for the reason it already had.
		if !errors.Is(err, membership.ErrConflict) && !errors.Is(err, membership.ErrCorrupt) {
			m.mu.Lock()
			m.blocked = "storage_error"
			m.mu.Unlock()
		}
		return err
	}
	return nil
}

func writeMembershipState(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".nearest-membership-*")
	if err != nil {
		return err
	}
	temp := file.Name()
	defer os.Remove(temp)
	if err = file.Chmod(0o600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(temp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	if err = dir.Sync(); err == nil {
		err = dir.Close()
	} else {
		_ = dir.Close()
	}
	return err
}

func (m *membershipRuntime) snapshot() membershipSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked()
}

func (m *membershipRuntime) snapshotLocked() membershipSnapshot {
	snapshot := membershipSnapshot{DeviceID: m.deviceID, Blocked: m.blocked}
	if m.store == nil || m.closed {
		return snapshot
	}
	if m.cfgKey != currentConfigKey() {
		snapshot.Blocked = "settings_changed"
		return snapshot
	}
	if len(m.conflicts) > 0 {
		snapshot.Blocked = "conflict"
		return snapshot
	}
	if time.Now().Before(m.quietUntil) {
		snapshot.Blocked = "quiet_period"
		return snapshot
	}
	roster, pending, uncertain := m.store.View()
	if pending != nil || uncertain || roster.Cluster == "" {
		return snapshot
	}
	fingerprint, err := membership.Fingerprint(roster)
	if err != nil {
		snapshot.Blocked = "storage_error"
		return snapshot
	}
	snapshot.Token = membershipToken{cluster: roster.Cluster, epoch: roster.Epoch, fingerprint: fingerprint, generation: m.generation}
	snapshot.Members = slices.Clone(roster.Members)
	snapshot.Authority = roster.Authority
	snapshot.Ready = true
	return snapshot
}

func (m *membershipRuntime) invalidate(reason string) {
	m.gate.Lock()
	m.mu.Lock()
	m.closed = true
	m.blocked = reason
	m.generation++
	m.mu.Unlock()
	m.gate.Unlock()
	m.releases.Wait()
	m.gateChanged()
}

func (m *membershipRuntime) reserve(token membershipToken) bool {
	m.gate.Lock()
	defer m.gate.Unlock()
	m.mu.Lock()
	snapshot := m.snapshotLocked()
	m.mu.Unlock()
	if !snapshot.Ready || snapshot.Token != token {
		return false
	}
	m.releases.Add(1)
	return true
}

func (m *membershipRuntime) release() {
	m.releases.Done()
}

// releaseIfCurrent is the final barrier immediately before a coordinated
// voice callback. It never calls the callback while holding membership locks.
func (m *membershipRuntime) releaseIfCurrent(token membershipToken, callback func()) bool {
	if !m.reserve(token) {
		return false
	}
	defer m.release()
	callback()
	return true
}

func (m *membershipRuntime) markReady() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.store == nil {
		m.closed = true
		m.blocked = "uninitialised"
		return
	}
	current, pending, uncertain := m.store.View()
	if current.Cluster == "" {
		m.closed, m.blocked = true, "joining"
		return
	}
	if m.store.Retired() {
		m.closed, m.blocked = true, "retired"
		return
	}
	if !slices.Contains(current.Members, m.deviceID) {
		m.closed, m.blocked = true, "removed"
		return
	}
	if pending != nil {
		m.closed = true
		m.blocked = "fencing"
		return
	}
	if uncertain {
		m.closed = true
		m.blocked = "storage_error"
		return
	}
	m.closed = false
	m.blocked = ""
	m.cfgKey = currentConfigKey()
	m.quietUntil = time.Now().Add(coordinatorGrant)
	m.generation++
	hook := m.onGate
	m.mu.Unlock()
	if hook != nil {
		hook()
	}
	m.mu.Lock()
}

// currentConfigKey identifies the settings a membership generation is valid under: mode, enablement,
// house word and device name. Any change makes the old generation unusable, whichever code path
// changed it.
func currentConfigKey() string {
	cfg := config.Get()
	word := sha256.Sum256([]byte(cfg.Home.HouseWord))
	return cfg.Nearest.EffectiveMode() + "|" + fmt.Sprint(cfg.Nearest.Enabled()) + "|" + hex.EncodeToString(word[:8]) + "|" + cfg.Device.Name
}

// refreshConfig starts a new generation after a settings change. Membership itself is kept: switching
// mode or house word never deletes a fence or the roster.
func (m *membershipRuntime) refreshConfig() bool {
	m.gate.Lock()
	m.mu.Lock()
	if m.cfgKey == currentConfigKey() {
		m.mu.Unlock()
		m.gate.Unlock()
		return false
	}
	m.cfgKey = currentConfigKey()
	m.generation++
	m.quietUntil = time.Now().Add(coordinatorGrant)
	hook := m.onGate
	m.mu.Unlock()
	m.gate.Unlock()
	m.releases.Wait()
	if hook != nil {
		hook()
	}
	return true
}

// committedMembers is the durable roster regardless of whether the gate is open: clock sampling and
// status exchange need it while wakes and elections are closed.
func (m *membershipRuntime) committedMembers() []string {
	store := m.rosterStore()
	if store == nil {
		return nil
	}
	return slices.Clone(store.Snapshot().Current.Members)
}

func (m *membershipRuntime) namesPath() string {
	return filepath.Join(m.stateDir, "nearest-membership", "names.json")
}

// loadNames restores the last announced display names, so a member that is offline after a restart keeps
// its name instead of being known only by its ID. Names are a convenience for elections and the page;
// they are never the identity. A missing or unreadable file means no names yet.
func (m *membershipRuntime) loadNames() {
	raw, err := os.ReadFile(m.namesPath())
	if err != nil {
		return
	}
	var names map[string]string
	if json.Unmarshal(raw, &names) == nil && len(names) <= 4*maxPeers {
		m.names = names
	}
}

// rememberName records an announced name. A rename keeps the stable ID, so the roster is untouched, but
// the election generation is restarted so no grant or lease made under the old name survives.
func (m *membershipRuntime) rememberName(id, name string) {
	name = lowerTrim(name)
	m.mu.Lock()
	if m.names == nil {
		m.names = make(map[string]string)
	}
	old := m.names[id]
	if old == name {
		m.mu.Unlock()
		return
	}
	m.names[id] = name
	if old != "" {
		m.generation++
	}
	raw, _ := json.Marshal(m.names)
	path, hook := m.namesPath(), m.onGate
	m.mu.Unlock()
	if err := writeMembershipState(path, raw); err != nil {
		slog.Warn("saving nearest device names failed", "err", err)
	}
	if old != "" && hook != nil {
		hook()
	}
}

// namesFor maps stable IDs to the lower-case display names elections use. An offline member with no
// known name keeps its ID, so it still counts in the denominator. Duplicate names are ambiguous.
func (m *membershipRuntime) namesFor(ids []string, selfName string) ([]string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(ids))
	seen := map[string]bool{}
	for _, id := range ids {
		name := m.names[id]
		if id == m.deviceID {
			name = lowerTrim(selfName)
		}
		if name == "" {
			name = id
		}
		if seen[name] {
			return nil, false
		}
		seen[name] = true
		out = append(out, name)
	}
	slices.Sort(out)
	return out, true
}

// valid is whether token is still the live, ready membership.
func (m *membershipRuntime) valid(token membershipToken) bool {
	snapshot := m.snapshot()
	return snapshot.Ready && snapshot.Token == token
}

func (m *membershipRuntime) gateChanged() {
	m.mu.Lock()
	hook := m.onGate
	m.mu.Unlock()
	if hook != nil {
		hook()
	}
}
