package nearest

import (
	"path/filepath"
	"slices"
	"strings"

	"github.com/HuskerMinion/techo5/echod/internal/feature/nearest/membership"
)

func (m *membershipRuntime) deviceIDValue() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deviceID
}

func (m *membershipRuntime) rosterStore() *membership.RosterStore {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.store
}

// info is the detached status this device reports to peers and the setup page.
func (m *membershipRuntime) info(name, mode string, enabled bool) membershipInfo {
	m.mu.Lock()
	store, phase, blocked := m.store, m.phase, m.blocked
	conflict := len(m.conflicts)
	note := m.note
	id := m.deviceID
	m.mu.Unlock()
	info := membershipInfo{RetiredClusters: m.retiredClusters(), DeviceID: id, Name: name, Mode: mode, Enabled: enabled, Blocked: blocked, Lifecycle: "uninitialised"}
	if blocked == "uninitialised" || blocked == "" {
		info.Blocked = note
	}
	if store == nil {
		switch {
		case blocked == "state_missing" || blocked == "storage_error":
			info.Lifecycle = "blocked"
		case phase != "":
			info.Lifecycle = phase
		}
		return info
	}
	snap := store.Snapshot()
	snapshot := m.snapshot()
	info.Cluster, info.Epoch, info.Authority = snap.Current.Cluster, snap.Current.Epoch, snap.Current.Authority
	info.Members = slices.Clone(snap.Current.Members)
	if snap.Current.Cluster != "" {
		info.Fingerprint, _ = membership.Fingerprint(snap.Current)
	}
	if snap.Pending != nil {
		info.Pending = snap.Pending.Transaction
	} else if snap.Prepared != nil {
		info.Pending = snap.Prepared.Transaction
	}
	if snapshot.Blocked != "" {
		info.Blocked = snapshot.Blocked
	}
	switch {
	case m.isRetired():
		info.Lifecycle, info.Retired, info.Blocked = "retired", true, "retired"
	case conflict > 0:
		info.Lifecycle = "conflict"
	case snap.Uncertain:
		info.Lifecycle = "blocked"
	case snap.Current.Cluster == "":
		info.Lifecycle = "joining"
	case snap.Pending != nil:
		info.Lifecycle = "fencing"
	case phase == "catching_up":
		info.Lifecycle = "catching_up"
	case snapshot.Ready:
		info.Lifecycle, info.Blocked = "ready", ""
	default:
		info.Lifecycle = "blocked"
	}
	return info
}

func (m *membershipRuntime) isRetired() bool {
	store := m.rosterStore()
	return store != nil && store.Retired()
}

func (m *membershipRuntime) retiredLocked() bool { return m.store != nil && m.store.Retired() }

func (m *membershipRuntime) setPhase(phase string) {
	m.mu.Lock()
	m.phase = phase
	m.mu.Unlock()
}

func (m *membershipRuntime) queueJoin(id, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.joins == nil {
		m.joins = make(map[string]string)
	}
	if _, queued := m.joins[id]; queued || len(m.joins) < maxPeers {
		m.joins[id] = name
	}
}

func (m *membershipRuntime) queuedJoins() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.joins))
	for id := range m.joins {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (m *membershipRuntime) dequeueJoin(id string) {
	m.mu.Lock()
	delete(m.joins, id)
	m.mu.Unlock()
}

// prepare durably records a joining device's exact intended successor. It stays unable to vote.
func (m *membershipRuntime) prepare(change membership.RosterChange) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deviceID == "" || m.blocked == "storage_error" || m.retiredLocked() {
		return membership.ErrConflict
	}
	if change.Kind != "add" || !slices.Contains(change.Next.Members, m.deviceID) ||
		slices.Contains(change.Parent.Members, m.deviceID) {
		return membership.ErrCorrupt
	}
	if m.store == nil && m.wasEnrolled() {
		return membership.ErrConflict
	}
	_, rosterPath := m.paths()
	store, err := membership.PrepareRoster(rosterPath, change)
	if err != nil {
		return err
	}
	if err := m.markEnrolled(); err != nil {
		return err
	}
	if m.store == nil {
		store.SetTiming(nil, membershipDrain)
		m.store, m.closed, m.blocked, m.generation = store, true, "joining", m.generation+1
	}
	return nil
}

// fence closes this device's gate for the exact successor, persists it, and acknowledges once the old
// generation has drained. ErrDraining means poll again.
func (m *membershipRuntime) fence(change membership.RosterChange) (membership.RosterAck, error) {
	m.mu.Lock()
	store, id, closed := m.store, m.deviceID, m.closed
	m.mu.Unlock()
	if store == nil {
		return membership.RosterAck{}, membership.ErrUninitialized
	}
	snap := store.Snapshot()
	if snap.Current.Cluster == "" || !slices.Contains(change.Parent.Members, id) {
		// Already past it: a retransmitted fence after this device installed the decision (a lost
		// acknowledgement, a retried round) is answered, not refused. Only a device that really took
		// part in this exact change can have its decision.
		if _, done := store.DecisionFor(change.Transaction); done && slices.Contains(change.Parent.Members, id) {
			return membership.RosterAck{Change: change, Voter: id}, nil
		}
		return membership.RosterAck{}, membership.ErrCorrupt
	}
	if _, done := store.DecisionFor(change.Transaction); done {
		return membership.RosterAck{Change: change, Voter: id}, nil
	}
	if !(closed && snap.Pending != nil && snap.Pending.Transaction == change.Transaction) {
		if err := m.closeForFence(change); err != nil {
			return membership.RosterAck{}, err
		}
	}
	if err := store.FinishDrain(); err != nil {
		return membership.RosterAck{}, err
	}
	return store.Acknowledge(change, id)
}

// install applies a retained decision on a prepared joiner or on a member, fencing a member first.
// Repeating a decision already installed is a no-op that does not touch the gate.
func (m *membershipRuntime) install(decision membership.RosterDecision) error {
	store := m.rosterStore()
	if store == nil {
		return membership.ErrUninitialized
	}
	if existing, ok := store.DecisionFor(decision.Change.Transaction); ok {
		if existing.Change.Next.Epoch != decision.Change.Next.Epoch {
			return membership.ErrConflict
		}
		return nil
	}
	if store.Snapshot().Prepared == nil {
		if err := m.closeForFence(decision.Change); err != nil {
			return err
		}
	}
	if err := store.Install(decision); err != nil {
		return err
	}
	m.markReady()
	return nil
}

func (m *membershipRuntime) historyAfter(after uint64) ([]membership.RosterDecision, bool, error) {
	store := m.rosterStore()
	if store == nil {
		return nil, false, membership.ErrUninitialized
	}
	return store.HistoryAfter(after, maxHistoryPage)
}

// retiredClusters lists clusters whose retired roster this device has parked on re-enrolling.
func (m *membershipRuntime) retiredClusters() []string {
	matches, _ := filepath.Glob(filepath.Join(m.stateDir, "nearest-membership", "roster.retired-*.json"))
	var out []string
	for _, match := range matches {
		name := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(match), "roster.retired-"), ".json")
		out = append(out, name)
	}
	return out
}
