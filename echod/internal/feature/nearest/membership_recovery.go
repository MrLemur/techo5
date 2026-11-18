package nearest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"
)

func (m *membershipRuntime) isOpened() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.opened
}

func (m *membershipRuntime) setNote(note string) {
	m.mu.Lock()
	m.note = note
	m.mu.Unlock()
}

func (m *membershipRuntime) currentBlocked() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.blocked
}

func (m *membershipRuntime) noBootstrapSet() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.noBootstrap
}

// settle starts the discovery settling clock on first use and reports whether it has elapsed.
func (m *membershipRuntime) settle(d time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.settling.IsZero() {
		m.settling = time.Now()
	}
	return time.Since(m.settling) >= d
}

// addConflict records another live cluster of this house. The gate stays closed until the conflict is
// resolved by retirement, never merely because the other cluster went out of sight.
func (m *membershipRuntime) addConflict(info membershipInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conflicts == nil {
		m.conflicts = make(map[string]membershipInfo)
	}
	if _, known := m.conflicts[info.Cluster]; !known {
		m.generation++
	}
	m.conflicts[info.Cluster] = info
}

func (m *membershipRuntime) conflictIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.conflicts))
	for id := range m.conflicts {
		ids = append(ids, id)
	}
	return ids
}

func (m *membershipRuntime) conflictList() []membershipInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]membershipInfo, 0, len(m.conflicts))
	for _, info := range m.conflicts {
		out = append(out, info)
	}
	return out
}

// resolveConflicts clears a conflict only once a strict majority of the other cluster's roster is seen
// reporting itself retired.
func (m *membershipRuntime) resolveConflicts(dir directory) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.retiredSeen == nil {
		m.retiredSeen = make(map[string]map[string]bool)
	}
	note := func(cluster, id string) {
		if m.retiredSeen[cluster] == nil {
			m.retiredSeen[cluster] = make(map[string]bool)
		}
		m.retiredSeen[cluster][id] = true
	}
	for id, e := range dir {
		if e.info.Cluster != "" && e.info.Retired {
			note(e.info.Cluster, id)
		}
		// A device that has already re-enrolled still carries the record of the cluster it left.
		for _, cluster := range e.info.RetiredClusters {
			note(cluster, id)
		}
	}
	for cluster, info := range m.conflicts {
		if len(m.retiredSeen[cluster]) > len(info.Members)/2 {
			delete(m.conflicts, cluster)
		}
	}
}

func (n *Nearest) handleRetire(req membershipRequest) (membershipResponse, bool) {
	if !n.membershipControlOpen() || req.Decision == nil || req.Decision.Change.Kind != "retire" {
		return membershipResponse{Status: statusRefused}, false
	}
	err := n.membership.install(*req.Decision)
	return membershipResponse{Status: statusFor(err)}, err == nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = f.Sync()
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

// reenrol parks the retired roster (history and all) and returns the device to the never-enrolled
// state, allowed to join but never to bootstrap.
func (m *membershipRuntime) reenrol() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.store == nil || !m.store.Retired() {
		return errors.New("nearest membership: only a retired device can be re-enrolled")
	}
	cluster := m.store.Snapshot().Current.Cluster
	_, rosterPath := m.paths()
	if err := os.Rename(rosterPath, filepath.Join(filepath.Dir(rosterPath), "roster.retired-"+cluster+".json")); err != nil {
		return err
	}
	if err := os.Remove(m.enrolledPath()); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := syncDir(filepath.Dir(rosterPath)); err != nil {
		return err
	}
	m.store, m.closed, m.blocked, m.phase, m.note = nil, true, "uninitialised", "", ""
	m.conflicts, m.noBootstrap, m.settling = nil, true, time.Time{}
	m.generation++
	return nil
}

// ReEnrol (called from the setup page) needs exactly one live cluster in sight to join.
func (n *Nearest) reenrolChecked() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir, _ := n.membershipDirectory(ctx)
	clusters := map[string]bool{}
	for _, e := range dir {
		if e.info.Cluster != "" && !e.info.Retired && e.info.Mode == modeElected {
			clusters[e.info.Cluster] = true
		}
	}
	if len(clusters) != 1 {
		return errors.New("nearest membership: exactly one retained cluster must be in sight to re-enrol")
	}
	return n.membership.reenrol()
}

// setDirectory keeps the last discovery result so the setup page shows reachability without polling.
func (m *membershipRuntime) setDirectory(dir directory) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen = make(map[string]membershipInfo, len(dir))
	for id, e := range dir {
		m.seen[id] = e.info
	}
	m.seenAt = time.Now()
}
