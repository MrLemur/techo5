package nearest

import (
	"errors"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
)

// MemberView is one committed member as the setup page shows it.
type MemberView struct {
	ID        string
	Name      string
	Self      bool
	Authority bool
	Reachable bool
}

// ClusterView is one cluster of this house, ours or a conflicting one.
type ClusterView struct {
	ID        string
	Epoch     uint64
	Authority string
	Members   []MemberView
	Reachable int
	Majority  int
	Retired   bool
}

// Capabilities decides which controls are offered and, because the operations check the same fields,
// which are allowed: the page and the server cannot disagree.
type Capabilities struct {
	ReplaceAuthority bool
	Remove           bool
	Keep             bool
	Retry            bool
	ReEnrol          bool
}

// MembershipView is a detached snapshot for the setup page.
type MembershipView struct {
	Mode        string
	Enabled     bool
	Lifecycle   string
	Blocked     string
	DeviceID    string
	Name        string
	Epoch       uint64
	Fingerprint string
	Own         ClusterView
	Conflicts   []ClusterView
	Coordinator string // the current wake coordinator, not the membership authority
	// NotAnswering is a short reason this device would drop a wake right now, empty when it would not.
	NotAnswering string
	Pending      string
	Operation    operationState
	Can          Capabilities
}

// MembershipView builds the snapshot. It reads RAM and the last discovery, and does no network I/O.
func (n *Nearest) MembershipView() MembershipView {
	cfg := config.Get()
	rt := n.membership
	info := n.membershipInfo()
	rt.mu.Lock()
	seen := make(map[string]membershipInfo, len(rt.seen))
	for id, i := range rt.seen {
		seen[id] = i
	}
	op := rt.op
	rt.mu.Unlock()
	view := MembershipView{Mode: info.Mode, Enabled: info.Enabled, Lifecycle: info.Lifecycle, Blocked: info.Blocked,
		DeviceID: info.DeviceID, Name: cfg.Device.Name, Epoch: info.Epoch, Fingerprint: info.Fingerprint,
		Pending: info.Pending, Operation: op}
	view.Own = clusterView(info, info.DeviceID, cfg.Device.Name, seen)
	view.Own.Retired = info.Retired
	for _, c := range rt.conflictList() {
		view.Conflicts = append(view.Conflicts, clusterView(c, info.DeviceID, cfg.Device.Name, seen))
	}
	if leader, _ := n.coordinator.leaderFor(time.Now()); leader != "" {
		view.Coordinator = leader
	}
	elected := info.Mode == modeElected && info.Enabled
	if elected {
		view.NotAnswering = n.notAnsweringReason()
	}
	settled := elected && info.Cluster != "" && !info.Retired && len(view.Conflicts) == 0 && !op.Running
	majority := view.Own.Reachable >= view.Own.Majority
	view.Can = Capabilities{
		ReplaceAuthority: settled && info.Pending == "" && len(info.Members) > 1 && majority,
		Remove:           settled && info.Pending == "" && len(info.Members) > 1 && majority,
		Retry:            elected && info.Pending != "" && info.Cluster != "" && !op.Running,
		Keep:             elected && !op.Running && len(view.Conflicts) > 0,
		ReEnrol:          info.Retired && !op.Running,
	}
	return view
}

func clusterView(info membershipInfo, self, selfName string, seen map[string]membershipInfo) ClusterView {
	c := ClusterView{ID: info.Cluster, Epoch: info.Epoch, Majority: len(info.Members)/2 + 1}
	for _, id := range info.Members {
		m := MemberView{ID: id, Self: id == self, Authority: id == info.Authority}
		if id == self {
			m.Name, m.Reachable = selfName, true
		} else if s, ok := seen[id]; ok {
			m.Name, m.Reachable = s.Name, true
		}
		if m.Reachable {
			c.Reachable++
		}
		if m.Authority {
			c.Authority = firstNonEmpty(m.Name, id)
		}
		c.Members = append(c.Members, m)
	}
	return c
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// SetMode is the explicit mode change. It keeps stored membership and any fence: switching away and
// back resumes them and never bootstraps anew.
func (n *Nearest) SetMode(mode string) error {
	if !validMode(mode) {
		return errors.New("nearest: unknown mode")
	}
	if err := config.Set().Nearest().Mode(mode); err != nil {
		return err
	}
	n.mode.Set(mode)
	n.settingsChanged()
	return nil
}

// Do* are the checked entry points for the setup forms: each re-reads the same capability snapshot the
// page was drawn from, then runs the real operation, which also rejects a stale cluster or epoch.

func (n *Nearest) DoReplaceAuthority(cluster string, epoch uint64, device string) error {
	if !n.MembershipView().Can.ReplaceAuthority {
		return errors.New("nearest: replacing the authority is not possible right now")
	}
	return n.ReplaceAuthority(cluster, epoch, device)
}

func (n *Nearest) DoRemoveDevice(cluster string, epoch uint64, device string) error {
	if !n.MembershipView().Can.Remove {
		return errors.New("nearest: removing a device is not possible right now")
	}
	return n.RemoveDevice(cluster, epoch, device)
}

func (n *Nearest) DoRetryPending(transaction string) error {
	if !n.MembershipView().Can.Retry {
		return errors.New("nearest: there is no pending change to retry")
	}
	return n.RetryPending(transaction)
}

// DoKeepCluster checks the cluster to keep against what is in sight and the epoch the page showed.
func (n *Nearest) DoKeepCluster(cluster string, epoch uint64) error {
	view := n.MembershipView()
	if !view.Can.Keep {
		return errors.New("nearest: there is no conflict to resolve")
	}
	for _, c := range append([]ClusterView{view.Own}, view.Conflicts...) {
		if c.ID == cluster && c.Epoch == epoch {
			return n.KeepCluster(cluster)
		}
	}
	return ErrStale
}

func (n *Nearest) DoReEnrol() error {
	if !n.MembershipView().Can.ReEnrol {
		return errors.New("nearest: this device is not retired")
	}
	return n.ReEnrol()
}

// notAnsweringReason mirrors the checks contend makes in Device quorum, in the same order, and names the
// first that would drop a wake. It reads RAM only and is empty when the device would answer.
func (n *Nearest) notAnsweringReason() string {
	cfg := config.Get()
	if !n.open() {
		return ""
	}
	snapshot := n.membership.snapshot()
	if !snapshot.Ready {
		switch snapshot.Blocked {
		case "conflict":
			return "group conflict"
		case "fencing", "quiet_period", "settings_changed":
			return "membership change in progress"
		}
		return "membership not ready"
	}
	if !n.peerModesReady(cfg.Home.HouseWord, cfg.Nearest.EffectiveMode()) {
		return "mode mismatch"
	}
	if len(snapshot.Members) == 1 && snapshot.Members[0] == snapshot.DeviceID {
		if len(hearing(n.peers())) > 0 {
			return "other devices visible but not in group"
		}
		return ""
	}
	roster, _, ok := n.electedRoster()
	if !ok {
		return "membership not ready"
	}
	active := n.clock.active(roster, cfg.Device.Name, time.Now())
	if _, complete := rosterPeers(limitPeers(hearing(n.peers())), active, cfg.Device.Name); !complete || len(active) <= len(roster)/2 {
		return "no majority reachable"
	}
	if _, ready := n.clockNow(time.Now()); !ready {
		return "waiting for clock sync"
	}
	if leader, _ := n.coordinator.leaderFor(time.Now()); leader == "" {
		return "waiting for a wake coordinator"
	}
	return ""
}
