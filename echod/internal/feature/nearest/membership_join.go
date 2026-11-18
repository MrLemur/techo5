package nearest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
	"github.com/HuskerMinion/techo5/echod/internal/feature/nearest/membership"
)

const membershipTick = 2 * time.Second

// membershipWarmupTick is the poll interval while this device is not yet ready, so that joining and
// catching up do not each wait out a full tick. A standing conflict is not polled faster.
const membershipWarmupTick = 500 * time.Millisecond

// bootstrapSettle is how long a never-enrolled device watches for an existing cluster before the
// lowest stable ID among the visible, never-enrolled devices creates a group of one. A variable so the
// tests can shorten it.
var bootstrapSettle = 10 * time.Second

// directoryEntry is one discovered peer whose membership status checked out.
type directoryEntry struct {
	peer announce.Peer
	info membershipInfo
}

type directory map[string]directoryEntry

func (n *Nearest) membershipLoop() {
	for {
		if n.membershipControlOpen() {
			n.membershipStep()
		}
		n.publishMembershipStatus()
		if s := n.membership.snapshot(); s.Ready || s.Blocked == "conflict" {
			time.Sleep(membershipTick)
		} else {
			time.Sleep(membershipWarmupTick)
		}
	}
}

// membershipDirectory asks every discovered microphone-capable peer for its membership status. Two
// peers claiming one stable ID, or one peer answering with a different name than it announced, make the
// mapping ambiguous: those are reported and never used.
func (n *Nearest) membershipDirectory(ctx context.Context) (directory, []string) {
	peers := limitPeers(hearing(n.peers()))
	type result struct {
		entry directoryEntry
		ok    bool
	}
	results := make([]result, len(peers))
	var wg sync.WaitGroup
	for i, peer := range peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := n.memCall(ctx, peer, "", "status", membershipRequest{})
			if err != nil || resp.Info == nil || resp.Status != statusOK {
				return
			}
			info := *resp.Info
			info.DeviceID = resp.From
			if !strings.EqualFold(strings.TrimSpace(info.Name), strings.TrimSpace(peer.Name)) {
				return
			}
			results[i] = result{directoryEntry{peer, info}, true}
		}()
	}
	wg.Wait()
	dir := make(directory)
	var ambiguous []string
	for _, r := range results {
		if !r.ok {
			continue
		}
		id := r.entry.info.DeviceID
		if _, dup := dir[id]; dup || id == n.membership.deviceIDValue() {
			ambiguous = append(ambiguous, id)
			continue
		}
		dir[id] = r.entry
	}
	for _, id := range ambiguous {
		delete(dir, id)
	}
	return dir, ambiguous
}

func (n *Nearest) membershipStep() {
	rt := n.membership
	if !rt.isOpened() {
		if err := rt.open(); err != nil {
			slog.Error("opening nearest membership failed", "err", err)
			return
		}
	}
	rt.refreshConfig()
	ctx, cancel := context.WithTimeout(context.Background(), membershipDrain+20*time.Second)
	defer cancel()
	dir, ambiguous := n.membershipDirectory(ctx)
	for id, e := range dir {
		rt.rememberName(id, e.info.Name)
	}
	rt.setDirectory(dir)
	store := rt.rosterStore()
	if len(ambiguous) > 0 {
		rt.setNote("ambiguous_identity")
	} else if store == nil {
		rt.setNote("")
	}
	if store == nil {
		n.discoverAndJoin(ctx, dir, len(ambiguous) > 0)
		return
	}
	n.maintain(ctx, store, dir)
}

// discoverAndJoin is the path of a device with no roster: join the one cluster in sight, or after the
// settling period bootstrap when this device has the lowest stable ID among the never-enrolled.
func (n *Nearest) discoverAndJoin(ctx context.Context, dir directory, ambiguous bool) {
	rt := n.membership
	if rt.currentBlocked() == "state_missing" || rt.currentBlocked() == "storage_error" {
		return
	}
	clusters := map[string][]directoryEntry{}
	candidates := []string{rt.deviceIDValue()}
	for id, e := range dir {
		if e.info.Mode != modeElected || !e.info.Enabled || e.info.Retired {
			continue
		}
		switch {
		case e.info.Cluster != "":
			clusters[e.info.Cluster] = append(clusters[e.info.Cluster], e)
		case e.info.Lifecycle == "uninitialised" || e.info.Lifecycle == "discovering":
			candidates = append(candidates, id)
		}
	}
	switch len(clusters) {
	case 0:
		if ambiguous || rt.noBootstrapSet() {
			rt.setPhase("discovering")
			return
		}
		if !rt.settle(bootstrapSettle) {
			rt.setPhase("discovering")
			return
		}
		slices.Sort(candidates)
		if candidates[0] != rt.deviceIDValue() {
			rt.setPhase("discovering")
			return
		}
		if err := rt.bootstrap("c" + newRequestID()[:16]); err != nil {
			slog.Error("nearest membership bootstrap failed", "err", err)
			return
		}
		rt.setPhase("")
		slog.Info("nearest membership bootstrapped a new cluster")
	case 1:
		for _, entries := range clusters {
			authority := entries[0].info.Authority
			target, ok := dir[authority]
			if !ok {
				rt.setPhase("joining")
				rt.setNote("authority_offline")
				return
			}
			rt.setPhase("joining")
			rt.setNote("")
			name := config.Get().Device.Name
			if _, err := n.memCall(ctx, target.peer, authority, "join", membershipRequest{Name: name}); err != nil {
				rt.setNote("authority_unreachable")
			}
		}
	default:
		rt.setPhase("discovering")
		rt.setNote("multiple_clusters")
	}
}

// maintain is the path of a device with a roster (or a prepared join): detect conflicts, catch up from
// retained history, resume pending work, and as authority admit queued joiners.
func (n *Nearest) maintain(ctx context.Context, store *membership.RosterStore, dir directory) {
	rt := n.membership
	me := rt.deviceIDValue()
	snap := store.Snapshot()
	mine, myEpoch := snap.Current.Cluster, snap.Current.Epoch
	if mine == "" && snap.Prepared != nil {
		mine, myEpoch = snap.Prepared.Parent.Cluster, snap.Prepared.Parent.Epoch
	}
	if store.Retired() {
		rt.setPhase("")
		return
	}
	// Conflicts and catch-up.
	rt.setNote("")
	catching := false
	for _, id := range sortedKeys(dir) {
		e := dir[id]
		if e.info.Retired || e.info.Cluster == "" || e.info.Mode != modeElected {
			continue
		}
		if e.info.Cluster != mine {
			rt.addConflict(e.info)
			continue
		}
		if e.info.Epoch > myEpoch && !catching {
			if err := n.catchUp(ctx, e, myEpoch); err != nil {
				catching = errors.Is(err, membership.ErrDraining)
				slog.Info("nearest membership catch-up incomplete", "from", e.info.Name, "err", err)
			}
			snap = store.Snapshot()
			if snap.Current.Cluster != "" {
				myEpoch = snap.Current.Epoch
			}
		}
	}
	rt.resolveConflicts(dir)
	if catching {
		rt.setPhase("catching_up")
		return
	}
	rt.setPhase("")
	if snap.Current.Cluster == "" {
		return // prepared: wait for the authority's commit or a peer's history
	}
	if store.Retired() {
		return
	}
	// Resume exact pending work. Only the authority does so unprompted; others use "retry pending".
	if snap.Pending != nil && snap.Current.Authority == me {
		if err := n.runTransition(ctx, *snap.Pending, dir); err != nil {
			rt.setNote("pending:" + errorKind(err))
		}
		return
	}
	if snap.Pending != nil || len(rt.conflictIDs()) > 0 {
		return
	}
	if snap.Current.Authority != me {
		n.redeliver(ctx, store, snap, dir)
		return
	}
	n.redeliver(ctx, store, snap, dir)
	for _, id := range rt.queuedJoins() {
		if slices.Contains(snap.Current.Members, id) {
			rt.dequeueJoin(id)
			continue
		}
		e, ok := dir[id]
		if !ok || e.info.Cluster != "" || e.info.Mode != modeElected || !e.info.Enabled {
			continue
		}
		if reachable(snap.Current, me, dir) <= len(snap.Current.Members)/2 {
			rt.setNote("no_majority")
			return
		}
		change, err := membership.NewRosterChange(snap.Current, newRequestID(), "add", id)
		if err != nil {
			continue
		}
		if err := n.runTransition(ctx, change, dir); err != nil {
			slog.Info("nearest membership join incomplete; will retry the same transaction", "device", id, "err", err)
			rt.setNote("pending:" + errorKind(err))
		} else {
			rt.dequeueJoin(id)
		}
		return // one change per tick
	}
}

func sortedKeys(dir directory) []string {
	keys := make([]string, 0, len(dir))
	for k := range dir {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// reachable counts members of the roster that this device can currently talk to, itself included.
func reachable(roster membership.Roster, me string, dir directory) int {
	count := 0
	for _, id := range roster.Members {
		if _, ok := dir[id]; ok || id == me {
			count++
		}
	}
	return count
}

func errorKind(err error) string {
	switch {
	case errors.Is(err, membership.ErrDraining), errors.Is(err, context.DeadlineExceeded):
		return statusDraining
	case errors.Is(err, membership.ErrConflict):
		return statusConflict
	case errors.Is(err, membership.ErrUncertain):
		return statusStorage
	default:
		return "failed"
	}
}

// catchUp installs a peer's retained history one parent-linked decision at a time. It never jumps to a
// higher epoch without each successor.
func (n *Nearest) catchUp(ctx context.Context, from directoryEntry, epoch uint64) error {
	remote := remoteParticipant{n: n, peer: from.peer, id: from.info.DeviceID}
	after := epoch
	for {
		resp, err := remote.poll(ctx, "history", membershipRequest{After: after})
		if err != nil {
			return err
		}
		if len(resp.History) == 0 || len(resp.History) > maxHistoryPage {
			return errors.New("nearest membership: peer returned no usable history")
		}
		for _, decision := range resp.History {
			if err := n.membership.install(decision); err != nil {
				return err
			}
			after = decision.Change.Next.Epoch
		}
		if !resp.More {
			return nil
		}
	}
}

// redeliver pushes retained decisions that some member might not have, then forgets them from the outbox.
func (n *Nearest) redeliver(ctx context.Context, store *membership.RosterStore, snap membership.RosterSnapshot, dir directory) {
	for _, tx := range snap.Outbox {
		decision, ok := store.DecisionFor(tx)
		if !ok {
			_ = store.Delivered(tx)
			continue
		}
		if err := membership.DistributeRosterDecision(ctx, decision, n.participants(decision.Change, dir)); err == nil {
			_ = store.Delivered(tx)
		}
	}
}

// participants builds the real set of contacted devices for a change: this device locally, others by
// current discovery. A device out of sight is left out of this change, not removed from the group.
func (n *Nearest) participants(change membership.RosterChange, dir directory) map[string]membership.RosterParticipant {
	me := n.membership.deviceIDValue()
	out := make(map[string]membership.RosterParticipant)
	for _, id := range append(slices.Clone(change.Parent.Members), change.Next.Members...) {
		if id == me && slices.Contains(change.Parent.Members, me) {
			out[id] = localParticipant{n}
		} else if e, ok := dir[id]; ok {
			out[id] = remoteParticipant{n: n, peer: e.peer, id: id}
		}
	}
	return out
}

// runTransition drives one exact change to a committed decision and distributes it. It is safe to call
// again with the same change after any failure: the proposal is persisted before the first remote fence
// and a collected decision is saved before distribution. Only one transition runs at a time.
func (n *Nearest) runTransition(ctx context.Context, change membership.RosterChange, dir directory) error {
	rt := n.membership
	rt.tx.Lock()
	defer rt.tx.Unlock()
	store := rt.rosterStore()
	if store == nil {
		return membership.ErrUninitialized
	}
	if _, done := store.DecisionFor(change.Transaction); done {
		return nil
	}
	snap := store.Snapshot()
	if snap.Pending != nil && snap.Pending.Transaction != change.Transaction {
		return membership.ErrConflict // an accepted different successor must finish first
	}
	var decision membership.RosterDecision
	if snap.Decision != nil {
		decision = *snap.Decision
	} else {
		// Persist the exact proposal and close the gate before any remote fence.
		if err := rt.closeForFence(change); err != nil {
			return err
		}
		var err error
		decision, err = membership.CollectRosterDecision(ctx, change, n.participants(change, dir))
		if err != nil {
			return err
		}
		if err := store.SaveDecision(decision); err != nil {
			return err
		}
	}
	if err := rt.install(decision); err != nil {
		return err
	}
	if err := membership.DistributeRosterDecision(ctx, decision, n.participants(decision.Change, dir)); err != nil {
		// The decision is committed locally and stays in the outbox for later retries.
		slog.Info("nearest membership decision committed; some devices still to be told", "err", err)
		return nil
	}
	return store.Delivered(decision.Change.Transaction)
}

// The recovery controls below are called from the setup page.

// ErrStale rejects a submission made against an older view than the device now has.
var ErrStale = errors.New("nearest membership: the cluster changed since this page was loaded")

type operationState struct {
	Name    string
	Running bool
	Error   string
	Done    time.Time
}

// startOperation runs one setup page action in the background and records how it went for the page.
func (n *Nearest) startOperation(name string, run func(ctx context.Context, dir directory) error) error {
	rt := n.membership
	rt.mu.Lock()
	if rt.op.Running {
		rt.mu.Unlock()
		return fmt.Errorf("nearest membership: %s is still running", rt.op.Name)
	}
	rt.op = operationState{Name: name, Running: true}
	rt.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), membershipDrain+30*time.Second)
		defer cancel()
		dir, _ := n.membershipDirectory(ctx)
		err := run(ctx, dir)
		rt.mu.Lock()
		rt.op = operationState{Name: name, Done: time.Now()}
		if err != nil {
			rt.op.Error = err.Error()
		}
		rt.mu.Unlock()
	}()
	return nil
}

func (n *Nearest) expectCurrent(cluster string, epoch uint64) (membership.RosterSnapshot, *membership.RosterStore, error) {
	store := n.membership.rosterStore()
	if store == nil {
		return membership.RosterSnapshot{}, nil, membership.ErrUninitialized
	}
	snap := store.Snapshot()
	if snap.Current.Cluster != cluster || snap.Current.Epoch != epoch {
		return snap, store, ErrStale
	}
	return snap, store, nil
}

// ReplaceAuthority makes an existing member the membership authority through a normal fenced
// transition. Membership does not change. The failed authority need not answer.
func (n *Nearest) ReplaceAuthority(cluster string, epoch uint64, newAuthority string) error {
	snap, _, err := n.expectCurrent(cluster, epoch)
	if err != nil {
		return err
	}
	if snap.Pending != nil {
		return errors.New("nearest membership: finish the pending change first")
	}
	change, err := membership.NewRosterChange(snap.Current, newRequestID(), "replace_authority", newAuthority)
	if err != nil {
		return err
	}
	return n.startOperation("replace_authority", func(ctx context.Context, dir directory) error {
		if reachable(snap.Current, n.membership.deviceIDValue(), dir) <= len(snap.Current.Members)/2 {
			return membership.ErrDraining
		}
		return n.runTransition(ctx, change, dir)
	})
}

// RemoveDevice removes a member other than the authority by an explicit parent-majority transition.
func (n *Nearest) RemoveDevice(cluster string, epoch uint64, device string) error {
	snap, _, err := n.expectCurrent(cluster, epoch)
	if err != nil {
		return err
	}
	if snap.Pending != nil {
		return errors.New("nearest membership: finish the pending change first")
	}
	if device == snap.Current.Authority {
		return errors.New("nearest membership: replace the authority before removing it")
	}
	change, err := membership.NewRosterChange(snap.Current, newRequestID(), "remove", device)
	if err != nil {
		return err
	}
	return n.startOperation("remove", func(ctx context.Context, dir directory) error {
		if reachable(snap.Current, n.membership.deviceIDValue(), dir) <= len(snap.Current.Members)/2 {
			return membership.ErrDraining
		}
		return n.runTransition(ctx, change, dir)
	})
}

// RetryPending resumes the exact pending change recorded on this device.
func (n *Nearest) RetryPending(transaction string) error {
	store := n.membership.rosterStore()
	if store == nil {
		return membership.ErrUninitialized
	}
	snap := store.Snapshot()
	if snap.Pending == nil || snap.Pending.Transaction != transaction {
		return ErrStale
	}
	change := *snap.Pending
	return n.startOperation("retry", func(ctx context.Context, dir directory) error {
		return n.runTransition(ctx, change, dir)
	})
}

// KeepCluster retires every other cluster in sight by a terminal transition approved by a strict
// majority of that cluster's own parent roster. If a losing cluster lacks a reachable majority the
// operation reports exactly who is missing and changes nothing for that cluster.
func (n *Nearest) KeepCluster(keep string) error {
	if keep == "" {
		return ErrStale
	}
	return n.startOperation("keep_cluster", func(ctx context.Context, dir directory) error {
		return n.retireOthers(ctx, keep, dir)
	})
}

func (n *Nearest) retireOthers(ctx context.Context, keep string, dir directory) error {
	rt := n.membership
	losers := map[string][]directoryEntry{}
	for _, e := range dir {
		if e.info.Cluster != "" && e.info.Cluster != keep && !e.info.Retired {
			losers[e.info.Cluster] = append(losers[e.info.Cluster], e)
		}
	}
	if snap := rt.rosterStore(); snap != nil {
		if cur := snap.Snapshot().Current; cur.Cluster != "" && cur.Cluster != keep && !snap.Retired() {
			info := n.membershipInfo()
			losers[cur.Cluster] = append(losers[cur.Cluster], directoryEntry{info: info})
		}
	}
	if len(losers) == 0 {
		return errors.New("nearest membership: no other cluster is in sight")
	}
	var failures []string
	for cluster, entries := range losers {
		sample := entries[0].info
		for _, e := range entries {
			if e.info.Epoch > sample.Epoch {
				sample = e.info
			}
		}
		parent := membership.Roster{Cluster: cluster, Epoch: sample.Epoch, Authority: sample.Authority, Members: sample.Members}
		change, err := membership.NewRosterChange(parent, fmt.Sprintf("retire:%s:%d", cluster, sample.Epoch), "retire", "")
		if err != nil {
			failures = append(failures, cluster+": "+err.Error())
			continue
		}
		parts := n.participants(change, dir)
		var missing []string
		for _, id := range parent.Members {
			if parts[id] == nil {
				missing = append(missing, id)
			}
		}
		if len(parent.Members)-len(missing) <= len(parent.Members)/2 {
			failures = append(failures, fmt.Sprintf("cluster %s has no reachable majority; missing %s. Power off or isolate those devices, then retry", cluster, strings.Join(missing, ", ")))
			continue
		}
		decision, err := membership.CollectRosterDecision(ctx, change, parts)
		if err != nil {
			failures = append(failures, cluster+": "+err.Error())
			continue
		}
		if err := membership.DistributeRosterDecision(ctx, decision, parts); err != nil {
			// Majority installed or not, the missing members catch up from history when they return;
			// the conflict stays until retirement is observed on a majority.
			slog.Info("nearest membership retirement not yet installed everywhere", "cluster", cluster, "err", err)
		}
	}
	if len(failures) > 0 {
		return errors.New(strings.Join(failures, "; "))
	}
	return nil
}

// ReEnrol lets a retired device join the retained cluster as a new device. The retired roster and its
// journal are kept under another name so a restart cannot resurrect the old cluster.
func (n *Nearest) ReEnrol() error { return n.reenrolChecked() }

// publishMembershipStatus mirrors the setup page's summary into one Home Assistant diagnostic entity.
func (n *Nearest) publishMembershipStatus() {
	if n.membershipStatus == nil {
		return
	}
	v := n.MembershipView()
	text := strings.ReplaceAll(v.Lifecycle, "_", " ")
	if v.Own.ID != "" {
		text += fmt.Sprintf(": epoch %d, %d of %d reachable (%d needed)", v.Own.Epoch, v.Own.Reachable, len(v.Own.Members), v.Own.Majority)
	}
	if v.Blocked != "" {
		text += "; " + v.Blocked
	}
	if v.NotAnswering != "" {
		text += "; not answering: " + v.NotAnswering
	}
	if len(text) > 250 {
		text = text[:250]
	}
	n.membershipStatus.Set(text)
}
