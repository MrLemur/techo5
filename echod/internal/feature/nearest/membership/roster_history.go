package membership

import (
	"encoding/json"
	"errors"
	"slices"
	"time"
)

// ErrBehind means a decision's parent is newer than this store's roster: the
// caller must install the missing earlier history first.
var ErrBehind = errors.New("membership: earlier history is missing")

// RosterSnapshot is a detached view of everything a status endpoint or the UI
// needs. Prepared is set on a joining device that has no committed roster yet.
type RosterSnapshot struct {
	Current   Roster
	Pending   *RosterChange
	Prepared  *RosterChange
	Drained   bool
	Decision  *RosterDecision
	Outbox    []string
	Uncertain bool
}

// Snapshot returns detached durable state.
func (s *RosterStore) Snapshot() RosterSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := RosterSnapshot{Current: s.state.Current, Drained: s.state.Drained,
		Outbox: slices.Clone(s.state.Outbox), Uncertain: s.uncertain}
	snap.Current.Members = slices.Clone(snap.Current.Members)
	if s.state.Pending != nil {
		c := cloneRosterChange(*s.state.Pending)
		snap.Pending = &c
	}
	if s.state.Prepared != nil {
		c := cloneRosterChange(*s.state.Prepared)
		snap.Prepared = &c
	}
	if s.state.Decision != nil {
		d := cloneDecision(*s.state.Decision)
		snap.Decision = &d
	}
	return snap
}

func cloneDecision(d RosterDecision) RosterDecision {
	raw, _ := json.Marshal(d)
	var out RosterDecision
	_ = json.Unmarshal(raw, &out)
	return out
}

// SetTiming replaces the monotonic clock and drain bound. The runtime derives
// the bound from the protocol's maximum vote grant; tests inject fake time.
func (s *RosterStore) SetTiming(now func() time.Time, drainFor time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if now != nil {
		s.now = now
		if s.openedDraining {
			s.drainFrom = now()
		}
	}
	if drainFor > 0 {
		s.drainFor = drainFor
	}
}

// SetWriter replaces the atomic writer, for fault injection.
func (s *RosterStore) SetWriter(write func(string, []byte) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.write = write
}

// Delivered removes one transaction from the retry outbox. History is kept.
func (s *RosterStore) Delivered(transaction string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	before := len(s.state.Outbox)
	s.state.Outbox = slices.DeleteFunc(s.state.Outbox, func(t string) bool { return t == transaction })
	if len(s.state.Outbox) == before {
		return nil
	}
	return s.persist()
}

// DecisionFor returns a retained committed decision by transaction.
func (s *RosterStore) DecisionFor(transaction string) (RosterDecision, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range s.state.Journal {
		if d.Change.Transaction == transaction {
			return cloneDecision(d), true
		}
	}
	return RosterDecision{}, false
}

// HistoryAfter returns at most limit retained decisions whose parent epoch is
// at least after, in order, and whether more remain.
func (s *RosterStore) HistoryAfter(after uint64, limit int) ([]RosterDecision, bool, error) {
	journal, err := s.History()
	if err != nil {
		return nil, false, err
	}
	var out []RosterDecision
	for _, d := range journal {
		if d.Change.Parent.Epoch >= after {
			out = append(out, d)
		}
	}
	if limit > 0 && len(out) > limit {
		return out[:limit], true, nil
	}
	return out, false, nil
}

// Install applies one retained decision on any kind of device: a prepared
// joiner, an old member that did not acknowledge, or one that already has it.
// A member fences first, so a returning device stops voting before the new
// roster takes effect. ErrDraining means retry after the drain interval.
func (s *RosterStore) Install(decision RosterDecision) error {
	if !validRosterDecision(decision) {
		return ErrCorrupt
	}
	s.mu.Lock()
	for _, d := range s.state.Journal {
		if d.Change.Transaction == decision.Change.Transaction {
			same := sameRosterChange(d.Change, decision.Change)
			s.mu.Unlock()
			if !same {
				return ErrConflict
			}
			return nil
		}
	}
	prepared := s.state.Prepared != nil
	current := s.state.Current
	s.mu.Unlock()
	if prepared {
		return s.CommitPrepared(decision)
	}
	switch {
	case equalRoster(current, decision.Change.Parent):
	case current.Cluster == decision.Change.Parent.Cluster && current.Epoch < decision.Change.Parent.Epoch:
		return ErrBehind
	default:
		return ErrConflict
	}
	if err := s.Fence(decision.Change); err != nil {
		return err
	}
	if err := s.FinishDrain(); err != nil {
		return err
	}
	return s.Commit(decision.Change, decision.Acknowledgments, decision.JoiningDevice)
}
