package membership

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
)

// RosterParticipant is one device taking part in a membership round. It closes its election and wake
// gates before it acknowledges a fence.
type RosterParticipant interface {
	PrepareJoin(context.Context, RosterChange) error
	FenceAndDrain(context.Context, RosterChange) (RosterAck, error)
	CommitRoster(context.Context, RosterDecision) error
}

// RosterDecision is the plain-text result of a membership round.
type RosterDecision struct {
	Change          RosterChange `json:"change"`
	Acknowledgments []RosterAck  `json:"acknowledgments"`
	JoiningDevice   string       `json:"joining_device,omitempty"`
}

func validRosterDecision(decision RosterDecision) bool {
	change := decision.Change
	if !validRosterChange(change) {
		return false
	}
	if change.Kind == "add" {
		if decision.JoiningDevice == "" || decision.JoiningDevice != normalName(decision.JoiningDevice) ||
			slices.Contains(change.Parent.Members, decision.JoiningDevice) ||
			!slices.Contains(change.Next.Members, decision.JoiningDevice) {
			return false
		}
	} else if decision.JoiningDevice != "" {
		return false
	}
	seen := make(map[string]bool, len(decision.Acknowledgments))
	for _, ack := range decision.Acknowledgments {
		if ack.Voter == "" || ack.Voter != normalName(ack.Voter) ||
			!slices.Contains(change.Parent.Members, ack.Voter) || seen[ack.Voter] ||
			!sameRosterChange(ack.Change, change) {
			return false
		}
		seen[ack.Voter] = true
	}
	return len(seen) > len(change.Parent.Members)/2
}

// SaveDecision persists the exact collected decision before distributing it.
// Once saved it must be retried unchanged after a persistence error.
func (s *RosterStore) SaveDecision(decision RosterDecision) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validRosterDecision(decision) {
		return ErrCorrupt
	}
	if s.state.Decision != nil {
		old, _ := digest(s.state.Decision)
		next, _ := digest(decision)
		if old != next {
			return ErrConflict
		}
		return s.persist()
	}
	if s.uncertain {
		return ErrUncertain
	}
	if s.state.Pending == nil || !s.state.Drained || !sameRosterChange(*s.state.Pending, decision.Change) {
		return ErrConflict
	}
	raw, err := json.Marshal(decision)
	if err != nil {
		return err
	}
	var copyDecision RosterDecision
	if err := json.Unmarshal(raw, &copyDecision); err != nil {
		return err
	}
	s.state.Decision = &copyDecision
	return s.persist()
}

// SavedDecision returns a detached copy for retrying distribution after restart.
func (s *RosterStore) SavedDecision() (*RosterDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain {
		return nil, ErrUncertain
	}
	if s.state.Decision == nil {
		return nil, nil
	}
	raw, err := json.Marshal(s.state.Decision)
	if err != nil {
		return nil, err
	}
	var decision RosterDecision
	if err := json.Unmarshal(raw, &decision); err != nil {
		return nil, err
	}
	return &decision, nil
}

// ClearDecision acknowledges local distribution. It never removes history:
// offline members require that parent-linked journal for catch-up.
func (s *RosterStore) ClearDecision() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Outbox = nil
	return s.persist()
}

// History returns a detached parent-linked transition journal. Entries remain
// available after delivery so an offline member cannot be stranded.
func (s *RosterStore) History() ([]RosterDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain {
		return nil, ErrUncertain
	}
	raw, err := json.Marshal(s.state.Journal)
	if err != nil {
		return nil, err
	}
	var journal []RosterDecision
	if err := json.Unmarshal(raw, &journal); err != nil {
		return nil, err
	}
	return journal, nil
}

// DistributeRosterDecision retries a retained decision on all old and new
// members. Missing devices are reported so callers know to keep the decision.
func DistributeRosterDecision(ctx context.Context, decision RosterDecision, participants map[string]RosterParticipant) error {
	if !validRosterDecision(decision) {
		return ErrCorrupt
	}
	names := append(slices.Clone(decision.Change.Parent.Members), decision.Change.Next.Members...)
	slices.Sort(names)
	names = slices.Compact(names)
	errs := make(chan error, len(names))
	for _, name := range names {
		peer := participants[name]
		if peer == nil {
			errs <- errors.New("membership: device unavailable")
			continue
		}
		go func() { errs <- peer.CommitRoster(ctx, decision) }()
	}
	var firstErr error
	for range names {
		if err := <-errs; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// CollectRosterDecision asks the joining device to prepare, then asks each old
// member to fence and drain. Participants are the devices actually contacted,
// never names taken from a message. A failure can leave members fenced, so the same change is retried.
func CollectRosterDecision(ctx context.Context, change RosterChange, participants map[string]RosterParticipant) (RosterDecision, error) {
	if !validRosterChange(change) {
		return RosterDecision{}, ErrCorrupt
	}
	decision := RosterDecision{Change: cloneRosterChange(change)}
	if change.Kind == "add" {
		for _, name := range change.Next.Members {
			if !slices.Contains(change.Parent.Members, name) {
				decision.JoiningDevice = name
				break
			}
		}
		peer := participants[decision.JoiningDevice]
		if peer == nil {
			return RosterDecision{}, errors.New("membership: joining device unavailable")
		}
		if err := peer.PrepareJoin(ctx, change); err != nil {
			return RosterDecision{}, err
		}
	}

	type result struct {
		name string
		ack  RosterAck
		err  error
	}
	results := make(chan result, len(change.Parent.Members))
	waiting := 0
	for _, name := range change.Parent.Members {
		peer := participants[name]
		if peer == nil {
			continue
		}
		waiting++
		go func(name string, peer RosterParticipant) {
			ack, err := peer.FenceAndDrain(ctx, change)
			results <- result{name: name, ack: ack, err: err}
		}(name, peer)
	}
	// A majority is enough: a member that has not acknowledged yet fences and drains itself when it
	// installs the decision, so waiting for slow or vanished members only delays the round.
	for range waiting {
		if len(decision.Acknowledgments) > len(change.Parent.Members)/2 {
			break
		}
		select {
		case <-ctx.Done():
			return RosterDecision{}, ctx.Err()
		case result := <-results:
			if result.err == nil && result.ack.Voter == result.name && sameRosterChange(result.ack.Change, change) {
				decision.Acknowledgments = append(decision.Acknowledgments, result.ack)
			}
		}
	}
	if len(decision.Acknowledgments) <= len(change.Parent.Members)/2 {
		return RosterDecision{}, ErrDraining
	}
	slices.SortFunc(decision.Acknowledgments, func(a, b RosterAck) int {
		return strings.Compare(a.Voter, b.Voter)
	})
	return decision, nil
}
