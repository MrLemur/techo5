package membership

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

// Roster is the durable membership used as the election quorum denominator.
// Names are normalised and sorted, and an offline member remains a member.
type Roster struct {
	Cluster   string   `json:"cluster"`
	Epoch     uint64   `json:"epoch"`
	Authority string   `json:"authority"`
	Members   []string `json:"members"`
}

// RosterChange identifies one exact successor of a committed roster.
type RosterChange struct {
	Transaction string `json:"transaction"`
	Kind        string `json:"kind"`
	Parent      Roster `json:"parent"`
	Next        Roster `json:"next"`
}

type rosterDisk struct {
	Version  int              `json:"version"`
	Current  Roster           `json:"current"`
	Pending  *RosterChange    `json:"pending,omitempty"`
	Prepared *RosterChange    `json:"prepared,omitempty"`
	Drained  bool             `json:"drained,omitempty"`
	Decision *RosterDecision  `json:"decision,omitempty"`
	Journal  []RosterDecision `json:"journal,omitempty"`
	Outbox   []string         `json:"outbox,omitempty"`
}

// RosterAck is a plain-text protocol acknowledgement from one old member.
// The transport must associate each response with the peer it contacted;
// names in an arbitrary incoming message are not evidence of a response.
type RosterAck struct {
	Change RosterChange `json:"change"`
	Voter  string       `json:"voter"`
}

// RosterStore persists membership, an accepted successor and an ordered
// transition journal. It does not send messages or control election leases:
// callers must close the runtime gate before Fence and keep it closed until a
// successor has been committed and old leases have been discarded.
type RosterStore struct {
	mu        sync.Mutex
	path      string
	state     rosterDisk
	uncertain bool
	drainFrom time.Time
	// openedDraining marks a drain interval started by opening the store, so injecting a clock restarts it
	// on that clock rather than comparing two clocks.
	openedDraining bool
	now            func() time.Time
	drainFor       time.Duration
	write          func(string, []byte) error
}

func normalName(name string) string { return strings.ToLower(strings.TrimSpace(name)) }

func validRoster(r Roster) bool {
	if r.Cluster == "" || r.Epoch == 0 || len(r.Members) == 0 ||
		r.Authority == "" || r.Authority != normalName(r.Authority) {
		return false
	}
	last := ""
	for _, name := range r.Members {
		if name == "" || name != normalName(name) || name <= last {
			return false
		}
		last = name
	}
	return slices.Contains(r.Members, r.Authority)
}

func equalRoster(a, b Roster) bool {
	return a.Cluster == b.Cluster && a.Epoch == b.Epoch &&
		a.Authority == b.Authority && slices.Equal(a.Members, b.Members)
}

// Fingerprint identifies one roster, so a message can say which roster it was sent under. It is not a
// signature.
func Fingerprint(roster Roster) (string, error) {
	if !validRoster(roster) {
		return "", ErrCorrupt
	}
	raw, err := json.Marshal(roster)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func validRosterChange(change RosterChange) bool {
	old, next := change.Parent, change.Next
	if change.Transaction == "" || !validRoster(old) || !validRoster(next) ||
		old.Cluster != next.Cluster || old.Epoch == ^uint64(0) || next.Epoch != old.Epoch+1 {
		return false
	}
	switch change.Kind {
	case "add":
		if next.Authority != old.Authority || len(next.Members) != len(old.Members)+1 {
			return false
		}
		for _, name := range old.Members {
			if !slices.Contains(next.Members, name) {
				return false
			}
		}
	case "remove":
		if next.Authority != old.Authority || len(next.Members)+1 != len(old.Members) {
			return false
		}
		for _, name := range next.Members {
			if !slices.Contains(old.Members, name) {
				return false
			}
		}
	case "replace_authority":
		if !slices.Equal(old.Members, next.Members) || old.Authority == next.Authority {
			return false
		}
	case "retire":
		// Terminal: the cluster keeps its members and authority and is closed for good.
		if !slices.Equal(old.Members, next.Members) || old.Authority != next.Authority {
			return false
		}
	default:
		return false
	}
	return true
}

func cloneRosterChange(change RosterChange) RosterChange {
	change.Parent.Members = slices.Clone(change.Parent.Members)
	change.Next.Members = slices.Clone(change.Next.Members)
	return change
}

func sameRosterChange(a, b RosterChange) bool {
	return a.Transaction == b.Transaction && a.Kind == b.Kind &&
		equalRoster(a.Parent, b.Parent) && equalRoster(a.Next, b.Next)
}

// NewRosterChange builds one successor without modifying the committed roster.
// The caller must use the same transaction and successor when retrying.
func NewRosterChange(parent Roster, transaction, kind, device string) (RosterChange, error) {
	if !validRoster(parent) || parent.Epoch == ^uint64(0) || transaction == "" {
		return RosterChange{}, ErrCorrupt
	}
	device = normalName(device)
	if device == "" && kind != "retire" {
		return RosterChange{}, ErrCorrupt
	}
	parent.Members = slices.Clone(parent.Members)
	next := parent
	next.Members = slices.Clone(parent.Members)
	next.Epoch++
	switch kind {
	case "add":
		if slices.Contains(next.Members, device) {
			return RosterChange{}, ErrConflict
		}
		next.Members = append(next.Members, device)
		slices.Sort(next.Members)
	case "remove":
		if !slices.Contains(next.Members, device) || device == next.Authority {
			return RosterChange{}, ErrConflict
		}
		next.Members = slices.DeleteFunc(next.Members, func(name string) bool { return name == device })
	case "replace_authority":
		if !slices.Contains(next.Members, device) || next.Authority == device {
			return RosterChange{}, ErrConflict
		}
		next.Authority = device
	case "retire":
	default:
		return RosterChange{}, ErrCorrupt
	}
	change := RosterChange{Transaction: transaction, Kind: kind, Parent: parent, Next: next}
	if !validRosterChange(change) {
		return RosterChange{}, ErrCorrupt
	}
	return change, nil
}

// CreateRoster creates a new roster without overwriting existing state.
// Bootstrap policy and cluster-ID generation belong to the caller.
func CreateRoster(path string, initial Roster) (*RosterStore, error) {
	if !validRoster(initial) || initial.Epoch != 1 {
		return nil, ErrCorrupt
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	initial.Members = slices.Clone(initial.Members)
	s := &RosterStore{path: path, state: rosterDisk{Version: 2, Current: initial}, now: time.Now, drainFor: 5 * time.Second, write: writeAtomic}
	if err := s.persist(); err != nil {
		return nil, err
	}
	return s, nil
}

// OpenRoster refuses missing or invalid state; it never bootstraps on error.
func OpenRoster(path string) (*RosterStore, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrUninitialized
	}
	if err != nil {
		return nil, err
	}
	var st rosterDisk
	if json.Unmarshal(data, &st) != nil || st.Version != 2 ||
		(!validRoster(st.Current) && (st.Prepared == nil || !validRosterChange(*st.Prepared))) ||
		(st.Prepared != nil && (validRoster(st.Current) || !validRosterChange(*st.Prepared))) ||
		(st.Pending == nil && st.Drained) ||
		(st.Pending != nil && (!validRosterChange(*st.Pending) || !equalRoster(st.Pending.Parent, st.Current))) ||
		(st.Decision != nil && (st.Pending == nil || !validRosterDecision(*st.Decision) ||
			!sameRosterChange(st.Decision.Change, *st.Pending))) {
		return nil, ErrCorrupt
	}
	if !validRoster(st.Current) && (st.Pending != nil || st.Decision != nil || len(st.Journal) != 0 || len(st.Outbox) != 0) {
		return nil, ErrCorrupt
	}
	seen := make(map[string]bool, len(st.Journal))
	var parent *Roster
	for i := range st.Journal {
		decision := st.Journal[i]
		if !validRosterDecision(decision) || seen[decision.Change.Transaction] ||
			(i > 0 && !equalRoster(*parent, decision.Change.Parent)) {
			return nil, ErrCorrupt
		}
		seen[decision.Change.Transaction] = true
		next := decision.Change.Next
		parent = &next
	}
	if len(st.Journal) > 0 && !equalRoster(st.Journal[len(st.Journal)-1].Change.Next, st.Current) {
		return nil, ErrCorrupt
	}
	for _, transaction := range st.Outbox {
		if transaction == "" || !seen[transaction] {
			return nil, ErrCorrupt
		}
	}
	s := &RosterStore{path: path, state: st, now: time.Now, drainFor: 5 * time.Second, write: writeAtomic}
	if st.Pending != nil {
		// A restart cannot prove that old process activity drained, even if an earlier run did:
		// the drain interval starts again before any acknowledgement or commit.
		s.state.Drained = false
		s.drainFrom = s.now()
		s.openedDraining = true
	}
	return s, nil
}

// PrepareRoster durably records a joining device's exact intended successor.
// It remains unable to vote or release wakes until CommitPrepared installs the
// decision. Repeating the same preparation is idempotent.
func PrepareRoster(path string, change RosterChange) (*RosterStore, error) {
	if !validRosterChange(change) || change.Kind != "add" {
		return nil, ErrCorrupt
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if _, err := os.Stat(path); err == nil {
		store, openErr := OpenRoster(path)
		if openErr != nil {
			return nil, openErr
		}
		if store.state.Prepared == nil || !sameRosterChange(*store.state.Prepared, change) {
			return nil, ErrConflict
		}
		return store, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	copyChange := cloneRosterChange(change)
	store := &RosterStore{path: path, state: rosterDisk{Version: 2, Prepared: &copyChange}, now: time.Now, drainFor: 5 * time.Second, write: writeAtomic}
	if err := store.persist(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *RosterStore) persist() error {
	data, err := json.Marshal(s.state)
	if err == nil {
		write := s.write
		if write == nil {
			write = writeAtomic
		}
		err = write(s.path, data)
	}
	s.uncertain = err != nil
	return err
}

// View returns detached state. A fenced or uncertain store cannot authorise
// elections or release wakes.
func (s *RosterStore) View() (Roster, *RosterChange, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.state.Current
	current.Members = slices.Clone(current.Members)
	var pending *RosterChange
	if s.state.Pending != nil {
		copyChange := cloneRosterChange(*s.state.Pending)
		pending = &copyChange
	}
	return current, pending, s.uncertain
}

// Fence records one exact successor. The caller must atomically close its
// election and wake gate before this call, and keep it closed on every error.
func (s *RosterStore) Fence(change RosterChange) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.Decision != nil && !sameRosterChange(s.state.Decision.Change, change) {
		return ErrConflict
	}
	if !validRosterChange(change) || !equalRoster(s.state.Current, change.Parent) {
		return ErrCorrupt
	}
	if s.retiredLocked() {
		return ErrConflict
	}
	if s.state.Pending != nil {
		if !sameRosterChange(*s.state.Pending, change) {
			return ErrConflict
		}
	} else {
		if s.uncertain {
			return ErrUncertain
		}
		copyChange := cloneRosterChange(change)
		s.state.Pending = &copyChange
		s.drainFrom, s.openedDraining = s.clock(), false
	}
	return s.persist()
}

// FinishDrain records that the old runtime has remained gated for the full
// lease-drain interval. The caller must first stop all in-flight releases.
func (s *RosterStore) FinishDrain() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.Pending == nil {
		return ErrUninitialized
	}
	if !s.state.Drained && (s.drainFrom.IsZero() || s.clock().Sub(s.drainFrom) < s.drainDuration()) {
		return ErrDraining
	}
	s.state.Drained = true
	return s.persist()
}

func (s *RosterStore) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *RosterStore) drainDuration() time.Duration {
	if s.drainFor > 0 {
		return s.drainFor
	}
	return 5 * time.Second
}

// Drained reports whether this device can acknowledge its persisted fence.
func (s *RosterStore) Drained(change RosterChange) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.uncertain && s.state.Drained && s.state.Pending != nil &&
		sameRosterChange(*s.state.Pending, change)
}

// Acknowledge returns this member's acknowledgement only after its fence has
// been persisted and its old activity has drained. The caller supplies the
// local device name, not a name taken from the requesting peer.
func (s *RosterStore) Acknowledge(change RosterChange, localName string) (RosterAck, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	localName = normalName(localName)
	if s.uncertain || !s.state.Drained || s.state.Pending == nil ||
		!sameRosterChange(*s.state.Pending, change) {
		return RosterAck{}, ErrDraining
	}
	if !slices.Contains(change.Parent.Members, localName) {
		return RosterAck{}, ErrCorrupt
	}
	return RosterAck{Change: cloneRosterChange(change), Voter: localName}, nil
}

// Commit installs a fenced successor after receiving acknowledgements from a
// strict majority of the old roster. For an addition, joiningDevice must be
// the new member and the caller must have received that device's readiness.
// The caller matches each acknowledgement to the device it asked, and keeps the election and wake gates
// closed until the old leases are gone.
func (s *RosterStore) Commit(change RosterChange, acknowledgements []RosterAck, joiningDevice string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.state.Journal) > 0 && sameRosterChange(s.state.Journal[len(s.state.Journal)-1].Change, change) &&
		equalRoster(s.state.Current, change.Next) {
		return s.persist()
	}
	if s.uncertain {
		return ErrUncertain
	}
	if s.state.Decision != nil {
		expected, _ := digest(*s.state.Decision)
		got, _ := digest(RosterDecision{Change: change, Acknowledgments: acknowledgements, JoiningDevice: joiningDevice})
		if expected != got {
			return ErrConflict
		}
	}
	if !validRosterChange(change) || s.state.Pending == nil || !s.state.Drained ||
		!sameRosterChange(*s.state.Pending, change) || !equalRoster(s.state.Current, change.Parent) {
		return ErrConflict
	}
	joiningDevice = normalName(joiningDevice)
	if change.Kind == "add" {
		if joiningDevice == "" || slices.Contains(change.Parent.Members, joiningDevice) ||
			!slices.Contains(change.Next.Members, joiningDevice) {
			return ErrCorrupt
		}
	} else if joiningDevice != "" {
		return ErrCorrupt
	}
	voters := make(map[string]bool, len(acknowledgements))
	for _, ack := range acknowledgements {
		if ack.Voter == "" || ack.Voter != normalName(ack.Voter) ||
			!slices.Contains(change.Parent.Members, ack.Voter) || voters[ack.Voter] ||
			!sameRosterChange(change, ack.Change) {
			return ErrCorrupt
		}
		voters[ack.Voter] = true
	}
	if len(voters) <= len(change.Parent.Members)/2 {
		return ErrDraining
	}
	copyChange := cloneRosterChange(change)
	s.state.Current = copyChange.Next
	s.state.Journal = append(s.state.Journal, RosterDecision{
		Change:          copyChange,
		Acknowledgments: slices.Clone(acknowledgements),
		JoiningDevice:   joiningDevice,
	})
	s.state.Outbox = append(s.state.Outbox, change.Transaction)
	s.state.Pending = nil
	s.state.Decision = nil
	s.state.Drained = false
	s.drainFrom, s.openedDraining = time.Time{}, false
	return s.persist()
}

// CommitPrepared installs an already prepared joining device after validating
// the exact majority decision. A prepared device was not a parent voter.
func (s *RosterStore) CommitPrepared(decision RosterDecision) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validRosterDecision(decision) || s.uncertain || s.state.Prepared == nil ||
		!sameRosterChange(*s.state.Prepared, decision.Change) || decision.Change.Kind != "add" {
		return ErrConflict
	}
	s.state.Current = cloneRosterChange(decision.Change).Next
	s.state.Journal = append(s.state.Journal, decision)
	s.state.Prepared = nil
	s.state.Pending = nil
	s.state.Decision = nil
	s.state.Drained = false
	s.drainFrom, s.openedDraining = time.Time{}, false
	return s.persist()
}

func (s *RosterStore) retiredLocked() bool {
	n := len(s.state.Journal)
	return n > 0 && s.state.Journal[n-1].Change.Kind == "retire"
}

// Retired reports whether the last committed transition retired the cluster. It is derived from the
// retained journal, so a restart cannot resurrect a retired cluster.
func (s *RosterStore) Retired() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.retiredLocked()
}
