package membership

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func newTestStore(t *testing.T, members ...string) (*RosterStore, string, *time.Time) {
	t.Helper()
	now := time.Unix(100, 0)
	path := filepath.Join(t.TempDir(), "roster.json")
	store, err := CreateRoster(path, Roster{Cluster: "c", Epoch: 1, Authority: members[0], Members: members})
	if err != nil {
		t.Fatal(err)
	}
	store.SetTiming(func() time.Time { return now }, time.Second)
	return store, path, &now
}

func reopen(t *testing.T, path string, now *time.Time) *RosterStore {
	t.Helper()
	store, err := OpenRoster(path)
	if err != nil {
		t.Fatal(err)
	}
	store.SetTiming(func() time.Time { return *now }, time.Second)
	return store
}

// A failed write leaves the store uncertain and gated; retrying the same change succeeds, and a different
// one is refused meanwhile. Durability is never claimed for a write that failed.
func TestFailedWriteIsUncertainAndRetriedExactly(t *testing.T) {
	store, _, _ := newTestStore(t, "a", "b", "c")
	current, _, _ := store.View()
	change, _ := NewRosterChange(current, "t1", "add", "d")
	other, _ := NewRosterChange(current, "t2", "add", "e")
	store.SetWriter(func(string, []byte) error { return errors.New("disk full") })
	if err := store.Fence(change); err == nil {
		t.Fatal("fence reported success on a failed write")
	}
	if _, _, uncertain := store.View(); !uncertain {
		t.Fatal("failed write not reported as uncertain")
	}
	if err := store.Fence(other); !errors.Is(err, ErrConflict) {
		t.Fatalf("a different successor while uncertain = %v, want ErrConflict", err)
	}
	if store.Drained(change) {
		t.Fatal("uncertain store claims a drained fence")
	}
	store.SetWriter(nil)
	if err := store.Fence(change); err != nil {
		t.Fatalf("retrying the same fence: %v", err)
	}
	if _, pending, uncertain := store.View(); uncertain || pending == nil || pending.Transaction != "t1" {
		t.Fatalf("after retry pending=%v uncertain=%v", pending, uncertain)
	}
}

// Every persistence boundary survives a restart: the fence stays fenced and conservatively restarts its
// drain, a saved decision is retained for redistribution, and a commit stays in history and the outbox.
func TestRestartAtEachWriteBoundary(t *testing.T) {
	store, path, now := newTestStore(t, "a", "b")
	current, _, _ := store.View()
	change, _ := NewRosterChange(current, "t1", "add", "c")

	if err := store.Fence(change); err != nil {
		t.Fatal(err)
	}
	store = reopen(t, path, now)
	if _, pending, _ := store.View(); pending == nil || pending.Transaction != "t1" {
		t.Fatal("fence lost across restart")
	}
	*now = now.Add(500 * time.Millisecond)
	if err := store.FinishDrain(); !errors.Is(err, ErrDraining) {
		t.Fatalf("drain finished early: %v", err)
	}
	*now = now.Add(time.Second)
	if err := store.FinishDrain(); err != nil {
		t.Fatal(err)
	}
	store = reopen(t, path, now)
	if store.Drained(change) {
		t.Fatal("a restart must not trust an earlier drain: old activity may be unaccounted for")
	}
	*now = now.Add(time.Second)
	if err := store.FinishDrain(); err != nil {
		t.Fatal(err)
	}

	decision := decide(t, change, "a", "b")
	if err := store.SaveDecision(decision); err != nil {
		t.Fatal(err)
	}
	store = reopen(t, path, now)
	saved, err := store.SavedDecision()
	if err != nil || saved == nil || saved.Change.Transaction != "t1" {
		t.Fatalf("decision not retained: %v %v", saved, err)
	}
	other, _ := NewRosterChange(current, "t9", "add", "z")
	if err := store.Fence(other); !errors.Is(err, ErrConflict) {
		t.Fatalf("a different successor next to a saved decision = %v", err)
	}
	*now = now.Add(time.Second)
	if err := store.Install(*saved); err != nil {
		t.Fatal(err)
	}
	store = reopen(t, path, now)
	if got, _, _ := store.View(); got.Epoch != 2 || len(got.Members) != 3 {
		t.Fatalf("commit lost: %+v", got)
	}
	snap := store.Snapshot()
	if len(snap.Outbox) != 1 {
		t.Fatalf("outbox = %v, want the committed transaction", snap.Outbox)
	}
	if err := store.Delivered("t1"); err != nil {
		t.Fatal(err)
	}
	if history, _, _ := store.HistoryAfter(0, 8); len(history) != 1 {
		t.Fatal("delivery erased catch-up history")
	}
	store = reopen(t, path, now)
	if len(store.Snapshot().Outbox) != 0 {
		t.Fatal("delivered transaction came back after restart")
	}
}

// Repeating a fence is a retry, not a reset: it can neither restart the drain clock nor skip it.
func TestRepeatFenceDoesNotResetDrain(t *testing.T) {
	store, _, now := newTestStore(t, "a", "b")
	current, _, _ := store.View()
	change, _ := NewRosterChange(current, "t1", "add", "c")
	if err := store.Fence(change); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(600 * time.Millisecond)
	if err := store.Fence(change); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(400 * time.Millisecond)
	if err := store.FinishDrain(); err != nil {
		t.Fatalf("a repeated fence restarted the drain: %v", err)
	}
	if err := store.Fence(change); err != nil || !store.Drained(change) {
		t.Fatalf("a repeated fence after drain undrained the store: %v", err)
	}
}

// A prepared joiner is idempotent for its exact change, refuses a different one, survives a restart and
// has no committed roster to vote with.
func TestPreparedJoinerIdempotentAndCannotVote(t *testing.T) {
	parent := Roster{Cluster: "c", Epoch: 1, Authority: "a", Members: []string{"a", "b"}}
	change, _ := NewRosterChange(parent, "t1", "add", "c")
	other, _ := NewRosterChange(parent, "t2", "add", "c")
	path := filepath.Join(t.TempDir(), "roster.json")
	if _, err := PrepareRoster(path, change); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareRoster(path, change); err != nil {
		t.Fatalf("identical preparation not idempotent: %v", err)
	}
	if _, err := PrepareRoster(path, other); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting preparation = %v", err)
	}
	store, err := OpenRoster(path)
	if err != nil {
		t.Fatal(err)
	}
	if current, _, _ := store.View(); current.Cluster != "" {
		t.Fatal("a prepared joiner has a committed roster")
	}
	if _, err := store.Acknowledge(change, "c"); err == nil {
		t.Fatal("a prepared joiner acknowledged a fence as if it were a parent member")
	}
	if snap := store.Snapshot(); snap.Prepared == nil {
		t.Fatal("preparation lost across restart")
	}
}

// Once a member has accepted one successor of a parent it can accept no other, and an unknown or
// duplicated acknowledger never counts toward the majority.
func TestOneSuccessorPerParentAndNoForgedAcknowledgements(t *testing.T) {
	store, _, now := newTestStore(t, "a", "b", "c")
	current, _, _ := store.View()
	first, _ := NewRosterChange(current, "t1", "add", "d")
	second, _ := NewRosterChange(current, "t2", "remove", "c")
	if err := store.Fence(first); err != nil {
		t.Fatal(err)
	}
	if err := store.Fence(second); !errors.Is(err, ErrConflict) {
		t.Fatalf("second successor of one parent = %v", err)
	}
	*now = now.Add(time.Second)
	if err := store.FinishDrain(); err != nil {
		t.Fatal(err)
	}
	forged := decide(t, first, "a", "a", "stranger")
	if err := store.Install(forged); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("duplicate and unknown acknowledgers accepted: %v", err)
	}
	if err := store.Commit(first, forged.Acknowledgments, "d"); err == nil {
		t.Fatal("commit accepted a forged acknowledgement set")
	}
}

// A decision for a different cluster or a reused transaction with a different body is refused.
func TestInstallRefusesForeignClusterAndReusedTransaction(t *testing.T) {
	store, _, now := newTestStore(t, "a", "b")
	current, _, _ := store.View()
	change, _ := NewRosterChange(current, "t1", "add", "c")
	if err := store.Fence(change); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	if err := store.Install(decide(t, change, "a", "b")); err != nil {
		t.Fatal(err)
	}
	next, _, _ := store.View()
	reused, _ := NewRosterChange(next, "t1", "add", "d")
	if err := store.Install(decide(t, reused, "a", "b", "c")); !errors.Is(err, ErrConflict) {
		t.Fatalf("reused transaction = %v", err)
	}
	foreign := Roster{Cluster: "other", Epoch: 3, Authority: "x", Members: []string{"x", "y"}}
	fc, _ := NewRosterChange(foreign, "f1", "add", "z")
	if err := store.Install(decide(t, fc, "x", "y")); !errors.Is(err, ErrConflict) {
		t.Fatalf("foreign cluster decision = %v", err)
	}
}
