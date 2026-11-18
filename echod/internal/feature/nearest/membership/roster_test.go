package membership

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestPreparedJoinInstallsOnlyItsExactDecision(t *testing.T) {
	parent := Roster{Cluster: "cluster", Epoch: 1, Authority: "a", Members: []string{"a"}}
	change, err := NewRosterChange(parent, "transaction", "add", "b")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "roster.json")
	store, err := PrepareRoster(path, change)
	if err != nil {
		t.Fatal(err)
	}
	decision := RosterDecision{Change: change, JoiningDevice: "b", Acknowledgments: []RosterAck{{Change: change, Voter: "a"}}}
	if err := store.CommitPrepared(decision); err != nil {
		t.Fatal(err)
	}
	current, pending, uncertain := store.View()
	if uncertain || pending != nil || current.Epoch != 2 || len(current.Members) != 2 {
		t.Fatalf("installed=%+v pending=%v uncertain=%v", current, pending, uncertain)
	}
}

func TestFenceUsesInjectedMonotonicClock(t *testing.T) {
	now := time.Unix(1, 0)
	parent := Roster{Cluster: "cluster", Epoch: 1, Authority: "a", Members: []string{"a"}}
	path := filepath.Join(t.TempDir(), "roster.json")
	store, err := CreateRoster(path, parent)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	store.drainFor = time.Second
	change, err := NewRosterChange(parent, "transaction", "add", "b")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Fence(change); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishDrain(); !errors.Is(err, ErrDraining) {
		t.Fatalf("FinishDrain error = %v, want draining", err)
	}
	now = now.Add(time.Second)
	if err := store.FinishDrain(); err != nil {
		t.Fatal(err)
	}
}

func TestCommittedDecisionRemainsInHistoryAfterDelivery(t *testing.T) {
	now := time.Unix(1, 0)
	parent := Roster{Cluster: "cluster", Epoch: 1, Authority: "a", Members: []string{"a"}}
	store, err := CreateRoster(filepath.Join(t.TempDir(), "roster.json"), parent)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	store.drainFor = time.Second
	change, err := NewRosterChange(parent, "transaction", "add", "b")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Fence(change); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := store.FinishDrain(); err != nil {
		t.Fatal(err)
	}
	decision := RosterDecision{Change: change, JoiningDevice: "b", Acknowledgments: []RosterAck{{Change: change, Voter: "a"}}}
	if err := store.SaveDecision(decision); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(change, decision.Acknowledgments, "b"); err != nil {
		t.Fatal(err)
	}
	if err := store.ClearDecision(); err != nil {
		t.Fatal(err)
	}
	history, err := store.History()
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Change.Transaction != "transaction" {
		t.Fatalf("history = %#v", history)
	}
}
