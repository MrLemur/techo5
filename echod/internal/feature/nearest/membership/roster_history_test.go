package membership

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func decide(t *testing.T, change RosterChange, voters ...string) RosterDecision {
	t.Helper()
	d := RosterDecision{Change: change}
	for _, v := range voters {
		d.Acknowledgments = append(d.Acknowledgments, RosterAck{Change: change, Voter: v})
	}
	if change.Kind == "add" {
		for _, m := range change.Next.Members {
			found := false
			for _, p := range change.Parent.Members {
				found = found || p == m
			}
			if !found {
				d.JoiningDevice = m
			}
		}
	}
	return d
}

// A device that was off for two epochs installs each retained successor in order, fencing and
// draining first; a later decision is refused until the earlier one is in place.
func TestReturningMemberCatchesUpInOrder(t *testing.T) {
	now := time.Unix(10, 0)
	e1 := Roster{Cluster: "c", Epoch: 1, Authority: "a", Members: []string{"a", "b", "c"}}
	path := filepath.Join(t.TempDir(), "roster.json")
	store, err := CreateRoster(path, e1)
	if err != nil {
		t.Fatal(err)
	}
	store.SetTiming(func() time.Time { return now }, time.Second)
	add, err := NewRosterChange(e1, "t1", "add", "d")
	if err != nil {
		t.Fatal(err)
	}
	d1 := decide(t, add, "a", "b")
	remove, err := NewRosterChange(add.Next, "t2", "remove", "b")
	if err != nil {
		t.Fatal(err)
	}
	d2 := decide(t, remove, "a", "c", "d")
	if err := store.Install(d2); !errors.Is(err, ErrBehind) {
		t.Fatalf("out-of-order install = %v, want ErrBehind", err)
	}
	if err := store.Install(d1); !errors.Is(err, ErrDraining) {
		t.Fatalf("install before drain = %v, want ErrDraining", err)
	}
	now = now.Add(time.Second)
	if err := store.Install(d1); err != nil {
		t.Fatal(err)
	}
	if err := store.Install(d1); err != nil {
		t.Fatalf("repeat install not idempotent: %v", err)
	}
	if err := store.Install(d2); !errors.Is(err, ErrDraining) {
		t.Fatalf("second install before drain = %v", err)
	}
	now = now.Add(time.Second)
	if err := store.Install(d2); err != nil {
		t.Fatal(err)
	}
	current, pending, _ := store.View()
	if current.Epoch != 3 || pending != nil || len(current.Members) != 3 {
		t.Fatalf("after catch-up: %+v pending=%v", current, pending)
	}
	history, more, err := store.HistoryAfter(2, 8)
	if err != nil || more || len(history) != 1 || history[0].Change.Transaction != "t2" {
		t.Fatalf("history after epoch 2 = %+v more=%v err=%v", history, more, err)
	}
}

// Retirement is a normal majority transition that closes the cluster for good, and survives a restart.
func TestRetirementIsTerminalAndSurvivesRestart(t *testing.T) {
	now := time.Unix(10, 0)
	e1 := Roster{Cluster: "c", Epoch: 1, Authority: "a", Members: []string{"a", "b", "c"}}
	path := filepath.Join(t.TempDir(), "roster.json")
	store, err := CreateRoster(path, e1)
	if err != nil {
		t.Fatal(err)
	}
	store.SetTiming(func() time.Time { return now }, time.Second)
	retire, err := NewRosterChange(e1, "retire:c:1", "retire", "")
	if err != nil {
		t.Fatal(err)
	}
	decision := decide(t, retire, "a", "b")
	if err := store.Install(decision); !errors.Is(err, ErrDraining) {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := store.Install(decision); err != nil {
		t.Fatal(err)
	}
	if !store.Retired() {
		t.Fatal("store not retired")
	}
	add, err := NewRosterChange(retire.Next, "later", "add", "d")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Fence(add); !errors.Is(err, ErrConflict) {
		t.Fatalf("a retired cluster accepted a new change: %v", err)
	}
	reopened, err := OpenRoster(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reopened.Retired() {
		t.Fatal("restart resurrected a retired cluster")
	}
}

// A decision without a strict majority of the parent roster is never installed.
func TestInstallRefusesMinorityDecision(t *testing.T) {
	e1 := Roster{Cluster: "c", Epoch: 1, Authority: "a", Members: []string{"a", "b", "c"}}
	store, err := CreateRoster(filepath.Join(t.TempDir(), "roster.json"), e1)
	if err != nil {
		t.Fatal(err)
	}
	add, _ := NewRosterChange(e1, "t1", "add", "d")
	if err := store.Install(decide(t, add, "a")); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("minority decision = %v, want ErrCorrupt", err)
	}
}
