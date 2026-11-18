package membership

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// storeDevice is a real participant over a real RosterStore, with a 1 ms drain.
type storeDevice struct {
	name string
	path string
	mu   sync.Mutex
	st   *RosterStore
	// lose is how many responses to drop, after the operation took effect, per operation name.
	lose map[string]int
}

func (d *storeDevice) lost(op string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lose[op] > 0 {
		d.lose[op]--
		return true
	}
	return false
}

func (d *storeDevice) PrepareJoin(_ context.Context, c RosterChange) error {
	st, err := PrepareRoster(d.path, c)
	if err != nil {
		return err
	}
	st.SetTiming(nil, time.Millisecond)
	d.st = st
	if d.lost("prepare") {
		return errors.New("response lost")
	}
	return nil
}

func (d *storeDevice) FenceAndDrain(_ context.Context, c RosterChange) (RosterAck, error) {
	if err := d.st.Fence(c); err != nil {
		return RosterAck{}, err
	}
	time.Sleep(5 * time.Millisecond)
	if err := d.st.FinishDrain(); err != nil {
		return RosterAck{}, err
	}
	if d.lost("fence") {
		return RosterAck{}, errors.New("response lost")
	}
	return d.st.Acknowledge(c, d.name)
}

func (d *storeDevice) CommitRoster(_ context.Context, decision RosterDecision) error {
	if err := d.st.Install(decision); err != nil {
		if errors.Is(err, ErrDraining) {
			time.Sleep(5 * time.Millisecond)
			err = d.st.Install(decision)
		}
		if err != nil {
			return err
		}
	}
	if d.lost("commit") {
		return errors.New("response lost")
	}
	return nil
}

// Responses lost at every step: the same transaction is retried, every device ends at the same epoch,
// the joiner has no committed roster (so cannot vote) until its commit lands, and no second successor
// to the same parent is ever invented.
func TestLostResponsesAreRetriedWithoutInventingASuccessor(t *testing.T) {
	dir := t.TempDir()
	parent := Roster{Cluster: "c", Epoch: 1, Authority: "a", Members: []string{"a", "b", "c"}}
	devices := map[string]*storeDevice{}
	for _, name := range []string{"a", "b", "c"} {
		path := filepath.Join(dir, name+".json")
		st, err := CreateRoster(path, parent)
		if err != nil {
			t.Fatal(err)
		}
		st.SetTiming(nil, time.Millisecond)
		devices[name] = &storeDevice{name: name, path: path, st: st, lose: map[string]int{}}
	}
	devices["d"] = &storeDevice{name: "d", path: filepath.Join(dir, "d.json"), lose: map[string]int{"prepare": 1, "commit": 1}}
	devices["b"].lose["fence"] = 1
	devices["c"].lose["commit"] = 1
	devices["a"].lose["commit"] = 1
	participants := map[string]RosterParticipant{}
	for name, d := range devices {
		participants[name] = d
	}
	change, err := NewRosterChange(parent, "t1", "add", "d")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var decision RosterDecision
	for attempt := 0; attempt < 5; attempt++ {
		decision, err = CollectRosterDecision(ctx, change, participants)
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("round never completed with the same change: %v", err)
	}
	if err := devices["a"].st.SaveDecision(decision); err != nil {
		t.Fatal(err)
	}
	if current, _, _ := devices["d"].st.View(); current.Cluster != "" {
		t.Fatal("the joiner has a committed roster before its commit")
	}
	for attempt := 0; attempt < 5; attempt++ {
		if err = DistributeRosterDecision(ctx, decision, participants); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("distribution never completed: %v", err)
	}
	want, _ := Fingerprint(change.Next)
	for name, d := range devices {
		current, pending, uncertain := d.st.View()
		got, _ := Fingerprint(current)
		if got != want || pending != nil || uncertain {
			t.Fatalf("%s: roster %+v pending=%v uncertain=%v", name, current, pending, uncertain)
		}
		if history, _, _ := d.st.HistoryAfter(0, 8); len(history) != 1 {
			t.Fatalf("%s holds %d transitions, want exactly the one", name, len(history))
		}
	}
}
