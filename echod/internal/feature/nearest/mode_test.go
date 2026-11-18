package nearest

import (
	"testing"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
)

func TestNearestModeValues(t *testing.T) {
	for _, mode := range []string{modeExisting, modeElected} {
		if !validMode(mode) {
			t.Errorf("mode %q not accepted", mode)
		}
	}
	if validMode("hosted") || validMode("unknown") {
		t.Fatal("accepted an unknown mode")
	}
	if got := (config.Nearest{}).EffectiveMode(); got != modeExisting {
		t.Fatalf("empty config mode = %q", got)
	}
	if got := (config.Nearest{Mode: modeElected}).EffectiveMode(); got != modeElected {
		t.Fatalf("elected mode = %q", got)
	}
	for _, mode := range []string{"hosted", "elected", "unknown"} {
		if got := (config.Nearest{Mode: mode}).EffectiveMode(); got != modeExisting {
			t.Fatalf("mode %q is read as %q, want classic", mode, got)
		}
	}
}

func TestCoordinatorSameEventOnlyOpensReturnedLocalTurn(t *testing.T) {
	h := newHouse(t)
	enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen"})
	h.n.readinessFanout = func([]announce.Peer, *pending, func() bool, func(time.Time, float64)) {}
	h.n.Woke(0)
	first := h.mine
	h.now = h.now.Add(20 * time.Millisecond)
	h.n.Woke(1)
	second := h.mine
	winner := first.claim
	for _, pending := range []*pending{first, second} {
		h.n.mu.Lock()
		pending.readinessComplete = true
		pending.earlyWinner = &winner
		h.n.mu.Unlock()
	}
	var opened []time.Time
	for _, pending := range []*pending{first, second} {
		h.n.decide(pending.Word, pending, func() bool { return true }, func(at time.Time, _ float64) { opened = append(opened, at) })
	}
	if len(opened) != 1 || !opened[0].Equal(first.at) {
		t.Fatalf("same event opened at %v; only selected local turn %v should answer", opened, first.at)
	}
}

// A coordinated win where the speech band was measured and no voice stood out opens no turn; a device that
// does not measure, or one that heard a voice, still answers.
func TestCoordinatedWinWithNoVoiceDoesNotAnswer(t *testing.T) {
	energy := 40.0
	for _, tc := range []struct {
		name    string
		clarity float64
		energy  *float64
		opens   bool
	}{
		{"no voice", 0, &energy, false},
		{"a voice", 12, &energy, true},
		{"not measured", 0, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHouse(t)
			enableCoordinatorRoster(t, h, "Office", []string{"Office", "Kitchen"})
			h.n.readinessFanout = func([]announce.Peer, *pending, func() bool, func(time.Time, float64)) {}
			h.n.Woke(0)
			mine := h.mine
			h.n.mu.Lock()
			mine.clarity, mine.Energy = tc.clarity, tc.energy
			winner := mine.claim
			mine.readinessComplete, mine.earlyWinner = true, &winner
			h.n.mu.Unlock()
			opened := false
			h.n.decide(mine.Word, mine, func() bool { return true }, func(time.Time, float64) { opened = true })
			if opened != tc.opens {
				t.Fatalf("opened = %v, want %v", opened, tc.opens)
			}
		})
	}
}
