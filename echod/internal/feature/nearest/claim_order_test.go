package nearest

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestBetterRichOrderAndOldPeers(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	m := func(name string, score, conf, energy float64) claim {
		return claim{From: name, Score: score, Confidence: f(conf), Energy: f(energy)}
	}
	// Each step down the order only decides when the one above ties.
	order := []claim{
		m("d", 20, 0.1, 10), // a closer device (Score more than 1 dB higher) beats a more confident one
		m("c", 10, 0.1, 60), // within 1 dB, energy decides
		m("b", 10.4, 0.9, 50),
		m("a", 10, 0.8, 50), // then confidence
		m("y", 10, 0.7, 50), // then name, ignoring case
		m("Z", 10, 0.7, 50),
	}
	for i := range order {
		for j := range order {
			if got, want := better(order[i], order[j]), i < j; got != want {
				t.Fatalf("better(%s, %s) = %v, want %v", order[i].From, order[j].From, got, want)
			}
		}
	}

	// The call bonus is part of Score, so it still decides.
	if !better(m("bonus", 15, 0.1, 10), m("plain", 10, 0.9, 90)) {
		t.Fatal("a call bonus raising Score by more than 1 dB should decide")
	}

	// An older device sends no measurements: it is compared on unrounded Score alone, in both directions.
	old := claim{From: "Old", Score: 10}
	if !better(m("new", 12, 0.1, 1), old) || better(old, m("new", 12, 0.1, 1)) {
		t.Fatal("a newer claim with the higher Score should beat an older one")
	}
	if better(m("new", 8, 0.99, 99), old) || !better(old, m("new", 8, 0.99, 99)) {
		t.Fatal("an older claim with the higher Score should beat a newer one")
	}
	if !better(claim{From: "A", Score: 10.4}, m("b", 10, 0.99, 99)) {
		t.Fatal("against an older claim Score is not rounded")
	}
	// A claim with only some of the fields is not measured.
	if part := (claim{From: "P", Score: 1, Confidence: f(1)}); better(part, claim{From: "Q", Score: 2, Confidence: f(0)}) {
		t.Fatal("a partial claim should compare by Score")
	}
}

func TestClaimMeasurementsOptionalOnTheWire(t *testing.T) {
	old, err := json.Marshal(claim{From: "A", Turn: "t", Score: 3})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"confidence", "energy"} {
		if strings.Contains(string(old), k) {
			t.Fatalf("an unmeasured claim sends %s: %s", k, old)
		}
	}
	var got claim
	if err := json.Unmarshal([]byte(`{"from":"A","turn":"t","score":3}`), &got); err != nil || got.measured() {
		t.Fatalf("older claim: %+v, %v", got, err)
	}
	nan := math.NaN()
	if finiteClaim(claim{Score: 1, Energy: &nan}) || !finiteClaim(claim{Score: 1}) {
		t.Fatal("finiteClaim should refuse a non-finite measurement only")
	}
}

// A wake word a device's own speaker said beats every claim that is not one, however well the others
// heard it through the wall, and two played claims fall back to the ordinary order.
func TestBetterPlayedBeatsHeard(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	played := claim{From: "speaker", Score: 0, Confidence: f(0), Energy: f(0), Busy: true, Played: true}
	heard := claim{From: "wall", Score: 30, Confidence: f(1), Energy: f(90)}
	if !better(played, heard) || better(heard, played) {
		t.Fatal("a played claim should beat a heard one")
	}
	if !better(played, claim{From: "old", Score: 99}) {
		t.Fatal("a played claim should beat an older device's claim")
	}
	louder := played
	louder.From, louder.Score = "louder", 5
	if !better(louder, played) || better(played, louder) {
		t.Fatal("two played claims should go by the ordinary order")
	}
}
