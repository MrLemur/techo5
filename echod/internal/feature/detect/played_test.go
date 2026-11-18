package detect

import (
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/hardware/speaker"
)

// stereo is n frames of the speaker's format, each channel at v.
func stereo(n int, v int16) []byte {
	b := make([]byte, n*speaker.Channels*2)
	for i := 0; i < len(b); i += 2 {
		binary.LittleEndian.PutUint16(b[i:], uint16(v))
	}
	return b
}

// A period becomes mono at the model's rate, carrying the frames that did not fill a group into the
// next period, and silence is told apart from sound.
func TestPlayedPeriodDecimates(t *testing.T) {
	config.Use(filepath.Join(t.TempDir(), "state.json"))
	if err := config.Set().Nearest().Mode("device_quorum"); err != nil {
		t.Fatal(err)
	}
	w := newPlayedWatcher(nil)
	w.period(stereo(1024, 1000), time.Now())
	w.period(stereo(1024, 1000), time.Now())
	a, b := <-w.in, <-w.in
	if got := len(a.samples) + len(b.samples); got != 2048/playedDecimate {
		t.Fatalf("got %d samples from 2048 frames, want %d", got, 2048/playedDecimate)
	}
	if a.samples[0] != 1000 || !a.loud {
		t.Fatalf("got sample %d loud %v, want 1000 and loud", a.samples[0], a.loud)
	}
	w.period(stereo(768, playedQuiet), time.Now())
	if q := <-w.in; q.loud {
		t.Fatal("a period at the quiet level should count as silence")
	}
}

// Outside Device quorum nothing acts on a played claim, so the speaker's output is not even passed on.
func TestPlayedIdleOutsideDeviceQuorum(t *testing.T) {
	config.Use(filepath.Join(t.TempDir(), "state.json"))
	w := newPlayedWatcher(nil)
	w.period(stereo(1024, 1000), time.Now())
	if len(w.in) != 0 {
		t.Fatal("a classic device passed its speaker's output on to be scored")
	}
}

// A watcher that finds nothing loaded in the first slot scores nothing and does not fail.
func TestPlayedNoModel(t *testing.T) {
	w := newPlayedWatcher(&Engine{})
	w.take(playedPeriod{samples: make([]int16, 320), loud: true, heardEnd: time.Now()})
	if w.det != nil {
		t.Fatal("no detector should be made without a model")
	}
}
