package detect

import (
	"context"
	"encoding/binary"
	"log/slog"
	"math"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/nearest"
	"github.com/HuskerMinion/techo5/echod/internal/hardware/speaker"
	"github.com/HuskerMinion/techo5/echod/internal/lib/wake"
)

const (
	// playedQuiet is the loudest sample of a period counted as silence (about -60 dBFS).
	playedQuiet = 32
	// playedSleep is how long the speaker is silent before the model is let go, and starts fresh with the
	// next sound.
	playedSleep = time.Second
	// playedHold is how long after one detection the next is not reported: a word goes on scoring above the
	// threshold for a few frames.
	playedHold = 1500 * time.Millisecond
	// playedThreshold is the score in what the speaker played that counts as it saying the wake word.
	playedThreshold = 0.4
	// playedNoted is the lowest best score of a stretch of sound worth a line in the log, so a wake word
	// played just under the threshold can be seen.
	playedNoted = 0.2
	// playedDecimate is the speaker's rate over the model's.
	playedDecimate = speaker.Rate / 16000
)

// playedWatcher runs the first wake word's model on what this device's own speaker plays, and tells
// nearest when the speaker said the word (nearest.Played). Echo cancellation removes the device's own
// sound from its microphones, so without this a reply that says the wake word would be claimed only by
// the devices that heard it through a wall, and the nearest of them would answer. It runs only in
// Device quorum, the one mode that acts on a played claim, and lets the model go while the speaker is
// silent.
//
// playedThreshold is lower than a slot's: the speaker's own output scores well below the same voice
// heard across a room, and stopping a played word costs little, since a played claim only quietens a
// wake the other devices heard at that moment.

// playedPeriod is one period the speaker sent, as mono samples at the model's rate.
type playedPeriod struct {
	samples  []int16
	loud     bool
	heardEnd time.Time
}

type playedWatcher struct {
	in     chan playedPeriod
	engine *Engine

	// held is the decimator's carry: frames of the last period that did not fill a group.
	held    []int32
	det     *Detector
	model   string
	quietAt time.Time
	firedAt time.Time
	// best is the highest score in the stretch of sound now playing, said in the log when it ends.
	best float64
}

func newPlayedWatcher(e *Engine) *playedWatcher {
	return &playedWatcher{in: make(chan playedPeriod, 32), engine: e}
}

// period is the speaker's hook (speaker.OnPlayed). It runs on the write loop, so it only converts and
// hands over; a period that finds the watcher behind is dropped.
func (w *playedWatcher) period(pcm []byte, heardEnd time.Time) {
	if !playedWanted() {
		return
	}
	p := playedPeriod{heardEnd: heardEnd}
	frame := speaker.Channels * 2
	for i := 0; i+frame <= len(pcm); i += frame {
		var sum int32
		for c := range speaker.Channels {
			v := int16(binary.LittleEndian.Uint16(pcm[i+2*c:]))
			if v > playedQuiet || v < -playedQuiet {
				p.loud = true
			}
			sum += int32(v)
		}
		w.held = append(w.held, sum/int32(speaker.Channels))
		if len(w.held) == playedDecimate {
			var s int32
			for _, v := range w.held {
				s += v
			}
			p.samples = append(p.samples, int16(s/playedDecimate))
			w.held = w.held[:0]
		}
	}
	select {
	case w.in <- p:
	default:
	}
}

func (w *playedWatcher) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-w.in:
			w.take(p)
		}
	}
}

// take scores one period, and tells nearest when the speaker said the wake word.
func (w *playedWatcher) take(p playedPeriod) {
	if !p.loud {
		if w.quietAt.IsZero() {
			w.quietAt = p.heardEnd
			if w.best >= playedNoted {
				slog.Info("own speaker scored", "id", w.model, "peak", round2(w.best), "threshold", playedThreshold)
			}
			w.best = 0
		}
		if w.det != nil && p.heardEnd.Sub(w.quietAt) >= playedSleep {
			w.det = nil
		}
		if w.det == nil {
			return
		}
	} else {
		w.quietAt = time.Time{}
	}

	m, ok := w.engine.Model(0)
	if !ok || m.Kind != wake.KindMicroWakeWord {
		w.det = nil
		return
	}
	if w.det == nil || w.model != m.ID {
		det, err := NewDetector(m)
		if err != nil {
			slog.Error("listening to the speaker for the wake word failed", "id", m.ID, "err", err)
			return
		}
		w.det, w.model = det, m.ID
	}
	peak := w.det.Peak(p.samples)
	w.best = max(w.best, peak)
	if peak < playedThreshold || p.heardEnd.Sub(w.firedAt) < playedHold {
		return
	}
	w.firedAt = p.heardEnd
	slog.Info("own speaker said the wake word", "id", m.ID, "peak", peak)
	nearest.Get().Played(m.ID, p.heardEnd)
}

// playedWanted is whether a played claim would be acted on: only Device quorum sends one.
func playedWanted() bool {
	n := config.Get().Nearest
	return n.Enabled() && n.EffectiveMode() == "device_quorum"
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }
