package mic

import (
	"encoding/json"
	"log/slog"
	"math"
	"math/cmplx"
	"os"
	"sync"

	"github.com/HuskerMinion/techo5/echod/internal/layout"
	"github.com/HuskerMinion/techo5/echod/internal/lib/aec"
	"github.com/HuskerMinion/techo5/echod/internal/lib/fft"
)

// barge is the barge-in beamformer: while the speaker plays, every microphone gets its own echo
// canceller, and the cancelled microphones are combined per frequency band (MVDR) to keep sound that
// arrives the way a person's voice does and turn down what arrives the way the leftover echo does.
//
// The voice's pattern across the microphones (steer) is learned from wake words heard with nothing
// playing: those are a person, from where people talk to this device, with no echo on top. The echo's
// pattern (echo covariance) is learned continuously while the speaker plays. On recordings of a
// Spot talking over its own reply, this caught half the wake words, where one cancelled microphone caught
// none.
//
// steer is saved to bargeVoicePath after every update and read back at start, so a restart keeps it.
type barge struct {
	cancel [maxMics]*aec.Canceller
	f      *fft.FFT
	win    []float32

	in    [maxMics][]float32 // last bargeN samples per cancelled microphone
	tail  []float32
	queue []int16
	fill  int
	buf   [maxMics][]complex64

	echo [][maxMics * maxMics]complex128 // per band, smoothed echo covariance

	mu    sync.Mutex
	steer [][maxMics]complex128 // per band, the voice's pattern, mic 0 = 1; nil until learned

	// recent holds the last second of raw microphones while nothing plays, for learning steer.
	recent [maxMics][]int16
	at     int
}

const (
	bargeN   = 512
	bargeHop = bargeN / 2
	bargeBin = bargeN/2 + 1
	// bargeAlpha smooths the echo covariance: about 1.6 s at a 16 ms hop.
	bargeAlpha = 0.99
	// bargeLoad is diagonal loading against an echo covariance that is nearly singular.
	bargeLoad = 1e-2
)

// bargeVoicePath holds the learned voice: per band, real and imaginary parts per microphone.
var bargeVoicePath = layout.StateDir + "/barge-voice.json"

func (b *barge) save() {
	out := make([][]float64, len(b.steer))
	for k, d := range b.steer {
		for m := range Mics {
			out[k] = append(out[k], real(d[m]), imag(d[m]))
		}
	}
	data, _ := json.Marshal(out)
	tmp := bargeVoicePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err == nil {
		_ = os.Rename(tmp, bargeVoicePath)
	}
}

func (b *barge) load() {
	data, err := os.ReadFile(bargeVoicePath)
	if err != nil {
		return
	}
	var in [][]float64
	if json.Unmarshal(data, &in) != nil || len(in) != bargeBin {
		return
	}
	steer := make([][maxMics]complex128, bargeBin)
	for k, v := range in {
		if len(v) != 2*Mics {
			return
		}
		for m := range Mics {
			steer[k][m] = complex(v[2*m], v[2*m+1])
		}
	}
	b.steer = steer
	slog.Info("barge-in beamformer: loaded the learned voice")
}

// bargeOn is whether the beamformer runs during playback.
var bargeOn = Mics >= 2

func newBarge() *barge {
	b := &barge{f: fft.New(bargeN), win: make([]float32, bargeN), tail: make([]float32, bargeN),
		queue: make([]int16, bargeHop), echo: make([][maxMics * maxMics]complex128, bargeBin)}
	for i := range b.win {
		b.win[i] = float32(math.Sqrt(0.5 - 0.5*math.Cos(2*math.Pi*float64(i)/bargeN)))
	}
	for m := range Mics {
		c, err := aec.New(aec.Config{Taps: cancelTaps, Mu: cancelMu})
		if err != nil {
			return nil
		}
		b.cancel[m] = c
		b.in[m] = make([]float32, bargeN)
		b.buf[m] = make([]complex64, bargeN)
		b.recent[m] = make([]int16, Rate)
	}
	b.load()
	return b
}

// quiet keeps the raw microphones while nothing plays, for learnVoice.
func (b *barge) quiet(mics [][]int16) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := range mics[0] {
		for m := range Mics {
			b.recent[m][b.at] = mics[m][i]
		}
		b.at = (b.at + 1) % Rate
	}
}

// learnVoice takes the loudest frames of the last second (the wake word just heard) as the voice and
// folds their pattern across the microphones into steer.
func (b *barge) learnVoice() {
	b.mu.Lock()
	defer b.mu.Unlock()
	var x [maxMics][]float32
	for m := range Mics {
		x[m] = make([]float32, Rate)
		for i := range Rate {
			x[m][i] = float32(b.recent[m][(b.at+i)%Rate])
		}
	}
	frames := (Rate - bargeN) / bargeHop
	type spec [maxMics][]complex64
	specs := make([]spec, frames)
	energy := make([]float64, frames)
	for t := range frames {
		for m := range Mics {
			s := make([]complex64, bargeN)
			for i := range bargeN {
				s[i] = complex(x[m][t*bargeHop+i]*b.win[i], 0)
			}
			b.f.Forward(s)
			specs[t][m] = s
			if m == 0 {
				for k := range bargeBin {
					energy[t] += float64(real(s[k])*real(s[k]) + imag(s[k])*imag(s[k]))
				}
			}
		}
	}
	// the loudest half of the frames: the word, not the quiet around it
	var mean float64
	for _, e := range energy {
		mean += e
	}
	mean /= float64(frames)
	if b.steer == nil {
		b.steer = make([][maxMics]complex128, bargeBin)
	}
	fresh := b.steer[1][0] == 0
	for k := range bargeBin {
		var p [maxMics * maxMics]complex128
		for t := range frames {
			if energy[t] < mean {
				continue
			}
			for i := range Mics {
				for j := range Mics {
					p[i*maxMics+j] += complex128(specs[t][i][k]) * cmplx.Conj(complex128(specs[t][j][k]))
				}
			}
		}
		d := bargePrincipal(p)
		if d[0] == 0 {
			continue
		}
		for m := range Mics {
			d[m] /= d[0]
			if fresh {
				b.steer[k][m] = d[m]
			} else {
				b.steer[k][m] = 0.7*b.steer[k][m] + 0.3*d[m]
			}
		}
	}
	b.save()
	slog.Info("barge-in beamformer: learned the voice from a wake word", "first", fresh)
}

// principal is the main eigenvector of a Hermitian matrix, by power iteration.
func bargePrincipal(p [maxMics * maxMics]complex128) [maxMics]complex128 {
	var v [maxMics]complex128
	for i := range Mics {
		v[i] = 1
	}
	for range 20 {
		var n [maxMics]complex128
		var norm float64
		for i := range Mics {
			for j := range Mics {
				n[i] += p[i*maxMics+j] * v[j]
			}
			norm += real(n[i])*real(n[i]) + imag(n[i])*imag(n[i])
		}
		if norm == 0 {
			return [maxMics]complex128{}
		}
		norm = math.Sqrt(norm)
		for i := range Mics {
			v[i] = n[i] / complex(norm, 0)
		}
	}
	return v
}

// ready says whether a voice has been learned.
func (b *barge) ready() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.steer != nil
}

// apply cancels every microphone against ref and beamforms them into out (one hop behind).
func (b *barge) apply(mics [][]int16, ref []int16, out []int16) {
	b.mu.Lock()
	steer := b.steer
	b.mu.Unlock()
	var e [maxMics][]int16
	for m := range Mics {
		r, err := b.cancel[m].Process(mics[m], ref)
		if err != nil {
			return
		}
		e[m] = append([]int16(nil), r...)
	}
	for i := range out {
		for m := range Mics {
			b.in[m][bargeHop+b.fill] = float32(e[m][i])
		}
		b.fill++
		if b.fill == bargeHop {
			b.hop(steer)
			b.fill = 0
		}
		out[i], b.queue = b.queue[0], b.queue[1:]
	}
}

func (b *barge) hop(steer [][maxMics]complex128) {
	for m := range Mics {
		for i := range bargeN {
			b.buf[m][i] = complex(b.in[m][i]*b.win[i], 0)
		}
		b.f.Forward(b.buf[m])
	}
	y := make([]complex64, bargeN)
	for k := range bargeBin {
		var x [maxMics]complex128
		for m := range Mics {
			x[m] = complex128(b.buf[m][k])
		}
		c := &b.echo[k]
		var tr float64
		for i := range Mics {
			for j := range Mics {
				c[i*maxMics+j] = bargeAlpha*c[i*maxMics+j] + (1-bargeAlpha)*x[i]*cmplx.Conj(x[j])
			}
			tr += real(c[i*maxMics+i])
		}
		out := x[0]
		if steer != nil && tr > 0 {
			// MVDR: w = R^-1 d / (d^H R^-1 d), distortionless towards the voice
			a := *c
			for i := range Mics {
				a[i*maxMics+i] += complex(bargeLoad*tr/float64(Mics), 0)
			}
			if u, ok := bargeSolve(a, steer[k]); ok {
				var den, num complex128
				for i := range Mics {
					den += cmplx.Conj(steer[k][i]) * u[i]
					num += cmplx.Conj(u[i]) * x[i]
				}
				if cmplx.Abs(den) > 1e-12 {
					out = num / cmplx.Conj(den)
				}
			}
		}
		y[k] = complex64(out)
		if k > 0 && k < bargeN/2 {
			y[bargeN-k] = complex64(cmplx.Conj(out))
		}
	}
	b.f.Inverse(y)
	for i := range bargeHop {
		v := b.tail[i] + real(y[i])*b.win[i]
		b.queue = append(b.queue, int16(max(-32768, min(32767, v))))
	}
	for i := range bargeHop {
		b.tail[i] = real(y[bargeHop+i]) * b.win[bargeHop+i]
	}
	for m := range Mics {
		copy(b.in[m], b.in[m][bargeHop:])
	}
}

// solve is a·u = d for a small Hermitian a, by Gaussian elimination with partial pivoting.
func bargeSolve(a [maxMics * maxMics]complex128, d [maxMics]complex128) ([maxMics]complex128, bool) {
	n := Mics
	u := d
	for c := range n {
		p := c
		for r := c + 1; r < n; r++ {
			if cmplx.Abs(a[r*maxMics+c]) > cmplx.Abs(a[p*maxMics+c]) {
				p = r
			}
		}
		if cmplx.Abs(a[p*maxMics+c]) < 1e-18 {
			return u, false
		}
		if p != c {
			for j := range n {
				a[c*maxMics+j], a[p*maxMics+j] = a[p*maxMics+j], a[c*maxMics+j]
			}
			u[c], u[p] = u[p], u[c]
		}
		for r := c + 1; r < n; r++ {
			f := a[r*maxMics+c] / a[c*maxMics+c]
			for j := c; j < n; j++ {
				a[r*maxMics+j] -= f * a[c*maxMics+j]
			}
			u[r] -= f * u[c]
		}
	}
	for c := n - 1; c >= 0; c-- {
		for j := c + 1; j < n; j++ {
			u[c] -= a[c*maxMics+j] * u[j]
		}
		u[c] /= a[c*maxMics+c]
	}
	return u, true
}
