package mic

import (
	"math"

	"github.com/HuskerMinion/techo5/echod/internal/lib/fft"
)

// residual is the second stage after the echo canceller: it turns down, band by band, what the linear
// filter left of the echo. The filter's own prediction of the echo (mic minus its output) says where in
// the spectrum the echo is; a share of that, resLeak, is taken to still be in the output, and each band
// is scaled by a Wiener-style gain against it. A voice over the echo keeps its bands, because it raises
// the output above the predicted residual; bands that hold only echo go down to resFloor.
//
// Measured on the Spot: the speaker reaches its microphones at about -22 dBFS and the linear filter
// takes 9-23 dB off, which leaves the residual as loud as a voice across the room.
type residual struct {
	f        *fft.FFT
	win      []float32
	inE, inY []float32 // the last resN samples of output and predicted echo
	outTail  []float32 // overlap-add carry
	pe, py   []float32 // smoothed band powers
	gain     []float32
	buf      []complex64
	bufY     []complex64
	queue    []int16 // output waiting to be handed back, one hop behind
	fill     int
}

const (
	resN   = 512
	resHop = resN / 2
)

// Tuned on recordings of a Spot talking over its own reply. Variables so the replay tests can try others.
var (
	resOn    = true
	resLeak  = float32(0.3)
	resOver  = float32(4)
	resFloor = float32(0.05)
	resAlpha = float32(0.6)
)

func newResidual() *residual {
	r := &residual{f: fft.New(resN), win: make([]float32, resN),
		inE: make([]float32, resN), inY: make([]float32, resN), outTail: make([]float32, resN),
		pe: make([]float32, resN/2+1), py: make([]float32, resN/2+1), gain: make([]float32, resN/2+1),
		buf: make([]complex64, resN), bufY: make([]complex64, resN), queue: make([]int16, resHop)}
	for i := range r.win { // sqrt-Hann, so analysis times synthesis sums to one at half overlap
		r.win[i] = float32(math.Sqrt(0.5 - 0.5*math.Cos(2*math.Pi*float64(i)/resN)))
	}
	return r
}

// apply suppresses the residual in out (the canceller's output) given mic (its input), in place. The
// result runs one hop (16 ms) behind, which nothing downstream minds.
func (r *residual) apply(mic, out []int16) {
	for i := range out {
		e := float32(out[i])
		y := float32(mic[i]) - e
		r.inE[resHop+r.fill], r.inY[resHop+r.fill] = e, y
		r.fill++
		if r.fill == resHop {
			r.hop()
			r.fill = 0
		}
		out[i], r.queue = r.queue[0], r.queue[1:]
	}
}

func (r *residual) hop() {
	for i := range resN {
		r.buf[i] = complex(r.inE[i]*r.win[i], 0)
		r.bufY[i] = complex(r.inY[i]*r.win[i], 0)
	}
	r.f.Forward(r.buf)
	r.f.Forward(r.bufY)
	a := resAlpha
	for k := 0; k <= resN/2; k++ {
		e, y := r.buf[k], r.bufY[k]
		pe := real(e)*real(e) + imag(e)*imag(e)
		py := real(y)*real(y) + imag(y)*imag(y)
		r.pe[k] = a*r.pe[k] + (1-a)*pe
		r.py[k] = a*r.py[k] + (1-a)*py
		g := 1 - resOver*resLeak*r.py[k]/(r.pe[k]+1e-3)
		r.gain[k] = max(g, resFloor)
	}
	for k := 0; k <= resN/2; k++ {
		r.buf[k] *= complex(r.gain[k], 0)
		if k > 0 && k < resN/2 {
			r.buf[resN-k] *= complex(r.gain[k], 0)
		}
	}
	r.f.Inverse(r.buf)
	for i := range resHop {
		v := r.outTail[i] + real(r.buf[i])*r.win[i]
		r.queue = append(r.queue, int16(max(-32768, min(32767, v))))
	}
	for i := range resHop {
		r.outTail[i] = real(r.buf[resHop+i]) * r.win[resHop+i]
	}
	copy(r.inE, r.inE[resHop:])
	copy(r.inY, r.inY[resHop:])
}
