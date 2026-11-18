// Package nearest decides which device answers a wake word that several of them heard.
//
// Home Assistant keeps whichever device reaches it first and refuses the rest, which is the fastest
// device rather than the one somebody spoke to. With this on, a device that hears its wake word tells the
// others how well it heard it, waits a moment to hear from them, and only opens its turn if none of them
// heard it better. The others go back to listening without a sound.
//
// How well is how far the word stood out from the device's own room (hardware/mic's Standout), plus an
// offset of the device's own for hardware that hears louder or quieter than the rest.
//
// Once a device has answered, it will not take the turn away: Home Assistant may refuse the same wake word
// from a second device for a moment after the first. A late claim can tell its sender to stand down only
// if that sender has not already answered. Claims lost or delayed beyond the configured gate can still
// produce duplicate answers, as can network partitions.
//
// In the default Classic mode it fails open: with no house word, no other devices, a claim that does not
// arrive or anything else going wrong, a device answers as it would without this after the bounded wait.
// Claims are best-effort, so a partition can still make two devices answer. In Device quorum it fails
// closed instead: a wake that cannot be settled is dropped. What is said while waiting is sent from the
// microphone's history.
package nearest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	esphome "github.com/ygelfand/go-esphome-device"

	"github.com/HuskerMinion/techo5/echod/internal/component"
	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
	"github.com/HuskerMinion/techo5/echod/internal/feature/phone"
	"github.com/HuskerMinion/techo5/echod/internal/feature/voice"
	"github.com/HuskerMinion/techo5/echod/internal/feature/web"
	"github.com/HuskerMinion/techo5/echod/internal/hardware/mic"
	"github.com/HuskerMinion/techo5/echod/internal/layout"
	"github.com/HuskerMinion/techo5/echod/internal/lib/safe"
)

func init() {
	component.Register(component.Device, Get(), component.Order(40))
}

const (
	// path is where claims arrive, on the device's web port, which the Wi-Fi firewall already lets in.
	path   = "/nearest"
	v3Path = "/nearest/v7"

	// heard is how far back the loudness is read at a wake: the word, which has just ended.
	heard = 1200 * time.Millisecond

	// kept is how long a claim from another device is remembered: longer than any window, so one that
	// arrived just before this device heard the word still counts.
	kept = 3 * time.Second

	// warnEvery is how often a claim that does not check out is written to the log, so a device on the
	// network sending them cannot fill it.
	warnEvery = time.Minute

	// bodyWithin is how long a signed-looking claim has to arrive whole: one is a few hundred bytes.
	bodyWithin = 2 * time.Second

	// maxPeers bounds the devices one wake word is told to, which mDNS alone decides.
	maxPeers = 16
)

// claim is what a device says when it hears its wake word. Word is which wake word, so two people
// saying different ones to different devices at once are not taken for one person saying one.
type claim struct {
	From     string   `json:"from"`
	Turn     string   `json:"turn"`
	Word     string   `json:"word,omitempty"`
	Score    float64  `json:"score"`
	Protocol int      `json:"protocol,omitempty"`
	HeardAt  int64    `json:"heard_at,omitempty"`
	Members  []string `json:"members,omitempty"`

	// Confidence and Energy are how this device measured the word, where it could: the wake-word model's
	// score and the speech band's level in dB. Both are left out when the device could not measure them, as
	// an older device always does, and better compares them only when both claims carry both.
	Confidence *float64 `json:"confidence,omitempty"`
	Energy     *float64 `json:"energy,omitempty"`

	// Busy is a claim from a device whose own turn is open: it heard the word but will not act on it. It
	// is sent to the elected coordinator so that a device which heard the word best, and is busy, still
	// keeps the others quiet, while a word said in another room is still won by the device nearest it.
	Busy bool `json:"busy,omitempty"`

	// Played is a busy claim for a wake word this device's own speaker played (Played). It beats every
	// claim that is not one, so the device that heard the speaker best through a wall does not answer it.
	Played bool `json:"played,omitempty"`

	// Cluster, Epoch and Fingerprint bind a Device quorum claim to one committed membership.
	Cluster     string `json:"cluster,omitempty"`
	Epoch       uint64 `json:"epoch,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
}

// arrived is a claim from another device and when it came.
type arrived struct {
	claim
	at time.Time
}

// pending is this device's claim for one wake word it heard.
type pending struct {
	claim
	at                time.Time // when the word was heard
	window            time.Duration
	decided           bool
	generation        uint64
	mode              string
	readinessComplete bool
	earlyWinner       *claim
	membership        membershipToken

	// clarity is how clearly this device heard it, before the offset and a call's lead: what Confirm
	// below nearest-device score is measured against (#503), so an offset set to lean the decision
	// doesn't make every wake faint, or none.
	clarity float64

	// answered is set once the turn opened here; elsewhere when a device that already answered said so.
	answered, elsewhere bool
}

// Nearest is the switch, settings, and in-flight claims.
type Nearest struct {
	on               *esphome.Switch
	window           *esphome.Number
	offset           *esphome.Number
	mode             *esphome.Select
	membershipStatus *esphome.TextSensor

	// The device, replaced in tests: which wakes open a turn, whether one could, how well this device
	// heard the word, whether it hears at all, the others, how a claim reaches them, and the clock.
	woke      func(slot int, at time.Time)
	wokeNow   func(slot int)
	faint     func(slot int, at time.Time) // a faint win (won): asked about first, else woke's (#503)
	busy      func() bool
	endTurn   func()
	canAnswer func(slot int) bool
	wakeWord  func(slot int) string
	score     func() float64
	energy    func() float64
	hears     func() bool
	// onCall is whether a call is up here and lets the wake word through (phone.WakeAllowed): a wake word
	// heard during one is most likely for this device, the one somebody wants to hang up, so its claim
	// leads by onCallLead. Only then: a device on a call that does not take wake words claims as any other.
	onCall          func() bool
	peers           func() []announce.Peer
	fanout          func(peers []announce.Peer, mine *pending)
	readinessFanout func(peers []announce.Peer, mine *pending, can func() bool, open func(time.Time, float64))
	pendingObserver func(*pending)
	now             func() time.Time
	after           func(d time.Duration, f func())
	wait            func(d time.Duration)
	clockNow        func(time.Time) (time.Time, bool)

	// confidence is the wake-word model's score for a slot's latest detection, zero when unknown. The
	// detector sets it (SetConfidence), as nearest cannot import it.
	confidence func(slot int) float64

	mu              sync.Mutex
	generation      uint64
	clock           electionClock
	clockOnce       sync.Once
	coordinator     coordinatorRuntime
	coordinatorOnce sync.Once
	peerStatusOnce  sync.Once
	membershipOnce  sync.Once
	peerStatuses    peerStatusCache
	membership      *membershipRuntime
	enabled         bool
	others          []arrived
	mine            *pending
	claims          []*pending
}

var (
	once   sync.Once
	shared *Nearest
)

// Get is the one.
func Get() *Nearest {
	once.Do(func() { shared = build() })
	return shared
}

func build() *Nearest {
	n := &Nearest{
		on: &esphome.Switch{Base: esphome.Base{
			ObjectID: "nearest_device_answers", Name: "Nearest device answers", Icon: "mdi:account-voice",
			Category: esphome.CategoryConfig,
		}},
		window: &esphome.Number{
			Base: esphome.Base{ObjectID: "nearest_device_window", Name: "Nearest device window", Icon: "mdi:timer-sand",
				Category: esphome.CategoryConfig},
			Min: config.MinNearestWindowMs, Max: config.MaxNearestWindowMs, Step: 10, Unit: "ms",
			Mode: esphome.NumberBox,
		},
		mode: &esphome.Select{
			Base: esphome.Base{ObjectID: "nearest_device_mode", Name: "Nearest device mode",
				Icon: "mdi:account-voice", Category: esphome.CategoryConfig},
			Options: []string{modeExisting, modeElected},
		},
		membershipStatus: &esphome.TextSensor{Base: esphome.Base{ObjectID: "nearest_device_membership",
			Name: "Nearest membership", Icon: "mdi:account-group-outline", Category: esphome.CategoryDiagnostic}},
		offset: &esphome.Number{
			Base: esphome.Base{ObjectID: "nearest_device_offset", Name: "Nearest device offset", Icon: "mdi:tune-variant",
				Category: esphome.CategoryConfig},
			Min: config.MinNearestOffset, Max: config.MaxNearestOffset, Step: 1, Unit: "dB",
			Mode: esphome.NumberBox,
		},
		woke:      func(slot int, at time.Time) { voice.Get().WokeAt(slot, at) },
		wokeNow:   func(slot int) { voice.Get().Woke(slot) },
		faint:     func(slot int, at time.Time) { voice.Get().WokeFaint(slot, at) },
		busy:      func() bool { return voice.Get().Busy() },
		endTurn:   func() { voice.Get().Cancel() },
		canAnswer: func(slot int) bool { return voice.Get().CanAnswer(slot) },
		wakeWord: func(slot int) string {
			if ids := voice.Get().ActiveWakeWords(); slot >= 0 && slot < len(ids) {
				return ids[slot]
			}
			return ""
		},
		score:  func() float64 { return mic.Get().Standout(heard) },
		energy: func() float64 { return mic.Get().Energy(heard) },
		hears:  layout.HasMicrophone,
		onCall: func() bool { return phone.Get().Busy() && phone.Get().WakeAllowed() },
		peers:  announce.Peers,
		now:    time.Now,
		after: func(d time.Duration, f func()) {
			time.AfterFunc(d, func() { safe.Go("nearest device decision", f) })
		},
		wait:       time.Sleep,
		membership: newMembershipRuntime(layout.StateDir),
	}
	n.readinessFanout = func(peers []announce.Peer, mine *pending, can func() bool, open func(time.Time, float64)) {
		safe.Go("nearest coordinator wake", func() {
			n.coordinatorWake(peers, mine, can, open)
		})
	}
	n.clockNow = n.electionTime
	n.membership.onGate = n.electionReset
	n.fanout = func(peers []announce.Peer, mine *pending) {
		if len(peers) > maxPeers {
			peers = peers[:maxPeers]
		}
		for _, p := range peers {
			safe.Go("nearest claim", func() {
				if post(p, mine.claim) {
					n.answeredElsewhere(mine, p.Name)
				}
			})
		}
	}
	n.on.OnCommand = func(on bool) {
		n.set(on)
		if err := config.Set().Nearest().On(on); err != nil {
			slog.Error("saving nearest device answers failed", "err", err)
		}
	}
	n.window.OnCommand = func(v float32) {
		n.window.Set(v)
		if err := config.Set().Nearest().WindowMs(int(v)); err != nil {
			slog.Error("saving the nearest device window failed", "err", err)
		}
	}
	n.offset.OnCommand = func(v float32) {
		n.offset.Set(v)
		if err := config.Set().Nearest().Offset(int(v)); err != nil {
			slog.Error("saving the nearest device offset failed", "err", err)
		}
	}
	n.mode.OnCommand = func(mode string) {
		if !validMode(mode) {
			slog.Warn("ignoring invalid nearest mode", "mode", mode)
			return
		}
		if err := config.Set().Nearest().Mode(mode); err != nil {
			slog.Error("saving nearest mode failed", "err", err)
			return
		}
		n.mode.Set(mode)
		n.settingsChanged()
	}
	web.Handle(path, "", n.open, n.receive)
	web.Handle(peerStatusPath, "", n.open, n.receivePeerStatus)
	n.registerMembership()
	web.Handle(v3Path, "", n.coordinationOpen, n.receiveReadiness)
	web.Handle(clockPath, "", n.clockOpen, n.receiveClock)
	web.Handle(coordinatorVotePath, "", n.coordinationOpen, func(w http.ResponseWriter, r *http.Request) {
		n.coordinatorReceive(w, r, false)
	})
	web.Handle(coordinatorWakePath, "", n.coordinationOpen, func(w http.ResponseWriter, r *http.Request) {
		n.coordinatorReceive(w, r, true)
	})
	return n
}

func (n *Nearest) Name() string { return "nearest device" }

func (n *Nearest) Entities() []esphome.Entity {
	return []esphome.Entity{n.on, n.window, n.offset, n.mode, n.membershipStatus}
}

func (n *Nearest) Restore(c config.Config) {
	n.set(c.Nearest.Enabled())
	n.window.Set(float32(c.Nearest.Window() / time.Millisecond))
	n.offset.Set(float32(c.Nearest.Offset))
	n.mode.Set(c.Nearest.EffectiveMode())
	n.mu.Lock()
	n.generation++
	n.mu.Unlock()
	slog.Info("restored", "what", n.on.ObjectID, "using", c.Nearest.Enabled())
}

// set turns taking part on or off. The loudness is measured from now on, so it has learned the room by
// the time a wake word comes; with no house word yet too, since nothing comes back here when one is set. A wake word already waiting on the others is still answered as it would
// have been: turning this off must not lose it.
func (n *Nearest) set(on bool) {
	if on {
		if config.Get().Nearest.EffectiveMode() == modeElected {
			if err := n.membership.open(); err != nil {
				slog.Error("opening nearest membership state failed", "err", err)
			}
		}
		warm(config.Get().Home.HouseWord)
		n.clockOnce.Do(func() { safe.Go("nearest coordinator clock", n.clockLoop) })
		n.coordinatorOnce.Do(func() { safe.Go("nearest coordinator", n.coordinatorLoop) })
		n.peerStatusOnce.Do(func() { safe.Go("nearest peer status", n.peerStatusLoop) })
		n.membershipOnce.Do(func() { safe.Go("nearest membership", n.membershipLoop) })
		if config.Get().Home.HouseWord == "" {
			slog.Warn("nearest device answers is on, but does nothing until this device has a house word")
		}
	}
	n.on.Set(on)
	mic.Get().SetStandout(on)
	n.mu.Lock()
	if n.enabled != on {
		n.generation++
	}
	n.enabled = on
	if !on {
		n.others = nil
	}
	n.mu.Unlock()
	web.Wake()
}

// open is whether claims are taken: only while this device takes part, can hear a wake word at all
// (not a Skylight), and the house has a word to sign them with.
func (n *Nearest) open() bool {
	n.mu.Lock()
	on := n.enabled
	n.mu.Unlock()
	return on && n.hears() && config.Get().Home.HouseWord != ""
}

// hearing is the others that could claim a wake word: a device without microphones (a Skylight) never
// does, and waiting on it would only cost the window. A device on an older release says nothing either
// way and is asked; one that had this off, as it was by default, still costs the window until it is
// updated.
func hearing(peers []announce.Peer) []announce.Peer {
	var out []announce.Peer
	for _, p := range peers {
		if p.Hears() {
			out = append(out, p)
		}
	}
	return out
}

// Woke is a wake word heard here, in place of voice's Woke. It opens the turn at once unless this device
// takes part, could answer, and has somebody to ask who could hear it too; otherwise it claims the word, and opens the turn
// after its configured window only if no device claimed it better or had already answered it.
func (n *Nearest) Woke(slot int) {
	n.contend(n.wakeWord(slot), n.confidenceOf(slot), n.now(),
		func() bool { return n.canAnswer(slot) },
		func(at time.Time, score float64) { n.won(slot, at, score) },
		func() { n.wokeNow(slot) })
}

// WokeGated is Woke for a wake word whose turn may still be called off (detect's contest with a command
// word): the claim goes out at once, as Woke's does, and gate decides whether the turn opens once this
// device has won it, sending what was said since at from the history.
func (n *Nearest) WokeGated(slot int, at time.Time, gate func(open func())) {
	n.contend(n.wakeWord(slot), n.confidenceOf(slot), at,
		func() bool { return n.canAnswer(slot) },
		func(at time.Time, score float64) { gate(func() { n.won(slot, at, score) }) },
		func() { gate(func() { n.woke(slot, at) }) })
}

// SetConfidence tells nearest where to read the wake-word model's score for a slot's latest detection.
func (n *Nearest) SetConfidence(f func(slot int) float64) {
	n.mu.Lock()
	n.confidence = f
	n.mu.Unlock()
}

func (n *Nearest) confidenceOf(slot int) float64 {
	n.mu.Lock()
	f := n.confidence
	n.mu.Unlock()
	if f == nil {
		return 0
	}
	return f(slot)
}

// won is a wake word this device won, having heard it with score (pending.clarity). One under Confirm below
// nearest-device score is asked about first (#503): a TV across the room can score as highly on the
// model as somebody at the device, and far lower on how clearly it stood out here, while a person in
// another room scores low here too, so it is asked about rather than dropped. Only the first wake word's:
// Home Assistant is asked about that one alone. A word with nobody to compare with never gets here.
func (n *Nearest) won(slot int, at time.Time, score float64) {
	if below := config.Get().Wake.OnHA.FaintBelow(); below > 0 && slot == 0 && score < below {
		slog.Info("nearest device: heard it faintly, asking home assistant", "score", round(score), "below", below)
		n.faint(slot, at)
		return
	}
	n.woke(slot, at)
}

// Answer is the same question for something other than a wake word, the next song word: of the devices
// that heard word, only the one that heard it best acts. It acts at once when this device does not take
// part, cannot act, or has nobody to ask, and only the nearest does otherwise. What it does is act, run
// after the window, and the others hear nothing of it. Word names what was said, so it is not taken for
// a wake word or for another word.
func (n *Nearest) Answer(word string, can func() bool, act func()) {
	n.contend(word, 0, n.now(), can, func(time.Time, float64) { act() }, act)
}

// contend is the claim every word goes through. open runs after the window, with the time the word was
// heard and this device's score for it, if this device is the nearest; now runs in its place when there
// is nobody to ask. A turn that is already open is not claimed over.
func (n *Nearest) contend(word string, confidence float64, at time.Time, can func() bool, open func(time.Time, float64), now func()) {
	if !n.open() {
		now()
		return
	}
	c := config.Get()
	if c.Nearest.EffectiveMode() != modeExisting && !n.peerModesReady(c.Home.HouseWord, c.Nearest.EffectiveMode()) {
		slog.Info("nearest peer modes unavailable or mismatched; not answering")
		return
	}
	if n.busy() || !can() {
		if c.Nearest.EffectiveMode() == modeExisting {
			now()
		} else if n.busy() {
			// The claim keeps the others quiet; the word is still this device's to act on, which is
			// what interrupts a reply it is speaking (voice: a wake while speaking takes over). If
			// another device wins the claim, busyLost ends the turn anyway.
			n.busyClaim(c, word, confidence, at)
			now()
		}
		return
	}
	// A committed membership of this device alone is a majority of itself: it answers without peers or
	// clock sync, but only while no other device is visible on this house word. The release fence still
	// applies, and the peer-mode check above has already refused any visible device in another mode.
	if c.Nearest.EffectiveMode() == modeElected {
		if s := n.membership.snapshot(); s.Ready && len(s.Members) == 1 && s.Members[0] == s.DeviceID {
			if len(hearing(n.peers())) > 0 {
				slog.Info("nearest elected coordinator: other devices visible but not in group; not answering", "word", word)
				return
			}
			slog.Info("nearest elected coordinator: sole member; answering", "word", word)
			if !n.membership.releaseIfCurrent(s.Token, func() { open(at, n.score()) }) {
				slog.Info("nearest device: membership changed before release; dropping the wake", "word", word)
			}
			return
		}
	}
	peers := limitPeers(hearing(n.peers()))
	if len(peers) == 0 {
		if c.Nearest.EffectiveMode() == modeExisting {
			now()
		} else {
			slog.Info("nearest elected coordinator: no peers; not answering")
		}
		return
	}

	window := c.Nearest.Window()
	clarity := n.score()
	protocol := 0
	var eventMembers []string
	var cohortPeers []announce.Peer
	var electedToken membershipToken
	electionAt := at
	if c.Nearest.EffectiveMode() != modeExisting {
		if active, selected, corrected, token, ok := n.electedCohort(c, peers, at); ok {
			protocol = coordinationProtocolVersion
			eventMembers = active
			cohortPeers = selected
			electionAt = corrected
			electedToken = token
		}
	}
	if c.Nearest.EffectiveMode() != modeExisting && protocol != coordinationProtocolVersion {
		slog.Info("nearest coordinator settings, membership, or clock unavailable; not answering", "word", word)
		return
	}
	own := n.claimOf(c, word, confidence, clarity, electionAt, eventMembers, protocol)
	own.Cluster, own.Epoch, own.Fingerprint = electedToken.cluster, electedToken.epoch, electedToken.fingerprint
	mine := &pending{
		claim: own,
		at:    at, window: window, clarity: clarity, mode: c.Nearest.EffectiveMode(),
		membership: electedToken,
	}
	n.mu.Lock()
	mine.generation = n.generation
	fresh := n.claims[:0]
	for _, p := range n.claims {
		if n.now().Sub(p.at) < kept {
			fresh = append(fresh, p)
		}
	}
	n.mine = mine
	if mine.Protocol == coordinationProtocolVersion {
		n.claims = append(fresh, mine)
	}
	n.mu.Unlock()

	if protocol == coordinationProtocolVersion {
		if n.pendingObserver != nil {
			n.pendingObserver(mine)
		}
		n.readinessFanout(cohortPeers, mine, can, open)
	} else {
		n.fanout(peers, mine)
		n.after(window, func() { n.decide(word, mine, can, open) })
	}
}

// measured is how this device measured the word: only both numbers, so a claim is either measured fully or
// compared as an older one is.
func (n *Nearest) measured(confidence float64) (*float64, *float64) {
	if confidence > 0 && n.energy != nil {
		e := n.energy()
		if finiteScore(confidence) && finiteScore(e) {
			return &confidence, &e
		}
	}
	return nil, nil
}

// busyClaim tells the coordinator that this device heard word while its own turn is open. The device does
// not act on the answer: it is already in a turn. The claim is there so that, when this device heard the
// word better than the others, they stay quiet, as they would had it been free. A word said in another room
// is still won by the device nearest to it. Classic mode has no coordinator and sends nothing.
func (n *Nearest) busyClaim(c config.Config, word string, confidence float64, at time.Time) {
	switch c.Nearest.EffectiveMode() {
	case modeElected:
		n.busyClaimElected(c, word, confidence, at, false)
	}
}

// Played is a wake word this device's own speaker played, heard at: the playback watcher (detect's
// played.go) found it in what was sent to the card. Echo cancellation takes this device's own sound out
// of what its microphones hear, so it would never claim the word itself, and a device in the next room
// that heard it through the wall would win by default. A played claim beats every claim that is not
// one (better), and the device that sent it never acts on it, so the word its speaker said opens no
// turn anywhere. A word somebody says in another room while this device talks is not caught up in it:
// the claim is only sent when the speaker itself said a wake word. Classic mode sends nothing.
func (n *Nearest) Played(word string, at time.Time) {
	if !n.open() || word == "" {
		return
	}
	c := config.Get()
	if c.Nearest.EffectiveMode() == modeExisting || !n.peerModesReady(c.Home.HouseWord, c.Nearest.EffectiveMode()) {
		return
	}
	switch c.Nearest.EffectiveMode() {
	case modeElected:
		n.busyClaimElected(c, word, 0, at, true)
	}
}

// busyLost is the answer to this device's busy claim mine: when another device won the word, the person
// said the wake word again and the device nearer them now has it, so the turn still open here is ended
// rather than left to answer over that one. A played claim ends nothing: the speaker said the word, not a
// person. A winner that is this device, or no winner, leaves the turn as it is.
func (n *Nearest) busyLost(mine, winner claim) {
	if mine.Played || winner.From == "" || strings.EqualFold(winner.From, mine.From) || !n.busy() {
		return
	}
	slog.Info("nearest device: the wake word was said again and another has it; ending this turn",
		"word", mine.Word, "by", winner.From, "mine", round(mine.Score), "theirs", round(winner.Score))
	n.endTurn()
}

// busyNote is what the log says once a busy or played claim has gone.
func busyNote(played bool) string {
	if played {
		return "nearest device: own speaker said the wake word, claim sent so that the others stay quiet"
	}
	return "nearest device: busy, claim sent so that the others stay quiet"
}

// claimOf is this device's claim for word, heard at the given time: its score plus the configured offset and
// the bonus for a call, and how it measured the word, where it could.
func (n *Nearest) claimOf(c config.Config, word string, confidence, clarity float64, heard time.Time, members []string, protocol int) claim {
	confidencePtr, energy := n.measured(confidence)
	return claim{From: c.Device.Name, Turn: newTurn(), Word: word,
		Score: clarity + float64(c.Nearest.Offset) + n.callBonus(), Protocol: protocol,
		Confidence: confidencePtr, Energy: energy, HeardAt: heard.UnixNano(), Members: members}
}

// electedCohort is the committed roster's active members, the peers among them, the time the word was heard
// on the shared clock and the membership token. ok is false while the roster, its quorum or the clock is not
// ready: nothing is claimed then.
func (n *Nearest) electedCohort(c config.Config, peers []announce.Peer, at time.Time) (active []string, selected []announce.Peer, heard time.Time, token membershipToken, ok bool) {
	configured, token, valid := n.electedRoster()
	if !valid {
		return nil, nil, time.Time{}, token, false
	}
	active = n.clock.active(configured, c.Device.Name, time.Now())
	selected, complete := rosterPeers(peers, active, c.Device.Name)
	if !complete || len(active) <= len(configured)/2 {
		return nil, nil, time.Time{}, token, false
	}
	heard, ready := n.clockNow(at)
	if !ready {
		return nil, nil, time.Time{}, token, false
	}
	return active, selected, heard, token, true
}

// busyClaimElected is the same for Device quorum: the claim goes to the elected leader, bound to the
// committed membership as any claim is, or into this device's own collection when it is the leader.
func (n *Nearest) busyClaimElected(c config.Config, word string, confidence float64, at time.Time, played bool) {
	active, selected, heard, token, ok := n.electedCohort(c, limitPeers(hearing(n.peers())), at)
	if !ok {
		return
	}
	mine := n.claimOf(c, word, confidence, n.score(), heard, active, coordinationProtocolVersion)
	mine.Busy, mine.Played = true, played
	mine.Cluster, mine.Epoch, mine.Fingerprint = token.cluster, token.epoch, token.fingerprint
	leader, term := n.coordinator.leaderFor(time.Now())
	if leader == "" {
		return
	}
	go func() {
		sent, decided := false, false
		var winner claim
		if strings.EqualFold(leader, c.Device.Name) {
			winner, decided = n.coordinatorCollect(mine, term, token)
			sent = decided
		} else {
			for _, peer := range selected {
				if strings.EqualFold(peer.Name, leader) {
					var resp coordinatorResponse
					resp, sent = n.coordinatorPost(peer, coordinatorWakePath, coordinatorRequest{
						From: c.Device.Name, Term: term, Members: active, Candidate: mine,
						Cluster: token.cluster, Epoch: token.epoch, Fingerprint: token.fingerprint,
					})
					winner, decided = resp.Winner, sent && resp.Granted
					break
				}
			}
		}
		if sent {
			slog.Info(busyNote(played), "word", word, "score", round(mine.Score))
		}
		if decided {
			n.busyLost(mine, winner)
		}
	}()
}

// pendingSettingsCurrentLocked keeps a result that arrives late from opening a turn after the mode, the
// membership or whether this device takes part has changed since the wake word was heard.
func (n *Nearest) pendingSettingsCurrentLocked(mine *pending) bool {
	cfg := config.Get()
	nearest := cfg.Nearest
	if nearest.EffectiveMode() != mine.mode {
		return false
	}
	if mine.mode != modeExisting && (mine.generation != n.generation || !n.enabled || !nearest.Enabled()) {
		return false
	}
	if mine.mode == modeElected && !n.membership.valid(mine.membership) {
		return false
	}
	if mine.mode != modeExisting && !n.peerModesReady(cfg.Home.HouseWord, mine.mode) {
		return false
	}
	return true
}

// decide is the end of the configured window: the turn opens unless a claim heard since a window before
// the word beat this one, or a device had already answered it. A claim delayed beyond this deadline can
// still arrive too late to prevent duplicate answers.
//
// Whether this device can still answer is asked again here: the window is long enough for Home Assistant
// to go, or a call to start. One that cannot does not claim the word as answered, so a later device that
// can is not told to stand down for it, and it is passed on all the same, to say so in the usual way.
func (n *Nearest) decide(word string, mine *pending, can func() bool, open func(time.Time, float64)) {
	defer func() {
		// Whatever went wrong here, the wake word is still somebody talking to a device.
		if r := recover(); r != nil {
			slog.Error("nearest device: deciding failed", "panic", r)
			if mine.Protocol != coordinationProtocolVersion {
				open(mine.at, mine.clarity)
			}
		}
	}()

	able := can()
	var best arrived
	var beaten, elsewhere, current, noVoice bool
	func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if !n.pendingSettingsCurrentLocked(mine) || (mine.Protocol == coordinationProtocolVersion && !mine.readinessComplete) {
			return
		}
		if mine.decided || ((mine.Protocol != coordinationProtocolVersion || !mine.readinessComplete) && n.mine != mine) {
			return
		}
		mine.decided = true
		current = true
		if mine.Protocol == coordinationProtocolVersion && mine.earlyWinner != nil {
			best = arrived{claim: *mine.earlyWinner, at: mine.at}
			// Keep pending wakes for distinct events independent. Only suppress another local
			// turn when the coordinator returned a different turn for this grouped event.
			sameDeviceOtherTurn := strings.EqualFold(best.From, mine.From) && best.Turn != mine.Turn
			beaten = sameDeviceOtherTurn || ((!strings.EqualFold(best.From, mine.From) || best.Turn != mine.Turn) && better(best.claim, mine.claim))
		} else if mine.Protocol == coordinationProtocolVersion && mine.readinessComplete {
			best, beaten = n.bestCoordinatedEvent(mine.claim)
		} else {
			best, beaten = n.bestSince(mine.at.Add(-mine.window), mine.claim)
		}
		elsewhere = mine.elsewhere
		noVoice = mine.mode != modeExisting && mine.clarity <= 0 && mine.Energy != nil && *mine.Energy > 0
		mine.answered = !beaten && !elsewhere && able && !noVoice
	}()
	if !current {
		return
	}

	switch {
	case elsewhere:
		slog.Info("nearest device: another had already answered", "word", word, "score", round(mine.Score))
	case beaten:
		slog.Info("nearest device: another heard it better", "word", word, "mine", round(mine.Score),
			"theirs", round(best.Score), "by", best.From)
	case noVoice:
		// Won, but no voice stood out here: the speech band was measured and nothing rose above the room.
		// Every claim that lost had less, so nobody near any device spoke the word, and a turn opened on it
		// would take Home Assistant's pipeline from the person who says it a moment later: Home Assistant
		// refuses their real wake as a duplicate.
		slog.Info("nearest device: won, but no voice stood out; not answering", "word", word, "score", round(mine.Score))
	case !able:
		slog.Info("nearest device: could not answer after all", "word", word)
		if mine.Protocol != coordinationProtocolVersion {
			open(mine.at, mine.clarity)
		}
	default:
		slog.Info("nearest device: answering", "word", word, "score", round(mine.Score), "clarity", round(mine.clarity),
			"waited", n.now().Sub(mine.at).Round(time.Millisecond))
		if mine.mode == modeElected {
			// The last check: the release is reserved against the live membership generation, and a fence
			// waits for it. A superseded generation drops the wake; nothing falls back.
			if !n.membership.releaseIfCurrent(mine.membership, func() { open(mine.at, mine.clarity) }) {
				slog.Info("nearest device: membership changed before release; dropping the wake", "word", word)
			}
		} else {
			open(mine.at, mine.clarity)
		}
	}
}

// onCallLead is added to a claim from a device on a call, in the same dB of standout the claims are
// made of. Enough to win a word another device heard about as well, the call being what somebody most
// likely meant; not so much that a word somebody said to a device in another room, and heard faintly
// here, goes to the call.
const onCallLead = 6

func (n *Nearest) callBonus() float64 {
	if n.onCall != nil && n.onCall() {
		return onCallLead
	}
	return 0
}

// bestSince is the best claim from another device since from, and whether it beats mine. Called with
// the lock held.
func (n *Nearest) bestSince(from time.Time, mine claim) (arrived, bool) {
	var best arrived
	found := false
	for _, a := range n.others {
		if a.at.Before(from) || a.Word != mine.Word {
			continue
		}
		if !found || better(a.claim, best.claim) {
			best, found = a, true
		}
	}
	return best, found && better(best.claim, mine)
}

// bestCoordinatedEvent is the best claim heard as part of the same event as mine, not just any claim for the
// same word. Called with the lock held.
func (n *Nearest) bestCoordinatedEvent(mine claim) (arrived, bool) {
	var best arrived
	found := false
	for _, a := range n.others {
		if !withinEvent(mine, a.claim) && !classicClaimCompetes(mine, time.Unix(0, mine.HeardAt), a) {
			continue
		}
		if !found || better(a.claim, best.claim) {
			best, found = a, true
		}
	}
	return best, found && better(best.claim, mine)
}

// classicClaimCompetes is whether a classic claim (from a device in Classic mode, or an older one) for the
// same word arrived close enough to a coordinated event to be the same wake. It stops the event being
// settled early. A classic claim carries no time it was heard, so it is matched on when it arrived.
func classicClaimCompetes(anchor claim, eventAt time.Time, a arrived) bool {
	return anchor.Protocol == coordinationProtocolVersion && a.Protocol != coordinationProtocolVersion &&
		a.Word == anchor.Word && absDuration(a.at.Sub(eventAt)) <= eventTolerance
}

// measured is whether a claim carries both measurements.
func (c claim) measured() bool { return c.Confidence != nil && c.Energy != nil }

// better is whether a heard the word better than b, the same on every device. Two measured claims go by
// Score (clarity, offset and call bonus) rounded to 1 dB so near-equal counts as a tie, then energy, then
// the model's confidence; a pair where either is from an older device without the measurements goes by
// Score unrounded. A tie goes to the name that sorts first, so two devices that tie still agree on one.
// Mixing older and newer devices can make the order non-transitive, so a house wants them all updated;
// until then each pair still has one answer.
func better(a, b claim) bool {
	if a.Played != b.Played {
		return a.Played
	}
	if a.measured() && b.measured() {
		if ra, rb := math.Round(a.Score), math.Round(b.Score); ra != rb {
			return ra > rb
		}
		if *a.Energy != *b.Energy {
			return *a.Energy > *b.Energy
		}
		if *a.Confidence != *b.Confidence {
			return *a.Confidence > *b.Confidence
		}
		return strings.ToLower(a.From) < strings.ToLower(b.From)
	}
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	return strings.ToLower(a.From) < strings.ToLower(b.From)
}

// answeredElsewhere is a device that had already answered telling this one so, for the claim mine.
func (n *Nearest) answeredElsewhere(mine *pending, by string) {
	n.mu.Lock()
	mine.elsewhere = true
	n.mu.Unlock()
	slog.Debug("nearest device: told the word was already answered", "by", by)
}

// took is a claim that arrived from another device. It answers whether this device has already
// answered the same wake word: a claim that comes after that, however much better, is too late, and the
// device that sent it stands down.
func (n *Nearest) took(c claim) (answeredHere bool) {
	now := n.now()
	n.mu.Lock()
	defer n.mu.Unlock()
	fresh := n.others[:0]
	for _, a := range n.others {
		if now.Sub(a.at) < kept {
			fresh = append(fresh, a)
		}
	}
	n.others = append(fresh, arrived{claim: c, at: now})
	if c.Protocol == coordinationProtocolVersion && config.Get().Nearest.EffectiveMode() != modeExisting {
		for _, p := range n.claims {
			if p.answered && withinEvent(p.claim, c) {
				return true
			}
		}
		return false
	}
	mine := n.mine
	return mine != nil && mine.answered && mine.Word == c.Word && now.Sub(mine.at) <= 2*mine.window
}

func round(v float64) float64 { return float64(int(v*10+0.5)) / 10 }

func newTurn() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// receive takes a claim from another device of this house.
func (n *Nearest) receive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		web.Refuse(w, "post a claim", http.StatusMethodNotAllowed)
		return
	}
	// A claim is this house's only: never one from away, over the tailnet.
	if web.AwayRequest(r) {
		http.NotFound(w, r)
		return
	}
	// The header first, and the body only with a deadline: nothing that has not signed is waited on.
	if err := looksSigned(r.Header.Get(authHeader), time.Now()); err != nil {
		warn(n.now(), "nearest device: a claim that does not check out", "from", r.RemoteAddr, "err", err)
		web.Refuse(w, "not this house", http.StatusForbidden)
		return
	}
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(bodyWithin))
	body, err := readBody(r)
	if err != nil {
		web.Refuse(w, "not a claim", http.StatusBadRequest)
		return
	}
	c := config.Get()
	if err := verify(c.Home.HouseWord, c.Device.Name, r.Header.Get(authHeader), body, time.Now()); err != nil {
		warn(n.now(), "nearest device: a claim that does not check out", "from", r.RemoteAddr, "err", err)
		web.Refuse(w, "not this house", http.StatusForbidden)
		return
	}
	var cl claim
	if err := json.Unmarshal(body, &cl); err != nil || cl.From == "" {
		web.Refuse(w, "not a claim", http.StatusBadRequest)
		return
	}
	if cl.From == c.Device.Name {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if n.took(cl) {
		w.Header().Set(answeredHeader, answeredMAC(c.Home.HouseWord, nonceOf(r.Header.Get(authHeader)), cl.From, c.Device.Name))
		http.Error(w, "answered here already", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// warned is when a warning about another device's claims or answers was last written, for warn.
var warned struct {
	sync.Mutex
	at time.Time
}

// warn logs, at most once every warnEvery: whatever is sending bad claims or unproven refusals may send
// them as fast as it likes.
func warn(now time.Time, msg string, args ...any) {
	warned.Lock()
	quiet := !warned.at.IsZero() && now.Sub(warned.at) < warnEvery
	if !quiet {
		warned.at = now
	}
	warned.Unlock()
	if !quiet {
		slog.Warn(msg, args...)
	}
}

// client sends claims. Connections are kept between wakes, so the next claim does not wait on a new
// one; the timeout is the longest window, after which a claim is of no use to anybody.
var client = &http.Client{
	Timeout: time.Duration(config.MaxNearestWindowMs) * time.Millisecond,
	Transport: &http.Transport{
		DialContext:         (&net.Dialer{Timeout: time.Duration(config.MaxNearestWindowMs) * time.Millisecond}).DialContext,
		MaxIdleConnsPerHost: 1,
		IdleConnTimeout:     10 * time.Minute,
	},
}

// post sends a claim to one other device, and answers whether that device said, with proof, that it had
// already answered the word. Anything else, failing to arrive included, says so in the log at most: the
// device answers without it, as it would have anyway.
func post(p announce.Peer, c claim) (answeredThere bool) {
	body, err := json.Marshal(c)
	if err != nil {
		slog.Warn("nearest device: a claim that could not be written", "err", err)
		return false
	}
	word := config.Get().Home.HouseWord
	ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
	defer cancel()
	url := "http://" + net.JoinHostPort(p.Address, fmt.Sprint(p.Port)) + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false
	}
	header := sign(word, p.Name, body, time.Now())
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(authHeader, header)
	resp, err := client.Do(req)
	if err != nil {
		slog.Debug("nearest device: claim not delivered", "to", p.Name, "err", err)
		return false
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		if answeredBy(word, header, resp.Header.Get(answeredHeader), c.From, p.Name) {
			return true
		}
		warn(time.Now(), "nearest device: told the word was answered, without proof; answering anyway", "by", p.Name)
		return false
	}
	if resp.StatusCode >= 300 {
		slog.Debug("nearest device: claim refused", "to", p.Name, "status", resp.Status)
	}
	return false
}
