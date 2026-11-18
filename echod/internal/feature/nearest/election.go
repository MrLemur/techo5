package nearest

import (
	"log/slog"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/HuskerMinion/techo5/echod/internal/feature/announce"
)

const (
	coordinationProtocolVersion = 7

	// eventCollectionWindow is how long the coordinator collects claims for one event. It does not follow
	// Nearest device window, which is Classic mode's wait.
	eventCollectionWindow = 150 * time.Millisecond

	// readinessDeadline caps the readiness gate; a slower or incomplete exchange is not answered.
	readinessDeadline = 400 * time.Millisecond

	// maximumClockSkew is the largest clock-offset interval accepted for an early decision. The
	// interval is measured from the HTTP request/response timestamps; wider uncertainty is refused.
	maximumClockSkew = 50 * time.Millisecond

	// eventTolerance is how far apart two claims may say they heard a word and be one fixed-window event.
	eventTolerance = eventCollectionWindow + maximumClockSkew

	// concurrentEventTolerance is how far apart two claims may say they heard a word and still be one event
	// when they arrive while it is being collected. A device whose clock estimate is a little off would
	// otherwise open an event of its own, and a lone claim wins by default: two devices would then both be
	// told to answer one word.
	concurrentEventTolerance = time.Second
)

type readiness struct {
	Version    int      `json:"version"`
	From       string   `json:"from"`
	Members    []string `json:"members"`
	Candidates []claim  `json:"candidates"`
	Ready      bool     `json:"ready"`
	ReceivedAt int64    `json:"received_at"`
	SentAt     int64    `json:"sent_at"`

	localSent     time.Time `json:"-"`
	localReceived time.Time `json:"-"`
}

// members is the frozen set of devices eligible for this election, including this device.
func members(me string, peers []announce.Peer) []string {
	out := []string{strings.ToLower(me)}
	for _, p := range hearing(peers) {
		out = append(out, strings.ToLower(p.Name))
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// rosterPeers is the announced peers named in memberNames, and whether every other member was found.
func rosterPeers(peers []announce.Peer, memberNames []string, me string) ([]announce.Peer, bool) {
	if len(memberNames) < 2 || len(memberNames) > maxPeers+1 {
		return nil, false
	}
	byName := make(map[string]announce.Peer)
	for _, p := range hearing(peers) {
		name := strings.ToLower(strings.TrimSpace(p.Name))
		if name == "" || byName[name].Name != "" {
			return nil, false
		}
		byName[name] = p
	}
	out := make([]announce.Peer, 0, len(memberNames)-1)
	for _, name := range memberNames {
		if name == strings.ToLower(strings.TrimSpace(me)) {
			continue
		}
		p, ok := byName[name]
		if !ok {
			return nil, false
		}
		out = append(out, p)
	}
	return out, true
}

func sameMembers(a, b []string) bool {
	return slices.Equal(a, b)
}

func withinEvent(a, b claim) bool {
	if a.Protocol != coordinationProtocolVersion || b.Protocol != coordinationProtocolVersion || a.Word != b.Word ||
		a.HeardAt == 0 || b.HeardAt == 0 {
		return false
	}
	atA, atB := time.Unix(0, a.HeardAt), time.Unix(0, b.HeardAt)
	return absDuration(atA.Sub(atB)) <= eventTolerance
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// clockInterval is whether the clock offset measured over one exchange is known to within
// maximumClockSkew. A slow or lopsided exchange is not, and its event is not settled early.
func clockInterval(localSent, localReceived time.Time, r readiness) bool {
	if r.ReceivedAt == 0 || r.SentAt == 0 || !localReceived.After(localSent) {
		return false
	}
	remoteReceived := time.Unix(0, r.ReceivedAt)
	remoteSent := time.Unix(0, r.SentAt)
	lower := remoteSent.Sub(localReceived)
	upper := remoteReceived.Sub(localSent)
	return lower <= upper && lower >= -maximumClockSkew && upper <= maximumClockSkew
}

// earlyWinner settles an event early only when every member reported the same claims and every
// exchange measured the clock offset within maximumClockSkew.
func earlyWinner(mine claim, localMembers []string, reports []readiness) (claim, bool) {
	if mine.Protocol != coordinationProtocolVersion || len(localMembers) == 0 || len(reports) != len(localMembers)-1 {
		return claim{}, false
	}
	seen := map[string]bool{strings.ToLower(mine.From): true}
	var candidates []claim
	for _, r := range reports {
		from := strings.ToLower(r.From)
		if !r.Ready {
			slog.Info("nearest readiness rejected", "peer", r.From, "reason", "peer not ready")
			return claim{}, false
		}
		if from == "" || seen[from] || !slices.Contains(localMembers, from) || !sameMembers(r.Members, localMembers) {
			slog.Info("nearest readiness rejected", "peer", r.From, "reason", "membership mismatch")
			return claim{}, false
		}
		if !clockInterval(r.localSent, r.localReceived, r) {
			lower := time.Unix(0, r.SentAt).Sub(r.localReceived)
			upper := time.Unix(0, r.ReceivedAt).Sub(r.localSent)
			slog.Info("nearest readiness rejected", "peer", r.From, "reason", "clock bound",
				"roundtrip_ms", r.localReceived.Sub(r.localSent).Milliseconds(),
				"offset_lower_ms", lower.Milliseconds(), "offset_upper_ms", upper.Milliseconds(),
				"server_processing_ms", time.Duration(r.SentAt-r.ReceivedAt).Milliseconds())
			return claim{}, false
		}
		if len(r.Candidates) > len(localMembers) {
			slog.Info("nearest readiness rejected", "peer", r.From, "reason", "too many candidates")
			return claim{}, false
		}
		seen[from] = true
		for _, c := range r.Candidates {
			if c.Protocol != coordinationProtocolVersion || !finiteClaim(c) || c.Turn == "" ||
				!strings.EqualFold(c.Word, mine.Word) || !slices.Contains(localMembers, strings.ToLower(c.From)) {
				return claim{}, false
			}
			if withinEvent(mine, c) && !containsClaim(candidates, c) {
				candidates = append(candidates, c)
			}
		}
	}
	if len(seen) != len(localMembers) {
		return claim{}, false
	}
	if !containsClaim(candidates, mine) {
		candidates = append(candidates, mine)
	}
	// Every peer must report exactly the same grouped candidates. A missing, delayed, or divergent claim
	// makes the result inconclusive and the wake is not answered.
	for _, r := range reports {
		if !sameCandidates(mine, candidates, r.Candidates) {
			slog.Info("nearest readiness rejected", "peer", r.From, "reason", "candidate mismatch", "expected_count", len(candidates), "reported_count", len(r.Candidates))
			return claim{}, false
		}
	}
	best := candidates[0]
	for _, c := range candidates[1:] {
		if better(c, best) {
			best = c
		}
	}
	return best, true
}

func containsClaim(claims []claim, target claim) bool {
	for _, c := range claims {
		if strings.EqualFold(c.From, target.From) && c.Turn == target.Turn {
			return true
		}
	}
	return false
}

func sameCandidates(anchor claim, a, b []claim) bool {
	filter := func(in []claim) []claim {
		out := compactEventClaims(anchor, in)
		slices.SortFunc(out, func(a, b claim) int {
			if v := strings.Compare(strings.ToLower(a.From), strings.ToLower(b.From)); v != 0 {
				return v
			}
			return strings.Compare(a.Turn, b.Turn)
		})
		return out
	}
	x, y := filter(a), filter(b)
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if !strings.EqualFold(x[i].From, y[i].From) || x[i].Turn != y[i].Turn ||
			x[i].Score != y[i].Score || x[i].HeardAt != y[i].HeardAt {
			return false
		}
	}
	return true
}

func finiteScore(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// finiteClaim is whether every number a claim carries can be compared.
func finiteClaim(c claim) bool {
	for _, v := range []*float64{c.Confidence, c.Energy} {
		if v != nil && !finiteScore(*v) {
			return false
		}
	}
	return finiteScore(c.Score)
}
