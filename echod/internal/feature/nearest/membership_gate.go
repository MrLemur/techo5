package nearest

import (
	"github.com/HuskerMinion/techo5/echod/internal/config"
)

// Four separate questions, answered separately so that joining cannot deadlock on its own gates:
//
//	endpoint available   n.open()                      the feature is on and has a house word
//	membership control   n.membershipControlOpen()     Device quorum selected; status, join, fence, history
//	clock sampling       n.clockOpen()                 the same, and continues while a fence is pending
//	wake/election        n.coordinationOpen()          membership ready: fresh generation, no fence, quiet time done
//
// Lock ordering: membershipRuntime.tx, then .gate, then .mu, then Nearest.mu and coordinatorRuntime.mu.
// No callback or network I/O runs while any of them is held. A final wake release is
// membershipRuntime.releaseIfCurrent: it reserves the generation, runs the voice callback with no lock
// held, and a fence waits for every reserved release before it acknowledges.
func (n *Nearest) clockOpen() bool { return n.membershipControlOpen() }

func (n *Nearest) coordinationOpen() bool {
	return n.membershipControlOpen() && n.membership.snapshot().Ready
}

// electedRoster is the committed membership as election names, with the token every elected message,
// grant, lease and release is bound to. The denominator is always the whole committed roster.
func (n *Nearest) electedRoster() ([]string, membershipToken, bool) {
	snapshot := n.membership.snapshot()
	if !snapshot.Ready {
		return nil, membershipToken{}, false
	}
	names, ok := n.membership.namesFor(snapshot.Members, config.Get().Device.Name)
	return names, snapshot.Token, ok
}

// clockRoster is the committed roster for sampling, available while the election gate is closed.
func (n *Nearest) clockRoster() ([]string, bool) {
	ids := n.membership.committedMembers()
	if len(ids) == 0 {
		return nil, false
	}
	return n.membership.namesFor(ids, config.Get().Device.Name)
}

// electionReset discards everything RAM-only that belongs to a previous membership generation. Votes
// are refused for a full grant afterwards, as after a restart.
func (n *Nearest) electionReset() {
	n.coordinator.reset(timeNow())
	n.clock.resetSamples()
}
