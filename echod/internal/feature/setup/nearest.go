package setup

import (
	"errors"
	"fmt"
	"html"
	"net/http"
	"strconv"
	"strings"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/nearest"
)

func init() {
	saves["nearest-mode"] = saveNearestMode
	saves["nearest-op"] = saveNearestOp
}

var nearestModes = []struct{ value, label string }{
	{"off", "Off (every device answers)"},
	{"classic", "Classic"},
	{"device_quorum", "Device quorum"},
}

// blockedText turns the machine-readable reason into a sentence. The reason itself stays the one the
// device reports to its peers.
func blockedText(reason string) string {
	switch reason {
	case "":
		return ""
	case "uninitialised", "discovering":
		return "Looking for the other devices. A device that is the first to be set up creates the group after about ten seconds."
	case "authority_offline":
		return "The group's membership authority is not in sight. Joining waits for it, or replace the authority from a device that is already in the group."
	case "authority_unreachable":
		return "The membership authority is in sight but did not answer."
	case "multiple_clusters":
		return "More than one group is in sight. Resolve this from a device that is already in a group."
	case "ambiguous_identity":
		return "Two devices report the same identity, so none of them is used until that is fixed."
	case "no_majority":
		return "A majority of the group is not reachable, so it cannot be changed."
	case "fencing":
		return "A membership change is in progress. Wakes are dropped until it finishes."
	case "joining":
		return "Joining the group."
	case "quiet_period":
		return "Waiting out the longest old vote after a restart or change. Wakes are dropped until it ends."
	case "settings_changed":
		return "Settings changed, so the group is being re-checked."
	case "conflict":
		return "Two groups of this house have met. Wakes are dropped until you choose which one to keep."
	case "retired":
		return "This device's group was retired. It can join the group you kept."
	case "removed":
		return "This device was removed from the group."
	case "state_missing":
		return "This device was in a group but its saved membership is missing. It will not start a new group by itself."
	case "storage_error":
		return "The membership could not be saved. Wakes are dropped and the same write is retried."
	}
	if rest, ok := strings.CutPrefix(reason, "pending:"); ok {
		return "A membership change has not finished (" + rest + "). It will be retried unchanged."
	}
	return reason
}

func nearestSection(w http.ResponseWriter, token string) {
	n := nearest.Get()
	v := n.MembershipView()
	fmt.Fprint(w, `<fieldset><legend>Nearest device</legend><form method="post" action="/setup/save">`)
	hidden(w, token, "nearest-mode")
	fmt.Fprint(w, `<label for="nearest-mode">Mode</label><select id="nearest-mode" name="mode">`)
	current := v.Mode
	if !config.Get().Nearest.Enabled() {
		current = "off"
	}
	for _, m := range nearestModes {
		sel := ""
		if m.value == current {
			sel = " selected"
		}
		fmt.Fprintf(w, `<option value="%s"%s>%s</option>`, m.value, sel, m.label)
	}
	c := config.Get().Nearest
	fmt.Fprintf(w, `</select>
	 <label for="nearest-window">Window (ms)</label><input id="nearest-window" name="window" type="number" min="%d" max="%d" step="10" value="%d">
	 <p class="note">How long a device waits to hear from the others. Raise it if two devices sometimes both answer; lower it if the wait is noticeable.</p>
	 <label for="nearest-offset">Offset (dB)</label><input id="nearest-offset" name="offset" type="number" min="%d" max="%d" value="%d">
	 <p class="note">Raise a device that loses when you speak right beside it; lower one that answers from across the room.</p>`,
		config.MinNearestWindowMs, config.MaxNearestWindowMs, c.Window().Milliseconds(),
		config.MinNearestOffset, config.MaxNearestOffset, c.Offset)
	fmt.Fprint(w, `
	 <p class="note">Off answers every wake word this device hears, so two devices may both answer. Classic is the default and works with older devices. <strong>Update every device
	  before choosing Device quorum.</strong> A device on an older release does not
	  know the new rules, and a device in another mode makes the others drop the wake word instead of
	  answering it.</p>
	 <p class="note">In Device quorum a wake word that cannot be settled is
	  dropped, never answered anyway. The devices trust each other on this network: the house word proves
	  a message came from a device that has it, nothing is encrypted, and a device that has the word could
	  pretend to be another.</p>
	 <p><button type="submit">Save</button></p></form>`)
	if v.Mode == "device_quorum" || v.Own.ID != "" {
		nearestStatus(w, token, v)
	}
	fmt.Fprint(w, `</fieldset>`)
}

func nearestStatus(w http.ResponseWriter, token string, v nearest.MembershipView) {
	fmt.Fprintf(w, `<h4>Device quorum</h4><p><strong>%s</strong>`, html.EscapeString(strings.ReplaceAll(v.Lifecycle, "_", " ")))
	if text := blockedText(v.Blocked); text != "" {
		fmt.Fprintf(w, ` — %s`, html.EscapeString(text))
	}
	fmt.Fprint(w, `</p>`)
	if v.NotAnswering != "" {
		fmt.Fprintf(w, `<p class="note">Not answering wakes right now: <strong>%s</strong>.</p>`, html.EscapeString(v.NotAnswering))
	}
	if v.Mode != "device_quorum" {
		fmt.Fprint(w, `<p class="note">Device quorum is not selected. The group this device belongs to is kept, and choosing Device quorum again resumes it.</p>`)
	}
	if v.Operation.Name != "" {
		switch {
		case v.Operation.Running:
			fmt.Fprintf(w, `<p class="note">Working on: %s…</p>`, html.EscapeString(v.Operation.Name))
		case v.Operation.Error != "":
			fmt.Fprintf(w, `<p class="note"><strong>%s did not finish:</strong> %s</p>`, html.EscapeString(v.Operation.Name), html.EscapeString(v.Operation.Error))
		default:
			fmt.Fprintf(w, `<p class="note">%s finished.</p>`, html.EscapeString(v.Operation.Name))
		}
	}
	if v.Own.ID != "" {
		nearestCluster(w, token, v, v.Own, "This device's group")
	}
	for _, c := range v.Conflicts {
		nearestCluster(w, token, v, c, "Another group of this house")
	}
	if v.Own.ID != "" {
		fmt.Fprintf(w, `<p class="note">Membership authority: <strong>%s</strong>, who adds and removes devices. Wake coordinator: <strong>%s</strong>, who settles wakes.</p>`,
			html.EscapeString(firstOr(v.Own.Authority, "unknown")), html.EscapeString(firstOr(v.Coordinator, "none right now")))
		fmt.Fprintf(w, `<p class="note">Epoch %d. A majority of %d devices (%d needed) must be reachable to change the group; %d are reachable now. A device that is off still counts as a member.</p>`,
			v.Own.Epoch, len(v.Own.Members), v.Own.Majority, v.Own.Reachable)
	}
	if v.Can.Retry {
		fmt.Fprint(w, `<form method="post" action="/setup/save">`)
		hidden(w, token, "nearest-op")
		fmt.Fprintf(w, `<input type="hidden" name="op" value="retry"><input type="hidden" name="tx" value="%s">
		 <p class="note">A change to the group started but has not finished. Retrying carries on with the same change.</p>
		 <p><button type="submit">Retry pending change</button></p></form>`, html.EscapeString(v.Pending))
	}
	if v.Can.ReplaceAuthority {
		fmt.Fprint(w, `<form method="post" action="/setup/save">`)
		hidden(w, token, "nearest-op")
		fmt.Fprintf(w, `<input type="hidden" name="op" value="authority"><input type="hidden" name="cluster" value="%s"><input type="hidden" name="epoch" value="%d">
		 <label for="nearest-authority">Replace membership authority</label><select id="nearest-authority" name="device">`, html.EscapeString(v.Own.ID), v.Own.Epoch)
		for _, m := range v.Own.Members {
			if !m.Authority {
				fmt.Fprintf(w, `<option value="%s">%s</option>`, html.EscapeString(m.ID), html.EscapeString(memberLabel(m)))
			}
		}
		fmt.Fprint(w, `</select><p class="note">Use it when the authority is gone. It needs a majority of the group, not the old authority, and keeps every member.</p>
		 <p><button type="submit">Replace authority</button></p></form>`)
	}
	if v.Can.Remove {
		fmt.Fprint(w, `<form method="post" action="/setup/save">`)
		hidden(w, token, "nearest-op")
		fmt.Fprintf(w, `<input type="hidden" name="op" value="remove"><input type="hidden" name="cluster" value="%s"><input type="hidden" name="epoch" value="%d">
		 <label for="nearest-remove">Remove device</label><select id="nearest-remove" name="device">`, html.EscapeString(v.Own.ID), v.Own.Epoch)
		for _, m := range v.Own.Members {
			if !m.Authority {
				fmt.Fprintf(w, `<option value="%s">%s</option>`, html.EscapeString(m.ID), html.EscapeString(memberLabel(m)))
			}
		}
		fmt.Fprint(w, `</select>
		 <p><label><input type="checkbox" name="confirm" value="yes" style="width:auto"> Remove the device chosen above from the group</label></p>
		 <p class="note">Devices are never removed for being off. To remove the authority, replace it first.</p>
		 <p><button type="submit">Remove device</button></p></form>`)
	}
	if v.Can.ReEnrol {
		fmt.Fprint(w, `<form method="post" action="/setup/save">`)
		hidden(w, token, "nearest-op")
		fmt.Fprint(w, `<input type="hidden" name="op" value="reenrol">
		 <p class="note">Exactly one kept group has to be in sight. This device keeps a copy of its old group's record so it cannot come back to it by restarting.</p>
		 <p><button type="submit">Join the kept group</button></p></form>`)
	}
}

func nearestCluster(w http.ResponseWriter, token string, v nearest.MembershipView, c nearest.ClusterView, title string) {
	fmt.Fprintf(w, `<h5>%s</h5><p class="note">Group %s, epoch %d, authority %s. %d of %d reachable.</p><ul>`,
		html.EscapeString(title), html.EscapeString(c.ID), c.Epoch, html.EscapeString(firstOr(c.Authority, "unknown")), c.Reachable, len(c.Members))
	for _, m := range c.Members {
		state := "not in sight"
		if m.Reachable {
			state = "reachable"
		}
		fmt.Fprintf(w, `<li>%s — %s</li>`, html.EscapeString(memberLabel(m)), state)
	}
	fmt.Fprint(w, `</ul>`)
	if v.Can.Keep {
		fmt.Fprint(w, `<form method="post" action="/setup/save">`)
		hidden(w, token, "nearest-op")
		fmt.Fprintf(w, `<input type="hidden" name="op" value="keep"><input type="hidden" name="cluster" value="%s"><input type="hidden" name="epoch" value="%d">
		 <p><label><input type="checkbox" name="confirm" value="yes" style="width:auto"> Keep group %s at epoch %d and retire the others</label></p>
		 <p><button type="submit">Keep this group</button></p></form>`,
			html.EscapeString(c.ID), c.Epoch, html.EscapeString(c.ID), c.Epoch)
	}
}

func memberLabel(m nearest.MemberView) string {
	name := firstOr(m.Name, m.ID)
	if m.Self {
		name += " (this device)"
	}
	if m.Authority {
		name += " — authority"
	}
	return name
}

func firstOr(s, fallback string) string {
	if s != "" {
		return s
	}
	return fallback
}

func saveNearestMode(r *http.Request) string {
	n := nearest.Get()
	c := config.Get().Nearest
	window, offset := int(c.Window().Milliseconds()), c.Offset
	if s := strings.TrimSpace(r.PostFormValue("window")); s != "" {
		var err error
		if window, err = strconv.Atoi(s); err != nil {
			return "the window must be a number"
		}
	}
	if s := strings.TrimSpace(r.PostFormValue("offset")); s != "" {
		var err error
		if offset, err = strconv.Atoi(s); err != nil {
			return "the offset must be a number"
		}
	}
	mode := r.PostFormValue("mode")
	if mode != "off" {
		if err := n.SetMode(mode); err != nil {
			return "could not change the mode: " + err.Error()
		}
	}
	n.SetBasics(mode != "off", window, offset)
	return ""
}

// saveNearestOp runs one recovery control. The session and CSRF checks are the setup page's own, done
// before any save runs; each control then re-checks the capability snapshot and the cluster and epoch the
// page showed, so a stale form is refused.
func saveNearestOp(r *http.Request) string {
	n := nearest.Get()
	epoch, _ := strconv.ParseUint(r.PostFormValue("epoch"), 10, 64)
	cluster, device := r.PostFormValue("cluster"), r.PostFormValue("device")
	var err error
	switch r.PostFormValue("op") {
	case "retry":
		err = n.DoRetryPending(r.PostFormValue("tx"))
	case "authority":
		err = n.DoReplaceAuthority(cluster, epoch, device)
	case "remove":
		if r.PostFormValue("confirm") != "yes" {
			return "tick the box to confirm which device is removed"
		}
		err = n.DoRemoveDevice(cluster, epoch, device)
	case "keep":
		if r.PostFormValue("confirm") != "yes" {
			return "tick the box to confirm which group is kept"
		}
		err = n.DoKeepCluster(cluster, epoch)
	case "reenrol":
		err = n.DoReEnrol()
	default:
		return "unknown action"
	}
	if errors.Is(err, nearest.ErrStale) {
		return "the group changed since this page was loaded; check it and try again"
	}
	if err != nil {
		return err.Error()
	}
	return ""
}
