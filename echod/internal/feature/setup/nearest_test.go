package setup

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/HuskerMinion/techo5/echod/internal/config"
	"github.com/HuskerMinion/techo5/echod/internal/feature/nearest"
)

func nearestForm(values url.Values) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/setup/save", strings.NewReader(values.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	_ = r.ParseForm()
	return r
}

// A destructive control needs its confirmation, and a control the device does not currently allow is
// refused by the server whatever the form says: the page and the server use one capability snapshot.
func TestNearestControlsRefuseUnconfirmedAndUnavailable(t *testing.T) {
	for _, form := range []url.Values{
		{"op": {"remove"}, "cluster": {"c"}, "epoch": {"1"}, "device": {"x"}},
		{"op": {"keep"}, "cluster": {"c"}, "epoch": {"1"}},
		{"op": {"remove"}, "confirm": {"yes"}, "cluster": {"c"}, "epoch": {"1"}, "device": {"x"}},
		{"op": {"keep"}, "confirm": {"yes"}, "cluster": {"c"}, "epoch": {"1"}},
		{"op": {"authority"}, "cluster": {"c"}, "epoch": {"1"}, "device": {"x"}},
		{"op": {"retry"}, "tx": {"t"}},
		{"op": {"reenrol"}},
		{"op": {"bogus"}},
	} {
		if problem := saveNearestOp(nearestForm(form)); problem == "" {
			t.Errorf("form %v was accepted on a device with no group", form)
		}
	}
}

func TestNearestModeRejectsUnknownMode(t *testing.T) {
	if problem := saveNearestMode(nearestForm(url.Values{"mode": {"banana"}})); problem == "" {
		t.Fatal("unknown mode accepted")
	}
}

func TestNearestSectionOffersTheThreeModesAndTheUpdateWarning(t *testing.T) {
	w := httptest.NewRecorder()
	nearestSection(w, "token")
	body := w.Body.String()
	for _, want := range []string{">Classic<", ">Device quorum<", "Update every device"} {
		if !strings.Contains(body, want) {
			t.Errorf("section lacks %q", want)
		}
	}
	if strings.Contains(body, "Remove device") || strings.Contains(body, "Keep this group") {
		t.Error("recovery controls offered with no group to recover")
	}
}

// Peer-supplied names are escaped wherever they are shown.
func TestNearestClusterEscapesPeerLabels(t *testing.T) {
	w := httptest.NewRecorder()
	view := nearest.MembershipView{}
	nearestCluster(w, "token", view, nearest.ClusterView{ID: "<b>c</b>", Authority: `<script>x</script>`,
		Members: []nearest.MemberView{{ID: "id", Name: `"><img src=x onerror=y>`, Reachable: true}}}, "Another")
	body := w.Body.String()
	for _, bad := range []string{"<script>", "<img", "<b>c</b>"} {
		if strings.Contains(body, bad) {
			t.Fatalf("unescaped %q in %s", bad, body)
		}
	}
}

// Off is a choice in the mode list: it switches the device out and keeps the mode it had, and the
// window and offset are saved with it, clamped as Home Assistant's numbers are.
func TestNearestModeOffAndBasics(t *testing.T) {
	config.Use(filepath.Join(t.TempDir(), "state.json"))
	if problem := saveNearestMode(nearestForm(url.Values{"mode": {"off"}, "window": {"9999"}, "offset": {"-4"}})); problem != "" {
		t.Fatal(problem)
	}
	c := config.Get().Nearest
	if c.Enabled() || c.WindowMs != config.MaxNearestWindowMs || c.Offset != -4 {
		t.Fatalf("saved %+v", c)
	}
	if problem := saveNearestMode(nearestForm(url.Values{"mode": {"classic"}})); problem != "" {
		t.Fatal(problem)
	}
	if c := config.Get().Nearest; !c.Enabled() || c.Offset != -4 {
		t.Fatalf("classic saved %+v", c)
	}
}
