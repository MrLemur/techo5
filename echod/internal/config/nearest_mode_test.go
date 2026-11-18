package config

import "testing"

func TestNearestEffectiveMode(t *testing.T) {
	for _, tc := range []struct {
		saved string
		want  string
	}{
		{"", "classic"},
		{"classic", "classic"},
		{"device_quorum", "device_quorum"},
		// A mode this release does not know answers as classic does rather than going quiet.
		{"hosted", "classic"},
		{"unexpected", "classic"},
	} {
		t.Run(tc.saved, func(t *testing.T) {
			if got := (Nearest{Mode: tc.saved}).EffectiveMode(); got != tc.want {
				t.Fatalf("mode %q: got %q, want %q", tc.saved, got, tc.want)
			}
		})
	}
}
