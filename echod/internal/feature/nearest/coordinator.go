package nearest

import (
	"slices"
	"strings"
)

const (
	modeExisting = "classic"
	modeElected  = "device_quorum"
)

func validMode(mode string) bool {
	return mode == modeExisting || mode == modeElected
}

// coordinatorQuorum counts distinct voters from the configured roster. A
// majority is required even when fewer devices currently appear online.
func coordinatorQuorum(members, voters []string) bool {
	if len(members) < 2 {
		return false
	}
	seen := make(map[string]bool, len(voters))
	for _, voter := range voters {
		voter = strings.ToLower(strings.TrimSpace(voter))
		if !slices.Contains(members, voter) || seen[voter] {
			return false
		}
		seen[voter] = true
	}
	return len(seen) > len(members)/2
}
