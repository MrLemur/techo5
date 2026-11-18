package nearest

import (
	"github.com/HuskerMinion/techo5/echod/internal/config"
)

func (n *Nearest) settingsChanged() {
	n.mu.Lock()
	n.generation++
	n.mu.Unlock()
	n.membership.invalidate("settings_changed")
	if config.Get().Nearest.EffectiveMode() == modeElected {
		if err := n.membership.open(); err != nil {
			n.membership.invalidate("storage_error")
		}
	}
}

// SetBasics saves what the setup page's Nearest device form sends: whether this device takes part, its
// window and its offset, exactly as Home Assistant's switch and numbers would.
func (n *Nearest) SetBasics(on bool, windowMs, offset int) {
	n.on.OnCommand(on)
	n.window.OnCommand(float32(min(max(windowMs, config.MinNearestWindowMs), config.MaxNearestWindowMs)))
	n.offset.OnCommand(float32(min(max(offset, config.MinNearestOffset), config.MaxNearestOffset)))
}
