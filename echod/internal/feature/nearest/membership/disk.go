// Package membership is the saved list of devices in a Device quorum group, and the numbered steps
// that change it. Wakes, clocks and elections are kept elsewhere (feature/nearest).
package membership

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

var (
	ErrUninitialized = errors.New("membership: state not initialized")
	ErrCorrupt       = errors.New("membership: invalid durable state")
	ErrConflict      = errors.New("membership: a different successor is already accepted")
	ErrUncertain     = errors.New("membership: durability uncertain; retry the same write")
	ErrDraining      = errors.New("membership: old activity is still draining")
)

// digest is the SHA-256 of v's JSON, to tell two decisions apart.
func digest(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// writeAtomic replaces path with data so that a crash leaves either the old file or the new one: it
// writes a temporary file beside it, syncs it, renames it over path and syncs the folder.
func writeAtomic(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".nearest-membership-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0o600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr = dir.Close()
	if err != nil {
		return err
	}
	return closeErr
}
