//go:build !linux

package shim

import "mydocker/internal/isolation"

// systemLaunchParentGuard is the fail-closed parent guard on unsupported platforms.
type systemLaunchParentGuard struct{}

// ClearParentDeathSignal reports that production parent-death control is Linux-only.
func (systemLaunchParentGuard) ClearParentDeathSignal() error {
	return isolation.ErrUnsupportedPlatform
}
