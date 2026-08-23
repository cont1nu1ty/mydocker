//go:build !linux

package slim

import (
	"context"

	"mydocker/internal/isolation"
	"mydocker/internal/provider"
)

// systemLauncherHost is the fail-closed non-Linux host adapter.
type systemLauncherHost struct{}

// Preflight always reports unsupported because M3 production isolation is Linux-only.
func (systemLauncherHost) Preflight(context.Context, string, provider.IsolationRequirements) error {
	return isolation.ErrUnsupportedPlatform
}

// ValidateExecutable fails because non-Linux execution cannot satisfy the launcher contract.
func (systemLauncherHost) ValidateExecutable(string) error { return isolation.ErrUnsupportedPlatform }

// KeeperCloneFlags returns no flags on unsupported platforms.
func (systemLauncherHost) KeeperCloneFlags() uintptr { return 0 }

// InitCloneFlags returns no flags on unsupported platforms.
func (systemLauncherHost) InitCloneFlags() uintptr { return 0 }
