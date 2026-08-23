//go:build !linux

package shim

import (
	"mydocker/internal/isolation"
)

// RunInitBootstrap fails closed because setns and PID-namespace PID1 bootstrap are Linux-only.
func RunInitBootstrap(InitBootstrap) error { return isolation.ErrUnsupportedPlatform }

// ValidateInitBootstrapCompletion fails closed because PID1 namespace evidence is Linux-only.
func ValidateInitBootstrapCompletion(RuntimeConfig, string, uint64, uint64) error {
	return isolation.ErrUnsupportedPlatform
}
