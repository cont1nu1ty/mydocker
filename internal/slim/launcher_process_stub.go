//go:build !linux

package slim

import (
	"context"

	"mydocker/internal/isolation"
)

// OSProcessFactory is the fail-closed non-Linux production process factory.
type OSProcessFactory struct{}

// Preflight fails because clone3 cgroup placement and pidfd return are Linux-only.
func (OSProcessFactory) Preflight(context.Context) error { return isolation.ErrUnsupportedPlatform }

// Start fails without creating a process because cgroup-at-fork and pidfds are Linux-only.
func (OSProcessFactory) Start(context.Context, ProcessLaunchSpec) (StartedProcess, error) {
	return nil, isolation.ErrUnsupportedPlatform
}
