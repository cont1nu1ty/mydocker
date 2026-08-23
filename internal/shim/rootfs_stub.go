//go:build !linux

package shim

import "mydocker/internal/isolation"

// PID1RootfsPreparer is the fail-closed non-Linux placeholder for the rootful preparer.
type PID1RootfsPreparer struct{}

// NewPID1RootfsPreparer returns a placeholder whose only operation reports the unsupported platform.
func NewPID1RootfsPreparer() *PID1RootfsPreparer { return &PID1RootfsPreparer{} }

// PrepareRootfs fails closed because PID namespaces, bind mounts, and pivot_root are Linux-only.
func (*PID1RootfsPreparer) PrepareRootfs(RootfsRequest) error {
	return isolation.ErrUnsupportedPlatform
}
