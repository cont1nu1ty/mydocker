//go:build linux

package shim

import "golang.org/x/sys/unix"

// systemLaunchParentGuard uses Linux parent identity and prctl only inside the production child.
type systemLaunchParentGuard struct{}

// ClearParentDeathSignal removes the launch-only SIGKILL binding after durable parent authorization.
func (systemLaunchParentGuard) ClearParentDeathSignal() error {
	return unix.Prctl(unix.PR_SET_PDEATHSIG, 0, 0, 0, 0)
}
