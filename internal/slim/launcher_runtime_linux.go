//go:build linux

package slim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"

	"mydocker/internal/isolation"
	"mydocker/internal/provider"

	"golang.org/x/sys/unix"
)

// systemLauncherHost implements read-only Linux probes and executable ownership checks.
type systemLauncherHost struct{}

// Preflight verifies rootful cgroup-v2, pidfd, and every required namespace without creating a process or namespace.
func (systemLauncherHost) Preflight(ctx context.Context, cgroupRoot string, requirements provider.IsolationRequirements) error {
	if err := requirements.Validate(); err != nil {
		return err
	}
	report, err := isolation.Preflight(ctx, isolation.NewSystemOps(), isolation.PreflightConfig{
		CgroupRoot: cgroupRoot, Namespaces: append([]isolation.NamespaceType(nil), requirements.Namespaces...),
	})
	if err != nil {
		return err
	}
	if !report.Rootful || !report.CgroupV2 || !report.Pidfd {
		return errors.New("Linux launcher host report is incomplete")
	}
	for _, namespace := range requirements.Namespaces {
		if !report.Namespaces[namespace] {
			return fmt.Errorf("required namespace %s was not verified", namespace)
		}
	}
	return nil
}

// ValidateExecutable requires a root-owned regular executable not writable by group or other users.
func (systemLauncherHost) ValidateExecutable(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect shim executable: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 || info.Mode().Perm()&0o111 == 0 {
		return errors.New("shim executable must be root-owned, regular, executable, and not group/other writable")
	}
	return nil
}

// KeeperCloneFlags returns the exact Sandbox namespace set created at keeper fork.
func (systemLauncherHost) KeeperCloneFlags() uintptr {
	return unix.CLONE_NEWUTS | unix.CLONE_NEWIPC | unix.CLONE_NEWNET
}

// InitCloneFlags returns the exact Attempt PID and mount namespaces created at init fork.
func (systemLauncherHost) InitCloneFlags() uintptr { return unix.CLONE_NEWPID | unix.CLONE_NEWNS }
