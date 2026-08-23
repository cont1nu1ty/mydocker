//go:build linux

package isolation

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// namespaceCloneFlag maps the bounded namespace vocabulary to Linux setns flags.
func namespaceCloneFlag(namespaceType NamespaceType) (int, error) {
	switch namespaceType {
	case NamespaceUTS:
		return unix.CLONE_NEWUTS, nil
	case NamespaceIPC:
		return unix.CLONE_NEWIPC, nil
	case NamespaceNetwork:
		return unix.CLONE_NEWNET, nil
	case NamespacePID:
		return unix.CLONE_NEWPID, nil
	case NamespaceMount:
		return unix.CLONE_NEWNS, nil
	default:
		return 0, fmt.Errorf("unsupported namespace %q", namespaceType)
	}
}

// mountNamespaceFlag returns the Linux clone flag required by rootfs preparation.
func mountNamespaceFlag() int { return unix.CLONE_NEWNS }

// filesystemContextFlag returns CLONE_FS for isolating root, cwd, and umask before mount setns.
func filesystemContextFlag() int { return unix.CLONE_FS }

// privateRecursiveFlags returns the mount flags that contain propagation inside the helper namespace.
func privateRecursiveFlags() uintptr { return unix.MS_PRIVATE | unix.MS_REC }

// selfBindRecursiveFlags returns the mount flags that turn rootfs into a private mount point.
func selfBindRecursiveFlags() uintptr { return unix.MS_BIND | unix.MS_REC }

// fileBindFlags returns the exact bind-only flags used for trusted DNS file injection.
func fileBindFlags() uintptr { return unix.MS_BIND }

// safeProcFlags returns restrictive flags for the fresh PID-namespace proc mount.
func safeProcFlags() uintptr { return unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC }

// safeDevFlags returns restrictive flags for the minimal tmpfs /dev mount.
func safeDevFlags() uintptr { return unix.MS_NOSUID | unix.MS_NOEXEC }

// detachUnmountFlag returns the non-blocking detach flag for the old root mount.
func detachUnmountFlag() int { return unix.MNT_DETACH }
