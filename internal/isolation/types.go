package isolation

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

const (
	// Cgroup2FSMagic identifies a cgroup v2 unified filesystem.
	Cgroup2FSMagic int64 = 0x63677270
	// NamespaceFSMagic identifies Linux nsfs namespace handles.
	NamespaceFSMagic int64 = 0x6e736673
)

var (
	// ErrUnsupportedPlatform reports use of Linux isolation on another operating system.
	ErrUnsupportedPlatform = errors.New("isolation is supported only on Linux")
	// ErrPreflight reports a host that cannot safely satisfy the M2 isolation contract.
	ErrPreflight = errors.New("isolation preflight failed")
	// ErrPrivilegedTestDenied reports a privileged test without explicit opt-in.
	ErrPrivilegedTestDenied = errors.New("privileged isolation test is not allowed")
	// ErrUnsafeTestEnvironment reports a privileged test outside a disposable environment.
	ErrUnsafeTestEnvironment = errors.New("privileged isolation test environment is not disposable")
	// ErrUnsafeIdentity reports process or namespace evidence that no longer proves ownership.
	ErrUnsafeIdentity = errors.New("unsafe or stale isolation identity")
	// ErrUnsafePath reports a rootfs path that can escape its configured ownership root.
	ErrUnsafePath = errors.New("unsafe rootfs path")
	// ErrWrongThread reports namespace work attempted outside its dedicated OS thread.
	ErrWrongThread = errors.New("namespace operation is not on its dedicated OS thread")
	// ErrClosed reports use of a released process, namespace, or thread handle.
	ErrClosed = errors.New("isolation handle is closed")
)

// FileInfo is the syscall-neutral subset of stat data needed for ownership checks.
type FileInfo struct {
	Mode uint32
	Dev  uint64
	Ino  uint64
}

// IsDirectory reports whether the stat mode identifies a directory.
func (i FileInfo) IsDirectory() bool { return i.Mode&0170000 == 0040000 }

// IsSymlink reports whether the stat mode identifies a symbolic link.
func (i FileInfo) IsSymlink() bool { return i.Mode&0170000 == 0120000 }

// IsRegular reports whether the stat mode identifies a regular file suitable for a file bind mount.
func (i FileInfo) IsRegular() bool { return i.Mode&0170000 == 0100000 }

// FileSystemInfo is the syscall-neutral filesystem identity used by preflight and nsfs checks.
type FileSystemInfo struct {
	Type int64
}

// Ops is the narrow host-operation boundary used by Linux production wiring and pure test doubles.
// Mutating methods must never be called by Preflight or process/namespace verification.
type Ops interface {
	// EffectiveUID returns the identity used by rootful preflight.
	EffectiveUID() int
	// ProcessID returns the current process for pidfd feature probing.
	ProcessID() int
	// ThreadID returns the current OS thread for namespace-session confinement.
	ThreadID() int
	// ReadFile reads immutable procfs or sysfs evidence.
	ReadFile(path string) ([]byte, error)
	// Readlink reads executable or other procfs link evidence.
	Readlink(path string) (string, error)
	// Lstat reads path identity without following its final component.
	Lstat(path string) (FileInfo, error)
	// StatFS reports filesystem magic without mounting anything.
	StatFS(path string) (FileSystemInfo, error)
	// OpenNamespace opens an nsfs handle without joining it.
	OpenNamespace(path string) (int, error)
	// OpenDirectoryNoSymlink opens an absolute directory with all symlinks rejected.
	OpenDirectoryNoSymlink(path string) (int, error)
	// OpenDirectoryBeneath opens a strict child directory without link traversal.
	OpenDirectoryBeneath(base, target string) (int, error)
	// OpenFileBeneath opens an existing strict child regular file without link traversal.
	OpenFileBeneath(base, target string) (int, error)
	// OpenDirectoryAt opens one relative child from an already-verified base descriptor.
	OpenDirectoryAt(baseFD int, relative string) (int, error)
	// OpenFileAt opens one relative existing file from an already-verified base descriptor.
	OpenFileAt(baseFD int, relative string) (int, error)
	// Fstat returns stable identity for an open descriptor.
	Fstat(fd int) (FileInfo, error)
	// FstatFS returns filesystem magic for an open descriptor.
	FstatFS(fd int) (FileSystemInfo, error)
	// ReadlinkFD returns kernel link text for an open namespace descriptor.
	ReadlinkFD(fd int) (string, error)
	// Close releases one runtime-only descriptor.
	Close(fd int) error
	// Dup creates a close-on-exec duplicate whose ownership transfers to the caller.
	Dup(fd int) (int, error)
	// PidfdOpen opens a stable process handle without authorizing an action.
	PidfdOpen(pid int) (int, error)
	// PidfdSendSignal probes with signal zero or signals an already verified handle.
	PidfdSendSignal(pidfd, signal int) error
}

// helperOps is deliberately private so namespace and rootfs mutations cannot be
// reached through the public read/identity Ops boundary without a LockedHelper.
type helperOps interface {
	Ops
	setns(fd, namespaceFlag int) error
	unshare(flags int) error
	hostname() (string, error)
	setHostname(hostname string) error
	loopbackUp() (bool, error)
	setLoopbackUp(up bool) error
	mount(source, target, filesystem string, flags uintptr, data string) error
	unmount(target string, flags int) error
	pivotRoot(newRoot, putOld string) error
	mkdir(path string, mode uint32) error
	remove(path string) error
	chdir(path string) error
}

// NamespaceType is the bounded namespace vocabulary supported by M2.
type NamespaceType string

const (
	// NamespaceUTS identifies the hostname namespace owned by a Sandbox.
	NamespaceUTS NamespaceType = "uts"
	// NamespaceIPC identifies the IPC namespace owned by a Sandbox.
	NamespaceIPC NamespaceType = "ipc"
	// NamespaceNetwork identifies the network namespace owned by a Sandbox.
	NamespaceNetwork NamespaceType = "net"
	// NamespacePID identifies the PID namespace normally owned by an Attempt.
	NamespacePID NamespaceType = "pid"
	// NamespaceMount identifies the mount namespace normally owned by an Attempt.
	NamespaceMount NamespaceType = "mnt"
)

// Valid reports whether a namespace type belongs to the supported M2 set.
func (t NamespaceType) Valid() bool {
	switch t {
	case NamespaceUTS, NamespaceIPC, NamespaceNetwork, NamespacePID, NamespaceMount:
		return true
	default:
		return false
	}
}

// procName returns the stable /proc namespace entry for this type.
func (t NamespaceType) procName() string { return string(t) }

// threadProcName returns the current-thread namespace entry, accounting for PID-for-children semantics.
func (t NamespaceType) threadProcName() string {
	if t == NamespacePID {
		return "pid_for_children"
	}
	return t.procName()
}

// validateContext rejects work that was already cancelled before touching host state.
func validateContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	return ctx.Err()
}

// requireOps rejects a missing host boundary before any validation or syscall attempt.
func requireOps(ops Ops) error {
	if ops == nil {
		return errors.New("isolation Ops must not be nil")
	}
	return nil
}

// cleanOwnedPath validates an absolute, non-root target strictly beneath its ownership root.
func cleanOwnedPath(allowedRoot, target string) (string, string, error) {
	if !filepath.IsAbs(allowedRoot) || !filepath.IsAbs(target) {
		return "", "", fmt.Errorf("%w: allowed root and target must be absolute", ErrUnsafePath)
	}
	root := filepath.Clean(allowedRoot)
	clean := filepath.Clean(target)
	if root == string(filepath.Separator) || clean == string(filepath.Separator) {
		return "", "", fmt.Errorf("%w: filesystem root is never an owned rootfs path", ErrUnsafePath)
	}
	relative, err := filepath.Rel(root, clean)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return "", "", fmt.Errorf("%w: target must be a strict child of allowed root", ErrUnsafePath)
	}
	return root, clean, nil
}
