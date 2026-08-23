//go:build linux

package isolation

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// systemOps implements Ops and private helper mutations with direct Linux syscalls.
type systemOps struct{}

// NewSystemOps returns the Linux syscall implementation of the isolation boundary.
func NewSystemOps() Ops { return systemOps{} }

// EffectiveUID returns the caller's effective user identity for rootful preflight.
func (systemOps) EffectiveUID() int { return unix.Geteuid() }

// ProcessID returns the current process identity for read-only feature probes.
func (systemOps) ProcessID() int { return unix.Getpid() }

// ThreadID returns the current Linux thread identity for dedicated-thread enforcement.
func (systemOps) ThreadID() int { return unix.Gettid() }

// ReadFile reads one procfs or sysfs evidence file without modifying host state.
func (systemOps) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) }

// Readlink reads one procfs identity link without following it.
func (systemOps) Readlink(path string) (string, error) { return os.Readlink(path) }

// Lstat reads one path without following its final component.
func (systemOps) Lstat(path string) (FileInfo, error) {
	var value unix.Stat_t
	if err := unix.Lstat(path, &value); err != nil {
		return FileInfo{}, err
	}
	return FileInfo{Mode: value.Mode, Dev: uint64(value.Dev), Ino: value.Ino}, nil
}

// StatFS reads filesystem identity for cgroup2 and nsfs validation.
func (systemOps) StatFS(path string) (FileSystemInfo, error) {
	var value unix.Statfs_t
	if err := unix.Statfs(path, &value); err != nil {
		return FileSystemInfo{}, err
	}
	return FileSystemInfo{Type: int64(value.Type)}, nil
}

// OpenNamespace opens a namespace descriptor without joining it.
func (systemOps) OpenNamespace(path string) (int, error) {
	return unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC, 0)
}

// OpenDirectoryNoSymlink opens an absolute directory after rejecting every symlink component.
func (systemOps) OpenDirectoryNoSymlink(path string) (int, error) {
	if !filepath.IsAbs(path) {
		return -1, fmt.Errorf("directory path %q is not absolute", path)
	}
	rootFD, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	defer unix.Close(rootFD)
	relative := strings.TrimPrefix(filepath.Clean(path), "/")
	if relative == "" {
		return -1, fmt.Errorf("refusing filesystem root")
	}
	return unix.Openat2(rootFD, relative, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS,
	})
}

// OpenDirectoryBeneath opens target relative to base with kernel-enforced no-symlink resolution.
func (ops systemOps) OpenDirectoryBeneath(base, target string) (int, error) {
	cleanBase, cleanTarget, err := cleanOwnedPath(base, target)
	if err != nil {
		return -1, err
	}
	baseFD, err := ops.OpenDirectoryNoSymlink(cleanBase)
	if err != nil {
		return -1, err
	}
	defer unix.Close(baseFD)
	relative, err := filepath.Rel(cleanBase, cleanTarget)
	if err != nil {
		return -1, err
	}
	return unix.Openat2(baseFD, relative, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS,
	})
}

// OpenFileBeneath opens an existing regular-file target under base with
// kernel-enforced containment and no symlink or magic-link traversal.
func (ops systemOps) OpenFileBeneath(base, target string) (int, error) {
	cleanBase, cleanTarget, err := cleanOwnedPath(base, target)
	if err != nil {
		return -1, err
	}
	baseFD, err := ops.OpenDirectoryNoSymlink(cleanBase)
	if err != nil {
		return -1, err
	}
	defer unix.Close(baseFD)
	relative, err := filepath.Rel(cleanBase, cleanTarget)
	if err != nil {
		return -1, err
	}
	return unix.Openat2(baseFD, relative, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS,
	})
}

// OpenDirectoryAt resolves a directory relative to one retained ownership-root
// descriptor, preventing a later path rename from changing the trusted base.
func (systemOps) OpenDirectoryAt(baseFD int, relative string) (int, error) {
	clean, err := cleanDescriptorRelative(relative)
	if err != nil {
		return -1, err
	}
	return unix.Openat2(baseFD, clean, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS,
	})
}

// OpenFileAt resolves an existing file relative to one retained ownership-root
// descriptor without following symbolic or magic links.
func (systemOps) OpenFileAt(baseFD int, relative string) (int, error) {
	clean, err := cleanDescriptorRelative(relative)
	if err != nil {
		return -1, err
	}
	return unix.Openat2(baseFD, clean, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS,
	})
}

// cleanDescriptorRelative rejects absolute, empty, current-directory, and
// escaping inputs before they reach an fd-relative openat2 operation.
func cleanDescriptorRelative(relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) || strings.ContainsRune(relative, '\x00') {
		return "", fmt.Errorf("%w: descriptor-relative path is malformed", ErrUnsafePath)
	}
	clean := filepath.Clean(relative)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || clean != relative {
		return "", fmt.Errorf("%w: descriptor-relative path is not clean and contained", ErrUnsafePath)
	}
	return clean, nil
}

// Fstat reads stable descriptor identity without reopening a path.
func (systemOps) Fstat(fd int) (FileInfo, error) {
	var value unix.Stat_t
	if err := unix.Fstat(fd, &value); err != nil {
		return FileInfo{}, err
	}
	return FileInfo{Mode: value.Mode, Dev: uint64(value.Dev), Ino: value.Ino}, nil
}

// FstatFS reads the filesystem identity of an already-open descriptor.
func (systemOps) FstatFS(fd int) (FileSystemInfo, error) {
	var value unix.Statfs_t
	if err := unix.Fstatfs(fd, &value); err != nil {
		return FileSystemInfo{}, err
	}
	return FileSystemInfo{Type: int64(value.Type)}, nil
}

// ReadlinkFD reads the kernel namespace spelling associated with an open descriptor.
func (systemOps) ReadlinkFD(fd int) (string, error) {
	return os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
}

// Close releases one kernel descriptor.
func (systemOps) Close(fd int) error { return unix.Close(fd) }

// Dup creates an independently owned close-on-exec descriptor for process inheritance setup.
func (systemOps) Dup(fd int) (int, error) {
	return unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 3)
}

// PidfdOpen obtains a stable process reference without authorizing any action.
func (systemOps) PidfdOpen(pid int) (int, error) { return unix.PidfdOpen(pid, 0) }

// PidfdSendSignal targets the process identified by pidfd; signal zero is a read-only liveness probe.
func (systemOps) PidfdSendSignal(pidfd, signal int) error {
	return unix.PidfdSendSignal(pidfd, unix.Signal(signal), nil, 0)
}

// setns joins the namespace referenced by fd on the current dedicated OS thread.
func (systemOps) setns(fd, namespaceFlag int) error { return unix.Setns(fd, namespaceFlag) }

// unshare creates a namespace for the current locked runtime-helper thread.
func (systemOps) unshare(flags int) error { return unix.Unshare(flags) }

// hostname reads the active UTS namespace nodename for configuration readback.
func (systemOps) hostname() (string, error) { return os.Hostname() }

// setHostname applies one bounded nodename inside the caller's already-created UTS namespace.
func (systemOps) setHostname(hostname string) error { return unix.Sethostname([]byte(hostname)) }

// loopbackUp reads the active network namespace's loopback administrative state.
func (systemOps) loopbackUp() (bool, error) {
	flags, err := loopbackFlags()
	if err != nil {
		return false, err
	}
	return flags&unix.IFF_UP != 0, nil
}

// setLoopbackUp changes only the loopback administrative-state bit and preserves all other interface flags.
func (systemOps) setLoopbackUp(up bool) error {
	fd, request, err := openLoopbackRequest()
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.IoctlIfreq(fd, unix.SIOCGIFFLAGS, request); err != nil {
		return err
	}
	flags := request.Uint16()
	if up {
		flags |= unix.IFF_UP
	} else {
		flags &^= unix.IFF_UP
	}
	request.SetUint16(flags)
	return unix.IoctlIfreq(fd, unix.SIOCSIFFLAGS, request)
}

// loopbackFlags obtains the loopback flags through a short-lived close-on-exec ioctl socket.
func loopbackFlags() (uint16, error) {
	fd, request, err := openLoopbackRequest()
	if err != nil {
		return 0, err
	}
	defer unix.Close(fd)
	if err := unix.IoctlIfreq(fd, unix.SIOCGIFFLAGS, request); err != nil {
		return 0, err
	}
	return request.Uint16(), nil
}

// openLoopbackRequest creates the exact interface request used for bounded loopback inspection and mutation.
func openLoopbackRequest() (int, *unix.Ifreq, error) {
	request, err := unix.NewIfreq("lo")
	if err != nil {
		return -1, nil, err
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, nil, err
	}
	return fd, request, nil
}

// mount performs one explicit mount step selected by the locked rootfs coordinator.
func (systemOps) mount(source, target, filesystem string, flags uintptr, data string) error {
	return unix.Mount(source, target, filesystem, flags, data)
}

// unmount detaches one explicitly owned mount selected by the locked rootfs coordinator.
func (systemOps) unmount(target string, flags int) error { return unix.Unmount(target, flags) }

// pivotRoot changes the helper process root after all path and mount checks pass.
func (systemOps) pivotRoot(newRoot, putOld string) error { return unix.PivotRoot(newRoot, putOld) }

// mkdir creates one rootfs preparation directory with an explicit mode.
func (systemOps) mkdir(path string, mode uint32) error { return unix.Mkdir(path, mode) }

// remove removes one empty preparation directory after its mount is detached.
func (systemOps) remove(path string) error { return os.Remove(path) }

// chdir changes the runtime helper's working directory during pivot_root.
func (systemOps) chdir(path string) error { return unix.Chdir(path) }
