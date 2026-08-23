package cgroupv2

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"golang.org/x/sys/unix"
)

// DirectoryHandle is a runtime-only opened cgroup directory descriptor. Its
// numeric FD must never be persisted as resource identity.
type DirectoryHandle interface {
	Fd() uintptr
	Close() error
}

// FileSystem is the narrow set of cgroup filesystem operations used by Manager.
// Implementations must not make Remove recursive.
type FileSystem interface {
	Lstat(path string) (fs.FileInfo, error)
	Mkdir(path string, perm fs.FileMode) error
	ReadFile(path string) ([]byte, error)
	WriteFile(path string, data []byte) error
	ReadDir(path string) ([]fs.DirEntry, error)
	Remove(path string) error
	OpenDir(path string) (DirectoryHandle, error)
}

// HostProbe identifies cgroup v2 and supplies host page size for canonical memory.max readback.
type HostProbe interface {
	IsCgroupV2(path string) (bool, error)
	PageSize() (uint64, error)
}

// OSFileSystem implements FileSystem without recursive deletion or implicit control-file creation.
type OSFileSystem struct{}

// Lstat returns path metadata without following a final symlink.
func (OSFileSystem) Lstat(path string) (fs.FileInfo, error) {
	return os.Lstat(path)
}

// Mkdir creates exactly one cgroup directory and leaves parent creation to the caller.
func (OSFileSystem) Mkdir(path string, perm fs.FileMode) error {
	return os.Mkdir(path, perm)
}

// ReadFile reads one bounded cgroup pseudo-file through the standard filesystem API.
func (OSFileSystem) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

// WriteFile writes one existing cgroup pseudo-file without O_CREATE and detects short writes.
func (OSFileSystem) WriteFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if written != len(data) {
		return io.ErrShortWrite
	}
	return closeErr
}

// ReadDir lists immediate entries so cleanup can reject child cgroups without traversing them.
func (OSFileSystem) ReadDir(path string) ([]fs.DirEntry, error) {
	return os.ReadDir(path)
}

// Remove removes exactly one empty cgroup directory and never traverses descendants.
func (OSFileSystem) Remove(path string) error {
	return os.Remove(path)
}

// OpenDir opens an exact cgroup directory with no final symlink following and close-on-exec enabled.
func (OSFileSystem) OpenDir(path string) (DirectoryHandle, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open cgroup directory returned an invalid file descriptor")
	}
	return file, nil
}

// LinuxHostProbe identifies cgroup v2 by the configured root's filesystem magic.
type LinuxHostProbe struct{}

// IsCgroupV2 returns true only for the unified cgroup2 filesystem and never treats v1 as compatible.
func (LinuxHostProbe) IsCgroupV2(path string) (bool, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return false, fmt.Errorf("statfs cgroup root %q: %w", path, err)
	}
	return uint64(stat.Type) == uint64(unix.CGROUP2_SUPER_MAGIC), nil
}

// PageSize returns the positive kernel page size used when memory.max canonicalizes byte limits.
func (LinuxHostProbe) PageSize() (uint64, error) {
	pageSize := os.Getpagesize()
	if pageSize <= 0 {
		return 0, errors.New("host page size is unavailable")
	}
	return uint64(pageSize), nil
}
