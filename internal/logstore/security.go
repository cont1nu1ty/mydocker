package logstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

type osFilePrimitives struct{}

// Lstat inspects a path component without following a symbolic link.
func (osFilePrimitives) Lstat(name string) (os.FileInfo, error) {
	return os.Lstat(name)
}

// OpenFile opens the final component with O_NOFOLLOW and close-on-exec so a log descriptor cannot leak into workloads.
func (osFilePrimitives) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	descriptor, err := unix.Open(name, flag|unix.O_NOFOLLOW|unix.O_CLOEXEC, uint32(perm.Perm()))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(descriptor), name), nil
}

// OpenDirectory opens one exact directory without following its final component and prevents descriptor inheritance.
func (osFilePrimitives) OpenDirectory(name string) (Directory, error) {
	descriptor, err := unix.Open(name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(descriptor), name), nil
}

// validateSecurePath rejects relative, noncanonical, symlinked, permissive, or foreign-owned log locations.
func validateSecurePath(files FilePrimitives, path string) (os.FileInfo, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("%w: path must be a nonempty canonical absolute path", ErrUnsafePath)
	}
	parent := filepath.Dir(path)
	if parent == path {
		return nil, fmt.Errorf("%w: path must name a file below a private directory", ErrUnsafePath)
	}
	components := strings.Split(strings.TrimPrefix(parent, string(filepath.Separator)), string(filepath.Separator))
	current := string(filepath.Separator)
	for _, component := range components {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := files.Lstat(current)
		if err != nil {
			return nil, fmt.Errorf("%w: inspect parent component %q: %v", ErrUnsafePath, current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return nil, fmt.Errorf("%w: parent component %q must be a real directory", ErrUnsafePath, current)
		}
	}
	parentInfo, err := files.Lstat(parent)
	if err != nil {
		return nil, fmt.Errorf("%w: inspect private parent: %v", ErrUnsafePath, err)
	}
	if err := validateOwnedMode(parentInfo, os.ModeDir|0o700, true); err != nil {
		return nil, fmt.Errorf("%w: private parent: %v", ErrUnsafePath, err)
	}
	info, err := files.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: inspect log file: %v", ErrUnsafePath, err)
	}
	if err := validateOwnedMode(info, 0o600, false); err != nil {
		return nil, fmt.Errorf("%w: log file: %v", ErrUnsafePath, err)
	}
	return info, nil
}

// validateOpenedFile proves that the opened descriptor is the same private regular file seen through the path.
func validateOpenedFile(files FilePrimitives, path string, before os.FileInfo, file File) error {
	opened, err := file.Stat()
	if err != nil {
		return fmt.Errorf("%w: inspect opened log descriptor: %v", ErrUnsafePath, err)
	}
	if err := validateOwnedMode(opened, 0o600, false); err != nil {
		return fmt.Errorf("%w: opened log descriptor: %v", ErrUnsafePath, err)
	}
	after, err := files.Lstat(path)
	if err != nil {
		return fmt.Errorf("%w: re-inspect log path: %v", ErrUnsafePath, err)
	}
	if err := validateOwnedMode(after, 0o600, false); err != nil {
		return fmt.Errorf("%w: reopened log path: %v", ErrUnsafePath, err)
	}
	if !os.SameFile(opened, after) || (before != nil && !os.SameFile(before, after)) {
		return fmt.Errorf("%w: log path changed while it was opened", ErrUnsafePath)
	}
	return nil
}

// syncParentDirectory durably publishes a created log entry after proving the opened directory still names the validated private parent.
func syncParentDirectory(files FilePrimitives, path string) (result error) {
	parent := filepath.Dir(path)
	before, err := files.Lstat(parent)
	if err != nil {
		return fmt.Errorf("%w: inspect log parent for synchronization: %v", ErrUnsafePath, err)
	}
	if err := validateOwnedMode(before, os.ModeDir|0o700, true); err != nil {
		return fmt.Errorf("%w: log parent for synchronization: %v", ErrUnsafePath, err)
	}
	directory, err := files.OpenDirectory(parent)
	if err != nil {
		return fmt.Errorf("open workload log parent for synchronization: %w", err)
	}
	defer func() {
		if closeErr := directory.Close(); closeErr != nil {
			result = errors.Join(result, fmt.Errorf("close workload log parent after synchronization: %w", closeErr))
		}
	}()
	opened, err := directory.Stat()
	if err != nil {
		return fmt.Errorf("inspect opened workload log parent: %w", err)
	}
	if err := validateOwnedMode(opened, os.ModeDir|0o700, true); err != nil {
		return fmt.Errorf("%w: opened log parent for synchronization: %v", ErrUnsafePath, err)
	}
	if !os.SameFile(before, opened) {
		return fmt.Errorf("%w: log parent changed while opening for synchronization", ErrUnsafePath)
	}
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("synchronize workload log parent directory: %w", err)
	}
	return nil
}

// acquireCommitLock takes an open-file-description byte-range lock so separate daemon and shim descriptors coordinate even inside one test process.
func acquireCommitLock(file File, lockType int16, wait bool) error {
	command := unix.F_OFD_SETLK
	if wait {
		command = unix.F_OFD_SETLKW
	}
	lock := unix.Flock_t{Type: lockType, Whence: int16(unix.SEEK_SET), Start: 0, Len: 1}
	return unix.FcntlFlock(file.Fd(), command, &lock)
}

// releaseCommitLock publishes the end of a successful append or snapshot-boundary capture on the same open file description.
func releaseCommitLock(file File) error {
	lock := unix.Flock_t{Type: unix.F_UNLCK, Whence: int16(unix.SEEK_SET), Start: 0, Len: 1}
	return unix.FcntlFlock(file.Fd(), unix.F_OFD_SETLK, &lock)
}

// validateOwnedMode enforces daemon ownership, exact private permissions, regular type, and single-link identity.
func validateOwnedMode(info os.FileInfo, expected os.FileMode, directory bool) error {
	if info.Mode()&os.ModeSymlink != 0 {
		return errors.New("symbolic links are not allowed")
	}
	if info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return errors.New("special permission bits are not allowed")
	}
	if directory {
		if !info.IsDir() {
			return errors.New("path is not a directory")
		}
		if info.Mode().Perm() != expected.Perm() {
			return fmt.Errorf("directory permissions must be %04o", expected.Perm())
		}
	} else {
		if !info.Mode().IsRegular() {
			return errors.New("path is not a regular file")
		}
		if info.Mode().Perm() != expected.Perm() {
			return fmt.Errorf("file permissions must be %04o", expected.Perm())
		}
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("path ownership metadata is unavailable")
	}
	if stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("path owner uid %d does not match daemon uid %d", stat.Uid, os.Geteuid())
	}
	if !directory && stat.Nlink != 1 {
		return fmt.Errorf("regular log file must have exactly one link, found %d", stat.Nlink)
	}
	return nil
}
