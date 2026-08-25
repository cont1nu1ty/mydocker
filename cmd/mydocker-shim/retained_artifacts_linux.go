//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"golang.org/x/sys/unix"

	"mydocker/internal/shim"
)

const retainedOwnerDirectoryMode uint32 = 0o700

// retainInitArtifacts opens the exact trusted owner directory without following
// its final path component, verifies its inode metadata, and rewrites only
// shim-local control/terminal paths through that retained FD for pivot_root.
func retainInitArtifacts(config shim.RuntimeConfig) (*os.File, shim.RuntimeConfig, error) {
	ownerRoot := filepath.Dir(config.ControlSocket)
	if filepath.Dir(config.TerminalPath) != ownerRoot || filepath.Dir(config.LogPath) != ownerRoot {
		return nil, shim.RuntimeConfig{}, errors.New("init artifacts must share one owner directory")
	}
	descriptor, err := unix.Open(ownerRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, shim.RuntimeConfig{}, fmt.Errorf("retain init artifact directory without following symlinks: %w", err)
	}
	directory := os.NewFile(uintptr(descriptor), ownerRoot)
	if directory == nil {
		return nil, shim.RuntimeConfig{}, errors.Join(errors.New("retain init artifact directory returned an invalid descriptor"), unix.Close(descriptor))
	}
	var metadata unix.Stat_t
	if err := unix.Fstat(descriptor, &metadata); err != nil {
		return nil, shim.RuntimeConfig{}, errors.Join(fmt.Errorf("inspect retained init artifact directory: %w", err), directory.Close())
	}
	if metadata.Mode&unix.S_IFMT != unix.S_IFDIR {
		return nil, shim.RuntimeConfig{}, errors.Join(fmt.Errorf("%w: retained init artifact descriptor is not a directory", shim.ErrUnsafeArtifact), directory.Close())
	}
	if metadata.Uid != uint32(os.Geteuid()) {
		return nil, shim.RuntimeConfig{}, errors.Join(fmt.Errorf("%w: retained init artifact directory has a foreign owner", shim.ErrUnsafeArtifact), directory.Close())
	}
	if metadata.Mode&0o7777 != retainedOwnerDirectoryMode {
		return nil, shim.RuntimeConfig{}, errors.Join(fmt.Errorf("%w: retained init artifact directory mode must be exactly 0700", shim.ErrUnsafeArtifact), directory.Close())
	}
	retainedRoot := filepath.Join("/proc/self/fd", strconv.Itoa(descriptor))
	retained := config
	retained.ControlSocket = filepath.Join(retainedRoot, filepath.Base(config.ControlSocket))
	retained.TerminalPath = filepath.Join(retainedRoot, filepath.Base(config.TerminalPath))
	return directory, retained, nil
}
