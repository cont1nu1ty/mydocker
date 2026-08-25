//go:build linux && mydocker_rootful

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// validateRootfulTestPaths performs the final read-only ownership and
// containment audit for every operator-supplied path before the suite creates
// a directory, daemon, namespace, mount, cgroup, or process.
func validateRootfulTestPaths(environment rootfulTestEnvironment) error {
	for name, path := range map[string]string{
		rootfulWorkRootEnvironment: environment.WorkRoot, rootfulCgroupRootEnvironment: environment.CgroupRoot,
		rootfulRootfsEnvironment: environment.Rootfs, rootfulShimEnvironment: environment.Shim,
	} {
		if !cleanAbsoluteRootfulPath(path) {
			return fmt.Errorf("%s must be a clean absolute non-root path", name)
		}
	}
	if len(environment.WorkRoot) > 8 {
		return errors.New("rootful work root must use at most 8 bytes so owner-token shim sockets fit Linux sockaddr_un")
	}
	if err := requireRootOwnedDirectory(environment.WorkRoot, true); err != nil {
		return fmt.Errorf("work root: %w", err)
	}
	if err := requireNoSymlinkTraversal(environment.WorkRoot, "work root"); err != nil {
		return err
	}
	markerPath := filepath.Join(environment.WorkRoot, rootfulWorkRootMarkerName)
	if err := requireRootOwnedRegular(markerPath, false); err != nil {
		return fmt.Errorf("work-root marker: %w", err)
	}
	markerInfo, err := os.Lstat(markerPath)
	if err != nil {
		return fmt.Errorf("inspect work-root marker: %w", err)
	}
	if markerInfo.Size() != int64(len(rootfulWorkRootMarkerContents)) {
		return errors.New("work-root marker has an unexpected size")
	}
	marker, err := os.ReadFile(markerPath)
	if err != nil {
		return fmt.Errorf("read work-root marker: %w", err)
	}
	if string(marker) != rootfulWorkRootMarkerContents {
		return errors.New("work-root marker does not contain the exact disposable-test declaration")
	}
	if err := requirePathBeneath(environment.WorkRoot, environment.Rootfs, "prepared rootfs"); err != nil {
		return err
	}
	if err := requirePathBeneath(environment.WorkRoot, environment.Shim, "shim executable"); err != nil {
		return err
	}
	if err := requireRootOwnedDirectory(environment.Rootfs, false); err != nil {
		return fmt.Errorf("prepared rootfs: %w", err)
	}
	if err := requireNoSymlinkTraversal(environment.Rootfs, "prepared rootfs"); err != nil {
		return err
	}
	if err := requireRootfsEntry(environment.Rootfs, "bin/sh", true); err != nil {
		return err
	}
	if err := requireRootfsEntry(environment.Rootfs, "bin/sleep", true); err != nil {
		return err
	}
	if err := requireRootOwnedRegular(filepath.Join(environment.Rootfs, "etc", "resolv.conf"), false); err != nil {
		return fmt.Errorf("prepared-rootfs etc/resolv.conf: %w", err)
	}
	if err := requireNoSymlinkTraversal(filepath.Join(environment.Rootfs, "etc", "resolv.conf"), "prepared-rootfs etc/resolv.conf"); err != nil {
		return err
	}
	if err := requireRootOwnedRegular(environment.Shim, true); err != nil {
		return fmt.Errorf("shim executable: %w", err)
	}
	if err := requireNoSymlinkTraversal(environment.Shim, "shim executable"); err != nil {
		return err
	}
	if err := requireRootOwnedDirectory(environment.CgroupRoot, false); err != nil {
		return fmt.Errorf("cgroup root: %w", err)
	}
	if err := requireNoSymlinkTraversal(environment.CgroupRoot, "cgroup root"); err != nil {
		return err
	}
	for _, runtimeName := range []string{"rn", "rl", "ro"} {
		if _, err := os.Lstat(filepath.Join(environment.WorkRoot, runtimeName)); err == nil {
			return fmt.Errorf("reserved runtime path %q already exists", runtimeName)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect reserved runtime path %q: %w", runtimeName, err)
		}
	}
	if !strings.HasPrefix(filepath.Base(environment.CgroupRoot), "mydocker-rootful-test-") {
		return errors.New("dedicated cgroup root basename must begin with mydocker-rootful-test-")
	}
	if err := requirePathBeneath("/sys/fs/cgroup", environment.CgroupRoot, "cgroup root"); err != nil {
		return err
	}
	return nil
}

// cleanAbsoluteRootfulPath accepts only canonical absolute non-root paths and
// rejects NUL-bearing strings before any filesystem lookup.
func cleanAbsoluteRootfulPath(path string) bool {
	return path != "" && !strings.ContainsRune(path, '\x00') && filepath.IsAbs(path) && filepath.Clean(path) == path && path != string(filepath.Separator)
}

// requirePathBeneath proves child is a strict lexical descendant of root and
// never accepts the ownership boundary itself as a mutable test target.
func requirePathBeneath(root, child, description string) error {
	relative, err := filepath.Rel(root, child)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return fmt.Errorf("%s must be a strict descendant of %s", description, root)
	}
	return nil
}

// requireNoSymlinkTraversal resolves every path component and requires the
// resulting canonical name to remain byte-identical to the operator input.
func requireNoSymlinkTraversal(path, description string) error {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", description, err)
	}
	if resolved != path {
		return fmt.Errorf("%s traverses a symlink", description)
	}
	return nil
}

// requireRootOwnedDirectory rejects symlinks, non-directories, foreign owners,
// writable-by-non-root modes, and optionally anything other than mode 0700.
func requireRootOwnedDirectory(path string, private bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("path is not a real directory")
	}
	if err := requireRootOwner(info); err != nil {
		return err
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("directory mode %#o permits group/other writes", info.Mode().Perm())
	}
	if private && info.Mode().Perm() != 0o700 {
		return fmt.Errorf("private directory mode is %#o, want 0700", info.Mode().Perm())
	}
	return nil
}

// requireRootOwnedRegular rejects a symlink or non-regular path and enforces
// root ownership, no group/other writes, and optional executable bits.
func requireRootOwnedRegular(path string, executable bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("path is not a real regular file")
	}
	if err := requireRootOwner(info); err != nil {
		return err
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("file mode %#o permits group/other writes", info.Mode().Perm())
	}
	if executable && info.Mode().Perm()&0o111 == 0 {
		return errors.New("file is not executable")
	}
	return nil
}

// requireRootOwner extracts Linux stat ownership without trusting path text or
// permission bits alone.
func requireRootOwner(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return errors.New("filesystem entry lacks Linux ownership metadata")
	}
	if stat.Uid != 0 {
		return fmt.Errorf("filesystem entry UID is %d, want root", stat.Uid)
	}
	return nil
}

// requireRootfsEntry resolves an allowed prepared-rootfs symlink, proves the
// final target remains inside that rootfs, and checks its regular-file role.
func requireRootfsEntry(rootfs, relative string, executable bool) error {
	candidate := filepath.Join(rootfs, filepath.FromSlash(relative))
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return fmt.Errorf("resolve prepared-rootfs %s: %w", relative, err)
	}
	if err := requirePathBeneath(rootfs, resolved, "resolved prepared-rootfs "+relative); err != nil {
		return err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("prepared-rootfs %s is not a regular file", relative)
	}
	if executable && info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("prepared-rootfs %s is not executable", relative)
	}
	return requireRootOwner(info)
}

// requireEmptyRootfulCgroupRoot proves the delegated root has no member process
// and no child cgroup; pseudo-files and enabled controller state are allowed.
func requireEmptyRootfulCgroupRoot(root string) error {
	members, err := os.ReadFile(filepath.Join(root, "cgroup.procs"))
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(members)) != "" {
		return errors.New("dedicated cgroup root contains member processes")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return fmt.Errorf("dedicated cgroup root contains child %q", entry.Name())
		}
	}
	return nil
}
