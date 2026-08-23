package isolation

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"syscall"
)

const (
	defaultDevSizeBytes int64 = 4 << 20
	maximumDevSizeBytes int64 = 64 << 20
)

// RootfsConfig identifies one owned rootfs and the only host subtree from which it may be opened.
type RootfsConfig struct {
	AllowedRoot  string `json:"allowed_root"`
	Rootfs       string `json:"rootfs"`
	OldRootName  string `json:"old_root_name,omitempty"`
	DevSizeBytes int64  `json:"dev_size_bytes,omitempty"`
}

// Validate rejects root, equal, escaping, relative, or malformed rootfs paths before host inspection.
func (c RootfsConfig) Validate() error {
	if _, _, err := cleanOwnedPath(c.AllowedRoot, c.Rootfs); err != nil {
		return err
	}
	oldRootName := c.OldRootName
	if oldRootName == "" {
		oldRootName = ".pivot_old"
	}
	if filepath.Base(oldRootName) != oldRootName || oldRootName == "." || oldRootName == ".." {
		return fmt.Errorf("%w: old-root name must be one path component", ErrUnsafePath)
	}
	if c.DevSizeBytes < 0 || c.DevSizeBytes > maximumDevSizeBytes {
		return fmt.Errorf("%w: /dev tmpfs size must be between zero and %d bytes", ErrUnsafePath, maximumDevSizeBytes)
	}
	return nil
}

// PrepareRoot performs the one-shot rootfs pivot only after the PID 1 action has durably checkpointed its owner and namespace receipts.
// It can only be invoked with the active capability supplied by RunLockedHelper;
// PID 1 wrappers call it after fork, never on the parent helper that requested CLONE_NEWPID.
func (h *LockedHelper) PrepareRoot(ctx context.Context, config RootfsConfig) error {
	return h.prepareRoot(ctx, config, -1)
}

// PrepareRootWithDNS bind-mounts one already-open, trusted resolv.conf file
// onto the existing rootfs target before pivot; the caller retains and closes dnsFD.
func (h *LockedHelper) PrepareRootWithDNS(ctx context.Context, config RootfsConfig, dnsFD int) error {
	if dnsFD < 0 {
		return fmt.Errorf("%w: DNS source descriptor must be non-negative", ErrUnsafeIdentity)
	}
	return h.prepareRoot(ctx, config, dnsFD)
}

// prepareRoot implements the shared one-shot pivot sequence and optionally
// binds a descriptor-backed DNS file without trusting a host path.
func (h *LockedHelper) prepareRoot(ctx context.Context, config RootfsConfig, dnsFD int) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := h.checkThread(); err != nil {
		return err
	}
	if !h.pid1Child {
		return fmt.Errorf("%w: rootfs preparation must run in the forked PID 1 wrapper", ErrUnsafeIdentity)
	}
	if h.rootfsStarted {
		return fmt.Errorf("%w: rootfs preparation was already attempted", ErrUnsafeIdentity)
	}
	if err := config.Validate(); err != nil {
		return err
	}
	mountInode, exists := h.created[NamespaceMount]
	if !exists || mountInode == 0 {
		return fmt.Errorf("%w: PrepareRoot requires a verified new mount namespace receipt", ErrUnsafeIdentity)
	}
	currentMountInode, err := currentNamespaceInode(h.ops, NamespaceMount)
	if err != nil {
		return fmt.Errorf("verify rootfs mount namespace: %w", err)
	}
	if currentMountInode != mountInode {
		return fmt.Errorf("%w: rootfs mount namespace inode changed", ErrUnsafeIdentity)
	}
	h.rootfsStarted = true
	allowedRoot, rootfs, _ := cleanOwnedPath(config.AllowedRoot, config.Rootfs)
	relativeRootfs, err := filepath.Rel(allowedRoot, rootfs)
	if err != nil {
		return fmt.Errorf("%w: derive rootfs ownership-relative path: %v", ErrUnsafePath, err)
	}
	allowedFD, err := h.ops.OpenDirectoryNoSymlink(allowedRoot)
	if err != nil {
		return fmt.Errorf("%w: open allowed root without symlinks: %v", ErrUnsafePath, err)
	}
	defer h.ops.Close(allowedFD)
	allowedStat, err := h.ops.Fstat(allowedFD)
	if err != nil || !allowedStat.IsDirectory() {
		return fmt.Errorf("%w: allowed root is not a verified directory", ErrUnsafePath)
	}
	rootfsFD, err := h.ops.OpenDirectoryAt(allowedFD, relativeRootfs)
	if err != nil {
		return fmt.Errorf("%w: open rootfs beneath allowed root without symlinks: %v", ErrUnsafePath, err)
	}
	defer h.ops.Close(rootfsFD)
	rootfsStat, err := h.ops.Fstat(rootfsFD)
	if err != nil || !rootfsStat.IsDirectory() {
		return fmt.Errorf("%w: rootfs is not a verified directory", ErrUnsafePath)
	}
	if allowedStat.Dev == rootfsStat.Dev && allowedStat.Ino == rootfsStat.Ino {
		return fmt.Errorf("%w: rootfs must not equal allowed root", ErrUnsafePath)
	}
	if err := validateContext(ctx); err != nil {
		return err
	}
	h.tainted = true
	if err := h.mount("", "/", "", privateRecursiveFlags(), ""); err != nil {
		return fmt.Errorf("make mount propagation private: %w", err)
	}
	rootfsDescriptorPath := "/proc/self/fd/" + strconv.Itoa(rootfsFD)
	if err := h.mount(rootfsDescriptorPath, rootfsDescriptorPath, "", selfBindRecursiveFlags(), ""); err != nil {
		return fmt.Errorf("self-bind rootfs: %w", err)
	}
	if dnsFD >= 0 {
		if err := h.bindDNSFile(ctx, allowedFD, relativeRootfs, dnsFD); err != nil {
			return err
		}
	}
	oldRootName := config.OldRootName
	if oldRootName == "" {
		oldRootName = ".pivot_old"
	}
	putOld := rootfsDescriptorPath + "/" + oldRootName
	if err := mkdirNew(h, putOld, 0700); err != nil {
		return fmt.Errorf("create put_old: %w", err)
	}
	if err := h.pivotRoot(rootfsDescriptorPath, putOld); err != nil {
		return fmt.Errorf("pivot root: %w", err)
	}
	if err := h.chdir("/"); err != nil {
		return fmt.Errorf("chdir new root: %w", err)
	}
	oldRoot := "/" + oldRootName
	if err := h.unmount(oldRoot, detachUnmountFlag()); err != nil {
		return fmt.Errorf("detach old root: %w", err)
	}
	if err := h.remove(oldRoot); err != nil {
		return fmt.Errorf("remove old-root directory: %w", err)
	}
	if err := mkdirIfMissing(h, "/proc", 0555); err != nil {
		return fmt.Errorf("prepare /proc: %w", err)
	}
	if err := h.mount("proc", "/proc", "proc", safeProcFlags(), ""); err != nil {
		return fmt.Errorf("mount fresh /proc: %w", err)
	}
	if err := mkdirIfMissing(h, "/dev", 0755); err != nil {
		return fmt.Errorf("prepare /dev: %w", err)
	}
	devSize := config.DevSizeBytes
	if devSize == 0 {
		devSize = defaultDevSizeBytes
	}
	data := "mode=0755,size=" + strconv.FormatInt(devSize, 10) + ",nr_inodes=1024"
	if err := h.mount("tmpfs", "/dev", "tmpfs", safeDevFlags(), data); err != nil {
		return fmt.Errorf("mount minimal /dev tmpfs: %w", err)
	}
	h.rootfsPrepared = true
	return nil
}

// bindDNSFile verifies both descriptor identities, performs the fixed
// /etc/resolv.conf bind, and reopens the target to prove the mount took effect.
func (h *LockedHelper) bindDNSFile(ctx context.Context, allowedFD int, relativeRootfs string, dnsFD int) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	source, err := h.ops.Fstat(dnsFD)
	if err != nil || !source.IsRegular() {
		return fmt.Errorf("%w: DNS source is not a verified regular file", ErrUnsafePath)
	}
	targetPath := filepath.Join(relativeRootfs, "etc", "resolv.conf")
	targetFD, err := h.ops.OpenFileAt(allowedFD, targetPath)
	if err != nil {
		return fmt.Errorf("%w: open rootfs /etc/resolv.conf without symlinks: %v", ErrUnsafePath, err)
	}
	target, statErr := h.ops.Fstat(targetFD)
	if statErr != nil || !target.IsRegular() {
		_ = h.ops.Close(targetFD)
		return fmt.Errorf("%w: rootfs /etc/resolv.conf is not a verified regular file", ErrUnsafePath)
	}
	sourcePath := "/proc/self/fd/" + strconv.Itoa(dnsFD)
	targetDescriptorPath := "/proc/self/fd/" + strconv.Itoa(targetFD)
	mountErr := h.mount(sourcePath, targetDescriptorPath, "", fileBindFlags(), "")
	closeErr := h.ops.Close(targetFD)
	if mountErr != nil {
		return errors.Join(fmt.Errorf("bind trusted DNS file: %w", mountErr), closeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close DNS target descriptor: %w", closeErr)
	}
	readbackFD, err := h.ops.OpenFileAt(allowedFD, targetPath)
	if err != nil {
		return fmt.Errorf("reopen bound rootfs DNS target: %w", err)
	}
	readback, readbackErr := h.ops.Fstat(readbackFD)
	closeErr = h.ops.Close(readbackFD)
	if readbackErr != nil {
		return errors.Join(fmt.Errorf("read back bound rootfs DNS target: %w", readbackErr), closeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close DNS readback descriptor: %w", closeErr)
	}
	if readback.Dev != source.Dev || readback.Ino != source.Ino {
		return fmt.Errorf("%w: bound DNS target identity differs from source", ErrUnsafeIdentity)
	}
	return nil
}

// RootPrepared reports whether this active PID 1 helper completed the one-shot pivot and required mount sequence.
func (h *LockedHelper) RootPrepared() (bool, error) {
	if err := h.checkThread(); err != nil {
		return false, err
	}
	return h.rootfsPrepared, nil
}

// mkdirIfMissing creates one preparation directory and treats an existing directory path as idempotent intent.
func mkdirIfMissing(helper *LockedHelper, path string, mode uint32) error {
	err := helper.mkdir(path, mode)
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.EEXIST) {
		stat, statErr := helper.ops.Lstat(path)
		if statErr != nil {
			return statErr
		}
		if stat.IsSymlink() || !stat.IsDirectory() {
			return fmt.Errorf("%w: existing path %s is not a real directory", ErrUnsafePath, path)
		}
		return nil
	}
	return err
}

// mkdirNew creates a directory that must not preexist, preventing a planted pivot target.
func mkdirNew(helper *LockedHelper, path string, mode uint32) error {
	if err := helper.mkdir(path, mode); err != nil {
		if errors.Is(err, syscall.EEXIST) {
			return fmt.Errorf("%w: pivot old-root path already exists", ErrUnsafePath)
		}
		return err
	}
	return nil
}
