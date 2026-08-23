//go:build linux

package shim

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"mydocker/internal/isolation"

	"golang.org/x/sys/unix"
)

// RunInitBootstrap validates the immutable config and inherited nsfs handles,
// joins keeper UTS/IPC/network state on one locked thread, then re-executes the
// same shim. Success never returns; callers must run it only in the NEWPID PID1 child.
func RunInitBootstrap(bootstrap InitBootstrap) error {
	if err := bootstrap.Validate(); err != nil {
		return err
	}
	if os.Getpid() != 1 {
		return errors.New("init bootstrap must execute as PID 1 in the new PID namespace")
	}
	config, err := LoadRuntimeConfig(bootstrap.ConfigPath)
	if err != nil {
		return err
	}
	configEvidence, err := RuntimeConfigEvidence(config)
	if err != nil {
		return err
	}
	if config.Mode != ModeInit || config.WrapperEvidence != bootstrap.ConfigEvidence || configEvidence != bootstrap.ConfigEvidence {
		return errors.New("init bootstrap config identity differs from launch intent")
	}
	runtime.LockOSThread()
	for _, namespace := range bootstrap.SortedNamespaces() {
		if err := verifyBootstrapNamespace(namespace); err != nil {
			return err
		}
		flag, err := bootstrapNamespaceFlag(namespace.Type)
		if err != nil {
			return err
		}
		if err := unix.Setns(namespace.FD, flag); err != nil {
			return fmt.Errorf("join %s namespace: %w", namespace.Type, err)
		}
		if err := verifyCurrentBootstrapNamespace(namespace); err != nil {
			return err
		}
	}
	for _, namespace := range bootstrap.Namespaces {
		if err := unix.Close(namespace.FD); err != nil {
			return fmt.Errorf("close inherited %s namespace: %w", namespace.Type, err)
		}
	}
	pidInode, err := currentBootstrapNamespaceInode(isolation.NamespacePID)
	if err != nil {
		return err
	}
	mountInode, err := currentBootstrapNamespaceInode(isolation.NamespaceMount)
	if err != nil {
		return err
	}
	arguments := []string{
		bootstrap.Executable, "-config", bootstrap.ConfigPath, "-config-evidence", bootstrap.ConfigEvidence,
		"-bootstrap-complete", "-pid-inode", strconv.FormatUint(pidInode, 10), "-mount-inode", strconv.FormatUint(mountInode, 10),
	}
	return unix.Exec(bootstrap.Executable, arguments, []string{"PATH=/usr/sbin:/usr/bin:/sbin:/bin"})
}

// ValidateInitBootstrapCompletion admits the ungated second exec only when it
// is PID1 in the exact PID/mount namespaces recorded immediately before exec
// and the immutable init config still matches the bootstrap evidence.
func ValidateInitBootstrapCompletion(config RuntimeConfig, configEvidence string, pidInode, mountInode uint64) error {
	if config.Mode != ModeInit || os.Getpid() != 1 || pidInode == 0 || mountInode == 0 {
		return errors.New("bootstrap completion requires an init config and PID namespace PID1")
	}
	observed, err := RuntimeConfigEvidence(config)
	if err != nil {
		return err
	}
	if config.WrapperEvidence != configEvidence || observed != configEvidence {
		return errors.New("bootstrap completion config evidence differs")
	}
	currentPID, err := currentBootstrapNamespaceInode(isolation.NamespacePID)
	if err != nil {
		return err
	}
	currentMount, err := currentBootstrapNamespaceInode(isolation.NamespaceMount)
	if err != nil {
		return err
	}
	if currentPID != pidInode || currentMount != mountInode {
		return errors.New("bootstrap completion PID or mount namespace identity changed")
	}
	return nil
}

// currentBootstrapNamespaceInode verifies one current namespace path is nsfs
// and returns its stable inode for the second-exec completion contract.
func currentBootstrapNamespaceInode(namespace isolation.NamespaceType) (uint64, error) {
	path := "/proc/thread-self/ns/" + string(namespace)
	var filesystem unix.Statfs_t
	if err := unix.Statfs(path, &filesystem); err != nil || uint64(filesystem.Type) != uint64(isolation.NamespaceFSMagic) {
		return 0, fmt.Errorf("current %s namespace is not nsfs", namespace)
	}
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return 0, fmt.Errorf("inspect current %s namespace: %w", namespace, err)
	}
	if stat.Ino == 0 {
		return 0, fmt.Errorf("current %s namespace has no inode", namespace)
	}
	return stat.Ino, nil
}

// verifyBootstrapNamespace proves an inherited descriptor is nsfs, has the
// expected inode, and names the expected namespace kind before setns.
func verifyBootstrapNamespace(namespace BootstrapNamespace) error {
	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(namespace.FD, &filesystem); err != nil || uint64(filesystem.Type) != uint64(isolation.NamespaceFSMagic) {
		return fmt.Errorf("inherited %s descriptor is not nsfs", namespace.Type)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(namespace.FD, &stat); err != nil {
		return fmt.Errorf("inspect inherited %s descriptor: %w", namespace.Type, err)
	}
	if stat.Ino != namespace.Inode {
		return fmt.Errorf("inherited %s namespace inode changed", namespace.Type)
	}
	target, err := os.Readlink("/proc/self/fd/" + strconv.Itoa(namespace.FD))
	if err != nil || !strings.HasPrefix(target, string(namespace.Type)+":[") {
		return fmt.Errorf("inherited descriptor does not name the expected %s namespace", namespace.Type)
	}
	return nil
}

// verifyCurrentBootstrapNamespace proves setns changed the calling thread to
// the exact keeper inode rather than trusting a successful syscall alone.
func verifyCurrentBootstrapNamespace(namespace BootstrapNamespace) error {
	var stat unix.Stat_t
	path := "/proc/thread-self/ns/" + string(namespace.Type)
	if err := unix.Stat(path, &stat); err != nil {
		return fmt.Errorf("inspect joined %s namespace: %w", namespace.Type, err)
	}
	if stat.Ino != namespace.Inode {
		return fmt.Errorf("joined %s namespace inode differs from keeper evidence", namespace.Type)
	}
	return nil
}

// bootstrapNamespaceFlag maps only the three Sandbox namespace kinds admitted by InitBootstrap.
func bootstrapNamespaceFlag(namespace isolation.NamespaceType) (int, error) {
	switch namespace {
	case isolation.NamespaceUTS:
		return unix.CLONE_NEWUTS, nil
	case isolation.NamespaceIPC:
		return unix.CLONE_NEWIPC, nil
	case isolation.NamespaceNetwork:
		return unix.CLONE_NEWNET, nil
	default:
		return 0, errors.New("unsupported init bootstrap namespace")
	}
}
