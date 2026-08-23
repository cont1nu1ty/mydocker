package server

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const socketProbeTimeout = 100 * time.Millisecond

// listenUnix creates an owner-controlled Unix socket after rejecting symlink,
// foreign-owner, insecure-parent, and active-listener replacement hazards.
func listenUnix(path string, mode os.FileMode) (*net.UnixListener, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("Unix socket path must be absolute")
	}
	if mode == 0 {
		mode = 0o660
	}
	if mode&^os.FileMode(0o777) != 0 || mode&0o600 != 0o600 || mode&0o007 != 0 {
		return nil, fmt.Errorf("Unix socket mode %04o must grant owner read/write and no access to other users", mode.Perm())
	}
	if err := ensureSocketParent(filepath.Dir(path)); err != nil {
		return nil, err
	}
	if err := removeOwnedStaleSocket(path); err != nil {
		return nil, err
	}
	address := &net.UnixAddr{Name: path, Net: "unix"}
	listener, err := net.ListenUnix("unix", address)
	if err != nil {
		return nil, fmt.Errorf("listen on Unix socket: %w", err)
	}
	listener.SetUnlinkOnClose(true)
	if err := os.Chmod(path, mode.Perm()); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("set Unix socket mode: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("verify Unix socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != mode.Perm() {
		_ = listener.Close()
		return nil, errors.New("Unix socket type or mode changed during startup")
	}
	if err := requireOwner(info, os.Geteuid(), "Unix socket"); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return listener, nil
}

// ensureSocketParent creates only the final private runtime directory and
// rejects an existing symlink, foreign owner, or group/world-writable directory.
func ensureSocketParent(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(path, 0o750); err != nil {
			return fmt.Errorf("create Unix socket directory: %w", err)
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return fmt.Errorf("inspect Unix socket directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("Unix socket parent must be a real directory")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("Unix socket parent mode %04o permits non-owner writes", info.Mode().Perm())
	}
	return requireOwner(info, os.Geteuid(), "Unix socket parent")
}

// removeOwnedStaleSocket removes only an inactive socket whose inode remains
// unchanged and whose owner matches the daemon; all other existing paths fail closed.
func removeOwnedStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect existing Unix socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("refusing to replace a non-socket Unix path")
	}
	if err := requireOwner(info, os.Geteuid(), "existing Unix socket"); err != nil {
		return err
	}
	connection, dialErr := net.DialTimeout("unix", path, socketProbeTimeout)
	if dialErr == nil {
		_ = connection.Close()
		return errors.New("another daemon is already listening on the Unix socket")
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) && !errors.Is(dialErr, syscall.ENOENT) {
		return fmt.Errorf("cannot prove existing Unix socket is stale: %w", dialErr)
	}
	current, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("reinspect stale Unix socket: %w", err)
	}
	if !os.SameFile(info, current) || current.Mode()&os.ModeSocket == 0 {
		return errors.New("Unix socket changed while checking stale ownership")
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove owned stale Unix socket: %w", err)
	}
	return nil
}

// requireOwner verifies a filesystem object against the daemon's effective UID.
func requireOwner(info os.FileInfo, expectedUID int, description string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%s owner metadata is unavailable", description)
	}
	if int(stat.Uid) != expectedUID {
		return fmt.Errorf("%s is owned by uid %d instead of %d", description, stat.Uid, expectedUID)
	}
	return nil
}
