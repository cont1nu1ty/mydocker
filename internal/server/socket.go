package server

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	linuxUnixPathMax   = 107
	socketProbeTimeout = 100 * time.Millisecond
	socketStagePattern = ".mydocker-socket-"
	socketStageName    = "socket"
)

// unixListenFunc is the narrow bind seam used to prove staging cleanup without
// changing process-wide state in the main test process.
type unixListenFunc func(string, *net.UnixAddr) (*net.UnixListener, error)

// socketParent holds an exclusive advisory lease on the verified runtime
// directory for the complete listener lifetime and records Linux GID inheritance.
type socketParent struct {
	path       string
	file       *os.File
	info       os.FileInfo
	socketGID  uint32
	setgid     bool
	closeOnce  sync.Once
	closeError error
}

// stagedUnixListener owns a listener whose pathname is still hidden below a
// private directory and therefore is not yet part of the public API surface.
type stagedUnixListener struct {
	listener  *net.UnixListener
	parent    *socketParent
	directory string
	path      string
	relative  string
	info      os.FileInfo
}

// publishedUnixListener tracks the inode moved to the configured public path,
// because net.UnixListener otherwise remembers and unlinks only the staging path.
type publishedUnixListener struct {
	*net.UnixListener
	parent     *socketParent
	path       string
	info       os.FileInfo
	closeOnce  sync.Once
	closeError error
}

// listenUnix creates an owner-controlled Unix socket without ever exposing the
// umask-derived bind mode at the configured public path. It rejects symlink,
// foreign-owner, insecure-parent, and active-listener replacement hazards.
func listenUnix(path string, mode os.FileMode) (net.Listener, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("Unix socket path must be absolute")
	}
	if err := validateUnixSocketPath(path, "public Unix socket"); err != nil {
		return nil, err
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
	parent, err := openSocketParent(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if err := removeOwnedStaleSocket(parent, path); err != nil {
		return nil, errors.Join(err, parent.close())
	}
	staged, err := newStagedUnixListenerAt(parent, mode.Perm(), net.ListenUnix)
	if err != nil {
		return nil, errors.Join(err, parent.close())
	}
	return staged.publish(path)
}

// newStagedUnixListener binds below a newly created private directory so a
// permissive process umask cannot make the socket reachable before chmod.
func newStagedUnixListener(parent string, mode os.FileMode) (*stagedUnixListener, error) {
	return newStagedUnixListenerWith(parent, mode, net.ListenUnix)
}

// newStagedUnixListenerWith creates, binds, tightens, and verifies one private
// socket; the injected binder permits deterministic cleanup and umask tests.
func newStagedUnixListenerWith(parent string, mode os.FileMode, bind unixListenFunc) (*stagedUnixListener, error) {
	verifiedParent, err := openSocketParent(parent)
	if err != nil {
		return nil, err
	}
	staged, err := newStagedUnixListenerAt(verifiedParent, mode, bind)
	if err != nil {
		return nil, errors.Join(err, verifiedParent.close())
	}
	return staged, nil
}

// newStagedUnixListenerAt creates and verifies one private socket while
// retaining the caller's parent-directory lease for publication or rollback.
func newStagedUnixListenerAt(parent *socketParent, mode os.FileMode, bind unixListenFunc) (*stagedUnixListener, error) {
	directory, relativeDirectory, err := newSocketStageDirectory(parent)
	if err != nil {
		return nil, err
	}
	stagedPath := filepath.Join(directory, socketStageName)
	relativePath := filepath.Join(relativeDirectory, socketStageName)
	staged := &stagedUnixListener{parent: parent, directory: directory, path: stagedPath, relative: relativePath}
	if err := validateUnixSocketPath(stagedPath, "private Unix socket staging"); err != nil {
		return nil, errors.Join(err, staged.discard())
	}
	if err := parent.verifyPath(); err != nil {
		return nil, errors.Join(err, staged.discard())
	}
	listener, err := bind("unix", &net.UnixAddr{Name: stagedPath, Net: "unix"})
	staged.listener = listener
	if err != nil {
		return nil, errors.Join(fmt.Errorf("listen on private Unix socket: %w", err), staged.discard())
	}
	if listener == nil {
		return nil, errors.Join(errors.New("listen on private Unix socket returned a nil listener"), staged.discard())
	}
	listener.SetUnlinkOnClose(false)
	if err := unix.Fchmodat(int(parent.file.Fd()), relativePath, uint32(mode.Perm()), 0); err != nil {
		return nil, errors.Join(fmt.Errorf("set private Unix socket mode: %w", err), staged.discard())
	}
	info, err := verifyUnixSocketAt(parent, relativePath, mode.Perm(), nil, parent.socketGID)
	if err != nil {
		return nil, errors.Join(err, staged.discard())
	}
	staged.info = info
	return staged, nil
}

// publish atomically moves the already tightened socket into place without
// replacing a path created by a concurrent daemon or filesystem actor.
func (staged *stagedUnixListener) publish(path string) (net.Listener, error) {
	if err := validateUnixSocketPath(path, "public Unix socket"); err != nil {
		return nil, errors.Join(err, staged.discard())
	}
	publicName, err := staged.parent.publicName(path)
	if err != nil {
		return nil, errors.Join(err, staged.discard())
	}
	if err := unix.Renameat2(int(staged.parent.file.Fd()), staged.relative, int(staged.parent.file.Fd()), publicName, unix.RENAME_NOREPLACE); err != nil {
		return nil, errors.Join(fmt.Errorf("publish Unix socket without replacement: %w", err), staged.discard())
	}
	info, err := verifyUnixSocketAt(staged.parent, publicName, staged.info.Mode().Perm(), staged.info, staged.parent.socketGID)
	if err != nil {
		return nil, errors.Join(err, staged.discardPublished(path))
	}
	if err := probePublishedUnixSocket(path); err != nil {
		return nil, errors.Join(err, staged.discardPublished(path))
	}
	if err := removeSocketStageDirectoryAt(staged.parent, filepath.Base(staged.directory)); err != nil {
		return nil, errors.Join(
			fmt.Errorf("remove private Unix socket staging directory: %w", err),
			staged.discardPublished(path),
		)
	}
	published := &publishedUnixListener{UnixListener: staged.listener, parent: staged.parent, path: path, info: info}
	staged.parent = nil
	return published, nil
}

// discard closes an unpublished listener and removes only its private socket
// inode and empty staging directory; callers may invoke it after any failure.
func (staged *stagedUnixListener) discard() error {
	var closeErr error
	if staged.listener != nil {
		staged.listener.SetUnlinkOnClose(false)
		closeErr = staged.listener.Close()
	}
	var removeErr, directoryErr, parentErr error
	if staged.parent != nil {
		removeErr = removePrivateUnixSocketAt(staged.parent, staged.relative, staged.info, "private Unix socket")
		directoryErr = removeSocketStageDirectoryAt(staged.parent, filepath.Base(staged.directory))
		parentErr = staged.parent.close()
		staged.parent = nil
	}
	return errors.Join(closeErr, removeErr, directoryErr, parentErr)
}

// discardPublished rolls back a publication failure while preserving any path
// that no longer names the staged inode.
func (staged *stagedUnixListener) discardPublished(path string) error {
	var removeErr error
	if staged.parent != nil {
		removeErr = removeExpectedUnixSocket(staged.parent, path, staged.info, "published Unix socket")
	}
	var closeErr error
	if staged.listener != nil {
		staged.listener.SetUnlinkOnClose(false)
		closeErr = staged.listener.Close()
	}
	var directoryErr, parentErr error
	if staged.parent != nil {
		directoryErr = removeSocketStageDirectoryAt(staged.parent, filepath.Base(staged.directory))
		parentErr = staged.parent.close()
		staged.parent = nil
	}
	return errors.Join(removeErr, closeErr, directoryErr, parentErr)
}

// Addr reports the configured public pathname instead of the private pathname
// retained by the kernel socket after atomic filesystem publication.
func (listener *publishedUnixListener) Addr() net.Addr {
	return &net.UnixAddr{Name: listener.path, Net: "unix"}
}

// Close closes the listener once and removes the public path only when it still
// names the inode this listener published, making repeated shutdown safe.
func (listener *publishedUnixListener) Close() error {
	listener.closeOnce.Do(func() {
		listener.closeError = errors.Join(
			removeExpectedUnixSocket(listener.parent, listener.path, listener.info, "published Unix socket"),
			listener.UnixListener.Close(),
			listener.parent.close(),
		)
	})
	return listener.closeError
}

// validateUnixSocketPath enforces Linux pathname-socket capacity before any
// bind or publication, so a successful startup always exposes a dialable name.
func validateUnixSocketPath(path, description string) error {
	if strings.ContainsRune(path, '\x00') {
		return fmt.Errorf("%s path must not contain NUL", description)
	}
	if len(path) > linuxUnixPathMax {
		return fmt.Errorf("%s path uses %d bytes; Linux pathname sockets allow at most %d", description, len(path), linuxUnixPathMax)
	}
	return nil
}

// openSocketParent opens the configured directory without following its final
// component, verifies its identity, and holds a nonblocking lifetime flock.
func openSocketParent(path string) (*socketParent, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open Unix socket parent: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open Unix socket parent returned an invalid descriptor")
	}
	if err := lockSocketParent(fd); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("inspect opened Unix socket parent: %w", err), file.Close())
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(info, current) {
		return nil, errors.Join(errors.New("Unix socket parent changed while opening its lease"), err, file.Close())
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
		return nil, errors.Join(errors.New("opened Unix socket parent is not a private real directory"), file.Close())
	}
	if err := requireOwner(info, os.Geteuid(), "Unix socket parent"); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, errors.Join(errors.New("Unix socket parent group metadata is unavailable"), file.Close())
	}
	parent := &socketParent{path: path, file: file, info: info, socketGID: uint32(os.Getegid())}
	if info.Mode()&os.ModeSetgid != 0 {
		parent.setgid = true
		parent.socketGID = stat.Gid
	}
	return parent, nil
}

// lockSocketParent serializes all cooperating daemon publication and cleanup
// against the same directory inode and retries only interrupted syscalls.
func lockSocketParent(fd int) error {
	for {
		err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return errors.New("another daemon holds the Unix socket directory lease")
		}
		if err != nil {
			return fmt.Errorf("lock Unix socket parent: %w", err)
		}
		return nil
	}
}

// verifyPath proves the configured pathname still resolves to the leased
// directory before an unavoidable pathname-based bind or dial operation.
func (parent *socketParent) verifyPath() error {
	if parent == nil || parent.file == nil {
		return errors.New("Unix socket parent lease is unavailable")
	}
	current, err := os.Lstat(parent.path)
	if err != nil {
		return fmt.Errorf("reinspect Unix socket parent path: %w", err)
	}
	if !os.SameFile(parent.info, current) {
		return errors.New("Unix socket parent path no longer names the leased directory")
	}
	return nil
}

// publicName validates that a configured public path is one direct child of
// the leased parent and returns its descriptor-relative basename.
func (parent *socketParent) publicName(path string) (string, error) {
	if filepath.Dir(path) != parent.path {
		return "", errors.New("Unix socket path is outside the leased parent directory")
	}
	if err := parent.verifyPath(); err != nil {
		return "", err
	}
	name := filepath.Base(path)
	if name == "." || name == string(filepath.Separator) {
		return "", errors.New("Unix socket path must name one file")
	}
	return name, nil
}

// close releases the lifetime directory lease exactly once after all socket
// names owned by the listener have been reconciled.
func (parent *socketParent) close() error {
	if parent == nil {
		return nil
	}
	parent.closeOnce.Do(func() {
		if parent.file != nil {
			parent.closeError = parent.file.Close()
		}
	})
	return parent.closeError
}

// stageMode preserves Linux setgid inheritance while keeping the staging
// directory inaccessible to group and other users.
func (parent *socketParent) stageMode() os.FileMode {
	mode := os.FileMode(0o700)
	if parent.setgid {
		mode |= os.ModeSetgid
	}
	return mode
}

// unixFileMode converts the portable os.FileMode setgid bit to Linux chmod bits.
func unixFileMode(mode os.FileMode) uint32 {
	result := uint32(mode.Perm())
	if mode&os.ModeSetgid != 0 {
		result |= unix.S_ISGID
	}
	return result
}

// newSocketStageDirectory creates and descriptor-verifies one private child,
// retaining an inherited setgid group without exposing any group permissions.
func newSocketStageDirectory(parent *socketParent) (string, string, error) {
	if err := parent.verifyPath(); err != nil {
		return "", "", err
	}
	directory, err := os.MkdirTemp(parent.path, socketStagePattern)
	if err != nil {
		return "", "", fmt.Errorf("create private Unix socket staging directory: %w", err)
	}
	relative := filepath.Base(directory)
	if err := unix.Fchmodat(int(parent.file.Fd()), relative, unixFileMode(parent.stageMode()), 0); err != nil {
		return "", "", errors.Join(
			fmt.Errorf("set private Unix socket staging directory mode: %w", err),
			removeSocketStageDirectoryAt(parent, relative),
		)
	}
	if err := verifySocketStageDirectoryAt(parent, relative); err != nil {
		return "", "", errors.Join(err, removeSocketStageDirectoryAt(parent, relative))
	}
	return directory, relative, nil
}

// verifySocketStageDirectoryAt proves the descriptor-relative bind parent is a
// daemon-owned private directory with the expected inherited group and setgid bit.
func verifySocketStageDirectoryAt(parent *socketParent, relative string) error {
	info, err := fileInfoAt(parent, relative)
	if err != nil {
		return fmt.Errorf("inspect private Unix socket staging directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 || (info.Mode()&os.ModeSetgid != 0) != parent.setgid {
		return errors.New("private Unix socket staging path has an invalid type, mode, or setgid state")
	}
	if err := requireOwner(info, os.Geteuid(), "private Unix socket staging directory"); err != nil {
		return err
	}
	return requireGroup(info, parent.socketGID, "private Unix socket staging directory")
}

// fileInfoAt opens one descriptor-relative path without following its final
// component and returns stable metadata for inode comparisons.
func fileInfoAt(parent *socketParent, relative string) (os.FileInfo, error) {
	fd, err := unix.Openat(int(parent.file.Fd()), relative, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), relative)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open descriptor-relative path returned an invalid descriptor")
	}
	defer file.Close()
	return file.Stat()
}

// verifyUnixSocketAt checks type, exact mode, owner, group, and optional inode
// identity through the leased parent descriptor.
func verifyUnixSocketAt(parent *socketParent, relative string, mode os.FileMode, expected os.FileInfo, expectedGID uint32) (os.FileInfo, error) {
	info, err := fileInfoAt(parent, relative)
	if err != nil {
		return nil, fmt.Errorf("verify Unix socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != mode.Perm() {
		return nil, errors.New("Unix socket type or mode changed during startup")
	}
	if expected != nil && !os.SameFile(expected, info) {
		return nil, errors.New("Unix socket inode changed during startup")
	}
	if err := requireOwner(info, os.Geteuid(), "Unix socket"); err != nil {
		return nil, err
	}
	if err := requireGroup(info, expectedGID, "Unix socket"); err != nil {
		return nil, err
	}
	return info, nil
}

// removePrivateUnixSocketAt removes a socket only inside the leased private
// staging directory, where no cooperating daemon can publish a replacement.
func removePrivateUnixSocketAt(parent *socketParent, relative string, expected os.FileInfo, description string) error {
	if relative == "" {
		return nil
	}
	info, err := fileInfoAt(parent, relative)
	if errors.Is(err, syscall.ENOENT) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect %s during cleanup: %w", description, err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 || expected != nil && !os.SameFile(expected, info) {
		return fmt.Errorf("refusing to remove changed %s", description)
	}
	if err := unix.Unlinkat(int(parent.file.Fd()), relative, 0); err != nil && !errors.Is(err, syscall.ENOENT) {
		return fmt.Errorf("remove %s: %w", description, err)
	}
	return nil
}

// removeExpectedUnixSocket atomically quarantines a public name before inode
// comparison; a mismatched socket is restored and is never unlinked by this listener.
func removeExpectedUnixSocket(parent *socketParent, path string, expected os.FileInfo, description string) error {
	publicName, err := parent.publicName(path)
	if err != nil {
		return err
	}
	directory, relativeDirectory, err := newSocketStageDirectory(parent)
	if err != nil {
		return err
	}
	claimedRelative := filepath.Join(relativeDirectory, socketStageName)
	if err := unix.Renameat2(int(parent.file.Fd()), publicName, int(parent.file.Fd()), claimedRelative, unix.RENAME_NOREPLACE); err != nil {
		directoryErr := removeSocketStageDirectoryAt(parent, relativeDirectory)
		if errors.Is(err, syscall.ENOENT) {
			return directoryErr
		}
		return errors.Join(fmt.Errorf("quarantine %s: %w", description, err), directoryErr)
	}
	claimed, inspectErr := fileInfoAt(parent, claimedRelative)
	if inspectErr == nil && expected != nil && os.SameFile(expected, claimed) {
		removeErr := unix.Unlinkat(int(parent.file.Fd()), claimedRelative, 0)
		return errors.Join(normalizeUnlinkError(removeErr, description), removeSocketStageDirectoryAt(parent, relativeDirectory))
	}
	restoreErr := unix.Renameat2(int(parent.file.Fd()), claimedRelative, int(parent.file.Fd()), publicName, unix.RENAME_NOREPLACE)
	if restoreErr != nil {
		return errors.Join(
			fmt.Errorf("refusing to remove changed %s; preserve it at %s", description, filepath.Join(directory, socketStageName)),
			inspectErr,
			fmt.Errorf("restore changed %s: %w", description, restoreErr),
		)
	}
	return errors.Join(
		fmt.Errorf("refusing to remove changed %s inode", description),
		inspectErr,
		removeSocketStageDirectoryAt(parent, relativeDirectory),
	)
}

// normalizeUnlinkError makes an already absent quarantined inode idempotent and
// adds cleanup context to all other descriptor-relative unlink failures.
func normalizeUnlinkError(err error, description string) error {
	if err == nil || errors.Is(err, syscall.ENOENT) {
		return nil
	}
	return fmt.Errorf("remove quarantined %s: %w", description, err)
}

// removeSocketStageDirectoryAt removes one empty descriptor-relative private
// directory and treats an already absent directory as idempotent success.
func removeSocketStageDirectoryAt(parent *socketParent, relative string) error {
	if parent == nil || relative == "" {
		return nil
	}
	if err := unix.Unlinkat(int(parent.file.Fd()), relative, unix.AT_REMOVEDIR); err != nil && !errors.Is(err, syscall.ENOENT) {
		return fmt.Errorf("remove private Unix socket staging directory: %w", err)
	}
	return nil
}

// probePublishedUnixSocket confirms the final Linux sockaddr path reaches the
// staged listening inode before startup reports success.
func probePublishedUnixSocket(path string) error {
	connection, err := net.DialTimeout("unix", path, socketProbeTimeout)
	if err != nil {
		return fmt.Errorf("dial atomically published Unix socket: %w", err)
	}
	if err := connection.Close(); err != nil {
		return fmt.Errorf("close published Unix socket probe: %w", err)
	}
	return nil
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
func removeOwnedStaleSocket(parent *socketParent, path string) error {
	publicName, err := parent.publicName(path)
	if err != nil {
		return err
	}
	info, err := fileInfoAt(parent, publicName)
	if errors.Is(err, syscall.ENOENT) {
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
	return removeExpectedUnixSocket(parent, path, info, "owned stale Unix socket")
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

// requireGroup verifies a filesystem object against the GID selected by Linux
// setgid-parent inheritance or the daemon's effective group.
func requireGroup(info os.FileInfo, expectedGID uint32, description string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%s group metadata is unavailable", description)
	}
	if stat.Gid != expectedGID {
		return fmt.Errorf("%s is owned by gid %d instead of %d", description, stat.Gid, expectedGID)
	}
	return nil
}
