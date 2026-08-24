package server

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	wideUmaskHelperEnv  = "MYDOCKER_WIDE_UMASK_SOCKET_HELPER"
	wideUmaskRootEnv    = "MYDOCKER_WIDE_UMASK_SOCKET_ROOT"
	wideUmaskHelperTest = "TestListenUnixWideUmaskHelper"
)

// TestListenUnixUsesPrivateStageUnderWideUmask runs the umask mutation in an
// isolated process and verifies that the public path appears only at its exact mode.
func TestListenUnixUsesPrivateStageUnderWideUmask(t *testing.T) {
	root := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^"+wideUmaskHelperTest+"$")
	command.Env = append(os.Environ(), wideUmaskHelperEnv+"=1", wideUmaskRootEnv+"="+root)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("wide-umask socket helper failed: %v\n%s", err, output)
	}
}

// TestListenUnixPreservesSetgidParentGroup verifies the private staging layer
// keeps the public parent's inherited group for a group-readable socket.
func TestListenUnixPreservesSetgidParentGroup(t *testing.T) {
	groupID := alternateProcessGroup(t)
	parent := filepath.Join(t.TempDir(), "runtime")
	if err := os.Mkdir(parent, 0o750); err != nil {
		t.Fatalf("create socket parent: %v", err)
	}
	if err := os.Chown(parent, -1, groupID); err != nil {
		t.Fatalf("select socket parent group %d: %v", groupID, err)
	}
	if err := os.Chmod(parent, os.ModeSetgid|0o750); err != nil {
		t.Fatalf("set socket parent setgid mode: %v", err)
	}
	socketPath := filepath.Join(parent, "mydockerd.sock")
	listener, err := listenUnix(socketPath, 0o660)
	if err != nil {
		t.Fatalf("listen below setgid parent: %v", err)
	}
	info, err := os.Lstat(socketPath)
	if err != nil {
		_ = listener.Close()
		t.Fatalf("inspect setgid socket: %v", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Gid) != groupID {
		_ = listener.Close()
		t.Fatalf("published socket gid metadata = %#v, want gid %d", info.Sys(), groupID)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close setgid socket: %v", err)
	}
	assertNoSocketStages(t, parent)
}

// TestPublishedListenerNeverUnlinksReplacement verifies conditional cleanup
// restores a different live inode before the old listener releases its lease.
func TestPublishedListenerNeverUnlinksReplacement(t *testing.T) {
	parent := t.TempDir()
	socketPath := filepath.Join(parent, "mydockerd.sock")
	original, err := listenUnix(socketPath, 0o600)
	if err != nil {
		t.Fatalf("listen original socket: %v", err)
	}
	if err := os.Remove(socketPath); err != nil {
		_ = original.Close()
		t.Fatalf("unlink original public name: %v", err)
	}
	replacement, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		_ = original.Close()
		t.Fatalf("bind replacement socket: %v", err)
	}
	defer replacement.Close()
	if err := original.Close(); err == nil || !strings.Contains(err.Error(), "changed published Unix socket inode") {
		t.Fatalf("close original over replacement error = %v, want changed-inode refusal", err)
	}
	connection, err := net.DialTimeout("unix", socketPath, time.Second)
	if err != nil {
		t.Fatalf("replacement socket was removed by old listener: %v", err)
	}
	_ = connection.Close()
	assertNoSocketStages(t, parent)
}

// TestListenUnixHoldsParentLeaseForListenerLifetime verifies a second daemon
// cannot enter stale cleanup or publication until the first listener is closed.
func TestListenUnixHoldsParentLeaseForListenerLifetime(t *testing.T) {
	parent := t.TempDir()
	firstPath := filepath.Join(parent, "first.sock")
	first, err := listenUnix(firstPath, 0o600)
	if err != nil {
		t.Fatalf("listen first socket: %v", err)
	}
	secondPath := filepath.Join(parent, "second.sock")
	if _, err := listenUnix(secondPath, 0o600); err == nil || !strings.Contains(err.Error(), "directory lease") {
		_ = first.Close()
		t.Fatalf("concurrent parent listener error = %v, want lease refusal", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first socket: %v", err)
	}
	second, err := listenUnix(secondPath, 0o600)
	if err != nil {
		t.Fatalf("listen second socket after lease release: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("close second socket: %v", err)
	}
	assertNoSocketStages(t, parent)
}

// TestListenUnixEnforcesLinuxPathCapacity verifies the exact pathname limit is
// dialable and an overlong public name fails before creating staging artifacts.
func TestListenUnixEnforcesLinuxPathCapacity(t *testing.T) {
	parent := t.TempDir()
	nameLength := linuxUnixPathMax - len(parent) - 1
	if nameLength <= 0 || nameLength >= 255 {
		t.Skipf("temporary path length %d cannot form a boundary socket name", len(parent))
	}
	validPath := filepath.Join(parent, strings.Repeat("s", nameLength))
	if len(validPath) != linuxUnixPathMax {
		t.Fatalf("valid boundary path length = %d, want %d", len(validPath), linuxUnixPathMax)
	}
	listener, err := listenUnix(validPath, 0o600)
	if err != nil {
		t.Fatalf("listen at Linux pathname boundary: %v", err)
	}
	connection, err := net.DialTimeout("unix", validPath, time.Second)
	if err != nil {
		_ = listener.Close()
		t.Fatalf("dial Linux pathname boundary: %v", err)
	}
	_ = connection.Close()
	if err := listener.Close(); err != nil {
		t.Fatalf("close boundary listener: %v", err)
	}
	if _, err := listenUnix(validPath+"x", 0o600); err == nil || !strings.Contains(err.Error(), "allow at most") {
		t.Fatalf("overlong public path error = %v, want Linux capacity rejection", err)
	}
	assertNoSocketStages(t, parent)
}

// TestListenUnixRejectsOverlongPrivateStage verifies a valid public name is
// rejected cleanly when the required private bind pathname would exceed sun_path.
func TestListenUnixRejectsOverlongPrivateStage(t *testing.T) {
	root := t.TempDir()
	targetParentLength := linuxUnixPathMax - len("/s")
	componentLength := targetParentLength - len(root) - 1
	if componentLength <= 0 || componentLength >= 255 {
		t.Skipf("temporary path length %d cannot form a staging-boundary parent", len(root))
	}
	parent := filepath.Join(root, strings.Repeat("p", componentLength))
	if err := os.Mkdir(parent, 0o750); err != nil {
		t.Fatalf("create long socket parent: %v", err)
	}
	publicPath := filepath.Join(parent, "s")
	if len(publicPath) != linuxUnixPathMax {
		t.Fatalf("public path length = %d, want %d", len(publicPath), linuxUnixPathMax)
	}
	if _, err := listenUnix(publicPath, 0o600); err == nil || !strings.Contains(err.Error(), "private Unix socket staging path uses") {
		t.Fatalf("overlong staging path error = %v, want explicit private capacity rejection", err)
	}
	if _, err := os.Lstat(publicPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("public socket appeared after staging length rejection: %v", err)
	}
	assertNoSocketStages(t, parent)
}

// TestListenUnixWideUmaskHelper proves the raw 0777 bind is contained below a
// verified 0700 directory until chmod and atomic publication complete.
func TestListenUnixWideUmaskHelper(t *testing.T) {
	if os.Getenv(wideUmaskHelperEnv) != "1" {
		t.Skip("subprocess helper")
	}
	previousUmask := syscall.Umask(0)
	defer syscall.Umask(previousUmask)

	parent := filepath.Join(os.Getenv(wideUmaskRootEnv), "runtime")
	if err := ensureSocketParent(parent); err != nil {
		t.Fatalf("ensure socket parent: %v", err)
	}
	finalPath := filepath.Join(parent, "mydockerd.sock")
	staged, err := newStagedUnixListenerWith(parent, 0o660, func(network string, address *net.UnixAddr) (*net.UnixListener, error) {
		listener, bindErr := net.ListenUnix(network, address)
		if bindErr != nil {
			return nil, bindErr
		}
		stageInfo, statErr := os.Lstat(filepath.Dir(address.Name))
		if statErr != nil {
			_ = listener.Close()
			t.Fatalf("inspect staging directory during bind: %v", statErr)
		}
		if stageInfo.Mode().Perm() != 0o700 {
			_ = listener.Close()
			t.Fatalf("staging directory mode during bind = %04o, want 0700", stageInfo.Mode().Perm())
		}
		rawInfo, statErr := os.Lstat(address.Name)
		if statErr != nil {
			_ = listener.Close()
			t.Fatalf("inspect raw socket during bind: %v", statErr)
		}
		if rawInfo.Mode().Perm() != 0o777 {
			_ = listener.Close()
			t.Fatalf("raw socket mode under umask 000 = %04o, want 0777", rawInfo.Mode().Perm())
		}
		if _, statErr := os.Lstat(finalPath); !errors.Is(statErr, os.ErrNotExist) {
			_ = listener.Close()
			t.Fatalf("public socket was visible before chmod and publication: %v", statErr)
		}
		return listener, nil
	})
	if err != nil {
		t.Fatalf("create staged listener: %v", err)
	}
	listener, err := staged.publish(finalPath)
	if err != nil {
		t.Fatalf("publish staged listener: %v", err)
	}
	address, ok := listener.Addr().(*net.UnixAddr)
	if !ok || address.Name != finalPath {
		t.Fatalf("published listener address = %#v, want %q", listener.Addr(), finalPath)
	}
	info, err := os.Lstat(finalPath)
	if err != nil {
		t.Fatalf("inspect published socket: %v", err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o660 {
		t.Fatalf("published socket mode = %v, want socket 0660", info.Mode())
	}
	connection, err := net.DialTimeout("unix", finalPath, time.Second)
	if err != nil {
		t.Fatalf("dial atomically published socket: %v", err)
	}
	_ = connection.Close()
	if err := listener.Close(); err != nil {
		t.Fatalf("close published listener: %v", err)
	}
	if _, err := os.Lstat(finalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("published socket remained after close: %v", err)
	}
	assertNoSocketStages(t, parent)
}

// TestNewStagedUnixListenerCleansUpBindFailure verifies a failed bind leaves no
// private staging directory or public filesystem entry.
func TestNewStagedUnixListenerCleansUpBindFailure(t *testing.T) {
	parent := t.TempDir()
	injected := errors.New("injected bind failure")
	var stagedDirectory string
	var failedListener *net.UnixListener
	_, err := newStagedUnixListenerWith(parent, 0o600, func(_ string, address *net.UnixAddr) (*net.UnixListener, error) {
		stagedDirectory = filepath.Dir(address.Name)
		listener, bindErr := net.ListenUnix("unix", address)
		if bindErr != nil {
			return nil, bindErr
		}
		failedListener = listener
		return listener, injected
	})
	if !errors.Is(err, injected) {
		t.Fatalf("staged bind error = %v, want injected failure", err)
	}
	if err := failedListener.SetDeadline(time.Now()); err == nil {
		t.Fatal("listener remained open after bind failure")
	}
	if _, err := os.Lstat(stagedDirectory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging directory remained after bind failure: %v", err)
	}
	assertNoSocketStages(t, parent)
}

// TestStagedUnixListenerDoesNotReplaceConcurrentPath verifies atomic publication
// fails closed and cleans its listener when a path appears after stale checking.
func TestStagedUnixListenerDoesNotReplaceConcurrentPath(t *testing.T) {
	parent := t.TempDir()
	finalPath := filepath.Join(parent, "mydockerd.sock")
	staged, err := newStagedUnixListener(parent, 0o600)
	if err != nil {
		t.Fatalf("create staged listener: %v", err)
	}
	stagedDirectory := staged.directory
	if err := os.Symlink("sentinel-target", finalPath); err != nil {
		t.Fatalf("create concurrent symlink: %v", err)
	}
	if _, err := staged.publish(finalPath); err == nil {
		t.Fatal("publication replaced a concurrently created path")
	}
	target, err := os.Readlink(finalPath)
	if err != nil || target != "sentinel-target" {
		t.Fatalf("concurrent symlink changed: %q, %v", target, err)
	}
	if _, err := os.Lstat(stagedDirectory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging directory remained after publication failure: %v", err)
	}
	if err := staged.listener.SetDeadline(time.Now()); err == nil {
		t.Fatal("staged listener remained open after publication failure")
	}
	assertNoSocketStages(t, parent)
}

// TestListenUnixHandlesExistingPaths verifies symlinks and active sockets are
// preserved while an owned stale socket is safely replaced by the staged listener.
func TestListenUnixHandlesExistingPaths(t *testing.T) {
	t.Run("symlink", func(t *testing.T) {
		parent := t.TempDir()
		finalPath := filepath.Join(parent, "mydockerd.sock")
		if err := os.Symlink("sentinel-target", finalPath); err != nil {
			t.Fatalf("create symlink: %v", err)
		}
		if _, err := listenUnix(finalPath, 0o600); err == nil {
			t.Fatal("listenUnix replaced an existing symlink")
		}
		target, err := os.Readlink(finalPath)
		if err != nil || target != "sentinel-target" {
			t.Fatalf("existing symlink changed: %q, %v", target, err)
		}
		assertNoSocketStages(t, parent)
	})

	t.Run("active socket", func(t *testing.T) {
		parent := t.TempDir()
		finalPath := filepath.Join(parent, "mydockerd.sock")
		active, err := net.ListenUnix("unix", &net.UnixAddr{Name: finalPath, Net: "unix"})
		if err != nil {
			t.Fatalf("bind active socket: %v", err)
		}
		defer active.Close()
		if _, err := listenUnix(finalPath, 0o600); err == nil {
			t.Fatal("listenUnix replaced an active socket")
		}
		connection, err := net.DialTimeout("unix", finalPath, time.Second)
		if err != nil {
			t.Fatalf("active socket became unreachable: %v", err)
		}
		_ = connection.Close()
		assertNoSocketStages(t, parent)
	})

	t.Run("stale socket", func(t *testing.T) {
		parent := t.TempDir()
		finalPath := filepath.Join(parent, "mydockerd.sock")
		stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: finalPath, Net: "unix"})
		if err != nil {
			t.Fatalf("bind stale socket: %v", err)
		}
		stale.SetUnlinkOnClose(false)
		if err := stale.Close(); err != nil {
			t.Fatalf("close stale socket: %v", err)
		}
		listener, err := listenUnix(finalPath, 0o640)
		if err != nil {
			t.Fatalf("replace owned stale socket: %v", err)
		}
		info, err := os.Lstat(finalPath)
		if err != nil || info.Mode().Perm() != 0o640 {
			t.Fatalf("replacement socket mode = %v, %v, want 0640", info, err)
		}
		connection, err := net.DialTimeout("unix", finalPath, time.Second)
		if err != nil {
			t.Fatalf("dial replacement socket: %v", err)
		}
		_ = connection.Close()
		if err := listener.Close(); err != nil {
			t.Fatalf("close replacement socket: %v", err)
		}
		if _, err := os.Lstat(finalPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("replacement socket remained after close: %v", err)
		}
		assertNoSocketStages(t, parent)
	})
}

// assertNoSocketStages fails when a private staging directory survives success
// or rollback, while ignoring unrelated entries owned by the surrounding test.
func assertNoSocketStages(t *testing.T, parent string) {
	t.Helper()
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatalf("read socket parent: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), socketStagePattern) {
			t.Errorf("private socket staging entry remained: %s", entry.Name())
		}
	}
}

// alternateProcessGroup returns one supplementary group distinct from the
// effective GID so setgid inheritance is observable, or skips the test.
func alternateProcessGroup(t *testing.T) int {
	t.Helper()
	groups, err := os.Getgroups()
	if err != nil {
		t.Fatalf("list process groups: %v", err)
	}
	for _, groupID := range groups {
		if groupID != os.Getegid() {
			return groupID
		}
	}
	t.Skip("process has no supplementary group distinct from its effective GID")
	return -1
}
