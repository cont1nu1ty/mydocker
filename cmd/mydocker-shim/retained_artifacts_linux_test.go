//go:build linux

package main

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"golang.org/x/sys/unix"

	"mydocker/internal/shim"
)

// TestRetainInitArtifactsRejectsSymlinkDirectory verifies O_NOFOLLOW applies to
// the owner-directory path itself before a retained authority FD can be issued.
func TestRetainInitArtifactsRejectsSymlinkDirectory(t *testing.T) {
	parent := t.TempDir()
	realDirectory := filepath.Join(parent, "real-owner")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "linked-owner")
	if err := os.Symlink(realDirectory, link); err != nil {
		t.Fatal(err)
	}
	if directory, _, err := retainInitArtifacts(retainedArtifactTestConfig(link)); err == nil {
		if directory != nil {
			_ = directory.Close()
		}
		t.Fatal("retainInitArtifacts() accepted a symlink owner directory")
	}
}

// TestRetainInitArtifactsRejectsWideDirectory verifies an otherwise valid
// same-owner directory cannot grant group or other access.
func TestRetainInitArtifactsRejectsWideDirectory(t *testing.T) {
	directoryPath := filepath.Join(t.TempDir(), "wide-owner")
	if err := os.Mkdir(directoryPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directoryPath, 0o750); err != nil {
		t.Fatal(err)
	}
	directory, _, err := retainInitArtifacts(retainedArtifactTestConfig(directoryPath))
	if directory != nil {
		_ = directory.Close()
	}
	if !errors.Is(err, shim.ErrUnsafeArtifact) {
		t.Fatalf("retainInitArtifacts() error = %v, want ErrUnsafeArtifact", err)
	}
}

// TestRetainInitArtifactsAcceptsPrivateOwnedDirectory verifies the retained
// authority is a close-on-exec directory FD owned by the effective user.
func TestRetainInitArtifactsAcceptsPrivateOwnedDirectory(t *testing.T) {
	directoryPath := filepath.Join(t.TempDir(), "private-owner")
	if err := os.Mkdir(directoryPath, 0o700); err != nil {
		t.Fatal(err)
	}
	directory, retained, err := retainInitArtifacts(retainedArtifactTestConfig(directoryPath))
	if err != nil {
		t.Fatalf("retainInitArtifacts() error = %v", err)
	}
	defer directory.Close()
	var metadata unix.Stat_t
	if err := unix.Fstat(int(directory.Fd()), &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.Mode&unix.S_IFMT != unix.S_IFDIR || metadata.Uid != uint32(os.Geteuid()) || metadata.Mode&0o7777 != retainedOwnerDirectoryMode {
		t.Fatalf("retained directory metadata = mode %#o uid %d", metadata.Mode, metadata.Uid)
	}
	flags, err := unix.FcntlInt(directory.Fd(), unix.F_GETFD, 0)
	if err != nil {
		t.Fatal(err)
	}
	if flags&unix.FD_CLOEXEC == 0 {
		t.Fatal("retained directory descriptor is not close-on-exec")
	}
	wantRoot := filepath.Join("/proc/self/fd", strconv.FormatUint(uint64(directory.Fd()), 10))
	if filepath.Dir(retained.ControlSocket) != filepath.Dir(retained.TerminalPath) || filepath.Dir(retained.ControlSocket) != wantRoot {
		t.Fatalf("retained paths = %q and %q, want one descriptor root %q", retained.ControlSocket, retained.TerminalPath, wantRoot)
	}
}

// TestRetainInitArtifactsStaysBoundToRenamedDirectory verifies a same-path
// replacement cannot redirect terminal/control operations away from the inode
// opened before pivot_root.
func TestRetainInitArtifactsStaysBoundToRenamedDirectory(t *testing.T) {
	parent := t.TempDir()
	originalPath := filepath.Join(parent, "owner")
	if err := os.Mkdir(originalPath, 0o700); err != nil {
		t.Fatal(err)
	}
	directory, retained, err := retainInitArtifacts(retainedArtifactTestConfig(originalPath))
	if err != nil {
		t.Fatalf("retainInitArtifacts() error = %v", err)
	}
	defer directory.Close()
	var retainedMetadata unix.Stat_t
	if err := unix.Fstat(int(directory.Fd()), &retainedMetadata); err != nil {
		t.Fatal(err)
	}
	renamedPath := filepath.Join(parent, "owner-retained")
	if err := os.Rename(originalPath, renamedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(originalPath, 0o700); err != nil {
		t.Fatal(err)
	}
	replacementPayload := []byte("replacement-must-remain-untouched")
	if err := os.WriteFile(filepath.Join(originalPath, "terminal.json"), replacementPayload, 0o600); err != nil {
		t.Fatal(err)
	}
	retainedPayload := []byte("retained-inode")
	if err := os.WriteFile(retained.TerminalPath, retainedPayload, 0o600); err != nil {
		t.Fatalf("write through retained descriptor path: %v", err)
	}
	if payload, err := os.ReadFile(filepath.Join(renamedPath, "terminal.json")); err != nil || string(payload) != string(retainedPayload) {
		t.Fatalf("renamed original payload = %q, %v", payload, err)
	}
	if payload, err := os.ReadFile(filepath.Join(originalPath, "terminal.json")); err != nil || string(payload) != string(replacementPayload) {
		t.Fatalf("replacement payload = %q, %v; retained write escaped its inode", payload, err)
	}
	var rootMetadata unix.Stat_t
	if err := unix.Stat(filepath.Dir(retained.TerminalPath), &rootMetadata); err != nil {
		t.Fatal(err)
	}
	if rootMetadata.Dev != retainedMetadata.Dev || rootMetadata.Ino != retainedMetadata.Ino {
		t.Fatalf("retained procfs root inode = %d:%d, want %d:%d", rootMetadata.Dev, rootMetadata.Ino, retainedMetadata.Dev, retainedMetadata.Ino)
	}
	var replacementMetadata unix.Stat_t
	if err := unix.Stat(originalPath, &replacementMetadata); err != nil {
		t.Fatal(err)
	}
	if replacementMetadata.Dev == retainedMetadata.Dev && replacementMetadata.Ino == retainedMetadata.Ino {
		t.Fatal("replacement unexpectedly shares retained directory identity")
	}
}

// retainedArtifactTestConfig provides only the three paths consumed by the retention boundary.
func retainedArtifactTestConfig(directory string) shim.RuntimeConfig {
	return shim.RuntimeConfig{
		ControlSocket: filepath.Join(directory, "control.sock"),
		TerminalPath:  filepath.Join(directory, "terminal.json"),
		LogPath:       filepath.Join(directory, "workload.log"),
	}
}
