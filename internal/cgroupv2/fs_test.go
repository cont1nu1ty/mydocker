package cgroupv2

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// TestOSFileSystemUsesExistingFilesAndExactRemoval verifies ordinary temporary paths without cgroup mounts or privileges.
func TestOSFileSystemUsesExistingFilesAndExactRemoval(t *testing.T) {
	filesystem := OSFileSystem{}
	root := t.TempDir()
	controlPath := filepath.Join(root, "control")
	if err := os.WriteFile(controlPath, []byte("old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := filesystem.WriteFile(controlPath, []byte("new\n")); err != nil {
		t.Fatalf("WriteFile(existing) error = %v", err)
	}
	value, err := filesystem.ReadFile(controlPath)
	if err != nil || string(value) != "new\n" {
		t.Fatalf("ReadFile() = %q, %v", value, err)
	}

	missingPath := filepath.Join(root, "must-not-be-created")
	if err := filesystem.WriteFile(missingPath, []byte("value")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("WriteFile(missing) error = %v, want fs.ErrNotExist", err)
	}
	if _, err := os.Lstat(missingPath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing control was unexpectedly created: %v", err)
	}

	handle, err := filesystem.OpenDir(root)
	if err != nil {
		t.Fatalf("OpenDir() error = %v", err)
	}
	if handle.Fd() == 0 {
		t.Fatal("OpenDir() returned descriptor zero")
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("directory handle Close() error = %v", err)
	}

	nonEmpty := filepath.Join(root, "non-empty")
	if err := filesystem.Mkdir(nonEmpty, 0o755); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(nonEmpty, "child")
	if err := os.WriteFile(child, []byte("owned"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := filesystem.Remove(nonEmpty); err == nil {
		t.Fatal("Remove(non-empty) error = nil")
	}
	if _, err := os.Stat(child); err != nil {
		t.Fatalf("exact Remove traversed child: %v", err)
	}
}

// TestOSFileSystemOpenDirDoesNotFollowFinalSymlink verifies the production descriptor boundary rejects alias paths.
func TestOSFileSystemOpenDirDoesNotFollowFinalSymlink(t *testing.T) {
	filesystem := OSFileSystem{}
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	info, err := filesystem.Lstat(alias)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&fs.ModeSymlink == 0 {
		t.Fatalf("Lstat(alias) mode = %v, want symlink", info.Mode())
	}
	if handle, err := filesystem.OpenDir(alias); err == nil {
		_ = handle.Close()
		t.Fatal("OpenDir(symlink) error = nil")
	}
}

// TestLinuxHostProbeRejectsOrdinaryFilesystem verifies the concrete probe does not mistake a temporary host directory for cgroup v2.
func TestLinuxHostProbeRejectsOrdinaryFilesystem(t *testing.T) {
	supported, err := (LinuxHostProbe{}).IsCgroupV2(t.TempDir())
	if err != nil {
		t.Fatalf("IsCgroupV2(temp directory) error = %v", err)
	}
	if supported {
		t.Fatal("IsCgroupV2(temp directory) = true")
	}
}

// TestLinuxHostProbeReportsPageSize verifies canonical memory-limit calculations receive a positive host fact without privileges.
func TestLinuxHostProbeReportsPageSize(t *testing.T) {
	pageSize, err := (LinuxHostProbe{}).PageSize()
	if err != nil {
		t.Fatalf("PageSize() error = %v", err)
	}
	if pageSize == 0 || pageSize&(pageSize-1) != 0 {
		t.Fatalf("PageSize() = %d, want a positive power of two", pageSize)
	}
}
