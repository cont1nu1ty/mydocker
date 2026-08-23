package logstore

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// TestOpenRejectsUnsafePaths verifies canonical location, parent privacy, symlink, file mode, and hard-link checks.
func TestOpenRejectsUnsafePaths(t *testing.T) {
	identity := testIdentity()
	t.Run("relative", func(t *testing.T) {
		if _, err := Open("relative.log", identity); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Open error = %v, want ErrUnsafePath", err)
		}
	})
	t.Run("permissive parent", func(t *testing.T) {
		parent := t.TempDir()
		if err := os.Chmod(parent, 0o755); err != nil {
			t.Fatalf("chmod parent: %v", err)
		}
		if _, err := Open(filepath.Join(parent, "attempt.log"), identity); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Open error = %v, want ErrUnsafePath", err)
		}
	})
	t.Run("final symlink", func(t *testing.T) {
		parent := t.TempDir()
		target := filepath.Join(parent, "target")
		if err := os.WriteFile(target, nil, 0o600); err != nil {
			t.Fatalf("create target: %v", err)
		}
		link := filepath.Join(parent, "attempt.log")
		if err := os.Symlink(target, link); err != nil {
			t.Fatalf("create symlink: %v", err)
		}
		if _, err := Open(link, identity); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Open error = %v, want ErrUnsafePath", err)
		}
	})
	t.Run("parent symlink", func(t *testing.T) {
		root := t.TempDir()
		realParent := filepath.Join(root, "real")
		if err := os.Mkdir(realParent, 0o700); err != nil {
			t.Fatalf("create real parent: %v", err)
		}
		linkParent := filepath.Join(root, "linked")
		if err := os.Symlink(realParent, linkParent); err != nil {
			t.Fatalf("create parent symlink: %v", err)
		}
		if _, err := Open(filepath.Join(linkParent, "attempt.log"), identity); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Open error = %v, want ErrUnsafePath", err)
		}
	})
	t.Run("permissive file", func(t *testing.T) {
		path := newLogPath(t)
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatalf("create permissive file: %v", err)
		}
		if _, err := Open(path, identity); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Open error = %v, want ErrUnsafePath", err)
		}
	})
	t.Run("hard linked file", func(t *testing.T) {
		path := newLogPath(t)
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("create source file: %v", err)
		}
		if err := os.Link(path, filepath.Join(filepath.Dir(path), "alias.log")); err != nil {
			t.Fatalf("create hard link: %v", err)
		}
		if _, err := Open(path, identity); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Open error = %v, want ErrUnsafePath", err)
		}
	})
	t.Run("special permission bit", func(t *testing.T) {
		path := newLogPath(t)
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("create source file: %v", err)
		}
		if err := syscall.Chmod(path, 0o4600); err != nil {
			t.Fatalf("set special permission bit: %v", err)
		}
		if _, err := Open(path, identity); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Open error = %v, want ErrUnsafePath", err)
		}
	})
}
