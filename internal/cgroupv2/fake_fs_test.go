package cgroupv2

import (
	"errors"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"mydocker/internal/domain"
)

type fakeWrite struct {
	Path string
	Data string
}

// resolveTestLimits converts raw Sandbox policy to the immutable Container limits required by cgroup manager tests.
func resolveTestLimits(t *testing.T, resources domain.Resources) domain.ResolvedResourceLimits {
	t.Helper()
	limits, err := domain.ResolveResourceLimits(resources)
	if err != nil {
		t.Fatalf("ResolveResourceLimits() setup error = %v", err)
	}
	return limits
}

type fakeFileSystem struct {
	modes          map[string]fs.FileMode
	files          map[string][]byte
	failures       map[string]error
	writeOverrides map[string][]byte
	writes         []fakeWrite
	removes        []string
	nextFD         uintptr
}

// newFakeFileSystem creates an in-memory delegated root with the bounded pseudo-files used by Manager tests.
func newFakeFileSystem(root string) *fakeFileSystem {
	fake := &fakeFileSystem{
		modes:          make(map[string]fs.FileMode),
		files:          make(map[string][]byte),
		failures:       make(map[string]error),
		writeOverrides: make(map[string][]byte),
		nextFD:         70,
	}
	fake.addDirectory(root)
	return fake
}

// addDirectory installs one fake cgroup directory and its kernel-provided control and observation files.
func (f *fakeFileSystem) addDirectory(path string) {
	path = filepath.Clean(path)
	f.modes[path] = fs.ModeDir | 0o755
	f.setFile(filepath.Join(path, "cgroup.controllers"), "cpu memory pids\n")
	f.setFile(filepath.Join(path, "cgroup.subtree_control"), "\n")
	f.setFile(filepath.Join(path, "cgroup.procs"), "")
	f.setFile(filepath.Join(path, "cgroup.events"), "populated 0\nfrozen 0\n")
	f.setFile(filepath.Join(path, "cpu.max"), "max 100000\n")
	f.setFile(filepath.Join(path, "memory.max"), "max\n")
	f.setFile(filepath.Join(path, "pids.max"), "max\n")
	f.setFile(filepath.Join(path, "memory.current"), "0\n")
	f.setFile(filepath.Join(path, "pids.current"), "0\n")
	f.setFile(filepath.Join(path, "memory.events.local"), "oom 0\noom_kill 0\noom_group_kill 0\n")
}

// setFile replaces one fake pseudo-file readback without recording a manager write.
func (f *fakeFileSystem) setFile(path, value string) {
	f.files[filepath.Clean(path)] = []byte(value)
}

// setFailure injects a deterministic failure for one operation and exact path.
func (f *fakeFileSystem) setFailure(operation, path string, err error) {
	f.failures[operation+"\x00"+filepath.Clean(path)] = err
}

// failure returns the deterministic error configured for one operation and exact path.
func (f *fakeFileSystem) failure(operation, path string) error {
	return f.failures[operation+"\x00"+filepath.Clean(path)]
}

// exists reports whether the exact fake directory or pseudo-file is present.
func (f *fakeFileSystem) exists(path string) bool {
	path = filepath.Clean(path)
	if _, ok := f.modes[path]; ok {
		return true
	}
	_, ok := f.files[path]
	return ok
}

// writesTo returns the ordered manager writes targeting one pseudo-file basename.
func (f *fakeFileSystem) writesTo(name string) []fakeWrite {
	var result []fakeWrite
	for _, write := range f.writes {
		if filepath.Base(write.Path) == name {
			result = append(result, write)
		}
	}
	return result
}

// Lstat exposes exact fake metadata without following simulated symlinks.
func (f *fakeFileSystem) Lstat(path string) (fs.FileInfo, error) {
	path = filepath.Clean(path)
	if err := f.failure("lstat", path); err != nil {
		return nil, err
	}
	if mode, ok := f.modes[path]; ok {
		return fakeFileInfo{name: filepath.Base(path), mode: mode}, nil
	}
	if data, ok := f.files[path]; ok {
		return fakeFileInfo{name: filepath.Base(path), mode: 0o644, size: int64(len(data))}, nil
	}
	return nil, fs.ErrNotExist
}

// Mkdir creates one exact fake cgroup and seeds only its kernel-like pseudo-files.
func (f *fakeFileSystem) Mkdir(path string, _ fs.FileMode) error {
	path = filepath.Clean(path)
	if err := f.failure("mkdir", path); err != nil {
		return err
	}
	if f.exists(path) {
		return fs.ErrExist
	}
	parentMode, ok := f.modes[filepath.Dir(path)]
	if !ok || !parentMode.IsDir() {
		return fs.ErrNotExist
	}
	f.addDirectory(path)
	return nil
}

// ReadFile returns a copy of one fake pseudo-file or its injected failure.
func (f *fakeFileSystem) ReadFile(path string) ([]byte, error) {
	path = filepath.Clean(path)
	if err := f.failure("read", path); err != nil {
		return nil, err
	}
	value, ok := f.files[path]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return append([]byte(nil), value...), nil
}

// WriteFile updates one existing fake pseudo-file and emulates subtree-control readback.
func (f *fakeFileSystem) WriteFile(path string, data []byte) error {
	path = filepath.Clean(path)
	if err := f.failure("write", path); err != nil {
		return err
	}
	if _, ok := f.files[path]; !ok {
		return fs.ErrNotExist
	}
	f.writes = append(f.writes, fakeWrite{Path: path, Data: string(data)})
	if override, ok := f.writeOverrides[path]; ok {
		f.files[path] = append([]byte(nil), override...)
		return nil
	}
	if filepath.Base(path) == "cgroup.subtree_control" {
		controllers := wordSet(string(data))
		words := make([]string, 0, len(controllers))
		for controller := range controllers {
			words = append(words, controller)
		}
		sort.Strings(words)
		f.files[path] = []byte(strings.Join(words, " ") + "\n")
		return nil
	}
	f.files[path] = append([]byte(nil), data...)
	return nil
}

// ReadDir lists only immediate fake entries so cleanup tests can prove non-recursive behavior.
func (f *fakeFileSystem) ReadDir(path string) ([]fs.DirEntry, error) {
	path = filepath.Clean(path)
	if err := f.failure("readdir", path); err != nil {
		return nil, err
	}
	mode, ok := f.modes[path]
	if !ok || !mode.IsDir() {
		return nil, fs.ErrNotExist
	}
	entries := make(map[string]fakeDirEntry)
	for candidate, candidateMode := range f.modes {
		if candidate != path && filepath.Dir(candidate) == path {
			name := filepath.Base(candidate)
			entries[name] = fakeDirEntry{info: fakeFileInfo{name: name, mode: candidateMode}}
		}
	}
	for candidate, value := range f.files {
		if filepath.Dir(candidate) == path {
			name := filepath.Base(candidate)
			entries[name] = fakeDirEntry{info: fakeFileInfo{name: name, mode: 0o644, size: int64(len(value))}}
		}
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]fs.DirEntry, 0, len(names))
	for _, name := range names {
		result = append(result, entries[name])
	}
	return result, nil
}

// Remove removes one exact empty fake cgroup and records every non-recursive attempt.
func (f *fakeFileSystem) Remove(path string) error {
	path = filepath.Clean(path)
	f.removes = append(f.removes, path)
	if err := f.failure("remove", path); err != nil {
		return err
	}
	mode, ok := f.modes[path]
	if !ok || !mode.IsDir() {
		return fs.ErrNotExist
	}
	for candidate, candidateMode := range f.modes {
		if candidate != path && filepath.Dir(candidate) == path && candidateMode.IsDir() {
			return errors.New("fake directory is not empty")
		}
	}
	delete(f.modes, path)
	for candidate := range f.files {
		if filepath.Dir(candidate) == path {
			delete(f.files, candidate)
		}
	}
	return nil
}

// OpenDir returns a runtime-only fake descriptor for an exact directory.
func (f *fakeFileSystem) OpenDir(path string) (DirectoryHandle, error) {
	path = filepath.Clean(path)
	if err := f.failure("open", path); err != nil {
		return nil, err
	}
	mode, ok := f.modes[path]
	if !ok || !mode.IsDir() {
		return nil, fs.ErrNotExist
	}
	f.nextFD++
	return &fakeDirectoryHandle{fd: f.nextFD}, nil
}

type fakeFileInfo struct {
	name string
	mode fs.FileMode
	size int64
}

// Name returns the final component used by fake directory listings.
func (i fakeFileInfo) Name() string { return i.name }

// Size returns the fake pseudo-file byte length and zero for directories.
func (i fakeFileInfo) Size() int64 { return i.size }

// Mode returns the exact simulated file mode used for symlink and directory safety checks.
func (i fakeFileInfo) Mode() fs.FileMode { return i.mode }

// ModTime returns a stable zero timestamp because fake cgroup metadata has no time semantics.
func (i fakeFileInfo) ModTime() time.Time { return time.Time{} }

// IsDir reports whether the simulated mode identifies a cgroup directory.
func (i fakeFileInfo) IsDir() bool { return i.mode.IsDir() }

// Sys returns no host metadata because tests must remain independent of a real cgroup filesystem.
func (i fakeFileInfo) Sys() any { return nil }

type fakeDirEntry struct {
	info fakeFileInfo
}

// Name returns the immediate fake entry name.
func (e fakeDirEntry) Name() string { return e.info.Name() }

// IsDir reports whether the immediate fake entry is a child cgroup.
func (e fakeDirEntry) IsDir() bool { return e.info.IsDir() }

// Type returns the simulated entry type bits without consulting a host filesystem.
func (e fakeDirEntry) Type() fs.FileMode { return e.info.Mode().Type() }

// Info returns deterministic fake metadata for cleanup inspection.
func (e fakeDirEntry) Info() (fs.FileInfo, error) { return e.info, nil }

type fakeDirectoryHandle struct {
	fd     uintptr
	closed bool
}

// Fd exposes the fake runtime-only descriptor number used by open tests.
func (h *fakeDirectoryHandle) Fd() uintptr { return h.fd }

// Close marks the fake descriptor closed and remains idempotent for test convenience.
func (h *fakeDirectoryHandle) Close() error {
	h.closed = true
	return nil
}

type fakeHostProbe struct {
	supported bool
	err       error
	pageSize  uint64
	pageErr   error
}

// IsCgroupV2 returns the configured fake host result without touching a real mount.
func (p fakeHostProbe) IsCgroupV2(string) (bool, error) {
	return p.supported, p.err
}

// PageSize returns a configurable host page size and defaults to the common 4096-byte test contract.
func (p fakeHostProbe) PageSize() (uint64, error) {
	if p.pageErr != nil {
		return 0, p.pageErr
	}
	if p.pageSize == 0 {
		return 4_096, nil
	}
	return p.pageSize, nil
}

// newFakeManager creates a validated Manager and delegated in-memory v2 root for unprivileged tests.
func newFakeManager(t *testing.T) (*Manager, *fakeFileSystem) {
	t.Helper()
	const root = "/delegated/mydocker"
	fake := newFakeFileSystem(root)
	manager, err := NewManager(Config{Root: root}, fake, fakeHostProbe{supported: true})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	return manager, fake
}
