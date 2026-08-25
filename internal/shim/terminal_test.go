package shim

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"mydocker/internal/domain"
)

// TestFileTerminalStoreAtomicRoundTrip verifies immutable publication, exact
// replay confirmation, and rejection of a different valid terminal record.
func TestFileTerminalStoreAtomicRoundTrip(t *testing.T) {
	directory := privateTempDir(t)
	path := filepath.Join(directory, "terminal.json")
	store, err := NewFileTerminalStore(path)
	if err != nil {
		t.Fatal(err)
	}
	record := startFailureRecord(t)
	if err := store.Commit(record); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := store.Load()
	if err != nil || !found {
		t.Fatalf("found=%v error=%v", found, err)
	}
	if !reflect.DeepEqual(loaded, record) {
		t.Fatalf("loaded record differs:\n got %+v\nwant %+v", loaded, record)
	}
	if err := store.Commit(record); err != nil {
		t.Fatalf("exact commit replay error=%v", err)
	}
	different := startFailureRecordWithDiagnostic(t, "different terminal fact")
	if err := store.Commit(different); !errors.Is(err, ErrTerminalExists) {
		t.Fatalf("different commit error=%v, want ErrTerminalExists", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("terminal mode=%o, want 600", info.Mode().Perm())
	}
}

// TestSameTerminalRecordRequiresCanonicalPayload verifies checksum equality
// alone cannot make different record content eligible for recovery success.
func TestSameTerminalRecordRequiresCanonicalPayload(t *testing.T) {
	left := startFailureRecord(t)
	right := left.Clone()
	right.Diagnostic = "different content with copied checksum"
	equal, err := sameTerminalRecord(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if equal {
		t.Fatal("different canonical terminal records compared equal")
	}
}

// TestLaunchAbortedTerminalRequiresUnknownOutcome verifies the durable reason
// distinguishes an executed workload from both pre-exec failure and known exit.
func TestLaunchAbortedTerminalRequiresUnknownOutcome(t *testing.T) {
	spec := testInitSpec(t, "op-launch-aborted", "container-launch-aborted", "attempt-launch-aborted")
	record, err := NewTerminalRecord(
		spec, TerminalLaunchAborted, domain.UnknownOutcome(domain.EvidenceUnknown), nil,
		"pidfd publication failed after exec; process tree reaped", testTime(),
	)
	if err != nil {
		t.Fatalf("NewTerminalRecord() error = %v", err)
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("record.Validate() error = %v", err)
	}
	record.Outcome = domain.NotApplicableOutcome()
	checksum, err := terminalChecksum(record)
	if err != nil {
		t.Fatal(err)
	}
	record.ChecksumSHA256 = checksum
	if err := record.Validate(); err == nil {
		t.Fatal("launch-aborted record accepted a not-applicable outcome")
	}
}

// TestFileTerminalStoreSyncOrder verifies file sync precedes rename and directory sync follows publication.
func TestFileTerminalStoreSyncOrder(t *testing.T) {
	directory := privateTempDir(t)
	filesystem := &recordingTerminalFS{base: osTerminalFS{}}
	store, err := newFileTerminalStore(filepath.Join(directory, "terminal.json"), filesystem)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(startFailureRecord(t)); err != nil {
		t.Fatal(err)
	}
	events := filesystem.Events()
	assertOrderedEvents(t, events, "create_temp", "write", "file_sync", "rename", "open_directory", "directory_sync")
}

// TestDirectorySyncFailureLeavesRecoverableRecord verifies ambiguous durability
// fails closed, then an exact retry re-syncs the directory without rewriting.
func TestDirectorySyncFailureLeavesRecoverableRecord(t *testing.T) {
	directory := privateTempDir(t)
	filesystem := &recordingTerminalFS{base: osTerminalFS{}, failDirectorySync: errors.New("injected directory sync failure")}
	store, err := newFileTerminalStore(filepath.Join(directory, "terminal.json"), filesystem)
	if err != nil {
		t.Fatal(err)
	}
	record := startFailureRecord(t)
	if err := store.Commit(record); err == nil {
		t.Fatal("commit unexpectedly succeeded")
	}
	loaded, found, err := store.Load()
	if err != nil || !found || !reflect.DeepEqual(loaded, record) {
		t.Fatalf("renamed record was not recoverable: found=%v error=%v record=%+v", found, err, loaded)
	}
	filesystem.failDirectorySync = nil
	filesystem.ResetEvents()
	if err := store.Commit(record); err != nil {
		t.Fatalf("exact recovery commit error=%v", err)
	}
	if events := filesystem.Events(); !reflect.DeepEqual(events, []string{"open_directory", "directory_sync", "directory_close"}) {
		t.Fatalf("exact recovery events=%v, want directory-only re-sync", events)
	}
}

// TestDirectoryCloseFailureIsReported verifies a post-sync descriptor cleanup
// failure is not hidden behind an otherwise durable terminal publication.
func TestDirectoryCloseFailureIsReported(t *testing.T) {
	directory := privateTempDir(t)
	injected := errors.New("injected directory close failure")
	filesystem := &recordingTerminalFS{base: osTerminalFS{}, failDirectoryClose: injected}
	store, err := newFileTerminalStore(filepath.Join(directory, "terminal.json"), filesystem)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(startFailureRecord(t)); !errors.Is(err, injected) {
		t.Fatalf("Commit() error=%v, want injected close failure", err)
	}
	loaded, found, err := store.Load()
	if err != nil || !found {
		t.Fatalf("published record after close failure: found=%v error=%v record=%+v", found, err, loaded)
	}
	filesystem.ResetEvents()
	if err := store.Commit(startFailureRecord(t)); !errors.Is(err, injected) {
		t.Fatalf("exact recovery Commit() error=%v, want injected close failure", err)
	}
	if events := filesystem.Events(); !reflect.DeepEqual(events, []string{"open_directory", "directory_sync", "directory_close"}) {
		t.Fatalf("exact recovery events=%v, want directory-only re-sync", events)
	}
	filesystem.failDirectoryClose = nil
	if err := store.Commit(startFailureRecord(t)); err != nil {
		t.Fatalf("exact recovery after close fault error=%v", err)
	}
}

// TestFileTerminalStoreRejectsTamperedChecksum verifies complete but modified JSON is never trusted.
func TestFileTerminalStoreRejectsTamperedChecksum(t *testing.T) {
	directory := privateTempDir(t)
	path := filepath.Join(directory, "terminal.json")
	store, err := NewFileTerminalStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(startFailureRecord(t)); err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record TerminalRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		t.Fatal(err)
	}
	record.Diagnostic = "tampered but otherwise valid"
	payload, err = json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load(); !errors.Is(err, ErrTerminalCorrupt) {
		t.Fatalf("load error=%v, want ErrTerminalCorrupt", err)
	}
}

// TestFileTerminalStoreRejectsSymlink verifies terminal artifacts cannot redirect publication or recovery.
func TestFileTerminalStoreRejectsSymlink(t *testing.T) {
	directory := privateTempDir(t)
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(target, []byte("not terminal"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "terminal.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	store, err := NewFileTerminalStore(link)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load(); !errors.Is(err, ErrUnsafeArtifact) {
		t.Fatalf("symlink load error=%v, want ErrUnsafeArtifact", err)
	}
}

// recordingTerminalFS wraps real safe primitives and records the crash-consistency operation order.
type recordingTerminalFS struct {
	base               osTerminalFS
	mu                 sync.Mutex
	events             []string
	failDirectorySync  error
	failDirectoryClose error
}

// record appends one atomic publication event for deterministic ordering assertions.
func (filesystem *recordingTerminalFS) record(event string) {
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	filesystem.events = append(filesystem.events, event)
}

// Events returns an independent copy of recorded filesystem operations.
func (filesystem *recordingTerminalFS) Events() []string {
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	return append([]string(nil), filesystem.events...)
}

// ResetEvents clears prior publication events so a recovery attempt can be
// checked independently for accidental file replacement or temporary writes.
func (filesystem *recordingTerminalFS) ResetEvents() {
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	filesystem.events = nil
}

// Lstat delegates non-following artifact inspection to production primitives.
func (filesystem *recordingTerminalFS) Lstat(path string) (os.FileInfo, error) {
	return filesystem.base.Lstat(path)
}

// ReadFile delegates bounded swap-checked terminal reads to production primitives.
func (filesystem *recordingTerminalFS) ReadFile(path string, limit int64) ([]byte, error) {
	return filesystem.base.ReadFile(path, limit)
}

// CreateTemp records creation and wraps the temporary file's write/sync operations.
func (filesystem *recordingTerminalFS) CreateTemp(directory, pattern string) (syncFile, string, error) {
	filesystem.record("create_temp")
	file, path, err := filesystem.base.CreateTemp(directory, pattern)
	if err != nil {
		return nil, "", err
	}
	return &recordingSyncFile{SyncFile: file, filesystem: filesystem, kind: "file"}, path, nil
}

// RenameNoReplace records and delegates atomic no-replace publication.
func (filesystem *recordingTerminalFS) RenameNoReplace(oldPath, newPath string) error {
	filesystem.record("rename")
	return filesystem.base.RenameNoReplace(oldPath, newPath)
}

// OpenDirectory records the durability boundary and wraps its Sync failure injection.
func (filesystem *recordingTerminalFS) OpenDirectory(path string) (syncFile, error) {
	filesystem.record("open_directory")
	file, err := filesystem.base.OpenDirectory(path)
	if err != nil {
		return nil, err
	}
	return &recordingSyncFile{
		SyncFile: file, filesystem: filesystem, kind: "directory",
		syncErr: filesystem.failDirectorySync, closeErr: filesystem.failDirectoryClose,
	}, nil
}

// Remove delegates unpublished temporary cleanup.
func (filesystem *recordingTerminalFS) Remove(path string) error {
	return filesystem.base.Remove(path)
}

// recordingSyncFile labels write and sync calls while retaining the production descriptor.
type recordingSyncFile struct {
	SyncFile   syncFile
	filesystem *recordingTerminalFS
	kind       string
	syncErr    error
	closeErr   error
}

// Write records payload publication before delegating the complete write.
func (file *recordingSyncFile) Write(payload []byte) (int, error) {
	file.filesystem.record("write")
	return file.SyncFile.Write(payload)
}

// Sync records file or directory durability and exposes an optional deterministic fault.
func (file *recordingSyncFile) Sync() error {
	file.filesystem.record(file.kind + "_sync")
	if file.syncErr != nil {
		return file.syncErr
	}
	return file.SyncFile.Sync()
}

// Close records descriptor cleanup, releases the production handle, and exposes
// an injected post-sync close failure without suppressing the real close error.
func (file *recordingSyncFile) Close() error {
	file.filesystem.record(file.kind + "_close")
	return errors.Join(file.SyncFile.Close(), file.closeErr)
}

// assertOrderedEvents verifies required atomic operations appear in strict relative order.
func assertOrderedEvents(t *testing.T, events []string, expected ...string) {
	t.Helper()
	position := 0
	for _, event := range events {
		if position < len(expected) && event == expected[position] {
			position++
		}
	}
	if position != len(expected) {
		t.Fatalf("events=%v do not contain ordered sequence %v", events, expected)
	}
}

// startFailureRecord constructs a fully valid checksum-bound terminal record for storage tests.
func startFailureRecord(t *testing.T) TerminalRecord {
	return startFailureRecordWithDiagnostic(t, "injected start failure")
}

// startFailureRecordWithDiagnostic constructs a valid record whose diagnostic
// lets tests distinguish exact replay from an attempted immutable replacement.
func startFailureRecordWithDiagnostic(t *testing.T, diagnostic string) TerminalRecord {
	t.Helper()
	spec := testInitSpec(t, "op-terminal", "container-terminal", "attempt-terminal")
	record, err := NewTerminalRecord(spec, TerminalStartFailed, domain.NotApplicableOutcome(), nil, diagnostic, testTime())
	if err != nil {
		t.Fatal(err)
	}
	return record
}

// privateTempDir creates an explicitly private same-owner artifact directory independent of test-runner umask behavior.
func privateTempDir(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}

// Ensure recordingSyncFile retains the write contract needed for file and directory handles.
var _ io.Writer = (*recordingSyncFile)(nil)
