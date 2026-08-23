package logstore

import (
	"errors"
	"os"
	"sync"
	"testing"
)

var (
	errInjectedWrite = errors.New("injected workload log write failure")
	errInjectedSync  = errors.New("injected workload log sync failure")
)

type faultPrimitives struct {
	base       FilePrimitives
	writeLimit int
	failSyncAt int
}

// Lstat delegates path safety checks so fault tests alter only append file primitives.
func (primitives *faultPrimitives) Lstat(name string) (os.FileInfo, error) {
	return primitives.base.Lstat(name)
}

// OpenFile wraps a production no-follow file with one deterministic write or sync failure.
func (primitives *faultPrimitives) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	file, err := primitives.base.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &faultFile{File: file, writeLimit: primitives.writeLimit, failSyncAt: primitives.failSyncAt}, nil
}

// OpenDirectory delegates directory synchronization so append faults remain isolated to the log descriptor.
func (primitives *faultPrimitives) OpenDirectory(name string) (Directory, error) {
	return primitives.base.OpenDirectory(name)
}

type faultFile struct {
	File
	mu         sync.Mutex
	writeLimit int
	failSyncAt int
	syncCalls  int
}

// Write emits a bounded prefix once and then reports an injected torn-append failure.
func (file *faultFile) Write(payload []byte) (int, error) {
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.writeLimit < 0 {
		return file.File.Write(payload)
	}
	limit := file.writeLimit
	file.writeLimit = -1
	if limit > len(payload) {
		limit = len(payload)
	}
	written, err := file.File.Write(payload[:limit])
	if err != nil {
		return written, err
	}
	return written, errInjectedWrite
}

// Sync fails once after a complete write so callers must treat append completion as unknown and reopen.
func (file *faultFile) Sync() error {
	file.mu.Lock()
	defer file.mu.Unlock()
	file.syncCalls++
	if file.failSyncAt > 0 && file.syncCalls == file.failSyncAt {
		return errInjectedSync
	}
	return file.File.Sync()
}

// TestWriteFailurePoisonsAppendUntilReopen verifies a torn frame is never indexed and is removed during recovery.
func TestWriteFailurePoisonsAppendUntilReopen(t *testing.T) {
	path := newLogPath(t)
	primitives := &faultPrimitives{base: osFilePrimitives{}, writeLimit: 5}
	store := openTestStore(t, path, testIdentity(), WithFilePrimitives(primitives))
	if _, err := store.Append(StreamStdout, []byte("torn")); !errors.Is(err, ErrAppendUnavailable) || !errors.Is(store.appendPoisoned, errInjectedWrite) {
		t.Fatalf("Append error = %v, poison = %v; want injected unavailable", err, store.appendPoisoned)
	}
	if _, err := store.Append(StreamStdout, []byte("retry")); !errors.Is(err, ErrAppendUnavailable) {
		t.Fatalf("second Append error = %v, want ErrAppendUnavailable", err)
	}
	frames, err := store.Read(0, 10)
	if err != nil || len(frames) != 0 {
		t.Fatalf("unconfirmed torn frame became readable: %+v, %v", frames, err)
	}
	closeTestStore(t, store)
	reopened := openTestStore(t, path, testIdentity())
	defer closeTestStore(t, reopened)
	if size := fileSize(t, path); size != 0 {
		t.Fatalf("reopen retained %d torn bytes", size)
	}
}

// TestSyncFailureDoesNotConfirmAppend verifies a prepared frame without a synchronized commit marker remains invisible and is removed during recovery.
func TestSyncFailureDoesNotConfirmAppend(t *testing.T) {
	path := newLogPath(t)
	primitives := &faultPrimitives{base: osFilePrimitives{}, writeLimit: -1, failSyncAt: 2}
	store := openTestStore(t, path, testIdentity(), WithFilePrimitives(primitives))
	if _, err := store.Append(StreamStderr, []byte("completion unknown")); !errors.Is(err, ErrAppendUnavailable) || !errors.Is(store.appendPoisoned, errInjectedSync) {
		t.Fatalf("Append error = %v, poison = %v; want injected sync failure", err, store.appendPoisoned)
	}
	frames, err := store.Read(0, 10)
	if err != nil || len(frames) != 0 {
		t.Fatalf("sync-failed frame was confirmed in memory: %+v, %v", frames, err)
	}
	closeTestStore(t, store)
	reopened := openTestStore(t, path, testIdentity())
	defer closeTestStore(t, reopened)
	recovered, err := reopened.Read(0, 10)
	if err != nil || len(recovered) != 0 {
		t.Fatalf("reopen exposed a frame whose prepared-data Sync failed: %+v, %v", recovered, err)
	}
}

// TestCommitMarkerSyncFailureStaysInvisible verifies failure of the publication barrier poisons append and Close durably removes the unconfirmed complete marker.
func TestCommitMarkerSyncFailureStaysInvisible(t *testing.T) {
	path := newLogPath(t)
	primitives := &faultPrimitives{base: osFilePrimitives{}, writeLimit: -1, failSyncAt: 3}
	store := openTestStore(t, path, testIdentity(), WithFilePrimitives(primitives))
	if _, err := store.Append(StreamStdout, []byte("commit marker uncertain")); !errors.Is(err, ErrAppendUnavailable) || !errors.Is(store.appendPoisoned, errInjectedSync) {
		t.Fatalf("Append error = %v, poison = %v; want commit Sync unavailable", err, store.appendPoisoned)
	}
	frames, err := store.Read(0, 10)
	if err != nil || len(frames) != 0 {
		t.Fatalf("commit-sync-failed frame became readable: %+v, %v", frames, err)
	}
	closeTestStore(t, store)
	reader, err := OpenReader(path, testIdentity())
	if err != nil {
		t.Fatalf("OpenReader() after poisoned close: %v", err)
	}
	frames, err = reader.Read(0, 10)
	if err != nil || len(frames) != 0 {
		t.Fatalf("reader exposed commit-sync-failed frame after close: %+v, %v", frames, err)
	}
}
