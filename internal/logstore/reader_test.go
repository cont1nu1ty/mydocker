package logstore

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

var (
	errInjectedDirectorySync  = errors.New("injected workload log parent sync failure")
	errInjectedDirectoryClose = errors.New("injected workload log parent close failure")
)

type trackingPrimitives struct {
	base         FilePrimitives
	file         *trackingFile
	directory    *trackingDirectory
	failFileSync bool
	failDirSync  bool
	failDirClose bool
}

// Lstat delegates path inspection while the fixture observes only parent-directory synchronization.
func (primitives *trackingPrimitives) Lstat(name string) (os.FileInfo, error) {
	return primitives.base.Lstat(name)
}

// OpenFile delegates no-follow file creation and opening without changing log data behavior.
func (primitives *trackingPrimitives) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	file, err := primitives.base.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	tracked := &trackingFile{File: file, failSync: primitives.failFileSync}
	primitives.file = tracked
	return tracked, nil
}

// OpenDirectory wraps the production directory descriptor so the fixture can count or reject Sync.
func (primitives *trackingPrimitives) OpenDirectory(name string) (Directory, error) {
	directory, err := primitives.base.OpenDirectory(name)
	if err != nil {
		return nil, err
	}
	tracked := &trackingDirectory{Directory: directory, failSync: primitives.failDirSync, failClose: primitives.failDirClose}
	primitives.directory = tracked
	return tracked, nil
}

type trackingFile struct {
	File
	syncs    int
	failSync bool
}

// Sync records the empty-file durability barrier and can inject its failure before directory publication.
func (file *trackingFile) Sync() error {
	file.syncs++
	if file.failSync {
		return errInjectedSync
	}
	return file.File.Sync()
}

type trackingDirectory struct {
	Directory
	syncs     int
	failSync  bool
	failClose bool
}

type controlledSyncPrimitives struct {
	base    FilePrimitives
	entered chan struct{}
	release chan struct{}
	fail    bool
}

// Lstat delegates secure path inspection while the fixture controls only append synchronization.
func (primitives *controlledSyncPrimitives) Lstat(name string) (os.FileInfo, error) {
	return primitives.base.Lstat(name)
}

// OpenFile wraps the writer descriptor with one controllable Sync boundary.
func (primitives *controlledSyncPrimitives) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	file, err := primitives.base.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &controlledSyncFile{File: file, entered: primitives.entered, release: primitives.release, fail: primitives.fail}, nil
}

// OpenDirectory delegates the unrelated log-entry publication barrier.
func (primitives *controlledSyncPrimitives) OpenDirectory(name string) (Directory, error) {
	return primitives.base.OpenDirectory(name)
}

type controlledSyncFile struct {
	File
	entered   chan struct{}
	release   chan struct{}
	fail      bool
	once      sync.Once
	mu        sync.Mutex
	failed    bool
	syncCalls int
}

// Sync blocks the first append at its commit point and then succeeds or fails deterministically.
func (file *controlledSyncFile) Sync() error {
	file.mu.Lock()
	file.syncCalls++
	syncCall := file.syncCalls
	file.mu.Unlock()
	if syncCall == 1 {
		return file.File.Sync()
	}
	file.once.Do(func() { close(file.entered) })
	<-file.release
	file.mu.Lock()
	failNow := file.fail && !file.failed
	file.failed = file.failed || failNow
	file.mu.Unlock()
	if failNow {
		return errInjectedSync
	}
	return file.File.Sync()
}

// Sync records the publication barrier and can inject an uncertain directory-entry result.
func (directory *trackingDirectory) Sync() error {
	directory.syncs++
	if directory.failSync {
		return errInjectedDirectorySync
	}
	return directory.Directory.Sync()
}

// Close releases the real directory descriptor and can report a deterministic close failure to the caller.
func (directory *trackingDirectory) Close() error {
	closeErr := directory.Directory.Close()
	if directory.failClose {
		return errors.Join(errInjectedDirectoryClose, closeErr)
	}
	return closeErr
}

// TestOpenSynchronizesParentDirectory verifies a new log entry is not reported open before its private parent is synchronized.
func TestOpenSynchronizesParentDirectory(t *testing.T) {
	path := newLogPath(t)
	primitives := &trackingPrimitives{base: osFilePrimitives{}}
	store := openTestStore(t, path, testIdentity(), WithFilePrimitives(primitives))
	defer closeTestStore(t, store)
	if primitives.directory == nil || primitives.directory.syncs != 1 {
		t.Fatalf("parent directory sync count = %#v, want one", primitives.directory)
	}
	if primitives.file == nil || primitives.file.syncs != 1 {
		t.Fatalf("empty log file sync count = %#v, want one", primitives.file)
	}
}

// TestOpenPropagatesEmptyFileSyncFailure verifies an unsynchronized empty inode is never reported as a successfully opened log.
func TestOpenPropagatesEmptyFileSyncFailure(t *testing.T) {
	path := newLogPath(t)
	primitives := &trackingPrimitives{base: osFilePrimitives{}, failFileSync: true}
	store, err := Open(path, testIdentity(), WithFilePrimitives(primitives))
	if store != nil || !errors.Is(err, errInjectedSync) {
		if store != nil {
			_ = store.Close()
		}
		t.Fatalf("Open() = (%#v, %v), want empty-file sync failure", store, err)
	}
	if primitives.directory != nil {
		t.Fatalf("parent directory was opened after empty-file sync failed: %#v", primitives.directory)
	}
	store = openTestStore(t, path, testIdentity())
	closeTestStore(t, store)
}

// TestOpenPropagatesParentCloseFailure verifies the directory descriptor close result is part of the publication contract.
func TestOpenPropagatesParentCloseFailure(t *testing.T) {
	path := newLogPath(t)
	primitives := &trackingPrimitives{base: osFilePrimitives{}, failDirClose: true}
	store, err := Open(path, testIdentity(), WithFilePrimitives(primitives))
	if store != nil || !errors.Is(err, errInjectedDirectoryClose) {
		if store != nil {
			_ = store.Close()
		}
		t.Fatalf("Open() = (%#v, %v), want parent close failure", store, err)
	}
}

// TestOpenRetriesUncertainParentDirectorySync verifies an existing entry is synchronized again after its first publication barrier failed.
func TestOpenRetriesUncertainParentDirectorySync(t *testing.T) {
	path := newLogPath(t)
	failing := &trackingPrimitives{base: osFilePrimitives{}, failDirSync: true}
	if store, err := Open(path, testIdentity(), WithFilePrimitives(failing)); store != nil || !errors.Is(err, errInjectedDirectorySync) {
		if store != nil {
			_ = store.Close()
		}
		t.Fatalf("Open() = (%#v, %v), want directory sync failure", store, err)
	}
	retry := &trackingPrimitives{base: osFilePrimitives{}}
	store := openTestStore(t, path, testIdentity(), WithFilePrimitives(retry))
	defer closeTestStore(t, store)
	if retry.directory == nil || retry.directory.syncs != 1 {
		t.Fatalf("retry parent directory sync count = %#v, want one", retry.directory)
	}
}

// TestReaderReopensWhileWriterOwnsExclusiveLock verifies daemon snapshots can follow synchronized appends without acquiring or leaking the shim lock descriptor.
func TestReaderReopensWhileWriterOwnsExclusiveLock(t *testing.T) {
	path := newLogPath(t)
	identity := testIdentity()
	writer := openTestStore(t, path, identity)
	defer closeTestStore(t, writer)
	if _, err := writer.Append(StreamStdout, []byte("first")); err != nil {
		t.Fatalf("append first frame: %v", err)
	}
	reader, err := OpenReader(path, identity)
	if err != nil {
		t.Fatalf("OpenReader() while writer lock held: %v", err)
	}
	first, err := reader.Read(0, 10)
	if err != nil || len(first) != 1 || string(first[0].Payload) != "first" {
		t.Fatalf("first reader snapshot = (%+v, %v)", first, err)
	}
	if _, err := writer.Append(StreamStderr, []byte("second")); err != nil {
		t.Fatalf("append second frame: %v", err)
	}
	second, err := reader.Read(first[0].Cursor, 10)
	if err != nil || len(second) != 1 || string(second[0].Payload) != "second" {
		t.Fatalf("reopened reader snapshot = (%+v, %v)", second, err)
	}
	second[0].Payload[0] = 'X'
	again, err := reader.Read(first[0].Cursor, 10)
	if err != nil || string(again[0].Payload) != "second" {
		t.Fatalf("reader returned aliased payload = (%+v, %v)", again, err)
	}
}

// TestReaderIgnoresIncompleteTailButRejectsCompleteCorruption verifies read-only snapshots never mutate torn output and never accept a corrupt complete frame.
func TestReaderIgnoresIncompleteTailButRejectsCompleteCorruption(t *testing.T) {
	identity := testIdentity()
	path := newLogPath(t)
	writer := openTestStore(t, path, identity)
	if _, err := writer.Append(StreamStdout, []byte("durable")); err != nil {
		t.Fatalf("append durable frame: %v", err)
	}
	closeTestStore(t, writer)
	before := fileSize(t, path)
	appendRaw(t, path, []byte{0, 1, 2, 3})
	reader, err := OpenReader(path, identity)
	if err != nil {
		t.Fatalf("OpenReader() torn tail: %v", err)
	}
	frames, err := reader.Read(0, 10)
	if err != nil || len(frames) != 1 || string(frames[0].Payload) != "durable" {
		t.Fatalf("torn-tail snapshot = (%+v, %v)", frames, err)
	}
	if size := fileSize(t, path); size != before+4 {
		t.Fatalf("read-only snapshot changed torn file size to %d", size)
	}

	corruptPath := createTwoFrameLog(t, identity)
	mutateAt(t, corruptPath, framePrefixBytes+36, func(payload []byte) { payload[0] ^= 0xff })
	if _, err := OpenReader(corruptPath, identity); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("OpenReader() corruption error = %v, want ErrCorrupt", err)
	}
}

// TestReaderRejectsIdentityAndPathSubstitution verifies a read-only source cannot be rebound to another Attempt or redirected through a final symlink.
func TestReaderRejectsIdentityAndPathSubstitution(t *testing.T) {
	path := newLogPath(t)
	writer := openTestStore(t, path, testIdentity())
	if _, err := writer.Append(StreamStdout, []byte("owned")); err != nil {
		t.Fatalf("append owned frame: %v", err)
	}
	closeTestStore(t, writer)
	other := Identity{ContainerID: "container-other", AttemptID: "attempt-other"}
	if _, err := OpenReader(path, other); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("OpenReader() identity error = %v, want ErrIdentityMismatch", err)
	}
	link := filepath.Join(filepath.Dir(path), "redirected.log")
	if err := os.Symlink(path, link); err != nil {
		t.Fatalf("create reader symlink: %v", err)
	}
	if _, err := OpenReader(link, testIdentity()); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("OpenReader() path error = %v, want ErrUnsafePath", err)
	}
}

// TestReaderWaitsForCommittedAppendBoundary verifies a complete write is invisible before Sync and remains unavailable after Sync failure until writer reopen.
func TestReaderWaitsForCommittedAppendBoundary(t *testing.T) {
	for _, testCase := range []struct {
		name string
		fail bool
	}{
		{name: "successful sync"},
		{name: "failed sync", fail: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			path := newLogPath(t)
			identity := testIdentity()
			primitives := &controlledSyncPrimitives{
				base: osFilePrimitives{}, entered: make(chan struct{}), release: make(chan struct{}), fail: testCase.fail,
			}
			writer := openTestStore(t, path, identity, WithFilePrimitives(primitives))
			reader, err := OpenReader(path, identity)
			if err != nil {
				t.Fatalf("OpenReader() before append: %v", err)
			}
			appendResult := make(chan error, 1)
			go func() {
				_, err := writer.Append(StreamStdout, []byte("commit controlled"))
				appendResult <- err
			}()
			<-primitives.entered
			if frames, err := reader.Read(0, 10); len(frames) != 0 || !errors.Is(err, ErrReadUnavailable) {
				t.Fatalf("Read() during Sync = (%+v, %v), want unavailable with no frames", frames, err)
			}
			close(primitives.release)
			appendErr := <-appendResult
			if testCase.fail {
				if !errors.Is(appendErr, ErrAppendUnavailable) {
					t.Fatalf("Append() failed Sync error = %v, want ErrAppendUnavailable", appendErr)
				}
				if frames, err := reader.Read(0, 10); len(frames) != 0 || !errors.Is(err, ErrReadUnavailable) {
					t.Fatalf("Read() after failed Sync = (%+v, %v), want unavailable with no frames", frames, err)
				}
			} else {
				if appendErr != nil {
					t.Fatalf("Append() successful Sync error = %v", appendErr)
				}
				frames, err := reader.Read(0, 10)
				if err != nil || len(frames) != 1 || string(frames[0].Payload) != "commit controlled" {
					t.Fatalf("Read() after successful Sync = (%+v, %v)", frames, err)
				}
			}
			closeTestStore(t, writer)
			if testCase.fail {
				frames, err := reader.Read(0, 10)
				if err != nil || len(frames) != 0 {
					t.Fatalf("Read() after failed writer closed = (%+v, %v), want no uncommitted frames", frames, err)
				}
			}
		})
	}
}

// TestBoundedPageScanDoesNotRetainHistory verifies writer state has no frame slice and a full validation retains only the requested suffix page.
func TestBoundedPageScanDoesNotRetainHistory(t *testing.T) {
	path := newLogPath(t)
	identity := testIdentity()
	writer := openTestStore(t, path, identity)
	const frameCount = 128
	for index := 0; index < frameCount; index++ {
		if _, err := writer.Append(StreamStdout, []byte("bounded-history-payload")); err != nil {
			t.Fatalf("append history frame %d: %v", index, err)
		}
	}
	if _, exists := reflect.TypeOf(writer).Elem().FieldByName("frames"); exists {
		t.Fatal("Store retains a resident frame history field")
	}
	page, err := writer.Read(frameCount-5, 3)
	if err != nil || len(page) != 3 || page[0].Cursor != frameCount-4 || page[2].Cursor != frameCount-2 {
		t.Fatalf("Store.Read() bounded page = (%+v, %v)", page, err)
	}
	closeTestStore(t, writer)
	reader, err := OpenReader(path, identity)
	if err != nil {
		t.Fatalf("OpenReader() history: %v", err)
	}
	page, err = reader.Read(frameCount-5, 2)
	if err != nil || len(page) != 2 || page[0].Cursor != frameCount-4 || page[1].Cursor != frameCount-3 {
		t.Fatalf("Reader.Read() bounded page = (%+v, %v)", page, err)
	}
}
