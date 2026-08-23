package logstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"mydocker/internal/domain"
)

// TestAppendReadAndReopen verifies durable mixed-stream ordering, pagination, deep copies, and recovery indexing.
func TestAppendReadAndReopen(t *testing.T) {
	path := newLogPath(t)
	identity := testIdentity()
	store := openTestStore(t, path, identity)
	payload := []byte("first stdout")
	first, err := store.Append(StreamStdout, payload)
	if err != nil {
		t.Fatalf("append first stdout: %v", err)
	}
	payload[0] = 'X'
	second, err := store.Append(StreamStderr, []byte("first stderr"))
	if err != nil {
		t.Fatalf("append first stderr: %v", err)
	}
	third, err := store.Append(StreamStdout, []byte("second stdout"))
	if err != nil {
		t.Fatalf("append second stdout: %v", err)
	}
	if first.Cursor != 1 || first.Sequence != 1 || second.Cursor != 2 || second.Sequence != 1 || third.Cursor != 3 || third.Sequence != 2 {
		t.Fatalf("unexpected cursor/sequence assignment: first=%+v second=%+v third=%+v", first, second, third)
	}
	first.Payload[0] = 'Z'
	page, err := store.Read(1, 1)
	if err != nil {
		t.Fatalf("read page: %v", err)
	}
	if len(page) != 1 || page[0].Cursor != 2 || string(page[0].Payload) != "first stderr" {
		t.Fatalf("unexpected page: %+v", page)
	}
	zero, err := store.Read(0, 0)
	if err != nil || len(zero) != 3 {
		t.Fatalf("zero-limit read must return all remaining frames, got %#v, %v", zero, err)
	}
	if _, err := store.Read(0, -1); !errors.Is(err, ErrInvalidLimit) {
		t.Fatalf("negative limit must fail with ErrInvalidLimit, got %v", err)
	}
	all, err := store.Read(0, 10)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if string(all[0].Payload) != "first stdout" {
		t.Fatalf("append retained caller-owned payload alias: %q", all[0].Payload)
	}
	all[0].Payload[0] = 'Y'
	again, err := store.Read(0, 10)
	if err != nil {
		t.Fatalf("read all again: %v", err)
	}
	if string(again[0].Payload) != "first stdout" {
		t.Fatalf("read returned store-owned payload alias: %q", again[0].Payload)
	}
	if cursor, err := store.LastCursor(); err != nil || cursor != 3 {
		t.Fatalf("last cursor = %d, %v; want 3", cursor, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	reopened := openTestStore(t, path, identity)
	defer closeTestStore(t, reopened)
	recovered, err := reopened.Read(0, 10)
	if err != nil {
		t.Fatalf("read recovered frames: %v", err)
	}
	if len(recovered) != 3 || recovered[2].Sequence != 2 || string(recovered[2].Payload) != "second stdout" {
		t.Fatalf("unexpected recovered frames: %+v", recovered)
	}
}

// TestReadAndAppendAfterClose verifies that descriptor ownership ends at Close while repeated Close remains harmless.
func TestReadAndAppendAfterClose(t *testing.T) {
	store := openTestStore(t, newLogPath(t), testIdentity())
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second close must be idempotent: %v", err)
	}
	if _, err := store.Append(StreamStdout, []byte("late")); !errors.Is(err, ErrClosed) {
		t.Fatalf("append after close must fail with ErrClosed, got %v", err)
	}
	if _, err := store.Read(0, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("read after close must fail with ErrClosed, got %v", err)
	}
	if _, err := store.LastCursor(); !errors.Is(err, ErrClosed) {
		t.Fatalf("last cursor after close must fail with ErrClosed, got %v", err)
	}
}

// TestOpenRecoversOnlyIncompleteTail verifies interrupted prepared data and an unpublished final commit marker are truncated and synchronized.
func TestOpenRecoversOnlyIncompleteTail(t *testing.T) {
	prepared, err := encodeFrame(newFrame(testIdentity(), StreamStdout, 2, 2, []byte("prepared")))
	if err != nil {
		t.Fatalf("encode prepared tail fixture: %v", err)
	}
	for _, testCase := range []struct {
		name     string
		fragment func() []byte
	}{
		{name: "partial prefix", fragment: func() []byte { return []byte{0, 0, 0, 1} }},
		{name: "partial body", fragment: func() []byte {
			fragment := make([]byte, framePrefixBytes+7)
			binary.BigEndian.PutUint64(fragment[0:8], frameFixedBytes)
			binary.BigEndian.PutUint64(fragment[8:16], ^uint64(frameFixedBytes))
			copy(fragment[framePrefixBytes:], frameMagic[:])
			return fragment
		}},
		{name: "missing commit marker", fragment: func() []byte {
			return append([]byte(nil), prepared[:len(prepared)-frameCommitBytes]...)
		}},
		{name: "partial commit marker", fragment: func() []byte {
			return append([]byte(nil), prepared[:len(prepared)-frameCommitBytes+7]...)
		}},
		{name: "full torn commit marker", fragment: func() []byte {
			fragment := append([]byte(nil), prepared...)
			fragment[len(fragment)-1] ^= 0xff
			return fragment
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			path := newLogPath(t)
			store := openTestStore(t, path, testIdentity())
			if _, err := store.Append(StreamStdout, []byte("durable")); err != nil {
				t.Fatalf("append durable frame: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("close store: %v", err)
			}
			before := fileSize(t, path)
			appendRaw(t, path, testCase.fragment())
			reopened := openTestStore(t, path, testIdentity())
			defer closeTestStore(t, reopened)
			if size := fileSize(t, path); size != before {
				t.Fatalf("recovered size = %d, want %d", size, before)
			}
			frames, err := reopened.Read(0, 10)
			if err != nil || len(frames) != 1 || string(frames[0].Payload) != "durable" {
				t.Fatalf("unexpected recovered durable prefix: %+v, %v", frames, err)
			}
		})
	}
}

// TestOpenRejectsCompleteCorruption verifies schema, checksum, identity, reserved bytes, and ordering failures never become tail recovery.
func TestOpenRejectsCompleteCorruption(t *testing.T) {
	identity := testIdentity()
	for _, testCase := range []struct {
		name   string
		offset int64
		mutate func([]byte)
		want   error
	}{
		{name: "length", offset: 0, mutate: func(payload []byte) { binary.BigEndian.PutUint64(payload, binary.BigEndian.Uint64(payload)+1) }, want: ErrCorrupt},
		{name: "schema", offset: framePrefixBytes + 4, mutate: func(payload []byte) { binary.BigEndian.PutUint32(payload, SchemaVersion+1) }, want: ErrUnsupportedSchema},
		{name: "record checksum", offset: 16, mutate: func(payload []byte) { payload[0] ^= 0xff }, want: ErrCorrupt},
		{name: "payload checksum", offset: framePrefixBytes + 36, mutate: func(payload []byte) { payload[0] ^= 0xff }, want: ErrCorrupt},
		{name: "identity", offset: framePrefixBytes + frameFixedBytes, mutate: func(payload []byte) { payload[0] = 'x' }, want: ErrCorrupt},
		{name: "stream", offset: framePrefixBytes + 12, mutate: func(payload []byte) { payload[0] = 2 }, want: ErrCorrupt},
		{name: "reserved", offset: framePrefixBytes + 13, mutate: func(payload []byte) { payload[0] = 1 }, want: ErrCorrupt},
		{name: "cursor", offset: framePrefixBytes + 16, mutate: func(payload []byte) { binary.BigEndian.PutUint64(payload, 2) }, want: ErrCorrupt},
		{name: "sequence", offset: framePrefixBytes + 24, mutate: func(payload []byte) { binary.BigEndian.PutUint64(payload, 2) }, want: ErrCorrupt},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			path := createTwoFrameLog(t, identity)
			mutateAt(t, path, testCase.offset, testCase.mutate)
			store, err := Open(path, identity)
			if store != nil {
				_ = store.Close()
				t.Fatal("Open unexpectedly accepted complete corruption")
			}
			if !errors.Is(err, testCase.want) {
				t.Fatalf("Open error = %v, want %v", err, testCase.want)
			}
		})
	}
}

// TestOpenRejectsOutOfBoundsTailLength verifies a complete malicious prefix is corruption, not an interrupted append.
func TestOpenRejectsOutOfBoundsTailLength(t *testing.T) {
	path := newLogPath(t)
	store := openTestStore(t, path, testIdentity())
	if err := store.Close(); err != nil {
		t.Fatalf("close empty store: %v", err)
	}
	prefix := make([]byte, framePrefixBytes)
	binary.BigEndian.PutUint64(prefix[0:8], maxFrameBodyBytes+1)
	binary.BigEndian.PutUint64(prefix[8:16], ^uint64(maxFrameBodyBytes+1))
	appendRaw(t, path, prefix)
	if _, err := Open(path, testIdentity()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open error = %v, want ErrCorrupt", err)
	}
	if size := fileSize(t, path); size != framePrefixBytes {
		t.Fatalf("corrupt complete prefix was truncated to %d bytes", size)
	}
}

// TestOpenRejectsDifferentAttempt verifies a valid log cannot be rebound to another Container or Attempt.
func TestOpenRejectsDifferentAttempt(t *testing.T) {
	path := newLogPath(t)
	store := openTestStore(t, path, testIdentity())
	if _, err := store.Append(StreamStdout, []byte("owned")); err != nil {
		t.Fatalf("append owned frame: %v", err)
	}
	closeTestStore(t, store)
	other := Identity{ContainerID: domain.ContainerID("container-other"), AttemptID: domain.AttemptID("attempt-other")}
	if _, err := Open(path, other); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("Open error = %v, want ErrIdentityMismatch", err)
	}
}

// TestConcurrentAppendAndRead verifies one Store serializes writers and serves race-free immutable snapshots to readers.
func TestConcurrentAppendAndRead(t *testing.T) {
	store := openTestStore(t, newLogPath(t), testIdentity())
	defer closeTestStore(t, store)
	const appendCount = 80
	var wait sync.WaitGroup
	errorsFound := make(chan error, appendCount)
	for index := 0; index < appendCount; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			stream := StreamStdout
			if index%2 == 1 {
				stream = StreamStderr
			}
			if _, err := store.Append(stream, []byte(fmt.Sprintf("frame-%03d", index))); err != nil {
				errorsFound <- err
				return
			}
			if _, err := store.Read(0, appendCount); err != nil {
				errorsFound <- err
			}
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("concurrent operation: %v", err)
	}
	frames, err := store.Read(0, appendCount+1)
	if err != nil {
		t.Fatalf("read final frames: %v", err)
	}
	if len(frames) != appendCount {
		t.Fatalf("final frame count = %d, want %d", len(frames), appendCount)
	}
	sequences := map[Stream]uint64{}
	for index, frame := range frames {
		if frame.Cursor != Cursor(index+1) {
			t.Fatalf("cursor[%d] = %d, want %d", index, frame.Cursor, index+1)
		}
		sequences[frame.Stream]++
		if frame.Sequence != sequences[frame.Stream] {
			t.Fatalf("%s sequence at cursor %d = %d, want %d", frame.Stream, frame.Cursor, frame.Sequence, sequences[frame.Stream])
		}
	}
}

// TestExclusiveOpen verifies two daemon owners cannot assign overlapping cursors to one path.
func TestExclusiveOpen(t *testing.T) {
	path := newLogPath(t)
	first := openTestStore(t, path, testIdentity())
	defer closeTestStore(t, first)
	second, err := Open(path, testIdentity())
	if second != nil {
		_ = second.Close()
		t.Fatal("second Open unexpectedly acquired the same log")
	}
	if !errors.Is(err, ErrInUse) {
		t.Fatalf("second Open error = %v, want ErrInUse", err)
	}
}

// testIdentity returns the stable Container Attempt binding used by isolated log tests.
func testIdentity() Identity {
	return Identity{ContainerID: domain.ContainerID("container-one"), AttemptID: domain.AttemptID("attempt-one")}
}

// newLogPath creates a private parent and returns a not-yet-created log path below it.
func newLogPath(t *testing.T) string {
	t.Helper()
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatalf("make test log parent private: %v", err)
	}
	return filepath.Join(parent, "attempt.log")
}

// openTestStore opens a production-default Store or terminates the scenario with context.
func openTestStore(t *testing.T, path string, identity Identity, options ...Option) *Store {
	t.Helper()
	store, err := Open(path, identity, options...)
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	return store
}

// closeTestStore closes a Store while preserving an earlier test failure location.
func closeTestStore(t *testing.T, store *Store) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Errorf("close Store: %v", err)
	}
}

// createTwoFrameLog writes two valid frames so mutation of the first is proven to be middle corruption.
func createTwoFrameLog(t *testing.T, identity Identity) string {
	t.Helper()
	path := newLogPath(t)
	store := openTestStore(t, path, identity)
	if _, err := store.Append(StreamStdout, []byte("first")); err != nil {
		t.Fatalf("append first frame: %v", err)
	}
	if _, err := store.Append(StreamStderr, []byte("second")); err != nil {
		t.Fatalf("append second frame: %v", err)
	}
	closeTestStore(t, store)
	return path
}

// appendRaw synchronously appends deliberately torn or corrupt bytes for recovery tests.
func appendRaw(t *testing.T, path string, payload []byte) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open raw append: %v", err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		t.Fatalf("append raw bytes: %v", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatalf("sync raw append: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close raw append: %v", err)
	}
}

// mutateAt changes complete frame bytes in place and synchronizes the corruption before Open.
func mutateAt(t *testing.T, path string, offset int64, mutate func([]byte)) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open mutation file: %v", err)
	}
	width := 8
	if offset == framePrefixBytes+12 || offset == framePrefixBytes+13 || offset == framePrefixBytes+36 || offset == framePrefixBytes+frameFixedBytes {
		width = 1
	}
	payload := make([]byte, width)
	if _, err := file.ReadAt(payload, offset); err != nil {
		_ = file.Close()
		t.Fatalf("read mutation bytes: %v", err)
	}
	mutate(payload)
	if _, err := file.WriteAt(payload, offset); err != nil {
		_ = file.Close()
		t.Fatalf("write mutation bytes: %v", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatalf("sync mutation: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close mutation file: %v", err)
	}
}

// fileSize returns a log's current size for exact tail-recovery assertions.
func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat log path: %v", err)
	}
	return info.Size()
}
