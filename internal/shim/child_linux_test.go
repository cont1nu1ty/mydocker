//go:build linux

package shim

import (
	"errors"
	"io"
	"testing"

	"golang.org/x/sys/unix"

	"mydocker/internal/domain"
)

// descendantWaitResult is one scripted wait4 result for PID1 reaper tests.
type descendantWaitResult struct {
	pid int
	err error
}

// scriptedDescendantWaiter returns deterministic exit, interruption, and empty-tree results.
type scriptedDescendantWaiter struct {
	results []descendantWaitResult
	calls   int
}

// WaitForExit returns the next scripted result without touching a real process table.
func (waiter *scriptedDescendantWaiter) WaitForExit() (int, error) {
	if waiter.calls >= len(waiter.results) {
		return 0, errors.New("unexpected descendant wait")
	}
	result := waiter.results[waiter.calls]
	waiter.calls++
	return result.pid, result.err
}

// failingStreamWriter returns a configured write shape without performing host I/O.
type failingStreamWriter struct {
	written int
	err     error
}

// Write returns the configured short-write or durable append failure.
func (writer failingStreamWriter) Write([]byte) (int, error) {
	return writer.written, writer.err
}

// TestStickyErrorWriterRetainsOutputFailure verifies a full-length write error
// remains observable even if os/exec later prefers a non-zero process status.
func TestStickyErrorWriterRetainsOutputFailure(t *testing.T) {
	injected := errors.New("injected durable log failure")
	writer := newStickyErrorWriter(failingStreamWriter{written: 4, err: injected})
	if written, err := writer.Write([]byte("data")); written != 4 || !errors.Is(err, injected) {
		t.Fatalf("Write()=(%d,%v), want full write plus injected error", written, err)
	}
	if !errors.Is(writer.Err(), injected) {
		t.Fatalf("Err()=%v, want injected error", writer.Err())
	}
}

// TestStickyErrorWriterTurnsShortWriteIntoFailure verifies a nil-error partial
// write cannot be hidden by a later workload exit status.
func TestStickyErrorWriterTurnsShortWriteIntoFailure(t *testing.T) {
	writer := newStickyErrorWriter(failingStreamWriter{written: 2})
	if written, err := writer.Write([]byte("data")); written != 2 || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Write()=(%d,%v), want short-write failure", written, err)
	}
	if !errors.Is(writer.Err(), io.ErrShortWrite) {
		t.Fatalf("Err()=%v, want io.ErrShortWrite", writer.Err())
	}
}

// TestOutputFailureInvalidatesKnownExit verifies durable log loss produces an
// unknown outcome even when a non-zero exit code was otherwise available.
func TestOutputFailureInvalidatesKnownExit(t *testing.T) {
	exitCode := int32(17)
	evidence := ChildExitEvidence{ExitCode: &exitCode, Signal: string(SignalTERM), OOM: domain.EvidenceUnknown}
	markChildWaitFailure(&evidence, errors.New("durable output unavailable"))
	if evidence.ExitCode != nil || evidence.Signal != "" || evidence.WaitError == "" {
		t.Fatalf("evidence after output failure=%+v, want unknown wait outcome", evidence)
	}
}

// TestReapDescendantsRetriesInterruptAndRequiresECHILD verifies PID1 drains
// every adopted child and only treats the kernel's empty-child result as complete.
func TestReapDescendantsRetriesInterruptAndRequiresECHILD(t *testing.T) {
	waiter := &scriptedDescendantWaiter{results: []descendantWaitResult{
		{err: unix.EINTR}, {pid: 17}, {pid: 19}, {err: unix.ECHILD},
	}}
	if err := reapDescendants(waiter); err != nil {
		t.Fatalf("reapDescendants() error = %v", err)
	}
	if waiter.calls != 4 {
		t.Fatalf("wait calls = %d, want 4", waiter.calls)
	}
}

// TestReapDescendantsFailsClosedOnUnexpectedWait verifies an uncertain process
// table cannot be treated as a fully drained Attempt.
func TestReapDescendantsFailsClosedOnUnexpectedWait(t *testing.T) {
	waiter := &scriptedDescendantWaiter{results: []descendantWaitResult{{err: unix.EIO}}}
	if err := reapDescendants(waiter); !errors.Is(err, unix.EIO) {
		t.Fatalf("reapDescendants() error = %v, want EIO", err)
	}
}

// TestDescendantWaitOptionsIncludeCloneChildren verifies PID1 cannot interpret
// ECHILD until Linux has considered both SIGCHLD and non-SIGCHLD clone children.
func TestDescendantWaitOptionsIncludeCloneChildren(t *testing.T) {
	if descendantWaitOptions != unix.WALL {
		t.Fatalf("descendant wait options = %#x, want __WALL %#x", descendantWaitOptions, unix.WALL)
	}
}

// TestSignalGateClosesWhenDirectChildExits verifies external signals cannot
// race PID reuse after the direct workload status has been collected.
func TestSignalGateClosesWhenDirectChildExits(t *testing.T) {
	child := &osChild{
		pidfd: 42, directExited: true,
		identity: ChildIdentity{Handle: "pidfd-42", EvidenceSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}
	if _, err := child.SignalVerified(SignalTERM); err == nil {
		t.Fatal("SignalVerified() after direct exit error = nil")
	}
}
