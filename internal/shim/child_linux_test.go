//go:build linux

package shim

import (
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

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

// endlessDescendantWaiter repeats one non-terminal result so deadline tests do
// not depend on allocating a large finite script.
type endlessDescendantWaiter struct {
	result descendantWaitResult
	calls  int
}

// neverCompletingDirectChild records termination while withholding direct-wait
// completion, modeling a killed child stuck in an uninterruptible kernel state.
type neverCompletingDirectChild struct {
	wait  <-chan error
	kills int
}

// Kill records the exact-child termination request without host process effects.
func (child *neverCompletingDirectChild) Kill() error {
	child.kills++
	return nil
}

// BeginWait returns a channel that never becomes ready.
func (child *neverCompletingDirectChild) BeginWait() <-chan error {
	return child.wait
}

// deadlineAbortRunner routes Start through the real post-exec abort helper and
// exposes its classification to wrapper-level fail-closed assertions.
type deadlineAbortRunner struct {
	target     directChildAbortTarget
	reaper     descendantWaiter
	stdoutCopy *childOutputCopy
	stderrCopy *childOutputCopy
	policy     descendantReapPolicy
	quiescent  bool
	abortErr   error
}

// Start reports the helper's post-exec result through the production typed boundary.
func (runner *deadlineAbortRunner) Start(domain.ProcessSpec, io.Writer, io.Writer) (Child, error) {
	runner.quiescent, runner.abortErr = abortStartedChildWithPolicy(
		runner.target, runner.reaper, runner.stdoutCopy, runner.stderrCopy, runner.policy,
	)
	return nil, NewExecutedChildStartError(runner.abortErr, runner.quiescent)
}

// WaitForExit returns the configured non-terminal result on every call.
func (waiter *endlessDescendantWaiter) WaitForExit() (int, error) {
	waiter.calls++
	return waiter.result.pid, waiter.result.err
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

// blockingStreamWriter proves closing the source pipe cannot interrupt a copy
// goroutine that has already entered a durable sink Write call.
type blockingStreamWriter struct {
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

// Write blocks after publishing an entry barrier until the test releases the simulated sink.
func (writer *blockingStreamWriter) Write(payload []byte) (int, error) {
	writer.once.Do(func() { close(writer.entered) })
	<-writer.release
	return len(payload), nil
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

// TestDescendantWaitOptionsIncludeCloneChildrenAndNeverBlock verifies PID1
// considers non-SIGCHLD clone children while keeping cleanup deadline-capable.
func TestDescendantWaitOptionsIncludeCloneChildrenAndNeverBlock(t *testing.T) {
	want := unix.WALL | unix.WNOHANG
	if descendantWaitOptions != want {
		t.Fatalf("descendant wait options = %#x, want __WALL|WNOHANG %#x", descendantWaitOptions, want)
	}
}

// TestReapDescendantsRepeatsKillUntilECHILD verifies a live descendant cannot
// exploit a fork/kill race between the first namespace kill and final reap.
func TestReapDescendantsRepeatsKillUntilECHILD(t *testing.T) {
	waiter := &scriptedDescendantWaiter{results: []descendantWaitResult{
		{pid: 0}, {pid: 27}, {pid: 0}, {err: unix.ECHILD},
	}}
	now := testTime()
	kills := 0
	policy := descendantReapPolicy{
		timeout: time.Second, pollInterval: time.Millisecond,
		now:   func() time.Time { return now },
		sleep: func(duration time.Duration) { now = now.Add(duration) },
		kill:  func() error { kills++; return nil },
	}
	if err := reapDescendantsWithPolicy(waiter, policy); err != nil {
		t.Fatalf("reapDescendantsWithPolicy() error = %v", err)
	}
	if kills != 2 || waiter.calls != 4 {
		t.Fatalf("kills=%d waits=%d, want 2 repeated kills and 4 waits", kills, waiter.calls)
	}
}

// TestReapDescendantsDeadlineFailsClosed verifies an unkillable descendant
// returns control without manufacturing the ECHILD proof required for terminal state.
func TestReapDescendantsDeadlineFailsClosed(t *testing.T) {
	waiter := &scriptedDescendantWaiter{results: []descendantWaitResult{{pid: 0}, {pid: 0}}}
	now := testTime()
	policy := descendantReapPolicy{
		timeout: time.Millisecond, pollInterval: time.Millisecond,
		now:   func() time.Time { return now },
		sleep: func(duration time.Duration) { now = now.Add(duration) },
		kill:  func() error { return unix.EPERM },
	}
	err := reapDescendantsWithPolicy(waiter, policy)
	if err == nil || !errors.Is(err, unix.EPERM) {
		t.Fatalf("reapDescendantsWithPolicy() error = %v, want bounded failure retaining EPERM", err)
	}
}

// TestReapDescendantsDeadlineCoversContinuousProgressAndInterrupts verifies
// neither a zombie stream nor repeated EINTR can bypass the cleanup deadline.
func TestReapDescendantsDeadlineCoversContinuousProgressAndInterrupts(t *testing.T) {
	for _, result := range []descendantWaitResult{{pid: 19}, {err: unix.EINTR}} {
		waiter := &endlessDescendantWaiter{result: result}
		now := testTime()
		policy := descendantReapPolicy{
			timeout: 3 * time.Millisecond, pollInterval: time.Millisecond,
			now: func() time.Time {
				now = now.Add(time.Millisecond)
				return now
			},
			sleep: func(time.Duration) {}, kill: func() error { return nil },
		}
		if err := reapDescendantsWithPolicy(waiter, policy); err == nil {
			t.Fatalf("result=%+v bypassed cleanup deadline", result)
		}
		if waiter.calls > 4 {
			t.Fatalf("result=%+v wait calls=%d, want bounded", result, waiter.calls)
		}
	}
}

// TestAwaitChildOutputCancellationIsBounded verifies a process retaining the
// write ends cannot make a failed quiescence path block on log drainage.
func TestAwaitChildOutputCancellationIsBounded(t *testing.T) {
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		t.Fatal(err)
	}
	defer stdoutWriter.Close()
	defer stderrWriter.Close()
	stdoutCopy := copyChildOutput(stdoutReader, newStickyErrorWriter(io.Discard))
	stderrCopy := copyChildOutput(stderrReader, newStickyErrorWriter(io.Discard))
	done := make(chan error, 1)
	go func() { done <- awaitChildOutput(stdoutCopy, stderrCopy, true, time.Second) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("awaitChildOutput() remained blocked after read cancellation")
	}
}

// TestAwaitChildOutputBoundsBlockedDurableSink verifies a log append stalled
// inside Write cannot hold an otherwise quiescent PID1 terminal path forever.
func TestAwaitChildOutputBoundsBlockedDurableSink(t *testing.T) {
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		t.Fatal(err)
	}
	release := make(chan struct{})
	sink := &blockingStreamWriter{entered: make(chan struct{}), release: release}
	stdoutCopy := copyChildOutput(stdoutReader, newStickyErrorWriter(sink))
	stderrCopy := copyChildOutput(stderrReader, newStickyErrorWriter(io.Discard))
	if err := stderrWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := stdoutWriter.Write([]byte("blocked durable log payload")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("output copy never entered the blocking durable sink")
	}
	startedAt := time.Now()
	outputErr := awaitChildOutput(stdoutCopy, stderrCopy, false, 25*time.Millisecond)
	if !errors.Is(outputErr, errChildOutputDeadlineExceeded) {
		t.Fatalf("awaitChildOutput() error = %v, want output deadline", outputErr)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("awaitChildOutput() elapsed = %v, want bounded return", elapsed)
	}
	close(release)
	_ = stdoutWriter.Close()
	select {
	case <-stdoutCopy.done:
	case <-time.After(time.Second):
		t.Fatal("output copy did not finish after releasing the simulated sink")
	}
}

// TestDirectChildWaitDeadlineKeepsExecutedStartNonTerminal verifies a killed
// but unreaped direct child cannot block Release or race PID1 wait4. The wrapper
// must retain unavailable state rather than committing a false terminal fact.
func TestDirectChildWaitDeadlineKeepsExecutedStartNonTerminal(t *testing.T) {
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		t.Fatal(err)
	}
	defer stdoutWriter.Close()
	defer stderrWriter.Close()
	stdoutCopy := copyChildOutput(stdoutReader, newStickyErrorWriter(io.Discard))
	stderrCopy := copyChildOutput(stderrReader, newStickyErrorWriter(io.Discard))
	target := &neverCompletingDirectChild{wait: make(chan error)}
	reaper := &scriptedDescendantWaiter{results: []descendantWaitResult{{err: unix.ECHILD}}}
	descendantKills := 0
	runner := &deadlineAbortRunner{
		target: target, reaper: reaper, stdoutCopy: stdoutCopy, stderrCopy: stderrCopy,
		policy: descendantReapPolicy{
			timeout: 25 * time.Millisecond, pollInterval: time.Millisecond,
			now: time.Now, sleep: time.Sleep,
			kill: func() error { descendantKills++; return nil },
		},
	}
	store := &memoryTerminalStore{}
	wrapper, err := NewInit(
		testInitSpec(t, "op-direct-wait-timeout", "container-direct-wait-timeout", "attempt-direct-wait-timeout"),
		InitDependencies{
			Runner: runner, Stdout: io.Discard, Stderr: io.Discard,
			Terminal: store, Clock: fixedClock{now: testTime()},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Now()
	if _, err := wrapper.Release(); !IsCode(err, CodeUnavailable) {
		t.Fatalf("Release() error = %v, want unavailable", err)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("Release() elapsed = %v, want bounded post-exec abort", elapsed)
	}
	if runner.quiescent || !errors.Is(runner.abortErr, errDirectChildWaitDeadlineExceeded) {
		t.Fatalf("abort result = (quiescent=%t, error=%v), want unconfirmed direct-wait deadline", runner.quiescent, runner.abortErr)
	}
	if target.kills != 1 || reaper.calls != 0 || descendantKills != 0 {
		t.Fatalf("cleanup calls = (direct kills=%d, waits=%d, descendant kills=%d), want (1,0,0)", target.kills, reaper.calls, descendantKills)
	}
	if _, err := wrapper.Inspect(); !IsCode(err, CodeUnavailable) {
		t.Fatalf("Inspect() error = %v, want unavailable", err)
	}
	if store.CommitCount() != 0 {
		t.Fatalf("terminal commits = %d, want 0 while direct-child reap is unconfirmed", store.CommitCount())
	}
	for name, done := range map[string]<-chan error{"stdout": stdoutCopy.done, "stderr": stderrCopy.done} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s output copy did not stop after direct-wait timeout", name)
		}
	}
}

// TestOSChildRunnerClassifiesValidationFailureAsPreExec verifies only explicit
// no-effect evidence can authorize the wrapper's not-applicable terminal path.
func TestOSChildRunnerClassifiesValidationFailureAsPreExec(t *testing.T) {
	_, err := (OSChildRunner{}).Start(domain.ProcessSpec{Argv: []string{"relative"}}, io.Discard, io.Discard)
	var preExec *PreExecChildStartError
	if !errors.As(err, &preExec) {
		t.Fatalf("OSChildRunner.Start() error = %v, want PreExecChildStartError", err)
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
