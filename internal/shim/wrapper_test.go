package shim

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/logstore"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
)

// fixedClock supplies stable terminal record times without timing assertions.
type fixedClock struct {
	now time.Time
}

// Now returns the fixed test timestamp.
func (clock fixedClock) Now() time.Time {
	return clock.now
}

// TestDurableExecutionWindowSurvivesWallClockRollbackAndJSONRoundTrip verifies
// persisted exit facts remain ordered when a clock source reports a backward wall step.
func TestDurableExecutionWindowSurvivesWallClockRollbackAndJSONRoundTrip(t *testing.T) {
	startedAt := testTime().Add(time.Hour)
	durableStartedAt, durableFinishedAt, duration := durableExecutionWindow(startedAt, startedAt.Add(-time.Hour))
	evidence := ChildExitEvidence{
		Identity: ChildIdentity{Handle: "clock-rollback-child", EvidenceSHA256: strings.Repeat("a", 64)},
		OOM:      domain.EvidenceUnknown, StartedAt: durableStartedAt, FinishedAt: durableFinishedAt,
		RunningDuration: duration, WaitError: "wait failed after clock rollback",
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var recovered ChildExitEvidence
	if err := json.Unmarshal(encoded, &recovered); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if err := recovered.Validate(); err != nil {
		t.Fatalf("recovered ChildExitEvidence.Validate() error = %v", err)
	}
	if recovered.RunningDuration != 0 || !recovered.FinishedAt.Equal(recovered.StartedAt) {
		t.Fatalf("recovered window = %s..%s duration=%s, want clamped durable zero interval", recovered.StartedAt, recovered.FinishedAt, recovered.RunningDuration)
	}
}

// memoryTerminalStore provides synchronized immutable terminal persistence for wrapper state tests.
type memoryTerminalStore struct {
	mu                  sync.Mutex
	record              *TerminalRecord
	commitErr           error
	commitAfterStoreErr error
	commits             int
}

// Load returns an independent in-memory terminal record when one was committed.
func (store *memoryTerminalStore) Load() (TerminalRecord, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.record == nil {
		return TerminalRecord{}, false, nil
	}
	return store.record.Clone(), true, nil
}

// Commit records the first terminal value and can expose deterministic failures
// either before publication or once after publication to model uncertain success.
func (store *memoryTerminalStore) Commit(record TerminalRecord) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.commits++
	if store.commitErr != nil {
		return store.commitErr
	}
	if store.record != nil {
		return ErrTerminalExists
	}
	clone := record.Clone()
	store.record = &clone
	if store.commitAfterStoreErr != nil {
		err := store.commitAfterStoreErr
		store.commitAfterStoreErr = nil
		return err
	}
	return nil
}

// CommitCount reports synchronized persistence attempts for bounded-retry assertions.
func (store *memoryTerminalStore) CommitCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.commits
}

// fakeRunner counts child starts and can deterministically return one injected error or child.
type fakeRunner struct {
	starts atomic.Int32
	child  Child
	err    error
}

// blockingRunner holds child creation between gate consumption and verified identity publication for starting-state tests.
type blockingRunner struct {
	started chan struct{}
	resume  chan struct{}
	child   Child
}

// Start announces the consumed gate, waits for the test checkpoint, and then returns the configured fake child.
func (runner *blockingRunner) Start(domain.ProcessSpec, io.Writer, io.Writer) (Child, error) {
	close(runner.started)
	<-runner.resume
	return runner.child, nil
}

// Start records one structured child request without performing fork or exec.
func (runner *fakeRunner) Start(domain.ProcessSpec, io.Writer, io.Writer) (Child, error) {
	runner.starts.Add(1)
	return runner.child, runner.err
}

// TestInspectDistinguishesConsumedGateWhileStarting verifies recovery never mistakes an in-flight one-shot start for a closed gate.
func TestInspectDistinguishesConsumedGateWhileStarting(t *testing.T) {
	child := newFakeChild()
	runner := &blockingRunner{started: make(chan struct{}), resume: make(chan struct{}), child: child}
	wrapper, err := NewInit(testInitSpec(t, "op-starting", "container-starting", "attempt-starting"), InitDependencies{
		Runner: runner, Stdout: io.Discard, Stderr: io.Discard, Terminal: &memoryTerminalStore{}, Clock: fixedClock{now: testTime()},
	})
	if err != nil {
		t.Fatalf("NewInit() error = %v", err)
	}
	released := make(chan error, 1)
	go func() {
		_, releaseErr := wrapper.Release()
		released <- releaseErr
	}()
	<-runner.started
	observation, err := wrapper.Inspect()
	if err != nil || observation.State != StateStarting {
		t.Fatalf("Inspect(starting) = (%#v, %v)", observation, err)
	}
	close(runner.resume)
	if err := <-released; err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	observation, err = wrapper.Inspect()
	if err != nil || observation.State != StateRunning {
		t.Fatalf("Inspect(running) = (%#v, %v)", observation, err)
	}
}

// fakeChild exposes channel-controlled wait facts and action-time signal evidence without OS side effects.
type fakeChild struct {
	identity ChildIdentity
	exit     chan ChildExitEvidence
	waitErr  error
	signals  atomic.Int32
}

// Identity returns the fake strong-handle evidence configured by the test.
func (child *fakeChild) Identity() ChildIdentity {
	return child.identity
}

// Wait blocks until the test supplies one terminal child result.
func (child *fakeChild) Wait() (ChildExitEvidence, error) {
	return <-child.exit, child.waitErr
}

// SignalVerified records a signal and returns evidence bound to this exact fake child identity.
func (child *fakeChild) SignalVerified(signal Signal) (SignalDelivery, error) {
	child.signals.Add(1)
	return SignalDelivery{Identity: child.identity, Signal: signal, Delivered: true}, nil
}

// TestConcurrentReleaseStartsOneChild verifies that concurrent gate release has exactly one winner and one child start.
func TestConcurrentReleaseStartsOneChild(t *testing.T) {
	spec := testInitSpec(t, "op-release", "container-release", "attempt-release")
	child := newFakeChild()
	runner := &fakeRunner{child: child}
	store := &memoryTerminalStore{}
	wrapper, err := NewInit(spec, InitDependencies{
		Runner: runner, Stdout: io.Discard, Stderr: io.Discard, Terminal: store,
		Clock: fixedClock{now: testTime()},
	})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 32
	var wait sync.WaitGroup
	wait.Add(callers)
	var successes atomic.Int32
	var alreadyReleased atomic.Int32
	for index := 0; index < callers; index++ {
		go func() {
			defer wait.Done()
			_, releaseErr := wrapper.Release()
			switch {
			case releaseErr == nil:
				successes.Add(1)
			case IsCode(releaseErr, CodeAlreadyReleased):
				alreadyReleased.Add(1)
			default:
				t.Errorf("unexpected release error: %v", releaseErr)
			}
		}()
	}
	wait.Wait()
	if successes.Load() != 1 || alreadyReleased.Load() != callers-1 || runner.starts.Load() != 1 {
		t.Fatalf("success=%d already=%d starts=%d", successes.Load(), alreadyReleased.Load(), runner.starts.Load())
	}
	observation, err := wrapper.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	if observation.State != StateRunning || observation.Child == nil || *observation.Child != child.identity {
		t.Fatalf("unexpected running observation: %+v", observation)
	}
	delivery, err := wrapper.ForwardSignal(SignalTERM)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Identity != child.identity || child.signals.Load() != 1 {
		t.Fatalf("signal was not bound to fake child: %+v", delivery)
	}
	if err := delivery.Validate(); err != nil {
		t.Fatalf("SignalDelivery.Validate() error = %v", err)
	}
	tamperedDelivery := delivery
	tamperedDelivery.DeliveredAt = tamperedDelivery.DeliveredAt.Add(time.Nanosecond)
	if err := tamperedDelivery.Validate(); err == nil {
		t.Fatal("SignalDelivery.Validate() accepted a timestamp not bound by its evidence")
	}
	child.exit <- successfulExit(child.identity, 7, domain.EvidenceFalse)
	terminal := waitForState(t, wrapper, StateTerminal)
	if terminal.Terminal == nil || terminal.Terminal.Outcome.ExitCode == nil || *terminal.Terminal.Outcome.ExitCode != 7 {
		t.Fatalf("missing captured terminal exit: %+v", terminal)
	}
	if store.commits != 1 {
		t.Fatalf("terminal commits=%d, want 1", store.commits)
	}
}

// TestUnconfirmedDescendantCleanupBlocksTerminal verifies an unexpected PID1
// reap failure remains non-terminal and cannot authorize later signal delivery.
func TestUnconfirmedDescendantCleanupBlocksTerminal(t *testing.T) {
	child := newFakeChild()
	child.waitErr = errDescendantCleanupUnconfirmed
	store := &memoryTerminalStore{}
	wrapper, err := NewInit(
		testInitSpec(t, "op-reap-unknown", "container-reap-unknown", "attempt-reap-unknown"),
		InitDependencies{
			Runner: &fakeRunner{child: child}, Stdout: io.Discard, Stderr: io.Discard,
			Terminal: store, Clock: fixedClock{now: testTime()},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapper.Release(); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	child.exit <- successfulExit(child.identity, 0, domain.EvidenceFalse)
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, inspectErr := wrapper.Inspect()
		if IsCode(inspectErr, CodeUnavailable) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Inspect() never exposed unconfirmed descendant cleanup: %v", inspectErr)
		}
		time.Sleep(time.Millisecond)
	}
	if store.commits != 0 {
		t.Fatalf("terminal commits = %d, want 0 while process-tree quiescence is unknown", store.commits)
	}
	if _, err := wrapper.ForwardSignal(SignalTERM); !IsCode(err, CodeNotRunning) {
		t.Fatalf("ForwardSignal() error = %v, want not_running after direct child exit", err)
	}
}

// TestForwardSignalRejectsRetainedInvalidIdentity verifies a rejected child identity cannot reach even the fake signal side-effect boundary before reap completes.
func TestForwardSignalRejectsRetainedInvalidIdentity(t *testing.T) {
	child := &fakeChild{
		identity: ChildIdentity{Handle: "", EvidenceSHA256: strings.Repeat("b", 64)},
		exit:     make(chan ChildExitEvidence, 1),
	}
	wrapper, err := NewInit(
		testInitSpec(t, "op-invalid-child", "container-invalid-child", "attempt-invalid-child"),
		InitDependencies{
			Runner: &fakeRunner{child: child}, Stdout: io.Discard, Stderr: io.Discard,
			Terminal: &memoryTerminalStore{}, Clock: fixedClock{now: testTime()},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapper.Release(); !IsCode(err, CodeStartFailed) {
		t.Fatalf("Release() error = %v, want start_failed", err)
	}
	if _, err := wrapper.ForwardSignal(SignalTERM); !IsCode(err, CodeNotRunning) {
		t.Fatalf("ForwardSignal() error = %v, want not_running", err)
	}
	if calls := child.signals.Load(); calls != 0 {
		t.Fatalf("SignalVerified() calls = %d, want 0", calls)
	}
	child.exit <- ChildExitEvidence{}
}

// TestStartFailureConsumesGateAndRestoresTerminal verifies a failed fork/exec is durable and never retryable after restart.
func TestStartFailureConsumesGateAndRestoresTerminal(t *testing.T) {
	spec := testInitSpec(t, "op-start-fail", "container-start-fail", "attempt-start-fail")
	store := &memoryTerminalStore{}
	runner := &fakeRunner{err: errors.New("injected fork failure")}
	wrapper, err := NewInit(spec, InitDependencies{
		Runner: runner, Stdout: io.Discard, Stderr: io.Discard, Terminal: store,
		Clock: fixedClock{now: testTime()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapper.Release(); !IsCode(err, CodeStartFailed) {
		t.Fatalf("release error=%v, want start_failed", err)
	}
	observation, err := wrapper.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	if observation.State != StateTerminal || observation.Terminal == nil || observation.Terminal.Outcome.Presence != domain.OutcomeNotApplicable {
		t.Fatalf("unexpected start-failure terminal state: %+v", observation)
	}
	restartRunner := &fakeRunner{child: newFakeChild()}
	restarted, err := NewInit(spec, InitDependencies{
		Runner: restartRunner, Stdout: io.Discard, Stderr: io.Discard, Terminal: store,
		Clock: fixedClock{now: testTime()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Release(); !IsCode(err, CodeAlreadyReleased) {
		t.Fatalf("restart release error=%v, want already_released", err)
	}
	if restartRunner.starts.Load() != 0 {
		t.Fatalf("restart runner starts=%d, want 0", restartRunner.starts.Load())
	}
}

// TestTerminalPersistenceFailureFailsInspectClosed verifies each Inspect makes
// only one retry and never exposes a terminal fact while persistence still fails.
func TestTerminalPersistenceFailureFailsInspectClosed(t *testing.T) {
	spec := testInitSpec(t, "op-persist-fail", "container-persist-fail", "attempt-persist-fail")
	child := newFakeChild()
	store := &memoryTerminalStore{commitErr: errors.New("injected sync failure")}
	wrapper, err := NewInit(spec, InitDependencies{
		Runner: &fakeRunner{child: child}, Stdout: io.Discard, Stderr: io.Discard, Terminal: store,
		Clock: fixedClock{now: testTime()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapper.Release(); err != nil {
		t.Fatal(err)
	}
	child.exit <- successfulExit(child.identity, 0, domain.EvidenceFalse)
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err = wrapper.Inspect()
		if IsCode(err, CodePersistenceFailed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("inspect never exposed persistence failure; last error=%v", err)
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := wrapper.ForwardSignal(SignalTERM); !IsCode(err, CodeNotRunning) {
		t.Fatalf("terminal signal error=%v, want not_running", err)
	}
}

// TestInspectRecoversExactTerminalAfterUncertainCommit verifies a post-publication
// error is cleared by one later exact retry without starting or rewriting a child.
func TestInspectRecoversExactTerminalAfterUncertainCommit(t *testing.T) {
	spec := testInitSpec(t, "op-persist-recover", "container-persist-recover", "attempt-persist-recover")
	child := newFakeChild()
	store := &memoryTerminalStore{commitAfterStoreErr: errors.New("injected post-publication uncertainty")}
	wrapper, err := NewInit(spec, InitDependencies{
		Runner: &fakeRunner{child: child}, Stdout: io.Discard, Stderr: io.Discard,
		Terminal: store, Clock: fixedClock{now: testTime()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapper.Release(); err != nil {
		t.Fatal(err)
	}
	child.exit <- successfulExit(child.identity, 0, domain.EvidenceFalse)
	observation := waitForState(t, wrapper, StateTerminal)
	if observation.Terminal == nil || observation.Terminal.Outcome.ExitCode == nil || *observation.Terminal.Outcome.ExitCode != 0 {
		t.Fatalf("recovered terminal observation=%+v", observation)
	}
	if commits := store.CommitCount(); commits != 2 {
		t.Fatalf("persistence attempts=%d, want initial uncertain commit plus one Inspect retry", commits)
	}
	if _, err := wrapper.Inspect(); err != nil {
		t.Fatalf("stable terminal Inspect() error=%v", err)
	}
	if commits := store.CommitCount(); commits != 2 {
		t.Fatalf("stable inspection retried persistence: attempts=%d, want 2", commits)
	}
}

// TestInspectRejectsDifferentExistingTerminal verifies recovery cannot clear a
// persistence error by loading a different valid immutable terminal record.
func TestInspectRejectsDifferentExistingTerminal(t *testing.T) {
	spec := testInitSpec(t, "op-persist-conflict", "container-persist-conflict", "attempt-persist-conflict")
	retained, err := NewTerminalRecord(spec, TerminalStartFailed, domain.NotApplicableOutcome(), nil, "retained fact", testTime())
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := NewTerminalRecord(spec, TerminalStartFailed, domain.NotApplicableOutcome(), nil, "different fact", testTime())
	if err != nil {
		t.Fatal(err)
	}
	store := &memoryTerminalStore{record: &foreign}
	wrapper, err := NewInit(spec, InitDependencies{
		Runner: &fakeRunner{}, Stdout: io.Discard, Stderr: io.Discard,
		Terminal: &memoryTerminalStore{}, Clock: fixedClock{now: testTime()},
	})
	if err != nil {
		t.Fatal(err)
	}
	wrapper.terminalStore = store
	if err := wrapper.commitTerminal(retained); !errors.Is(err, ErrTerminalExists) {
		t.Fatalf("commit conflict error=%v, want ErrTerminalExists", err)
	}
	if _, err := wrapper.Inspect(); !IsCode(err, CodePersistenceFailed) || !errors.Is(err, ErrTerminalExists) {
		t.Fatalf("Inspect() error=%v, want persistence_failed wrapping ErrTerminalExists", err)
	}
	loaded, found, err := store.Load()
	if err != nil || !found || loaded.ChecksumSHA256 != foreign.ChecksumSHA256 {
		t.Fatalf("foreign terminal was changed: found=%v error=%v record=%+v", found, err, loaded)
	}
}

// TestMissingOOMEvidencePreservesRawExitWithoutInventingOutcome verifies a known wait result remains usable while OOM attribution is independently unknown.
func TestMissingOOMEvidencePreservesRawExitWithoutInventingOutcome(t *testing.T) {
	identity := ChildIdentity{Handle: "child-unknown-oom", EvidenceSHA256: strings.Repeat("b", 64)}
	exit := successfulExit(identity, 137, domain.EvidenceUnknown)
	outcome := exit.DomainOutcome()
	if outcome.Presence != domain.OutcomeCaptured || outcome.ExitCode == nil || *outcome.ExitCode != 137 || outcome.OOM != domain.EvidenceUnknown {
		t.Fatalf("outcome=%+v exit=%+v", outcome, exit)
	}
	if err := outcome.Validate(); err != nil {
		t.Fatal(err)
	}
}

// TestReapRetainsChildDiagnosticAlongsideHandleCloseFailure verifies a durable
// output error is not discarded when pidfd cleanup independently fails.
func TestReapRetainsChildDiagnosticAlongsideHandleCloseFailure(t *testing.T) {
	child := newFakeChild()
	child.waitErr = errors.New("injected pidfd close failure")
	store := &memoryTerminalStore{}
	wrapper, err := NewInit(
		testInitSpec(t, "op-output-fail", "container-output-fail", "attempt-output-fail"),
		InitDependencies{
			Runner: &fakeRunner{child: child}, Stdout: io.Discard, Stderr: io.Discard,
			Terminal: store, Clock: fixedClock{now: testTime()},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapper.Release(); err != nil {
		t.Fatal(err)
	}
	startedAt := testTime()
	child.exit <- ChildExitEvidence{
		Identity: child.identity, OOM: domain.EvidenceUnknown,
		StartedAt: startedAt, FinishedAt: startedAt.Add(time.Second), RunningDuration: time.Second,
		WaitError: "durable stdout append failed",
	}
	terminal := waitForState(t, wrapper, StateTerminal)
	if terminal.Terminal == nil || terminal.Terminal.ChildExit == nil {
		t.Fatalf("terminal observation=%+v, want child evidence", terminal)
	}
	diagnostic := terminal.Terminal.ChildExit.WaitError
	if !strings.Contains(diagnostic, "durable stdout append failed") || !strings.Contains(diagnostic, "pidfd close failure") {
		t.Fatalf("terminal diagnostic=%q, want output and handle cleanup failures", diagnostic)
	}
}

// TestKeeperRejectsWorkloadActions verifies the namespace keeper only exposes prepared inspection.
func TestKeeperRejectsWorkloadActions(t *testing.T) {
	wrapper, err := NewKeeper(testKeeperSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	observation, err := wrapper.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	if observation.Mode != ModeKeeper || observation.State != StatePrepared {
		t.Fatalf("unexpected keeper observation: %+v", observation)
	}
	if _, err := wrapper.Release(); !IsCode(err, CodeWrongMode) {
		t.Fatalf("keeper release error=%v, want wrong_mode", err)
	}
	if _, err := wrapper.ForwardSignal(SignalTERM); !IsCode(err, CodeWrongMode) {
		t.Fatalf("keeper signal error=%v, want wrong_mode", err)
	}
}

// testInitSpec creates one exact ownership-bound init spec for fake-only tests.
func testInitSpec(t *testing.T, operationID, containerID, attemptID string) InitSpec {
	t.Helper()
	owner, err := ownership.NewOwnerKey(
		operation.OperationID(operationID),
		operation.Target{Kind: operation.TargetContainer, ID: containerID},
		domain.InitialGeneration,
	)
	if err != nil {
		t.Fatal(err)
	}
	return InitSpec{
		Owner: owner, SandboxID: domain.SandboxID("sandbox-test"),
		ContainerID: domain.ContainerID(containerID), AttemptID: domain.AttemptID(attemptID),
		WrapperEvidence: strings.Repeat("a", 64),
		Process:         domain.ProcessSpec{Argv: []string{"/fake/workload", "argument with space"}},
	}
}

// testKeeperSpec creates one exact Sandbox ownership-bound keeper spec.
func testKeeperSpec(t *testing.T) KeeperSpec {
	t.Helper()
	owner, err := ownership.NewOwnerKey(
		operation.OperationID("op-keeper"),
		operation.Target{Kind: operation.TargetSandbox, ID: "sandbox-keeper"},
		domain.InitialGeneration,
	)
	if err != nil {
		t.Fatal(err)
	}
	return KeeperSpec{Owner: owner, SandboxID: "sandbox-keeper", WrapperEvidence: strings.Repeat("c", 64)}
}

// newFakeChild creates a strong-identity child whose exit remains blocked until the test releases it.
func newFakeChild() *fakeChild {
	return &fakeChild{
		identity: ChildIdentity{Handle: "fake-child", EvidenceSHA256: strings.Repeat("b", 64)},
		exit:     make(chan ChildExitEvidence, 1),
	}
}

// successfulExit constructs coherent captured wait and OOM evidence for the fake child.
func successfulExit(identity ChildIdentity, code int32, oom domain.EvidenceState) ChildExitEvidence {
	startedAt := testTime()
	finishedAt := startedAt.Add(5 * time.Second)
	return ChildExitEvidence{
		Identity: identity, ExitCode: &code, OOM: oom, StartedAt: startedAt,
		FinishedAt: finishedAt, RunningDuration: 5 * time.Second,
	}
}

// waitForState waits only for asynchronous fake reap completion, not for a performance threshold.
func waitForState(t *testing.T, wrapper *Wrapper, state State) Observation {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		observation, err := wrapper.Inspect()
		if err == nil && observation.State == state {
			return observation
		}
		if time.Now().After(deadline) {
			t.Fatalf("wrapper never reached %s; last observation=%+v error=%v", state, observation, err)
		}
		time.Sleep(time.Millisecond)
	}
}

// testTime returns a JSON-stable wall timestamp with no monotonic component.
func testTime() time.Time {
	return time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
}

// TestLogWriterPersistsExactChunks verifies injected stdout/stderr writers preserve byte chunks without aliasing.
func TestLogWriterPersistsExactChunks(t *testing.T) {
	appender := &fakeAppender{}
	writer, err := NewLogWriter(appender, "stdout")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("hello")
	written, err := writer.Write(payload)
	if err != nil || written != len(payload) {
		t.Fatalf("written=%d error=%v", written, err)
	}
	payload[0] = 'x'
	if !bytes.Equal(appender.payload, []byte("hello")) {
		t.Fatalf("persisted payload=%q", appender.payload)
	}
}

// fakeAppender captures one logstore-compatible append for stream-writer tests.
type fakeAppender struct {
	payload []byte
}

// Append retains an independent payload copy and returns an otherwise unused frame.
func (appender *fakeAppender) Append(_ logstore.Stream, payload []byte) (logstore.Frame, error) {
	appender.payload = append([]byte(nil), payload...)
	return logstore.Frame{}, nil
}
