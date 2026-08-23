package shim

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/ownership"
)

// Clock supplies wall-clock terminal metadata and deterministic test timestamps.
type Clock interface {
	// Now returns the current wall time; elapsed child duration comes from Child wait evidence instead.
	Now() time.Time
}

// wallClock is the production clock used only for diagnostic record timestamps.
type wallClock struct{}

// Now returns the current process wall clock.
func (wallClock) Now() time.Time {
	return time.Now()
}

// InitDependencies are explicit process, output, persistence, and clock boundaries for one init wrapper.
type InitDependencies struct {
	Runner   ChildRunner
	Stdout   io.Writer
	Stderr   io.Writer
	Terminal TerminalStore
	Clock    Clock
	Rootfs   RootfsPreparer
}

// Validate rejects incomplete wiring before the one-shot gate can be exposed.
func (dependencies InitDependencies) Validate() error {
	if dependencies.Runner == nil || dependencies.Stdout == nil || dependencies.Stderr == nil || dependencies.Terminal == nil {
		return errors.New("init wrapper requires runner, stdout, stderr, and terminal store")
	}
	if dependencies.Clock == nil {
		return errors.New("init wrapper requires a clock")
	}
	return nil
}

// Wrapper is a long-lived keeper or init supervisor whose executable is never replaced by workload exec.
type Wrapper struct {
	mode            Mode
	owner           ownership.OwnerKey
	sandboxID       domain.SandboxID
	containerID     domain.ContainerID
	attemptID       domain.AttemptID
	wrapperEvidence string
	process         domain.ProcessSpec
	runner          ChildRunner
	stdout          io.Writer
	stderr          io.Writer
	terminalStore   TerminalStore
	clock           Clock

	mu             sync.Mutex
	gateConsumed   bool
	starting       bool
	child          Child
	childIdentity  ChildIdentity
	childStartedAt time.Time
	terminal       *TerminalRecord
	persistenceErr error
	rootfsPreparer RootfsPreparer
	rootfsStarted  bool
	rootfsRequest  RootfsRequest
	rootfs         *RootfsPreparation
	rootfsErr      error
	controlMu      sync.Mutex
	controlEntries map[string]*controlEntry
}

// NewKeeper creates a prepared, workload-free wrapper for provider-arranged Sandbox namespaces.
func NewKeeper(spec KeeperSpec) (*Wrapper, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	return &Wrapper{
		mode: ModeKeeper, owner: spec.Owner, sandboxID: spec.SandboxID,
		wrapperEvidence: spec.WrapperEvidence, controlEntries: make(map[string]*controlEntry),
	}, nil
}

// NewInit creates a closed-gate wrapper or restores an exact previously committed terminal result.
func NewInit(spec InitSpec, dependencies InitDependencies) (*Wrapper, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	if err := dependencies.Validate(); err != nil {
		return nil, err
	}
	wrapper := &Wrapper{
		mode: ModeInit, owner: spec.Owner, sandboxID: spec.SandboxID,
		containerID: spec.ContainerID, attemptID: spec.AttemptID,
		wrapperEvidence: spec.WrapperEvidence, process: spec.Process.Clone(),
		runner: dependencies.Runner, stdout: dependencies.Stdout, stderr: dependencies.Stderr,
		terminalStore: dependencies.Terminal, clock: dependencies.Clock,
		rootfsPreparer: dependencies.Rootfs,
		controlEntries: make(map[string]*controlEntry),
	}
	record, found, err := dependencies.Terminal.Load()
	if err != nil {
		return nil, fmt.Errorf("load init terminal state: %w", err)
	}
	if found {
		if err := wrapper.validateTerminalScope(record); err != nil {
			return nil, err
		}
		clone := record.Clone()
		wrapper.terminal = &clone
		wrapper.gateConsumed = true
	}
	return wrapper, nil
}

// NewInitWithDefaults creates a production init wrapper using the wall clock and explicit side-effect dependencies.
func NewInitWithDefaults(spec InitSpec, runner ChildRunner, stdout, stderr io.Writer, terminal TerminalStore) (*Wrapper, error) {
	return NewInit(spec, InitDependencies{Runner: runner, Stdout: stdout, Stderr: stderr, Terminal: terminal, Clock: wallClock{}})
}

// NewInitWithRootfs creates the production closed-gate wrapper whose Release
// remains unavailable until its injected PID1 preparer returns a durable ACK.
func NewInitWithRootfs(spec InitSpec, runner ChildRunner, stdout, stderr io.Writer, terminal TerminalStore, rootfs RootfsPreparer) (*Wrapper, error) {
	if rootfs == nil {
		return nil, errors.New("production init wrapper requires a rootfs preparer")
	}
	return NewInit(spec, InitDependencies{
		Runner: runner, Stdout: stdout, Stderr: stderr, Terminal: terminal,
		Clock: wallClock{}, Rootfs: rootfs,
	})
}

// Mode returns the immutable keeper or init role selected at construction.
func (wrapper *Wrapper) Mode() Mode {
	return wrapper.mode
}

// Owner returns the immutable persisted ownership key required by every control request.
func (wrapper *Wrapper) Owner() ownership.OwnerKey {
	return wrapper.owner
}

// Inspect returns an independently checksummed prepared, running, or durable terminal fact.
func (wrapper *Wrapper) Inspect() (Observation, error) {
	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()
	return wrapper.inspectLocked()
}

// inspectLocked builds one coherent observation and performs at most one exact
// terminal persistence retry when a prior commit ended with uncertain durability.
func (wrapper *Wrapper) inspectLocked() (Observation, error) {
	if wrapper.persistenceErr != nil && wrapper.terminal != nil {
		record := wrapper.terminal.Clone()
		wrapper.persistenceErr = wrapper.persistTerminal(record)
	}
	if wrapper.persistenceErr != nil {
		return Observation{}, newError(CodePersistenceFailed, "terminal result is not durably committed", wrapper.persistenceErr)
	}
	observation := Observation{
		SchemaVersion: SchemaVersion, Mode: wrapper.mode, Owner: wrapper.owner,
		SandboxID: wrapper.sandboxID, ContainerID: wrapper.containerID, AttemptID: wrapper.attemptID,
		WrapperEvidence: wrapper.wrapperEvidence,
	}
	if wrapper.rootfs != nil {
		rootfs := *wrapper.rootfs
		observation.Rootfs = &rootfs
	}
	switch {
	case wrapper.mode == ModeKeeper:
		observation.State = StatePrepared
	case wrapper.terminal != nil:
		observation.State = StateTerminal
		terminal := wrapper.terminal.Clone()
		observation.Terminal = &terminal
	case wrapper.child != nil:
		observation.State = StateRunning
		identity := wrapper.childIdentity
		observation.Child = &identity
	case wrapper.starting:
		observation.State = StateStarting
	default:
		observation.State = StatePrepared
	}
	digest, err := observationDigest(observation)
	if err != nil {
		return Observation{}, err
	}
	observation.EvidenceSHA256 = digest
	if err := observation.Validate(); err != nil {
		return Observation{}, err
	}
	return observation.Clone(), nil
}

// Release consumes the start gate exactly once, starts one child, and leaves the wrapper executable resident.
func (wrapper *Wrapper) Release() (Observation, error) {
	wrapper.mu.Lock()
	if wrapper.mode != ModeInit {
		wrapper.mu.Unlock()
		return Observation{}, newError(CodeWrongMode, "keeper has no workload start gate", nil)
	}
	if wrapper.gateConsumed {
		wrapper.mu.Unlock()
		return Observation{}, newError(CodeAlreadyReleased, "start gate has already been consumed", nil)
	}
	if wrapper.rootfsPreparer != nil && (wrapper.rootfs == nil || wrapper.rootfsErr != nil) {
		wrapper.mu.Unlock()
		return Observation{}, newError(CodeRootfsFailed, "rootfs preparation is not confirmed", wrapper.rootfsErr)
	}
	wrapper.gateConsumed = true
	wrapper.starting = true
	wrapper.childStartedAt = wrapper.clock.Now()
	process := wrapper.process.Clone()
	runner := wrapper.runner
	stdout := wrapper.stdout
	stderr := wrapper.stderr
	wrapper.mu.Unlock()

	child, startErr := runner.Start(process, stdout, stderr)
	if startErr != nil {
		persistErr := wrapper.recordStartFailure(startErr)
		if persistErr != nil {
			return Observation{}, newError(CodePersistenceFailed, "child start failed and terminal result was not durable", errors.Join(startErr, persistErr))
		}
		return Observation{}, newError(CodeStartFailed, "workload child could not be started", startErr)
	}
	if child == nil {
		startErr = errors.New("child runner returned nil child")
		persistErr := wrapper.recordStartFailure(startErr)
		if persistErr != nil {
			return Observation{}, newError(CodePersistenceFailed, "nil child result was not durably recorded", errors.Join(startErr, persistErr))
		}
		return Observation{}, newError(CodeStartFailed, "workload child could not be started", startErr)
	}
	identity := child.Identity()
	if err := identity.Validate(); err != nil {
		// The process object is retained and reaped, but no observation or signal is authorized by invalid identity.
		wrapper.mu.Lock()
		wrapper.starting = false
		wrapper.child = child
		wrapper.childIdentity = identity
		wrapper.mu.Unlock()
		go wrapper.reap(child)
		return Observation{}, newError(CodeStartFailed, "child runner returned unsafe identity", err)
	}
	wrapper.mu.Lock()
	wrapper.starting = false
	wrapper.child = child
	wrapper.childIdentity = identity
	observation, inspectErr := wrapper.inspectLocked()
	wrapper.mu.Unlock()
	go wrapper.reap(child)
	return observation, inspectErr
}

// PrepareRootfs executes the injected PID1 preparation at most once, exactly
// replays the same semantic command after response loss, and seals every partial failure.
func (wrapper *Wrapper) PrepareRootfs(request RootfsRequest) (RootfsPreparation, error) {
	if err := request.Validate(); err != nil {
		return RootfsPreparation{}, newError(CodeInvalidArgument, "invalid rootfs preparation request", err)
	}
	requestDigest, err := ownership.EvidenceDigest(request.Clone())
	if err != nil {
		return RootfsPreparation{}, newError(CodeInvalidArgument, "hash rootfs preparation request", err)
	}
	wrapper.mu.Lock()
	if wrapper.mode != ModeInit || wrapper.rootfsPreparer == nil {
		wrapper.mu.Unlock()
		return RootfsPreparation{}, newError(CodeWrongMode, "wrapper has no PID1 rootfs preparer", nil)
	}
	if wrapper.rootfs != nil {
		preparation := *wrapper.rootfs
		wrapper.mu.Unlock()
		if preparation.RequestSHA256 != requestDigest {
			return RootfsPreparation{}, newError(CodeDuplicateRequest, "rootfs was prepared from different immutable input", nil)
		}
		return preparation, nil
	}
	if wrapper.rootfsStarted {
		rootfsErr := wrapper.rootfsErr
		wrapper.mu.Unlock()
		return RootfsPreparation{}, newError(CodeRootfsFailed, "rootfs preparation was already attempted", rootfsErr)
	}
	wrapper.rootfsStarted = true
	wrapper.rootfsRequest = request.Clone()
	preparer := wrapper.rootfsPreparer
	wrapper.mu.Unlock()

	if err := preparer.PrepareRootfs(request.Clone()); err != nil {
		wrapper.mu.Lock()
		wrapper.rootfsErr = err
		wrapper.mu.Unlock()
		return RootfsPreparation{}, newError(CodeRootfsFailed, "PID1 rootfs preparation failed", err)
	}
	preparation, err := newRootfsPreparation(request, wrapper.clock.Now())
	if err != nil {
		wrapper.mu.Lock()
		wrapper.rootfsErr = err
		wrapper.mu.Unlock()
		return RootfsPreparation{}, newError(CodeRootfsFailed, "rootfs ACK construction failed", err)
	}
	wrapper.mu.Lock()
	wrapper.rootfs = &preparation
	wrapper.mu.Unlock()
	return preparation, nil
}

// recordStartFailure persists the terminal not-applicable result after the one-shot fork/exec attempt fails.
func (wrapper *Wrapper) recordStartFailure(startErr error) error {
	recordedAt := wrapper.clock.Now()
	record, err := NewTerminalRecord(
		wrapper.initSpec(), TerminalStartFailed, domain.NotApplicableOutcome(), nil,
		boundedDiagnostic(startErr), recordedAt,
	)
	if err != nil {
		return err
	}
	return wrapper.commitTerminal(record)
}

// reap waits exactly once, correlates exit and OOM evidence, commits terminal state, and keeps the wrapper alive.
func (wrapper *Wrapper) reap(child Child) {
	exit, waitErr := child.Wait()
	validationErr := exit.Validate()
	if waitErr != nil || validationErr != nil {
		wrapper.mu.Lock()
		startedAt := wrapper.childStartedAt
		wrapper.mu.Unlock()
		startedAt, finishedAt, runningDuration := durableExecutionWindow(startedAt, wrapper.clock.Now())
		var retainedDiagnostic error
		if exit.WaitError != "" {
			retainedDiagnostic = errors.New(exit.WaitError)
		}
		exit = ChildExitEvidence{
			Identity: child.Identity(), OOM: domain.EvidenceUnknown,
			StartedAt: startedAt, FinishedAt: finishedAt,
			RunningDuration: runningDuration,
			WaitError:       boundedDiagnostic(errors.Join(retainedDiagnostic, waitErr, validationErr)),
		}
	}
	reason := TerminalChildExit
	if exit.WaitError != "" {
		reason = TerminalWaitFailed
	}
	record, err := NewTerminalRecord(wrapper.initSpec(), reason, exit.DomainOutcome(), &exit, exit.WaitError, wrapper.clock.Now())
	if err != nil {
		wrapper.mu.Lock()
		wrapper.child = nil
		wrapper.persistenceErr = err
		wrapper.mu.Unlock()
		return
	}
	_ = wrapper.commitTerminal(record)
}

// commitTerminal publishes one immutable terminal fact and retains that exact
// fact for later inspection-driven recovery if durability remains uncertain.
func (wrapper *Wrapper) commitTerminal(record TerminalRecord) error {
	err := wrapper.persistTerminal(record)
	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()
	wrapper.starting = false
	wrapper.child = nil
	clone := record.Clone()
	wrapper.terminal = &clone
	wrapper.persistenceErr = err
	return err
}

// persistTerminal performs one bounded persistence attempt and accepts an
// existing destination only when its validated canonical record is identical.
func (wrapper *Wrapper) persistTerminal(record TerminalRecord) error {
	err := wrapper.terminalStore.Commit(record)
	if errors.Is(err, ErrTerminalExists) {
		existing, found, loadErr := wrapper.terminalStore.Load()
		if loadErr != nil {
			return errors.Join(err, loadErr)
		}
		if !found {
			return errors.Join(err, errors.New("existing terminal record could not be loaded"))
		}
		if validateErr := existing.Validate(); validateErr != nil {
			return errors.Join(err, validateErr)
		}
		equal, compareErr := sameTerminalRecord(existing, record)
		if compareErr != nil {
			return errors.Join(err, compareErr)
		}
		if !equal {
			return fmt.Errorf("%w: existing terminal record differs from the retained terminal fact", err)
		}
		return nil
	}
	return err
}

// ForwardSignal validates the retained child identity before delegating delivery, then stamps action-completion evidence for exact control replay.
func (wrapper *Wrapper) ForwardSignal(signal Signal) (SignalDelivery, error) {
	if !signal.Valid() {
		return SignalDelivery{}, newError(CodeInvalidArgument, "unsupported signal name", nil)
	}
	wrapper.mu.Lock()
	if wrapper.mode != ModeInit {
		wrapper.mu.Unlock()
		return SignalDelivery{}, newError(CodeWrongMode, "keeper has no workload child", nil)
	}
	if wrapper.child == nil || wrapper.terminal != nil {
		wrapper.mu.Unlock()
		return SignalDelivery{}, newError(CodeNotRunning, "no verified running workload child", nil)
	}
	identity := wrapper.childIdentity
	if err := identity.Validate(); err != nil {
		wrapper.mu.Unlock()
		return SignalDelivery{}, newError(CodeNotRunning, "retained workload child identity is unsafe", err)
	}
	child := wrapper.child
	wrapper.mu.Unlock()

	delivery, err := child.SignalVerified(signal)
	if err != nil {
		return SignalDelivery{}, newError(CodeNotRunning, "verified signal delivery failed", err)
	}
	// The wrapper clock is stamped immediately after the child-owned verified
	// action returns, so every exact control replay carries the same grace anchor.
	delivery.DeliveredAt = wrapper.clock.Now().Round(0).UTC()
	if err := delivery.Validate(); err != nil {
		return SignalDelivery{}, newError(CodeNotRunning, "child returned invalid signal evidence", err)
	}
	if delivery.Identity != identity || delivery.Signal != signal {
		return SignalDelivery{}, newError(CodeNotRunning, "signal evidence does not match the running child", nil)
	}
	return delivery, nil
}

// initSpec reconstructs immutable init identity for terminal records without exposing runtime state.
func (wrapper *Wrapper) initSpec() InitSpec {
	return InitSpec{
		Owner: wrapper.owner, SandboxID: wrapper.sandboxID, ContainerID: wrapper.containerID,
		AttemptID: wrapper.attemptID, WrapperEvidence: wrapper.wrapperEvidence, Process: wrapper.process.Clone(),
	}
}

// validateTerminalScope prevents a stale or swapped terminal file from closing this wrapper's gate.
func (wrapper *Wrapper) validateTerminalScope(record TerminalRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	if record.Owner != wrapper.owner || record.ContainerID != wrapper.containerID || record.AttemptID != wrapper.attemptID ||
		record.WrapperEvidence != wrapper.wrapperEvidence {
		return errors.New("terminal record does not belong to this wrapper instance")
	}
	return nil
}

// boundedDiagnostic turns a local error into persistence-safe, bounded text without making it a reason label.
func boundedDiagnostic(err error) string {
	if err == nil {
		return "unknown failure"
	}
	message := err.Error()
	if len(message) > 2048 {
		message = message[:2048]
	}
	bytes := []byte(message)
	for index := range bytes {
		if bytes[index] == 0 {
			bytes[index] = '?'
		}
	}
	return string(bytes)
}
