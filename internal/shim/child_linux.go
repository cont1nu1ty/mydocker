//go:build linux

package shim

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"mydocker/internal/domain"
	"mydocker/internal/ownership"
)

// OSChildRunner uses os/exec to fork one workload child and immediately captures a pidfd strong handle.
type OSChildRunner struct{}

const (
	descendantWaitOptions         = unix.WALL | unix.WNOHANG
	descendantCleanupTimeout      = 5 * time.Second
	descendantCleanupPollInterval = 10 * time.Millisecond
)

var errDirectChildWaitDeadlineExceeded = errors.New("direct workload wait cleanup deadline exceeded")
var errChildOutputDeadlineExceeded = errors.New("workload output drain deadline exceeded")

// descendantWaiter is the PID1-only wait boundary used after the direct
// workload status has been collected without competing with exec.Cmd.Wait.
type descendantWaiter interface {
	WaitForExit() (int, error)
}

// systemDescendantWaiter waits for any child currently adopted by PID1.
type systemDescendantWaiter struct{}

// WaitForExit blocks until one adopted child exits or the namespace has no children.
func (systemDescendantWaiter) WaitForExit() (int, error) {
	var status unix.WaitStatus
	pid, err := unix.Wait4(-1, &status, descendantWaitOptions, nil)
	return pid, err
}

// directChildAbortTarget starts the direct-child wait asynchronously so an
// uninterruptible child cannot hold the post-exec Start failure path forever.
// Only that background wait may reap the direct child; PID1 wait4 cleanup does
// not begin until its completion has been observed.
type directChildAbortTarget interface {
	Kill() error
	BeginWait() <-chan error
}

// execDirectChildAbortTarget adapts one started exec.Cmd to the bounded abort boundary.
type execDirectChildAbortTarget struct {
	command *exec.Cmd
}

// Kill requests termination of the exact direct child started by exec.Cmd.
func (target execDirectChildAbortTarget) Kill() error {
	if target.command == nil || target.command.Process == nil {
		return errors.New("started workload command is not configured")
	}
	return target.command.Process.Kill()
}

// BeginWait leaves exec.Cmd.Wait running after an abort timeout so the direct
// child is eventually reaped without a competing wait4 call.
func (target execDirectChildAbortTarget) BeginWait() <-chan error {
	done := make(chan error, 1)
	go func() {
		if target.command == nil {
			done <- errors.New("started workload command is not configured")
			return
		}
		done <- unexpectedCommandWaitError(target.command.Wait())
	}()
	return done
}

// descendantReapPolicy bounds nonblocking PID1 cleanup and provides a
// deterministic clock, sleeper, and repeated-kill seam for failure tests.
type descendantReapPolicy struct {
	timeout      time.Duration
	pollInterval time.Duration
	now          func() time.Time
	sleep        func(time.Duration)
	kill         func() error
}

// defaultDescendantReapPolicy returns the production bounded cleanup policy.
func defaultDescendantReapPolicy() descendantReapPolicy {
	return descendantReapPolicy{
		timeout: descendantCleanupTimeout, pollInterval: descendantCleanupPollInterval,
		now: time.Now, sleep: time.Sleep, kill: killNamespaceDescendants,
	}
}

// validate rejects a policy that could spin forever or omit repeated process-tree termination.
func (policy descendantReapPolicy) validate() error {
	if policy.timeout <= 0 || policy.pollInterval <= 0 || policy.now == nil || policy.sleep == nil || policy.kill == nil {
		return errors.New("PID1 descendant reap policy is incomplete")
	}
	return nil
}

// Start forks and execs a structured absolute argv without a shell and returns a pidfd-backed Child.
func (OSChildRunner) Start(process domain.ProcessSpec, stdout, stderr io.Writer) (Child, error) {
	if err := process.Validate(); err != nil {
		return nil, NewPreExecChildStartError(err)
	}
	if stdout == nil || stderr == nil {
		return nil, NewPreExecChildStartError(errors.New("OS child runner requires stdout and stderr writers"))
	}
	if !filepath.IsAbs(process.Argv[0]) || filepath.Clean(process.Argv[0]) != process.Argv[0] {
		return nil, NewPreExecChildStartError(errors.New("OS child executable must be a clean absolute path"))
	}
	if os.Getpid() != 1 {
		return nil, NewPreExecChildStartError(errors.New("OS child runner requires the init wrapper to be PID 1"))
	}
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		return nil, NewPreExecChildStartError(fmt.Errorf("create workload stdout pipe: %w", err))
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		return nil, NewPreExecChildStartError(errors.Join(fmt.Errorf("create workload stderr pipe: %w", err), stdoutReader.Close(), stdoutWriter.Close()))
	}
	command := exec.Command(process.Argv[0], process.Argv[1:]...)
	command.Env = make([]string, len(process.Environment))
	for index, variable := range process.Environment {
		command.Env[index] = variable.Name + "=" + variable.Value
	}
	command.Dir = process.WorkingDirectory
	durableStdout := newStickyErrorWriter(stdout)
	durableStderr := newStickyErrorWriter(stderr)
	command.Stdout = stdoutWriter
	command.Stderr = stderrWriter
	startedAt := time.Now()
	if err := command.Start(); err != nil {
		return nil, NewPreExecChildStartError(errors.Join(fmt.Errorf("fork/exec workload child: %w", err),
			stdoutReader.Close(), stdoutWriter.Close(), stderrReader.Close(), stderrWriter.Close()))
	}
	stdoutCopy := copyChildOutput(stdoutReader, durableStdout)
	stderrCopy := copyChildOutput(stderrReader, durableStderr)
	if err := errors.Join(stdoutWriter.Close(), stderrWriter.Close()); err != nil {
		quiescent, abortErr := abortStartedChild(command, systemDescendantWaiter{}, stdoutCopy, stderrCopy)
		return nil, NewExecutedChildStartError(
			errors.Join(errors.New("close parent workload output descriptors"), err, abortErr), quiescent,
		)
	}
	pidfd, err := unix.PidfdOpen(command.Process.Pid, 0)
	if err != nil {
		quiescent, abortErr := abortStartedChild(command, systemDescendantWaiter{}, stdoutCopy, stderrCopy)
		return nil, NewExecutedChildStartError(errors.Join(fmt.Errorf("capture workload pidfd: %w", err), abortErr), quiescent)
	}
	evidence, err := ownership.EvidenceDigest(struct {
		PID        int       `json:"pid"`
		Executable string    `json:"executable"`
		StartedAt  time.Time `json:"started_at"`
	}{command.Process.Pid, process.Argv[0], startedAt})
	if err != nil {
		_ = unix.Close(pidfd)
		quiescent, abortErr := abortStartedChild(command, systemDescendantWaiter{}, stdoutCopy, stderrCopy)
		return nil, NewExecutedChildStartError(errors.Join(err, abortErr), quiescent)
	}
	return &osChild{
		command: command, pidfd: pidfd, startedAt: startedAt,
		stdout: durableStdout, stderr: durableStderr,
		stdoutCopy: stdoutCopy, stderrCopy: stderrCopy, reaper: systemDescendantWaiter{},
		killDescendants: killNamespaceDescendants,
		identity:        ChildIdentity{Handle: "pidfd-" + strconv.Itoa(pidfd), EvidenceSHA256: evidence},
	}, nil
}

// childOutputCopy owns one explicit read descriptor and completion channel so
// failed descendant cleanup can cancel inherited-writer drainage deterministically.
type childOutputCopy struct {
	reader     *os.File
	done       <-chan error
	cancelOnce sync.Once
	cancelErr  error
}

// copyChildOutput drains one explicit child pipe independently from direct
// process waiting so inherited descriptors cannot postpone descendant cleanup.
func copyChildOutput(reader *os.File, writer *stickyErrorWriter) *childOutputCopy {
	done := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(writer, reader)
		done <- errors.Join(copyErr, reader.Close())
	}()
	return &childOutputCopy{reader: reader, done: done}
}

// cancel closes the read descriptor once so a writer retained by an
// unquiescent process tree cannot make the wrapper wait without bound.
func (copy *childOutputCopy) cancel() error {
	if copy == nil || copy.reader == nil {
		return errors.New("workload output copy is not configured")
	}
	copy.cancelOnce.Do(func() {
		copy.cancelErr = copy.reader.Close()
		if errors.Is(copy.cancelErr, os.ErrClosed) {
			copy.cancelErr = nil
		}
	})
	return copy.cancelErr
}

// awaitChildOutput joins both copy results within one explicit budget. Closing
// readers interrupts a pipe read, while the deadline also prevents a durable
// sink blocked in Write from holding terminal publication forever.
func awaitChildOutput(stdoutCopy, stderrCopy *childOutputCopy, cancel bool, maximum time.Duration) error {
	if stdoutCopy == nil || stderrCopy == nil || stdoutCopy.done == nil || stderrCopy.done == nil {
		return errors.New("workload output copy completion is not configured")
	}
	if maximum <= 0 {
		return errors.Join(errChildOutputDeadlineExceeded, cancelChildOutput(stdoutCopy, stderrCopy))
	}
	var cancelErr error
	if cancel {
		cancelErr = cancelChildOutput(stdoutCopy, stderrCopy)
	}
	timer := time.NewTimer(maximum)
	defer timer.Stop()
	stdoutDone := stdoutCopy.done
	stderrDone := stderrCopy.done
	var stdoutErr error
	var stderrErr error
	for stdoutDone != nil || stderrDone != nil {
		select {
		case result, ok := <-stdoutDone:
			stdoutDone = nil
			if !ok {
				stdoutErr = errors.New("workload stdout copy closed without a result")
			} else {
				stdoutErr = result
			}
		case result, ok := <-stderrDone:
			stderrDone = nil
			if !ok {
				stderrErr = errors.New("workload stderr copy closed without a result")
			} else {
				stderrErr = result
			}
		case <-timer.C:
			return errors.Join(cancelErr, stdoutErr, stderrErr, errChildOutputDeadlineExceeded, cancelChildOutput(stdoutCopy, stderrCopy))
		}
	}
	return errors.Join(cancelErr, stdoutErr, stderrErr)
}

// cancelChildOutput closes both read descriptors without waiting for a
// potentially blocked durable sink, allowing an unconfirmed abort to return.
func cancelChildOutput(stdoutCopy, stderrCopy *childOutputCopy) error {
	if stdoutCopy == nil || stderrCopy == nil {
		return errors.New("workload output copy is not configured")
	}
	return errors.Join(stdoutCopy.cancel(), stderrCopy.cancel())
}

// abortStartedChild kills one unpublishable direct child, destroys every
// remaining namespace descendant, drains output, and returns an independent
// ECHILD-based process-tree quiescence fact before Start reports failure.
func abortStartedChild(command *exec.Cmd, reaper descendantWaiter, stdoutCopy, stderrCopy *childOutputCopy) (bool, error) {
	if command == nil || command.Process == nil {
		return false, errors.New("started workload command is not configured")
	}
	return abortStartedChildWithPolicy(
		execDirectChildAbortTarget{command: command}, reaper, stdoutCopy, stderrCopy,
		defaultDescendantReapPolicy(),
	)
}

// abortStartedChildWithPolicy applies one cleanup deadline to the direct wait
// and all later PID1 descendant cleanup. A direct-wait timeout deliberately
// skips wait4 so two waiters can never race to reap the same child.
func abortStartedChildWithPolicy(target directChildAbortTarget, reaper descendantWaiter, stdoutCopy, stderrCopy *childOutputCopy, policy descendantReapPolicy) (bool, error) {
	if target == nil {
		return false, errors.New("started workload abort target is not configured")
	}
	if err := policy.validate(); err != nil {
		return false, err
	}
	deadline := policy.now().Add(policy.timeout)
	killErr := target.Kill()
	if errors.Is(killErr, os.ErrProcessDone) {
		killErr = nil
	}
	waitDone := target.BeginWait()
	remaining := deadline.Sub(policy.now())
	if remaining <= 0 {
		return false, errors.Join(killErr, errDirectChildWaitDeadlineExceeded, cancelChildOutput(stdoutCopy, stderrCopy))
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	var waitErr error
	select {
	case completedErr, ok := <-waitDone:
		if !ok {
			return false, errors.Join(killErr, errors.New("direct workload wait closed without a result"), cancelChildOutput(stdoutCopy, stderrCopy))
		}
		waitErr = completedErr
	case <-timer.C:
		return false, errors.Join(killErr, errDirectChildWaitDeadlineExceeded, cancelChildOutput(stdoutCopy, stderrCopy))
	}
	descendantKillErr := policy.kill()
	reapErr := reapDescendantsUntil(reaper, policy, deadline)
	outputBudget := deadline.Sub(policy.now())
	return reapErr == nil, errors.Join(killErr, waitErr, descendantKillErr, reapErr, awaitChildOutput(stdoutCopy, stderrCopy, reapErr != nil, outputBudget))
}

// unexpectedCommandWaitError filters the expected non-zero or signaled direct
// status while preserving failures that prevented a trustworthy wait result.
func unexpectedCommandWaitError(err error) error {
	var exitError *exec.ExitError
	if err == nil || errors.As(err, &exitError) {
		return nil
	}
	return err
}

// killNamespaceDescendants asks Linux PID1 semantics to kill every visible
// process except the wrapper itself; it refuses to run outside a PID1 context.
func killNamespaceDescendants() error {
	if os.Getpid() != 1 {
		return errors.New("namespace descendant cleanup requires PID 1")
	}
	err := unix.Kill(-1, unix.SIGKILL)
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	return err
}

// reapDescendants waits until the PID namespace contains no adopted child,
// retrying interrupted waits and rejecting impossible non-positive results.
func reapDescendants(reaper descendantWaiter) error {
	return reapDescendantsWithPolicy(reaper, defaultDescendantReapPolicy())
}

// reapDescendantsWithPolicy repeats kill and nonblocking wait4 until __WALL
// reports ECHILD, or fails closed at a bounded deadline without claiming the
// process tree is quiescent.
func reapDescendantsWithPolicy(reaper descendantWaiter, policy descendantReapPolicy) error {
	if reaper == nil {
		return errors.New("PID1 descendant reaper is not configured")
	}
	if err := policy.validate(); err != nil {
		return err
	}
	deadline := policy.now().Add(policy.timeout)
	return reapDescendantsUntil(reaper, policy, deadline)
}

// reapDescendantsUntil consumes only the portion of an existing cleanup budget
// left after the direct child has been reaped.
func reapDescendantsUntil(reaper descendantWaiter, policy descendantReapPolicy, deadline time.Time) error {
	if reaper == nil {
		return errors.New("PID1 descendant reaper is not configured")
	}
	if err := policy.validate(); err != nil {
		return err
	}
	var repeatedKillErr error
	for {
		pid, err := reaper.WaitForExit()
		if errors.Is(err, unix.ECHILD) {
			return nil
		}
		if !policy.now().Before(deadline) {
			return errors.Join(errors.New("PID1 descendant cleanup deadline exceeded"), repeatedKillErr, err)
		}
		switch {
		case errors.Is(err, unix.EINTR):
			continue
		case err != nil:
			return fmt.Errorf("reap PID1 descendant: %w", err)
		case pid > 0:
			continue
		case pid < 0:
			return errors.New("PID1 descendant wait returned an invalid process")
		default:
			killErr := policy.kill()
			if killErr != nil && !errors.Is(killErr, unix.ESRCH) {
				repeatedKillErr = errors.Join(repeatedKillErr, killErr)
			}
			policy.sleep(policy.pollInterval)
		}
	}
}

// stickyErrorWriter records the first durable output failure independently of
// os/exec's exit-status precedence while preserving normal io.Writer behavior.
type stickyErrorWriter struct {
	mu     sync.Mutex
	writer io.Writer
	err    error
}

// newStickyErrorWriter wraps one already validated stream writer for child-copy diagnostics.
func newStickyErrorWriter(writer io.Writer) *stickyErrorWriter {
	return &stickyErrorWriter{writer: writer}
}

// Write forwards one chunk and retains an error or short write for later wait correlation.
func (writer *stickyErrorWriter) Write(payload []byte) (int, error) {
	written, err := writer.writer.Write(payload)
	if err == nil && written != len(payload) {
		err = io.ErrShortWrite
	}
	if err != nil {
		writer.mu.Lock()
		if writer.err == nil {
			writer.err = err
		}
		writer.mu.Unlock()
	}
	return written, err
}

// Err returns the first output persistence failure observed by the copy goroutine.
func (writer *stickyErrorWriter) Err() error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.err
}

// osChild owns a live pidfd through wait and serializes action-time signal delivery against handle close.
type osChild struct {
	command         *exec.Cmd
	pidfd           int
	startedAt       time.Time
	identity        ChildIdentity
	stdout          *stickyErrorWriter
	stderr          *stickyErrorWriter
	stdoutCopy      *childOutputCopy
	stderrCopy      *childOutputCopy
	reaper          descendantWaiter
	killDescendants func() error
	waitMu          sync.Mutex
	stateMu         sync.Mutex
	waited          bool
	directExited    bool
	closed          bool
}

// Identity returns immutable diagnostic evidence for the pidfd-backed child object.
func (child *osChild) Identity() ChildIdentity {
	return child.identity
}

// Wait reaps the workload exactly once, closes its pidfd, and preserves unknown OOM rather than guessing from SIGKILL.
func (child *osChild) Wait() (ChildExitEvidence, error) {
	child.waitMu.Lock()
	defer child.waitMu.Unlock()
	if child.waited {
		return ChildExitEvidence{}, errors.New("workload child has already been waited")
	}
	child.waited = true
	waitErr := child.command.Wait()
	directFinishedAt := time.Now()
	child.stateMu.Lock()
	child.directExited = true
	child.stateMu.Unlock()
	killDescendantsErr := errors.New("workload descendant cleanup is not configured")
	if child.killDescendants != nil {
		killDescendantsErr = child.killDescendants()
	}
	reapErr := reapDescendants(child.reaper)
	outputErr := awaitChildOutput(child.stdoutCopy, child.stderrCopy, reapErr != nil, descendantCleanupTimeout)
	var resultErr error
	startedAt, finishedAt, runningDuration := durableExecutionWindow(child.startedAt, directFinishedAt)
	child.stateMu.Lock()
	child.closed = true
	closeErr := unix.Close(child.pidfd)
	child.stateMu.Unlock()
	resultErr = closeErr
	if reapErr != nil {
		resultErr = errors.Join(resultErr, fmt.Errorf("%w: %v", errDescendantCleanupUnconfirmed, reapErr))
	}
	streamErr := errors.Join(child.stdout.Err(), child.stderr.Err(), outputErr)
	evidence := ChildExitEvidence{
		Identity: child.identity, OOM: domain.EvidenceUnknown,
		StartedAt: startedAt, FinishedAt: finishedAt,
		RunningDuration: runningDuration,
	}
	if child.command.ProcessState == nil {
		markChildWaitFailure(&evidence, errors.Join(waitErr, killDescendantsErr, reapErr, streamErr, errors.New("missing process state")))
		return evidence, resultErr
	}
	waitStatus, ok := child.command.ProcessState.Sys().(syscall.WaitStatus)
	if !ok {
		markChildWaitFailure(&evidence, errors.Join(waitErr, killDescendantsErr, reapErr, streamErr, errors.New("unsupported wait status")))
		return evidence, resultErr
	}
	if waitStatus.Signaled() {
		evidence.Signal = signalName(waitStatus.Signal())
	} else if waitStatus.Exited() {
		exitCode := int32(waitStatus.ExitStatus())
		evidence.ExitCode = &exitCode
	} else {
		markChildWaitFailure(&evidence, errors.Join(waitErr, streamErr, errors.New("process was reaped without terminal status")))
	}
	markChildWaitFailure(&evidence, errors.Join(unexpectedCommandWaitError(waitErr), killDescendantsErr, reapErr, streamErr))
	return evidence, resultErr
}

// markChildWaitFailure prevents a stream or wait failure from being persisted
// as a known exit outcome even when the kernel also supplied terminal status.
func markChildWaitFailure(evidence *ChildExitEvidence, err error) {
	if err == nil {
		return
	}
	evidence.ExitCode = nil
	evidence.Signal = ""
	evidence.WaitError = boundedDiagnostic(err)
}

// SignalVerified sends a bounded signal through the still-open pidfd while handle close is excluded.
func (child *osChild) SignalVerified(signal Signal) (SignalDelivery, error) {
	kernelSignal, err := linuxSignal(signal)
	if err != nil {
		return SignalDelivery{}, err
	}
	child.stateMu.Lock()
	defer child.stateMu.Unlock()
	if child.closed || child.directExited {
		return SignalDelivery{}, errors.New("workload pidfd is closed")
	}
	if err := unix.PidfdSendSignal(child.pidfd, kernelSignal, nil, 0); err != nil {
		return SignalDelivery{}, fmt.Errorf("send verified pidfd signal: %w", err)
	}
	return SignalDelivery{Identity: child.identity, Signal: signal, Delivered: true}, nil
}

// linuxSignal translates the bounded protocol name to a kernel signal without accepting integers.
func linuxSignal(signal Signal) (unix.Signal, error) {
	switch signal {
	case SignalHUP:
		return unix.SIGHUP, nil
	case SignalINT:
		return unix.SIGINT, nil
	case SignalQUIT:
		return unix.SIGQUIT, nil
	case SignalKILL:
		return unix.SIGKILL, nil
	case SignalTERM:
		return unix.SIGTERM, nil
	case SignalUSR1:
		return unix.SIGUSR1, nil
	case SignalUSR2:
		return unix.SIGUSR2, nil
	default:
		return 0, fmt.Errorf("unsupported signal %q", signal)
	}
}

// signalName returns a persistence-safe canonical name for any Linux terminal signal.
func signalName(signal syscall.Signal) string {
	switch signal {
	case syscall.SIGHUP:
		return string(SignalHUP)
	case syscall.SIGINT:
		return string(SignalINT)
	case syscall.SIGQUIT:
		return string(SignalQUIT)
	case syscall.SIGKILL:
		return string(SignalKILL)
	case syscall.SIGTERM:
		return string(SignalTERM)
	case syscall.SIGUSR1:
		return string(SignalUSR1)
	case syscall.SIGUSR2:
		return string(SignalUSR2)
	default:
		return "SIG" + strconv.Itoa(int(signal))
	}
}
