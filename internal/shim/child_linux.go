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

const descendantWaitOptions = unix.WALL

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

// Start forks and execs a structured absolute argv without a shell and returns a pidfd-backed Child.
func (OSChildRunner) Start(process domain.ProcessSpec, stdout, stderr io.Writer) (Child, error) {
	if err := process.Validate(); err != nil {
		return nil, err
	}
	if stdout == nil || stderr == nil {
		return nil, errors.New("OS child runner requires stdout and stderr writers")
	}
	if !filepath.IsAbs(process.Argv[0]) || filepath.Clean(process.Argv[0]) != process.Argv[0] {
		return nil, errors.New("OS child executable must be a clean absolute path")
	}
	if os.Getpid() != 1 {
		return nil, errors.New("OS child runner requires the init wrapper to be PID 1")
	}
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("create workload stdout pipe: %w", err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create workload stderr pipe: %w", err), stdoutReader.Close(), stdoutWriter.Close())
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
		return nil, errors.Join(fmt.Errorf("fork/exec workload child: %w", err),
			stdoutReader.Close(), stdoutWriter.Close(), stderrReader.Close(), stderrWriter.Close())
	}
	stdoutDone := copyChildOutput(stdoutReader, durableStdout)
	stderrDone := copyChildOutput(stderrReader, durableStderr)
	if err := errors.Join(stdoutWriter.Close(), stderrWriter.Close()); err != nil {
		abortErr := abortStartedChild(command, systemDescendantWaiter{}, stdoutDone, stderrDone)
		return nil, errors.Join(errors.New("close parent workload output descriptors"), err, abortErr)
	}
	pidfd, err := unix.PidfdOpen(command.Process.Pid, 0)
	if err != nil {
		abortErr := abortStartedChild(command, systemDescendantWaiter{}, stdoutDone, stderrDone)
		return nil, errors.Join(fmt.Errorf("capture workload pidfd: %w", err), abortErr)
	}
	evidence, err := ownership.EvidenceDigest(struct {
		PID        int       `json:"pid"`
		Executable string    `json:"executable"`
		StartedAt  time.Time `json:"started_at"`
	}{command.Process.Pid, process.Argv[0], startedAt})
	if err != nil {
		_ = unix.Close(pidfd)
		abortErr := abortStartedChild(command, systemDescendantWaiter{}, stdoutDone, stderrDone)
		return nil, errors.Join(err, abortErr)
	}
	return &osChild{
		command: command, pidfd: pidfd, startedAt: startedAt,
		stdout: durableStdout, stderr: durableStderr,
		stdoutDone: stdoutDone, stderrDone: stderrDone, reaper: systemDescendantWaiter{},
		killDescendants: killNamespaceDescendants,
		identity:        ChildIdentity{Handle: "pidfd-" + strconv.Itoa(pidfd), EvidenceSHA256: evidence},
	}, nil
}

// copyChildOutput drains one explicit child pipe independently from direct
// process waiting so inherited descriptors cannot postpone descendant cleanup.
func copyChildOutput(reader *os.File, writer *stickyErrorWriter) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(writer, reader)
		done <- errors.Join(copyErr, reader.Close())
	}()
	return done
}

// awaitChildOutput joins both bounded copy results after PID1 has killed and reaped every descendant.
func awaitChildOutput(stdoutDone, stderrDone <-chan error) error {
	if stdoutDone == nil || stderrDone == nil {
		return errors.New("workload output copy completion is not configured")
	}
	return errors.Join(<-stdoutDone, <-stderrDone)
}

// abortStartedChild kills one unpublishable direct child, destroys every
// remaining namespace descendant, and drains output before Start returns an error.
func abortStartedChild(command *exec.Cmd, reaper descendantWaiter, stdoutDone, stderrDone <-chan error) error {
	if command == nil || command.Process == nil {
		return errors.New("started workload command is not configured")
	}
	killErr := command.Process.Kill()
	if errors.Is(killErr, os.ErrProcessDone) {
		killErr = nil
	}
	waitErr := unexpectedCommandWaitError(command.Wait())
	return errors.Join(killErr, waitErr, killNamespaceDescendants(), reapDescendants(reaper), awaitChildOutput(stdoutDone, stderrDone))
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
	if reaper == nil {
		return errors.New("PID1 descendant reaper is not configured")
	}
	for {
		pid, err := reaper.WaitForExit()
		switch {
		case errors.Is(err, unix.EINTR):
			continue
		case errors.Is(err, unix.ECHILD):
			return nil
		case err != nil:
			return fmt.Errorf("reap PID1 descendant: %w", err)
		case pid <= 0:
			return errors.New("PID1 descendant wait returned no process")
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
	stdoutDone      <-chan error
	stderrDone      <-chan error
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
	outputErr := awaitChildOutput(child.stdoutDone, child.stderrDone)
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
