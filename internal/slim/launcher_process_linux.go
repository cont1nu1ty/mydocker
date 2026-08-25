//go:build linux

package slim

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"mydocker/internal/shim"

	"golang.org/x/sys/unix"
)

// OSProcessFactory uses Linux clone-time cgroup placement, pidfd capture, and
// a parent-owned release pipe; construction has no side effects. The pipe is
// deliberately the sole parent-liveness gate because Linux parent-death
// signals bind to the creating thread rather than the daemon process.
type OSProcessFactory struct{}

var errUnpublishedCleanupTimeout = errors.New("timed out waiting for unpublished shim process cleanup")

// clone3ProbeArgs mirrors the stable Linux clone_args layout through the
// cgroup field so a feature-specific preflight never depends on libc wrappers.
type clone3ProbeArgs struct {
	flags      uint64
	pidfd      uint64
	childTID   uint64
	parentTID  uint64
	exitSignal uint64
	stack      uint64
	stackSize  uint64
	tls        uint64
	setTID     uint64
	setTIDSize uint64
	cgroup     uint64
}

// clone3ProbeInvalidCgroupFD is representable by the kernel's signed file
// descriptor type but cannot be opened under Linux's NR_OPEN ceiling. Using
// an all-bits-set uint64 would be rejected as EINVAL before clone3 resolves
// the descriptor, so it could not prove CLONE_INTO_CGROUP support.
const clone3ProbeInvalidCgroupFD = uint64(1<<31 - 1)

// Preflight submits a clone3 request with CLONE_PIDFD, CLONE_INTO_CGROUP, a
// valid pidfd output pointer, and an intentionally invalid cgroup descriptor;
// only EBADF proves the kernel parsed both requested features without creating a child.
func (OSProcessFactory) Preflight(ctx context.Context) error {
	if ctx == nil {
		return errors.New("process factory preflight context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return classifyClone3CgroupPIDFDProbe(clone3CgroupPIDFDProbe())
}

// clone3CgroupPIDFDProbe asks the kernel to parse the exact required clone3
// flags but guarantees failure before process creation through cgroup fd -1.
func clone3CgroupPIDFDProbe() syscall.Errno {
	pidfd := int32(-1)
	arguments := clone3ProbeArgs{
		flags:  uint64(unix.CLONE_PIDFD) | uint64(unix.CLONE_INTO_CGROUP),
		pidfd:  uint64(uintptr(unsafe.Pointer(&pidfd))),
		cgroup: clone3ProbeInvalidCgroupFD,
	}
	_, _, errno := unix.Syscall(unix.SYS_CLONE3, uintptr(unsafe.Pointer(&arguments)), unsafe.Sizeof(arguments), 0)
	return errno
}

// classifyClone3CgroupPIDFDProbe accepts only EBADF, the expected result after
// both required flags were parsed and the deliberately invalid cgroup fd was resolved.
func classifyClone3CgroupPIDFDProbe(errno syscall.Errno) error {
	if errno == unix.EBADF {
		return nil
	}
	if errno == 0 {
		return errors.New("clone3 cgroup/pidfd probe unexpectedly created a child")
	}
	return fmt.Errorf("clone3 with CLONE_INTO_CGROUP and CLONE_PIDFD is unavailable or blocked: %w", errno)
}

// Start executes one shim behind a closed release gate. It never uses
// CommandContext's raw-PID cancellation and transfers every supplied ExtraFD.
func (OSProcessFactory) Start(ctx context.Context, spec ProcessLaunchSpec) (_ StartedProcess, resultErr error) {
	rawExtraFDs := append([]int(nil), spec.ExtraFDs...)
	rawExtraOwned := true
	defer func() {
		if resultErr != nil && rawExtraOwned {
			resultErr = errors.Join(resultErr, closeTransferredFDs(rawExtraFDs, spec.CgroupFD))
		}
	}()
	if ctx == nil {
		return nil, errors.New("process factory context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("create launch release pipe: %w", err)
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, reader.Close(), writer.Close())
		}
	}()
	extraFiles := make([]*os.File, 0, len(spec.ExtraFDs)+1)
	for index, fd := range spec.ExtraFDs {
		file := os.NewFile(uintptr(fd), fmt.Sprintf("mydocker-namespace-%d", index))
		if file == nil {
			return nil, errors.New("process launch extra descriptor is invalid")
		}
		extraFiles = append(extraFiles, file)
	}
	rawExtraOwned = false
	extraFiles = append(extraFiles, reader)
	pidfd := -1
	command := exec.Command(spec.Executable, spec.Arguments...)
	command.Env = append([]string(nil), spec.Environment...)
	command.ExtraFiles = extraFiles
	command.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: spec.CloneFlags, Setsid: true,
		UseCgroupFD: true, CgroupFD: spec.CgroupFD, PidFD: &pidfd,
	}
	startErr := command.Start()
	closeErrors := make([]error, 0, len(extraFiles))
	for _, file := range extraFiles {
		closeErrors = append(closeErrors, file.Close())
	}
	if startErr != nil {
		return nil, errors.Join(append([]error{fmt.Errorf("start shim process: %w", startErr)}, closeErrors...)...)
	}
	if closeErr := errors.Join(closeErrors...); closeErr != nil {
		terminate := command.Process.Kill
		if pidfd >= 0 {
			terminate = func() error {
				return errors.Join(unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0), unix.Close(pidfd))
			}
		}
		cleanupErr := terminateUnpublishedCommand(command, terminate, defaultLauncherCleanupTimeout)
		return nil, errors.Join(fmt.Errorf("close transferred launch descriptors: %w", closeErr), cleanupErr)
	}
	if pidfd < 0 {
		cleanupErr := terminateUnpublishedCommand(command, command.Process.Kill, defaultLauncherCleanupTimeout)
		return nil, errors.Join(errors.New("kernel did not return a clone-time pidfd"), cleanupErr)
	}
	cleanupFD, err := unix.FcntlInt(uintptr(pidfd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		cleanupErr := terminateUnpublishedCommand(command, func() error {
			return errors.Join(unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0), unix.Close(pidfd))
		}, defaultLauncherCleanupTimeout)
		return nil, errors.Join(fmt.Errorf("duplicate cleanup pidfd: %w", err), cleanupErr)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	return &osStartedProcess{
		pid: command.Process.Pid, pidfd: pidfd, cleanupFD: cleanupFD,
		release: writer, done: done, state: startedProcessGated,
		signalPIDFD: func(fd int) error { return unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0) },
		closeFD:     unix.Close, abortDone: make(chan struct{}),
	}, nil
}

// terminateUnpublishedCommand makes post-exec construction failures bounded.
// Wait continues in one background goroutine after the deadline so the exact
// child is eventually reaped without allowing Start to hang on an unkillable
// task. The caller must supply the strongest available termination handle.
func terminateUnpublishedCommand(command *exec.Cmd, terminate func() error, timeout time.Duration) error {
	if command == nil || command.Process == nil || terminate == nil || timeout <= 0 {
		return errors.New("unpublished process cleanup dependencies are incomplete")
	}
	terminateErr := terminate()
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case waitErr := <-done:
		return errors.Join(terminateErr, normalizeAbortedWait(waitErr))
	case <-timer.C:
		return errors.Join(terminateErr, errUnpublishedCleanupTimeout)
	}
}

// closeTransferredFDs closes each unique valid transferred descriptor once
// while preserving a borrowed cgroup descriptor from malformed alias input.
func closeTransferredFDs(descriptors []int, borrowedCgroupFD int) error {
	seen := make(map[int]struct{}, len(descriptors))
	var result error
	for _, fd := range descriptors {
		if fd < 0 || fd == borrowedCgroupFD {
			continue
		}
		if _, duplicate := seen[fd]; duplicate {
			continue
		}
		seen[fd] = struct{}{}
		result = errors.Join(result, unix.Close(fd))
	}
	return result
}

// startedProcessState is the closed transition vocabulary for launch-time handle ownership.
type startedProcessState uint8

const (
	startedProcessGated startedProcessState = iota
	startedProcessReleased
	startedProcessReleaseFailed
	startedProcessAborting
	startedProcessAborted
	startedProcessCommitted
)

// osStartedProcess owns launch-time runtime handles but no durable identity.
type osStartedProcess struct {
	mu          sync.Mutex
	pid         int
	pidfd       int
	cleanupFD   int
	release     *os.File
	done        <-chan error
	state       startedProcessState
	signalPIDFD func(int) error
	closeFD     func(int) error
	abortDone   chan struct{}
	abortErr    error
}

// PID returns the transient child PID paired with the clone-time pidfd.
func (process *osStartedProcess) PID() int { return process.pid }

// TakePIDFD transfers the clone-time pidfd exactly once for strong evidence capture.
func (process *osStartedProcess) TakePIDFD() (int, error) {
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.state != startedProcessGated && process.state != startedProcessReleased {
		return -1, errors.New("clone-time pidfd cannot transfer in the current launch state")
	}
	if process.pidfd < 0 {
		return -1, errors.New("clone-time pidfd was already transferred")
	}
	fd := process.pidfd
	process.pidfd = -1
	return fd, nil
}

// Release writes the sole authorization byte and closes the parent endpoint so
// the child can distinguish authorization from parent death.
func (process *osStartedProcess) Release() error {
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.state != startedProcessGated || process.release == nil {
		return errors.New("launch release gate is not available in the current state")
	}
	written, writeErr := process.release.Write([]byte{shim.LaunchReleaseByte})
	if writeErr == nil && written != 1 {
		writeErr = io.ErrShortWrite
	}
	closeErr := process.release.Close()
	process.release = nil
	result := errors.Join(writeErr, closeErr)
	if result != nil {
		process.state = startedProcessReleaseFailed
		return result
	}
	process.state = startedProcessReleased
	return nil
}

// Abort closes the unreleased gate, sends SIGKILL through the retained cleanup
// pidfd, and waits for the exact child or caller cancellation.
func (process *osStartedProcess) Abort(ctx context.Context) error {
	if ctx == nil {
		return errors.New("process abort context must not be nil")
	}
	process.mu.Lock()
	switch process.state {
	case startedProcessCommitted:
		process.mu.Unlock()
		return errors.New("committed process cannot be aborted through launch-time handles")
	case startedProcessAborted:
		result := process.abortErr
		process.mu.Unlock()
		return result
	case startedProcessAborting:
		done := process.abortDone
		process.mu.Unlock()
		select {
		case <-done:
			process.mu.Lock()
			result := process.abortErr
			process.mu.Unlock()
			return result
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	process.state = startedProcessAborting
	var releaseErr error
	if process.release != nil {
		releaseErr = process.release.Close()
		process.release = nil
	}
	cleanupFD := process.cleanupFD
	process.cleanupFD = -1
	pidfd := process.pidfd
	process.pidfd = -1
	done := process.done
	signalPIDFD := process.signalPIDFD
	closeFD := process.closeFD
	process.mu.Unlock()
	var signalErr error
	if cleanupFD >= 0 {
		signalErr = signalPIDFD(cleanupFD)
		if errors.Is(signalErr, syscall.ESRCH) {
			signalErr = nil
		}
		signalErr = errors.Join(signalErr, closeFD(cleanupFD))
	}
	if pidfd >= 0 {
		signalErr = errors.Join(signalErr, closeFD(pidfd))
	}
	var result error
	select {
	case waitErr := <-done:
		result = errors.Join(releaseErr, signalErr, normalizeAbortedWait(waitErr))
	case <-ctx.Done():
		result = errors.Join(releaseErr, signalErr, ctx.Err())
	}
	process.mu.Lock()
	process.state = startedProcessAborted
	process.abortErr = result
	close(process.abortDone)
	process.mu.Unlock()
	return result
}

// Commit releases only the cleanup pidfd after journaled readiness; the Wait
// goroutine remains responsible for eventual child reaping.
func (process *osStartedProcess) Commit() error {
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.state == startedProcessCommitted {
		return nil
	}
	if process.state != startedProcessReleased || process.release != nil || process.pidfd >= 0 {
		return errors.New("started process cannot commit outside released state after pidfd transfer")
	}
	process.state = startedProcessCommitted
	if process.cleanupFD < 0 {
		return nil
	}
	err := process.closeFD(process.cleanupFD)
	process.cleanupFD = -1
	return err
}

// normalizeAbortedWait suppresses the expected nonzero wait result after an exact abort.
func normalizeAbortedWait(waitErr error) error {
	if waitErr == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		return nil
	}
	return waitErr
}
