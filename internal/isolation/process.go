package isolation

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
)

const bootIDPath = "/proc/sys/kernel/random/boot_id"

// ProcessOwner is trusted provider intent used to reject capturing an unrelated process.
type ProcessOwner struct {
	CgroupPath string `json:"cgroup_path"`
	Executable string `json:"executable"`
}

// Validate rejects ownership intent that cannot be matched exactly against procfs evidence.
func (o ProcessOwner) Validate() error {
	if cleanCgroupPath(o.CgroupPath) != o.CgroupPath || o.CgroupPath == "/" {
		return fmt.Errorf("%w: expected cgroup path must be a clean non-root absolute path", ErrUnsafeIdentity)
	}
	if !strings.HasPrefix(o.Executable, "/") || strings.ContainsRune(o.Executable, '\x00') || path.Clean(o.Executable) != o.Executable {
		return fmt.Errorf("%w: expected executable must be a clean absolute path", ErrUnsafeIdentity)
	}
	return nil
}

// ProcessEvidence is the serializable identity needed to re-open and reverify one owned process.
// The pidfd itself is deliberately runtime-only and is never a persistence format.
type ProcessEvidence struct {
	PID        int    `json:"pid"`
	BootID     string `json:"boot_id"`
	StartTime  uint64 `json:"start_time_ticks"`
	CgroupPath string `json:"cgroup_path"`
	Executable string `json:"executable"`
}

// Validate rejects incomplete evidence before it can authorize a pidfd lookup.
func (e ProcessEvidence) Validate() error {
	if e.PID <= 0 || e.StartTime == 0 {
		return fmt.Errorf("%w: PID and proc start time must be positive", ErrUnsafeIdentity)
	}
	if err := validateBootID(e.BootID); err != nil {
		return err
	}
	return (ProcessOwner{CgroupPath: e.CgroupPath, Executable: e.Executable}).Validate()
}

// ProcessHandle couples runtime-only pidfd ownership with serializable, action-time procfs evidence.
type ProcessHandle struct {
	mu       sync.Mutex
	ops      Ops
	pidfd    int
	evidence ProcessEvidence
	closed   bool
}

// CaptureProcessHandle opens a new pidfd only after current procfs evidence matches trusted owner intent.
func CaptureProcessHandle(ctx context.Context, ops Ops, pid int, owner ProcessOwner) (*ProcessHandle, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if err := requireOps(ops); err != nil {
		return nil, err
	}
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	first, err := readProcessEvidence(ctx, ops, pid)
	if err != nil {
		return nil, err
	}
	if first.CgroupPath != owner.CgroupPath || first.Executable != owner.Executable {
		return nil, fmt.Errorf("%w: process does not match expected cgroup and executable owner", ErrUnsafeIdentity)
	}
	return openVerifiedProcessHandle(ctx, ops, first)
}

// CaptureProcessHandleExecutable opens a pidfd around two exact evidence reads
// and verifies the expected executable for SO_PEERCRED recovery. Callers must
// immediately confirm exact cgroup-manager membership before persistence or action.
func CaptureProcessHandleExecutable(ctx context.Context, ops Ops, pid int, executable string) (*ProcessHandle, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if err := requireOps(ops); err != nil {
		return nil, err
	}
	if err := (ProcessOwner{CgroupPath: "/placeholder", Executable: executable}).Validate(); err != nil {
		return nil, err
	}
	evidence, err := readProcessEvidence(ctx, ops, pid)
	if err != nil {
		return nil, err
	}
	if evidence.Executable != executable {
		return nil, fmt.Errorf("%w: process executable differs from recovery intent", ErrUnsafeIdentity)
	}
	return openVerifiedProcessHandle(ctx, ops, evidence)
}

// CaptureProcessHandleFromPIDFD adopts the pidfd returned atomically by a
// cgroup-at-fork process launch, matches current procfs evidence to trusted
// owner intent, and closes the supplied descriptor on every failure. On
// success the returned handle exclusively owns the descriptor.
func CaptureProcessHandleFromPIDFD(ctx context.Context, ops Ops, pid, pidfd int, owner ProcessOwner) (_ *ProcessHandle, resultErr error) {
	if err := requireOps(ops); err != nil {
		return nil, err
	}
	if pidfd < 0 {
		return nil, fmt.Errorf("%w: pidfd must be non-negative", ErrUnsafeIdentity)
	}
	transferred := false
	defer func() {
		if !transferred {
			resultErr = errors.Join(resultErr, ops.Close(pidfd))
		}
	}()
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	boundPID, err := readPIDFDProcessID(ctx, ops, pidfd)
	if err != nil {
		return nil, err
	}
	if boundPID != pid {
		return nil, fmt.Errorf("%w: pidfd identifies PID %d instead of requested PID %d", ErrUnsafeIdentity, boundPID, pid)
	}
	evidence, err := readProcessEvidence(ctx, ops, pid)
	if err != nil {
		return nil, err
	}
	if evidence.CgroupPath != owner.CgroupPath || evidence.Executable != owner.Executable {
		return nil, fmt.Errorf("%w: process does not match expected cgroup and executable owner", ErrUnsafeIdentity)
	}
	handle := &ProcessHandle{ops: ops, pidfd: pidfd, evidence: evidence}
	if err := handle.Verify(ctx); err != nil {
		return nil, err
	}
	transferred = true
	return handle, nil
}

// CaptureProcessHandleFromPIDFDExecutable adopts an atomic clone-time pidfd and
// verifies its PID plus executable while preserving the observed cgroup path in
// evidence. Callers must immediately confirm exact manager membership before
// persisting or acting on the handle; this function alone does not authorize a cgroup.
func CaptureProcessHandleFromPIDFDExecutable(ctx context.Context, ops Ops, pid, pidfd int, executable string) (_ *ProcessHandle, resultErr error) {
	if err := requireOps(ops); err != nil {
		return nil, err
	}
	if pidfd < 0 {
		return nil, fmt.Errorf("%w: pidfd must be non-negative", ErrUnsafeIdentity)
	}
	transferred := false
	defer func() {
		if !transferred {
			resultErr = errors.Join(resultErr, ops.Close(pidfd))
		}
	}()
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if err := (ProcessOwner{CgroupPath: "/placeholder", Executable: executable}).Validate(); err != nil {
		return nil, err
	}
	boundPID, err := readPIDFDProcessID(ctx, ops, pidfd)
	if err != nil {
		return nil, err
	}
	if boundPID != pid {
		return nil, fmt.Errorf("%w: pidfd identifies PID %d instead of requested PID %d", ErrUnsafeIdentity, boundPID, pid)
	}
	evidence, err := readProcessEvidence(ctx, ops, pid)
	if err != nil {
		return nil, err
	}
	if evidence.Executable != executable {
		return nil, fmt.Errorf("%w: process executable differs from launch intent", ErrUnsafeIdentity)
	}
	handle := &ProcessHandle{ops: ops, pidfd: pidfd, evidence: evidence}
	if err := handle.Verify(ctx); err != nil {
		return nil, err
	}
	transferred = true
	return handle, nil
}

// RestoreProcessHandle reopens a pidfd from persisted evidence and fails if any identity component changed.
func RestoreProcessHandle(ctx context.Context, ops Ops, evidence ProcessEvidence) (*ProcessHandle, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if err := requireOps(ops); err != nil {
		return nil, err
	}
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	return openVerifiedProcessHandle(ctx, ops, evidence)
}

// ProcessEvidencePresent distinguishes the exact persisted process from
// verified exit/PID reuse without treating ambiguous procfs failures as absence.
func ProcessEvidencePresent(ctx context.Context, ops Ops, expected ProcessEvidence) (bool, error) {
	if err := validateContext(ctx); err != nil {
		return false, err
	}
	if err := requireOps(ops); err != nil {
		return false, err
	}
	if err := expected.Validate(); err != nil {
		return false, err
	}
	current, err := readProcessEvidence(ctx, ops, expected.PID)
	if err != nil {
		if processDefinitelyAbsent(err) {
			return false, nil
		}
		return false, err
	}
	if current != expected {
		return false, nil
	}
	handle, err := openVerifiedProcessHandle(ctx, ops, expected)
	if err == nil {
		return true, handle.Close()
	}
	current, retryErr := readProcessEvidence(ctx, ops, expected.PID)
	if processDefinitelyAbsent(retryErr) || (retryErr == nil && current != expected) {
		return false, nil
	}
	if retryErr != nil {
		return false, retryErr
	}
	return false, err
}

// processDefinitelyAbsent recognizes only kernel absence results; permission,
// malformed procfs, and other observation failures remain unknown.
func processDefinitelyAbsent(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH) || errors.Is(err, syscall.ENOENT)
}

// openVerifiedProcessHandle opens pidfd between two evidence reads to close PID-reuse races.
func openVerifiedProcessHandle(ctx context.Context, ops Ops, expected ProcessEvidence) (*ProcessHandle, error) {
	pidfd, err := ops.PidfdOpen(expected.PID)
	if err != nil {
		return nil, fmt.Errorf("open pidfd for PID %d: %w", expected.PID, err)
	}
	boundPID, err := readPIDFDProcessID(ctx, ops, pidfd)
	if err != nil {
		_ = ops.Close(pidfd)
		return nil, err
	}
	if boundPID != expected.PID {
		_ = ops.Close(pidfd)
		return nil, fmt.Errorf("%w: opened pidfd identifies PID %d instead of expected PID %d", ErrUnsafeIdentity, boundPID, expected.PID)
	}
	handle := &ProcessHandle{ops: ops, pidfd: pidfd, evidence: expected}
	if err := handle.Verify(ctx); err != nil {
		_ = ops.Close(pidfd)
		return nil, err
	}
	return handle, nil
}

// readPIDFDProcessID proves a supplied descriptor is a pidfd for one positive
// PID in the caller's PID namespace before it can be paired with procfs
// evidence or authorize a signal.
func readPIDFDProcessID(ctx context.Context, ops Ops, pidfd int) (int, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if pidfd < 0 {
		return 0, fmt.Errorf("%w: pidfd must be non-negative", ErrUnsafeIdentity)
	}
	payload, err := ops.ReadFile(fmt.Sprintf("/proc/self/fdinfo/%d", pidfd))
	if err != nil {
		return 0, fmt.Errorf("%w: read pidfd identity: %v", ErrUnsafeIdentity, err)
	}
	pid, err := parsePIDFDInfo(payload)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrUnsafeIdentity, err)
	}
	return pid, nil
}

// parsePIDFDInfo accepts exactly one positive kernel Pid field and rejects
// ordinary descriptors, pidfds outside the caller namespace, and ambiguity.
func parsePIDFDInfo(payload []byte) (int, error) {
	found := false
	pid := 0
	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "Pid:" {
			continue
		}
		if found || len(fields) != 2 {
			return 0, fmt.Errorf("pidfd info has an ambiguous Pid field")
		}
		parsed, err := strconv.Atoi(fields[1])
		if err != nil || parsed <= 0 {
			return 0, fmt.Errorf("pidfd info has an invalid Pid field")
		}
		found = true
		pid = parsed
	}
	if !found {
		return 0, fmt.Errorf("descriptor info has no pidfd Pid field")
	}
	return pid, nil
}

// Evidence returns the immutable serializable identity associated with this runtime pidfd.
func (h *ProcessHandle) Evidence() (ProcessEvidence, error) {
	if h == nil {
		return ProcessEvidence{}, fmt.Errorf("%w: nil process handle", ErrClosed)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ProcessEvidence{}, ErrClosed
	}
	return h.evidence, nil
}

// Verify rechecks pidfd liveness plus boot ID, start time, cgroup, and executable immediately before use.
func (h *ProcessHandle) Verify(ctx context.Context) error {
	if h == nil {
		return fmt.Errorf("%w: nil process handle", ErrClosed)
	}
	if err := validateContext(ctx); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.verifyLocked(ctx)
}

// VerifiedPID performs an action-time strong identity check and returns the transient PID for an immediate cgroup attachment.
// Callers must not persist or reuse the numeric value as process authority.
func (h *ProcessHandle) VerifiedPID(ctx context.Context) (int, error) {
	if h == nil {
		return 0, fmt.Errorf("%w: nil process handle", ErrClosed)
	}
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.verifyLocked(ctx); err != nil {
		return 0, err
	}
	return h.evidence.PID, nil
}

// verifyLocked performs action-time verification while the pidfd cannot be concurrently closed.
func (h *ProcessHandle) verifyLocked(ctx context.Context) error {
	if h.closed {
		return ErrClosed
	}
	if err := h.ops.PidfdSendSignal(h.pidfd, 0); err != nil {
		return fmt.Errorf("%w: pidfd is no longer live: %v", ErrUnsafeIdentity, err)
	}
	current, err := readProcessEvidence(ctx, h.ops, h.evidence.PID)
	if err != nil {
		return err
	}
	if current != h.evidence {
		return fmt.Errorf("%w: process evidence changed", ErrUnsafeIdentity)
	}
	return nil
}

// Signal reverifies ownership under the handle lock and then signals only through pidfd.
func (h *ProcessHandle) Signal(ctx context.Context, signal int) error {
	if h == nil {
		return fmt.Errorf("%w: nil process handle", ErrClosed)
	}
	if signal <= 0 || signal > 64 {
		return fmt.Errorf("signal must be between 1 and 64")
	}
	if err := validateContext(ctx); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.verifyLocked(ctx); err != nil {
		return err
	}
	if err := h.ops.PidfdSendSignal(h.pidfd, signal); err != nil {
		return fmt.Errorf("pidfd signal %d: %w", signal, err)
	}
	return nil
}

// WaitForExit waits until signal-zero on this exact pidfd reports ESRCH,
// allowing cleanup to prove termination without trusting PID reuse or child wait ownership.
func (h *ProcessHandle) WaitForExit(ctx context.Context, pollInterval time.Duration) error {
	if h == nil {
		return fmt.Errorf("%w: nil process handle", ErrClosed)
	}
	if err := validateContext(ctx); err != nil {
		return err
	}
	if pollInterval <= 0 {
		return errors.New("process exit poll interval must be positive")
	}
	for {
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			return ErrClosed
		}
		err := h.ops.PidfdSendSignal(h.pidfd, 0)
		h.mu.Unlock()
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("observe pidfd exit: %w", err)
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Close releases the runtime pidfd exactly once; persisted evidence remains a separate caller value.
func (h *ProcessHandle) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	if err := h.ops.Close(h.pidfd); err != nil {
		return fmt.Errorf("close pidfd: %w", err)
	}
	return nil
}

// readProcessEvidence reads every strong identity component without creating or mutating host resources.
func readProcessEvidence(ctx context.Context, ops Ops, pid int) (ProcessEvidence, error) {
	if pid <= 0 {
		return ProcessEvidence{}, fmt.Errorf("%w: PID must be positive", ErrUnsafeIdentity)
	}
	if err := validateContext(ctx); err != nil {
		return ProcessEvidence{}, err
	}
	bootBytes, err := ops.ReadFile(bootIDPath)
	if err != nil {
		return ProcessEvidence{}, fmt.Errorf("read boot ID: %w", err)
	}
	bootID := strings.TrimSpace(string(bootBytes))
	if err := validateBootID(bootID); err != nil {
		return ProcessEvidence{}, err
	}
	statBytes, err := ops.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return ProcessEvidence{}, fmt.Errorf("read process stat: %w", err)
	}
	startTime, err := parseProcStatStartTime(statBytes)
	if err != nil {
		return ProcessEvidence{}, fmt.Errorf("%w: %v", ErrUnsafeIdentity, err)
	}
	cgroupBytes, err := ops.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return ProcessEvidence{}, fmt.Errorf("read process cgroup: %w", err)
	}
	cgroupPath, err := parseUnifiedCgroup(cgroupBytes)
	if err != nil {
		return ProcessEvidence{}, fmt.Errorf("%w: %v", ErrUnsafeIdentity, err)
	}
	executable, err := ops.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return ProcessEvidence{}, fmt.Errorf("read process executable: %w", err)
	}
	evidence := ProcessEvidence{PID: pid, BootID: bootID, StartTime: startTime, CgroupPath: cgroupPath, Executable: executable}
	if err := evidence.Validate(); err != nil {
		return ProcessEvidence{}, err
	}
	return evidence, nil
}

// parseProcStatStartTime extracts field 22 without being confused by spaces or parentheses in comm.
func parseProcStatStartTime(value []byte) (uint64, error) {
	text := strings.TrimSpace(string(value))
	close := strings.LastIndex(text, ")")
	open := strings.Index(text, "(")
	if open <= 0 || close <= open || close+1 >= len(text) {
		return 0, fmt.Errorf("malformed /proc stat")
	}
	fields := strings.Fields(text[close+1:])
	if len(fields) <= 19 {
		return 0, fmt.Errorf("/proc stat has %d post-comm fields, need at least 20", len(fields))
	}
	startTime, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || startTime == 0 {
		return 0, fmt.Errorf("invalid /proc start time")
	}
	return startTime, nil
}

// parseUnifiedCgroup accepts exactly one cgroup-v2 membership line and returns its clean absolute path.
func parseUnifiedCgroup(value []byte) (string, error) {
	lines := strings.Fields(string(value))
	if len(lines) != 1 {
		return "", fmt.Errorf("expected one unified cgroup membership, got %d", len(lines))
	}
	parts := strings.SplitN(lines[0], ":", 3)
	if len(parts) != 3 || parts[0] != "0" || parts[1] != "" {
		return "", fmt.Errorf("process is not in a single cgroup-v2 membership")
	}
	clean := cleanCgroupPath(parts[2])
	if clean == "" || clean != parts[2] {
		return "", fmt.Errorf("invalid cgroup-v2 path")
	}
	return clean, nil
}

// cleanCgroupPath returns a clean absolute cgroup path or an empty string when invalid.
func cleanCgroupPath(value string) string {
	if !strings.HasPrefix(value, "/") || strings.ContainsRune(value, '\x00') {
		return ""
	}
	return path.Clean(value)
}

// validateBootID rejects missing or unsafe boot-identity evidence.
func validateBootID(value string) error {
	if value == "" || len(value) > 128 {
		return fmt.Errorf("%w: boot ID is empty or too long", ErrUnsafeIdentity)
	}
	for _, character := range value {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return fmt.Errorf("%w: boot ID contains whitespace or control characters", ErrUnsafeIdentity)
		}
	}
	return nil
}
