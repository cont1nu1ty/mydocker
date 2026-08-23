package isolation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"
)

// testProcessOwner returns the trusted owner intent used by process-handle scenarios.
func testProcessOwner() ProcessOwner {
	return ProcessOwner{CgroupPath: "/mydocker/sandbox-1/attempt-1", Executable: "/usr/bin/workload"}
}

// TestProcessEvidencePresentTreatsExactMismatchAsAbsence verifies PID reuse is
// verified absence of the persisted process rather than authority over the replacement.
func TestProcessEvidencePresentTreatsExactMismatchAsAbsence(t *testing.T) {
	ops := newFakeOps()
	expected := ProcessEvidence{PID: 123, BootID: strings.TrimSpace(string(ops.files[bootIDPath])), StartTime: 777, CgroupPath: testProcessOwner().CgroupPath, Executable: testProcessOwner().Executable}
	ops.setProcessEvidence(123, 778, testProcessOwner().CgroupPath, testProcessOwner().Executable)
	present, err := ProcessEvidencePresent(context.Background(), ops, expected)
	if err != nil || present {
		t.Fatalf("ProcessEvidencePresent()=(%v,%v), want verified absence", present, err)
	}
}

// TestProcessHandleWaitForExitUsesExactPidfd verifies cleanup completes only
// after the retained pidfd reports kernel ESRCH and sends no nonzero signal itself.
func TestProcessHandleWaitForExitUsesExactPidfd(t *testing.T) {
	ops := newFakeOps()
	handle := captureTestProcess(t, ops)
	ops.fail["pidfd-signal:0"] = syscall.ESRCH
	if err := handle.WaitForExit(context.Background(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

// captureTestProcess opens a verified fake process handle and registers cleanup for the test.
func captureTestProcess(t *testing.T, ops *fakeOps) *ProcessHandle {
	t.Helper()
	handle, err := CaptureProcessHandle(context.Background(), ops, 123, testProcessOwner())
	if err != nil {
		t.Fatalf("CaptureProcessHandle() error = %v", err)
	}
	t.Cleanup(func() {
		if err := handle.Close(); err != nil {
			t.Errorf("ProcessHandle.Close() error = %v", err)
		}
	})
	return handle
}

// TestProcessHandleSignalReverifies verifies a nonzero signal follows a fresh strong-identity check and uses pidfd.
func TestProcessHandleSignalReverifies(t *testing.T) {
	ops := newFakeOps()
	handle := captureTestProcess(t, ops)
	if err := handle.Signal(context.Background(), 15); err != nil {
		t.Fatalf("Signal() error = %v", err)
	}
	if !containsCall(ops.mutations, ":15") {
		t.Fatalf("Signal() mutations = %v, want pidfd signal 15", ops.mutations)
	}
	if containsCall(ops.mutations, "setns") || containsCall(ops.mutations, "mount") {
		t.Fatalf("Signal() unrelated mutations = %v", ops.mutations)
	}
}

// TestProcessHandleRejectsDrift verifies start-time reuse is detected before a nonzero signal can be sent.
func TestProcessHandleRejectsDrift(t *testing.T) {
	ops := newFakeOps()
	handle := captureTestProcess(t, ops)
	ops.setProcessEvidence(123, 778, testProcessOwner().CgroupPath, testProcessOwner().Executable)
	err := handle.Signal(context.Background(), 9)
	if !errors.Is(err, ErrUnsafeIdentity) {
		t.Fatalf("Signal() error = %v, want ErrUnsafeIdentity", err)
	}
	if containsCall(ops.mutations, ":9") {
		t.Fatalf("Signal() sent after identity drift: %v", ops.mutations)
	}
}

// TestProcessHandleVerifiedPIDSatisfiesAttachmentBoundary verifies a transient PID is returned only after fresh strong evidence.
func TestProcessHandleVerifiedPIDSatisfiesAttachmentBoundary(t *testing.T) {
	ops := newFakeOps()
	handle := captureTestProcess(t, ops)
	pid, err := handle.VerifiedPID(context.Background())
	if err != nil || pid != 123 {
		t.Fatalf("VerifiedPID() = (%d, %v), want (123, nil)", pid, err)
	}
	ops.setProcessEvidence(123, 778, testProcessOwner().CgroupPath, testProcessOwner().Executable)
	if _, err := handle.VerifiedPID(context.Background()); !errors.Is(err, ErrUnsafeIdentity) {
		t.Fatalf("VerifiedPID(stale) error = %v, want ErrUnsafeIdentity", err)
	}
}

// TestProcessHandleVerifiedPIDRejectsClosed verifies a released pidfd cannot yield even a transient attachment PID.
func TestProcessHandleVerifiedPIDRejectsClosed(t *testing.T) {
	ops := newFakeOps()
	handle := captureTestProcess(t, ops)
	if err := handle.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := handle.VerifiedPID(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("VerifiedPID(closed) error = %v, want ErrClosed", err)
	}
}

// TestCaptureProcessHandleRejectsWrongOwner verifies arbitrary processes cannot become owned by self-observation alone.
func TestCaptureProcessHandleRejectsWrongOwner(t *testing.T) {
	ops := newFakeOps()
	_, err := CaptureProcessHandle(context.Background(), ops, 123, ProcessOwner{
		CgroupPath: "/mydocker/other", Executable: "/usr/bin/workload",
	})
	if !errors.Is(err, ErrUnsafeIdentity) {
		t.Fatalf("CaptureProcessHandle() error = %v, want ErrUnsafeIdentity", err)
	}
	if containsCall(ops.calls, "pidfd-open") {
		t.Fatalf("CaptureProcessHandle() opened pidfd for wrong owner: %v", ops.calls)
	}
}

// TestCaptureProcessHandleFromPIDFDAdoptsAtomicLaunchHandle verifies a
// clone-time pidfd is used directly, strongly checked, and not reopened.
func TestCaptureProcessHandleFromPIDFDAdoptsAtomicLaunchHandle(t *testing.T) {
	ops := newFakeOps()
	const pidfd = 77
	ops.files[fmt.Sprintf("/proc/self/fdinfo/%d", pidfd)] = []byte("pos:\t0\nPid:\t123\nNSpid:\t123\n")
	handle, err := CaptureProcessHandleFromPIDFD(context.Background(), ops, 123, pidfd, testProcessOwner())
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range ops.calls {
		if strings.HasPrefix(call, "pidfd-open:") {
			t.Fatalf("atomic launch pidfd was replaced through %q", call)
		}
	}
	if err := handle.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	if !containsCall(ops.calls, fmt.Sprintf("close:%d", pidfd)) {
		t.Fatal("adopted launch pidfd was not closed with its ProcessHandle")
	}
}

// TestCaptureProcessHandleFromPIDFDExecutableRetainsObservedCgroup verifies the
// clone-time path captures exact pidfd/PID/starttime/executable evidence while
// leaving manager membership authorization to its required caller.
func TestCaptureProcessHandleFromPIDFDExecutableRetainsObservedCgroup(t *testing.T) {
	ops := newFakeOps()
	const pidfd = 78
	ops.files[fmt.Sprintf("/proc/self/fdinfo/%d", pidfd)] = []byte("pos:\t0\nPid:\t123\nNSpid:\t123\n")
	handle, err := CaptureProcessHandleFromPIDFDExecutable(context.Background(), ops, 123, pidfd, "/usr/bin/workload")
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	evidence, err := handle.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	if evidence.PID != 123 || evidence.StartTime != 777 || evidence.CgroupPath != "/mydocker/sandbox-1/attempt-1" || evidence.Executable != "/usr/bin/workload" {
		t.Fatalf("captured evidence=%+v", evidence)
	}
}

// TestCaptureProcessHandleFromPIDFDFailureCloses verifies stale or wrong-owner
// launch descriptors cannot leak or become process authority.
func TestCaptureProcessHandleFromPIDFDFailureCloses(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fakeOps)
		owner  ProcessOwner
	}{
		{name: "wrong owner", owner: ProcessOwner{CgroupPath: "/mydocker/another", Executable: "/usr/bin/workload"}},
		{name: "stale pidfd", owner: testProcessOwner(), mutate: func(ops *fakeOps) {
			ops.fail["pidfd-signal:0"] = errors.New("stale pidfd")
		}},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ops := newFakeOps()
			if test.mutate != nil {
				test.mutate(ops)
			}
			pidfd := 90 + index
			ops.files[fmt.Sprintf("/proc/self/fdinfo/%d", pidfd)] = []byte("Pid:\t123\n")
			if _, err := CaptureProcessHandleFromPIDFD(context.Background(), ops, 123, pidfd, test.owner); !errors.Is(err, ErrUnsafeIdentity) {
				t.Fatalf("error = %v, want ErrUnsafeIdentity", err)
			}
			if !containsCall(ops.calls, fmt.Sprintf("close:%d", pidfd)) {
				t.Fatal("rejected launch pidfd was not closed")
			}
		})
	}
}

// TestCaptureProcessHandleFromPIDFDRejectsMismatchedPair verifies two live but
// unrelated PID and pidfd inputs cannot be combined into signal authority.
func TestCaptureProcessHandleFromPIDFDRejectsMismatchedPair(t *testing.T) {
	ops := newFakeOps()
	const pidfd = 78
	ops.files[fmt.Sprintf("/proc/self/fdinfo/%d", pidfd)] = []byte("Pid:\t124\nNSpid:\t124\n")
	if _, err := CaptureProcessHandleFromPIDFD(context.Background(), ops, 123, pidfd, testProcessOwner()); !errors.Is(err, ErrUnsafeIdentity) {
		t.Fatalf("error = %v, want ErrUnsafeIdentity", err)
	}
	if !containsCall(ops.calls, fmt.Sprintf("close:%d", pidfd)) {
		t.Fatal("mismatched pidfd was not closed")
	}
}

// TestParsePIDFDInfoRejectsNonPIDFDAndAmbiguity verifies descriptor metadata
// without one exact positive Pid field cannot authorize process ownership.
func TestParsePIDFDInfoRejectsNonPIDFDAndAmbiguity(t *testing.T) {
	for _, payload := range []string{
		"pos:\t0\nflags:\t02\n",
		"Pid:\t-1\nNSpid:\t-1\n",
		"Pid:\t123\nPid:\t124\n",
		"Pid:\t123 extra\n",
	} {
		if _, err := parsePIDFDInfo([]byte(payload)); err == nil {
			t.Fatalf("parsePIDFDInfo(%q) error = nil", payload)
		}
	}
}

// TestRestoreProcessHandleRejectsBootChange verifies persisted evidence cannot cross a host reboot.
func TestRestoreProcessHandleRejectsBootChange(t *testing.T) {
	ops := newFakeOps()
	handle := captureTestProcess(t, ops)
	evidence, err := handle.Evidence()
	if err != nil {
		t.Fatalf("Evidence() error = %v", err)
	}
	ops.files[bootIDPath] = []byte("different-boot\n")
	_, err = RestoreProcessHandle(context.Background(), ops, evidence)
	if !errors.Is(err, ErrUnsafeIdentity) {
		t.Fatalf("RestoreProcessHandle() error = %v, want ErrUnsafeIdentity", err)
	}
}

// TestParseProcStatStartTime verifies comm whitespace and parentheses cannot shift field 22 parsing.
func TestParseProcStatStartTime(t *testing.T) {
	value, err := parseProcStatStartTime([]byte(fakeProcStat(55, "name with ) parens", 123456)))
	if err != nil || value != 123456 {
		t.Fatalf("parseProcStatStartTime() = (%d, %v), want (123456, nil)", value, err)
	}
	if _, err := parseProcStatStartTime([]byte("55 broken")); err == nil {
		t.Fatal("parseProcStatStartTime() accepted malformed input")
	}
}

// containsCall reports whether any recorded fake operation contains the requested fragment.
func containsCall(calls []string, fragment string) bool {
	for _, call := range calls {
		if strings.Contains(call, fragment) {
			return true
		}
	}
	return false
}
