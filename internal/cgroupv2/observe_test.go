package cgroupv2

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	"mydocker/internal/domain"
)

// createTestAttempt prepares one fake Sandbox/Attempt hierarchy for observation and cleanup scenarios.
func createTestAttempt(t *testing.T) (*Manager, *fakeFileSystem, domain.SandboxID, domain.AttemptID, string, string) {
	t.Helper()
	manager, fake := newFakeManager(t)
	sandboxID := domain.SandboxID("sandbox-observe")
	attemptID := domain.AttemptID("attempt-observe")
	sandbox, err := manager.CreateSandbox(context.Background(), sandboxID)
	if err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	attempt, _, err := manager.CreateAttempt(context.Background(), sandboxID, attemptID, resolveTestLimits(t, domain.Resources{}))
	if err != nil {
		t.Fatalf("CreateAttempt() error = %v", err)
	}
	return manager, fake, sandboxID, attemptID, sandbox.Path, attempt.Path
}

// TestAttemptObservationsAndOOMDelta verifies deterministic membership, current/events parsing, and local OOM attribution.
func TestAttemptObservationsAndOOMDelta(t *testing.T) {
	manager, fake, sandboxID, attemptID, _, attemptPath := createTestAttempt(t)
	fake.setFile(filepath.Join(attemptPath, "cgroup.procs"), "41\n7\n29\n")
	fake.setFile(filepath.Join(attemptPath, "memory.current"), "8192\n")
	fake.setFile(filepath.Join(attemptPath, "pids.current"), "3\n")
	fake.setFile(filepath.Join(attemptPath, "cgroup.events"), "populated 1\nfrozen 1\n")
	fake.setFile(filepath.Join(attemptPath, "memory.events.local"), "low 4\noom 5\noom_kill 2\noom_group_kill 1\n")

	observation, err := manager.ObserveAttempt(context.Background(), sandboxID, attemptID)
	if err != nil {
		t.Fatalf("ObserveAttempt() error = %v", err)
	}
	want := AttemptObservation{
		Membership: []int{7, 29, 41},
		Current:    Current{MemoryBytes: 8192, Pids: 3},
		Events:     Events{Populated: true, Frozen: true},
		OOM:        OOMSnapshot{OOM: 5, OOMKill: 2, OOMGroupKill: 1},
	}
	if !reflect.DeepEqual(observation, want) {
		t.Fatalf("ObserveAttempt() = %+v, want %+v", observation, want)
	}

	earlier := OOMSnapshot{OOM: 3, OOMKill: 1}
	delta, err := observation.OOM.Delta(earlier)
	if err != nil {
		t.Fatalf("OOMSnapshot.Delta() error = %v", err)
	}
	if delta != (OOMDelta{OOM: 2, OOMKill: 1, OOMGroupKill: 1}) || !delta.Killed() {
		t.Fatalf("OOM delta = %+v, Killed = %v", delta, delta.Killed())
	}
	if _, err := earlier.Delta(observation.OOM); !errors.Is(err, ErrUnknownState) {
		t.Fatalf("regressed OOM Delta() error = %v, want ErrUnknownState", err)
	}
}

// TestObservationMethodsExposeEachBoundedView verifies individual read APIs use the same parsed semantics as the aggregate view.
func TestObservationMethodsExposeEachBoundedView(t *testing.T) {
	manager, fake, sandboxID, attemptID, _, attemptPath := createTestAttempt(t)
	fake.setFile(filepath.Join(attemptPath, "cgroup.procs"), "9\n2\n")
	fake.setFile(filepath.Join(attemptPath, "memory.current"), "1000\n")
	fake.setFile(filepath.Join(attemptPath, "pids.current"), "2\n")
	fake.setFile(filepath.Join(attemptPath, "cgroup.events"), "populated 1\n")
	fake.setFile(filepath.Join(attemptPath, "memory.events.local"), "oom 6\noom_kill 4\n")

	members, err := manager.Membership(context.Background(), sandboxID, attemptID)
	if err != nil || !reflect.DeepEqual(members, []int{2, 9}) {
		t.Fatalf("Membership() = %v, %v", members, err)
	}
	current, err := manager.ReadCurrent(context.Background(), sandboxID, attemptID)
	if err != nil || current != (Current{MemoryBytes: 1000, Pids: 2}) {
		t.Fatalf("ReadCurrent() = %+v, %v", current, err)
	}
	events, err := manager.ReadEvents(context.Background(), sandboxID, attemptID)
	if err != nil || events != (Events{Populated: true}) {
		t.Fatalf("ReadEvents() = %+v, %v", events, err)
	}
	oom, err := manager.SnapshotOOM(context.Background(), sandboxID, attemptID)
	if err != nil || oom != (OOMSnapshot{OOM: 6, OOMKill: 4}) {
		t.Fatalf("SnapshotOOM() = %+v, %v", oom, err)
	}
	effective, err := manager.ReadEffectiveLimits(context.Background(), sandboxID, attemptID)
	if err != nil || !effective.Equal(EffectiveLimits{
		CPU:    CPUMax{Unlimited: true, PeriodMicros: CPUPeriodMicros},
		Memory: ScalarLimit{Unlimited: true},
		Pids:   ScalarLimit{Value: DefaultPidsLimit},
	}) {
		t.Fatalf("ReadEffectiveLimits() = %+v, %v", effective, err)
	}
}

// TestMalformedObservationsFailUnknown verifies unsafe or contradictory pseudo-file evidence never becomes trusted state.
func TestMalformedObservationsFailUnknown(t *testing.T) {
	tests := []struct {
		name   string
		file   string
		value  string
		invoke func(*Manager, domain.SandboxID, domain.AttemptID) error
	}{
		{
			name:  "wrong CPU period",
			file:  "cpu.max",
			value: "1000 99999\n",
			invoke: func(manager *Manager, sandboxID domain.SandboxID, attemptID domain.AttemptID) error {
				_, err := manager.ReadEffectiveLimits(context.Background(), sandboxID, attemptID)
				return err
			},
		},
		{
			name:  "duplicate membership",
			file:  "cgroup.procs",
			value: "4\n4\n",
			invoke: func(manager *Manager, sandboxID domain.SandboxID, attemptID domain.AttemptID) error {
				_, err := manager.Membership(context.Background(), sandboxID, attemptID)
				return err
			},
		},
		{
			name:  "invalid current",
			file:  "memory.current",
			value: "not-a-number\n",
			invoke: func(manager *Manager, sandboxID domain.SandboxID, attemptID domain.AttemptID) error {
				_, err := manager.ReadCurrent(context.Background(), sandboxID, attemptID)
				return err
			},
		},
		{
			name:  "missing populated",
			file:  "cgroup.events",
			value: "frozen 0\n",
			invoke: func(manager *Manager, sandboxID domain.SandboxID, attemptID domain.AttemptID) error {
				_, err := manager.ReadEvents(context.Background(), sandboxID, attemptID)
				return err
			},
		},
		{
			name:  "missing OOM kill",
			file:  "memory.events.local",
			value: "oom 1\n",
			invoke: func(manager *Manager, sandboxID domain.SandboxID, attemptID domain.AttemptID) error {
				_, err := manager.SnapshotOOM(context.Background(), sandboxID, attemptID)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager, fake, sandboxID, attemptID, _, attemptPath := createTestAttempt(t)
			fake.setFile(filepath.Join(attemptPath, test.file), test.value)
			if err := test.invoke(manager, sandboxID, attemptID); !errors.Is(err, ErrUnknownState) {
				t.Fatalf("observation error = %v, want ErrUnknownState", err)
			}
		})
	}
}

type fakeProcessReference struct {
	pid           int
	err           error
	verifications []fakeProcessVerification
	calls         int
}

type fakeProcessVerification struct {
	pid int
	err error
}

// VerifiedPID returns sequential action-time test evidence and records every identity verification without reading a persisted raw PID.
func (p *fakeProcessReference) VerifiedPID(context.Context) (int, error) {
	call := p.calls
	p.calls++
	if len(p.verifications) != 0 {
		if call >= len(p.verifications) {
			return 0, errors.New("unexpected extra process identity verification")
		}
		verification := p.verifications[call]
		return verification.pid, verification.err
	}
	return p.pid, p.err
}

// TestAttachProcessAndOpenAttempt verifies membership confirmation is read-only and directory handles remain runtime-only.
func TestAttachProcessAndOpenAttempt(t *testing.T) {
	manager, fake, sandboxID, attemptID, _, attemptPath := createTestAttempt(t)
	fake.setFile(filepath.Join(attemptPath, "cgroup.procs"), "42\n")
	writesBefore := len(fake.writes)
	reference := &fakeProcessReference{pid: 42}
	if err := manager.AttachProcess(context.Background(), sandboxID, attemptID, reference); err != nil {
		t.Fatalf("AttachProcess() error = %v", err)
	}
	if reference.calls != 2 {
		t.Fatalf("AttachProcess() identity verifications = %d, want 2", reference.calls)
	}
	if got := string(fake.files[filepath.Join(attemptPath, "cgroup.procs")]); got != "42\n" {
		t.Fatalf("cgroup.procs = %q, want 42", got)
	}
	if len(fake.writes) != writesBefore {
		t.Fatal("AttachProcess() migrated a process instead of confirming membership")
	}

	handle, err := manager.OpenAttempt(context.Background(), sandboxID, attemptID)
	if err != nil {
		t.Fatalf("OpenAttempt() error = %v", err)
	}
	if handle.Fd() == 0 {
		t.Fatal("OpenAttempt() returned descriptor zero")
	}
	fakeHandle, ok := handle.(*fakeDirectoryHandle)
	if !ok {
		t.Fatalf("OpenAttempt() handle type = %T", handle)
	}
	if err := handle.Close(); err != nil || !fakeHandle.closed {
		t.Fatalf("Close() error = %v, closed = %v", err, fakeHandle.closed)
	}

	verifyErr := errors.New("process identity changed")
	if err := manager.AttachProcess(context.Background(), sandboxID, attemptID, &fakeProcessReference{err: verifyErr}); !errors.Is(err, verifyErr) {
		t.Fatalf("AttachProcess(unverified) error = %v", err)
	}
	if err := manager.AttachProcess(context.Background(), sandboxID, attemptID, &fakeProcessReference{pid: 0}); err == nil {
		t.Fatal("AttachProcess(zero PID) error = nil")
	}
	if len(fake.writes) != writesBefore {
		t.Fatal("unverified process caused a cgroup write")
	}

	fake.setFile(filepath.Join(attemptPath, "cgroup.procs"), "7\n")
	missingReference := &fakeProcessReference{pid: 43}
	if err := manager.AttachProcess(context.Background(), sandboxID, attemptID, missingReference); !errors.Is(err, ErrEffectiveMismatch) {
		t.Fatalf("AttachProcess(missing readback) error = %v, want ErrEffectiveMismatch", err)
	}
	if missingReference.calls != 1 {
		t.Fatalf("missing membership identity verifications = %d, want 1", missingReference.calls)
	}
	if len(fake.writes) != writesBefore {
		t.Fatal("missing membership caused a cgroup.procs write")
	}

	fake.setFile(filepath.Join(attemptPath, "cgroup.procs"), "42\n")
	reusedPID := &fakeProcessReference{verifications: []fakeProcessVerification{{pid: 42}, {pid: 43}}}
	if err := manager.AttachProcess(context.Background(), sandboxID, attemptID, reusedPID); !errors.Is(err, ErrEffectiveMismatch) {
		t.Fatalf("AttachProcess(PID changed after readback) error = %v, want ErrEffectiveMismatch", err)
	}
	secondVerifyErr := errors.New("pidfd no longer identifies a live process")
	exitedProcess := &fakeProcessReference{verifications: []fakeProcessVerification{{pid: 42}, {err: secondVerifyErr}}}
	if err := manager.AttachProcess(context.Background(), sandboxID, attemptID, exitedProcess); !errors.Is(err, secondVerifyErr) {
		t.Fatalf("AttachProcess(second verification failed) error = %v, want %v", err, secondVerifyErr)
	}
	if len(fake.writes) != writesBefore {
		t.Fatal("identity change during membership confirmation caused a cgroup write")
	}
}

// TestKeeperMembershipConfirmationIsReadOnly verifies keeper identity checks
// observe the fixed leaf without migration and expose only a runtime-only FD.
func TestKeeperMembershipConfirmationIsReadOnly(t *testing.T) {
	manager, fake := newFakeManager(t)
	sandboxID := domain.SandboxID("sandbox-keeper-membership")
	if _, err := manager.CreateSandbox(context.Background(), sandboxID); err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	keeper, err := manager.CreateKeeper(context.Background(), sandboxID)
	if err != nil {
		t.Fatalf("CreateKeeper() error = %v", err)
	}
	fake.setFile(filepath.Join(keeper.Path, "cgroup.procs"), "42\n7\n")
	writesBefore := len(fake.writes)
	members, err := manager.KeeperMembership(context.Background(), sandboxID)
	if err != nil || !reflect.DeepEqual(members, []int{7, 42}) {
		t.Fatalf("KeeperMembership() = %v, %v", members, err)
	}
	reference := &fakeProcessReference{pid: 42}
	if err := manager.ConfirmKeeperProcess(context.Background(), sandboxID, reference); err != nil {
		t.Fatalf("ConfirmKeeperProcess() error = %v", err)
	}
	if reference.calls != 2 {
		t.Fatalf("ConfirmKeeperProcess() identity verifications = %d, want 2", reference.calls)
	}
	if err := manager.ConfirmKeeperProcess(context.Background(), sandboxID, &fakeProcessReference{pid: 99}); !errors.Is(err, ErrEffectiveMismatch) {
		t.Fatalf("ConfirmKeeperProcess(missing) error = %v, want ErrEffectiveMismatch", err)
	}
	verifyErr := errors.New("keeper identity changed")
	if err := manager.ConfirmKeeperProcess(context.Background(), sandboxID, &fakeProcessReference{err: verifyErr}); !errors.Is(err, verifyErr) {
		t.Fatalf("ConfirmKeeperProcess(unverified) error = %v", err)
	}
	if len(fake.writes) != writesBefore {
		t.Fatalf("keeper membership checks performed writes: before=%d after=%d", writesBefore, len(fake.writes))
	}
	handle, err := manager.OpenKeeper(context.Background(), sandboxID)
	if err != nil {
		t.Fatalf("OpenKeeper() error = %v", err)
	}
	if handle.Fd() == 0 {
		t.Fatal("OpenKeeper() returned descriptor zero")
	}
	fakeHandle, ok := handle.(*fakeDirectoryHandle)
	if !ok {
		t.Fatalf("OpenKeeper() handle type = %T", handle)
	}
	if err := handle.Close(); err != nil || !fakeHandle.closed {
		t.Fatalf("OpenKeeper Close() error = %v, closed = %v", err, fakeHandle.closed)
	}
}

// TestExactCleanupIsIdempotentAndFailClosed verifies populated, unknown, child, and kernel-busy states retain ownership evidence.
func TestExactCleanupIsIdempotentAndFailClosed(t *testing.T) {
	t.Run("populated keeper", func(t *testing.T) {
		manager, fake := newFakeManager(t)
		sandboxID := domain.SandboxID("sandbox-populated-keeper")
		if _, err := manager.CreateSandbox(context.Background(), sandboxID); err != nil {
			t.Fatalf("CreateSandbox() error = %v", err)
		}
		keeper, err := manager.CreateKeeper(context.Background(), sandboxID)
		if err != nil {
			t.Fatalf("CreateKeeper() error = %v", err)
		}
		fake.setFile(filepath.Join(keeper.Path, "cgroup.events"), "populated 1\n")
		if err := manager.RemoveKeeper(context.Background(), sandboxID); !errors.Is(err, ErrPopulated) {
			t.Fatalf("RemoveKeeper() error = %v, want ErrPopulated", err)
		}
		if len(fake.removes) != 0 || !fake.exists(keeper.Path) {
			t.Fatal("populated keeper was passed to removal")
		}
	})

	t.Run("populated", func(t *testing.T) {
		manager, fake, sandboxID, attemptID, _, attemptPath := createTestAttempt(t)
		fake.setFile(filepath.Join(attemptPath, "cgroup.events"), "populated 1\n")
		if err := manager.RemoveAttempt(context.Background(), sandboxID, attemptID); !errors.Is(err, ErrPopulated) {
			t.Fatalf("RemoveAttempt() error = %v, want ErrPopulated", err)
		}
		if len(fake.removes) != 0 || !fake.exists(attemptPath) {
			t.Fatal("populated cgroup was passed to removal")
		}
	})

	t.Run("unknown events", func(t *testing.T) {
		manager, fake, sandboxID, attemptID, _, attemptPath := createTestAttempt(t)
		fake.setFile(filepath.Join(attemptPath, "cgroup.events"), "frozen 0\n")
		if err := manager.RemoveAttempt(context.Background(), sandboxID, attemptID); !errors.Is(err, ErrUnknownState) {
			t.Fatalf("RemoveAttempt() error = %v, want ErrUnknownState", err)
		}
		if len(fake.removes) != 0 || !fake.exists(attemptPath) {
			t.Fatal("unknown cgroup was passed to removal")
		}
	})

	t.Run("unknown child listing", func(t *testing.T) {
		manager, fake, sandboxID, attemptID, _, attemptPath := createTestAttempt(t)
		fake.setFailure("readdir", attemptPath, errors.New("injected list failure"))
		if err := manager.RemoveAttempt(context.Background(), sandboxID, attemptID); !errors.Is(err, ErrUnknownState) {
			t.Fatalf("RemoveAttempt() error = %v, want ErrUnknownState", err)
		}
		if len(fake.removes) != 0 || !fake.exists(attemptPath) {
			t.Fatal("uninspectable cgroup was passed to removal")
		}
	})

	t.Run("unknown directory identity", func(t *testing.T) {
		manager, fake, sandboxID, attemptID, _, attemptPath := createTestAttempt(t)
		fake.setFailure("lstat", attemptPath, errors.New("injected identity failure"))
		if err := manager.RemoveAttempt(context.Background(), sandboxID, attemptID); !errors.Is(err, ErrUnknownState) {
			t.Fatalf("RemoveAttempt() error = %v, want ErrUnknownState", err)
		}
		if len(fake.removes) != 0 || !fake.exists(attemptPath) {
			t.Fatal("unverified cgroup was passed to removal")
		}
	})

	t.Run("child cgroup", func(t *testing.T) {
		manager, fake, sandboxID, _, sandboxPath, _ := createTestAttempt(t)
		if err := manager.RemoveSandbox(context.Background(), sandboxID); !errors.Is(err, ErrBusy) {
			t.Fatalf("RemoveSandbox() error = %v, want ErrBusy", err)
		}
		if len(fake.removes) != 0 || !fake.exists(sandboxPath) {
			t.Fatal("Sandbox with child was passed to removal")
		}
	})

	t.Run("kernel busy", func(t *testing.T) {
		manager, fake, sandboxID, attemptID, _, attemptPath := createTestAttempt(t)
		fake.setFailure("remove", attemptPath, syscall.EBUSY)
		if err := manager.RemoveAttempt(context.Background(), sandboxID, attemptID); !errors.Is(err, ErrBusy) {
			t.Fatalf("RemoveAttempt() error = %v, want ErrBusy", err)
		}
		if !fake.exists(attemptPath) {
			t.Fatal("busy cgroup disappeared")
		}
	})

	t.Run("success then absent", func(t *testing.T) {
		manager, fake, sandboxID, attemptID, _, attemptPath := createTestAttempt(t)
		if err := manager.RemoveAttempt(context.Background(), sandboxID, attemptID); err != nil {
			t.Fatalf("RemoveAttempt() error = %v", err)
		}
		if fake.exists(attemptPath) || len(fake.removes) != 1 || fake.removes[0] != attemptPath {
			t.Fatalf("exact removals = %v, path still exists = %v", fake.removes, fake.exists(attemptPath))
		}
		if err := manager.RemoveAttempt(context.Background(), sandboxID, attemptID); err != nil {
			t.Fatalf("repeated RemoveAttempt() error = %v", err)
		}
		if len(fake.removes) != 1 {
			t.Fatalf("absent retry invoked removal again: %v", fake.removes)
		}
	})

	t.Run("configured root", func(t *testing.T) {
		manager, fake := newFakeManager(t)
		if err := manager.removeExact(manager.Root()); !errors.Is(err, ErrOutsideRoot) {
			t.Fatalf("removeExact(root) error = %v, want ErrOutsideRoot", err)
		}
		if len(fake.removes) != 0 || !fake.exists(manager.Root()) {
			t.Fatal("configured ownership root was passed to removal")
		}
	})
}
