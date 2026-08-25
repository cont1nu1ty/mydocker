package isolation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// openTestNamespace opens one fake owned namespace and registers all descriptor cleanup.
func openTestNamespace(t *testing.T, ops *fakeOps, namespaceType NamespaceType) (*ProcessHandle, *NamespaceHandle) {
	t.Helper()
	process := captureTestProcess(t, ops)
	handle, err := OpenNamespaceHandle(context.Background(), process, namespaceType)
	if err != nil {
		t.Fatalf("OpenNamespaceHandle() error = %v", err)
	}
	t.Cleanup(func() {
		if err := handle.Close(); err != nil {
			t.Errorf("NamespaceHandle.Close() error = %v", err)
		}
	})
	return process, handle
}

// assertOriginalFDsClosed verifies cleanup attempted Close for every captured original namespace descriptor.
func assertOriginalFDsClosed(t *testing.T, ops *fakeOps, descriptors []int) {
	t.Helper()
	for _, descriptor := range descriptors {
		want := fmt.Sprintf("close:%d", descriptor)
		found := false
		for _, call := range ops.calls {
			if call == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("cleanup calls = %v, missing %s", ops.calls, want)
		}
	}
}

// TestNamespaceHandleBindsOwnerAndNSFS verifies namespace identity includes strong process owner, type, and inode.
func TestNamespaceHandleBindsOwnerAndNSFS(t *testing.T) {
	ops := newFakeOps()
	process, handle := openTestNamespace(t, ops, NamespaceUTS)
	evidence, err := handle.Evidence()
	if err != nil {
		t.Fatalf("Evidence() error = %v", err)
	}
	owner, err := process.Evidence()
	if err != nil {
		t.Fatalf("Process Evidence() error = %v", err)
	}
	if evidence.Type != NamespaceUTS || evidence.Inode != ops.processNS[NamespaceUTS] || evidence.Owner != owner {
		t.Fatalf("Namespace Evidence() = %#v, owner = %#v", evidence, owner)
	}
	if err := handle.Verify(context.Background()); err != nil {
		t.Fatalf("Namespace Verify() error = %v", err)
	}
}

// TestUnshareNamespacesVerifiesNewInodes verifies Sandbox and Attempt namespace creation is bounded and returns changed evidence.
func TestUnshareNamespacesVerifiesNewInodes(t *testing.T) {
	ops := newFakeOps()
	beforeUTS := ops.currentNS[NamespaceUTS]
	beforePID := ops.currentNS[NamespacePID]
	var created CreatedNamespaceSet
	locker, err := runFakeLockedHelper(ops, func(helper *LockedHelper) error {
		var actionErr error
		created, actionErr = helper.UnshareNamespaces(context.Background(), NamespaceUTS, NamespaceIPC, NamespaceNetwork, NamespacePID)
		return actionErr
	})
	if err != nil {
		t.Fatalf("UnshareNamespaces() error = %v", err)
	}
	if locker.locks != 1 || locker.unlocks != 0 {
		t.Fatalf("helper lock transitions = (%d, %d), want tainted (1, 0)", locker.locks, locker.unlocks)
	}
	if err := created.Validate(); err != nil {
		t.Fatalf("CreatedNamespaceSet.Validate() error = %v", err)
	}
	if created.Inodes[NamespaceUTS] == beforeUTS || created.Inodes[NamespacePID] == beforePID {
		t.Fatalf("created namespace evidence = %#v, want changed UTS/PID inodes", created)
	}
	clone := created.Clone()
	clone.Inodes[NamespaceUTS]++
	if clone.Inodes[NamespaceUTS] == created.Inodes[NamespaceUTS] {
		t.Fatal("CreatedNamespaceSet.Clone() retained map alias")
	}
}

// TestUnshareNamespacesRejectsInvalidIntentBeforeMutation verifies duplicates and unsupported kinds never call unshare.
func TestUnshareNamespacesRejectsInvalidIntentBeforeMutation(t *testing.T) {
	for name, namespaces := range map[string][]NamespaceType{
		"empty":       nil,
		"duplicate":   {NamespaceUTS, NamespaceUTS},
		"unsupported": {"future"},
	} {
		t.Run(name, func(t *testing.T) {
			ops := newFakeOps()
			locker, err := runFakeLockedHelper(ops, func(helper *LockedHelper) error {
				_, actionErr := helper.UnshareNamespaces(context.Background(), namespaces...)
				return actionErr
			})
			if err == nil {
				t.Fatal("UnshareNamespaces() error = nil, want rejection")
			}
			if containsCall(ops.mutations, "unshare") {
				t.Fatalf("invalid namespace intent mutated host: %v", ops.mutations)
			}
			if locker.locks != 1 || locker.unlocks != 1 {
				t.Fatalf("helper lock transitions = (%d, %d), want safe release (1, 1)", locker.locks, locker.unlocks)
			}
		})
	}
}

// TestUnshareNamespacesRejectsWrongThread verifies a helper capability cannot mutate after thread identity drift.
func TestUnshareNamespacesRejectsWrongThread(t *testing.T) {
	ops := newFakeOps()
	locker, err := runFakeLockedHelper(ops, func(helper *LockedHelper) error {
		ops.tid++
		_, actionErr := helper.UnshareNamespaces(context.Background(), NamespaceUTS)
		return actionErr
	})
	if !errors.Is(err, ErrWrongThread) {
		t.Fatalf("UnshareNamespaces() error = %v, want ErrWrongThread", err)
	}
	if containsCall(ops.mutations, "unshare") {
		t.Fatalf("wrong-thread helper mutated host: %v", ops.mutations)
	}
	if locker.locks != 1 || locker.unlocks != 1 {
		t.Fatalf("helper lock transitions = (%d, %d), want safe release (1, 1)", locker.locks, locker.unlocks)
	}
}

// TestUnshareNamespacesDiscardsThreadAfterFailedVerification verifies a successful unshare taints the helper even when evidence validation fails.
func TestUnshareNamespacesDiscardsThreadAfterFailedVerification(t *testing.T) {
	ops := newFakeOps()
	ops.unshareNoop = true
	locker, err := runFakeLockedHelper(ops, func(helper *LockedHelper) error {
		_, actionErr := helper.UnshareNamespaces(context.Background(), NamespaceUTS)
		return actionErr
	})
	if !errors.Is(err, ErrUnsafeIdentity) {
		t.Fatalf("UnshareNamespaces() error = %v, want ErrUnsafeIdentity", err)
	}
	if locker.locks != 1 || locker.unlocks != 0 {
		t.Fatalf("helper lock transitions = (%d, %d), want discarded tainted thread (1, 0)", locker.locks, locker.unlocks)
	}
}

// TestNamespaceHandleRejectsInvalidInode verifies a zero or mismatched nsfs identity cannot be opened.
func TestNamespaceHandleRejectsInvalidInode(t *testing.T) {
	ops := newFakeOps()
	process := captureTestProcess(t, ops)
	ops.processNS[NamespaceUTS] = 0
	_, err := OpenNamespaceHandle(context.Background(), process, NamespaceUTS)
	if !errors.Is(err, ErrUnsafeIdentity) {
		t.Fatalf("OpenNamespaceHandle() error = %v, want ErrUnsafeIdentity", err)
	}
}

// TestRunNamespaceSessionRestoresButDiscardsJoinedThread verifies automatic cleanup restores identity while never recycling a setns-tainted thread.
func TestRunNamespaceSessionRestoresButDiscardsJoinedThread(t *testing.T) {
	ops := newFakeOps()
	_, handle := openTestNamespace(t, ops, NamespaceUTS)
	original := ops.currentNS[NamespaceUTS]
	locker, err := runFakeNamespaceSession(context.Background(), ops, []*NamespaceHandle{handle}, func(context.Context, *LockedHelper) error {
		if got := ops.currentNS[NamespaceUTS]; got != ops.processNS[NamespaceUTS] {
			t.Fatalf("joined UTS inode = %d, want %d", got, ops.processNS[NamespaceUTS])
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunNamespaceSession() error = %v", err)
	}
	if got := ops.currentNS[NamespaceUTS]; got != original {
		t.Fatalf("cleanup UTS inode = %d, want restored %d", got, original)
	}
	if locker.locks != 1 || locker.unlocks != 0 {
		t.Fatalf("helper lock transitions = (%d, %d), want discarded setns thread (1, 0)", locker.locks, locker.unlocks)
	}
}

// TestRunNamespaceSessionCancelledContextStillCleans verifies request cancellation cannot cancel the independent restoration context.
func TestRunNamespaceSessionCancelledContextStillCleans(t *testing.T) {
	ops := newFakeOps()
	_, handle := openTestNamespace(t, ops, NamespaceUTS)
	original := ops.currentNS[NamespaceUTS]
	ctx, cancel := context.WithCancel(context.Background())
	locker, err := runFakeNamespaceSession(ctx, ops, []*NamespaceHandle{handle}, func(actionCtx context.Context, _ *LockedHelper) error {
		cancel()
		return actionCtx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunNamespaceSession() error = %v, want context.Canceled", err)
	}
	if got := ops.currentNS[NamespaceUTS]; got != original {
		t.Fatalf("cancel cleanup UTS inode = %d, want restored %d", got, original)
	}
	if locker.unlocks != 0 {
		t.Fatalf("cancel cleanup unlocked tainted thread %d times", locker.unlocks)
	}
}

// TestRunNamespaceSessionRestoreFailureDiscardsDirtyThread verifies failed cleanup never returns a joined thread to the runtime pool.
func TestRunNamespaceSessionRestoreFailureDiscardsDirtyThread(t *testing.T) {
	ops := newFakeOps()
	_, uts := openTestNamespace(t, ops, NamespaceUTS)
	_, ipc := openTestNamespace(t, ops, NamespaceIPC)
	ops.setnsFailAt[3] = errors.New("restore denied")
	var originals []int
	locker, err := runFakeNamespaceSession(context.Background(), ops, []*NamespaceHandle{uts, ipc}, func(_ context.Context, helper *LockedHelper) error {
		for _, original := range helper.session.originals {
			originals = append(originals, original.fd)
		}
		ops.fail[fmt.Sprintf("close:%d", originals[0])] = errors.New("close denied")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "restore denied") || !strings.Contains(err.Error(), "close denied") {
		t.Fatalf("RunNamespaceSession() error = %v, want aggregated restore and close failures", err)
	}
	if got := ops.currentNS[NamespaceIPC]; got != ops.processNS[NamespaceIPC] {
		t.Fatalf("failed restore IPC inode = %d, want still joined %d", got, ops.processNS[NamespaceIPC])
	}
	if locker.locks != 1 || locker.unlocks != 0 {
		t.Fatalf("helper lock transitions = (%d, %d), want discarded dirty thread (1, 0)", locker.locks, locker.unlocks)
	}
	assertOriginalFDsClosed(t, ops, originals)
}

// TestRunNamespaceSessionCleanupTimeoutClosesOriginals verifies a bounded cleanup deadline still releases every captured descriptor.
func TestRunNamespaceSessionCleanupTimeoutClosesOriginals(t *testing.T) {
	ops := newFakeOps()
	_, uts := openTestNamespace(t, ops, NamespaceUTS)
	_, ipc := openTestNamespace(t, ops, NamespaceIPC)
	var originals []int
	locker := &fakeThreadLocker{}
	err := runNamespaceSession(context.Background(), ops, locker, time.Nanosecond, []*NamespaceHandle{uts, ipc}, func(_ context.Context, helper *LockedHelper) error {
		for _, original := range helper.session.originals {
			originals = append(originals, original.fd)
		}
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunNamespaceSession() error = %v, want cleanup DeadlineExceeded", err)
	}
	if locker.unlocks != 0 {
		t.Fatalf("timed-out cleanup unlocked dirty thread %d times", locker.unlocks)
	}
	assertOriginalFDsClosed(t, ops, originals)
}

// TestRunNamespaceSessionWrongThreadFailsClosed verifies cleanup cannot unlock when thread identity no longer matches its runner.
func TestRunNamespaceSessionWrongThreadFailsClosed(t *testing.T) {
	ops := newFakeOps()
	_, handle := openTestNamespace(t, ops, NamespaceUTS)
	locker, err := runFakeNamespaceSession(context.Background(), ops, []*NamespaceHandle{handle}, func(context.Context, *LockedHelper) error {
		ops.tid++
		return nil
	})
	if !errors.Is(err, ErrWrongThread) {
		t.Fatalf("RunNamespaceSession() error = %v, want ErrWrongThread", err)
	}
	if locker.unlocks != 0 {
		t.Fatalf("wrong-thread cleanup unlocked %d times", locker.unlocks)
	}
}

// TestRunNamespaceSessionPIDUsesChildrenView verifies PID setns readback and cleanup observe pid_for_children semantics.
func TestRunNamespaceSessionPIDUsesChildrenView(t *testing.T) {
	ops := newFakeOps()
	_, handle := openTestNamespace(t, ops, NamespacePID)
	_, err := runFakeNamespaceSession(context.Background(), ops, []*NamespaceHandle{handle}, func(context.Context, *LockedHelper) error {
		return nil
	})
	if err != nil {
		t.Fatalf("RunNamespaceSession() error = %v", err)
	}
	if !containsCall(ops.calls, "/proc/thread-self/ns/pid_for_children") {
		t.Fatalf("PID session calls = %v, want pid_for_children", ops.calls)
	}
	if containsCall(ops.calls, "/proc/self/ns/") {
		t.Fatalf("PID session inspected the thread-group leader instead of the locked thread: %v", ops.calls)
	}
}

// TestRunNamespaceSessionRejectsWrongActualInode verifies setns success is not accepted unless current nsfs inode equals handle evidence.
func TestRunNamespaceSessionRejectsWrongActualInode(t *testing.T) {
	ops := newFakeOps()
	_, handle := openTestNamespace(t, ops, NamespaceUTS)
	original := ops.currentNS[NamespaceUTS]
	ops.setnsInode = 9_999
	locker, err := runFakeNamespaceSession(context.Background(), ops, []*NamespaceHandle{handle}, func(context.Context, *LockedHelper) error {
		t.Fatal("namespace action ran after wrong-inode setns")
		return nil
	})
	if !errors.Is(err, ErrUnsafeIdentity) {
		t.Fatalf("RunNamespaceSession() error = %v, want ErrUnsafeIdentity", err)
	}
	if got := ops.currentNS[NamespaceUTS]; got != original {
		t.Fatalf("failed Join() UTS inode = %d, want restored %d", got, original)
	}
	if locker.unlocks != 0 {
		t.Fatalf("wrong-inode setns unlocked tainted thread %d times", locker.unlocks)
	}
}

// TestRunNamespaceSessionStopsOnSetnsError verifies an invalid setns that leaves the original inode releases an untainted helper safely.
func TestRunNamespaceSessionStopsOnSetnsError(t *testing.T) {
	ops := newFakeOps()
	_, handle := openTestNamespace(t, ops, NamespaceUTS)
	original := ops.currentNS[NamespaceUTS]
	ops.fail["setns"] = errors.New("invalid setns")
	locker, err := runFakeNamespaceSession(context.Background(), ops, []*NamespaceHandle{handle}, func(context.Context, *LockedHelper) error {
		t.Fatal("namespace action ran after failed setns")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "invalid setns") {
		t.Fatalf("RunNamespaceSession() error = %v, want injected setns error", err)
	}
	if got := ops.currentNS[NamespaceUTS]; got != original {
		t.Fatalf("failed setns UTS inode = %d, want %d", got, original)
	}
	if locker.locks != 1 || locker.unlocks != 1 {
		t.Fatalf("helper lock transitions = (%d, %d), want safe untainted release (1, 1)", locker.locks, locker.unlocks)
	}
}

// TestRunNamespaceSessionUnsharesFSBeforeMountSetns verifies CLONE_FS precedes a mount namespace join on the same helper thread.
func TestRunNamespaceSessionUnsharesFSBeforeMountSetns(t *testing.T) {
	ops := newFakeOps()
	_, handle := openTestNamespace(t, ops, NamespaceMount)
	_, err := runFakeNamespaceSession(context.Background(), ops, []*NamespaceHandle{handle}, func(context.Context, *LockedHelper) error {
		return nil
	})
	if err != nil {
		t.Fatalf("RunNamespaceSession() error = %v", err)
	}
	if len(ops.mutations) < 2 || ops.mutations[0] != "unshare:"+integerString(filesystemContextFlag()) || !strings.HasPrefix(ops.mutations[1], "setns:") {
		t.Fatalf("mount join mutation order = %v, want CLONE_FS then setns", ops.mutations)
	}
}
