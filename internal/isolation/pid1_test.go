package isolation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// fakePID1Launcher models fork plus child readiness while keeping parent and child namespace state separate.
type fakePID1Launcher struct {
	parent       *fakeOps
	child        *fakeOps
	childLocker  *fakeThreadLocker
	wrapperRan   bool
	wrongReceipt bool
}

// ForkPID1 records fork before running the fake child bootstrap and returns a handle owned by the final wrapper.
func (l *fakePID1Launcher) ForkPID1(ctx context.Context, bootstrap PID1Bootstrap) (*ProcessHandle, error) {
	l.parent.mu.Lock()
	l.parent.record("fork-pid1", true)
	l.parent.mu.Unlock()
	child := newFakeOps()
	child.pid = 1
	for namespaceType, inode := range l.parent.currentNS {
		child.currentNS[namespaceType] = inode
	}
	child.currentNS[NamespacePID] = bootstrap.Namespaces.Inodes[NamespacePID]
	child.currentNS[NamespaceMount] = bootstrap.Namespaces.Inodes[NamespaceMount]
	locker := &fakeThreadLocker{}
	if err := runPID1Child(ctx, child, locker, defaultNamespaceCleanupTimeout, bootstrap, func(actionCtx context.Context, helper *LockedHelper) error {
		if err := helper.PrepareRoot(actionCtx, bootstrap.Rootfs); err != nil {
			return err
		}
		l.wrapperRan = true
		return nil
	}); err != nil {
		return nil, fmt.Errorf("fake child bootstrap: %w", err)
	}
	l.child = child
	l.childLocker = locker
	if !l.wrongReceipt {
		l.parent.processNS[NamespacePID] = bootstrap.Namespaces.Inodes[NamespacePID]
		l.parent.processNS[NamespaceMount] = bootstrap.Namespaces.Inodes[NamespaceMount]
	}
	return CaptureProcessHandle(ctx, l.parent, 123, testProcessOwner())
}

// countMutation returns how many fake mutations contain one operation fragment.
func countMutation(mutations []string, fragment string) int {
	count := 0
	for _, mutation := range mutations {
		if strings.Contains(mutation, fragment) {
			count++
		}
	}
	return count
}

// mutationIndex returns the first fake mutation index containing a fragment or minus one.
func mutationIndex(mutations []string, fragment string) int {
	for index, mutation := range mutations {
		if strings.Contains(mutation, fragment) {
			return index
		}
	}
	return -1
}

// testPID1ChildBootstrap returns valid active PID/mount evidence for one fake PID 1 wrapper.
func testPID1ChildBootstrap(ops *fakeOps) PID1Bootstrap {
	return PID1Bootstrap{
		SchemaVersion: PID1BootstrapSchemaVersion,
		Namespaces: CreatedNamespaceSet{Inodes: map[NamespaceType]uint64{
			NamespacePID:   ops.currentNS[NamespacePID],
			NamespaceMount: ops.currentNS[NamespaceMount],
		}},
		Rootfs: testRootfsConfig(),
	}
}

// TestRunPID1ChildEntersActionBeforeRootfs verifies identity readiness has no implicit mount or pivot side effect.
func TestRunPID1ChildEntersActionBeforeRootfs(t *testing.T) {
	ops := newFakeOps()
	ops.pid = 1
	bootstrap := testPID1ChildBootstrap(ops)
	locker := &fakeThreadLocker{}
	actionRan := false
	err := runPID1Child(context.Background(), ops, locker, defaultNamespaceCleanupTimeout, bootstrap, func(context.Context, *LockedHelper) error {
		actionRan = true
		if len(ops.mutations) != 0 {
			t.Fatalf("mutations before PID 1 action = %v, want none", ops.mutations)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runPID1Child() error = %v", err)
	}
	if !actionRan || len(ops.mutations) != 0 {
		t.Fatalf("identity-only actionRan=%v mutations=%v", actionRan, ops.mutations)
	}
	if locker.unlocks != 0 {
		t.Fatalf("PID 1 thread unlocks = %d, want discarded child thread", locker.unlocks)
	}
}

// TestRunPID1ChildExplicitPrepareRoot verifies the action controls the one-shot rootfs/proc transition.
func TestRunPID1ChildExplicitPrepareRoot(t *testing.T) {
	ops := newFakeOps()
	ops.pid = 1
	bootstrap := testPID1ChildBootstrap(ops)
	locker := &fakeThreadLocker{}
	err := runPID1Child(context.Background(), ops, locker, defaultNamespaceCleanupTimeout, bootstrap, func(actionCtx context.Context, helper *LockedHelper) error {
		if len(ops.mutations) != 0 {
			t.Fatalf("mutations before explicit PrepareRoot = %v, want none", ops.mutations)
		}
		if prepared, err := helper.RootPrepared(); err != nil || prepared {
			return fmt.Errorf("RootPrepared(before) = (%v, %v), want false", prepared, err)
		}
		if err := helper.PrepareRoot(actionCtx, bootstrap.Rootfs); err != nil {
			return err
		}
		if prepared, err := helper.RootPrepared(); err != nil || !prepared {
			return fmt.Errorf("RootPrepared(after) = (%v, %v), want true", prepared, err)
		}
		mutationCount := len(ops.mutations)
		if err := helper.PrepareRoot(actionCtx, bootstrap.Rootfs); !errors.Is(err, ErrUnsafeIdentity) {
			return fmt.Errorf("second PrepareRoot error = %v, want ErrUnsafeIdentity", err)
		}
		if len(ops.mutations) != mutationCount {
			return fmt.Errorf("second PrepareRoot added mutations: %v", ops.mutations[mutationCount:])
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runPID1Child() error = %v", err)
	}
	if !containsCall(ops.mutations, "pivot:") || !containsCall(ops.mutations, "mount:proc:/proc:proc:") {
		t.Fatalf("explicit rootfs mutations = %v, want pivot and fresh proc", ops.mutations)
	}
}

// TestRunPID1ChildCheckpointFailureHasNoRootfsSideEffect verifies a durable-checkpoint failure can abort before rootfs mutation.
func TestRunPID1ChildCheckpointFailureHasNoRootfsSideEffect(t *testing.T) {
	ops := newFakeOps()
	ops.pid = 1
	bootstrap := testPID1ChildBootstrap(ops)
	checkpointErr := errors.New("checkpoint failed")
	checkpointAttempted := false
	locker := &fakeThreadLocker{}
	err := runPID1Child(context.Background(), ops, locker, defaultNamespaceCleanupTimeout, bootstrap, func(context.Context, *LockedHelper) error {
		checkpointAttempted = true
		return checkpointErr
	})
	if !errors.Is(err, checkpointErr) {
		t.Fatalf("runPID1Child() error = %v, want checkpoint failure", err)
	}
	if !checkpointAttempted || containsCall(ops.mutations, "mount:") || containsCall(ops.mutations, "pivot:") {
		t.Fatalf("checkpointAttempted=%v mutations=%v, want no rootfs side effect", checkpointAttempted, ops.mutations)
	}
	if locker.unlocks != 0 {
		t.Fatalf("failed PID 1 checkpoint unlocked child thread %d times", locker.unlocks)
	}
}

// TestAttemptIsolationOrderAndFinalOwnerReceipt verifies join, one PID/mount unshare, fork, child rootfs/proc, and final-owner evidence.
func TestAttemptIsolationOrderAndFinalOwnerReceipt(t *testing.T) {
	ctx := context.Background()
	ops := newFakeOps()
	_, uts := openTestNamespace(t, ops, NamespaceUTS)
	_, ipc := openTestNamespace(t, ops, NamespaceIPC)
	_, network := openTestNamespace(t, ops, NamespaceNetwork)
	launcher := &fakePID1Launcher{parent: ops}
	var created CreatedNamespaceSet
	var receipt AttemptIsolationReceipt
	var process *ProcessHandle
	locker, err := runFakeNamespaceSession(ctx, ops, []*NamespaceHandle{uts, ipc, network}, func(actionCtx context.Context, helper *LockedHelper) error {
		var actionErr error
		created, actionErr = helper.UnshareNamespaces(actionCtx, NamespacePID, NamespaceMount)
		if actionErr != nil {
			return actionErr
		}
		bootstrap, actionErr := helper.NewPID1Bootstrap(actionCtx, testRootfsConfig())
		if actionErr != nil {
			return actionErr
		}
		process, receipt, actionErr = helper.ForkPID1(actionCtx, launcher, bootstrap)
		return actionErr
	})
	if process != nil {
		defer process.Close()
	}
	if err != nil {
		t.Fatalf("Attempt isolation flow error = %v", err)
	}
	if !launcher.wrapperRan || launcher.child == nil {
		t.Fatal("PID 1 wrapper did not complete child rootfs readiness")
	}
	if locker.unlocks != 0 || launcher.childLocker.unlocks != 0 {
		t.Fatalf("tainted helper unlocks = parent %d child %d, want zero", locker.unlocks, launcher.childLocker.unlocks)
	}
	wantUnshare := "unshare:" + integerString(mustNamespaceFlags(t, NamespacePID, NamespaceMount))
	if countMutation(ops.mutations, "unshare:") != 1 || mutationIndex(ops.mutations, wantUnshare) < 0 {
		t.Fatalf("parent mutations = %v, want exactly one combined PID/mount unshare", ops.mutations)
	}
	unshareIndex := mutationIndex(ops.mutations, wantUnshare)
	forkIndex := mutationIndex(ops.mutations, "fork-pid1")
	if len(ops.mutations) < 5 || unshareIndex != 3 || forkIndex != 4 {
		t.Fatalf("parent mutation order = %v, want join before unshare before fork", ops.mutations)
	}
	for index := 0; index < 3; index++ {
		if !strings.HasPrefix(ops.mutations[index], "setns:") {
			t.Fatalf("parent mutation order = %v, want UTS/IPC/net joins first", ops.mutations)
		}
	}
	if countMutation(ops.mutations, "mount:") != 0 {
		t.Fatalf("parent helper performed rootfs mounts: %v", ops.mutations)
	}
	if countMutation(launcher.child.mutations, "unshare:") != 0 || !containsCall(launcher.child.mutations, "mount:proc:/proc:proc:") {
		t.Fatalf("child mutations = %v, want no repeated unshare and a fresh proc mount", launcher.child.mutations)
	}
	if receipt.PID.Inode != created.Inodes[NamespacePID] || receipt.Mount.Inode != created.Inodes[NamespaceMount] {
		t.Fatalf("final receipt = %#v, created = %#v", receipt, created)
	}
	if receipt.PID.Owner != receipt.Owner || receipt.Mount.Owner != receipt.Owner {
		t.Fatalf("receipt namespaces are not bound to final owner: %#v", receipt)
	}
	if launcher.child.currentNS[NamespaceMount] != receipt.Mount.Inode {
		t.Fatalf("rootfs mount inode = %d, receipt = %d", launcher.child.currentNS[NamespaceMount], receipt.Mount.Inode)
	}
}

// mustNamespaceFlags combines fake-test namespace flags or fails the test on an unsupported kind.
func mustNamespaceFlags(t *testing.T, namespaceTypes ...NamespaceType) int {
	t.Helper()
	flags := 0
	for _, namespaceType := range namespaceTypes {
		flag, err := namespaceCloneFlag(namespaceType)
		if err != nil {
			t.Fatalf("namespaceCloneFlag(%q) error = %v", namespaceType, err)
		}
		flags |= flag
	}
	return flags
}

// TestRunPID1ChildRejectsParentAndWrongNamespace verifies rootfs cannot run before fork or with stale bootstrap evidence.
func TestRunPID1ChildRejectsParentAndWrongNamespace(t *testing.T) {
	for name, mutate := range map[string]func(*fakeOps, *PID1Bootstrap){
		"not pid 1": func(ops *fakeOps, _ *PID1Bootstrap) {},
		"wrong inode": func(ops *fakeOps, bootstrap *PID1Bootstrap) {
			ops.pid = 1
			bootstrap.Namespaces.Inodes[NamespaceMount]++
		},
	} {
		t.Run(name, func(t *testing.T) {
			ops := newFakeOps()
			bootstrap := PID1Bootstrap{
				SchemaVersion: PID1BootstrapSchemaVersion,
				Namespaces: CreatedNamespaceSet{Inodes: map[NamespaceType]uint64{
					NamespacePID:   ops.currentNS[NamespacePID],
					NamespaceMount: ops.currentNS[NamespaceMount],
				}},
				Rootfs: testRootfsConfig(),
			}
			mutate(ops, &bootstrap)
			locker := &fakeThreadLocker{}
			err := runPID1Child(context.Background(), ops, locker, defaultNamespaceCleanupTimeout, bootstrap, func(context.Context, *LockedHelper) error {
				t.Fatal("PID 1 action ran after failed child identity validation")
				return nil
			})
			if !errors.Is(err, ErrUnsafeIdentity) {
				t.Fatalf("runPID1Child() error = %v, want ErrUnsafeIdentity", err)
			}
			if containsCall(ops.mutations, "mount:") {
				t.Fatalf("invalid PID 1 child mounted rootfs: %v", ops.mutations)
			}
			if locker.unlocks != 1 {
				t.Fatalf("unmodified invalid child thread unlocks = %d, want 1", locker.unlocks)
			}
		})
	}
}

// TestForkPID1RejectsNonOwnerNamespaceEvidence verifies parent-created inode guesses cannot replace final wrapper evidence.
func TestForkPID1RejectsNonOwnerNamespaceEvidence(t *testing.T) {
	ops := newFakeOps()
	launcher := &fakePID1Launcher{parent: ops, wrongReceipt: true}
	locker, err := runFakeLockedHelper(ops, func(helper *LockedHelper) error {
		if _, actionErr := helper.UnshareNamespaces(context.Background(), NamespacePID, NamespaceMount); actionErr != nil {
			return actionErr
		}
		bootstrap, actionErr := helper.NewPID1Bootstrap(context.Background(), testRootfsConfig())
		if actionErr != nil {
			return actionErr
		}
		_, _, actionErr = helper.ForkPID1(context.Background(), launcher, bootstrap)
		return actionErr
	})
	if !errors.Is(err, ErrUnsafeIdentity) {
		t.Fatalf("ForkPID1() error = %v, want ErrUnsafeIdentity", err)
	}
	if locker.unlocks != 0 {
		t.Fatalf("failed fork validation unlocked tainted parent thread %d times", locker.unlocks)
	}
}
