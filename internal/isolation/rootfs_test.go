package isolation

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"syscall"
	"testing"
)

// testRootfsConfig returns one valid owned prepared-rootfs path for syscall-order tests.
func testRootfsConfig() RootfsConfig {
	return RootfsConfig{AllowedRoot: "/var/lib/mydocker/rootfs", Rootfs: "/var/lib/mydocker/rootfs/attempt-1"}
}

// prepareTestRoot runs rootfs preparation as a synthetic PID 1 wrapper with verified active namespace receipts.
func prepareTestRoot(ops *fakeOps, config RootfsConfig) (*fakeThreadLocker, error) {
	ops.pid = 1
	bootstrap := PID1Bootstrap{
		SchemaVersion: PID1BootstrapSchemaVersion,
		Namespaces: CreatedNamespaceSet{Inodes: map[NamespaceType]uint64{
			NamespacePID:   ops.currentNS[NamespacePID],
			NamespaceMount: ops.currentNS[NamespaceMount],
		}},
		Rootfs: config,
	}
	locker := &fakeThreadLocker{}
	err := runPID1Child(context.Background(), ops, locker, defaultNamespaceCleanupTimeout, bootstrap, func(actionCtx context.Context, helper *LockedHelper) error {
		return helper.PrepareRoot(actionCtx, bootstrap.Rootfs)
	})
	return locker, err
}

// TestRootfsConfigRejectsEscapes verifies root, equal, relative, and parent-escaping paths fail before Ops use.
func TestRootfsConfigRejectsEscapes(t *testing.T) {
	tests := []RootfsConfig{
		{AllowedRoot: "/", Rootfs: "/tmp/rootfs"},
		{AllowedRoot: "/var/lib/mydocker", Rootfs: "/"},
		{AllowedRoot: "relative", Rootfs: "/var/lib/mydocker/rootfs"},
		{AllowedRoot: "/var/lib/mydocker", Rootfs: "/var/lib/mydocker"},
		{AllowedRoot: "/var/lib/mydocker", Rootfs: "/var/lib/other"},
		{AllowedRoot: "/var/lib/mydocker", Rootfs: "/var/lib/mydocker/rootfs", OldRootName: "../old"},
	}
	for _, config := range tests {
		if err := config.Validate(); !errors.Is(err, ErrUnsafePath) {
			t.Errorf("RootfsConfig.Validate(%#v) error = %v, want ErrUnsafePath", config, err)
		}
	}
}

// TestPrepareRootSequence verifies private propagation, self-bind, pivot, old-root detach, proc, and /dev ordering.
func TestPrepareRootSequence(t *testing.T) {
	ops := newFakeOps()
	locker, err := prepareTestRoot(ops, testRootfsConfig())
	if err != nil {
		t.Fatalf("PrepareRoot() error = %v", err)
	}
	if locker.locks != 1 || locker.unlocks != 0 {
		t.Fatalf("helper lock transitions = (%d, %d), want discarded tainted thread (1, 0)", locker.locks, locker.unlocks)
	}
	root := "/proc/self/fd/801"
	want := []string{
		"mount::/:" + ":" + unsignedString(privateRecursiveFlags()) + ":",
		"mount:" + root + ":" + root + "::" + unsignedString(selfBindRecursiveFlags()) + ":",
		"mkdir:" + root + "/.pivot_old:700",
		"pivot:" + root + ":" + root + "/.pivot_old",
		"chdir:/",
		"unmount:/.pivot_old:" + integerString(detachUnmountFlag()),
		"remove:/.pivot_old",
		"mkdir:/proc:555",
		"mount:proc:/proc:proc:" + unsignedString(safeProcFlags()) + ":",
		"mkdir:/dev:755",
		"mount:tmpfs:/dev:tmpfs:" + unsignedString(safeDevFlags()) + ":mode=0755,size=4194304,nr_inodes=1024",
	}
	if !reflect.DeepEqual(ops.mutations, want) {
		t.Fatalf("PrepareRoot() mutations =\n%v\nwant\n%v", ops.mutations, want)
	}
}

// TestPrepareRootWithDNSBindsDescriptorBeforePivot verifies the fixed
// resolv.conf mount is descriptor-based, precedes pivot, and is read back by identity.
func TestPrepareRootWithDNSBindsDescriptorBeforePivot(t *testing.T) {
	ops := newFakeOps()
	ops.pid = 1
	const dnsFD = 900
	ops.fdStats[dnsFD] = FileInfo{Mode: 0100000, Dev: 1, Ino: 12}
	bootstrap := testPID1ChildBootstrap(ops)
	locker := &fakeThreadLocker{}
	err := runPID1Child(context.Background(), ops, locker, defaultNamespaceCleanupTimeout, bootstrap, func(actionCtx context.Context, helper *LockedHelper) error {
		return helper.PrepareRootWithDNS(actionCtx, bootstrap.Rootfs, dnsFD)
	})
	if err != nil {
		t.Fatalf("PrepareRootWithDNS() error = %v", err)
	}
	bind := "mount:/proc/self/fd/900:/proc/self/fd/802::" + unsignedString(fileBindFlags()) + ":"
	pivot := "pivot:/proc/self/fd/801:/proc/self/fd/801/.pivot_old"
	bindIndex, pivotIndex := -1, -1
	for index, mutation := range ops.mutations {
		if mutation == bind {
			bindIndex = index
		}
		if mutation == pivot {
			pivotIndex = index
		}
	}
	if bindIndex < 0 || pivotIndex < 0 || bindIndex >= pivotIndex {
		t.Fatalf("DNS bind/pivot order = %v", ops.mutations)
	}
}

// TestPrepareRootRejectsSymlinkResolution verifies kernel-style no-symlink open failure precedes all mutations.
func TestPrepareRootRejectsSymlinkResolution(t *testing.T) {
	ops := newFakeOps()
	config := testRootfsConfig()
	name := "open-directory-at:800:attempt-1"
	ops.fail[name] = errors.New("symlink rejected")
	locker, err := prepareTestRoot(ops, config)
	if !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("PrepareRoot() error = %v, want ErrUnsafePath", err)
	}
	if len(ops.mutations) != 0 {
		t.Fatalf("PrepareRoot() mutations = %v, want none", ops.mutations)
	}
	if locker.locks != 1 || locker.unlocks != 0 {
		t.Fatalf("helper lock transitions = (%d, %d), want discarded PID 1 thread (1, 0)", locker.locks, locker.unlocks)
	}
}

// TestPrepareRootRetainsOneAllowedRootDescriptor verifies rootfs and DNS target
// resolution share the initially verified base identity instead of reopening its path.
func TestPrepareRootRetainsOneAllowedRootDescriptor(t *testing.T) {
	ops := newFakeOps()
	ops.pid = 1
	const dnsFD = 900
	ops.fdStats[dnsFD] = FileInfo{Mode: 0100000, Dev: 1, Ino: 12}
	bootstrap := testPID1ChildBootstrap(ops)
	err := runPID1Child(context.Background(), ops, &fakeThreadLocker{}, defaultNamespaceCleanupTimeout, bootstrap, func(actionCtx context.Context, helper *LockedHelper) error {
		return helper.PrepareRootWithDNS(actionCtx, bootstrap.Rootfs, dnsFD)
	})
	if err != nil {
		t.Fatal(err)
	}
	calls := strings.Join(ops.calls, "\n")
	if strings.Count(calls, "open-root:"+bootstrap.Rootfs.AllowedRoot) != 1 {
		t.Fatalf("allowed-root opens:\n%s", calls)
	}
	if !strings.Contains(calls, "open-directory-at:800:attempt-1") ||
		strings.Count(calls, "open-file-at:800:attempt-1/etc/resolv.conf") != 2 {
		t.Fatalf("descriptor-relative opens:\n%s", calls)
	}
	if strings.Contains(calls, "open-beneath:") || strings.Contains(calls, "open-file-beneath:") {
		t.Fatalf("rootfs preparation reopened an ownership-root path:\n%s", calls)
	}
}

// TestPrepareRootRejectsPlantedDirectories verifies pivot and mount targets cannot be preplanted links.
func TestPrepareRootRejectsPlantedDirectories(t *testing.T) {
	t.Run("put old must be new", func(t *testing.T) {
		ops := newFakeOps()
		ops.fail["mkdir:/proc/self/fd/801/.pivot_old"] = syscall.EEXIST
		_, err := prepareTestRoot(ops, testRootfsConfig())
		if !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("PrepareRoot() error = %v, want ErrUnsafePath", err)
		}
		if containsCall(ops.mutations, "pivot:") {
			t.Fatalf("PrepareRoot() pivoted through planted put_old: %v", ops.mutations)
		}
	})
	t.Run("proc symlink rejected", func(t *testing.T) {
		ops := newFakeOps()
		ops.fail["mkdir:/proc"] = syscall.EEXIST
		ops.pathStats["/proc"] = FileInfo{Mode: 0120000, Dev: 1, Ino: 12}
		_, err := prepareTestRoot(ops, testRootfsConfig())
		if !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("PrepareRoot() error = %v, want ErrUnsafePath", err)
		}
		if containsCall(ops.mutations, "mount:proc") {
			t.Fatalf("PrepareRoot() mounted proc through a link: %v", ops.mutations)
		}
	})
}

// TestPrepareRootFaultBoundaries verifies each injected syscall failure stops all later rootfs steps.
func TestPrepareRootFaultBoundaries(t *testing.T) {
	tests := []struct {
		name string
		fail string
		last string
	}{
		{name: "private propagation", fail: "mount:/", last: "mount::/:"},
		{name: "self bind", fail: "mount:/proc/self/fd/801", last: "mount:/proc/self/fd/801:/proc/self/fd/801:"},
		{name: "pivot", fail: "pivot", last: "pivot:"},
		{name: "old root detach", fail: "unmount:/.pivot_old", last: "unmount:/.pivot_old:"},
		{name: "proc mount", fail: "mount:/proc", last: "mount:proc:/proc:"},
		{name: "dev mount", fail: "mount:/dev", last: "mount:tmpfs:/dev:"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ops := newFakeOps()
			ops.fail[test.fail] = errors.New("injected " + test.name)
			locker, err := prepareTestRoot(ops, testRootfsConfig())
			if err == nil {
				t.Fatalf("PrepareRoot() error = nil, want injected failure; mutations = %v", ops.mutations)
			}
			if locker.locks != 1 || locker.unlocks != 0 {
				t.Fatalf("helper lock transitions = (%d, %d), want discarded PID 1 thread (1, 0)", locker.locks, locker.unlocks)
			}
			if len(ops.mutations) == 0 || !strings.Contains(ops.mutations[len(ops.mutations)-1], test.last) {
				t.Fatalf("failure did not stop at %q: %v", test.fail, ops.mutations)
			}
		})
	}
}

// TestPrepareRootFailureCannotReplay verifies a partial pivot attempt is sealed and later retries add no host mutation.
func TestPrepareRootFailureCannotReplay(t *testing.T) {
	ops := newFakeOps()
	ops.pid = 1
	ops.fail["mount:/proc"] = errors.New("injected post-pivot failure")
	bootstrap := testPID1ChildBootstrap(ops)
	locker := &fakeThreadLocker{}
	err := runPID1Child(context.Background(), ops, locker, defaultNamespaceCleanupTimeout, bootstrap, func(actionCtx context.Context, helper *LockedHelper) error {
		firstErr := helper.PrepareRoot(actionCtx, bootstrap.Rootfs)
		if firstErr == nil {
			return errors.New("first PrepareRoot unexpectedly succeeded")
		}
		mutationCount := len(ops.mutations)
		delete(ops.fail, "mount:/proc")
		if retryErr := helper.PrepareRoot(actionCtx, bootstrap.Rootfs); !errors.Is(retryErr, ErrUnsafeIdentity) {
			return fmt.Errorf("retry PrepareRoot error = %v, want ErrUnsafeIdentity", retryErr)
		}
		if len(ops.mutations) != mutationCount {
			return fmt.Errorf("retry PrepareRoot added mutations: %v", ops.mutations[mutationCount:])
		}
		return firstErr
	})
	if err == nil || !strings.Contains(err.Error(), "post-pivot failure") {
		t.Fatalf("runPID1Child() error = %v, want injected post-pivot failure", err)
	}
	if !containsCall(ops.mutations, "pivot:") {
		t.Fatalf("failure happened before pivot: %v", ops.mutations)
	}
}

// TestPrepareRootRejectsExpiredHelper verifies rootfs mutation cannot be invoked without the executor-owned marker.
func TestPrepareRootRejectsExpiredHelper(t *testing.T) {
	ops := newFakeOps()
	helper := &LockedHelper{ops: ops, threadID: ops.tid}
	err := helper.PrepareRoot(context.Background(), testRootfsConfig())
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("PrepareRoot() error = %v, want ErrClosed", err)
	}
	if len(ops.mutations) != 0 {
		t.Fatalf("expired helper mutations = %v, want none", ops.mutations)
	}
}

// integerString formats a signed test constant without embedding platform-specific flag numbers.
func integerString(value int) string { return fmtSprint(value) }

// unsignedString formats a mount-flag test constant without embedding platform-specific flag numbers.
func unsignedString(value uintptr) string { return fmtSprint(value) }

// fmtSprint is the single formatting helper used by syscall-order expectations.
func fmtSprint(value any) string { return fmt.Sprint(value) }
