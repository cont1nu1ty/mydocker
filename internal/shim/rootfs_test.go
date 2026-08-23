package shim

import (
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"mydocker/internal/isolation"
)

// TestRootfsPreparationRejectsForgedOrWrongRequestACK verifies digest-shaped
// fields cannot substitute for recomputed effect evidence or exact command binding.
func TestRootfsPreparationRejectsForgedOrWrongRequestACK(t *testing.T) {
	request := testRootfsRequest()
	preparation, err := newRootfsPreparation(request, testTime())
	if err != nil {
		t.Fatal(err)
	}
	forged := preparation
	forged.EvidenceSHA256 = strings.Repeat("b", 64)
	if err := forged.Validate(); err == nil {
		t.Fatal("digest-shaped forged ACK was accepted")
	}
	wrong := request.Clone()
	wrong.DNS = []string{"9.9.9.9"}
	if err := preparation.ValidateFor(wrong); err == nil {
		t.Fatal("ACK from another rootfs command was accepted")
	}
}

// fakeRootfsPreparer records one pure preparation attempt and exposes an injected permanent failure.
type fakeRootfsPreparer struct {
	mu       sync.Mutex
	requests []RootfsRequest
	err      error
}

// PrepareRootfs records the immutable command without invoking mount, pivot_root, or any host syscall.
func (preparer *fakeRootfsPreparer) PrepareRootfs(request RootfsRequest) error {
	preparer.mu.Lock()
	defer preparer.mu.Unlock()
	preparer.requests = append(preparer.requests, request.Clone())
	return preparer.err
}

// count reports how many privileged preparation attempts the wrapper delegated.
func (preparer *fakeRootfsPreparer) count() int {
	preparer.mu.Lock()
	defer preparer.mu.Unlock()
	return len(preparer.requests)
}

// testRootfsRequest returns one valid deferred command with already-checkpointed namespace identities.
func testRootfsRequest() RootfsRequest {
	return RootfsRequest{
		SourceID: "prepared-rootfs-one",
		Source: isolation.RootfsConfig{
			AllowedRoot: "/trusted/prepared", Rootfs: "/trusted/prepared/rootfs",
		},
		DNS:               []string{"1.1.1.1", "2001:4860:4860::8888"},
		PIDNamespaceInode: 101, MountNamespaceInode: 202,
		ConfigurationSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
}

// TestRootfsPreparationGatesReleaseAndReplaysSemantically verifies PID1 ACK is
// mandatory, one mount attempt serves new request IDs, and conflicting input fails closed.
func TestRootfsPreparationGatesReleaseAndReplaysSemantically(t *testing.T) {
	preparer := &fakeRootfsPreparer{}
	wrapper, err := NewInit(testInitSpec(t, "op-rootfs-gate", "container-rootfs-gate", "attempt-rootfs-gate"), InitDependencies{
		Runner: &fakeRunner{child: newFakeChild()}, Stdout: io.Discard, Stderr: io.Discard,
		Terminal: &memoryTerminalStore{}, Clock: fixedClock{now: testTime()}, Rootfs: preparer,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapper.Release(); !IsCode(err, CodeRootfsFailed) {
		t.Fatalf("Release(before rootfs) error = %v, want rootfs_failed", err)
	}
	request := testRootfsRequest()
	first, err := wrapper.PrepareRootfs(request)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := wrapper.PrepareRootfs(request.Clone())
	if err != nil || replayed != first || preparer.count() != 1 {
		t.Fatalf("semantic replay = (%#v, %v), calls=%d", replayed, err, preparer.count())
	}
	conflict := request.Clone()
	conflict.DNS = []string{"9.9.9.9"}
	if _, err := wrapper.PrepareRootfs(conflict); !IsCode(err, CodeDuplicateRequest) {
		t.Fatalf("conflicting preparation error = %v, want duplicate_request", err)
	}
	observation, err := wrapper.Inspect()
	if err != nil || observation.Rootfs == nil || *observation.Rootfs != first {
		t.Fatalf("Inspect() rootfs = (%#v, %v), want %#v", observation.Rootfs, err, first)
	}
	if _, err := wrapper.Release(); err != nil {
		t.Fatalf("Release(after rootfs ACK) error = %v", err)
	}
}

// TestRootfsPreparationFailureIsOneShot verifies a partial privileged failure
// can never be retried by changing either request ID or semantic content.
func TestRootfsPreparationFailureIsOneShot(t *testing.T) {
	injected := errors.New("injected pivot failure")
	preparer := &fakeRootfsPreparer{err: injected}
	wrapper, err := NewInit(testInitSpec(t, "op-rootfs-fail", "container-rootfs-fail", "attempt-rootfs-fail"), InitDependencies{
		Runner: &fakeRunner{child: newFakeChild()}, Stdout: io.Discard, Stderr: io.Discard,
		Terminal: &memoryTerminalStore{}, Clock: fixedClock{now: testTime()}, Rootfs: preparer,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapper.PrepareRootfs(testRootfsRequest()); !IsCode(err, CodeRootfsFailed) {
		t.Fatalf("first preparation error = %v, want rootfs_failed", err)
	}
	if _, err := wrapper.PrepareRootfs(testRootfsRequest()); !IsCode(err, CodeRootfsFailed) || preparer.count() != 1 {
		t.Fatalf("retry error = %v, calls=%d; privileged effect repeated", err, preparer.count())
	}
}
