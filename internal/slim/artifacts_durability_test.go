package slim

import (
	"errors"
	"path/filepath"
	"testing"

	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
)

// TestArtifactDirectoryCreationSyncsEachParent verifies an artifact cannot be
// reported durable after only syncing its file and innermost directory.
func TestArtifactDirectoryCreationSyncsEachParent(t *testing.T) {
	root := privateSlimRoot(t)
	store, err := newArtifactStore(root)
	if err != nil {
		t.Fatalf("newArtifactStore() error = %v", err)
	}
	owner := testOwner(t, "artifact-sync-create", operation.TargetContainer, "container-artifact-sync")
	evidence, err := ownership.EvidenceDigest(map[string]string{"artifact": "gate"})
	if err != nil {
		t.Fatalf("EvidenceDigest() error = %v", err)
	}
	var synced []string
	store.syncDirectory = func(path string) error {
		synced = append(synced, filepath.Clean(path))
		return syncArtifactDirectory(path)
	}
	if _, err := store.Ensure(owner, ownership.KindStartGate, evidence, "closed"); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	want := map[string]bool{
		filepath.Clean(root):                       false,
		filepath.Join(root, "owners"):              false,
		filepath.Join(root, "owners", owner.Token): false,
	}
	for _, path := range synced {
		if _, tracked := want[path]; tracked {
			want[path] = true
		}
	}
	for path, found := range want {
		if !found {
			t.Fatalf("directory %q was not synced; calls=%v", path, synced)
		}
	}
}

// TestStartGateTransitionPersistsConsumptionBeforeRelease verifies the gate
// can only advance closed -> consuming -> released and an init launch can no
// longer treat the durable consumption intent as a fresh closed gate.
func TestStartGateTransitionPersistsConsumptionBeforeRelease(t *testing.T) {
	root := privateSlimRoot(t)
	store, err := newArtifactStore(root)
	if err != nil {
		t.Fatalf("newArtifactStore() error = %v", err)
	}
	owner := testOwner(t, "artifact-gate-consume", operation.TargetContainer, "container-artifact-gate")
	receipt, err := newSlimReceipt(owner, ownership.KindStartGate, map[string]string{attemptIDAttribute: "attempt-artifact-gate"})
	if err != nil {
		t.Fatalf("newSlimReceipt() error = %v", err)
	}
	if _, err := store.Ensure(owner, ownership.KindStartGate, receipt.EvidenceSHA256, artifactStateClosed); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if err := store.Transition(owner, ownership.KindStartGate, receipt.EvidenceSHA256, artifactStateReleased); !errors.Is(err, ErrArtifactUnsafe) {
		t.Fatalf("direct closed -> released error = %v, want ErrArtifactUnsafe", err)
	}
	if err := store.Transition(owner, ownership.KindStartGate, receipt.EvidenceSHA256, artifactStateConsuming); err != nil {
		t.Fatalf("Transition(consuming) error = %v", err)
	}
	record, found, err := store.Read(owner, ownership.KindStartGate)
	if err != nil || !found || record.State != artifactStateConsuming {
		t.Fatalf("Read(consuming) = (%+v, %t, %v)", record, found, err)
	}
	if err := validateInitArtifact(store, owner, receipt, ownership.KindStartGate, "attempt-artifact-gate", artifactStateClosed); err == nil {
		t.Fatal("init artifact validation accepted a consuming gate as closed")
	}
	if err := store.Transition(owner, ownership.KindStartGate, receipt.EvidenceSHA256, artifactStateReleased); err != nil {
		t.Fatalf("Transition(released) error = %v", err)
	}
	if err := store.Transition(owner, ownership.KindStartGate, receipt.EvidenceSHA256, artifactStateConsuming); !errors.Is(err, ErrArtifactUnsafe) {
		t.Fatalf("released -> consuming error = %v, want ErrArtifactUnsafe", err)
	}
}

// TestArtifactRemovalRetryConfirmsDirectoryDurability verifies a sync failure
// after unlink remains retryable and an already-absent retry fsyncs the parents.
func TestArtifactRemovalRetryConfirmsDirectoryDurability(t *testing.T) {
	root := privateSlimRoot(t)
	store, err := newArtifactStore(root)
	if err != nil {
		t.Fatalf("newArtifactStore() error = %v", err)
	}
	owner := testOwner(t, "artifact-sync-remove", operation.TargetContainer, "container-artifact-remove")
	evidence, err := ownership.EvidenceDigest(map[string]string{"artifact": "streams"})
	if err != nil {
		t.Fatalf("EvidenceDigest() error = %v", err)
	}
	if _, err := store.Ensure(owner, ownership.KindStreams, evidence, "ready"); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	ownerRoot := filepath.Join(root, "owners", owner.Token)
	injected := errors.New("injected directory sync failure")
	failed := false
	store.syncDirectory = func(path string) error {
		if filepath.Clean(path) == filepath.Clean(ownerRoot) && !failed {
			failed = true
			return injected
		}
		return syncArtifactDirectory(path)
	}
	if disposition, err := store.Remove(owner, ownership.KindStreams, evidence); !errors.Is(err, injected) || disposition != "" {
		t.Fatalf("Remove(first) = (%q, %v), want injected uncertainty", disposition, err)
	}
	store.syncDirectory = syncArtifactDirectory
	disposition, err := store.Remove(owner, ownership.KindStreams, evidence)
	if err != nil || disposition != provider.CleanupAlreadyAbsent {
		t.Fatalf("Remove(retry) = (%q, %v), want already_absent durability confirmation", disposition, err)
	}
}
