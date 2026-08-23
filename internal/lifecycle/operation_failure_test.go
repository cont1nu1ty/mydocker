package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"mydocker/internal/operation"
	"mydocker/internal/state"
)

// TestOperationFailureClassificationSurvivesRetryAndReopen verifies the first
// terminal response, same-process replay, and disk-backed replay expose one
// identical bounded error rather than reconstructing it from mutable state.
func TestOperationFailureClassificationSurvivesRetryAndReopen(t *testing.T) {
	path := secureLifecycleStatePath(t)
	store, err := state.NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	coordinator, err := NewCoordinatorForProfile(store, state.HostProfileLinuxM2)
	if err != nil {
		t.Fatalf("NewCoordinatorForProfile() error = %v", err)
	}
	create := SandboxCreateRequest{OperationID: "op-durable-failure", SandboxID: "sandbox-durable-failure", Spec: testSandboxSpec()}
	begin, err := coordinator.BeginSandboxCreate(context.Background(), create)
	if err != nil {
		t.Fatalf("BeginSandboxCreate() error = %v", err)
	}
	failure := SandboxCreateFailureRequest{
		OperationID: create.OperationID, SandboxID: create.SandboxID, Fingerprint: begin.Fingerprint,
		Failure:      Failure{Reason: operation.ReasonPrecondition, Message: "rootful host preflight failed"},
		Verification: testVerification(VerificationSandboxAbsent, nil),
	}
	for phase := 0; phase < 2; phase++ {
		_, err = coordinator.FailSandboxCreateAfterRollback(context.Background(), failure)
		var classified *OperationFailureError
		if !errors.As(err, &classified) || classified.OperationID != create.OperationID || classified.Reason != operation.ReasonPrecondition {
			t.Fatalf("failure replay %d error = %#v, want durable precondition classification", phase, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	store, err = state.NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(reopen) error = %v", err)
	}
	defer store.Close()
	coordinator, err = NewCoordinatorForProfile(store, state.HostProfileLinuxM2)
	if err != nil {
		t.Fatalf("NewCoordinatorForProfile(reopen) error = %v", err)
	}
	_, err = coordinator.FailSandboxCreateAfterRollback(context.Background(), failure)
	var classified *OperationFailureError
	if !errors.As(err, &classified) || classified.OperationID != create.OperationID || classified.Reason != operation.ReasonPrecondition {
		t.Fatalf("failure replay after reopen error = %#v, want durable precondition classification", err)
	}
}

// secureLifecycleStatePath creates an owner-only path below the package
// workspace because FileStore intentionally rejects the world-writable /tmp ancestry.
func secureLifecycleStatePath(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp(".", ".lifecycle-state-test-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		_ = os.RemoveAll(directory)
		t.Fatalf("Abs() error = %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(absolute); err != nil {
			t.Errorf("RemoveAll() error = %v", err)
		}
		anchors, globErr := filepath.Glob(filepath.Join(filepath.Dir(absolute), "."+filepath.Base(absolute)+"-*.state.lock"))
		if globErr != nil {
			t.Errorf("Glob(state lock) error = %v", globErr)
			return
		}
		for _, anchor := range anchors {
			if err := os.Remove(anchor); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Errorf("Remove(state lock) error = %v", err)
			}
		}
	})
	return filepath.Join(absolute, "state.json")
}
