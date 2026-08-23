package lifecycle

import (
	"context"
	"reflect"
	"testing"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
)

// TestReconcileConditionPersistsPostEventReplay verifies recovery facts and their observation cursor replay exactly after later state changes.
func TestReconcileConditionPersistsPostEventReplay(t *testing.T) {
	coordinator, _ := testCoordinator(t)
	sandbox := createReadySandbox(t, coordinator, "sandbox-reconcile-condition")
	target := operation.Target{Kind: operation.TargetSandbox, ID: string(sandbox.ID)}
	request := ReconcileConditionRequest{
		OperationID: "op-reconcile-condition-first",
		Target:      target,
		Condition: &domain.Condition{
			Type: domain.ConditionProcessIdentityUnknown, Reason: "DaemonRestart",
			Message: "runtime supervisor cannot be reconnected in M3",
		},
		Evidence:   "read-only discovery found retained runtime ownership",
		ObservedAt: testWallTime(),
	}
	first, err := coordinator.ReconcileCondition(context.Background(), request)
	if err != nil {
		t.Fatalf("ReconcileCondition() error = %v", err)
	}
	if first.Sandbox == nil || first.Operation.State != operation.StateSucceeded ||
		first.Sandbox.Status.LastObservation.OperationID != string(request.OperationID) {
		t.Fatalf("ReconcileCondition() = %#v", first)
	}
	original := first.Sandbox.Clone()
	secondRequest := ReconcileConditionRequest{
		OperationID: "op-reconcile-condition-second", Target: target,
		Condition: &domain.Condition{Type: domain.ConditionFailure, Reason: "LaterFact"},
		Evidence:  "a later independent recovery observation", ObservedAt: testWallTime(),
	}
	if _, err := coordinator.ReconcileCondition(context.Background(), secondRequest); err != nil {
		t.Fatalf("ReconcileCondition(second) error = %v", err)
	}
	replayed, err := coordinator.ReconcileCondition(context.Background(), request)
	if err != nil {
		t.Fatalf("ReconcileCondition(replay) error = %v", err)
	}
	if !reflect.DeepEqual(replayed.Sandbox, &original) || replayed.Operation.Response == nil {
		t.Fatalf("ReconcileCondition(replay) = %#v, want exact %#v", replayed, original)
	}
}

// TestFailActiveOperationStoresConditionAndExactResponse verifies an unrecoverable non-create intent becomes a stable failed result without inventing a phase transition.
func TestFailActiveOperationStoresConditionAndExactResponse(t *testing.T) {
	coordinator, _ := testCoordinator(t)
	sandbox := createReadySandbox(t, coordinator, "sandbox-reconcile-failure")
	begin, err := coordinator.BeginSandboxStop(context.Background(), SandboxActionRequest{
		OperationID: "op-reconcile-stop-failure", SandboxID: sandbox.ID,
	})
	if err != nil {
		t.Fatalf("BeginSandboxStop() error = %v", err)
	}
	request := FailOperationRequest{
		OperationID: begin.Operation.ID,
		Target:      begin.Operation.Target,
		Fingerprint: begin.Fingerprint,
		Failure:     Failure{Reason: operation.ReasonInternal, Message: "provider observation is permanently invalid"},
		Condition:   domain.Condition{Type: domain.ConditionFailure, Reason: "RecoveryRejected", Message: "operator action required"},
		ObservedAt:  testWallTime(),
	}
	failed, err := coordinator.FailActiveOperation(context.Background(), request)
	if err != nil {
		t.Fatalf("FailActiveOperation() error = %v", err)
	}
	if failed.State != operation.StateFailed || failed.Reason != operation.ReasonInternal || len(failed.Response) == 0 {
		t.Fatalf("FailActiveOperation() = %#v", failed)
	}
	current, err := coordinator.GetSandbox(context.Background(), sandbox.ID)
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if current.Status.Phase != domain.SandboxStopping || current.Status.LastObservation.OperationID != string(request.OperationID) {
		t.Fatalf("failed stop Sandbox = %#v", current.Status)
	}
	replayed, err := coordinator.FailActiveOperation(context.Background(), request)
	if err != nil {
		t.Fatalf("FailActiveOperation(replay) error = %v", err)
	}
	if !reflect.DeepEqual(replayed, failed) {
		t.Fatalf("FailActiveOperation(replay) = %#v, want %#v", replayed, failed)
	}
}
