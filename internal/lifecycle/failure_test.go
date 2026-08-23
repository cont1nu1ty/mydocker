package lifecycle

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/rollback"
	"mydocker/internal/state"
)

// TestFailSandboxCreateAfterRollbackRequiresAbsenceAndReplays verifies preflight failure deletes only Creating metadata and persists one exact failure result.
func TestFailSandboxCreateAfterRollbackRequiresAbsenceAndReplays(t *testing.T) {
	_, store := testCoordinator(t)
	coordinator := testCoordinatorForStoreProfile(t, store, state.HostProfileLinuxM2)
	request := SandboxCreateRequest{OperationID: "op-fail-sandbox", SandboxID: "sandbox-fail", Spec: testSandboxSpec()}
	begin, err := coordinator.BeginSandboxCreate(context.Background(), request)
	if err != nil {
		t.Fatalf("BeginSandboxCreate() error = %v", err)
	}
	failureRequest := SandboxCreateFailureRequest{
		OperationID: request.OperationID, SandboxID: request.SandboxID, Fingerprint: begin.Fingerprint,
		Failure:      Failure{Reason: operation.ReasonPrecondition, Message: "rootful preflight is unavailable"},
		Verification: testVerification(VerificationSandboxAbsent, nil),
	}
	failed, err := coordinator.FailSandboxCreateAfterRollback(context.Background(), failureRequest)
	if err == nil {
		t.Fatal("FailSandboxCreateAfterRollback() error = nil, want stable operation failure")
	}
	if failed.Operation.State != operation.StateFailed || failed.Operation.Result != operation.ResultFailed || failed.Operation.Reason != operation.ReasonPrecondition || !failed.Removed {
		t.Fatalf("failed Sandbox result = %#v", failed)
	}
	if _, err := coordinator.GetSandbox(context.Background(), request.SandboxID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetSandbox(after failed create) error = %v, want ErrNotFound", err)
	}
	replayed, err := coordinator.FailSandboxCreateAfterRollback(context.Background(), failureRequest)
	if err == nil || replayed.Resolution != operation.ResolutionReplay || !reflect.DeepEqual(replayed.Operation.Response, failed.Operation.Response) {
		t.Fatalf("FailSandboxCreateAfterRollback(retry) = (%#v, %v)", replayed, err)
	}
}

// TestFailContainerCreateAfterCompletedRollbackRetainsHistory verifies verified cleanup produces a stopped not-run Attempt and immutable failure condition.
func TestFailContainerCreateAfterCompletedRollbackRetainsHistory(t *testing.T) {
	abstractCoordinator, store := testCoordinator(t)
	sandbox := createReadySandbox(t, abstractCoordinator, "sandbox-fail-container")
	coordinator := testCoordinatorForStoreProfile(t, store, state.HostProfileLinuxM2)
	request := ContainerCreateRequest{
		OperationID: "op-fail-container", SandboxID: sandbox.ID, ContainerID: "container-fail", AttemptID: "attempt-fail",
		Process: testProcessSpec(),
	}
	begin, err := coordinator.BeginContainerCreate(context.Background(), request)
	if err != nil {
		t.Fatalf("BeginContainerCreate() error = %v", err)
	}
	target := operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}
	receipt, rollbackRecord := testCheckpointAcquisition(
		t, request.OperationID, target, ownership.ProviderCgroupV2, ownership.KindAttemptCgroup,
		"attempt-cgroup-failure", ownership.ActionRemoveCgroup,
	)
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: request.OperationID, Target: target, Fingerprint: begin.Fingerprint,
		Stage: operation.StagePrepareCgroup, Rollback: []rollback.Record{rollbackRecord}, Receipts: []ownership.Receipt{receipt},
	}); err != nil {
		t.Fatalf("CheckpointOperation(acquired) error = %v", err)
	}
	rollbackRecord.Started = true
	rollbackRecord.Succeeded = true
	rollbackCause, err := rollback.NewCause(operation.ReasonInternal, "namespace preparation failed")
	if err != nil {
		t.Fatalf("NewCause() error = %v", err)
	}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: request.OperationID, Target: target, Fingerprint: begin.Fingerprint,
		Stage: operation.StageRollback, RollbackCause: &rollbackCause, Rollback: []rollback.Record{rollbackRecord}, Receipts: []ownership.Receipt{receipt},
	}); err != nil {
		t.Fatalf("CheckpointOperation(rollback complete) error = %v", err)
	}
	failureRequest := ContainerCreateFailureRequest{
		OperationID: request.OperationID, ContainerID: request.ContainerID, Fingerprint: begin.Fingerprint,
		Failure:      Failure{Reason: operation.ReasonInternal, Message: "namespace preparation failed"},
		Verification: testVerification(VerificationAttemptAbsent, nil),
	}
	failed, err := coordinator.FailContainerCreateAfterRollback(context.Background(), failureRequest)
	if err == nil {
		t.Fatal("FailContainerCreateAfterRollback() error = nil, want stable operation failure")
	}
	if failed.Operation.State != operation.StateFailed || failed.ContainerAttempt == nil || failed.ContainerAttempt.Attempt.Phase != domain.AttemptStopped || failed.ContainerAttempt.Attempt.Outcome.Presence != domain.OutcomeNotApplicable {
		t.Fatalf("failed Container result = %#v", failed)
	}
	conditions := failed.ContainerAttempt.Attempt.Conditions
	if len(conditions) != 1 || conditions[0].Type != domain.ConditionFailure || conditions[0].Reason != string(operation.ReasonInternal) {
		t.Fatalf("failure conditions = %#v", conditions)
	}
	replayed, err := coordinator.FailContainerCreateAfterRollback(context.Background(), failureRequest)
	if err == nil || replayed.Resolution != operation.ResolutionReplay || !reflect.DeepEqual(replayed.ContainerAttempt, failed.ContainerAttempt) {
		t.Fatalf("FailContainerCreateAfterRollback(retry) = (%#v, %v)", replayed, err)
	}
}

// TestFailContainerCreateRejectsPendingRollback verifies terminal failure cannot strand a started inverse and the transaction remains resumable.
func TestFailContainerCreateRejectsPendingRollback(t *testing.T) {
	abstractCoordinator, store := testCoordinator(t)
	sandbox := createReadySandbox(t, abstractCoordinator, "sandbox-pending-rollback")
	coordinator := testCoordinatorForStoreProfile(t, store, state.HostProfileLinuxM2)
	request := ContainerCreateRequest{
		OperationID: "op-pending-rollback", SandboxID: sandbox.ID, ContainerID: "container-pending", AttemptID: "attempt-pending",
		Process: testProcessSpec(),
	}
	begin, err := coordinator.BeginContainerCreate(context.Background(), request)
	if err != nil {
		t.Fatalf("BeginContainerCreate() error = %v", err)
	}
	target := operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}
	receipt, rollbackRecord := testCheckpointAcquisition(
		t, request.OperationID, target, ownership.ProviderCgroupV2, ownership.KindAttemptCgroup,
		"attempt-cgroup-pending", ownership.ActionRemoveCgroup,
	)
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: request.OperationID, Target: target, Fingerprint: begin.Fingerprint,
		Stage: operation.StagePrepareCgroup, Rollback: []rollback.Record{rollbackRecord}, Receipts: []ownership.Receipt{receipt},
	}); err != nil {
		t.Fatalf("CheckpointOperation(acquired) error = %v", err)
	}
	rollbackRecord.Started = true
	rollbackCause, err := rollback.NewCause(operation.ReasonCleanup, "owned cgroup remains busy")
	if err != nil {
		t.Fatalf("NewCause() error = %v", err)
	}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: request.OperationID, Target: target, Fingerprint: begin.Fingerprint,
		Stage: operation.StageRollback, RollbackCause: &rollbackCause, Rollback: []rollback.Record{rollbackRecord}, Receipts: []ownership.Receipt{receipt},
	}); err != nil {
		t.Fatalf("CheckpointOperation(rollback pending) error = %v", err)
	}
	_, err = coordinator.FailContainerCreateAfterRollback(context.Background(), ContainerCreateFailureRequest{
		OperationID: request.OperationID, ContainerID: request.ContainerID, Fingerprint: begin.Fingerprint,
		Failure:      Failure{Reason: operation.ReasonCleanup, Message: "owned cgroup remains busy"},
		Verification: testVerification(VerificationAttemptAbsent, nil),
	})
	if !errors.Is(err, state.ErrInvalidRecord) {
		t.Fatalf("FailContainerCreateAfterRollback(pending cleanup) error = %v, want ErrInvalidRecord", err)
	}
	pair, getErr := coordinator.GetContainer(context.Background(), request.ContainerID)
	if getErr != nil || pair.Attempt.Phase != domain.AttemptCreating {
		t.Fatalf("GetContainer(after rejected failure) = (%#v, %v)", pair, getErr)
	}
	progress, getErr := coordinator.GetOperationProgress(context.Background(), request.OperationID)
	if getErr != nil || progress.Operation.State != operation.StateRunning || !progress.Rollback[0].Started || progress.Rollback[0].Succeeded {
		t.Fatalf("GetOperationProgress(after rejected failure) = (%#v, %v)", progress, getErr)
	}
}
