package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/rollback"
	"mydocker/internal/state"
)

// testCheckpointReceipt returns one owner-bound provider observation for recovery tests.
func testCheckpointReceipt(t *testing.T, operationID operation.OperationID, target operation.Target, provider ownership.Provider, kind ownership.Kind, localID string) ownership.Receipt {
	t.Helper()
	owner, err := ownership.NewOwnerKey(operationID, target, domain.InitialGeneration)
	if err != nil {
		t.Fatalf("NewOwnerKey() error = %v", err)
	}
	evidence, err := ownership.EvidenceDigest(map[string]string{"local_id": localID, "kind": string(kind)})
	if err != nil {
		t.Fatalf("EvidenceDigest() error = %v", err)
	}
	return ownership.Receipt{
		SchemaVersion:  ownership.SchemaVersion,
		Provider:       provider,
		Kind:           kind,
		LocalID:        localID,
		Owner:          owner,
		EvidenceSHA256: evidence,
	}
}

// testCheckpointAcquisition returns one owner-bound receipt and its bounded inverse for recovery tests.
func testCheckpointAcquisition(t *testing.T, operationID operation.OperationID, target operation.Target, provider ownership.Provider, kind ownership.Kind, localID string, action ownership.Action) (ownership.Receipt, rollback.Record) {
	t.Helper()
	receipt := testCheckpointReceipt(t, operationID, target, provider, kind, localID)
	descriptor, err := ownership.InverseDescriptor(receipt, action)
	if err != nil {
		t.Fatalf("InverseDescriptor() error = %v", err)
	}
	return receipt, rollback.Record{Descriptor: descriptor}
}

// testCheckpointProfile builds the complete ordered M2 receipt and actionable inverse journals for one lifecycle target.
func testCheckpointProfile(t *testing.T, operationID operation.OperationID, target operation.Target) ([]ownership.Receipt, []rollback.Record) {
	t.Helper()
	type acquisition struct {
		provider ownership.Provider
		kind     ownership.Kind
		action   ownership.Action
	}
	var acquisitions []acquisition
	if target.Kind == operation.TargetSandbox {
		acquisitions = []acquisition{
			{ownership.ProviderCgroupV2, ownership.KindSandboxCgroup, ownership.ActionRemoveCgroup},
			{ownership.ProviderCgroupV2, ownership.KindKeeperCgroup, ownership.ActionRemoveCgroup},
			{ownership.ProviderLinux, ownership.KindKeeperProcess, ownership.ActionStopProcess},
			{ownership.ProviderLinux, ownership.KindUTSNamespace, ""},
			{ownership.ProviderLinux, ownership.KindIPCNamespace, ""},
			{ownership.ProviderLinux, ownership.KindNetworkNamespace, ""},
		}
	} else {
		acquisitions = []acquisition{
			{ownership.ProviderCgroupV2, ownership.KindAttemptCgroup, ownership.ActionRemoveCgroup},
			{ownership.ProviderLinux, ownership.KindStartGate, ownership.ActionCloseGate},
			{ownership.ProviderLinux, ownership.KindStreams, ownership.ActionCloseStreams},
			{ownership.ProviderLinux, ownership.KindInitProcess, ownership.ActionStopProcess},
			{ownership.ProviderLinux, ownership.KindPIDNamespace, ""},
			{ownership.ProviderLinux, ownership.KindMountNamespace, ""},
			{ownership.ProviderLinux, ownership.KindRootfsMount, ownership.ActionUnmountRoot},
		}
	}
	var receipts []ownership.Receipt
	var records []rollback.Record
	for index, item := range acquisitions {
		localID := fmt.Sprintf("%s-%d", item.kind, index)
		if item.action == "" {
			receipts = append(receipts, testCheckpointReceipt(t, operationID, target, item.provider, item.kind, localID))
			continue
		}
		receipt, record := testCheckpointAcquisition(t, operationID, target, item.provider, item.kind, localID, item.action)
		receipts = append(receipts, receipt)
		records = append(records, record)
	}
	return receipts, records
}

// checkpointTestProfile acknowledges one acquisition per transaction so crash tests cannot hide multiple host effects in one checkpoint.
func checkpointTestProfile(t *testing.T, coordinator *Coordinator, operationID operation.OperationID, target operation.Target, fingerprint operation.RequestFingerprint, receipts []ownership.Receipt, rollbackRecords []rollback.Record) {
	t.Helper()
	var acknowledgedReceipts []ownership.Receipt
	var acknowledgedRollback []rollback.Record
	rollbackIndex := 0
	for index, receipt := range receipts {
		acknowledgedReceipts = append(acknowledgedReceipts, receipt.Clone())
		if testReceiptNeedsInverse(receipt.Kind) {
			if rollbackIndex >= len(rollbackRecords) {
				t.Fatalf("receipt %d kind %s lacks test rollback setup", index, receipt.Kind)
			}
			acknowledgedRollback = append(acknowledgedRollback, rollbackRecords[rollbackIndex].Clone())
			rollbackIndex++
		}
		if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
			OperationID: operationID, Target: target, Fingerprint: fingerprint,
			Stage: testReceiptCheckpointStage(receipt.Kind), Rollback: acknowledgedRollback, Receipts: acknowledgedReceipts,
		}); err != nil {
			t.Fatalf("CheckpointOperation(receipt %d %s) error = %v", index, receipt.Kind, err)
		}
	}
	if rollbackIndex != len(rollbackRecords) {
		t.Fatalf("checkpointed %d rollback records, setup contains %d", rollbackIndex, len(rollbackRecords))
	}
}

// testReceiptNeedsInverse mirrors the direct-resource cleanup roles used by checkpoint fixtures.
func testReceiptNeedsInverse(kind ownership.Kind) bool {
	switch kind {
	case ownership.KindSandboxCgroup, ownership.KindKeeperCgroup, ownership.KindAttemptCgroup,
		ownership.KindKeeperProcess, ownership.KindInitProcess, ownership.KindRootfsMount,
		ownership.KindStartGate, ownership.KindStreams:
		return true
	default:
		return false
	}
}

// testReceiptCheckpointStage maps fixture roles to the earliest durable acknowledgement allowed by the Linux profile.
func testReceiptCheckpointStage(kind ownership.Kind) operation.Stage {
	switch kind {
	case ownership.KindSandboxCgroup, ownership.KindKeeperCgroup, ownership.KindAttemptCgroup:
		return operation.StagePrepareCgroup
	case ownership.KindStartGate:
		return operation.StagePrepareStartGate
	case ownership.KindStreams:
		return operation.StagePrepareStreams
	case ownership.KindKeeperProcess, ownership.KindInitProcess:
		return operation.StageCreateProcess
	case ownership.KindUTSNamespace, ownership.KindIPCNamespace, ownership.KindNetworkNamespace,
		ownership.KindPIDNamespace, ownership.KindMountNamespace:
		return operation.StagePrepareNamespaces
	case ownership.KindRootfsMount:
		return operation.StagePrepareRootfs
	default:
		return operation.StageComplete
	}
}

// testCleanupReleases creates exact delete-bound absence proof for a complete adopted inventory.
func testCleanupReleases(t *testing.T, deleteOperationID operation.OperationID, pending []ownership.Receipt) []ownership.Release {
	t.Helper()
	releases := make([]ownership.Release, len(pending))
	for index, receipt := range pending {
		adopted, err := receipt.Adopt()
		if err != nil {
			t.Fatalf("Receipt.Adopt(%d) setup error = %v", index, err)
		}
		release, err := ownership.NewRelease(deleteOperationID, adopted, map[string]any{"presence": "absent", "index": index})
		if err != nil {
			t.Fatalf("NewRelease(%d) setup error = %v", index, err)
		}
		releases[index] = release
	}
	return releases
}

// TestCheckpointOperationPersistsOrderedRecoveryProgress verifies host stages, inverse descriptors, events, and exact retry semantics.
func TestCheckpointOperationPersistsOrderedRecoveryProgress(t *testing.T) {
	_, store := testCoordinator(t)
	coordinator := testCoordinatorForStoreProfile(t, store, state.HostProfileLinuxM2)
	request := SandboxCreateRequest{OperationID: "op-checkpoint", SandboxID: "sandbox-checkpoint", Spec: testSandboxSpec()}
	begin, err := coordinator.BeginSandboxCreate(context.Background(), request)
	if err != nil {
		t.Fatalf("BeginSandboxCreate() error = %v", err)
	}
	target := operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)}
	cgroupReceipt, cgroupRollback := testCheckpointAcquisition(
		t, request.OperationID, target, ownership.ProviderCgroupV2, ownership.KindSandboxCgroup,
		"sandbox-cgroup", ownership.ActionRemoveCgroup,
	)
	firstRollback := []rollback.Record{cgroupRollback}
	firstReceipts := []ownership.Receipt{cgroupReceipt}
	firstDuration := operation.Duration(5 * time.Millisecond)
	first, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: request.OperationID,
		Target:      target,
		Fingerprint: begin.Fingerprint,
		Stage:       operation.StagePrepareCgroup,
		Rollback:    firstRollback,
		Receipts:    firstReceipts,
		Duration:    &firstDuration,
		Details:     map[string]string{"provider": "cgroupv2"},
	})
	if err != nil || !first.Changed || first.Operation.Stage != operation.StagePrepareCgroup {
		t.Fatalf("CheckpointOperation(first) = (%#v, %v)", first, err)
	}

	retryDuration := operation.Duration(9 * time.Millisecond)
	retry, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: request.OperationID,
		Target:      target,
		Fingerprint: begin.Fingerprint,
		Stage:       operation.StagePrepareCgroup,
		Rollback:    firstRollback,
		Receipts:    firstReceipts,
		Duration:    &retryDuration,
	})
	if err != nil || retry.Changed {
		t.Fatalf("CheckpointOperation(exact retry) = (%#v, %v), want unchanged", retry, err)
	}
	if err := store.View(context.Background(), func(reader state.Reader) error {
		events, readErr := reader.EventsAfter(0, 0)
		if readErr != nil {
			return readErr
		}
		if len(events) != 2 || events[1].Duration == nil || *events[1].Duration != firstDuration {
			t.Fatalf("events after exact retry = %#v, want one unchanged measured checkpoint", events)
		}
		return nil
	}); err != nil {
		t.Fatalf("View(events after exact retry) error = %v", err)
	}

	keeperReceipt, keeperRollback := testCheckpointAcquisition(
		t, request.OperationID, target, ownership.ProviderCgroupV2, ownership.KindKeeperCgroup,
		"keeper-cgroup", ownership.ActionRemoveCgroup,
	)
	secondRollback := append(cloneRollbackRecords(firstRollback), keeperRollback)
	secondReceipts := append(cloneOwnershipReceipts(firstReceipts), keeperReceipt)
	second, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: request.OperationID,
		Target:      target,
		Fingerprint: begin.Fingerprint,
		Stage:       operation.StagePrepareCgroup,
		Rollback:    secondRollback,
		Receipts:    secondReceipts,
		Details:     map[string]string{"provider": "cgroupv2", "resource": "keeper_leaf"},
	})
	if err != nil || !second.Changed || second.Operation.Stage != operation.StagePrepareCgroup {
		t.Fatalf("CheckpointOperation(second) = (%#v, %v)", second, err)
	}
	cleanup := domain.Condition{Type: domain.ConditionCleanupPending, Reason: "KeeperBusy", Message: "retry after observation"}
	rollbackCause, err := rollback.NewCause(operation.ReasonCleanup, "keeper cleanup pending")
	if err != nil {
		t.Fatalf("NewCause() error = %v", err)
	}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID:     request.OperationID,
		Target:          operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)},
		Fingerprint:     begin.Fingerprint,
		Stage:           operation.StageRollback,
		RollbackCause:   &rollbackCause,
		Rollback:        secondRollback,
		Receipts:        secondReceipts,
		UpsertCondition: &cleanup,
	}); err != nil {
		t.Fatalf("CheckpointOperation(upsert cleanup condition) error = %v", err)
	}
	conditioned, err := coordinator.GetSandbox(context.Background(), request.SandboxID)
	if err != nil || len(conditioned.Status.Conditions) != 1 || conditioned.Status.Conditions[0] != cleanup {
		t.Fatalf("GetSandbox(conditioned) = (%#v, %v)", conditioned, err)
	}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID:    request.OperationID,
		Target:         operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)},
		Fingerprint:    begin.Fingerprint,
		Stage:          operation.StageRollback,
		RollbackCause:  &rollbackCause,
		Rollback:       secondRollback,
		Receipts:       secondReceipts,
		ClearCondition: domain.ConditionCleanupPending,
	}); err != nil {
		t.Fatalf("CheckpointOperation(clear cleanup condition) error = %v", err)
	}

	progress, err := coordinator.GetOperationProgress(context.Background(), request.OperationID)
	if err != nil || !reflect.DeepEqual(progress.Rollback, secondRollback) || !reflect.DeepEqual(progress.Receipts, secondReceipts) {
		t.Fatalf("GetOperationProgress() = (%#v, %v)", progress, err)
	}
	progress.Rollback[0].Descriptor.Metadata[0] = 'x'
	progress.Receipts[0].Attributes = map[string]string{"mutated": "true"}
	again, err := coordinator.GetOperationProgress(context.Background(), request.OperationID)
	if err != nil || reflect.DeepEqual(progress.Rollback, again.Rollback) || reflect.DeepEqual(progress.Receipts, again.Receipts) {
		t.Fatalf("GetOperationProgress() retained caller alias: (%#v, %v)", again, err)
	}

	events, err := coordinator.ListEvents(context.Background(), 0, 0)
	if err != nil || len(events) != 5 {
		t.Fatalf("ListEvents() = (%#v, %v), want intent plus four checkpoints", events, err)
	}
}

// TestSandboxCreateAdoptsAndRemoveConsumesHostInventory verifies acquisition evidence transfers atomically and is cleared only after absence proof.
func TestSandboxCreateAdoptsAndRemoveConsumesHostInventory(t *testing.T) {
	_, store := testCoordinator(t)
	coordinator := testCoordinatorForStoreProfile(t, store, state.HostProfileLinuxM2)
	request := SandboxCreateRequest{OperationID: "op-owned-sandbox", SandboxID: "sandbox-owned", Spec: testSandboxSpec()}
	begin, err := coordinator.BeginSandboxCreate(context.Background(), request)
	if err != nil {
		t.Fatalf("BeginSandboxCreate() error = %v", err)
	}
	target := operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)}
	receipts, rollbackRecords := testCheckpointProfile(t, request.OperationID, target)
	checkpointTestProfile(t, coordinator, request.OperationID, target, begin.Fingerprint, receipts, rollbackRecords)
	ready, err := coordinator.ConfirmSandboxCreate(context.Background(), SandboxConfirmRequest{
		OperationID: request.OperationID, SandboxID: request.SandboxID, Fingerprint: begin.Fingerprint,
		Verification: testVerification(VerificationSandboxReady, nil),
	})
	if err != nil || ready.Sandbox == nil || ready.Sandbox.Status.Phase != domain.SandboxReady {
		t.Fatalf("ConfirmSandboxCreate() = (%#v, %v)", ready, err)
	}
	if err := store.View(context.Background(), func(reader state.Reader) error {
		operationRecord, err := reader.GetOperation(request.OperationID)
		if err != nil {
			return err
		}
		sandboxRecord, err := reader.GetSandbox(request.SandboxID)
		if err != nil {
			return err
		}
		if len(operationRecord.Receipts) != len(receipts) || len(sandboxRecord.HostResources) != len(receipts) {
			return errors.New("create receipts were not transferred exactly")
		}
		for _, receipt := range append(operationRecord.Receipts, sandboxRecord.HostResources...) {
			if !receipt.Adopted {
				return errors.New("create receipt remained pending after Ready")
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("owned Sandbox state error = %v", err)
	}

	stopping, err := coordinator.BeginSandboxStop(context.Background(), SandboxActionRequest{OperationID: "op-stop-owned-sandbox", SandboxID: request.SandboxID})
	if err != nil {
		t.Fatalf("BeginSandboxStop() error = %v", err)
	}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: "op-stop-owned-sandbox", Target: target, Fingerprint: stopping.Fingerprint,
		Stage: operation.StageObserveProcess,
	}); err != nil {
		t.Fatalf("CheckpointOperation(Sandbox stop observation) error = %v", err)
	}
	if _, err := coordinator.ConfirmSandboxStop(context.Background(), SandboxConfirmRequest{
		OperationID: "op-stop-owned-sandbox", SandboxID: request.SandboxID, Fingerprint: stopping.Fingerprint,
		Verification: testVerification(VerificationSandboxStopped, nil),
	}); err != nil {
		t.Fatalf("ConfirmSandboxStop() error = %v", err)
	}
	removing, err := coordinator.BeginSandboxRemove(context.Background(), SandboxActionRequest{OperationID: "op-remove-owned-sandbox", SandboxID: request.SandboxID})
	if err != nil {
		t.Fatalf("BeginSandboxRemove() error = %v", err)
	}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: "op-remove-owned-sandbox", Target: target, Fingerprint: removing.Fingerprint,
		Stage: operation.StageTeardown, Releases: testCleanupReleases(t, "op-remove-owned-sandbox", receipts),
	}); err != nil {
		t.Fatalf("CheckpointOperation(Sandbox teardown) error = %v", err)
	}
	if _, err := coordinator.ConfirmSandboxRemove(context.Background(), SandboxConfirmRequest{
		OperationID: "op-remove-owned-sandbox", SandboxID: request.SandboxID, Fingerprint: removing.Fingerprint,
		Verification: testVerification(VerificationSandboxAbsent, nil),
	}); err != nil {
		t.Fatalf("ConfirmSandboxRemove() error = %v", err)
	}
	if _, err := coordinator.GetSandbox(context.Background(), request.SandboxID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetSandbox(removed) error = %v, want ErrNotFound", err)
	}
}

// TestContainerCreateAdoptsAndDeleteConsumesCompleteAttemptInventory verifies every per-Attempt role survives create and disappears only after teardown proof.
func TestContainerCreateAdoptsAndDeleteConsumesCompleteAttemptInventory(t *testing.T) {
	abstractCoordinator, store := testCoordinator(t)
	sandbox := createReadySandbox(t, abstractCoordinator, "sandbox-owned-attempt")
	coordinator := testCoordinatorForStoreProfile(t, store, state.HostProfileLinuxM2)
	request := ContainerCreateRequest{
		OperationID: "op-owned-attempt", SandboxID: sandbox.ID, ContainerID: "container-owned", AttemptID: "attempt-owned",
		Process: testProcessSpec(),
	}
	begin, err := coordinator.BeginContainerCreate(context.Background(), request)
	if err != nil {
		t.Fatalf("BeginContainerCreate() error = %v", err)
	}
	target := operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}
	receipts, rollbackRecords := testCheckpointProfile(t, request.OperationID, target)
	checkpointTestProfile(t, coordinator, request.OperationID, target, begin.Fingerprint, receipts, rollbackRecords)
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: request.OperationID, Target: target, Fingerprint: begin.Fingerprint,
		Stage: operation.StageAttachCgroup, Rollback: rollbackRecords, Receipts: receipts,
	}); err != nil {
		t.Fatalf("CheckpointOperation(attach cgroup) error = %v", err)
	}
	created, err := coordinator.ConfirmContainerCreate(context.Background(), ContainerConfirmRequest{
		OperationID: request.OperationID, ContainerID: request.ContainerID, Fingerprint: begin.Fingerprint,
		Verification: testVerification(VerificationAttemptCreated, nil),
	})
	if err != nil || created.ContainerAttempt == nil || created.ContainerAttempt.Attempt.Phase != domain.AttemptCreated {
		t.Fatalf("ConfirmContainerCreate() = (%#v, %v)", created, err)
	}
	if err := store.View(context.Background(), func(reader state.Reader) error {
		record, err := reader.GetContainerAttempt(request.ContainerID)
		if err != nil {
			return err
		}
		if len(record.HostResources) != len(receipts) {
			return errors.New("Attempt inventory did not receive the complete acquisition set")
		}
		return nil
	}); err != nil {
		t.Fatalf("owned Attempt state error = %v", err)
	}
	deleting, err := coordinator.BeginContainerDelete(context.Background(), ContainerActionRequest{OperationID: "op-delete-owned-attempt", ContainerID: request.ContainerID})
	if err != nil {
		t.Fatalf("BeginContainerDelete() error = %v", err)
	}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: "op-delete-owned-attempt", Target: target, Fingerprint: deleting.Fingerprint,
		Stage: operation.StageTeardown, Releases: testCleanupReleases(t, "op-delete-owned-attempt", receipts),
	}); err != nil {
		t.Fatalf("CheckpointOperation(Container teardown) error = %v", err)
	}
	if _, err := coordinator.ConfirmContainerDelete(context.Background(), ContainerConfirmRequest{
		OperationID: "op-delete-owned-attempt", ContainerID: request.ContainerID, Fingerprint: deleting.Fingerprint,
		Verification: testVerification(VerificationAttemptAbsent, nil),
	}); err != nil {
		t.Fatalf("ConfirmContainerDelete() error = %v", err)
	}
	if _, err := coordinator.GetContainer(context.Background(), request.ContainerID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetContainer(deleted) error = %v, want ErrNotFound", err)
	}
}

// TestCheckpointOperationRejectsBindingAndStageRegression verifies recovery cannot attach progress to another request or move backward.
func TestCheckpointOperationRejectsBindingAndStageRegression(t *testing.T) {
	_, store := testCoordinator(t)
	coordinator := testCoordinatorForStoreProfile(t, store, state.HostProfileLinuxM2)
	request := SandboxCreateRequest{OperationID: "op-checkpoint-guard", SandboxID: "sandbox-checkpoint-guard", Spec: testSandboxSpec()}
	begin, err := coordinator.BeginSandboxCreate(context.Background(), request)
	if err != nil {
		t.Fatalf("BeginSandboxCreate() error = %v", err)
	}
	target := operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: request.OperationID, Target: target, Fingerprint: begin.Fingerprint,
		Stage: operation.StageCreateProcess,
	}); err != nil {
		t.Fatalf("CheckpointOperation(forward) error = %v", err)
	}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: request.OperationID, Target: target, Fingerprint: begin.Fingerprint,
		Stage: operation.StagePrepareCgroup,
	}); !errors.Is(err, state.ErrInvalidRecord) {
		t.Fatalf("CheckpointOperation(regression) error = %v, want ErrInvalidRecord", err)
	}
	wrongFingerprint, err := operation.CanonicalRequestFingerprint(map[string]string{"different": "semantic request"})
	if err != nil {
		t.Fatalf("CanonicalRequestFingerprint() error = %v", err)
	}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: request.OperationID, Target: target, Fingerprint: wrongFingerprint,
		Stage: operation.StageObserveProcess,
	}); !errors.Is(err, operation.ErrBindingMismatch) {
		t.Fatalf("CheckpointOperation(binding mismatch) error = %v, want ErrBindingMismatch", err)
	}
	if _, err := coordinator.GetOperationProgress(context.Background(), ""); err == nil {
		t.Fatal("GetOperationProgress(empty ID) error = nil")
	}
}

// TestLinuxProfileRejectsCreateWithoutCompleteReceipts verifies a provider-stage shortcut cannot produce a Ready execution record.
func TestLinuxProfileRejectsCreateWithoutCompleteReceipts(t *testing.T) {
	abstractCoordinator, store := testCoordinator(t)
	sandbox := createReadySandbox(t, abstractCoordinator, "sandbox-strict-empty")
	coordinator := testCoordinatorForStoreProfile(t, store, state.HostProfileLinuxM2)
	request := ContainerCreateRequest{
		OperationID: "op-strict-empty", SandboxID: sandbox.ID, ContainerID: "container-strict-empty",
		AttemptID: "attempt-strict-empty", Process: testProcessSpec(),
	}
	begin, err := coordinator.BeginContainerCreate(context.Background(), request)
	if err != nil {
		t.Fatalf("BeginContainerCreate() error = %v", err)
	}
	target := operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: request.OperationID, Target: target, Fingerprint: begin.Fingerprint,
		Stage: operation.StageAttachCgroup,
	}); err != nil {
		t.Fatalf("CheckpointOperation() error = %v", err)
	}
	if _, err := coordinator.ConfirmContainerCreate(context.Background(), ContainerConfirmRequest{
		OperationID: request.OperationID, ContainerID: request.ContainerID, Fingerprint: begin.Fingerprint,
		Verification: testVerification(VerificationAttemptCreated, nil),
	}); !errors.Is(err, state.ErrInvalidRecord) {
		t.Fatalf("ConfirmContainerCreate(without receipts) error = %v, want ErrInvalidRecord", err)
	}
	current, err := coordinator.GetContainer(context.Background(), request.ContainerID)
	if err != nil || current.Attempt.Phase != domain.AttemptCreating {
		t.Fatalf("GetContainer(after rejected confirmation) = (%#v, %v), want Creating", current, err)
	}
}

// TestLinuxProfileRequiresStartAndExitObservationStages verifies trusted confirmation cannot bypass gate or terminal-process evidence.
func TestLinuxProfileRequiresStartAndExitObservationStages(t *testing.T) {
	abstractCoordinator, store := testCoordinator(t)
	sandbox := createReadySandbox(t, abstractCoordinator, "sandbox-strict-stages")
	coordinator := testCoordinatorForStoreProfile(t, store, state.HostProfileLinuxM2)
	createRequest := ContainerCreateRequest{
		OperationID: "op-strict-stage-create", SandboxID: sandbox.ID, ContainerID: "container-strict-stages",
		AttemptID: "attempt-strict-stages", Process: testProcessSpec(),
	}
	creating, err := coordinator.BeginContainerCreate(context.Background(), createRequest)
	if err != nil {
		t.Fatalf("BeginContainerCreate() error = %v", err)
	}
	createTarget := operation.Target{Kind: operation.TargetContainer, ID: string(createRequest.ContainerID)}
	receipts, rollbackRecords := testCheckpointProfile(t, createRequest.OperationID, createTarget)
	checkpointTestProfile(t, coordinator, createRequest.OperationID, createTarget, creating.Fingerprint, receipts, rollbackRecords)
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: createRequest.OperationID, Target: createTarget, Fingerprint: creating.Fingerprint,
		Stage: operation.StageAttachCgroup, Rollback: rollbackRecords, Receipts: receipts,
	}); err != nil {
		t.Fatalf("CheckpointOperation(create attachment) error = %v", err)
	}
	if _, err := coordinator.ConfirmContainerCreate(context.Background(), ContainerConfirmRequest{
		OperationID: createRequest.OperationID, ContainerID: createRequest.ContainerID, Fingerprint: creating.Fingerprint,
		Verification: testVerification(VerificationAttemptCreated, nil),
	}); err != nil {
		t.Fatalf("ConfirmContainerCreate() error = %v", err)
	}

	startRequest := ContainerActionRequest{OperationID: "op-strict-stage-start", ContainerID: createRequest.ContainerID}
	starting, err := coordinator.BeginContainerStart(context.Background(), startRequest)
	if err != nil {
		t.Fatalf("BeginContainerStart() error = %v", err)
	}
	identity := testIdentity()
	startConfirm := ContainerConfirmRequest{
		OperationID: startRequest.OperationID, ContainerID: startRequest.ContainerID, Fingerprint: starting.Fingerprint,
		Verification: testVerification(VerificationAttemptRunning, &identity),
	}
	if _, err := coordinator.ConfirmContainerStart(context.Background(), startConfirm); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("ConfirmContainerStart(without gate checkpoint) error = %v, want failed precondition", err)
	}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: startRequest.OperationID, Target: createTarget, Fingerprint: starting.Fingerprint,
		Stage: operation.StageReleaseStartGate,
	}); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("CheckpointOperation(release before attachment) error = %v, want failed precondition", err)
	}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: startRequest.OperationID, Target: createTarget, Fingerprint: starting.Fingerprint,
		Stage: operation.StageAttachCgroup,
	}); err != nil {
		t.Fatalf("CheckpointOperation(attach cgroup) error = %v", err)
	}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: startRequest.OperationID, Target: createTarget, Fingerprint: starting.Fingerprint,
		Stage: operation.StageReleaseStartGate,
	}); err != nil {
		t.Fatalf("CheckpointOperation(release gate) error = %v", err)
	}
	if _, err := coordinator.ConfirmContainerStart(context.Background(), startConfirm); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("ConfirmContainerStart(without process observation) error = %v, want failed precondition", err)
	}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: startRequest.OperationID, Target: createTarget, Fingerprint: starting.Fingerprint,
		Stage: operation.StageObserveProcess,
	}); err != nil {
		t.Fatalf("CheckpointOperation(observe running) error = %v", err)
	}
	if _, err := coordinator.ConfirmContainerStart(context.Background(), startConfirm); err != nil {
		t.Fatalf("ConfirmContainerStart() error = %v", err)
	}

	stopRequest := RecordStoppedRequest{
		OperationID: "op-strict-stage-exit", ContainerID: createRequest.ContainerID, Outcome: testExitOutcome(0),
		Verification: testVerification(VerificationAttemptStopped, nil),
	}
	stopping, err := coordinator.BeginRecordStopped(context.Background(), stopRequest)
	if err != nil {
		t.Fatalf("BeginRecordStopped() error = %v", err)
	}
	if _, err := coordinator.RecordStopped(context.Background(), stopRequest); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("RecordStopped(without observation checkpoint) error = %v, want failed precondition", err)
	}
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: stopRequest.OperationID, Target: createTarget, Fingerprint: stopping.Fingerprint,
		Stage: operation.StageObserveProcess,
	}); err != nil {
		t.Fatalf("CheckpointOperation(observe exit) error = %v", err)
	}
	stopped, err := coordinator.RecordStopped(context.Background(), stopRequest)
	if err != nil || stopped.ContainerAttempt == nil || stopped.ContainerAttempt.Attempt.Phase != domain.AttemptStopped {
		t.Fatalf("RecordStopped() = (%#v, %v)", stopped, err)
	}
}

// TestRecordContainerStartTerminalPersistsExactFailure verifies a post-release wrapper failure closes the active Start and replays its stopped projection unchanged.
func TestRecordContainerStartTerminalPersistsExactFailure(t *testing.T) {
	abstractCoordinator, store := testCoordinator(t)
	sandbox := createReadySandbox(t, abstractCoordinator, "sandbox-start-terminal")
	coordinator := testCoordinatorForStoreProfile(t, store, state.HostProfileLinuxM2)
	createRequest := ContainerCreateRequest{
		OperationID: "op-create-start-terminal", SandboxID: sandbox.ID, ContainerID: "container-start-terminal",
		AttemptID: "attempt-start-terminal", Process: testProcessSpec(),
	}
	creating, err := coordinator.BeginContainerCreate(context.Background(), createRequest)
	if err != nil {
		t.Fatalf("BeginContainerCreate() error = %v", err)
	}
	target := operation.Target{Kind: operation.TargetContainer, ID: string(createRequest.ContainerID)}
	receipts, rollbackRecords := testCheckpointProfile(t, createRequest.OperationID, target)
	checkpointTestProfile(t, coordinator, createRequest.OperationID, target, creating.Fingerprint, receipts, rollbackRecords)
	if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
		OperationID: createRequest.OperationID, Target: target, Fingerprint: creating.Fingerprint,
		Stage: operation.StageAttachCgroup, Rollback: rollbackRecords, Receipts: receipts,
	}); err != nil {
		t.Fatalf("CheckpointOperation(create attachment) error = %v", err)
	}
	if _, err := coordinator.ConfirmContainerCreate(context.Background(), ContainerConfirmRequest{
		OperationID: createRequest.OperationID, ContainerID: createRequest.ContainerID, Fingerprint: creating.Fingerprint,
		Verification: testVerification(VerificationAttemptCreated, nil),
	}); err != nil {
		t.Fatalf("ConfirmContainerCreate() error = %v", err)
	}

	startRequest := ContainerActionRequest{OperationID: "op-start-terminal-fact", ContainerID: createRequest.ContainerID}
	starting, err := coordinator.BeginContainerStart(context.Background(), startRequest)
	if err != nil {
		t.Fatalf("BeginContainerStart() error = %v", err)
	}
	for _, stage := range []operation.Stage{operation.StageAttachCgroup, operation.StageReleaseStartGate, operation.StageObserveProcess} {
		if _, err := coordinator.CheckpointOperation(context.Background(), CheckpointRequest{
			OperationID: startRequest.OperationID, Target: target, Fingerprint: starting.Fingerprint, Stage: stage,
		}); err != nil {
			t.Fatalf("CheckpointOperation(%s) error = %v", stage, err)
		}
	}
	terminalRequest := ContainerStartTerminalRequest{
		OperationID: startRequest.OperationID, ContainerID: startRequest.ContainerID, Fingerprint: starting.Fingerprint,
		Outcome: domain.NotApplicableOutcome(), Verification: testVerification(VerificationAttemptStopped, nil),
	}
	failed, err := coordinator.RecordContainerStartTerminal(context.Background(), terminalRequest)
	if err == nil {
		t.Fatal("RecordContainerStartTerminal() error = nil, want stable failed-operation error")
	}
	if failed.Operation.State != operation.StateFailed || failed.Operation.Result != operation.ResultFailed || failed.ContainerAttempt == nil || failed.ContainerAttempt.Attempt.Phase != domain.AttemptStopped {
		t.Fatalf("RecordContainerStartTerminal() = %#v", failed)
	}
	replayed, err := coordinator.RecordContainerStartTerminal(context.Background(), terminalRequest)
	if err == nil {
		t.Fatal("RecordContainerStartTerminal(retry) error = nil, want stable failed-operation error")
	}
	if replayed.Resolution != operation.ResolutionReplay || !reflect.DeepEqual(replayed.Operation.Response, failed.Operation.Response) || !reflect.DeepEqual(replayed.ContainerAttempt, failed.ContainerAttempt) {
		t.Fatalf("RecordContainerStartTerminal(retry) = %#v, want exact replay %#v", replayed, failed)
	}
}
