package engine

import (
	"context"
	"errors"
	"fmt"

	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/lifecycle"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
)

// CreateSandbox persists intent, checkpoints the canonical M2 Sandbox receipt
// profile one acquisition at a time, and confirms Ready only after full evidence.
func (engine *Engine) CreateSandbox(ctx context.Context, request lifecycle.SandboxCreateRequest) (lifecycle.SandboxResult, error) {
	operationStartedAt := engine.beginMeasurement()
	target := operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)}
	release := engine.targetLocks.lock(target)
	defer release()
	result, err := engine.lifecycle.BeginSandboxCreate(ctx, request)
	if err != nil || result.Operation.State.Terminal() {
		return result, err
	}
	if result.Operation.Stage == operation.StageRollback {
		if _, err := engine.resumeCreateRollbackLocked(ctx, request.OperationID); err != nil {
			return result, err
		}
		return engine.lifecycle.BeginSandboxCreate(ctx, request)
	}
	retained, err := engine.lifecycle.GetSandbox(ctx, request.SandboxID)
	if err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonInternal, err)
	}
	canonicalNetworkMode, err := provider.SandboxNetworkMode(retained.Spec.Network.Mode).Canonical()
	if err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonPrecondition, err)
	}
	retained.Spec.Network.Mode = string(canonicalNetworkMode)
	preflightStartedAt := engine.beginMeasurement()
	capabilities, err := engine.preflight(ctx)
	preflightMeasurement := engine.finishMeasurement(preflightStartedAt)
	if err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonPrecondition, err)
	}
	progress, err := engine.lifecycle.GetOperationProgress(ctx, request.OperationID)
	if err != nil {
		return result, err
	}
	if len(progress.Receipts) == 0 && progress.Operation.Stage == operation.StagePersistIntent {
		if _, err = engine.checkpointProgress(ctx, request.OperationID, target, result.Fingerprint, operation.StageHostPreflight, capabilities, preflightMeasurement); err != nil {
			return result, err
		}
	}
	owner, err := ownership.NewOwnerKey(request.OperationID, target, domain.InitialGeneration)
	if err != nil {
		return result, err
	}
	for {
		progress, err = engine.lifecycle.GetOperationProgress(ctx, request.OperationID)
		if err != nil {
			return result, err
		}
		if len(progress.Receipts) >= 6 {
			break
		}
		acquireStartedAt := engine.beginMeasurement()
		receipt, stage, acquireErr := engine.acquireSandboxReceipt(ctx, owner, request.SandboxID, retained.Spec, progress.Receipts)
		acquireMeasurement := engine.finishMeasurement(acquireStartedAt)
		if acquireErr != nil {
			if provider.IsNoEffect(acquireErr) || provider.IsRollbackRequired(acquireErr) {
				return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonInternal, acquireErr)
			}
			return result, acquireErr
		}
		if _, err = engine.checkpointReceipt(ctx, request.OperationID, target, result.Fingerprint, stage, receipt, acquireMeasurement); err != nil {
			return result, err
		}
	}
	if err := ownership.ValidateReceiptJournalProfile(target.Kind, progress.Receipts); err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonInternal, err)
	}
	evidence, err := ownership.EvidenceDigest(progress.Receipts)
	if err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonInternal, err)
	}
	completion := engine.finishOperationMeasurement(operationStartedAt, result.Resolution)
	return engine.lifecycle.ConfirmSandboxCreate(ctx, lifecycle.SandboxConfirmRequest{
		OperationID: request.OperationID, SandboxID: request.SandboxID, Fingerprint: result.Fingerprint,
		Verification: lifecycle.Verification{
			Kind: lifecycle.VerificationSandboxReady, Verified: true, Evidence: evidence,
			ObservedAt: completion.occurredAt, Duration: completion.duration,
		},
	})
}

// StopSandbox verifies that every retained Sandbox resource remains owned and
// no Attempt is active, then records the stable environment as quiescent.
func (engine *Engine) StopSandbox(ctx context.Context, request lifecycle.SandboxActionRequest) (lifecycle.SandboxResult, error) {
	operationStartedAt := engine.beginMeasurement()
	target := operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)}
	release := engine.targetLocks.lock(target)
	defer release()
	result, err := engine.lifecycle.BeginSandboxStop(ctx, request)
	if err != nil || result.Operation.State.Terminal() {
		return result, err
	}
	inventory, err := engine.sandboxInventory(ctx, request.SandboxID)
	if err != nil {
		return result, err
	}
	if err := ownership.ValidateReceiptJournalProfile(target.Kind, inventory); err != nil {
		return result, err
	}
	observations := make([]provider.ResourceObservation, 0, len(inventory))
	observationStartedAt := engine.beginMeasurement()
	for _, receipt := range inventory {
		observation, inspectErr := engine.inspectReceipt(ctx, receipt)
		if inspectErr != nil {
			return result, inspectErr
		}
		if observation.Presence != provider.PresencePresent {
			return result, errors.New("Sandbox stop cannot confirm while an owned resource is not verified present")
		}
		observations = append(observations, observation)
	}
	observationMeasurement := engine.finishMeasurement(observationStartedAt)
	if _, err = engine.checkpointProgress(ctx, request.OperationID, target, result.Fingerprint, operation.StageObserveProcess, observations, observationMeasurement); err != nil {
		return result, err
	}
	progress, err := engine.lifecycle.GetOperationProgress(ctx, request.OperationID)
	if err != nil {
		return result, err
	}
	if err = engine.checkpointClearCondition(ctx, progress, target, result.Fingerprint, domain.ConditionCleanupPending,
		map[string]string{"recovery": "all Sandbox resources are verified present"}); err != nil {
		return result, err
	}
	evidence, err := ownership.EvidenceDigest(observations)
	if err != nil {
		return result, err
	}
	completion := engine.finishOperationMeasurement(operationStartedAt, result.Resolution)
	return engine.lifecycle.ConfirmSandboxStop(ctx, lifecycle.SandboxConfirmRequest{
		OperationID: request.OperationID, SandboxID: request.SandboxID, Fingerprint: result.Fingerprint,
		Verification: lifecycle.Verification{Kind: lifecycle.VerificationSandboxStopped, Verified: true, Evidence: evidence,
			ObservedAt: completion.occurredAt, Duration: completion.duration},
	})
}

// RemoveSandbox tears down the complete adopted Sandbox inventory using exact
// provider receipts and confirms metadata deletion only after every absence proof is durable.
func (engine *Engine) RemoveSandbox(ctx context.Context, request lifecycle.SandboxActionRequest) (lifecycle.SandboxResult, error) {
	operationStartedAt := engine.beginMeasurement()
	target := operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)}
	releaseTarget := engine.targetLocks.lock(target)
	defer releaseTarget()
	result, err := engine.lifecycle.BeginSandboxRemove(ctx, request)
	if err != nil || result.Operation.State.Terminal() {
		return result, err
	}
	inventory, inventoryErr := engine.sandboxInventory(ctx, request.SandboxID)
	if inventoryErr != nil {
		if lifecycle.IsCheckpointNotFound(inventoryErr) {
			completion := engine.finishOperationMeasurement(operationStartedAt, result.Resolution)
			return engine.lifecycle.ConfirmSandboxRemove(ctx, lifecycle.SandboxConfirmRequest{
				OperationID: request.OperationID, SandboxID: request.SandboxID, Fingerprint: result.Fingerprint,
				Verification: lifecycle.Verification{Kind: lifecycle.VerificationSandboxAbsent, Verified: true, Evidence: "metadata_absent_without_owned_receipts",
					ObservedAt: completion.occurredAt, Duration: completion.duration},
			})
		}
		return result, inventoryErr
	}
	order := []ownership.Kind{
		ownership.KindKeeperProcess,
		ownership.KindNetworkNamespace, ownership.KindIPCNamespace, ownership.KindUTSNamespace,
		ownership.KindKeeperCgroup, ownership.KindSandboxCgroup,
	}
	for _, kind := range order {
		receipt, receiptErr := receiptByKind(inventory, kind)
		if receiptErr != nil {
			return result, receiptErr
		}
		progress, progressErr := engine.lifecycle.GetOperationProgress(ctx, request.OperationID)
		if progressErr != nil {
			return result, progressErr
		}
		if releaseByKind(progress.Releases, kind) != nil {
			continue
		}
		cleanupStartedAt := engine.beginMeasurement()
		observation, cleanupErr := engine.cleanupReceipt(ctx, receipt)
		cleanupMeasurement := engine.finishMeasurement(cleanupStartedAt)
		if cleanupErr != nil {
			return result, cleanupErr
		}
		release, releaseErr := observation.Release(request.OperationID, receipt)
		if releaseErr != nil {
			return result, releaseErr
		}
		if _, checkpointErr := engine.checkpointRelease(ctx, request.OperationID, target, result.Fingerprint, release, cleanupMeasurement); checkpointErr != nil {
			return result, checkpointErr
		}
	}
	progress, err := engine.lifecycle.GetOperationProgress(ctx, request.OperationID)
	if err != nil {
		return result, err
	}
	evidence, err := ownership.EvidenceDigest(progress.Releases)
	if err != nil {
		return result, err
	}
	completion := engine.finishOperationMeasurement(operationStartedAt, result.Resolution)
	return engine.lifecycle.ConfirmSandboxRemove(ctx, lifecycle.SandboxConfirmRequest{
		OperationID: request.OperationID, SandboxID: request.SandboxID, Fingerprint: result.Fingerprint,
		Verification: lifecycle.Verification{Kind: lifecycle.VerificationSandboxAbsent, Verified: true, Evidence: evidence,
			ObservedAt: completion.occurredAt, Duration: completion.duration},
	})
}

// acquireSandboxReceipt performs exactly the next canonical Sandbox acquisition for restart-safe replay.
func (engine *Engine) acquireSandboxReceipt(
	ctx context.Context,
	owner ownership.OwnerKey,
	sandboxID domain.SandboxID,
	spec domain.SandboxSpec,
	receipts []ownership.Receipt,
) (ownership.Receipt, operation.Stage, error) {
	switch len(receipts) {
	case 0:
		receipt, err := engine.providers.Cgroup.EnsureSandboxCgroup(ctx, provider.SandboxCgroupRequest{Owner: owner, SandboxID: sandboxID})
		return receipt, operation.StagePrepareCgroup, err
	case 1:
		receipt, err := engine.providers.Cgroup.EnsureKeeperCgroup(ctx, provider.KeeperCgroupRequest{Owner: owner, SandboxID: sandboxID, Parent: receipts[0]})
		return receipt, operation.StagePrepareCgroup, err
	case 2:
		receipt, err := engine.providers.Isolation.EnsureKeeperProcess(ctx, provider.KeeperProcessRequest{Owner: owner, SandboxID: sandboxID, Cgroup: receipts[1]})
		return receipt, operation.StageCreateProcess, err
	case 3, 4, 5:
		namespaceTypes := []isolation.NamespaceType{isolation.NamespaceUTS, isolation.NamespaceIPC, isolation.NamespaceNetwork}
		namespace := namespaceTypes[len(receipts)-3]
		namespaceRequest := provider.NamespaceRequest{Owner: owner, Process: receipts[2], Namespace: namespace}
		if namespace == isolation.NamespaceUTS {
			namespaceRequest.Hostname = spec.Hostname
		}
		if namespace == isolation.NamespaceNetwork {
			mode, err := provider.SandboxNetworkMode(spec.Network.Mode).Canonical()
			if err != nil {
				return ownership.Receipt{}, operation.StagePrepareNamespaces, err
			}
			namespaceRequest.NetworkMode = mode
		}
		receipt, err := engine.providers.Isolation.EnsureNamespace(ctx, namespaceRequest)
		return receipt, operation.StagePrepareNamespaces, err
	default:
		return ownership.Receipt{}, "", fmt.Errorf("Sandbox receipt prefix length %d exceeds the canonical profile", len(receipts))
	}
}
