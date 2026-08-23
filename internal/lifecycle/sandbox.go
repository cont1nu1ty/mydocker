package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/state"
)

// BeginSandboxCreate atomically persists generation-one creating intent, its operation, and its first event.
func (c *Coordinator) BeginSandboxCreate(ctx context.Context, request SandboxCreateRequest) (SandboxResult, error) {
	binding, err := bindingFor(request.OperationID, operation.TypeCreate, operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)}, sandboxCreateSemantic{SandboxID: request.SandboxID, Spec: request.Spec})
	if err != nil {
		return SandboxResult{}, err
	}
	var result SandboxResult
	err = c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = sandboxResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionResume {
			record, getErr := tx.GetSandbox(request.SandboxID)
			if getErr != nil {
				return getErr
			}
			sandbox := record.Sandbox.Clone()
			result = SandboxResult{Resolution: resolved.Resolution, Operation: resolved.Record.Operation.Clone(), Fingerprint: binding.Fingerprint, Sandbox: &sandbox}
			return nil
		}

		existing, getErr := tx.GetSandbox(request.SandboxID)
		if getErr == nil {
			existingBinding, fingerprintErr := bindingFor(request.OperationID, operation.TypeCreate, binding.Target, sandboxCreateSemantic{SandboxID: existing.Sandbox.ID, Spec: existing.Sandbox.Spec})
			if fingerprintErr != nil {
				return fingerprintErr
			}
			if existing.Sandbox.Status.Phase != domain.SandboxReady || !existingBinding.Fingerprint.Equal(binding.Fingerprint) {
				return domain.NewError(domain.CodeAlreadyExists, "sandbox_id", "already exists with different or unreconciled create intent")
			}
			operationRecord, putErr := c.putNewTerminalOperation(tx, binding, operation.ResultNoop, nil)
			if putErr != nil {
				return putErr
			}
			event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StageComplete, operation.ResultNoop, c.clock.Now(), 0, existing.Sandbox.Status.Generation, existing.Sandbox.Status.ObservedGeneration, map[string]string{"phase": existing.Sandbox.Status.Phase.String()})
			if eventErr != nil {
				return eventErr
			}
			existing, putErr = observeSandbox(tx, existing, event)
			if putErr != nil {
				return putErr
			}
			response, encodeErr := encodeResponse(sandboxResponse{Sandbox: sandboxPointer(existing.Sandbox)})
			if encodeErr != nil {
				return encodeErr
			}
			operationRecord, putErr = finalizeOperationResponse(tx, operationRecord, response)
			if putErr != nil {
				return putErr
			}
			result, putErr = sandboxResultFromRecord(operation.ResolutionNew, operationRecord)
			return putErr
		}
		if !errors.Is(getErr, state.ErrNotFound) {
			return getErr
		}

		sandbox, createErr := domain.NewSandbox(request.SandboxID, request.Spec)
		if createErr != nil {
			return createErr
		}
		operationRecord, putErr := c.putNewActiveOperation(tx, binding, nil)
		if putErr != nil {
			return putErr
		}
		storedSandbox, putErr := tx.PutSandbox(state.NewSandboxRecord(sandbox), 0)
		if putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StagePersistIntent, operation.ResultPending, c.clock.Now(), 0, sandbox.Status.Generation, sandbox.Status.ObservedGeneration, map[string]string{"phase": sandbox.Status.Phase.String()})
		if eventErr != nil {
			return eventErr
		}
		storedSandbox, putErr = observeSandbox(tx, storedSandbox, event)
		if putErr != nil {
			return putErr
		}
		result = SandboxResult{Resolution: operation.ResolutionNew, Operation: operationRecord.Operation, Fingerprint: binding.Fingerprint, Sandbox: sandboxPointer(storedSandbox.Sandbox)}
		return nil
	})
	return finishSandboxResult(result, err)
}

// ConfirmSandboxCreate marks a creating Sandbox Ready only after explicit ready verification.
func (c *Coordinator) ConfirmSandboxCreate(ctx context.Context, request SandboxConfirmRequest) (SandboxResult, error) {
	if err := request.Verification.validateFor(VerificationSandboxReady); err != nil {
		return SandboxResult{}, err
	}
	binding, err := suppliedBinding(request.OperationID, operation.TypeCreate, operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)}, request.Fingerprint)
	if err != nil {
		return SandboxResult{}, err
	}
	return c.confirmSandboxTransition(ctx, binding, request.Verification, domain.SandboxCreating, domain.SandboxReady)
}

// BeginSandboxStop atomically records stop intent and moves a Ready Sandbox to stopping without host action.
func (c *Coordinator) BeginSandboxStop(ctx context.Context, request SandboxActionRequest) (SandboxResult, error) {
	binding, err := bindingFor(request.OperationID, operation.TypeStop, operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)}, sandboxActionSemantic{SandboxID: request.SandboxID})
	if err != nil {
		return SandboxResult{}, err
	}
	var result SandboxResult
	err = c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = sandboxResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		record, getErr := tx.GetSandbox(request.SandboxID)
		if getErr != nil {
			return getErr
		}
		if resolved.Resolution == operation.ResolutionResume {
			result = SandboxResult{Resolution: resolved.Resolution, Operation: resolved.Record.Operation.Clone(), Fingerprint: binding.Fingerprint, Sandbox: sandboxPointer(record.Sandbox)}
			return nil
		}
		if record.Sandbox.Status.Phase == domain.SandboxStopped {
			return c.persistSandboxNoop(tx, binding, record, &result)
		}
		if record.Sandbox.Status.Phase != domain.SandboxReady {
			return domain.NewError(domain.CodeFailedPrecondition, "sandbox.phase", "must be Ready or already Stopped")
		}
		pairs, listErr := tx.ListContainerAttempts(request.SandboxID)
		if listErr != nil {
			return listErr
		}
		for _, pairRecord := range pairs {
			if domain.IsActiveAttempt(pairRecord.ContainerAttempt.Attempt.Phase) {
				return domain.NewError(domain.CodeFailedPrecondition, "sandbox.active_attempt", "must have no active Attempt before stop")
			}
		}
		if transitionErr := record.Sandbox.Transition(domain.SandboxStopping); transitionErr != nil {
			return transitionErr
		}
		operationRecord, putErr := c.putNewActiveOperation(tx, binding, nil)
		if putErr != nil {
			return putErr
		}
		updated, putErr := tx.PutSandbox(record, record.Revision)
		if putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StagePersistIntent, operation.ResultPending, c.clock.Now(), 0, updated.Sandbox.Status.Generation, updated.Sandbox.Status.ObservedGeneration, map[string]string{"phase": updated.Sandbox.Status.Phase.String()})
		if eventErr != nil {
			return eventErr
		}
		updated, putErr = observeSandbox(tx, updated, event)
		if putErr != nil {
			return putErr
		}
		result = SandboxResult{Resolution: operation.ResolutionNew, Operation: operationRecord.Operation, Fingerprint: binding.Fingerprint, Sandbox: sandboxPointer(updated.Sandbox)}
		return nil
	})
	return finishSandboxResult(result, err)
}

// ConfirmSandboxStop marks a stopping Sandbox Stopped only after explicit stopped verification.
func (c *Coordinator) ConfirmSandboxStop(ctx context.Context, request SandboxConfirmRequest) (SandboxResult, error) {
	if err := request.Verification.validateFor(VerificationSandboxStopped); err != nil {
		return SandboxResult{}, err
	}
	binding, err := suppliedBinding(request.OperationID, operation.TypeStop, operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)}, request.Fingerprint)
	if err != nil {
		return SandboxResult{}, err
	}
	return c.confirmSandboxTransition(ctx, binding, request.Verification, domain.SandboxStopping, domain.SandboxStopped)
}

// BeginSandboxRemove persists two-phase remove intent even when metadata is absent, so a provider must still confirm host absence.
func (c *Coordinator) BeginSandboxRemove(ctx context.Context, request SandboxActionRequest) (SandboxResult, error) {
	binding, err := bindingFor(request.OperationID, operation.TypeDelete, operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)}, sandboxActionSemantic{SandboxID: request.SandboxID})
	if err != nil {
		return SandboxResult{}, err
	}
	var result SandboxResult
	err = c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = sandboxResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		record, getErr := tx.GetSandbox(request.SandboxID)
		if errors.Is(getErr, state.ErrNotFound) {
			if resolved.Resolution == operation.ResolutionResume {
				result = SandboxResult{Resolution: resolved.Resolution, Operation: resolved.Record.Operation.Clone(), Fingerprint: binding.Fingerprint}
				return nil
			}
			operationRecord, putErr := c.putNewActiveOperation(tx, binding, nil)
			if putErr != nil {
				return putErr
			}
			if _, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StagePersistIntent, operation.ResultPending, c.clock.Now(), 0, 0, 0, map[string]string{"phase": "metadata_absent_pending_host_verification"}); eventErr != nil {
				return eventErr
			}
			result = SandboxResult{Resolution: operation.ResolutionNew, Operation: operationRecord.Operation, Fingerprint: binding.Fingerprint}
			return nil
		}
		if getErr != nil {
			return getErr
		}
		if resolved.Resolution == operation.ResolutionResume {
			result = SandboxResult{Resolution: resolved.Resolution, Operation: resolved.Record.Operation.Clone(), Fingerprint: binding.Fingerprint, Sandbox: sandboxPointer(record.Sandbox)}
			return nil
		}
		if record.Sandbox.Status.Phase != domain.SandboxStopped {
			return domain.NewError(domain.CodeFailedPrecondition, "sandbox.phase", "must be Stopped before remove")
		}
		pairs, listErr := tx.ListContainerAttempts(request.SandboxID)
		if listErr != nil {
			return listErr
		}
		if len(pairs) != 0 {
			return domain.NewError(domain.CodeFailedPrecondition, "sandbox.containers", "must be empty before remove")
		}
		operationRecord, putErr := c.putNewActiveOperation(tx, binding, nil)
		if putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StagePersistIntent, operation.ResultPending, c.clock.Now(), 0, record.Sandbox.Status.Generation, record.Sandbox.Status.ObservedGeneration, map[string]string{"phase": record.Sandbox.Status.Phase.String()})
		if eventErr != nil {
			return eventErr
		}
		record, putErr = observeSandbox(tx, record, event)
		if putErr != nil {
			return putErr
		}
		result = SandboxResult{Resolution: operation.ResolutionNew, Operation: operationRecord.Operation, Fingerprint: binding.Fingerprint, Sandbox: sandboxPointer(record.Sandbox)}
		return nil
	})
	return finishSandboxResult(result, err)
}

// ConfirmSandboxRemove deletes Sandbox metadata only after explicit absence verification.
func (c *Coordinator) ConfirmSandboxRemove(ctx context.Context, request SandboxConfirmRequest) (SandboxResult, error) {
	if err := request.Verification.validateFor(VerificationSandboxAbsent); err != nil {
		return SandboxResult{}, err
	}
	binding, err := suppliedBinding(request.OperationID, operation.TypeDelete, operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)}, request.Fingerprint)
	if err != nil {
		return SandboxResult{}, err
	}
	var result SandboxResult
	err = c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionNew {
			return fmt.Errorf("confirm Sandbox remove operation %q: %w", request.OperationID, state.ErrNotFound)
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = sandboxResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		record, getErr := tx.GetSandbox(request.SandboxID)
		if errors.Is(getErr, state.ErrNotFound) {
			if len(resolved.Record.Releases) != 0 {
				return domain.NewError(domain.CodeFailedPrecondition, "sandbox.host_resources", "metadata-absent delete cannot carry unmatched cleanup releases")
			}
			response, encodeErr := encodeResponse(sandboxResponse{Removed: true})
			if encodeErr != nil {
				return encodeErr
			}
			operationRecord, putErr := completeOperation(tx, *resolved.Record, operation.ResultNoop, response)
			if putErr != nil {
				return putErr
			}
			if _, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StageComplete, operation.ResultNoop, request.Verification.ObservedAt, request.Verification.Duration, 0, 0, request.Verification); eventErr != nil {
				return eventErr
			}
			result, putErr = sandboxResultFromRecord(operation.ResolutionResume, operationRecord)
			return putErr
		}
		if getErr != nil {
			return getErr
		}
		if record.Sandbox.Status.Phase != domain.SandboxStopped {
			return domain.NewError(domain.CodeFailedPrecondition, "sandbox.phase", "must remain Stopped until remove confirmation")
		}
		pairs, listErr := tx.ListContainerAttempts(request.SandboxID)
		if listErr != nil {
			return listErr
		}
		if len(pairs) != 0 {
			return domain.NewError(domain.CodeFailedPrecondition, "sandbox.containers", "must remain empty through remove confirmation")
		}
		if releaseErr := validateCompleteCleanupReleases(*resolved.Record, record.HostResources); releaseErr != nil {
			return releaseErr
		}
		record.HostResources = nil
		record, putErr := tx.PutSandbox(record, record.Revision)
		if putErr != nil {
			return putErr
		}
		response, encodeErr := encodeResponse(sandboxResponse{Removed: true})
		if encodeErr != nil {
			return encodeErr
		}
		operationRecord, putErr := completeOperation(tx, *resolved.Record, operation.ResultSucceeded, response)
		if putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StageComplete, operation.ResultSucceeded, request.Verification.ObservedAt, request.Verification.Duration, record.Sandbox.Status.Generation, record.Sandbox.Status.ObservedGeneration, request.Verification)
		if eventErr != nil {
			return eventErr
		}
		record, putErr = observeSandbox(tx, record, event)
		if putErr != nil {
			return putErr
		}
		if deleteErr := tx.DeleteSandbox(request.SandboxID, record.Revision); deleteErr != nil {
			return deleteErr
		}
		result, putErr = sandboxResultFromRecord(operation.ResolutionResume, operationRecord)
		return putErr
	})
	return finishSandboxResult(result, err)
}

// confirmSandboxTransition applies one verified Sandbox transition and completes its original operation atomically.
func (c *Coordinator) confirmSandboxTransition(ctx context.Context, binding operation.Binding, verification Verification, from, to domain.SandboxPhase) (SandboxResult, error) {
	var result SandboxResult
	err := c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionNew {
			return fmt.Errorf("confirm Sandbox operation %q: %w", binding.ID, state.ErrNotFound)
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = sandboxResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		minimumStage := operation.StageObserveProcess
		if binding.Type == operation.TypeCreate {
			minimumStage = operation.StagePrepareNamespaces
		}
		if stageErr := requireLinuxStage(*resolved.Record, minimumStage); stageErr != nil {
			return stageErr
		}
		record, getErr := tx.GetSandbox(domain.SandboxID(binding.Target.ID))
		if getErr != nil {
			return getErr
		}
		if record.Sandbox.Status.Phase != from {
			return domain.NewError(domain.CodeFailedPrecondition, "sandbox.phase", fmt.Sprintf("must be %s before confirmation", from))
		}
		if transitionErr := record.Sandbox.Transition(to); transitionErr != nil {
			return transitionErr
		}
		operationSource := resolved.Record.Clone()
		if from == domain.SandboxCreating && to == domain.SandboxReady {
			var adoptErr error
			operationSource, record.HostResources, adoptErr = adoptOperationReceipts(operationSource)
			if adoptErr != nil {
				return adoptErr
			}
		}
		updated, putErr := tx.PutSandbox(record, record.Revision)
		if putErr != nil {
			return putErr
		}
		operationRecord, putErr := completeOperation(tx, operationSource, operation.ResultSucceeded, nil)
		if putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StageComplete, operation.ResultSucceeded, verification.ObservedAt, verification.Duration, updated.Sandbox.Status.Generation, updated.Sandbox.Status.ObservedGeneration, verification)
		if eventErr != nil {
			return eventErr
		}
		updated, putErr = observeSandbox(tx, updated, event)
		if putErr != nil {
			return putErr
		}
		response, encodeErr := encodeResponse(sandboxResponse{Sandbox: sandboxPointer(updated.Sandbox)})
		if encodeErr != nil {
			return encodeErr
		}
		operationRecord, putErr = finalizeOperationResponse(tx, operationRecord, response)
		if putErr != nil {
			return putErr
		}
		result, putErr = sandboxResultFromRecord(operation.ResolutionResume, operationRecord)
		return putErr
	})
	return finishSandboxResult(result, err)
}

// persistSandboxNoop stores a replayable no-op operation after confirming the target phase already exists.
func (c *Coordinator) persistSandboxNoop(tx state.Tx, binding operation.Binding, record state.SandboxRecord, result *SandboxResult) error {
	operationRecord, err := c.putNewTerminalOperation(tx, binding, operation.ResultNoop, nil)
	if err != nil {
		return err
	}
	event, err := appendOperationEvent(tx, operationRecord.Operation, operation.StageComplete, operation.ResultNoop, c.clock.Now(), 0, record.Sandbox.Status.Generation, record.Sandbox.Status.ObservedGeneration, map[string]string{"phase": record.Sandbox.Status.Phase.String()})
	if err != nil {
		return err
	}
	record, err = observeSandbox(tx, record, event)
	if err != nil {
		return err
	}
	response, err := encodeResponse(sandboxResponse{Sandbox: sandboxPointer(record.Sandbox)})
	if err != nil {
		return err
	}
	operationRecord, err = finalizeOperationResponse(tx, operationRecord, response)
	if err != nil {
		return err
	}
	decoded, err := sandboxResultFromRecord(operation.ResolutionNew, operationRecord)
	if err != nil {
		return err
	}
	*result = decoded
	return nil
}

// sandboxPointer returns a caller-owned deep copy for result and replay payload construction.
func sandboxPointer(sandbox domain.Sandbox) *domain.Sandbox {
	clone := sandbox.Clone()
	return &clone
}
