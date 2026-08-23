package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/state"
)

// BeginContainerCreate atomically persists a one-to-one creating pair, Sandbox current refs, operation, and event.
func (c *Coordinator) BeginContainerCreate(ctx context.Context, request ContainerCreateRequest) (ContainerResult, error) {
	semantic := containerCreateSemantic{SandboxID: request.SandboxID, ContainerID: request.ContainerID, AttemptID: request.AttemptID, Process: request.Process, ImageDigest: request.ImageDigest, RootFS: request.RootFS}
	binding, err := bindingFor(request.OperationID, operation.TypeCreate, operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}, semantic)
	if err != nil {
		return ContainerResult{}, err
	}
	var result ContainerResult
	err = c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = containerResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionResume {
			record, getErr := tx.GetContainerAttempt(request.ContainerID)
			if getErr != nil {
				return getErr
			}
			result = ContainerResult{Resolution: resolved.Resolution, Operation: resolved.Record.Operation.Clone(), Fingerprint: binding.Fingerprint, ContainerAttempt: pairPointer(record.ContainerAttempt)}
			return nil
		}

		existing, getErr := tx.GetContainerAttempt(request.ContainerID)
		if getErr == nil {
			existingSemantic := containerCreateSemantic{
				SandboxID:   existing.ContainerAttempt.Container.SandboxID,
				ContainerID: existing.ContainerAttempt.Container.ID,
				AttemptID:   existing.ContainerAttempt.Attempt.ID,
				Process:     existing.ContainerAttempt.Container.Spec.Process,
				ImageDigest: existing.ContainerAttempt.Container.Spec.ImageDigest,
				RootFS:      existing.ContainerAttempt.Container.Spec.RootFS,
			}
			existingBinding, fingerprintErr := bindingFor(request.OperationID, operation.TypeCreate, binding.Target, existingSemantic)
			if fingerprintErr != nil {
				return fingerprintErr
			}
			if !existingBinding.Fingerprint.Equal(binding.Fingerprint) {
				return domain.NewError(domain.CodeAlreadyExists, "container_id", "already exists with different immutable create intent")
			}
			return c.persistContainerNoop(tx, binding, existing, &result)
		}
		if !errors.Is(getErr, state.ErrNotFound) {
			return getErr
		}
		sandboxRecord, getErr := tx.GetSandbox(request.SandboxID)
		if getErr != nil {
			return getErr
		}
		existingPairs, listErr := tx.ListContainerAttempts(request.SandboxID)
		if listErr != nil {
			return listErr
		}
		for _, existingPair := range existingPairs {
			if domain.IsActiveAttempt(existingPair.ContainerAttempt.Attempt.Phase) {
				return domain.NewError(domain.CodeFailedPrecondition, "sandbox.active_attempt", "must be terminal before creating the next Container/Attempt pair")
			}
		}
		pair, createErr := domain.NewContainerAttempt(sandboxRecord.Sandbox, request.ContainerID, request.AttemptID, request.Process, request.ImageDigest, request.RootFS)
		if createErr != nil {
			return createErr
		}
		if setErr := sandboxRecord.Sandbox.SetCurrentPair(request.ContainerID, request.AttemptID); setErr != nil {
			return setErr
		}
		operationRecord, putErr := c.putNewActiveOperation(tx, binding, nil)
		if putErr != nil {
			return putErr
		}
		storedPair, putErr := tx.PutContainerAttempt(state.NewContainerAttemptRecord(pair), 0)
		if putErr != nil {
			return putErr
		}
		if _, putErr = tx.PutSandbox(sandboxRecord, sandboxRecord.Revision); putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StagePersistIntent, operation.ResultPending, c.clock.Now(), 0, pair.Container.Status.Generation, pair.Container.Status.ObservedGeneration, map[string]string{"phase": pair.Attempt.Phase.String(), "attempt_id": string(pair.Attempt.ID)}, containerEventResources(pair)...)
		if eventErr != nil {
			return eventErr
		}
		storedPair, putErr = observeContainer(tx, storedPair, event)
		if putErr != nil {
			return putErr
		}
		result = ContainerResult{Resolution: operation.ResolutionNew, Operation: operationRecord.Operation, Fingerprint: binding.Fingerprint, ContainerAttempt: pairPointer(storedPair.ContainerAttempt)}
		return nil
	})
	return finishContainerResult(result, err)
}

// ConfirmContainerCreate marks a creating pair Created only after explicit preparation verification.
func (c *Coordinator) ConfirmContainerCreate(ctx context.Context, request ContainerConfirmRequest) (ContainerResult, error) {
	if err := request.Verification.validateFor(VerificationAttemptCreated); err != nil {
		return ContainerResult{}, err
	}
	binding, err := suppliedBinding(request.OperationID, operation.TypeCreate, operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}, request.Fingerprint)
	if err != nil {
		return ContainerResult{}, err
	}
	return c.confirmContainerTransition(ctx, binding, request.Verification, domain.AttemptCreating, domain.AttemptCreated)
}

// BeginContainerStart persists start intent while leaving the start gate and Attempt phase unchanged.
func (c *Coordinator) BeginContainerStart(ctx context.Context, request ContainerActionRequest) (ContainerResult, error) {
	binding, err := bindingFor(request.OperationID, operation.TypeStart, operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}, containerActionSemantic{ContainerID: request.ContainerID})
	if err != nil {
		return ContainerResult{}, err
	}
	var result ContainerResult
	err = c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = containerResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		record, getErr := tx.GetContainerAttempt(request.ContainerID)
		if getErr != nil {
			return getErr
		}
		if resolved.Resolution == operation.ResolutionResume {
			result = ContainerResult{Resolution: resolved.Resolution, Operation: resolved.Record.Operation.Clone(), Fingerprint: binding.Fingerprint, ContainerAttempt: pairPointer(record.ContainerAttempt)}
			return nil
		}
		if record.ContainerAttempt.Attempt.Phase == domain.AttemptRunning {
			return c.persistContainerNoop(tx, binding, record, &result)
		}
		if record.ContainerAttempt.Attempt.Phase != domain.AttemptCreated {
			return domain.NewError(domain.CodeFailedPrecondition, "attempt.phase", "must be Created or already Running")
		}
		operationRecord, putErr := c.putNewActiveOperation(tx, binding, nil)
		if putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StagePersistIntent, operation.ResultPending, c.clock.Now(), 0, record.ContainerAttempt.Container.Status.Generation, record.ContainerAttempt.Container.Status.ObservedGeneration, map[string]string{"phase": record.ContainerAttempt.Attempt.Phase.String()}, containerEventResources(record.ContainerAttempt)...)
		if eventErr != nil {
			return eventErr
		}
		record, putErr = observeContainer(tx, record, event)
		if putErr != nil {
			return putErr
		}
		result = ContainerResult{Resolution: operation.ResolutionNew, Operation: operationRecord.Operation, Fingerprint: binding.Fingerprint, ContainerAttempt: pairPointer(record.ContainerAttempt)}
		return nil
	})
	return finishContainerResult(result, err)
}

// ConfirmContainerStart marks a Created pair Running only after verified strong process identity is supplied.
func (c *Coordinator) ConfirmContainerStart(ctx context.Context, request ContainerConfirmRequest) (ContainerResult, error) {
	if err := request.Verification.validateFor(VerificationAttemptRunning); err != nil {
		return ContainerResult{}, err
	}
	if request.Verification.ProcessIdentity == nil {
		return ContainerResult{}, fmt.Errorf("%w: running confirmation requires process identity", ErrVerificationRequired)
	}
	binding, err := suppliedBinding(request.OperationID, operation.TypeStart, operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}, request.Fingerprint)
	if err != nil {
		return ContainerResult{}, err
	}
	return c.confirmContainerTransition(ctx, binding, request.Verification, domain.AttemptCreated, domain.AttemptRunning)
}

// BeginContainerDelete persists two-phase teardown intent even when metadata is absent, so host absence still requires confirmation.
func (c *Coordinator) BeginContainerDelete(ctx context.Context, request ContainerActionRequest) (ContainerResult, error) {
	binding, err := bindingFor(request.OperationID, operation.TypeDelete, operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}, containerActionSemantic{ContainerID: request.ContainerID})
	if err != nil {
		return ContainerResult{}, err
	}
	var result ContainerResult
	err = c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = containerResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		record, getErr := tx.GetContainerAttempt(request.ContainerID)
		if errors.Is(getErr, state.ErrNotFound) {
			if resolved.Resolution == operation.ResolutionResume {
				result = ContainerResult{Resolution: resolved.Resolution, Operation: resolved.Record.Operation.Clone(), Fingerprint: binding.Fingerprint}
				return nil
			}
			operationRecord, putErr := c.putNewActiveOperation(tx, binding, nil)
			if putErr != nil {
				return putErr
			}
			if _, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StagePersistIntent, operation.ResultPending, c.clock.Now(), 0, 0, 0, map[string]string{"phase": "metadata_absent_pending_host_verification"}); eventErr != nil {
				return eventErr
			}
			result = ContainerResult{Resolution: operation.ResolutionNew, Operation: operationRecord.Operation, Fingerprint: binding.Fingerprint}
			return nil
		}
		if getErr != nil {
			return getErr
		}
		if resolved.Resolution == operation.ResolutionResume {
			result = ContainerResult{Resolution: resolved.Resolution, Operation: resolved.Record.Operation.Clone(), Fingerprint: binding.Fingerprint, ContainerAttempt: pairPointer(record.ContainerAttempt)}
			return nil
		}
		switch record.ContainerAttempt.Attempt.Phase {
		case domain.AttemptCreated:
			if transitionErr := record.ContainerAttempt.Transition(domain.AttemptStopped, domain.NotApplicableOutcome()); transitionErr != nil {
				return transitionErr
			}
		case domain.AttemptStopped:
		default:
			return domain.NewError(domain.CodeFailedPrecondition, "attempt.phase", "must be Created or Stopped before delete")
		}
		operationRecord, putErr := c.putNewActiveOperation(tx, binding, nil)
		if putErr != nil {
			return putErr
		}
		updated, putErr := tx.PutContainerAttempt(record, record.Revision)
		if putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StagePersistIntent, operation.ResultPending, c.clock.Now(), 0, updated.ContainerAttempt.Container.Status.Generation, updated.ContainerAttempt.Container.Status.ObservedGeneration, map[string]string{"phase": updated.ContainerAttempt.Attempt.Phase.String()}, containerEventResources(updated.ContainerAttempt)...)
		if eventErr != nil {
			return eventErr
		}
		updated, putErr = observeContainer(tx, updated, event)
		if putErr != nil {
			return putErr
		}
		result = ContainerResult{Resolution: operation.ResolutionNew, Operation: operationRecord.Operation, Fingerprint: binding.Fingerprint, ContainerAttempt: pairPointer(updated.ContainerAttempt)}
		return nil
	})
	return finishContainerResult(result, err)
}

// ConfirmContainerDelete removes a stopped pair and matching Sandbox current refs only after absence verification.
func (c *Coordinator) ConfirmContainerDelete(ctx context.Context, request ContainerConfirmRequest) (ContainerResult, error) {
	if err := request.Verification.validateFor(VerificationAttemptAbsent); err != nil {
		return ContainerResult{}, err
	}
	binding, err := suppliedBinding(request.OperationID, operation.TypeDelete, operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}, request.Fingerprint)
	if err != nil {
		return ContainerResult{}, err
	}
	var result ContainerResult
	err = c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionNew {
			return fmt.Errorf("confirm Container delete operation %q: %w", request.OperationID, state.ErrNotFound)
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = containerResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		pairRecord, getErr := tx.GetContainerAttempt(request.ContainerID)
		if errors.Is(getErr, state.ErrNotFound) {
			if len(resolved.Record.Releases) != 0 {
				return domain.NewError(domain.CodeFailedPrecondition, "attempt.host_resources", "metadata-absent delete cannot carry unmatched cleanup releases")
			}
			response, encodeErr := encodeResponse(containerResponse{Removed: true})
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
			result, putErr = containerResultFromRecord(operation.ResolutionResume, operationRecord)
			return putErr
		}
		if getErr != nil {
			return getErr
		}
		pair := pairRecord.ContainerAttempt
		if pair.Attempt.Phase != domain.AttemptStopped {
			return domain.NewError(domain.CodeFailedPrecondition, "attempt.phase", "must be Stopped through delete confirmation")
		}
		if releaseErr := validateCompleteCleanupReleases(*resolved.Record, pairRecord.HostResources); releaseErr != nil {
			return releaseErr
		}
		hostBinding, bindingErr := containerHostBindingFromRecord(pairRecord)
		if bindingErr != nil {
			return bindingErr
		}
		sandboxRecord, getErr := tx.GetSandbox(pair.Container.SandboxID)
		if getErr != nil {
			return getErr
		}
		if currentPairMatches(sandboxRecord.Sandbox, pair.Container.ID, pair.Attempt.ID) {
			if clearErr := sandboxRecord.Sandbox.ClearCurrentPair(pair.Container.ID, pair.Attempt.ID); clearErr != nil {
				return clearErr
			}
			if _, putErr := tx.PutSandbox(sandboxRecord, sandboxRecord.Revision); putErr != nil {
				return putErr
			}
		}
		pairRecord.HostResources = nil
		pairRecord, putErr := tx.PutContainerAttempt(pairRecord, pairRecord.Revision)
		if putErr != nil {
			return putErr
		}
		response, encodeErr := encodeResponse(containerResponse{HostBinding: hostBinding, Removed: true})
		if encodeErr != nil {
			return encodeErr
		}
		operationRecord, putErr := completeOperation(tx, *resolved.Record, operation.ResultSucceeded, response)
		if putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StageComplete, operation.ResultSucceeded, request.Verification.ObservedAt, request.Verification.Duration, pair.Container.Status.Generation, pair.Container.Status.ObservedGeneration, request.Verification, containerEventResources(pair)...)
		if eventErr != nil {
			return eventErr
		}
		pairRecord, putErr = observeContainer(tx, pairRecord, event)
		if putErr != nil {
			return putErr
		}
		if deleteErr := tx.DeleteContainerAttempt(request.ContainerID, pairRecord.Revision); deleteErr != nil {
			return deleteErr
		}
		result, putErr = containerResultFromRecord(operation.ResolutionResume, operationRecord)
		return putErr
	})
	return finishContainerResult(result, err)
}

// BeginRecordStopped persists Linux observation intent before OOM and terminal process evidence are checkpointed.
func (c *Coordinator) BeginRecordStopped(ctx context.Context, request RecordStoppedRequest) (ContainerResult, error) {
	if err := request.Verification.validateFor(VerificationAttemptStopped); err != nil {
		return ContainerResult{}, err
	}
	if err := request.Outcome.Validate(); err != nil {
		return ContainerResult{}, err
	}
	fingerprint, err := request.RequestFingerprint()
	if err != nil {
		return ContainerResult{}, err
	}
	binding, err := suppliedBinding(request.OperationID, operation.TypeStop, operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}, fingerprint)
	if err != nil {
		return ContainerResult{}, err
	}
	var result ContainerResult
	err = c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = containerResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		record, getErr := tx.GetContainerAttempt(request.ContainerID)
		if getErr != nil {
			return getErr
		}
		if resolved.Resolution == operation.ResolutionResume {
			result = ContainerResult{Resolution: resolved.Resolution, Operation: resolved.Record.Operation.Clone(), Fingerprint: binding.Fingerprint, ContainerAttempt: pairPointer(record.ContainerAttempt)}
			return nil
		}
		if record.ContainerAttempt.Attempt.Phase != domain.AttemptRunning {
			return domain.NewError(domain.CodeFailedPrecondition, "attempt.phase", "terminal observation intent requires a Running Attempt")
		}
		operationRecord, putErr := c.putNewActiveOperation(tx, binding, nil)
		if putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StagePersistIntent, operation.ResultPending, c.clock.Now(), 0,
			record.ContainerAttempt.Container.Status.Generation, record.ContainerAttempt.Container.Status.ObservedGeneration,
			map[string]string{"phase": "terminal_observation_pending"}, containerEventResources(record.ContainerAttempt)...)
		if eventErr != nil {
			return eventErr
		}
		record, putErr = observeContainer(tx, record, event)
		if putErr != nil {
			return putErr
		}
		result = ContainerResult{Resolution: operation.ResolutionNew, Operation: operationRecord.Operation, Fingerprint: binding.Fingerprint, ContainerAttempt: pairPointer(record.ContainerAttempt)}
		return nil
	})
	return finishContainerResult(result, err)
}

// RecordStopped atomically persists a standalone verified terminal observation and replayable stop operation.
func (c *Coordinator) RecordStopped(ctx context.Context, request RecordStoppedRequest) (ContainerResult, error) {
	if err := request.Verification.validateFor(VerificationAttemptStopped); err != nil {
		return ContainerResult{}, err
	}
	if err := request.Outcome.Validate(); err != nil {
		return ContainerResult{}, err
	}
	fingerprint, err := request.RequestFingerprint()
	if err != nil {
		return ContainerResult{}, err
	}
	binding, err := suppliedBinding(request.OperationID, operation.TypeStop, operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}, fingerprint)
	if err != nil {
		return ContainerResult{}, err
	}
	return c.recordStopped(ctx, binding, request.Outcome, request.Conditions, request.Verification, nil)
}

// RecordContainerStartTerminal completes an active Start when the wrapper proves
// the one-shot gate was consumed and the child became terminal before a Running
// response could be durably confirmed.
func (c *Coordinator) RecordContainerStartTerminal(ctx context.Context, request ContainerStartTerminalRequest) (ContainerResult, error) {
	if err := request.Verification.validateFor(VerificationAttemptStopped); err != nil {
		return ContainerResult{}, err
	}
	if err := request.Outcome.Validate(); err != nil {
		return ContainerResult{}, err
	}
	binding, err := suppliedBinding(
		request.OperationID, operation.TypeStart,
		operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)},
		request.Fingerprint,
	)
	if err != nil {
		return ContainerResult{}, err
	}
	var result ContainerResult
	err = c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionNew {
			return fmt.Errorf("record terminal start operation %q: %w", request.OperationID, state.ErrNotFound)
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = containerResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		if stageErr := requireLinuxStage(*resolved.Record, operation.StageObserveProcess); stageErr != nil {
			return stageErr
		}
		record, getErr := tx.GetContainerAttempt(request.ContainerID)
		if getErr != nil {
			return getErr
		}
		if record.ContainerAttempt.Attempt.Phase != domain.AttemptCreated {
			return domain.NewError(domain.CodeFailedPrecondition, "attempt.phase", "terminal Start completion requires the original Created Attempt")
		}
		if conditionErr := record.ContainerAttempt.SetConditions(request.Conditions); conditionErr != nil {
			return conditionErr
		}
		if request.Outcome.Presence == domain.OutcomeNotApplicable {
			if transitionErr := record.ContainerAttempt.Transition(domain.AttemptStopped, request.Outcome); transitionErr != nil {
				return transitionErr
			}
		} else {
			if transitionErr := record.ContainerAttempt.Transition(domain.AttemptRunning, domain.PendingOutcome()); transitionErr != nil {
				return transitionErr
			}
			if transitionErr := record.ContainerAttempt.Transition(domain.AttemptStopped, request.Outcome); transitionErr != nil {
				return transitionErr
			}
		}
		updated, putErr := tx.PutContainerAttempt(record, record.Revision)
		if putErr != nil {
			return putErr
		}
		operationResult := operation.ResultSucceeded
		operationRecord := state.OperationRecord{}
		if request.Outcome.Presence == domain.OutcomeNotApplicable {
			operationResult = operation.ResultFailed
			operationRecord, putErr = failOperation(tx, *resolved.Record, operation.ReasonInternal, nil)
		} else {
			operationRecord, putErr = completeOperation(tx, *resolved.Record, operationResult, nil)
		}
		if putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(
			tx, operationRecord.Operation, operation.StageComplete, operationResult,
			request.Verification.ObservedAt, request.Verification.Duration,
			updated.ContainerAttempt.Container.Status.Generation,
			updated.ContainerAttempt.Container.Status.ObservedGeneration,
			request.Verification, containerEventResources(updated.ContainerAttempt)...,
		)
		if eventErr != nil {
			return eventErr
		}
		updated, putErr = observeContainer(tx, updated, event)
		if putErr != nil {
			return putErr
		}
		response, encodeErr := encodeResponse(containerResponse{ContainerAttempt: pairPointer(updated.ContainerAttempt)})
		if encodeErr != nil {
			return encodeErr
		}
		operationRecord, putErr = finalizeOperationResponse(tx, operationRecord, response)
		if putErr != nil {
			return putErr
		}
		result, putErr = containerResultFromRecord(operation.ResolutionResume, operationRecord)
		return putErr
	})
	return finishContainerResult(result, err)
}

// PlanKill returns a reverified active plan or an already-stopped terminal no-op; it never sends a signal.
func (c *Coordinator) PlanKill(ctx context.Context, request KillRequest) (KillResult, error) {
	if err := request.Policy.Validate(); err != nil {
		return KillResult{}, err
	}
	binding, err := bindingFor(request.OperationID, operation.TypeKill, operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}, killSemantic{ContainerID: request.ContainerID, Policy: request.Policy})
	if err != nil {
		return KillResult{}, err
	}
	var result KillResult
	err = c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = killResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionResume {
			prior, decodeErr := decodeKillResponse(resolved.Record.Operation.Response)
			if decodeErr != nil {
				return decodeErr
			}
			pairRecord, getErr := tx.GetContainerAttempt(request.ContainerID)
			if getErr != nil {
				return getErr
			}
			identity, verifyErr := c.verifyKillTarget(ctx, binding.Target, pairRecord.ContainerAttempt)
			if verifyErr != nil {
				return verifyErr
			}
			result = KillResult{Resolution: resolved.Resolution, Operation: resolved.Record.Operation.Clone(), Fingerprint: binding.Fingerprint, Plan: prior.Plan, Actionable: true, ProcessIdentity: identity, ContainerAttempt: pairPointer(pairRecord.ContainerAttempt)}
			return nil
		}
		pairRecord, getErr := tx.GetContainerAttempt(request.ContainerID)
		if getErr != nil {
			return getErr
		}
		pair := pairRecord.ContainerAttempt
		if pair.Attempt.Phase == domain.AttemptStopped {
			operationRecord, putErr := c.putNewTerminalOperation(tx, binding, operation.ResultNoop, nil)
			if putErr != nil {
				return putErr
			}
			event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StageComplete, operation.ResultNoop, c.clock.Now(), 0, pair.Container.Status.Generation, pair.Container.Status.ObservedGeneration, map[string]string{"phase": pair.Attempt.Phase.String(), "plan": "not_required"}, containerEventResources(pair)...)
			if eventErr != nil {
				return eventErr
			}
			pairRecord, putErr = observeContainer(tx, pairRecord, event)
			if putErr != nil {
				return putErr
			}
			var identity domain.ProcessIdentity
			if pairRecord.ContainerAttempt.Attempt.ProcessIdentity != nil {
				identity = *pairRecord.ContainerAttempt.Attempt.ProcessIdentity
			}
			response, encodeErr := encodeResponse(killResponse{Actionable: false, ProcessIdentity: identity, ContainerAttempt: pairPointer(pairRecord.ContainerAttempt)})
			if encodeErr != nil {
				return encodeErr
			}
			operationRecord, putErr = finalizeOperationResponse(tx, operationRecord, response)
			if putErr != nil {
				return putErr
			}
			result, putErr = killResultFromRecord(operation.ResolutionNew, operationRecord)
			return putErr
		}
		plan, planErr := domain.NewKillPlan(request.Policy)
		if planErr != nil {
			return planErr
		}
		identity, verifyErr := c.verifyKillTarget(ctx, binding.Target, pair)
		if verifyErr != nil {
			return verifyErr
		}
		responseValue := killResponse{Plan: plan, Actionable: true, ProcessIdentity: identity, ContainerAttempt: pairPointer(pair)}
		response, encodeErr := encodeResponse(responseValue)
		if encodeErr != nil {
			return encodeErr
		}
		operationRecord, putErr := c.putNewActiveOperation(tx, binding, response)
		if putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StagePersistIntent, operation.ResultPending, c.clock.Now(), 0, pair.Container.Status.Generation, pair.Container.Status.ObservedGeneration, map[string]string{"phase": pair.Attempt.Phase.String(), "plan": "recorded_not_executed"}, containerEventResources(pair)...)
		if eventErr != nil {
			return eventErr
		}
		pairRecord, putErr = observeContainer(tx, pairRecord, event)
		if putErr != nil {
			return putErr
		}
		result = KillResult{Resolution: operation.ResolutionNew, Operation: operationRecord.Operation, Fingerprint: binding.Fingerprint, Plan: plan, Actionable: true, ProcessIdentity: identity, ContainerAttempt: pairPointer(pairRecord.ContainerAttempt)}
		return nil
	})
	return finishKillResult(result, err)
}

// RecordKillStopped completes a previously planned kill after explicit terminal verification without sending a signal.
func (c *Coordinator) RecordKillStopped(ctx context.Context, request KillStoppedRequest) (KillResult, error) {
	if err := request.Verification.validateFor(VerificationAttemptStopped); err != nil {
		return KillResult{}, err
	}
	if err := request.Outcome.Validate(); err != nil {
		return KillResult{}, err
	}
	binding, err := suppliedBinding(request.OperationID, operation.TypeKill, operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}, request.Fingerprint)
	if err != nil {
		return KillResult{}, err
	}
	var result KillResult
	err = c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionNew {
			return fmt.Errorf("record kill stop operation %q: %w", request.OperationID, state.ErrNotFound)
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = killResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		if stageErr := requireLinuxStage(*resolved.Record, operation.StageObserveProcess); stageErr != nil {
			return stageErr
		}
		prior, decodeErr := decodeKillResponse(resolved.Record.Operation.Response)
		if decodeErr != nil {
			return decodeErr
		}
		pairRecord, getErr := tx.GetContainerAttempt(request.ContainerID)
		if getErr != nil {
			return getErr
		}
		if pairRecord.ContainerAttempt.Attempt.Phase != domain.AttemptRunning {
			return domain.NewError(domain.CodeFailedPrecondition, "attempt.phase", "planned kill completion requires the original Running Attempt")
		}
		if setErr := pairRecord.ContainerAttempt.SetConditions(request.Conditions); setErr != nil {
			return setErr
		}
		if transitionErr := pairRecord.ContainerAttempt.Transition(domain.AttemptStopped, request.Outcome); transitionErr != nil {
			return transitionErr
		}
		updated, putErr := tx.PutContainerAttempt(pairRecord, pairRecord.Revision)
		if putErr != nil {
			return putErr
		}
		operationRecord, putErr := completeOperation(tx, *resolved.Record, operation.ResultSucceeded, nil)
		if putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StageComplete, operation.ResultSucceeded, request.Verification.ObservedAt, request.Verification.Duration, updated.ContainerAttempt.Container.Status.Generation, updated.ContainerAttempt.Container.Status.ObservedGeneration, request.Verification, containerEventResources(updated.ContainerAttempt)...)
		if eventErr != nil {
			return eventErr
		}
		updated, putErr = observeContainer(tx, updated, event)
		if putErr != nil {
			return putErr
		}
		response, encodeErr := encodeResponse(killResponse{Plan: prior.Plan, Actionable: false, ProcessIdentity: prior.ProcessIdentity, ContainerAttempt: pairPointer(updated.ContainerAttempt)})
		if encodeErr != nil {
			return encodeErr
		}
		operationRecord, putErr = finalizeOperationResponse(tx, operationRecord, response)
		if putErr != nil {
			return putErr
		}
		result, putErr = killResultFromRecord(operation.ResolutionResume, operationRecord)
		return putErr
	})
	return finishKillResult(result, err)
}

// confirmContainerTransition applies one verified Attempt edge and completes its original operation atomically.
func (c *Coordinator) confirmContainerTransition(ctx context.Context, binding operation.Binding, verification Verification, from, to domain.AttemptPhase) (ContainerResult, error) {
	var result ContainerResult
	err := c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionNew {
			return fmt.Errorf("confirm Container operation %q: %w", binding.ID, state.ErrNotFound)
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = containerResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		minimumStage := operation.StageObserveProcess
		if binding.Type == operation.TypeCreate {
			minimumStage = operation.StageAttachCgroup
		}
		if stageErr := requireLinuxStage(*resolved.Record, minimumStage); stageErr != nil {
			return stageErr
		}
		record, getErr := tx.GetContainerAttempt(domain.ContainerID(binding.Target.ID))
		if getErr != nil {
			return getErr
		}
		if record.ContainerAttempt.Attempt.Phase != from {
			return domain.NewError(domain.CodeFailedPrecondition, "attempt.phase", fmt.Sprintf("must be %s before confirmation", from))
		}
		if verification.ProcessIdentity != nil {
			if setErr := record.ContainerAttempt.SetProcessIdentity(*verification.ProcessIdentity); setErr != nil {
				return setErr
			}
		}
		if setErr := record.ContainerAttempt.SetStreams(verification.Streams); setErr != nil {
			return setErr
		}
		if transitionErr := record.ContainerAttempt.Transition(to, domain.PendingOutcome()); transitionErr != nil {
			return transitionErr
		}
		operationSource := resolved.Record.Clone()
		if from == domain.AttemptCreating && to == domain.AttemptCreated {
			var adoptErr error
			operationSource, record.HostResources, adoptErr = adoptOperationReceipts(operationSource)
			if adoptErr != nil {
				return adoptErr
			}
		}
		updated, putErr := tx.PutContainerAttempt(record, record.Revision)
		if putErr != nil {
			return putErr
		}
		operationRecord, putErr := completeOperation(tx, operationSource, operation.ResultSucceeded, nil)
		if putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StageComplete, operation.ResultSucceeded, verification.ObservedAt, verification.Duration, updated.ContainerAttempt.Container.Status.Generation, updated.ContainerAttempt.Container.Status.ObservedGeneration, verification, containerEventResources(updated.ContainerAttempt)...)
		if eventErr != nil {
			return eventErr
		}
		updated, putErr = observeContainer(tx, updated, event)
		if putErr != nil {
			return putErr
		}
		var hostBinding *ContainerHostBinding
		if binding.Type == operation.TypeCreate {
			hostBinding, putErr = containerHostBindingFromRecord(updated)
			if putErr != nil {
				return putErr
			}
		}
		response, encodeErr := encodeResponse(containerResponse{ContainerAttempt: pairPointer(updated.ContainerAttempt), HostBinding: hostBinding})
		if encodeErr != nil {
			return encodeErr
		}
		operationRecord, putErr = finalizeOperationResponse(tx, operationRecord, response)
		if putErr != nil {
			return putErr
		}
		result, putErr = containerResultFromRecord(operation.ResolutionResume, operationRecord)
		return putErr
	})
	return finishContainerResult(result, err)
}

// recordStopped wraps terminal observation in one Store transaction for standalone stop operations.
func (c *Coordinator) recordStopped(ctx context.Context, binding operation.Binding, outcome domain.Outcome, conditions []domain.Condition, verification Verification, existing *resolvedOperation) (ContainerResult, error) {
	var result ContainerResult
	err := c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if existing != nil {
			resolved = *existing
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = containerResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		if c.profile == state.HostProfileLinuxM2 {
			if resolved.Resolution == operation.ResolutionNew {
				return domain.NewError(domain.CodeFailedPrecondition, "operation", "Linux M2 terminal observation must begin and checkpoint provider evidence first")
			}
			if stageErr := requireLinuxStage(*resolved.Record, operation.StageObserveProcess); stageErr != nil {
				return stageErr
			}
		}
		result, resolveErr = c.recordStoppedInTx(tx, binding, outcome, conditions, verification, resolved)
		return resolveErr
	})
	return finishContainerResult(result, err)
}

// verifyKillTarget revalidates the current strong process identity every time a caller may act on a kill plan.
func (c *Coordinator) verifyKillTarget(ctx context.Context, target operation.Target, pair domain.ContainerAttempt) (domain.ProcessIdentity, error) {
	if pair.Attempt.Phase != domain.AttemptRunning || pair.Attempt.ProcessIdentity == nil {
		return domain.ProcessIdentity{}, domain.NewError(domain.CodeFailedPrecondition, "attempt.phase", "kill requires a Running Attempt with strong identity")
	}
	identity := *pair.Attempt.ProcessIdentity
	if err := identity.Validate(); err != nil {
		return domain.ProcessIdentity{}, err
	}
	if err := c.verifier.Verify(ctx, target, identity); err != nil {
		return domain.ProcessIdentity{}, fmt.Errorf("action-time process identity verification: %w", err)
	}
	return identity, nil
}

// recordStoppedInTx transitions one Attempt to Stopped and completes or creates its bound operation.
func (c *Coordinator) recordStoppedInTx(tx state.Tx, binding operation.Binding, outcome domain.Outcome, conditions []domain.Condition, verification Verification, resolved resolvedOperation) (ContainerResult, error) {
	record, err := tx.GetContainerAttempt(domain.ContainerID(binding.Target.ID))
	if err != nil {
		return ContainerResult{}, err
	}
	if record.ContainerAttempt.Attempt.Phase == domain.AttemptStopped {
		if resolved.Resolution != operation.ResolutionNew {
			return ContainerResult{}, domain.NewError(domain.CodeFailedPrecondition, "attempt.phase", "active operation cannot rediscover an unrelated stopped result")
		}
		var result ContainerResult
		if err := c.persistContainerNoop(tx, binding, record, &result); err != nil {
			return ContainerResult{}, err
		}
		return result, nil
	}
	if err := record.ContainerAttempt.SetConditions(conditions); err != nil {
		return ContainerResult{}, err
	}
	if err := record.ContainerAttempt.Transition(domain.AttemptStopped, outcome); err != nil {
		return ContainerResult{}, err
	}
	var operationRecord state.OperationRecord
	if resolved.Resolution == operation.ResolutionNew {
		operationRecord, err = c.putNewActiveOperation(tx, binding, nil)
		if err != nil {
			return ContainerResult{}, err
		}
		event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StagePersistIntent, operation.ResultPending, c.clock.Now(), 0, record.ContainerAttempt.Container.Status.Generation, record.ContainerAttempt.Container.Status.ObservedGeneration, map[string]string{"phase": "terminal_observation"}, containerEventResources(record.ContainerAttempt)...)
		if eventErr != nil {
			return ContainerResult{}, eventErr
		}
		if eventErr := record.ContainerAttempt.SetLastObservation(lifecycleObservation(event)); eventErr != nil {
			return ContainerResult{}, eventErr
		}
		resolved.Record = &operationRecord
	}
	updated, err := tx.PutContainerAttempt(record, record.Revision)
	if err != nil {
		return ContainerResult{}, err
	}
	operationRecord, err = completeOperation(tx, *resolved.Record, operation.ResultSucceeded, nil)
	if err != nil {
		return ContainerResult{}, err
	}
	event, eventErr := appendOperationEvent(tx, operationRecord.Operation, operation.StageComplete, operation.ResultSucceeded, verification.ObservedAt, verification.Duration, updated.ContainerAttempt.Container.Status.Generation, updated.ContainerAttempt.Container.Status.ObservedGeneration, verification, containerEventResources(updated.ContainerAttempt)...)
	if eventErr != nil {
		return ContainerResult{}, eventErr
	}
	updated, err = observeContainer(tx, updated, event)
	if err != nil {
		return ContainerResult{}, err
	}
	response, err := encodeResponse(containerResponse{ContainerAttempt: pairPointer(updated.ContainerAttempt)})
	if err != nil {
		return ContainerResult{}, err
	}
	operationRecord, err = finalizeOperationResponse(tx, operationRecord, response)
	if err != nil {
		return ContainerResult{}, err
	}
	return containerResultFromRecord(resolved.Resolution, operationRecord)
}

// persistContainerNoop stores a replayable no-op after confirming the target phase already exists.
func (c *Coordinator) persistContainerNoop(tx state.Tx, binding operation.Binding, record state.ContainerAttemptRecord, result *ContainerResult) error {
	operationRecord, err := c.putNewTerminalOperation(tx, binding, operation.ResultNoop, nil)
	if err != nil {
		return err
	}
	event, err := appendOperationEvent(tx, operationRecord.Operation, operation.StageComplete, operation.ResultNoop, c.clock.Now(), 0, record.ContainerAttempt.Container.Status.Generation, record.ContainerAttempt.Container.Status.ObservedGeneration, map[string]string{"phase": record.ContainerAttempt.Attempt.Phase.String()}, containerEventResources(record.ContainerAttempt)...)
	if err != nil {
		return err
	}
	record, err = observeContainer(tx, record, event)
	if err != nil {
		return err
	}
	var hostBinding *ContainerHostBinding
	if binding.Type == operation.TypeCreate {
		hostBinding, err = containerHostBindingFromRecord(record)
		if err != nil {
			return err
		}
	}
	response, err := encodeResponse(containerResponse{ContainerAttempt: pairPointer(record.ContainerAttempt), HostBinding: hostBinding})
	if err != nil {
		return err
	}
	operationRecord, err = finalizeOperationResponse(tx, operationRecord, response)
	if err != nil {
		return err
	}
	decoded, err := containerResultFromRecord(operation.ResolutionNew, operationRecord)
	if err != nil {
		return err
	}
	*result = decoded
	return nil
}

// currentPairMatches reports whether Sandbox current refs identify the pair being deleted.
func currentPairMatches(sandbox domain.Sandbox, containerID domain.ContainerID, attemptID domain.AttemptID) bool {
	return sandbox.Status.CurrentContainerID != nil && sandbox.Status.CurrentAttemptID != nil && *sandbox.Status.CurrentContainerID == containerID && *sandbox.Status.CurrentAttemptID == attemptID
}

// pairPointer returns a caller-owned deep copy for result and replay payload construction.
func pairPointer(pair domain.ContainerAttempt) *domain.ContainerAttempt {
	clone := pair.Clone()
	return &clone
}

// containerHostBindingFromRecord extracts one immutable artifact owner from a
// complete adopted inventory; abstract M1 records without host resources have
// no binding, while mixed or unadopted ownership fails closed.
func containerHostBindingFromRecord(record state.ContainerAttemptRecord) (*ContainerHostBinding, error) {
	if len(record.HostResources) == 0 {
		return nil, nil
	}
	owner := record.HostResources[0].Owner
	for _, receipt := range record.HostResources {
		if !receipt.Adopted || receipt.Owner != owner {
			return nil, errors.New("Container host inventory does not have one adopted acquisition owner")
		}
	}
	binding := ContainerHostBinding{
		ContainerID: record.ContainerAttempt.Container.ID,
		AttemptID:   record.ContainerAttempt.Attempt.ID,
		Generation:  record.ContainerAttempt.Container.Status.Generation,
		Owner:       owner,
	}
	if err := binding.Validate(); err != nil {
		return nil, err
	}
	return &binding, nil
}
