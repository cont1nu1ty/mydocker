package lifecycle

import (
	"context"
	"fmt"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/state"
)

// failureEventDetails keeps bounded classification next to the provider's explicit absence observation.
type failureEventDetails struct {
	Failure      Failure      `json:"failure"`
	Verification Verification `json:"verification"`
}

// FailSandboxCreateAfterRollback deletes creating metadata and records an exact failed result only after verified host absence.
func (c *Coordinator) FailSandboxCreateAfterRollback(ctx context.Context, request SandboxCreateFailureRequest) (SandboxResult, error) {
	if err := request.Failure.Validate(); err != nil {
		return SandboxResult{}, err
	}
	if err := request.Verification.validateFor(VerificationSandboxAbsent); err != nil {
		return SandboxResult{}, err
	}
	binding, err := suppliedBinding(
		request.OperationID, operation.TypeCreate,
		operation.Target{Kind: operation.TargetSandbox, ID: string(request.SandboxID)},
		request.Fingerprint,
	)
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
			return fmt.Errorf("fail Sandbox create operation %q: %w", request.OperationID, state.ErrNotFound)
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = sandboxResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		record, getErr := tx.GetSandbox(request.SandboxID)
		if getErr != nil {
			return getErr
		}
		if record.Sandbox.Status.Phase != domain.SandboxCreating {
			return domain.NewError(domain.CodeFailedPrecondition, "sandbox.phase", "failed create rollback requires Creating metadata")
		}
		if len(record.HostResources) != 0 {
			return domain.NewError(domain.CodeFailedPrecondition, "sandbox.host_resources", "failed create cannot own adopted host resources")
		}
		if transitionErr := record.Sandbox.Transition(domain.SandboxAbsent); transitionErr != nil {
			return transitionErr
		}
		record, putErr := tx.PutSandbox(record, record.Revision)
		if putErr != nil {
			return putErr
		}
		response, encodeErr := encodeResponse(sandboxResponse{Removed: true})
		if encodeErr != nil {
			return encodeErr
		}
		operationRecord, putErr := failOperation(tx, *resolved.Record, request.Failure.Reason, response)
		if putErr != nil {
			return putErr
		}
		if _, eventErr := appendOperationEvent(
			tx, operationRecord.Operation, operation.StageComplete, operation.ResultFailed,
			request.Verification.ObservedAt, request.Verification.Duration,
			record.Sandbox.Status.Generation, record.Sandbox.Status.ObservedGeneration,
			failureEventDetails{Failure: request.Failure, Verification: request.Verification},
		); eventErr != nil {
			return eventErr
		}
		if deleteErr := tx.DeleteSandbox(request.SandboxID, record.Revision); deleteErr != nil {
			return deleteErr
		}
		result, putErr = sandboxResultFromRecord(operation.ResolutionResume, operationRecord)
		return putErr
	})
	return finishSandboxResult(result, err)
}

// FailContainerCreateAfterRollback retains an immutable stopped history record and fails the operation only after Attempt absence proof.
func (c *Coordinator) FailContainerCreateAfterRollback(ctx context.Context, request ContainerCreateFailureRequest) (ContainerResult, error) {
	if err := request.Failure.Validate(); err != nil {
		return ContainerResult{}, err
	}
	if err := request.Verification.validateFor(VerificationAttemptAbsent); err != nil {
		return ContainerResult{}, err
	}
	binding, err := suppliedBinding(
		request.OperationID, operation.TypeCreate,
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
			return fmt.Errorf("fail Container create operation %q: %w", request.OperationID, state.ErrNotFound)
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result, resolveErr = containerResultFromRecord(resolved.Resolution, *resolved.Record)
			return resolveErr
		}
		record, getErr := tx.GetContainerAttempt(request.ContainerID)
		if getErr != nil {
			return getErr
		}
		if record.ContainerAttempt.Attempt.Phase != domain.AttemptCreating {
			return domain.NewError(domain.CodeFailedPrecondition, "attempt.phase", "failed create rollback requires Creating metadata")
		}
		if len(record.HostResources) != 0 {
			return domain.NewError(domain.CodeFailedPrecondition, "attempt.host_resources", "failed create cannot own adopted host resources")
		}
		failureCondition := domain.Condition{
			Type: domain.ConditionFailure, Reason: string(request.Failure.Reason), Message: request.Failure.Message,
		}
		if conditionErr := record.ContainerAttempt.UpsertCondition(failureCondition); conditionErr != nil {
			return conditionErr
		}
		if transitionErr := record.ContainerAttempt.Transition(domain.AttemptStopped, domain.NotApplicableOutcome()); transitionErr != nil {
			return transitionErr
		}
		updated, putErr := tx.PutContainerAttempt(record, record.Revision)
		if putErr != nil {
			return putErr
		}
		operationRecord, putErr := failOperation(tx, *resolved.Record, request.Failure.Reason, nil)
		if putErr != nil {
			return putErr
		}
		event, eventErr := appendOperationEvent(
			tx, operationRecord.Operation, operation.StageComplete, operation.ResultFailed,
			request.Verification.ObservedAt, request.Verification.Duration,
			updated.ContainerAttempt.Container.Status.Generation,
			updated.ContainerAttempt.Container.Status.ObservedGeneration,
			failureEventDetails{Failure: request.Failure, Verification: request.Verification},
			containerEventResources(updated.ContainerAttempt)...,
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
