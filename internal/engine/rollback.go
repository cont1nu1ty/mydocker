package engine

import (
	"context"
	"errors"
	"fmt"

	"mydocker/internal/domain"
	"mydocker/internal/lifecycle"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
	"mydocker/internal/rollback"
)

// RollbackCreate seals and persists an active create rollback before executing
// each bounded inverse in LIFO order, then records terminal failure only after
// every receipt is independently observed absent.
func (engine *Engine) RollbackCreate(ctx context.Context, operationID operation.OperationID, failure lifecycle.Failure) (operation.Operation, error) {
	if err := failure.Validate(); err != nil {
		return operation.Operation{}, err
	}
	progress, err := engine.lifecycle.GetOperationProgress(ctx, operationID)
	if err != nil {
		return operation.Operation{}, err
	}
	if progress.Operation.Type != operation.TypeCreate {
		return operation.Operation{}, errors.New("only an active create operation can enter acquisition rollback")
	}
	if progress.Operation.State.Terminal() {
		return progress.Operation.Clone(), nil
	}
	target := progress.Operation.Target
	releaseTarget := engine.targetLocks.lock(target)
	defer releaseTarget()
	return engine.rollbackCreateLocked(ctx, operationID, failure)
}

// rollbackCreateLocked performs the persisted LIFO rollback while the caller owns the operation target lock.
// Create paths use it to close a definite failure without recursively acquiring the same lock.
func (engine *Engine) rollbackCreateLocked(ctx context.Context, operationID operation.OperationID, failure lifecycle.Failure) (operation.Operation, error) {
	if err := failure.Validate(); err != nil {
		return operation.Operation{}, err
	}
	progress, err := engine.lifecycle.GetOperationProgress(ctx, operationID)
	if err != nil {
		return operation.Operation{}, err
	}
	if progress.Operation.Type != operation.TypeCreate {
		return operation.Operation{}, errors.New("only an active create operation can enter acquisition rollback")
	}
	if progress.Operation.State.Terminal() {
		return progress.Operation.Clone(), nil
	}
	cause := progress.RollbackCause
	if cause == nil {
		created, causeErr := rollback.NewCause(failure.Reason, failure.Message)
		if causeErr != nil {
			return operation.Operation{}, causeErr
		}
		cause = &created
	}
	failure = lifecycle.Failure{Reason: cause.Reason, Message: cause.Message}
	target := progress.Operation.Target
	records := cloneRollback(progress.Rollback)
	for index := range records {
		records[index].Started = true
	}
	if err := engine.checkpointRollback(ctx, progress, cause, records, failure, rollbackCondition(cause, nil), engine.unmeasuredCheckpoint()); err != nil {
		return operation.Operation{}, err
	}
	var resolver rollback.Resolver
	if len(records) > 0 {
		owner, ownerErr := rollbackOwner(progress.Receipts)
		if ownerErr != nil {
			return operation.Operation{}, ownerErr
		}
		resolver, err = engine.providers.Rollback.Resolver(owner)
		if err != nil {
			return operation.Operation{}, errors.Join(errors.New(cause.Message), err)
		}
	}
	report := rollback.Report{Primary: errors.New(failure.Message)}
	for index := len(records) - 1; index >= 0; index-- {
		if records[index].Succeeded {
			continue
		}
		inverse, resolveErr := resolver(records[index].Descriptor.Clone())
		if resolveErr != nil {
			updated, recordErr := records[index].RecordFailure(resolveErr)
			if recordErr != nil {
				return operation.Operation{}, recordErr
			}
			records[index] = updated
			report.Failures = append(report.Failures, rollback.StepFailure{Descriptor: records[index].Descriptor.Clone(), Err: resolveErr})
			progress, err = engine.lifecycle.GetOperationProgress(ctx, operationID)
			if err != nil {
				return operation.Operation{}, err
			}
			if err := engine.checkpointRollback(ctx, progress, cause, records, map[string]string{"failed_inverse": records[index].Descriptor.Name}, rollbackCondition(cause, &report.Failures[len(report.Failures)-1]), engine.unmeasuredCheckpoint()); err != nil {
				return operation.Operation{}, err
			}
			continue
		}
		inverseStartedAt := engine.beginMeasurement()
		inverseErr := inverse(ctx)
		inverseMeasurement := engine.finishMeasurement(inverseStartedAt)
		if inverseErr != nil {
			updated, recordErr := records[index].RecordFailure(inverseErr)
			if recordErr != nil {
				return operation.Operation{}, recordErr
			}
			records[index] = updated
			report.Failures = append(report.Failures, rollback.StepFailure{Descriptor: records[index].Descriptor.Clone(), Err: inverseErr})
			progress, err = engine.lifecycle.GetOperationProgress(ctx, operationID)
			if err != nil {
				return operation.Operation{}, err
			}
			if err := engine.checkpointRollback(ctx, progress, cause, records, map[string]string{"failed_inverse": records[index].Descriptor.Name}, rollbackCondition(cause, &report.Failures[len(report.Failures)-1]), inverseMeasurement); err != nil {
				return operation.Operation{}, err
			}
			continue
		}
		records[index].Succeeded = true
		progress, err = engine.lifecycle.GetOperationProgress(ctx, operationID)
		if err != nil {
			return operation.Operation{}, err
		}
		if err := engine.checkpointRollback(ctx, progress, cause, records, map[string]string{"completed_inverse": records[index].Descriptor.Name}, rollbackCondition(cause, nil), inverseMeasurement); err != nil {
			return operation.Operation{}, err
		}
	}
	if len(report.Failures) > 0 {
		return operation.Operation{}, report.Err()
	}
	observations := make([]provider.ResourceObservation, 0, len(progress.Receipts))
	for _, receipt := range progress.Receipts {
		observation, inspectErr := engine.inspectReceipt(ctx, receipt)
		if inspectErr != nil {
			return operation.Operation{}, inspectErr
		}
		if observation.Presence != provider.PresenceAbsent {
			return operation.Operation{}, fmt.Errorf("rollback cannot complete while %s %q is %s", receipt.Kind, receipt.LocalID, observation.Presence)
		}
		if receipt.Kind == ownership.KindInitProcess {
			engine.forgetProcessIdentity(receipt)
		}
		observations = append(observations, observation)
	}
	evidence, err := ownership.EvidenceDigest(struct {
		Failure      lifecycle.Failure              `json:"failure"`
		Observations []provider.ResourceObservation `json:"observations"`
	}{failure, observations})
	if err != nil {
		return operation.Operation{}, err
	}
	verification := lifecycle.Verification{Verified: true, Evidence: evidence, ObservedAt: engine.diagnosticNow()}
	switch target.Kind {
	case operation.TargetSandbox:
		verification.Kind = lifecycle.VerificationSandboxAbsent
		result, failErr := engine.lifecycle.FailSandboxCreateAfterRollback(ctx, lifecycle.SandboxCreateFailureRequest{
			OperationID: operationID, SandboxID: domain.SandboxID(target.ID), Fingerprint: progress.Operation.Fingerprint,
			Failure: failure, Verification: verification,
		})
		if result.Operation.State == operation.StateFailed {
			return result.Operation.Clone(), nil
		}
		return result.Operation.Clone(), failErr
	case operation.TargetContainer:
		verification.Kind = lifecycle.VerificationAttemptAbsent
		result, failErr := engine.lifecycle.FailContainerCreateAfterRollback(ctx, lifecycle.ContainerCreateFailureRequest{
			OperationID: operationID, ContainerID: domain.ContainerID(target.ID), Fingerprint: progress.Operation.Fingerprint,
			Failure: failure, Verification: verification,
		})
		if result.Operation.State == operation.StateFailed {
			return result.Operation.Clone(), nil
		}
		return result.Operation.Clone(), failErr
	default:
		return operation.Operation{}, fmt.Errorf("unsupported create rollback target %q", target.Kind)
	}
}

// failDefiniteCreateLocked closes one create after a failure is known terminal
// and every possible effect is covered by already-checkpointed owner inverses.
// It preserves the primary diagnostic while joining any persisted cleanup failure.
func (engine *Engine) failDefiniteCreateLocked(ctx context.Context, operationID operation.OperationID, reason operation.ReasonClass, primary error) error {
	if primary == nil {
		primary = errors.New("create failed before the next host effect")
	}
	failed, rollbackErr := engine.rollbackCreateLocked(ctx, operationID, lifecycle.Failure{Reason: reason, Message: primary.Error()})
	if rollbackErr != nil {
		return errors.Join(primary, rollbackErr)
	}
	if failed.State == operation.StateFailed {
		return lifecycle.NewOperationFailureError(failed)
	}
	return primary
}

// resumeCreateRollbackLocked reloads the immutable primary failure instead of
// inventing a new diagnostic when a client retry or daemon restart resumes cleanup.
func (engine *Engine) resumeCreateRollbackLocked(ctx context.Context, operationID operation.OperationID) (operation.Operation, error) {
	progress, err := engine.lifecycle.GetOperationProgress(ctx, operationID)
	if err != nil {
		return operation.Operation{}, err
	}
	if progress.RollbackCause == nil {
		return operation.Operation{}, errors.New("persisted rollback operation has no primary failure cause")
	}
	failure := lifecycle.Failure{Reason: progress.RollbackCause.Reason, Message: progress.RollbackCause.Message}
	return engine.rollbackCreateLocked(ctx, operationID, failure)
}

// checkpointRollback atomically persists sealed or advanced inverse progress before another cleanup action begins.
func (engine *Engine) checkpointRollback(ctx context.Context, progress lifecycle.OperationProgress, cause *rollback.Cause, records []rollback.Record, details any, condition *domain.Condition, measurement stageMeasurement) error {
	_, err := engine.lifecycle.CheckpointOperation(ctx, lifecycle.CheckpointRequest{
		OperationID: progress.Operation.ID, Target: progress.Operation.Target, Fingerprint: progress.Operation.Fingerprint,
		Stage: operation.StageRollback, RollbackCause: cause, OOMBaseline: progress.OOMBaseline,
		Rollback: cloneRollback(records), Receipts: cloneReceipts(progress.Receipts),
		Releases: cloneReleases(progress.Releases), OccurredAt: measurement.occurredAt, Duration: measurement.duration, Details: details,
		UpsertCondition: condition,
	})
	return err
}

// rollbackCondition exposes the durable primary cause and latest inverse failure without replacing the operation-scoped evidence.
func rollbackCondition(cause *rollback.Cause, failure *rollback.StepFailure) *domain.Condition {
	message := cause.Message
	if failure != nil {
		message = fmt.Sprintf("%s; rollback step %s: %v", message, failure.Descriptor.Name, failure.Err)
		if bounded, err := rollback.NewCause(cause.Reason, message); err == nil {
			message = bounded.Message
		}
	}
	condition := domain.Condition{Type: domain.ConditionCleanupPending, Reason: string(cause.Reason), Message: message}
	return &condition
}

// rollbackOwner requires every acquired receipt to share the exact operation owner used by the registry resolver.
func rollbackOwner(receipts []ownership.Receipt) (ownership.OwnerKey, error) {
	if len(receipts) == 0 {
		return ownership.OwnerKey{}, errors.New("rollback records require at least one acquisition receipt")
	}
	owner := receipts[0].Owner
	if err := owner.Validate(); err != nil {
		return ownership.OwnerKey{}, err
	}
	for _, receipt := range receipts[1:] {
		if receipt.Owner != owner {
			return ownership.OwnerKey{}, errors.New("rollback acquisition receipts do not share one owner")
		}
	}
	return owner, nil
}
