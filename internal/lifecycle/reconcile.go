package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/state"
)

// ReconcileConditionRequest records or clears one orthogonal recovery fact as
// its own replayable state operation after daemon-wide read-only discovery.
type ReconcileConditionRequest struct {
	OperationID operation.OperationID
	Target      operation.Target
	Condition   *domain.Condition
	Clear       string
	Evidence    string
	ObservedAt  time.Time
}

// ReconcileConditionResult returns the exact persisted resource projection and operation replay record.
type ReconcileConditionResult struct {
	Operation        operation.Operation
	Sandbox          *domain.Sandbox
	ContainerAttempt *domain.ContainerAttempt
}

// RequestFingerprint returns the immutable condition-decision fingerprint used
// to bind an internal recovery operation; transport identity and observation
// time are deliberately excluded so engine recovery can validate a retained ID.
func (request ReconcileConditionRequest) RequestFingerprint() (operation.RequestFingerprint, error) {
	semantic := reconcileConditionSemantic{
		Target: request.Target, Condition: cloneCondition(request.Condition),
		Clear: request.Clear, Evidence: request.Evidence,
	}
	fingerprint, err := operation.CanonicalRequestFingerprint(semantic)
	if err != nil {
		return operation.RequestFingerprint{}, fmt.Errorf("canonical reconciliation fingerprint: %w", err)
	}
	return fingerprint, nil
}

// reconcileConditionSemantic excludes transport metadata while binding a state operation to one exact condition decision.
type reconcileConditionSemantic struct {
	Target    operation.Target  `json:"target"`
	Condition *domain.Condition `json:"condition,omitempty"`
	Clear     string            `json:"clear,omitempty"`
	Evidence  string            `json:"evidence"`
}

// ReconcileCondition atomically updates one resource condition, appends its
// observation event, and stores an exact terminal response for restart replay.
func (c *Coordinator) ReconcileCondition(ctx context.Context, request ReconcileConditionRequest) (ReconcileConditionResult, error) {
	if err := validateReconcileConditionRequest(request); err != nil {
		return ReconcileConditionResult{}, err
	}
	semantic := reconcileConditionSemantic{Target: request.Target, Condition: cloneCondition(request.Condition), Clear: request.Clear, Evidence: request.Evidence}
	fingerprint, err := request.RequestFingerprint()
	if err != nil {
		return ReconcileConditionResult{}, err
	}
	binding, err := suppliedBinding(request.OperationID, operation.TypeState, request.Target, fingerprint)
	if err != nil {
		return ReconcileConditionResult{}, err
	}
	var result ReconcileConditionResult
	err = c.store.Update(ctx, func(tx state.Tx) error {
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result.Operation = resolved.Record.Operation.Clone()
			return decodeReconcileResponse(resolved.Record.Operation.Response, &result)
		}
		if resolved.Resolution != operation.ResolutionNew {
			return errors.New("new reconciliation state operation unexpectedly resolved as active")
		}
		var resources []operation.ResourceRef
		var generation, observed domain.Generation
		switch request.Target.Kind {
		case operation.TargetSandbox:
			record, getErr := tx.GetSandbox(domain.SandboxID(request.Target.ID))
			if getErr != nil {
				return getErr
			}
			if updateErr := updateSandboxCondition(&record.Sandbox, request); updateErr != nil {
				return updateErr
			}
			updated, putErr := tx.PutSandbox(record, record.Revision)
			if putErr != nil {
				return putErr
			}
			resources = []operation.ResourceRef{request.Target}
			generation, observed = updated.Sandbox.Status.Generation, updated.Sandbox.Status.ObservedGeneration
		case operation.TargetContainer:
			record, getErr := tx.GetContainerAttempt(domain.ContainerID(request.Target.ID))
			if getErr != nil {
				return getErr
			}
			if updateErr := updateContainerCondition(&record.ContainerAttempt, request); updateErr != nil {
				return updateErr
			}
			updated, putErr := tx.PutContainerAttempt(record, record.Revision)
			if putErr != nil {
				return putErr
			}
			resources = containerEventResources(updated.ContainerAttempt)
			generation = updated.ContainerAttempt.Container.Status.Generation
			observed = updated.ContainerAttempt.Container.Status.ObservedGeneration
		default:
			return domain.NewError(domain.CodeInvalidArgument, "target.kind", "reconciliation conditions require Sandbox or Container target")
		}
		operationRecord, putErr := c.putNewTerminalOperation(tx, binding, operation.ResultSucceeded, nil)
		if putErr != nil {
			return putErr
		}
		occurredAt := request.ObservedAt
		if occurredAt.IsZero() {
			occurredAt = c.clock.Now()
		}
		event, eventErr := appendOperationEvent(
			tx, operationRecord.Operation, operation.StageComplete, operation.ResultSucceeded,
			occurredAt, nil, generation, observed, semantic, resources...,
		)
		if eventErr != nil {
			return eventErr
		}
		if eventErr := observeEventTarget(tx, request.Target, event); eventErr != nil {
			return eventErr
		}
		response, responseErr := reconciliationResourceResponse(tx, request.Target)
		if responseErr != nil {
			return responseErr
		}
		encoded, encodeErr := encodeResponse(response)
		if encodeErr != nil {
			return encodeErr
		}
		operationRecord, putErr = finalizeOperationResponse(tx, operationRecord, encoded)
		if putErr != nil {
			return putErr
		}
		result = ReconcileConditionResult{
			Operation: operationRecord.Operation.Clone(), Sandbox: response.Sandbox,
			ContainerAttempt: response.ContainerAttempt,
		}
		return nil
	})
	return result, err
}

// reconcileResponse is the exact durable response shape for condition replay.
type reconcileResponse struct {
	Sandbox          *domain.Sandbox          `json:"sandbox,omitempty"`
	ContainerAttempt *domain.ContainerAttempt `json:"container_attempt,omitempty"`
}

// decodeReconcileResponse restores the exact terminal state projection without consulting mutable current state.
func decodeReconcileResponse(encoded json.RawMessage, result *ReconcileConditionResult) error {
	var response reconcileResponse
	if len(encoded) == 0 {
		return errors.New("reconciliation operation has no replay response")
	}
	if err := json.Unmarshal(encoded, &response); err != nil {
		return fmt.Errorf("decode reconciliation replay response: %w", err)
	}
	if response.Sandbox != nil {
		sandbox := response.Sandbox.Clone()
		result.Sandbox = &sandbox
	}
	if response.ContainerAttempt != nil {
		pair := response.ContainerAttempt.Clone()
		result.ContainerAttempt = &pair
	}
	return nil
}

// reconciliationResourceResponse reloads the post-event projection used as the immutable replay response.
func reconciliationResourceResponse(tx state.Tx, target operation.Target) (reconcileResponse, error) {
	switch target.Kind {
	case operation.TargetSandbox:
		record, err := tx.GetSandbox(domain.SandboxID(target.ID))
		if err != nil {
			return reconcileResponse{}, err
		}
		return reconcileResponse{Sandbox: sandboxPointer(record.Sandbox)}, nil
	case operation.TargetContainer:
		record, err := tx.GetContainerAttempt(domain.ContainerID(target.ID))
		if err != nil {
			return reconcileResponse{}, err
		}
		return reconcileResponse{ContainerAttempt: pairPointer(record.ContainerAttempt)}, nil
	default:
		return reconcileResponse{}, domain.NewError(domain.CodeInvalidArgument, "target.kind", "reconciliation response requires Sandbox or Container target")
	}
}

// validateReconcileConditionRequest enforces one mutation choice and explicit diagnostic evidence.
func validateReconcileConditionRequest(request ReconcileConditionRequest) error {
	if err := request.OperationID.Validate(); err != nil {
		return err
	}
	if err := request.Target.Validate(); err != nil {
		return err
	}
	if request.Target.Kind != operation.TargetSandbox && request.Target.Kind != operation.TargetContainer {
		return domain.NewError(domain.CodeInvalidArgument, "target.kind", "must be Sandbox or Container")
	}
	if (request.Condition == nil) == (request.Clear == "") {
		return domain.NewError(domain.CodeInvalidArgument, "condition", "exactly one condition upsert or clear action is required")
	}
	if request.Condition != nil {
		if err := request.Condition.Validate(); err != nil {
			return err
		}
	}
	if request.Evidence == "" {
		return domain.NewError(domain.CodeInvalidArgument, "evidence", "must describe the read-only reconciliation observation")
	}
	return nil
}

// cloneCondition copies the optional value so request fingerprinting never retains caller memory.
func cloneCondition(condition *domain.Condition) *domain.Condition {
	if condition == nil {
		return nil
	}
	clone := *condition
	return &clone
}

// updateSandboxCondition applies one already validated recovery decision to a cloned transaction record.
func updateSandboxCondition(sandbox *domain.Sandbox, request ReconcileConditionRequest) error {
	if request.Condition != nil {
		return sandbox.UpsertCondition(*request.Condition)
	}
	return sandbox.ClearCondition(request.Clear)
}

// updateContainerCondition applies one already validated recovery decision while preserving the Attempt projection.
func updateContainerCondition(pair *domain.ContainerAttempt, request ReconcileConditionRequest) error {
	if request.Condition != nil {
		return pair.UpsertCondition(*request.Condition)
	}
	return pair.ClearCondition(request.Clear)
}

// FailOperationRequest terminates a non-create active operation after recovery
// proves it cannot safely continue, retaining the current resource phase and a visible condition.
type FailOperationRequest struct {
	OperationID operation.OperationID
	Target      operation.Target
	Fingerprint operation.RequestFingerprint
	Failure     Failure
	Condition   domain.Condition
	ObservedAt  time.Time
}

// FailActiveOperation stores an exact failed response for a non-create operation instead of leaving restart recovery permanently active.
func (c *Coordinator) FailActiveOperation(ctx context.Context, request FailOperationRequest) (operation.Operation, error) {
	if err := request.OperationID.Validate(); err != nil {
		return operation.Operation{}, err
	}
	if err := request.Target.Validate(); err != nil {
		return operation.Operation{}, err
	}
	if err := request.Fingerprint.Validate(); err != nil {
		return operation.Operation{}, err
	}
	if err := request.Failure.Validate(); err != nil {
		return operation.Operation{}, err
	}
	if err := request.Condition.Validate(); err != nil {
		return operation.Operation{}, err
	}
	var result operation.Operation
	err := c.store.Update(ctx, func(tx state.Tx) error {
		record, getErr := tx.GetOperation(request.OperationID)
		if getErr != nil {
			return getErr
		}
		binding, bindingErr := suppliedBinding(request.OperationID, record.Operation.Type, request.Target, request.Fingerprint)
		if bindingErr != nil {
			return bindingErr
		}
		resolved, resolveErr := c.resolveOperation(tx, binding)
		if resolveErr != nil {
			return resolveErr
		}
		if resolved.Resolution == operation.ResolutionReplay {
			result = resolved.Record.Operation.Clone()
			return nil
		}
		if record.Operation.Type == operation.TypeCreate {
			return domain.NewError(domain.CodeFailedPrecondition, "operation.type", "create failure requires verified rollback-specific completion")
		}
		resources, generation, observed, updateErr := updateFailureResourceCondition(tx, request.Target, request.Condition)
		if updateErr != nil {
			return updateErr
		}
		failed, putErr := failOperation(tx, record, request.Failure.Reason, nil)
		if putErr != nil {
			return putErr
		}
		occurredAt := request.ObservedAt
		if occurredAt.IsZero() {
			occurredAt = c.clock.Now()
		}
		event, eventErr := appendOperationEvent(
			tx, failed.Operation, operation.StageComplete, operation.ResultFailed,
			occurredAt, nil, generation, observed, request.Failure, resources...,
		)
		if eventErr != nil {
			return eventErr
		}
		if eventErr := observeEventTarget(tx, request.Target, event); eventErr != nil {
			return eventErr
		}
		response, responseErr := failureResourceResponse(tx, request.Target)
		if responseErr != nil {
			return responseErr
		}
		failed, putErr = finalizeOperationResponse(tx, failed, response)
		if putErr != nil {
			return putErr
		}
		result = failed.Operation.Clone()
		return nil
	})
	return result, err
}

// updateFailureResourceCondition upserts the recovery condition and returns stable event identity facts.
func updateFailureResourceCondition(tx state.Tx, target operation.Target, condition domain.Condition) ([]operation.ResourceRef, domain.Generation, domain.Generation, error) {
	switch target.Kind {
	case operation.TargetSandbox:
		record, err := tx.GetSandbox(domain.SandboxID(target.ID))
		if err != nil {
			return nil, 0, 0, err
		}
		if err := record.Sandbox.UpsertCondition(condition); err != nil {
			return nil, 0, 0, err
		}
		updated, err := tx.PutSandbox(record, record.Revision)
		if err != nil {
			return nil, 0, 0, err
		}
		return []operation.ResourceRef{target}, updated.Sandbox.Status.Generation, updated.Sandbox.Status.ObservedGeneration, nil
	case operation.TargetContainer:
		record, err := tx.GetContainerAttempt(domain.ContainerID(target.ID))
		if err != nil {
			return nil, 0, 0, err
		}
		if err := record.ContainerAttempt.UpsertCondition(condition); err != nil {
			return nil, 0, 0, err
		}
		updated, err := tx.PutContainerAttempt(record, record.Revision)
		if err != nil {
			return nil, 0, 0, err
		}
		return containerEventResources(updated.ContainerAttempt), updated.ContainerAttempt.Container.Status.Generation,
			updated.ContainerAttempt.Container.Status.ObservedGeneration, nil
	default:
		return nil, 0, 0, fmt.Errorf("unsupported failure target kind %q", target.Kind)
	}
}

// failureResourceResponse encodes the post-event resource projection for exact failed-operation replay.
func failureResourceResponse(tx state.Tx, target operation.Target) (json.RawMessage, error) {
	switch target.Kind {
	case operation.TargetSandbox:
		record, err := tx.GetSandbox(domain.SandboxID(target.ID))
		if err != nil {
			return nil, err
		}
		return encodeResponse(sandboxResponse{Sandbox: sandboxPointer(record.Sandbox)})
	case operation.TargetContainer:
		record, err := tx.GetContainerAttempt(domain.ContainerID(target.ID))
		if err != nil {
			return nil, err
		}
		return encodeResponse(containerResponse{ContainerAttempt: pairPointer(record.ContainerAttempt)})
	default:
		return nil, fmt.Errorf("unsupported failure target kind %q", target.Kind)
	}
}
