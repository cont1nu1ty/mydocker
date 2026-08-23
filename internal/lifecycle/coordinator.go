package lifecycle

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/state"
)

// resolvedOperation carries the deterministic retry decision and any existing CAS record.
type resolvedOperation struct {
	Resolution operation.Resolution
	Record     *state.OperationRecord
}

// sandboxResponse is the durable replay payload for Sandbox lifecycle operations.
type sandboxResponse struct {
	Sandbox *domain.Sandbox `json:"sandbox,omitempty"`
	Removed bool            `json:"removed,omitempty"`
}

// containerResponse is the durable replay payload for Container lifecycle operations.
type containerResponse struct {
	ContainerAttempt *domain.ContainerAttempt `json:"container_attempt,omitempty"`
	HostBinding      *ContainerHostBinding    `json:"host_binding,omitempty"`
	Removed          bool                     `json:"removed,omitempty"`
}

// killResponse is the durable replay payload for a planned or completed kill operation.
type killResponse struct {
	Plan             domain.KillPlan          `json:"plan"`
	Actionable       bool                     `json:"actionable"`
	ProcessIdentity  domain.ProcessIdentity   `json:"process_identity"`
	ContainerAttempt *domain.ContainerAttempt `json:"container_attempt,omitempty"`
}

// conflictResponse is the common replay payload for a deterministically rejected operation.
type conflictResponse struct {
	ActiveOperationID operation.OperationID `json:"active_operation_id"`
}

// sandboxCreateSemantic excludes transport metadata while retaining every create-time semantic field.
type sandboxCreateSemantic struct {
	SandboxID domain.SandboxID   `json:"sandbox_id"`
	Spec      domain.SandboxSpec `json:"spec"`
}

// sandboxActionSemantic is the canonical payload for stop and remove requests.
type sandboxActionSemantic struct {
	SandboxID domain.SandboxID `json:"sandbox_id"`
}

// containerCreateSemantic excludes operation identity while preserving argv and environment order.
type containerCreateSemantic struct {
	SandboxID   domain.SandboxID   `json:"sandbox_id"`
	ContainerID domain.ContainerID `json:"container_id"`
	AttemptID   domain.AttemptID   `json:"attempt_id"`
	Process     domain.ProcessSpec `json:"process"`
	ImageDigest string             `json:"image_digest,omitempty"`
	RootFS      string             `json:"rootfs,omitempty"`
}

// containerActionSemantic is the canonical payload for start and delete requests.
type containerActionSemantic struct {
	ContainerID domain.ContainerID `json:"container_id"`
}

// stoppedSemantic binds standalone terminal observation facts to one operation identity.
type stoppedSemantic struct {
	ContainerID domain.ContainerID `json:"container_id"`
	Outcome     domain.Outcome     `json:"outcome"`
	Conditions  []domain.Condition `json:"conditions,omitempty"`
}

// killSemantic binds a kill plan to one Container and explicit termination policy.
type killSemantic struct {
	ContainerID domain.ContainerID       `json:"container_id"`
	Policy      domain.TerminationPolicy `json:"policy"`
}

// bindingFor computes a canonical semantic fingerprint and builds the immutable operation tuple.
func bindingFor(id operation.OperationID, operationType operation.Type, target operation.Target, semantic any) (operation.Binding, error) {
	fingerprint, err := operation.CanonicalRequestFingerprint(semantic)
	if err != nil {
		return operation.Binding{}, fmt.Errorf("canonical request fingerprint: %w", err)
	}
	binding := operation.Binding{ID: id, Type: operationType, Target: target, Fingerprint: fingerprint}
	if err := binding.Validate(); err != nil {
		return operation.Binding{}, err
	}
	return binding, nil
}

// suppliedBinding validates a Confirm request against the original operation's immutable fingerprint.
func suppliedBinding(id operation.OperationID, operationType operation.Type, target operation.Target, fingerprint operation.RequestFingerprint) (operation.Binding, error) {
	binding := operation.Binding{ID: id, Type: operationType, Target: target, Fingerprint: fingerprint}
	if err := binding.Validate(); err != nil {
		return operation.Binding{}, err
	}
	return binding, nil
}

// resolveOperation applies ID replay/resume/mismatch rules and durably rejects another active operation.
func (c *Coordinator) resolveOperation(tx state.Tx, binding operation.Binding) (resolvedOperation, error) {
	record, err := tx.GetOperation(binding.ID)
	if err == nil {
		if record.HostProfile != c.profile {
			return resolvedOperation{}, fmt.Errorf("operation %q host profile %q does not match coordinator profile %q: %w",
				binding.ID, record.HostProfile, c.profile, state.ErrInvalidRecord)
		}
		resolution, resolveErr := operation.Resolve(&record.Operation, binding)
		if resolveErr != nil {
			return resolvedOperation{}, resolveErr
		}
		return resolvedOperation{Resolution: resolution, Record: &record}, nil
	}
	if !errors.Is(err, state.ErrNotFound) {
		return resolvedOperation{}, err
	}
	active, activeErr := tx.ActiveOperation(binding.Target)
	if activeErr == nil {
		if err := operation.CheckActiveConflict(&active.Operation, binding); err != nil {
			return c.persistOperationConflict(tx, binding, active.Operation)
		}
	} else if !errors.Is(activeErr, state.ErrNotFound) {
		return resolvedOperation{}, activeErr
	}
	return resolvedOperation{Resolution: operation.ResolutionNew}, nil
}

// persistOperationConflict records a terminal failure and event so rejection remains stable across retries.
func (c *Coordinator) persistOperationConflict(tx state.Tx, binding operation.Binding, active operation.Operation) (resolvedOperation, error) {
	response, err := encodeResponse(conflictResponse{ActiveOperationID: active.ID})
	if err != nil {
		return resolvedOperation{}, err
	}
	value := newActiveOperation(binding, nil)
	value.State = operation.StateFailed
	value.Stage = operation.StageComplete
	value.Result = operation.ResultFailed
	value.Reason = operation.ReasonConflict
	value.Response = response
	recordValue, err := state.NewOperationRecordForProfile(value, c.profile)
	if err != nil {
		return resolvedOperation{}, err
	}
	record, err := tx.PutOperation(recordValue, 0)
	if err != nil {
		return resolvedOperation{}, err
	}
	resources, generation, observed, err := eventResourceState(tx, binding.Target)
	resourceAbsent := errors.Is(err, state.ErrNotFound) && binding.Type == operation.TypeDelete
	if resourceAbsent {
		resources = []operation.Target{binding.Target}
		generation = 0
		observed = 0
		err = nil
	}
	if err != nil {
		return resolvedOperation{}, err
	}
	event := operation.Event{
		SchemaVersion: operation.SchemaVersion, OperationID: value.ID, Type: value.Type,
		Target: value.Target, Resources: resources, Stage: value.Stage, Result: value.Result,
		Reason: value.Reason, OccurredAt: c.clock.Now(),
		Generation: uint64(generation), ObservedGeneration: uint64(observed),
		Details: response,
	}
	storedEvent, err := tx.AppendEvent(event)
	if err != nil {
		return resolvedOperation{}, err
	}
	if err := observeEventTarget(tx, binding.Target, storedEvent); err != nil && !(resourceAbsent && errors.Is(err, state.ErrNotFound)) {
		return resolvedOperation{}, err
	}
	return resolvedOperation{Resolution: operation.ResolutionReplay, Record: &record}, nil
}

// eventResourceState resolves related resource identities and generation facts for an operation event.
func eventResourceState(tx state.Tx, target operation.Target) ([]operation.Target, domain.Generation, domain.Generation, error) {
	switch target.Kind {
	case operation.TargetSandbox:
		record, err := tx.GetSandbox(domain.SandboxID(target.ID))
		if err != nil {
			return nil, 0, 0, err
		}
		return []operation.Target{target}, record.Sandbox.Status.Generation, record.Sandbox.Status.ObservedGeneration, nil
	case operation.TargetContainer:
		record, err := tx.GetContainerAttempt(domain.ContainerID(target.ID))
		if err != nil {
			return nil, 0, 0, err
		}
		return containerEventResources(record.ContainerAttempt), record.ContainerAttempt.Container.Status.Generation, record.ContainerAttempt.Container.Status.ObservedGeneration, nil
	default:
		return []operation.Target{target}, 0, 0, nil
	}
}

// observeEventTarget projects a committed conflict event onto its retained primary resource.
func observeEventTarget(tx state.Tx, target operation.Target, event operation.Event) error {
	switch target.Kind {
	case operation.TargetSandbox:
		record, err := tx.GetSandbox(domain.SandboxID(target.ID))
		if err != nil {
			return err
		}
		_, err = observeSandbox(tx, record, event)
		return err
	case operation.TargetContainer:
		record, err := tx.GetContainerAttempt(domain.ContainerID(target.ID))
		if err != nil {
			return err
		}
		_, err = observeContainer(tx, record, event)
		return err
	default:
		return nil
	}
}

// newActiveOperation creates a resumable durable intent before any later provider action is attempted.
func newActiveOperation(binding operation.Binding, response json.RawMessage) operation.Operation {
	return operation.Operation{
		SchemaVersion: operation.SchemaVersion,
		ID:            binding.ID,
		Type:          binding.Type,
		Target:        binding.Target,
		Fingerprint:   binding.Fingerprint,
		State:         operation.StateRunning,
		Stage:         operation.StagePersistIntent,
		Result:        operation.ResultPending,
		Reason:        operation.ReasonNone,
		Response:      append(json.RawMessage(nil), response...),
	}
}

// terminalOperation converts a cloned durable record into an immutable replay result.
func terminalOperation(value operation.Operation, result operation.Result, response json.RawMessage) operation.Operation {
	value.State = operation.StateSucceeded
	value.Stage = operation.StageComplete
	value.Result = result
	value.Reason = operation.ReasonNone
	value.Response = append(json.RawMessage(nil), response...)
	return value
}

// putNewActiveOperation atomically stages a new active intent in the surrounding transaction.
func (c *Coordinator) putNewActiveOperation(tx state.Tx, binding operation.Binding, response json.RawMessage) (state.OperationRecord, error) {
	value := newActiveOperation(binding, response)
	record, err := state.NewOperationRecordForProfile(value, c.profile)
	if err != nil {
		return state.OperationRecord{}, err
	}
	return tx.PutOperation(record, 0)
}

// putNewTerminalOperation atomically persists a new no-op or single-transaction terminal result.
func (c *Coordinator) putNewTerminalOperation(tx state.Tx, binding operation.Binding, result operation.Result, response json.RawMessage) (state.OperationRecord, error) {
	value := terminalOperation(newActiveOperation(binding, nil), result, response)
	record, err := state.NewOperationRecordForProfile(value, c.profile)
	if err != nil {
		return state.OperationRecord{}, err
	}
	return tx.PutOperation(record, 0)
}

// completeOperation advances one active CAS record to a terminal replayable result.
func completeOperation(tx state.Tx, record state.OperationRecord, result operation.Result, response json.RawMessage) (state.OperationRecord, error) {
	value := terminalOperation(record.Operation.Clone(), result, response)
	record.Operation = value
	return tx.PutOperation(record, record.Revision)
}

// failOperation advances one active record to a replayable failed result after callers have persisted completed cleanup evidence.
func failOperation(tx state.Tx, record state.OperationRecord, reason operation.ReasonClass, response json.RawMessage) (state.OperationRecord, error) {
	if !reason.Valid() || reason == operation.ReasonNone {
		return state.OperationRecord{}, fmt.Errorf("invalid terminal failure reason %q", reason)
	}
	value := record.Operation.Clone()
	value.State = operation.StateFailed
	value.Stage = operation.StageComplete
	value.Result = operation.ResultFailed
	value.Reason = reason
	value.Response = append(json.RawMessage(nil), response...)
	record.Operation = value
	return tx.PutOperation(record, record.Revision)
}

// adoptOperationReceipts transfers every verified acquisition from an active create operation into its resource inventory.
// The returned operation and inventory use independent receipt clones and must be persisted in the same transaction.
func adoptOperationReceipts(record state.OperationRecord) (state.OperationRecord, []ownership.Receipt, error) {
	updated := record.Clone()
	inventory := make([]ownership.Receipt, len(updated.Receipts))
	for index, receipt := range updated.Receipts {
		adopted, err := receipt.Adopt()
		if err != nil {
			return state.OperationRecord{}, nil, fmt.Errorf("adopt ownership receipt %d: %w", index, err)
		}
		updated.Receipts[index] = adopted
		inventory[index] = adopted.Clone()
	}
	return updated, inventory, nil
}

// validateCompleteCleanupReleases requires a delete operation to prove provider-observed absence for every adopted inventory entry.
func validateCompleteCleanupReleases(record state.OperationRecord, inventory []ownership.Receipt) error {
	if len(record.Releases) != len(inventory) {
		return domain.NewError(domain.CodeFailedPrecondition, "host_resources", "cleanup releases must exactly cover the retained host inventory")
	}
	for _, receipt := range inventory {
		found := false
		for _, release := range record.Releases {
			if reflect.DeepEqual(release.Resource, receipt) {
				found = true
				break
			}
		}
		if !found {
			return domain.NewError(domain.CodeFailedPrecondition, "host_resources", "cleanup release does not match the retained host inventory")
		}
	}
	return nil
}

// requireLinuxStage keeps abstract M1 confirmation available while making Linux M2 provider checkpoints mandatory.
func requireLinuxStage(record state.OperationRecord, minimum operation.Stage) error {
	if record.HostProfile != state.HostProfileLinuxM2 {
		return nil
	}
	if lifecycleStageRank(record.Operation.Stage) < lifecycleStageRank(minimum) {
		return domain.NewError(domain.CodeFailedPrecondition, "operation.stage",
			fmt.Sprintf("Linux M2 confirmation requires stage %s or later", minimum))
	}
	return nil
}

// lifecycleStageRank mirrors the durable bounded operation order at the coordinator confirmation boundary.
func lifecycleStageRank(stage operation.Stage) int {
	switch stage {
	case operation.StageValidate:
		return 0
	case operation.StagePersistIntent:
		return 1
	case operation.StageCheckPreconditions:
		return 2
	case operation.StageHostPreflight:
		return 3
	case operation.StagePrepareCgroup:
		return 4
	case operation.StagePrepareStartGate:
		return 5
	case operation.StagePrepareStreams:
		return 6
	case operation.StageCreateProcess:
		return 7
	case operation.StagePrepareNamespaces:
		return 8
	case operation.StageJoinNamespaces:
		return 9
	case operation.StagePrepareRootfs:
		return 10
	case operation.StageAttachCgroup:
		return 11
	case operation.StageReleaseStartGate:
		return 12
	case operation.StageSignalProcess:
		return 13
	case operation.StageObserveProcess:
		return 14
	case operation.StageTeardown:
		return 15
	case operation.StageTransition:
		return 16
	case operation.StagePersistState:
		return 17
	case operation.StageRollback:
		return 18
	case operation.StagePersistResult:
		return 19
	case operation.StageComplete:
		return 20
	default:
		return -1
	}
}

// finalizeOperationResponse stores the exact caller-visible result after its completion event has been projected.
func finalizeOperationResponse(tx state.Tx, record state.OperationRecord, response json.RawMessage) (state.OperationRecord, error) {
	if !record.Operation.State.Terminal() {
		return state.OperationRecord{}, fmt.Errorf("operation %q response cannot be finalized before terminal state", record.Operation.ID)
	}
	record.Operation.Response = append(json.RawMessage(nil), response...)
	return tx.PutOperation(record, record.Revision)
}

// appendOperationEvent associates a stage fact with its already-persisted operation inside the same transaction.
func appendOperationEvent(
	tx state.Tx,
	value operation.Operation,
	stage operation.Stage,
	result operation.Result,
	occurredAt time.Time,
	duration operation.Duration,
	generation domain.Generation,
	observed domain.Generation,
	details any,
	resources ...operation.Target,
) (operation.Event, error) {
	encoded, err := json.Marshal(details)
	if err != nil {
		return operation.Event{}, fmt.Errorf("marshal lifecycle event details: %w", err)
	}
	if len(resources) == 0 {
		resources = []operation.Target{value.Target}
	}
	event := operation.Event{
		SchemaVersion:      operation.SchemaVersion,
		OperationID:        value.ID,
		Type:               value.Type,
		Target:             value.Target,
		Resources:          append([]operation.Target(nil), resources...),
		Stage:              stage,
		Result:             result,
		Reason:             value.Reason,
		OccurredAt:         occurredAt,
		Duration:           duration,
		Generation:         uint64(generation),
		ObservedGeneration: uint64(observed),
		Details:            encoded,
	}
	return tx.AppendEvent(event)
}

// lifecycleObservation converts one committed event into the domain query projection.
func lifecycleObservation(event operation.Event) domain.LifecycleObservation {
	return domain.LifecycleObservation{OperationID: string(event.OperationID), EventSequence: uint64(event.Sequence), Reason: string(event.Reason)}
}

// observeSandbox projects a committed event and CAS-writes the retained Sandbox in the same transaction.
func observeSandbox(tx state.Tx, record state.SandboxRecord, event operation.Event) (state.SandboxRecord, error) {
	if err := record.Sandbox.SetLastObservation(lifecycleObservation(event)); err != nil {
		return state.SandboxRecord{}, err
	}
	return tx.PutSandbox(record, record.Revision)
}

// observeContainer projects a committed event and CAS-writes the retained pair in the same transaction.
func observeContainer(tx state.Tx, record state.ContainerAttemptRecord, event operation.Event) (state.ContainerAttemptRecord, error) {
	if err := record.ContainerAttempt.SetLastObservation(lifecycleObservation(event)); err != nil {
		return state.ContainerAttemptRecord{}, err
	}
	return tx.PutContainerAttempt(record, record.Revision)
}

// containerEventResources returns the stable Sandbox, Container, and Attempt identities for one execution event.
func containerEventResources(pair domain.ContainerAttempt) []operation.Target {
	return []operation.Target{
		{Kind: operation.TargetSandbox, ID: string(pair.Container.SandboxID)},
		{Kind: operation.TargetContainer, ID: string(pair.Container.ID)},
		{Kind: operation.TargetAttempt, ID: string(pair.Attempt.ID)},
	}
}

// encodeResponse serializes a typed lifecycle result for exact terminal replay.
func encodeResponse(value any) (json.RawMessage, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode lifecycle response: %w", err)
	}
	return encoded, nil
}

// decodeSandboxResponse restores an immutable Sandbox replay payload.
func decodeSandboxResponse(encoded json.RawMessage) (sandboxResponse, error) {
	var response sandboxResponse
	if len(encoded) == 0 {
		return response, errors.New("sandbox operation has no replay response")
	}
	if err := json.Unmarshal(encoded, &response); err != nil {
		return sandboxResponse{}, fmt.Errorf("decode sandbox replay response: %w", err)
	}
	return response, nil
}

// decodeContainerResponse restores an immutable Container/Attempt replay payload.
func decodeContainerResponse(encoded json.RawMessage) (containerResponse, error) {
	var response containerResponse
	if len(encoded) == 0 {
		return response, errors.New("container operation has no replay response")
	}
	if err := json.Unmarshal(encoded, &response); err != nil {
		return containerResponse{}, fmt.Errorf("decode container replay response: %w", err)
	}
	if response.HostBinding != nil {
		if err := response.HostBinding.Validate(); err != nil {
			return containerResponse{}, fmt.Errorf("validate container replay host binding: %w", err)
		}
		if response.ContainerAttempt != nil &&
			(response.HostBinding.ContainerID != response.ContainerAttempt.Container.ID ||
				response.HostBinding.AttemptID != response.ContainerAttempt.Attempt.ID ||
				response.HostBinding.Generation != response.ContainerAttempt.Container.Status.Generation) {
			return containerResponse{}, errors.New("container replay host binding differs from retained Container Attempt")
		}
	}
	return response, nil
}

// decodeKillResponse restores a side-effect-free plan and any terminal aggregate replay payload.
func decodeKillResponse(encoded json.RawMessage) (killResponse, error) {
	var response killResponse
	if len(encoded) == 0 {
		return response, errors.New("kill operation has no replay response")
	}
	if err := json.Unmarshal(encoded, &response); err != nil {
		return killResponse{}, fmt.Errorf("decode kill replay response: %w", err)
	}
	return response, nil
}

// sandboxResultFromRecord translates a persisted operation response into a caller-owned result.
func sandboxResultFromRecord(resolution operation.Resolution, record state.OperationRecord) (SandboxResult, error) {
	response, err := decodeSandboxResponse(record.Operation.Response)
	if err != nil {
		return SandboxResult{}, err
	}
	return SandboxResult{Resolution: resolution, Operation: record.Operation.Clone(), Fingerprint: record.Operation.Fingerprint, Sandbox: response.Sandbox, Removed: response.Removed}.Clone(), nil
}

// containerResultFromRecord translates a persisted operation response into a caller-owned result.
func containerResultFromRecord(resolution operation.Resolution, record state.OperationRecord) (ContainerResult, error) {
	response, err := decodeContainerResponse(record.Operation.Response)
	if err != nil {
		return ContainerResult{}, err
	}
	return ContainerResult{
		Resolution: resolution, Operation: record.Operation.Clone(), Fingerprint: record.Operation.Fingerprint,
		ContainerAttempt: response.ContainerAttempt, HostBinding: response.HostBinding, Removed: response.Removed,
	}.Clone(), nil
}

// killResultFromRecord translates a persisted kill response into a caller-owned plan or terminal result.
func killResultFromRecord(resolution operation.Resolution, record state.OperationRecord) (KillResult, error) {
	response, err := decodeKillResponse(record.Operation.Response)
	if err != nil {
		return KillResult{}, err
	}
	return KillResult{Resolution: resolution, Operation: record.Operation.Clone(), Fingerprint: record.Operation.Fingerprint, Plan: response.Plan, Actionable: response.Actionable, ProcessIdentity: response.ProcessIdentity, ContainerAttempt: response.ContainerAttempt}.Clone(), nil
}

// finishSandboxResult maps a committed failed operation to its stable caller-visible error.
func finishSandboxResult(result SandboxResult, err error) (SandboxResult, error) {
	if err != nil {
		return SandboxResult{}, err
	}
	if failure := replayedOperationError(result.Operation); failure != nil {
		return result.Clone(), failure
	}
	return result.Clone(), nil
}

// finishContainerResult maps a committed failed operation to its stable caller-visible error.
func finishContainerResult(result ContainerResult, err error) (ContainerResult, error) {
	if err != nil {
		return ContainerResult{}, err
	}
	if failure := replayedOperationError(result.Operation); failure != nil {
		return result.Clone(), failure
	}
	return result.Clone(), nil
}

// finishKillResult maps a committed failed operation to its stable caller-visible error.
func finishKillResult(result KillResult, err error) (KillResult, error) {
	if err != nil {
		return KillResult{}, err
	}
	if failure := replayedOperationError(result.Operation); failure != nil {
		return result.Clone(), failure
	}
	return result.Clone(), nil
}

// replayedOperationError reconstructs the typed deterministic error stored in a terminal failure response.
func replayedOperationError(value operation.Operation) error {
	if value.State != operation.StateFailed {
		return nil
	}
	return NewOperationFailureError(value)
}

// ensureOperationMethod prevents a Confirm method from advancing a different lifecycle verb.
func ensureOperationMethod(record state.OperationRecord, operationType operation.Type, target operation.Target) error {
	if record.Operation.Type != operationType || !record.Operation.Target.Equal(target) {
		return fmt.Errorf("%w: operation %q is %s on %s/%s", ErrOperationType, record.Operation.ID, record.Operation.Type, record.Operation.Target.Kind, record.Operation.Target.ID)
	}
	return nil
}
