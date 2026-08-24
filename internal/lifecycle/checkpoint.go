package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
	"mydocker/internal/rollback"
	"mydocker/internal/state"
)

// CheckpointRequest advances one active lifecycle operation after a provider stage has produced durable evidence.
// Callers use it after each acquisition, before the next host side effect, and before or after each rollback attempt.
type CheckpointRequest struct {
	OperationID            operation.OperationID
	Target                 operation.Target
	Fingerprint            operation.RequestFingerprint
	Stage                  operation.Stage
	RollbackCause          *rollback.Cause
	OOMBaseline            *provider.OOMSnapshot
	KillEscalationDeadline *time.Time
	Rollback               []rollback.Record
	Receipts               []ownership.Receipt
	Releases               []ownership.Release
	OccurredAt             time.Time
	Duration               *operation.Duration
	Details                any
	UpsertCondition        *domain.Condition
	ClearCondition         string
}

// CheckpointResult reports the durable operation and whether this call persisted a new stage event.
type CheckpointResult struct {
	Operation operation.Operation
	Changed   bool
}

// OperationProgress is the recovery-facing view of an operation and its persisted inverse progress.
// Engine reconcilers use this instead of reconstructing cleanup state from events or mutable resources.
type OperationProgress struct {
	Operation              operation.Operation
	RollbackCause          *rollback.Cause
	OOMBaseline            *provider.OOMSnapshot
	KillEscalationDeadline *time.Time
	Rollback               []rollback.Record
	Receipts               []ownership.Receipt
	Releases               []ownership.Release
}

// CheckpointOperation atomically stores provider progress, rollback descriptors, a stage event, and resource observation.
// An exact retry is a no-op; terminal operations are returned unchanged so recovery cannot reopen completed work.
func (c *Coordinator) CheckpointOperation(ctx context.Context, request CheckpointRequest) (CheckpointResult, error) {
	if err := validateCheckpointRequest(request); err != nil {
		return CheckpointResult{}, err
	}
	var result CheckpointResult
	err := c.store.Update(ctx, func(tx state.Tx) error {
		record, err := tx.GetOperation(request.OperationID)
		if err != nil {
			return err
		}
		binding := operation.Binding{
			ID: request.OperationID, Type: record.Operation.Type, Target: request.Target,
			Fingerprint: request.Fingerprint,
		}
		resolution, err := operation.Resolve(&record.Operation, binding)
		if err != nil {
			return err
		}
		if resolution == operation.ResolutionReplay {
			result.Operation = record.Operation.Clone()
			return nil
		}
		if record.Operation.Stage == request.Stage && reflect.DeepEqual(record.RollbackCause, request.RollbackCause) &&
			reflect.DeepEqual(record.OOMBaseline, request.OOMBaseline) &&
			timesEqual(record.KillEscalationDeadline, request.KillEscalationDeadline) &&
			reflect.DeepEqual(record.Rollback, request.Rollback) &&
			reflect.DeepEqual(record.Receipts, request.Receipts) &&
			reflect.DeepEqual(record.Releases, request.Releases) &&
			request.UpsertCondition == nil && request.ClearCondition == "" {
			result.Operation = record.Operation.Clone()
			return nil
		}
		if err := validateLinuxCheckpointAdvance(record, request.Stage); err != nil {
			return err
		}

		record.Operation.Stage = request.Stage
		record.RollbackCause = cloneRollbackCause(request.RollbackCause)
		record.OOMBaseline = cloneOOMBaseline(request.OOMBaseline)
		record.KillEscalationDeadline = cloneTime(request.KillEscalationDeadline)
		record.Rollback = cloneRollbackRecords(request.Rollback)
		record.Receipts = cloneOwnershipReceipts(request.Receipts)
		record.Releases = cloneOwnershipReleases(request.Releases)
		updated, err := tx.PutOperation(record, record.Revision)
		if err != nil {
			return err
		}
		if err := applyCheckpointCondition(tx, request.Target, request.UpsertCondition, request.ClearCondition); err != nil {
			return err
		}
		resources, generation, observed, err := eventResourceState(tx, request.Target)
		if err != nil {
			return err
		}
		occurredAt := request.OccurredAt
		if occurredAt.IsZero() {
			occurredAt = c.clock.Now()
		}
		event, err := appendOperationEvent(
			tx, updated.Operation, request.Stage, operation.ResultPending,
			occurredAt, request.Duration, generation, observed, request.Details, resources...,
		)
		if err != nil {
			return err
		}
		if err := observeEventTarget(tx, request.Target, event); err != nil {
			return err
		}
		result = CheckpointResult{Operation: updated.Operation.Clone(), Changed: true}
		return nil
	})
	return result, err
}

// validateLinuxCheckpointAdvance preserves the mandatory attach, gate-release, and process-observation acknowledgements for Start.
func validateLinuxCheckpointAdvance(record state.OperationRecord, requested operation.Stage) error {
	if record.HostProfile != state.HostProfileLinuxM2 || record.Operation.Type != operation.TypeStart || record.Operation.Stage == requested {
		return nil
	}
	currentRank := lifecycleStageRank(record.Operation.Stage)
	requestedRank := lifecycleStageRank(requested)
	attachRank := lifecycleStageRank(operation.StageAttachCgroup)
	releaseRank := lifecycleStageRank(operation.StageReleaseStartGate)
	observeRank := lifecycleStageRank(operation.StageObserveProcess)
	switch {
	case currentRank < attachRank && requestedRank > attachRank:
		return domain.NewError(domain.CodeFailedPrecondition, "operation.stage", "Linux M2 Start must checkpoint cgroup attachment before later stages")
	case currentRank == attachRank && requestedRank > attachRank && requested != operation.StageReleaseStartGate:
		return domain.NewError(domain.CodeFailedPrecondition, "operation.stage", "Linux M2 Start must checkpoint start-gate release immediately after cgroup attachment")
	case currentRank >= releaseRank && currentRank < observeRank && requestedRank > releaseRank && requested != operation.StageObserveProcess:
		return domain.NewError(domain.CodeFailedPrecondition, "operation.stage", "Linux M2 Start must checkpoint process observation immediately after gate release")
	}
	return nil
}

// GetOperationProgress returns an independent operation and rollback snapshot for provider reconciliation.
func (c *Coordinator) GetOperationProgress(ctx context.Context, id operation.OperationID) (OperationProgress, error) {
	if err := id.Validate(); err != nil {
		return OperationProgress{}, err
	}
	var progress OperationProgress
	err := c.store.View(ctx, func(reader state.Reader) error {
		record, err := reader.GetOperation(id)
		if err != nil {
			return err
		}
		progress = OperationProgress{
			Operation:              record.Operation.Clone(),
			RollbackCause:          cloneRollbackCause(record.RollbackCause),
			OOMBaseline:            cloneOOMBaseline(record.OOMBaseline),
			KillEscalationDeadline: cloneTime(record.KillEscalationDeadline),
			Rollback:               cloneRollbackRecords(record.Rollback),
			Receipts:               cloneOwnershipReceipts(record.Receipts),
			Releases:               cloneOwnershipReleases(record.Releases),
		}
		return nil
	})
	return progress, err
}

// validateCheckpointRequest rejects transport or terminal stages and malformed rollback progress before opening a transaction.
func validateCheckpointRequest(request CheckpointRequest) error {
	if err := request.OperationID.Validate(); err != nil {
		return err
	}
	if err := request.Target.Validate(); err != nil {
		return err
	}
	if err := request.Fingerprint.Validate(); err != nil {
		return err
	}
	if !request.Stage.Valid() {
		return fmt.Errorf("invalid provider checkpoint stage %q", request.Stage)
	}
	switch request.Stage {
	case operation.StageValidate, operation.StagePersistIntent, operation.StageComplete:
		return fmt.Errorf("stage %q is not a provider checkpoint", request.Stage)
	}
	if request.Duration != nil {
		if err := request.Duration.Validate(); err != nil {
			return err
		}
	}
	if request.UpsertCondition != nil && request.ClearCondition != "" {
		return errors.New("provider checkpoint cannot upsert and clear a condition in one request")
	}
	if request.UpsertCondition != nil {
		if err := request.UpsertCondition.Validate(); err != nil {
			return err
		}
	}
	if request.RollbackCause != nil {
		if err := request.RollbackCause.Validate(); err != nil {
			return err
		}
		if request.Stage != operation.StageRollback {
			return errors.New("rollback cause requires rollback stage")
		}
	}
	if request.OOMBaseline != nil {
		if err := request.OOMBaseline.Validate(); err != nil {
			return fmt.Errorf("OOM baseline: %w", err)
		}
		if request.Target.Kind != operation.TargetContainer ||
			(request.Stage != operation.StageAttachCgroup && request.Stage != operation.StageRollback) {
			return errors.New("OOM baseline requires a Container cgroup-attachment checkpoint")
		}
	}
	if request.KillEscalationDeadline != nil {
		if request.KillEscalationDeadline.IsZero() {
			return errors.New("Kill escalation deadline must be non-zero")
		}
		if request.Target.Kind != operation.TargetContainer ||
			(request.Stage != operation.StageSignalProcess && request.Stage != operation.StageObserveProcess) {
			return errors.New("Kill escalation deadline requires a Container signal or observation checkpoint")
		}
	}
	if request.Stage == operation.StageRollback && request.RollbackCause == nil {
		return errors.New("rollback stage requires the durable primary cause")
	}
	seen := make(map[string]struct{}, len(request.Rollback))
	for index, record := range request.Rollback {
		if err := record.Validate(); err != nil {
			return fmt.Errorf("rollback record %d: %w", index, err)
		}
		if _, exists := seen[record.Descriptor.Name]; exists {
			return fmt.Errorf("rollback record %d duplicates name %q", index, record.Descriptor.Name)
		}
		seen[record.Descriptor.Name] = struct{}{}
		if _, _, err := ownership.ReceiptFromDescriptor(record.Descriptor); err != nil {
			return fmt.Errorf("rollback record %d is not a bounded ownership inverse: %w", index, err)
		}
	}
	for index, receipt := range request.Receipts {
		if err := receipt.Validate(); err != nil {
			return fmt.Errorf("ownership receipt %d: %w", index, err)
		}
		if receipt.Owner.OperationID != request.OperationID || !receipt.Owner.Target.Equal(request.Target) {
			return fmt.Errorf("ownership receipt %d does not match checkpoint binding", index)
		}
	}
	seenReleases := make(map[string]struct{}, len(request.Releases))
	for index, release := range request.Releases {
		if err := release.Validate(); err != nil {
			return fmt.Errorf("ownership release %d: %w", index, err)
		}
		if release.CleanupOperationID != request.OperationID || !release.Resource.Owner.Target.Equal(request.Target) {
			return fmt.Errorf("ownership release %d does not match checkpoint binding", index)
		}
		key := string(release.Resource.Provider) + "\x00" + string(release.Resource.Kind)
		if _, exists := seenReleases[key]; exists {
			return fmt.Errorf("ownership release %d duplicates provider/kind", index)
		}
		seenReleases[key] = struct{}{}
	}
	if len(request.Releases) > 0 && request.Stage != operation.StageTeardown &&
		request.Stage != operation.StageTransition && request.Stage != operation.StagePersistState &&
		request.Stage != operation.StagePersistResult && request.Stage != operation.StageRollback {
		return fmt.Errorf("ownership releases require a teardown or later checkpoint")
	}
	return nil
}

// cloneRollbackCause returns a caller-owned primary failure diagnostic.
func cloneRollbackCause(cause *rollback.Cause) *rollback.Cause {
	if cause == nil {
		return nil
	}
	clone := cause.Clone()
	return &clone
}

// cloneOOMBaseline returns a caller-owned cgroup counter snapshot for later terminal attribution.
func cloneOOMBaseline(snapshot *provider.OOMSnapshot) *provider.OOMSnapshot {
	if snapshot == nil {
		return nil
	}
	clone := *snapshot
	return &clone
}

// cloneTime returns an independent wall-clock fact while preserving its exact serialized instant.
func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

// timesEqual compares optional wall-clock instants without depending on location pointers or monotonic components.
func timesEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

// applyCheckpointCondition updates one resource condition in the same transaction as provider progress and its event.
func applyCheckpointCondition(tx state.Tx, target operation.Target, upsert *domain.Condition, clear string) error {
	if upsert == nil && clear == "" {
		return nil
	}
	switch target.Kind {
	case operation.TargetSandbox:
		record, err := tx.GetSandbox(domain.SandboxID(target.ID))
		if err != nil {
			return err
		}
		if upsert != nil {
			err = record.Sandbox.UpsertCondition(*upsert)
		} else {
			err = record.Sandbox.ClearCondition(clear)
		}
		if err != nil {
			return err
		}
		_, err = tx.PutSandbox(record, record.Revision)
		return err
	case operation.TargetContainer:
		record, err := tx.GetContainerAttempt(domain.ContainerID(target.ID))
		if err != nil {
			return err
		}
		if upsert != nil {
			err = record.ContainerAttempt.UpsertCondition(*upsert)
		} else {
			err = record.ContainerAttempt.ClearCondition(clear)
		}
		if err != nil {
			return err
		}
		_, err = tx.PutContainerAttempt(record, record.Revision)
		return err
	default:
		return fmt.Errorf("provider checkpoint conditions require a Sandbox or Container target, got %q", target.Kind)
	}
}

// cloneRollbackRecords returns persistence progress without borrowing descriptor metadata from a provider stack.
func cloneRollbackRecords(records []rollback.Record) []rollback.Record {
	if records == nil {
		return nil
	}
	clones := make([]rollback.Record, len(records))
	for index, record := range records {
		clones[index] = record.Clone()
	}
	return clones
}

// cloneOwnershipReceipts returns provider evidence without borrowing mutable diagnostic attributes.
func cloneOwnershipReceipts(receipts []ownership.Receipt) []ownership.Receipt {
	if receipts == nil {
		return nil
	}
	clones := make([]ownership.Receipt, len(receipts))
	for index, receipt := range receipts {
		clones[index] = receipt.Clone()
	}
	return clones
}

// cloneOwnershipReleases returns absence evidence without borrowing nested provider diagnostics.
func cloneOwnershipReleases(releases []ownership.Release) []ownership.Release {
	if releases == nil {
		return nil
	}
	clones := make([]ownership.Release, len(releases))
	for index, release := range releases {
		clones[index] = release.Clone()
	}
	return clones
}

// IsCheckpointNotFound reports an operation lookup that cannot be resumed by a provider.
func IsCheckpointNotFound(err error) bool {
	return errors.Is(err, state.ErrNotFound)
}
