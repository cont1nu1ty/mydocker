package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"mydocker/internal/lifecycle"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
	"mydocker/internal/rollback"
)

// checkpointProgress advances an active operation without changing its acquisition journal or an already persisted Kill deadline.
func (engine *Engine) checkpointProgress(
	ctx context.Context,
	operationID operation.OperationID,
	target operation.Target,
	fingerprint operation.RequestFingerprint,
	stage operation.Stage,
	details any,
) (lifecycle.OperationProgress, error) {
	progress, err := engine.lifecycle.GetOperationProgress(ctx, operationID)
	if err != nil {
		return lifecycle.OperationProgress{}, err
	}
	_, err = engine.lifecycle.CheckpointOperation(ctx, lifecycle.CheckpointRequest{
		OperationID: operationID, Target: target, Fingerprint: fingerprint, Stage: stage,
		KillEscalationDeadline: progress.KillEscalationDeadline,
		Rollback:               progress.Rollback, Receipts: progress.Receipts, Releases: progress.Releases,
		OccurredAt: engine.clock.Now(), Details: details,
	})
	if err != nil {
		return lifecycle.OperationProgress{}, err
	}
	return engine.lifecycle.GetOperationProgress(ctx, operationID)
}

// checkpointClearCondition removes one stale recovery condition in the same
// durable operation stage before the caller confirms a newly verified fact.
func (engine *Engine) checkpointClearCondition(
	ctx context.Context,
	progress lifecycle.OperationProgress,
	target operation.Target,
	fingerprint operation.RequestFingerprint,
	conditionType string,
	details any,
) error {
	_, err := engine.lifecycle.CheckpointOperation(ctx, lifecycle.CheckpointRequest{
		OperationID: progress.Operation.ID, Target: target, Fingerprint: fingerprint,
		Stage: progress.Operation.Stage, RollbackCause: progress.RollbackCause,
		OOMBaseline: progress.OOMBaseline, KillEscalationDeadline: progress.KillEscalationDeadline,
		Rollback: progress.Rollback, Receipts: progress.Receipts, Releases: progress.Releases,
		OccurredAt: engine.clock.Now(), Details: details, ClearCondition: conditionType,
	})
	return err
}

// checkpointKillSignal atomically binds the first verified delivery to one immutable absolute escalation deadline before the engine waits or returns to the caller.
func (engine *Engine) checkpointKillSignal(
	ctx context.Context,
	operationID operation.OperationID,
	target operation.Target,
	fingerprint operation.RequestFingerprint,
	delivery provider.SignalObservation,
	grace time.Duration,
) (lifecycle.OperationProgress, error) {
	if err := delivery.Validate(); err != nil {
		return lifecycle.OperationProgress{}, err
	}
	if grace < 0 {
		return lifecycle.OperationProgress{}, errors.New("Kill grace period must not be negative")
	}
	deadline := delivery.DeliveredAt.Add(grace).Round(0).UTC()
	progress, err := engine.lifecycle.GetOperationProgress(ctx, operationID)
	if err != nil {
		return lifecycle.OperationProgress{}, err
	}
	if progress.KillEscalationDeadline != nil && !progress.KillEscalationDeadline.Equal(deadline) {
		return lifecycle.OperationProgress{}, errors.New("Kill escalation deadline changed during exact retry")
	}
	_, err = engine.lifecycle.CheckpointOperation(ctx, lifecycle.CheckpointRequest{
		OperationID: operationID, Target: target, Fingerprint: fingerprint,
		Stage: operation.StageSignalProcess, KillEscalationDeadline: &deadline,
		Rollback: progress.Rollback, Receipts: progress.Receipts, Releases: progress.Releases,
		OccurredAt: engine.clock.Now(), Details: delivery,
	})
	if err != nil {
		return lifecycle.OperationProgress{}, err
	}
	return engine.lifecycle.GetOperationProgress(ctx, operationID)
}

// checkpointReceipt validates one provider acquisition, arms its bounded inverse when actionable,
// and atomically persists that single journal append before any later host side effect.
func (engine *Engine) checkpointReceipt(
	ctx context.Context,
	operationID operation.OperationID,
	target operation.Target,
	fingerprint operation.RequestFingerprint,
	stage operation.Stage,
	receipt ownership.Receipt,
) (lifecycle.OperationProgress, error) {
	if err := receipt.Validate(); err != nil {
		return lifecycle.OperationProgress{}, fmt.Errorf("provider receipt: %w", err)
	}
	if receipt.Adopted || receipt.Owner.OperationID != operationID || !receipt.Owner.Target.Equal(target) {
		return lifecycle.OperationProgress{}, errors.New("provider returned a receipt outside the active acquisition owner")
	}
	progress, err := engine.lifecycle.GetOperationProgress(ctx, operationID)
	if err != nil {
		return lifecycle.OperationProgress{}, err
	}
	if len(progress.Receipts) > 0 {
		last := progress.Receipts[len(progress.Receipts)-1]
		if last.Provider == receipt.Provider && last.Kind == receipt.Kind && last.LocalID == receipt.LocalID {
			if last.EvidenceSHA256 != receipt.EvidenceSHA256 || last.Owner != receipt.Owner {
				return lifecycle.OperationProgress{}, errors.New("provider receipt identity changed during exact retry")
			}
			return progress, nil
		}
	}
	progress.Receipts = append(cloneReceipts(progress.Receipts), receipt.Clone())
	if action, actionable := inverseAction(receipt); actionable {
		descriptor, descriptorErr := ownership.InverseDescriptor(receipt, action)
		if descriptorErr != nil {
			return lifecycle.OperationProgress{}, descriptorErr
		}
		progress.Rollback = append(cloneRollback(progress.Rollback), rollback.Record{Descriptor: descriptor})
	}
	_, err = engine.lifecycle.CheckpointOperation(ctx, lifecycle.CheckpointRequest{
		OperationID: operationID, Target: target, Fingerprint: fingerprint, Stage: stage,
		Rollback: progress.Rollback, Receipts: progress.Receipts, Releases: progress.Releases,
		OccurredAt: engine.clock.Now(), Details: receipt,
	})
	if err != nil {
		return lifecycle.OperationProgress{}, err
	}
	return engine.lifecycle.GetOperationProgress(ctx, operationID)
}

// checkpointOOMBaseline persists the gated Attempt's owner-scoped memory
// counters before the workload can start, making later OOM attribution restart-safe.
func (engine *Engine) checkpointOOMBaseline(
	ctx context.Context,
	operationID operation.OperationID,
	target operation.Target,
	fingerprint operation.RequestFingerprint,
	baseline provider.OOMSnapshot,
) (lifecycle.OperationProgress, error) {
	if err := baseline.Validate(); err != nil {
		return lifecycle.OperationProgress{}, err
	}
	progress, err := engine.lifecycle.GetOperationProgress(ctx, operationID)
	if err != nil {
		return lifecycle.OperationProgress{}, err
	}
	if progress.OOMBaseline != nil {
		if *progress.OOMBaseline != baseline {
			return lifecycle.OperationProgress{}, errors.New("Attempt OOM baseline changed during exact retry")
		}
		return progress, nil
	}
	_, err = engine.lifecycle.CheckpointOperation(ctx, lifecycle.CheckpointRequest{
		OperationID: operationID, Target: target, Fingerprint: fingerprint, Stage: operation.StageAttachCgroup,
		OOMBaseline: &baseline, Rollback: progress.Rollback, Receipts: progress.Receipts, Releases: progress.Releases,
		OccurredAt: engine.clock.Now(), Details: baseline,
	})
	if err != nil {
		return lifecycle.OperationProgress{}, err
	}
	return engine.lifecycle.GetOperationProgress(ctx, operationID)
}

// inverseAction maps only independently actionable resources to their bounded idempotent cleanup operation.
func inverseAction(receipt ownership.Receipt) (ownership.Action, bool) {
	switch receipt.Kind {
	case ownership.KindSandboxCgroup, ownership.KindKeeperCgroup, ownership.KindAttemptCgroup:
		return ownership.ActionRemoveCgroup, true
	case ownership.KindKeeperProcess, ownership.KindInitProcess:
		return ownership.ActionStopProcess, true
	case ownership.KindRootfsMount:
		return ownership.ActionUnmountRoot, true
	case ownership.KindStartGate:
		return ownership.ActionCloseGate, true
	case ownership.KindStreams:
		return ownership.ActionCloseStreams, true
	default:
		return "", false
	}
}

// cloneRollback protects descriptor metadata when a progress snapshot is extended.
func cloneRollback(records []rollback.Record) []rollback.Record {
	clones := make([]rollback.Record, len(records))
	for index, record := range records {
		clones[index] = record.Clone()
	}
	return clones
}

// checkpointRelease atomically appends one provider-verified absence proof
// after its teardown effect and before the next owned resource is removed.
func (engine *Engine) checkpointRelease(
	ctx context.Context,
	operationID operation.OperationID,
	target operation.Target,
	fingerprint operation.RequestFingerprint,
	release ownership.Release,
) (lifecycle.OperationProgress, error) {
	if err := release.Validate(); err != nil {
		return lifecycle.OperationProgress{}, err
	}
	if release.CleanupOperationID != operationID || !release.Resource.Owner.Target.Equal(target) {
		return lifecycle.OperationProgress{}, errors.New("cleanup release does not match active delete binding")
	}
	progress, err := engine.lifecycle.GetOperationProgress(ctx, operationID)
	if err != nil {
		return lifecycle.OperationProgress{}, err
	}
	for _, existing := range progress.Releases {
		if existing.Resource.Provider == release.Resource.Provider && existing.Resource.Kind == release.Resource.Kind {
			if existing.EvidenceSHA256 != release.EvidenceSHA256 || existing.Resource.LocalID != release.Resource.LocalID {
				return lifecycle.OperationProgress{}, errors.New("cleanup evidence changed during exact retry")
			}
			return progress, nil
		}
	}
	progress.Releases = append(cloneReleases(progress.Releases), release.Clone())
	_, err = engine.lifecycle.CheckpointOperation(ctx, lifecycle.CheckpointRequest{
		OperationID: operationID, Target: target, Fingerprint: fingerprint, Stage: operation.StageTeardown,
		Rollback: progress.Rollback, Receipts: progress.Receipts, Releases: progress.Releases,
		OccurredAt: engine.clock.Now(), Details: release,
	})
	if err != nil {
		return lifecycle.OperationProgress{}, err
	}
	return engine.lifecycle.GetOperationProgress(ctx, operationID)
}

// cloneReleases protects nested receipt attributes while extending teardown progress.
func cloneReleases(releases []ownership.Release) []ownership.Release {
	clones := make([]ownership.Release, len(releases))
	for index, release := range releases {
		clones[index] = release.Clone()
	}
	return clones
}
