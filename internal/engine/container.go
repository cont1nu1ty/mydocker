package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/lifecycle"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
)

const escalationObservationTimeout = 5 * time.Second

// CreateContainer persists a one-to-one Container/Attempt, checkpoints every
// canonical host acquisition, confirms init membership, and only then marks it Created.
func (engine *Engine) CreateContainer(ctx context.Context, request lifecycle.ContainerCreateRequest) (lifecycle.ContainerResult, error) {
	operationStartedAt := engine.beginMeasurement()
	target := operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}
	release := engine.targetLocks.lock(target)
	defer release()
	result, err := engine.lifecycle.BeginContainerCreate(ctx, request)
	if err != nil || result.Operation.State.Terminal() {
		return result, err
	}
	if result.Operation.Stage == operation.StageRollback {
		if _, err := engine.resumeCreateRollbackLocked(ctx, request.OperationID); err != nil {
			return result, err
		}
		return engine.lifecycle.BeginContainerCreate(ctx, request)
	}
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
	pair, err := engine.lifecycle.GetContainer(ctx, request.ContainerID)
	if err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonInternal, err)
	}
	retainedSandbox, err := engine.lifecycle.GetSandbox(ctx, pair.Container.SandboxID)
	if err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonPrecondition, err)
	}
	owner, err := ownership.NewOwnerKey(request.OperationID, target, pair.Container.Status.Generation)
	if err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonInternal, err)
	}
	sandboxInventory, err := engine.sandboxInventory(ctx, pair.Container.SandboxID)
	if err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonPrecondition, err)
	}
	sandboxNamespaces, err := sandboxNamespaces(sandboxInventory)
	if err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonPrecondition, err)
	}
	for {
		progress, err = engine.lifecycle.GetOperationProgress(ctx, request.OperationID)
		if err != nil {
			return result, err
		}
		if len(progress.Receipts) >= 7 {
			break
		}
		acquireStartedAt := engine.beginMeasurement()
		receipt, stage, acquireErr := engine.acquireContainerReceipt(ctx, owner, pair, sandboxNamespaces, retainedSandbox.Spec.DNS, progress.Receipts)
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
	cgroup := progress.Receipts[0]
	process := progress.Receipts[3]
	if err := engine.registerProcessIdentity(process); err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonInternal, err)
	}
	attachmentStartedAt := engine.beginMeasurement()
	attachment, err := engine.providers.Cgroup.AttachAttemptProcess(ctx, provider.AttachProcessRequest{Owner: owner, Cgroup: cgroup, Process: process})
	attachmentMeasurement := engine.finishMeasurement(attachmentStartedAt)
	if err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonInternal, err)
	}
	if err := attachment.ValidateFor(provider.AttachProcessRequest{Owner: owner, Cgroup: cgroup, Process: process}); err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonInternal, err)
	}
	if _, err = engine.checkpointProgress(ctx, request.OperationID, target, result.Fingerprint, operation.StageAttachCgroup, attachment, attachmentMeasurement); err != nil {
		return result, err
	}
	progress, err = engine.lifecycle.GetOperationProgress(ctx, request.OperationID)
	if err != nil {
		return result, err
	}
	if progress.OOMBaseline == nil {
		baselineStartedAt := engine.beginMeasurement()
		baseline, snapshotErr := engine.providers.Cgroup.SnapshotAttemptOOM(ctx, provider.OwnedReceiptRequest{Owner: owner, Receipt: cgroup})
		baselineMeasurement := engine.finishMeasurement(baselineStartedAt)
		if snapshotErr != nil {
			return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonInternal, snapshotErr)
		}
		if _, err = engine.checkpointOOMBaseline(ctx, request.OperationID, target, result.Fingerprint, baseline, baselineMeasurement); err != nil {
			return result, err
		}
	}
	prepared, err := engine.providers.Supervisor.ObserveAttempt(ctx, provider.OwnedReceiptRequest{Owner: owner, Receipt: process})
	if err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonInternal, err)
	}
	if err := prepared.Validate(); err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonInternal, err)
	}
	if !prepared.Prepared {
		prepareErr := errors.New("init wrapper is not verified prepared behind its start gate")
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonInternal, prepareErr)
	}
	streams := streamReferences(progress.Receipts[2])
	identity := domain.ProcessIdentity{Verified: true, Handle: processIdentityHandle(process), Evidence: process.EvidenceSHA256}
	evidence, err := ownership.EvidenceDigest(struct {
		Receipts   []ownership.Receipt            `json:"receipts"`
		Attachment provider.AttachmentObservation `json:"attachment"`
		Prepared   AttemptObservation             `json:"prepared"`
	}{progress.Receipts, attachment, prepared})
	if err != nil {
		return result, engine.failDefiniteCreateLocked(ctx, request.OperationID, operation.ReasonInternal, err)
	}
	completion := engine.finishOperationMeasurement(operationStartedAt, result.Resolution)
	return engine.lifecycle.ConfirmContainerCreate(ctx, lifecycle.ContainerConfirmRequest{
		OperationID: request.OperationID, ContainerID: request.ContainerID, Fingerprint: result.Fingerprint,
		Verification: lifecycle.Verification{
			Kind: lifecycle.VerificationAttemptCreated, Verified: true, Evidence: evidence,
			ObservedAt: completion.occurredAt, Duration: completion.duration, ProcessIdentity: &identity, Streams: streams,
		},
	})
}

// DeleteContainer removes the complete retained Attempt inventory in dependency order
// and deletes metadata only after every exact absence receipt is durable.
func (engine *Engine) DeleteContainer(ctx context.Context, request lifecycle.ContainerActionRequest) (lifecycle.ContainerResult, error) {
	operationStartedAt := engine.beginMeasurement()
	target := operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}
	releaseTarget := engine.targetLocks.lock(target)
	defer releaseTarget()
	result, err := engine.lifecycle.BeginContainerDelete(ctx, request)
	if err != nil || result.Operation.State.Terminal() {
		return result, err
	}
	inventory, inventoryErr := engine.containerInventory(ctx, request.ContainerID)
	if inventoryErr != nil {
		if lifecycle.IsCheckpointNotFound(inventoryErr) {
			completion := engine.finishOperationMeasurement(operationStartedAt, result.Resolution)
			return engine.lifecycle.ConfirmContainerDelete(ctx, lifecycle.ContainerConfirmRequest{
				OperationID: request.OperationID, ContainerID: request.ContainerID, Fingerprint: result.Fingerprint,
				Verification: lifecycle.Verification{Kind: lifecycle.VerificationAttemptAbsent, Verified: true, Evidence: "metadata_absent_without_owned_receipts",
					ObservedAt: completion.occurredAt, Duration: completion.duration},
			})
		}
		return result, inventoryErr
	}
	order := []ownership.Kind{
		ownership.KindRootfsMount,
		ownership.KindInitProcess,
		ownership.KindMountNamespace, ownership.KindPIDNamespace,
		ownership.KindStreams, ownership.KindStartGate,
		ownership.KindAttemptCgroup,
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
		if kind == ownership.KindInitProcess {
			engine.forgetProcessIdentity(receipt)
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
	return engine.lifecycle.ConfirmContainerDelete(ctx, lifecycle.ContainerConfirmRequest{
		OperationID: request.OperationID, ContainerID: request.ContainerID, Fingerprint: result.Fingerprint,
		Verification: lifecycle.Verification{Kind: lifecycle.VerificationAttemptAbsent, Verified: true, Evidence: evidence,
			ObservedAt: completion.occurredAt, Duration: completion.duration},
	})
}

// KillContainer executes a side-effect-free lifecycle plan through the verified wrapper provider, persists a delivery-anchored absolute grace deadline, and records only a supervisor terminal fact.
func (engine *Engine) KillContainer(ctx context.Context, request lifecycle.KillRequest) (lifecycle.KillResult, error) {
	operationStartedAt := engine.beginMeasurement()
	target := operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}
	releaseTarget := engine.targetLocks.lock(target)
	defer releaseTarget()
	result, err := engine.lifecycle.PlanKill(ctx, request)
	if err != nil || result.Operation.State.Terminal() || !result.Actionable {
		return result, err
	}
	inventory, err := engine.containerInventory(ctx, request.ContainerID)
	if err != nil {
		return result, err
	}
	process, err := receiptByKind(inventory, ownership.KindInitProcess)
	if err != nil {
		return result, err
	}
	cgroup, err := receiptByKind(inventory, ownership.KindAttemptCgroup)
	if err != nil {
		return result, err
	}
	progress, err := engine.lifecycle.GetOperationProgress(ctx, request.OperationID)
	if err != nil {
		return result, err
	}
	var observation AttemptObservation
	var observationStartedAt time.Time
	terminalAfterSignalError := false
	switch progress.Operation.Stage {
	case operation.StagePersistIntent:
		initialSignal, signalErr := providerSignal(result.Plan.Signal)
		if signalErr != nil {
			return result, signalErr
		}
		signalStartedAt := engine.beginMeasurement()
		delivery, signalErr := engine.providers.Isolation.SignalVerified(ctx, provider.SignalRequest{
			Owner: process.Owner, Process: process, ActionOperationID: request.OperationID,
			Step: provider.SignalStepInitial, Signal: initialSignal,
		})
		signalMeasurement := engine.finishMeasurement(signalStartedAt)
		if signalErr != nil {
			observationStartedAt = engine.beginMeasurement()
			observation, err = engine.observeAfterSignalError(ctx, process, signalErr)
			if err != nil {
				return result, err
			}
			terminalAfterSignalError = true
			break
		}
		progress, err = engine.checkpointKillSignal(ctx, request.OperationID, target, result.Fingerprint, delivery, result.Plan.GracePeriod, signalMeasurement)
		if err != nil {
			return result, err
		}
		observationStartedAt = engine.beginMeasurement()
	case operation.StageSignalProcess:
		// A confirmed initial delivery is never sent again; an unconfirmed response-loss retry uses the provider's durable action key above.
		observationStartedAt = engine.beginMeasurement()
	case operation.StageObserveProcess:
		observationStartedAt = engine.beginMeasurement()
		observation, err = engine.providers.Supervisor.ObserveAttempt(ctx, provider.OwnedReceiptRequest{Owner: process.Owner, Receipt: process})
		if err != nil {
			return result, err
		}
		if err = observation.Validate(); err != nil {
			return result, err
		}
		if observation.Terminal {
			terminalAfterSignalError = true
		} else if progress.KillEscalationDeadline == nil {
			return result, errors.New("observed Kill checkpoint has neither a terminal result nor a durable escalation deadline")
		}
	default:
		return result, fmt.Errorf("Kill operation cannot resume from stage %q", progress.Operation.Stage)
	}
	if !terminalAfterSignalError {
		if progress.KillEscalationDeadline == nil {
			return result, errors.New("signaled Kill operation is missing its durable escalation deadline")
		}
		observation, err = engine.waitForTerminalUntil(ctx, process, *progress.KillEscalationDeadline)
		if err != nil {
			return result, err
		}
	}
	if !observation.Terminal {
		escalation, signalErr := providerSignal(result.Plan.EscalationSignal)
		if signalErr != nil {
			return result, signalErr
		}
		if _, signalErr = engine.providers.Isolation.SignalVerified(ctx, provider.SignalRequest{
			Owner: process.Owner, Process: process, ActionOperationID: request.OperationID,
			Step: provider.SignalStepEscalation, Signal: escalation,
		}); signalErr != nil {
			observation, err = engine.observeAfterSignalError(ctx, process, signalErr)
			if err != nil {
				return result, err
			}
		} else {
			observation, err = engine.waitForConfirmedTerminal(ctx, process, escalationObservationTimeout)
			if err != nil {
				return result, err
			}
		}
	}
	observation, err = engine.attributeTerminalOOM(ctx, cgroup, observation)
	if err != nil {
		return result, err
	}
	observationMeasurement := engine.finishMeasurement(observationStartedAt)
	if _, err = engine.checkpointProgress(ctx, request.OperationID, target, result.Fingerprint, operation.StageObserveProcess, observation, observationMeasurement); err != nil {
		return result, err
	}
	conditions := terminalConditions(observation.Outcome)
	completion := engine.finishOperationMeasurement(operationStartedAt, result.Resolution)
	return engine.lifecycle.RecordKillStopped(ctx, lifecycle.KillStoppedRequest{
		OperationID: request.OperationID, ContainerID: request.ContainerID, Fingerprint: result.Fingerprint,
		Outcome: observation.Outcome, Conditions: conditions,
		Verification: lifecycle.Verification{Kind: lifecycle.VerificationAttemptStopped, Verified: true, Evidence: observation.Evidence,
			ObservedAt: completion.occurredAt, Duration: completion.duration},
	})
}

// observeAfterSignalError resolves the natural-exit race by accepting only a
// subsequently verified terminal fact; a still-live or unavailable wrapper preserves the original signal error.
func (engine *Engine) observeAfterSignalError(ctx context.Context, process ownership.Receipt, signalErr error) (AttemptObservation, error) {
	observation, observeErr := engine.providers.Supervisor.ObserveAttempt(ctx, provider.OwnedReceiptRequest{Owner: process.Owner, Receipt: process})
	if observeErr != nil {
		return AttemptObservation{}, errors.Join(signalErr, observeErr)
	}
	if err := observation.Validate(); err != nil {
		return AttemptObservation{}, errors.Join(signalErr, err)
	}
	if !observation.Terminal {
		return AttemptObservation{}, signalErr
	}
	return observation, nil
}

// waitForConfirmedTerminal allows the wrapper to reap and durably publish an escalated child exit before the operation is failed as unavailable.
func (engine *Engine) waitForConfirmedTerminal(ctx context.Context, process ownership.Receipt, maximum time.Duration) (AttemptObservation, error) {
	if maximum <= 0 {
		return AttemptObservation{}, errors.New("terminal observation timeout must be positive")
	}
	waitContext, cancel := context.WithTimeout(ctx, maximum)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		observation, err := engine.providers.Supervisor.ObserveAttempt(waitContext, provider.OwnedReceiptRequest{Owner: process.Owner, Receipt: process})
		if err != nil {
			return AttemptObservation{}, err
		}
		if err := observation.Validate(); err != nil {
			return AttemptObservation{}, err
		}
		if observation.Terminal {
			return observation, nil
		}
		select {
		case <-waitContext.Done():
			return AttemptObservation{}, fmt.Errorf("wait for terminal outcome after escalation: %w", waitContext.Err())
		case <-ticker.C:
		}
	}
}

// RecordTerminal observes a naturally exited wrapper and persists the outcome through the two-stage Linux lifecycle boundary.
func (engine *Engine) RecordTerminal(ctx context.Context, operationID operation.OperationID, containerID domain.ContainerID) (lifecycle.ContainerResult, error) {
	operationStartedAt := engine.beginMeasurement()
	target := operation.Target{Kind: operation.TargetContainer, ID: string(containerID)}
	releaseTarget := engine.targetLocks.lock(target)
	defer releaseTarget()
	observationStartedAt := engine.beginMeasurement()
	observation, err := engine.observeRetainedAttemptLocked(ctx, containerID)
	observationMeasurement := engine.finishMeasurement(observationStartedAt)
	if err != nil {
		return lifecycle.ContainerResult{}, err
	}
	if !observation.Terminal {
		return lifecycle.ContainerResult{}, errors.New("natural terminal record requires a terminal supervisor observation")
	}
	return engine.recordObservedTerminalLocked(ctx, operationID, containerID, observation, observationMeasurement, operationStartedAt)
}

// observeRetainedAttemptLocked reads and validates one exact adopted wrapper
// while the caller owns the Container target lock, attributing OOM only after terminal reap.
func (engine *Engine) observeRetainedAttemptLocked(ctx context.Context, containerID domain.ContainerID) (AttemptObservation, error) {
	inventory, err := engine.containerInventory(ctx, containerID)
	if err != nil {
		return AttemptObservation{}, err
	}
	process, err := receiptByKind(inventory, ownership.KindInitProcess)
	if err != nil {
		return AttemptObservation{}, err
	}
	cgroup, err := receiptByKind(inventory, ownership.KindAttemptCgroup)
	if err != nil {
		return AttemptObservation{}, err
	}
	observation, err := engine.providers.Supervisor.ObserveAttempt(ctx, provider.OwnedReceiptRequest{Owner: process.Owner, Receipt: process})
	if err != nil {
		return AttemptObservation{}, err
	}
	if err := observation.Validate(); err != nil {
		return AttemptObservation{}, err
	}
	if !observation.Terminal {
		return observation, nil
	}
	observation, err = engine.attributeTerminalOOM(ctx, cgroup, observation)
	if err != nil {
		return AttemptObservation{}, err
	}
	return observation, nil
}

// recordObservedTerminalLocked persists one already validated terminal fact
// without re-observing or reacquiring the Container target lock. The supplied
// operation start precedes the observation so a new complete span contains its stage.
func (engine *Engine) recordObservedTerminalLocked(ctx context.Context, operationID operation.OperationID, containerID domain.ContainerID, observation AttemptObservation, measurement stageMeasurement, operationStartedAt time.Time) (lifecycle.ContainerResult, error) {
	if err := observation.Validate(); err != nil {
		return lifecycle.ContainerResult{}, err
	}
	if !observation.Terminal {
		return lifecycle.ContainerResult{}, errors.New("terminal lifecycle persistence requires a terminal observation")
	}
	target := operation.Target{Kind: operation.TargetContainer, ID: string(containerID)}
	request := engine.terminalRecordRequest(operationID, containerID, observation)
	begin, err := engine.lifecycle.BeginRecordStopped(ctx, request)
	if err != nil || begin.Operation.State.Terminal() {
		return begin, err
	}
	if _, err = engine.checkpointProgress(ctx, operationID, target, begin.Fingerprint, operation.StageObserveProcess, observation, measurement); err != nil {
		return begin, err
	}
	completion := engine.finishOperationMeasurement(operationStartedAt, begin.Resolution)
	request.Verification.ObservedAt = completion.occurredAt
	request.Verification.Duration = completion.duration
	return engine.lifecycle.RecordStopped(ctx, request)
}

// terminalRecordRequest builds the exact standalone Stop semantics shared by
// natural-terminal recovery identity selection and two-stage persistence.
func (engine *Engine) terminalRecordRequest(operationID operation.OperationID, containerID domain.ContainerID, observation AttemptObservation) lifecycle.RecordStoppedRequest {
	return lifecycle.RecordStoppedRequest{
		OperationID: operationID, ContainerID: containerID, Outcome: observation.Outcome,
		Conditions: terminalConditions(observation.Outcome),
		Verification: lifecycle.Verification{
			Kind: lifecycle.VerificationAttemptStopped, Verified: true,
			Evidence: observation.Evidence, ObservedAt: engine.diagnosticNow(),
		},
	}
}

// attributeTerminalOOM combines independent wait facts with owner-scoped
// counters captured before workload release and after terminal reap.
func (engine *Engine) attributeTerminalOOM(ctx context.Context, cgroup ownership.Receipt, observation AttemptObservation) (AttemptObservation, error) {
	if !observation.Terminal || observation.Outcome.Presence != domain.OutcomeCaptured || observation.Outcome.OOM != domain.EvidenceUnknown {
		return observation, nil
	}
	progress, err := engine.lifecycle.GetOperationProgress(ctx, cgroup.Owner.OperationID)
	if err != nil {
		return AttemptObservation{}, err
	}
	if progress.OOMBaseline == nil {
		return AttemptObservation{}, errors.New("captured terminal wait result has no durable pre-start OOM baseline")
	}
	current, err := engine.providers.Cgroup.SnapshotAttemptOOM(ctx, provider.OwnedReceiptRequest{Owner: cgroup.Owner, Receipt: cgroup})
	if err != nil {
		return AttemptObservation{}, err
	}
	oom, err := current.Delta(*progress.OOMBaseline)
	if err != nil {
		return AttemptObservation{}, err
	}
	observation.Outcome.OOM = oom
	attributedEvidence, err := ownership.EvidenceDigest(struct {
		WrapperEvidence string               `json:"wrapper_evidence"`
		Baseline        provider.OOMSnapshot `json:"baseline"`
		Current         provider.OOMSnapshot `json:"current"`
		Outcome         domain.Outcome       `json:"outcome"`
	}{observation.Evidence, *progress.OOMBaseline, current, observation.Outcome})
	if err != nil {
		return AttemptObservation{}, err
	}
	observation.Evidence = attributedEvidence
	if err := observation.Validate(); err != nil {
		return AttemptObservation{}, err
	}
	return observation, nil
}

// waitForTerminalUntil observes immediately and polls only until the immutable operation deadline, so caller timeouts and daemon restarts cannot reset grace.
func (engine *Engine) waitForTerminalUntil(ctx context.Context, process ownership.Receipt, deadline time.Time) (AttemptObservation, error) {
	if deadline.IsZero() {
		return AttemptObservation{}, errors.New("terminal observation deadline must be non-zero")
	}
	for {
		observation, err := engine.providers.Supervisor.ObserveAttempt(ctx, provider.OwnedReceiptRequest{Owner: process.Owner, Receipt: process})
		if err != nil {
			return AttemptObservation{}, err
		}
		if err := observation.Validate(); err != nil {
			return AttemptObservation{}, err
		}
		now := engine.diagnosticNow()
		if observation.Terminal || !now.Before(deadline) {
			return observation, nil
		}
		wait := deadline.Sub(now)
		if wait > 10*time.Millisecond {
			wait = 10 * time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return AttemptObservation{}, ctx.Err()
		case <-timer.C:
		}
	}
}

// providerSignal maps explicit domain policy names to the provider's bounded signal vocabulary.
func providerSignal(value string) (provider.Signal, error) {
	signal := provider.Signal(value)
	if !signal.Valid() {
		return "", fmt.Errorf("unsupported provider signal %q", value)
	}
	return signal, nil
}

// terminalConditions supplies the mandatory condition when terminal outcome evidence is explicitly unknown.
func terminalConditions(outcome domain.Outcome) []domain.Condition {
	if outcome.Presence != domain.OutcomeUnknown {
		return nil
	}
	return []domain.Condition{{Type: domain.ConditionOutcomeUnknown, Reason: "supervisor_evidence_incomplete", Message: "wrapper confirmed termination but could not recover a complete child outcome"}}
}

// acquireContainerReceipt performs exactly the next canonical Attempt acquisition for crash-safe checkpointing.
func (engine *Engine) acquireContainerReceipt(
	ctx context.Context,
	owner ownership.OwnerKey,
	pair domain.ContainerAttempt,
	sandboxNamespaces provider.SandboxNamespaces,
	sandboxDNS []string,
	receipts []ownership.Receipt,
) (ownership.Receipt, operation.Stage, error) {
	switch len(receipts) {
	case 0:
		receipt, err := engine.providers.Cgroup.EnsureAttemptCgroup(ctx, provider.AttemptCgroupRequest{
			Owner: owner, SandboxID: pair.Container.SandboxID, AttemptID: pair.Attempt.ID, Limits: pair.Container.Spec.Limits,
		})
		return receipt, operation.StagePrepareCgroup, err
	case 1:
		receipt, err := engine.providers.Isolation.EnsureStartGate(ctx, provider.AttemptResourceRequest{Owner: owner, AttemptID: pair.Attempt.ID})
		return receipt, operation.StagePrepareStartGate, err
	case 2:
		receipt, err := engine.providers.Isolation.EnsureStreams(ctx, provider.AttemptResourceRequest{Owner: owner, AttemptID: pair.Attempt.ID})
		return receipt, operation.StagePrepareStreams, err
	case 3:
		receipt, err := engine.providers.Isolation.EnsureInitProcess(ctx, provider.InitProcessRequest{
			Owner: owner, SandboxID: pair.Container.SandboxID, AttemptID: pair.Attempt.ID,
			Cgroup: receipts[0], Gate: receipts[1], Streams: receipts[2], SandboxNamespaces: sandboxNamespaces,
			Process: pair.Container.Spec.Process,
		})
		return receipt, operation.StageCreateProcess, err
	case 4:
		receipt, err := engine.providers.Isolation.EnsureNamespace(ctx, provider.NamespaceRequest{Owner: owner, Process: receipts[3], Namespace: isolation.NamespacePID})
		return receipt, operation.StagePrepareNamespaces, err
	case 5:
		receipt, err := engine.providers.Isolation.EnsureNamespace(ctx, provider.NamespaceRequest{Owner: owner, Process: receipts[3], Namespace: isolation.NamespaceMount})
		return receipt, operation.StagePrepareNamespaces, err
	case 6:
		receipt, err := engine.providers.Isolation.EnsureRootfs(ctx, provider.RootfsRequest{
			Owner: owner, AttemptID: pair.Attempt.ID, Process: receipts[3], PID: receipts[4], Mount: receipts[5],
			SourceID: provider.OpaqueID(pair.Container.Spec.RootFS), DNS: append([]string(nil), sandboxDNS...),
		})
		return receipt, operation.StagePrepareRootfs, err
	default:
		return ownership.Receipt{}, "", fmt.Errorf("Container receipt prefix length %d exceeds the canonical profile", len(receipts))
	}
}

// sandboxNamespaces extracts and validates the stable namespace subset required by one Attempt launcher.
func sandboxNamespaces(receipts []ownership.Receipt) (provider.SandboxNamespaces, error) {
	uts, err := receiptByKind(receipts, ownership.KindUTSNamespace)
	if err != nil {
		return provider.SandboxNamespaces{}, err
	}
	ipc, err := receiptByKind(receipts, ownership.KindIPCNamespace)
	if err != nil {
		return provider.SandboxNamespaces{}, err
	}
	network, err := receiptByKind(receipts, ownership.KindNetworkNamespace)
	if err != nil {
		return provider.SandboxNamespaces{}, err
	}
	result := provider.SandboxNamespaces{UTS: uts, IPC: ipc, Network: network}
	if err := result.ValidateFor(domain.SandboxID(uts.Owner.Target.ID)); err != nil {
		return provider.SandboxNamespaces{}, err
	}
	return result, nil
}

// StartContainer verifies adopted preparation, persists attach/release/observe in order,
// and confirms Running only from an explicit workload-child observation.
func (engine *Engine) StartContainer(ctx context.Context, request lifecycle.ContainerActionRequest) (lifecycle.ContainerResult, error) {
	operationStartedAt := engine.beginMeasurement()
	target := operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}
	release := engine.targetLocks.lock(target)
	defer release()
	result, err := engine.lifecycle.BeginContainerStart(ctx, request)
	if err != nil || result.Operation.State.Terminal() {
		return result, err
	}
	inventory, err := engine.containerInventory(ctx, request.ContainerID)
	if err != nil {
		return result, err
	}
	if err := ownership.ValidateReceiptJournalProfile(target.Kind, inventory); err != nil {
		return result, err
	}
	for index := range inventory {
		if !inventory[index].Adopted {
			return result, errors.New("Container start requires a fully adopted host inventory")
		}
	}
	resourceOwner := inventory[0].Owner
	cgroup := inventory[0]
	gate := inventory[1]
	process := inventory[3]
	rootfs := inventory[6]
	progress, err := engine.lifecycle.GetOperationProgress(ctx, request.OperationID)
	if err != nil {
		return result, err
	}
	if progress.Operation.Stage == operation.StagePersistIntent {
		attachmentStartedAt := engine.beginMeasurement()
		attachment, attachErr := engine.providers.Cgroup.AttachAttemptProcess(ctx, provider.AttachProcessRequest{Owner: resourceOwner, Cgroup: cgroup, Process: process})
		attachmentMeasurement := engine.finishMeasurement(attachmentStartedAt)
		if attachErr != nil {
			return result, attachErr
		}
		if _, err = engine.checkpointProgress(ctx, request.OperationID, target, result.Fingerprint, operation.StageAttachCgroup, attachment, attachmentMeasurement); err != nil {
			return result, err
		}
		progress, err = engine.lifecycle.GetOperationProgress(ctx, request.OperationID)
		if err != nil {
			return result, err
		}
	}
	if progress.Operation.Stage == operation.StageAttachCgroup {
		releaseStartedAt := engine.beginMeasurement()
		attachment, attachErr := engine.providers.Cgroup.AttachAttemptProcess(ctx, provider.AttachProcessRequest{Owner: resourceOwner, Cgroup: cgroup, Process: process})
		if attachErr != nil {
			return result, attachErr
		}
		beforeRelease, observeErr := engine.providers.Supervisor.ObserveAttempt(ctx, provider.OwnedReceiptRequest{Owner: resourceOwner, Receipt: process})
		if observeErr != nil {
			return result, observeErr
		}
		if err := beforeRelease.Validate(); err != nil {
			return result, err
		}
		releaseEvidence := any(beforeRelease)
		if beforeRelease.Prepared {
			gateObservation, releaseErr := engine.providers.Isolation.ReleaseStartGate(ctx, provider.ReleaseGateRequest{
				Owner: resourceOwner, Gate: gate, Process: process, Cgroup: cgroup, Rootfs: rootfs, Attachment: attachment,
			})
			if releaseErr == nil {
				if err := gateObservation.Validate(); err != nil {
					return result, err
				}
				releaseEvidence = gateObservation
			} else {
				afterRelease, recoveryErr := engine.providers.Supervisor.ObserveAttempt(ctx, provider.OwnedReceiptRequest{Owner: resourceOwner, Receipt: process})
				if recoveryErr != nil {
					return result, errors.Join(releaseErr, recoveryErr)
				}
				if validateErr := afterRelease.Validate(); validateErr != nil {
					return result, errors.Join(releaseErr, validateErr)
				}
				if afterRelease.Prepared {
					return result, releaseErr
				}
				releaseEvidence = afterRelease
			}
		}
		releaseMeasurement := engine.finishMeasurement(releaseStartedAt)
		if _, err = engine.checkpointProgress(ctx, request.OperationID, target, result.Fingerprint, operation.StageReleaseStartGate, releaseEvidence, releaseMeasurement); err != nil {
			return result, err
		}
	}
	observationStartedAt := engine.beginMeasurement()
	workload, err := engine.providers.Supervisor.ObserveAttempt(ctx, provider.OwnedReceiptRequest{Owner: resourceOwner, Receipt: process})
	if err != nil {
		return result, err
	}
	if err := workload.Validate(); err != nil {
		return result, err
	}
	workload, err = engine.attributeTerminalOOM(ctx, cgroup, workload)
	if err != nil {
		return result, err
	}
	observationMeasurement := engine.finishMeasurement(observationStartedAt)
	if _, err = engine.checkpointProgress(ctx, request.OperationID, target, result.Fingerprint, operation.StageObserveProcess, workload, observationMeasurement); err != nil {
		return result, err
	}
	progress, err = engine.lifecycle.GetOperationProgress(ctx, request.OperationID)
	if err != nil {
		return result, err
	}
	if err = engine.checkpointClearCondition(ctx, progress, target, result.Fingerprint, domain.ConditionProcessIdentityUnknown,
		map[string]string{"recovery": "workload start now has a verified terminal or running fact"}); err != nil {
		return result, err
	}
	if workload.Terminal {
		completion := engine.finishOperationMeasurement(operationStartedAt, result.Resolution)
		return engine.lifecycle.RecordContainerStartTerminal(ctx, lifecycle.ContainerStartTerminalRequest{
			OperationID: request.OperationID, ContainerID: request.ContainerID, Fingerprint: result.Fingerprint,
			Outcome: workload.Outcome, OperationFailed: workload.StartFailed, Conditions: terminalConditions(workload.Outcome),
			Verification: lifecycle.Verification{
				Kind: lifecycle.VerificationAttemptStopped, Verified: true, Evidence: workload.Evidence,
				ObservedAt: completion.occurredAt, Duration: completion.duration,
			},
		})
	}
	if !workload.Running {
		return result, errors.New("workload child is not yet verified running")
	}
	pair, err := engine.lifecycle.GetContainer(ctx, request.ContainerID)
	if err != nil {
		return result, err
	}
	identity := domain.ProcessIdentity{Verified: true, Handle: processIdentityHandle(process), Evidence: process.EvidenceSHA256}
	completion := engine.finishOperationMeasurement(operationStartedAt, result.Resolution)
	return engine.lifecycle.ConfirmContainerStart(ctx, lifecycle.ContainerConfirmRequest{
		OperationID: request.OperationID, ContainerID: request.ContainerID, Fingerprint: result.Fingerprint,
		Verification: lifecycle.Verification{
			Kind: lifecycle.VerificationAttemptRunning, Verified: true, Evidence: workload.Evidence,
			ObservedAt: completion.occurredAt, Duration: completion.duration, ProcessIdentity: &identity, Streams: pair.Attempt.Streams,
		},
	})
}

// streamReferences converts provider-issued opaque stream attributes into persisted API references.
func streamReferences(receipt ownership.Receipt) domain.StreamReferences {
	stdout := receipt.Attributes["stdout"]
	stderr := receipt.Attributes["stderr"]
	stdin := receipt.Attributes["stdin"]
	if stdout == "" {
		stdout = receipt.LocalID + ":stdout"
	}
	if stderr == "" {
		stderr = receipt.LocalID + ":stderr"
	}
	return domain.StreamReferences{Stdin: stdin, Stdout: stdout, Stderr: stderr}
}
