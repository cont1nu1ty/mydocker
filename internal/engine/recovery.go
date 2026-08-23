package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"mydocker/internal/domain"
	"mydocker/internal/lifecycle"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
	"mydocker/internal/state"
)

// ReceiptDiscovery binds one durable recovery input to its read-only live observation.
type ReceiptDiscovery struct {
	Target      operation.Target
	Receipt     ownership.Receipt
	Observation provider.ResourceObservation
}

// RecoveryIssue records a retained resource whose exact owned host object is absent or unprovable.
type RecoveryIssue struct {
	Target               operation.Target
	Kind                 ownership.Kind
	Presence             provider.Presence
	Reason               string
	Receipt              *ownership.Receipt
	AttemptID            domain.AttemptID
	RetryableObservation bool
}

// RunningDiscovery records the supervisor fact captured behind the global read-only recovery barrier.
type RunningDiscovery struct {
	ContainerID domain.ContainerID
	Process     ownership.Receipt
	Observation AttemptObservation
}

// RecoveryPlan is a deterministic read-only snapshot from which reconciliation may begin.
type RecoveryPlan struct {
	ActiveOperations []operation.Operation
	Receipts         []ReceiptDiscovery
	Issues           []RecoveryIssue
	Running          []RunningDiscovery
}

// RecoveryReport identifies operations resumed or recovery conditions recorded after one complete discovery barrier.
type RecoveryReport struct {
	Plan       RecoveryPlan
	Reconciled []operation.OperationID
	Recorded   []operation.OperationID
}

// recoverySnapshot owns deep-copied persistence records collected in one consistent Store view.
type recoverySnapshot struct {
	sandboxes  []state.SandboxRecord
	containers []state.ContainerAttemptRecord
	operations []state.OperationRecord
}

// recoveryEpoch binds a candidate internal operation identity to the exact
// durable resource state from which reconciliation made its decision.
type recoveryEpoch struct {
	LastObservation domain.LifecycleObservation
	StateSHA256     string
}

// Discover performs a daemon-wide read-only inventory before publishing any
// process identity or authorizing a host mutation.
func (engine *Engine) Discover(ctx context.Context) (RecoveryPlan, error) {
	identityRevision := engine.identityRevision()
	snapshot, err := engine.recoverySnapshot(ctx)
	if err != nil {
		return RecoveryPlan{}, err
	}
	plan := RecoveryPlan{}
	activeTargets := make(map[string]struct{})
	attemptIDs := make(map[domain.ContainerID]domain.AttemptID, len(snapshot.containers))
	for _, record := range snapshot.operations {
		if !record.Operation.State.Active() {
			continue
		}
		plan.ActiveOperations = append(plan.ActiveOperations, record.Operation.Clone())
		activeTargets[recoveryTargetKey(record.Operation.Target)] = struct{}{}
	}
	for _, record := range snapshot.sandboxes {
		target := operation.Target{Kind: operation.TargetSandbox, ID: string(record.Sandbox.ID)}
		if err := engine.discoverReceipts(ctx, target, "", record.HostResources, &plan); err != nil {
			return RecoveryPlan{}, err
		}
	}
	for _, record := range snapshot.containers {
		target := operation.Target{Kind: operation.TargetContainer, ID: string(record.ContainerAttempt.Container.ID)}
		attemptIDs[record.ContainerAttempt.Container.ID] = record.ContainerAttempt.Attempt.ID
		if err := engine.discoverReceipts(ctx, target, record.ContainerAttempt.Attempt.ID, record.HostResources, &plan); err != nil {
			return RecoveryPlan{}, err
		}
	}
	for _, record := range snapshot.operations {
		if !record.Operation.State.Active() {
			continue
		}
		attemptID := domain.AttemptID("")
		if record.Operation.Target.Kind == operation.TargetContainer {
			attemptID = attemptIDs[domain.ContainerID(record.Operation.Target.ID)]
		}
		if err := engine.discoverReceipts(ctx, record.Operation.Target, attemptID, record.Receipts, &plan); err != nil {
			return RecoveryPlan{}, err
		}
	}
	identities := make(map[string]ownership.Receipt)
	for _, discovery := range plan.Receipts {
		if discovery.Receipt.Kind != ownership.KindInitProcess || discovery.Observation.Presence != provider.PresencePresent {
			continue
		}
		handle := processIdentityHandle(discovery.Receipt)
		if existing, found := identities[handle]; found &&
			(existing.Owner != discovery.Receipt.Owner || existing.EvidenceSHA256 != discovery.Receipt.EvidenceSHA256) {
			return RecoveryPlan{}, errors.New("read-only discovery found colliding init-process identities")
		}
		identities[handle] = discovery.Receipt.Clone()
	}
	for _, record := range snapshot.containers {
		pair := record.ContainerAttempt
		if pair.Attempt.Phase != domain.AttemptRunning {
			continue
		}
		containerTarget := operation.Target{Kind: operation.TargetContainer, ID: string(pair.Container.ID)}
		if _, active := activeTargets[recoveryTargetKey(containerTarget)]; active {
			continue
		}
		process, receiptErr := receiptByKind(record.HostResources, ownership.KindInitProcess)
		if receiptErr != nil {
			plan.Issues = append(plan.Issues, RecoveryIssue{
				Target: containerTarget, Kind: ownership.KindInitProcess, Presence: provider.PresenceUnknown,
				Reason: "running Attempt has no durable init-process receipt", AttemptID: pair.Attempt.ID,
			})
			continue
		}
		if _, present := identities[processIdentityHandle(process)]; !present {
			continue
		}
		observation, observeErr := engine.providers.Supervisor.ObserveAttempt(ctx, provider.OwnedReceiptRequest{Owner: process.Owner, Receipt: process})
		if observeErr != nil {
			if provider.IsObservationUnavailable(observeErr) {
				plan.Issues = append(plan.Issues, RecoveryIssue{
					Target: containerTarget, Kind: ownership.KindInitProcess, Presence: provider.PresenceUnknown,
					Reason:  "workload supervisor observation is temporarily unavailable",
					Receipt: receiptPointer(process), AttemptID: pair.Attempt.ID, RetryableObservation: true,
				})
				continue
			}
			return RecoveryPlan{}, fmt.Errorf("discover running Container %q: %w", pair.Container.ID, observeErr)
		}
		if err := observation.Validate(); err != nil {
			return RecoveryPlan{}, fmt.Errorf("validate running Container %q observation: %w", pair.Container.ID, err)
		}
		plan.Running = append(plan.Running, RunningDiscovery{ContainerID: pair.Container.ID, Process: process.Clone(), Observation: observation})
	}
	engine.publishDiscoveredIdentities(identityRevision, identities)
	return plan, nil
}

// recoverySnapshot copies every record in one Store view so provider reads never hold a persistence lock.
func (engine *Engine) recoverySnapshot(ctx context.Context) (recoverySnapshot, error) {
	var snapshot recoverySnapshot
	err := engine.store.View(ctx, func(reader state.Reader) error {
		var err error
		snapshot.sandboxes, err = reader.ListSandboxes()
		if err != nil {
			return err
		}
		for _, sandbox := range snapshot.sandboxes {
			pairs, listErr := reader.ListContainerAttempts(sandbox.Sandbox.ID)
			if listErr != nil {
				return listErr
			}
			snapshot.containers = append(snapshot.containers, pairs...)
		}
		snapshot.operations, err = reader.ListOperations()
		return err
	})
	return snapshot, err
}

// discoverReceipts inspects an ordered inventory without mutating providers or
// persistence, retaining exact owner and Attempt-incarnation evidence for any
// issue that may become an internal recovery operation.
func (engine *Engine) discoverReceipts(ctx context.Context, target operation.Target, attemptID domain.AttemptID, receipts []ownership.Receipt, plan *RecoveryPlan) error {
	for _, receipt := range receipts {
		observation, err := engine.inspectReceipt(ctx, receipt)
		if err != nil {
			if !provider.IsObservationUnavailable(err) {
				return fmt.Errorf("discover %s %q: %w", receipt.Kind, receipt.LocalID, err)
			}
			observation = provider.ResourceObservation{
				Presence: provider.PresenceUnknown, Verified: false,
				Reason: "owned resource observation is temporarily unavailable",
			}
		}
		plan.Receipts = append(plan.Receipts, ReceiptDiscovery{Target: target, Receipt: receipt.Clone(), Observation: observation})
		if observation.Presence != provider.PresencePresent {
			reason := observation.Reason
			if reason == "" {
				reason = "durable owned resource is absent"
			}
			plan.Issues = append(plan.Issues, RecoveryIssue{
				Target: target, Kind: receipt.Kind, Presence: observation.Presence, Reason: reason,
				Receipt: receiptPointer(receipt), AttemptID: attemptID,
			})
		}
	}
	return nil
}

// receiptPointer returns an independent optional receipt so recovery reports
// cannot mutate the provider attributes used for operation identity evidence.
func receiptPointer(receipt ownership.Receipt) *ownership.Receipt {
	clone := receipt.Clone()
	return &clone
}

// Reconcile completes the global discovery barrier before resuming active intents or recording M3 restart facts.
func (engine *Engine) Reconcile(ctx context.Context) (RecoveryReport, error) {
	plan, err := engine.Discover(ctx)
	if err != nil {
		return RecoveryReport{}, err
	}
	report := RecoveryReport{Plan: plan}
	issuesByTarget := recoveryIssuesByTarget(plan.Issues)
	handledIssues := make(map[string]struct{})
	for _, active := range plan.ActiveOperations {
		key := recoveryTargetKey(active.Target)
		if active.Type == operation.TypeDelete {
			if issues := issuesByTarget[key]; recoveryIssuesContainUnknown(issues) {
				if err := engine.checkpointRecoveryIssue(ctx, active, issues); err != nil {
					return report, err
				}
				handledIssues[key] = struct{}{}
				report.Reconciled = append(report.Reconciled, active.ID)
				continue
			}
			// Absence can be the expected effect in the crash window between a
			// provider teardown and its release checkpoint. The idempotent delete
			// path must inspect and journal that fact instead of failing the intent.
			if err := engine.reconcileActiveOperation(ctx, active); err != nil {
				return report, err
			}
			handledIssues[key] = struct{}{}
			report.Reconciled = append(report.Reconciled, active.ID)
			continue
		}
		if issues := issuesByTarget[key]; len(issues) != 0 {
			evidence := recoveryIssueEvidence(issues)
			if recoveryIssuesContainUnknown(issues) || active.Type == operation.TypeStop {
				if err := engine.checkpointRecoveryIssue(ctx, active, issues); err != nil {
					return report, err
				}
			} else if active.Type == operation.TypeCreate {
				if _, err := engine.RollbackCreate(ctx, active.ID, lifecycle.Failure{
					Reason: operation.ReasonCleanup, Message: evidence,
				}); err != nil {
					return report, err
				}
			} else {
				if _, err := engine.lifecycle.FailActiveOperation(ctx, lifecycle.FailOperationRequest{
					OperationID: active.ID, Target: active.Target, Fingerprint: active.Fingerprint,
					Failure:   lifecycle.Failure{Reason: operation.ReasonCleanup, Message: evidence},
					Condition: recoveryIssueCondition(active.Target, issues), ObservedAt: engine.clock.Now(),
				}); err != nil {
					return report, err
				}
			}
			handledIssues[key] = struct{}{}
			report.Reconciled = append(report.Reconciled, active.ID)
			continue
		}
		if err := engine.reconcileActiveOperation(ctx, active); err != nil {
			return report, err
		}
		report.Reconciled = append(report.Reconciled, active.ID)
	}
	issueKeys := make([]string, 0, len(issuesByTarget))
	for key := range issuesByTarget {
		issueKeys = append(issueKeys, key)
	}
	sort.Strings(issueKeys)
	for _, key := range issueKeys {
		issues := issuesByTarget[key]
		if _, handled := handledIssues[key]; handled {
			continue
		}
		target := issues[0].Target
		evidence := recoveryIssueEvidence(issues)
		request := lifecycle.ReconcileConditionRequest{
			Target: target, Condition: conditionPointer(recoveryIssueCondition(target, issues)),
			Evidence: evidence, ObservedAt: engine.clock.Now(),
		}
		fingerprint, fingerprintErr := request.RequestFingerprint()
		if fingerprintErr != nil {
			return report, fingerprintErr
		}
		operationID, identityErr := engine.recoveryOperationID(ctx, "resource-issue", operation.TypeState, target, fingerprint)
		if identityErr != nil {
			return report, identityErr
		}
		request.OperationID = operationID
		if _, err := engine.lifecycle.ReconcileCondition(ctx, request); err != nil {
			return report, err
		}
		report.Recorded = append(report.Recorded, operationID)
	}
	for _, running := range plan.Running {
		if running.Observation.Terminal {
			target := operation.Target{Kind: operation.TargetContainer, ID: string(running.ContainerID)}
			request := engine.terminalRecordRequest("", running.ContainerID, running.Observation)
			fingerprint, fingerprintErr := request.RequestFingerprint()
			if fingerprintErr != nil {
				return report, fingerprintErr
			}
			operationID, identityErr := engine.recoveryOperationID(ctx, "terminal", operation.TypeStop, target, fingerprint)
			if identityErr != nil {
				return report, identityErr
			}
			if _, err := engine.RecordTerminal(ctx, operationID, running.ContainerID); err != nil {
				return report, err
			}
			report.Recorded = append(report.Recorded, operationID)
			continue
		}
		target := operation.Target{Kind: operation.TargetContainer, ID: string(running.ContainerID)}
		evidence := "M3 restart observed the retained init wrapper but supervisor reconnection is deferred to M5: " + running.Observation.Evidence
		request := lifecycle.ReconcileConditionRequest{
			Target: target,
			Condition: &domain.Condition{
				Type: domain.ConditionProcessIdentityUnknown, Reason: "DaemonRestart",
				Message: "workload state was observed read-only, but M3 does not claim a reconnectable supervisor channel",
			},
			Evidence: evidence, ObservedAt: engine.clock.Now(),
		}
		fingerprint, fingerprintErr := request.RequestFingerprint()
		if fingerprintErr != nil {
			return report, fingerprintErr
		}
		operationID, identityErr := engine.recoveryOperationID(ctx, "identity-unknown", operation.TypeState, target, fingerprint)
		if identityErr != nil {
			return report, identityErr
		}
		request.OperationID = operationID
		_, err := engine.lifecycle.ReconcileCondition(ctx, request)
		if err != nil {
			return report, err
		}
		report.Recorded = append(report.Recorded, operationID)
	}
	return report, nil
}

// recoveryIssuesByTarget groups read-only discovery failures without allowing duplicate inventory observations to trigger duplicate mutations.
func recoveryIssuesByTarget(issues []RecoveryIssue) map[string][]RecoveryIssue {
	grouped := make(map[string][]RecoveryIssue)
	seen := make(map[string]struct{})
	for _, issue := range issues {
		key := recoveryTargetKey(issue.Target)
		identity := key + "\x00" + string(issue.Kind) + "\x00" + string(issue.Presence) + "\x00" + issue.Reason + "\x00" + string(issue.AttemptID)
		if issue.Receipt != nil {
			identity += "\x00" + string(issue.Receipt.Owner.OperationID) + "\x00" + issue.Receipt.Owner.Token +
				"\x00" + issue.Receipt.LocalID + "\x00" + issue.Receipt.EvidenceSHA256
		}
		if _, exists := seen[identity]; exists {
			continue
		}
		seen[identity] = struct{}{}
		grouped[key] = append(grouped[key], issue)
	}
	return grouped
}

// recoveryIssuesContainUnknown reports whether any retained host resource
// lacks a verified presence or absence fact and therefore authorizes no mutation.
func recoveryIssuesContainUnknown(issues []RecoveryIssue) bool {
	for _, issue := range issues {
		if issue.Presence == provider.PresenceUnknown {
			return true
		}
	}
	return false
}

// checkpointRecoveryIssue keeps a temporarily unprovable active intent
// resumable while atomically projecting one visible fail-closed condition.
func (engine *Engine) checkpointRecoveryIssue(ctx context.Context, active operation.Operation, issues []RecoveryIssue) error {
	progress, err := engine.lifecycle.GetOperationProgress(ctx, active.ID)
	if err != nil {
		return err
	}
	stage := progress.Operation.Stage
	if stage == operation.StagePersistIntent {
		stage = operation.StageCheckPreconditions
	}
	condition := recoveryIssueCondition(active.Target, issues)
	_, err = engine.lifecycle.CheckpointOperation(ctx, lifecycle.CheckpointRequest{
		OperationID: active.ID, Target: active.Target, Fingerprint: active.Fingerprint,
		Stage: stage, RollbackCause: progress.RollbackCause, OOMBaseline: progress.OOMBaseline,
		KillEscalationDeadline: progress.KillEscalationDeadline,
		Rollback:               cloneRollback(progress.Rollback), Receipts: cloneReceipts(progress.Receipts),
		Releases: cloneReleases(progress.Releases), OccurredAt: engine.clock.Now(),
		Details:         map[string]string{"recovery": "owned resource observation remains unverified"},
		UpsertCondition: &condition,
	})
	return err
}

// recoveryIssueEvidence produces one deterministic bounded diagnostic whose
// leading digest binds every exact receipt owner, token, local ID, evidence,
// and Attempt incarnation even when the human-readable suffix is truncated.
func recoveryIssueEvidence(issues []RecoveryIssue) string {
	encoded, err := json.Marshal(issues)
	if err != nil {
		return "owned resource discovery could not be encoded"
	}
	digest := sha256.Sum256(encoded)
	const diagnosticLimit = 1900
	if len(encoded) > diagnosticLimit {
		encoded = []byte(strings.ToValidUTF8(string(encoded[:diagnosticLimit]), ""))
	}
	return "sha256:" + hex.EncodeToString(digest[:]) + ";detail:" + string(encoded)
}

// recoveryIssueCondition maps missing process authority to identity-unknown and
// distinguishes a retryable supervisor transport outage from absent ownership.
func recoveryIssueCondition(target operation.Target, issues []RecoveryIssue) domain.Condition {
	conditionType := domain.ConditionCleanupPending
	if target.Kind == operation.TargetContainer {
		for _, issue := range issues {
			if issue.Kind == ownership.KindInitProcess {
				conditionType = domain.ConditionProcessIdentityUnknown
				break
			}
		}
	}
	reason := "DaemonRecoveryEvidenceMissing"
	if recoveryIssuesAreRetryableObservations(issues) {
		reason = "SupervisorObservationUnavailable"
	}
	return domain.Condition{Type: conditionType, Reason: reason, Message: recoveryIssueEvidence(issues)}
}

// recoveryIssuesAreRetryableObservations reports whether every issue is a
// temporary supervisor read failure rather than missing host ownership proof.
func recoveryIssuesAreRetryableObservations(issues []RecoveryIssue) bool {
	if len(issues) == 0 {
		return false
	}
	for _, issue := range issues {
		if !issue.RetryableObservation {
			return false
		}
	}
	return true
}

// conditionPointer returns a caller-owned condition pointer for lifecycle reconciliation requests.
func conditionPointer(condition domain.Condition) *domain.Condition {
	return &condition
}

// reconcileActiveOperation reconstructs only semantic requests retained in immutable resource state or operation response.
func (engine *Engine) reconcileActiveOperation(ctx context.Context, active operation.Operation) error {
	if active.Stage == operation.StageRollback {
		progress, progressErr := engine.lifecycle.GetOperationProgress(ctx, active.ID)
		if progressErr != nil {
			return progressErr
		}
		if progress.RollbackCause == nil {
			return errors.New("active rollback is missing its durable primary failure cause")
		}
		_, err := engine.RollbackCreate(ctx, active.ID, lifecycle.Failure{
			Reason: progress.RollbackCause.Reason, Message: progress.RollbackCause.Message,
		})
		return err
	}
	switch active.Type {
	case operation.TypeCreate:
		if active.Target.Kind == operation.TargetSandbox {
			sandbox, err := engine.lifecycle.GetSandbox(ctx, domain.SandboxID(active.Target.ID))
			if err != nil {
				return err
			}
			_, err = engine.CreateSandbox(ctx, lifecycle.SandboxCreateRequest{OperationID: active.ID, SandboxID: sandbox.ID, Spec: sandbox.Spec})
			return err
		}
		pair, err := engine.lifecycle.GetContainer(ctx, domain.ContainerID(active.Target.ID))
		if err != nil {
			return err
		}
		_, err = engine.CreateContainer(ctx, lifecycle.ContainerCreateRequest{
			OperationID: active.ID, SandboxID: pair.Container.SandboxID, ContainerID: pair.Container.ID,
			AttemptID: pair.Attempt.ID, Process: pair.Container.Spec.Process,
			ImageDigest: pair.Container.Spec.ImageDigest, RootFS: pair.Container.Spec.RootFS,
		})
		return err
	case operation.TypeStart:
		containerID := domain.ContainerID(active.Target.ID)
		_, err := engine.StartContainer(ctx, lifecycle.ContainerActionRequest{OperationID: active.ID, ContainerID: containerID})
		if err == nil {
			return nil
		}
		observation, observeErr := engine.observeStartingAttempt(ctx, containerID)
		if observeErr != nil {
			if provider.IsObservationUnavailable(observeErr) {
				return engine.checkpointRecoveryIssue(ctx, active, []RecoveryIssue{{
					Target: active.Target, Kind: ownership.KindInitProcess, Presence: provider.PresenceUnknown,
					Reason: "workload start observation is temporarily unavailable",
				}})
			}
			return errors.Join(err, observeErr)
		}
		if observation.Starting {
			return engine.checkpointStartingRecovery(ctx, active)
		}
		if observation.Running || observation.Terminal {
			_, retryErr := engine.StartContainer(ctx, lifecycle.ContainerActionRequest{OperationID: active.ID, ContainerID: containerID})
			return retryErr
		}
		return err
	case operation.TypeStop:
		if active.Target.Kind == operation.TargetContainer {
			_, err := engine.RecordTerminal(ctx, active.ID, domain.ContainerID(active.Target.ID))
			if provider.IsObservationUnavailable(err) {
				issue, issueErr := engine.temporarySupervisorRecoveryIssue(ctx, active.Target, "natural terminal observation is temporarily unavailable")
				if issueErr != nil {
					return errors.Join(err, issueErr)
				}
				return engine.checkpointRecoveryIssue(ctx, active, []RecoveryIssue{issue})
			}
			return err
		}
		if active.Target.Kind != operation.TargetSandbox {
			return fmt.Errorf("unsupported active stop target %q", active.Target.Kind)
		}
		_, err := engine.StopSandbox(ctx, lifecycle.SandboxActionRequest{OperationID: active.ID, SandboxID: domain.SandboxID(active.Target.ID)})
		return err
	case operation.TypeDelete:
		if active.Target.Kind == operation.TargetSandbox {
			_, err := engine.RemoveSandbox(ctx, lifecycle.SandboxActionRequest{OperationID: active.ID, SandboxID: domain.SandboxID(active.Target.ID)})
			return err
		}
		_, err := engine.DeleteContainer(ctx, lifecycle.ContainerActionRequest{OperationID: active.ID, ContainerID: domain.ContainerID(active.Target.ID)})
		return err
	case operation.TypeKill:
		policy, err := killPolicyFromOperation(active)
		if err != nil {
			return err
		}
		return engine.reconcileActiveKill(ctx, active, policy)
	case operation.TypeState:
		return nil
	default:
		return fmt.Errorf("unsupported active operation type %q", active.Type)
	}
}

// temporarySupervisorRecoveryIssue binds one retryable control-channel outage
// to the exact retained init receipt and Attempt incarnation before it is
// projected as a condition or retried by the online terminal watcher.
func (engine *Engine) temporarySupervisorRecoveryIssue(ctx context.Context, target operation.Target, reason string) (RecoveryIssue, error) {
	if target.Kind != operation.TargetContainer {
		return RecoveryIssue{}, errors.New("temporary supervisor issue requires a Container target")
	}
	pair, err := engine.lifecycle.GetContainer(ctx, domain.ContainerID(target.ID))
	if err != nil {
		return RecoveryIssue{}, err
	}
	inventory, err := engine.containerInventory(ctx, pair.Container.ID)
	if err != nil {
		return RecoveryIssue{}, err
	}
	process, err := receiptByKind(inventory, ownership.KindInitProcess)
	if err != nil {
		return RecoveryIssue{}, err
	}
	return RecoveryIssue{
		Target: target, Kind: ownership.KindInitProcess, Presence: provider.PresenceUnknown,
		Reason: reason, Receipt: receiptPointer(process), AttemptID: pair.Attempt.ID, RetryableObservation: true,
	}, nil
}

// reconcileActiveKill observes a future-deadline Kill once during daemon
// startup and leaves it active rather than blocking UDS publication for the
// entire remaining grace period; due operations resume normal escalation.
func (engine *Engine) reconcileActiveKill(ctx context.Context, active operation.Operation, policy domain.TerminationPolicy) error {
	progress, err := engine.lifecycle.GetOperationProgress(ctx, active.ID)
	if err != nil {
		return err
	}
	if progress.Operation.Stage == operation.StagePersistIntent {
		return engine.checkpointInitialKillDuringRecovery(ctx, active, policy)
	}
	if progress.KillEscalationDeadline == nil {
		return errors.New("active signaled Kill is missing its durable escalation deadline")
	}
	if !engine.clock.Now().Before(*progress.KillEscalationDeadline) {
		_, err = engine.KillContainer(ctx, lifecycle.KillRequest{
			OperationID: active.ID, ContainerID: domain.ContainerID(active.Target.ID), Policy: policy,
		})
		return err
	}
	inventory, err := engine.containerInventory(ctx, domain.ContainerID(active.Target.ID))
	if err != nil {
		return err
	}
	process, err := receiptByKind(inventory, ownership.KindInitProcess)
	if err != nil {
		return err
	}
	observation, err := engine.providers.Supervisor.ObserveAttempt(ctx, provider.OwnedReceiptRequest{Owner: process.Owner, Receipt: process})
	if err != nil {
		if provider.IsObservationUnavailable(err) {
			return engine.checkpointRecoveryIssue(ctx, active, []RecoveryIssue{{
				Target: active.Target, Kind: ownership.KindInitProcess, Presence: provider.PresenceUnknown,
				Reason: "Kill grace-period observation is temporarily unavailable",
			}})
		}
		return err
	}
	if err := observation.Validate(); err != nil {
		return err
	}
	if !observation.Terminal {
		return nil
	}
	_, err = engine.KillContainer(ctx, lifecycle.KillRequest{
		OperationID: active.ID, ContainerID: domain.ContainerID(active.Target.ID), Policy: policy,
	})
	return err
}

// checkpointInitialKillDuringRecovery delivers at most the initial signal and
// persists its absolute deadline, allowing daemon startup to publish the UDS
// without synchronously waiting an arbitrarily long grace period.
func (engine *Engine) checkpointInitialKillDuringRecovery(ctx context.Context, active operation.Operation, policy domain.TerminationPolicy) error {
	target := active.Target
	if target.Kind != operation.TargetContainer {
		return fmt.Errorf("Kill recovery target must be a Container, got %q", target.Kind)
	}
	releaseTarget := engine.targetLocks.lock(target)
	defer releaseTarget()
	progress, err := engine.lifecycle.GetOperationProgress(ctx, active.ID)
	if err != nil {
		return err
	}
	if progress.Operation.State.Terminal() || progress.Operation.Stage != operation.StagePersistIntent {
		return nil
	}
	inventory, err := engine.containerInventory(ctx, domain.ContainerID(target.ID))
	if err != nil {
		return err
	}
	process, err := receiptByKind(inventory, ownership.KindInitProcess)
	if err != nil {
		return err
	}
	cgroup, err := receiptByKind(inventory, ownership.KindAttemptCgroup)
	if err != nil {
		return err
	}
	signal, err := providerSignal(policy.Signal)
	if err != nil {
		return err
	}
	delivery, signalErr := engine.providers.Isolation.SignalVerified(ctx, provider.SignalRequest{
		Owner: process.Owner, Process: process, ActionOperationID: active.ID,
		Step: provider.SignalStepInitial, Signal: signal,
	})
	if signalErr == nil {
		_, err = engine.checkpointKillSignal(ctx, active.ID, target, active.Fingerprint, delivery, policy.GracePeriod)
		return err
	}
	observation, observeErr := engine.observeAfterSignalError(ctx, process, signalErr)
	if observeErr != nil {
		if provider.IsObservationUnavailable(observeErr) {
			return nil
		}
		return observeErr
	}
	return engine.recordRecoveredKillTerminalLocked(ctx, active, cgroup, observation)
}

// recordRecoveredKillTerminalLocked closes the natural-exit race discovered
// while resuming a pre-deadline Kill without sending another physical signal.
func (engine *Engine) recordRecoveredKillTerminalLocked(ctx context.Context, active operation.Operation, cgroup ownership.Receipt, observation AttemptObservation) error {
	attributed, err := engine.attributeTerminalOOM(ctx, cgroup, observation)
	if err != nil {
		return err
	}
	if _, err = engine.checkpointProgress(ctx, active.ID, active.Target, active.Fingerprint, operation.StageObserveProcess, attributed); err != nil {
		return err
	}
	_, err = engine.lifecycle.RecordKillStopped(ctx, lifecycle.KillStoppedRequest{
		OperationID: active.ID, ContainerID: domain.ContainerID(active.Target.ID), Fingerprint: active.Fingerprint,
		Outcome: attributed.Outcome, Conditions: terminalConditions(attributed.Outcome),
		Verification: lifecycle.Verification{
			Kind: lifecycle.VerificationAttemptStopped, Verified: true,
			Evidence: attributed.Evidence, ObservedAt: engine.clock.Now(),
		},
	})
	return err
}

// observeStartingAttempt rechecks the exact retained init receipt after a
// resumed Start reports an error, distinguishing an in-flight child launch
// from a permanent provider failure without authorizing another gate release.
func (engine *Engine) observeStartingAttempt(ctx context.Context, containerID domain.ContainerID) (AttemptObservation, error) {
	inventory, err := engine.containerInventory(ctx, containerID)
	if err != nil {
		return AttemptObservation{}, err
	}
	process, err := receiptByKind(inventory, ownership.KindInitProcess)
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
	return observation, nil
}

// checkpointStartingRecovery preserves an active Start and projects its
// consumed-gate, not-yet-running fact instead of failing daemon startup.
func (engine *Engine) checkpointStartingRecovery(ctx context.Context, active operation.Operation) error {
	progress, err := engine.lifecycle.GetOperationProgress(ctx, active.ID)
	if err != nil {
		return err
	}
	condition := domain.Condition{
		Type: domain.ConditionProcessIdentityUnknown, Reason: "StartInProgress",
		Message: "start gate is consumed but workload child readiness is not yet observable; retry the same operation ID",
	}
	_, err = engine.lifecycle.CheckpointOperation(ctx, lifecycle.CheckpointRequest{
		OperationID: active.ID, Target: active.Target, Fingerprint: active.Fingerprint,
		Stage: progress.Operation.Stage, RollbackCause: progress.RollbackCause, OOMBaseline: progress.OOMBaseline,
		KillEscalationDeadline: progress.KillEscalationDeadline,
		Rollback:               cloneRollback(progress.Rollback), Receipts: cloneReceipts(progress.Receipts),
		Releases: cloneReleases(progress.Releases), OccurredAt: engine.clock.Now(),
		Details: map[string]string{"recovery": "workload start remains in progress"}, UpsertCondition: &condition,
	})
	return err
}

// recoveryTargetKey creates a collision-free in-memory map key for one bounded target.
func recoveryTargetKey(target operation.Target) string {
	return string(target.Kind) + "\x00" + target.ID
}

// recoveryOperationID selects an engine-owned identity without weakening public
// tombstone semantics. It reuses only the current resource observation's exact
// retained binding; otherwise it probes deterministic IDs derived from the
// current checksummed resource epoch until it finds an identity never used by a
// full record or tombstone.
func (engine *Engine) recoveryOperationID(
	ctx context.Context,
	purpose string,
	operationType operation.Type,
	target operation.Target,
	fingerprint operation.RequestFingerprint,
) (operation.OperationID, error) {
	probeBinding := operation.Binding{
		ID: "internal-recovery-probe", Type: operationType,
		Target: target, Fingerprint: fingerprint,
	}
	if err := probeBinding.Validate(); err != nil {
		return "", fmt.Errorf("validate internal recovery binding: %w", err)
	}
	prefix := "reconcile-" + purpose + "-" + fingerprint.SHA256[:32] + "-"
	if err := operation.OperationID(prefix + "00000000000000000000000000000000").Validate(); err != nil {
		return "", fmt.Errorf("validate internal recovery identity prefix: %w", err)
	}
	epoch, err := engine.recoveryTargetEpoch(ctx, target)
	if err != nil {
		return "", err
	}
	lastID := operation.OperationID(epoch.LastObservation.OperationID)
	if strings.HasPrefix(string(lastID), prefix) {
		retained, getErr := engine.lifecycle.GetOperation(ctx, lastID)
		switch {
		case getErr == nil && retained.Type == operationType && retained.Target.Equal(target) && retained.Fingerprint.Equal(fingerprint):
			return lastID, nil
		case getErr == nil:
			// A caller may choose the internal-looking prefix; its different
			// binding remains immutable and is treated only as an occupied ID.
		case errors.Is(getErr, state.ErrOperationExpired):
			// Public retry remains expired; internal recovery allocates a new
			// epoch-derived identity for the still-current durable observation.
		case errors.Is(getErr, state.ErrNotFound):
			return "", fmt.Errorf("last recovery observation %q has no durable operation identity: %w", lastID, state.ErrInvariantViolation)
		default:
			return "", getErr
		}
	}
	for nonce := uint64(0); nonce <= uint64(state.DefaultOperationIdentityLimit); nonce++ {
		digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", epoch.StateSHA256, nonce)))
		candidate := operation.OperationID(prefix + hex.EncodeToString(digest[:16]))
		_, getErr := engine.lifecycle.GetOperation(ctx, candidate)
		if errors.Is(getErr, state.ErrNotFound) {
			return candidate, nil
		}
		if getErr == nil || errors.Is(getErr, state.ErrOperationExpired) {
			continue
		}
		return "", getErr
	}
	return "", fmt.Errorf("internal recovery identity probe space exhausted: %w", state.ErrRetentionCapacity)
}

// recoveryTargetEpoch hashes the complete current resource record, including
// CAS revision, execution incarnation, HostResources, and last observation, so
// a deleted/recreated identity cannot reuse an older internal recovery nonce.
func (engine *Engine) recoveryTargetEpoch(ctx context.Context, target operation.Target) (recoveryEpoch, error) {
	var epoch recoveryEpoch
	err := engine.store.View(ctx, func(reader state.Reader) error {
		var value any
		switch target.Kind {
		case operation.TargetSandbox:
			record, err := reader.GetSandbox(domain.SandboxID(target.ID))
			if err != nil {
				return err
			}
			epoch.LastObservation = record.Sandbox.Status.LastObservation
			value = record
		case operation.TargetContainer:
			record, err := reader.GetContainerAttempt(domain.ContainerID(target.ID))
			if err != nil {
				return err
			}
			epoch.LastObservation = record.ContainerAttempt.Attempt.LastObservation
			value = record
		default:
			return fmt.Errorf("internal recovery epoch does not support target kind %q", target.Kind)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("encode durable recovery epoch: %w", err)
		}
		digest := sha256.Sum256(encoded)
		epoch.StateSHA256 = hex.EncodeToString(digest[:])
		return nil
	})
	return epoch, err
}
