package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/lifecycle"
	"mydocker/internal/operation"
	"mydocker/internal/provider"
	"mydocker/internal/state"
)

// terminalWatchPlan captures whether one Running Attempt may be observed and,
// for an interrupted natural-stop intent, which durable operation must resume.
type terminalWatchPlan struct {
	Observe     bool
	OperationID operation.OperationID
}

// SynchronizeTerminals performs one read-observe-record pass for Running
// Attempts that are not currently owned by another lifecycle operation.
func (engine *Engine) SynchronizeTerminals(ctx context.Context) ([]operation.OperationID, error) {
	snapshot, err := engine.recoverySnapshot(ctx)
	if err != nil {
		return nil, err
	}
	recorded := make([]operation.OperationID, 0)
	for _, record := range snapshot.containers {
		pair := record.ContainerAttempt
		target := operation.Target{Kind: operation.TargetContainer, ID: string(pair.Container.ID)}
		releaseTarget := engine.targetLocks.lock(target)
		plan, eligibilityErr := engine.planTerminalWatch(ctx, pair.Container.ID)
		if eligibilityErr != nil {
			releaseTarget()
			return recorded, eligibilityErr
		}
		if !plan.Observe {
			releaseTarget()
			continue
		}
		operationStartedAt := engine.beginMeasurement()
		observationStartedAt := engine.beginMeasurement()
		observation, observeErr := engine.observeRetainedAttemptLocked(ctx, pair.Container.ID)
		observationMeasurement := engine.finishMeasurement(observationStartedAt)
		if observeErr != nil {
			releaseTarget()
			if provider.IsObservationUnavailable(observeErr) {
				continue
			}
			return recorded, observeErr
		}
		if !observation.Terminal {
			releaseTarget()
			continue
		}
		operationID := plan.OperationID
		if operationID == "" {
			request := engine.terminalRecordRequest("", pair.Container.ID, observation)
			fingerprint, fingerprintErr := request.RequestFingerprint()
			if fingerprintErr != nil {
				releaseTarget()
				return recorded, fingerprintErr
			}
			operationID, eligibilityErr = engine.recoveryOperationID(ctx, "terminal-watch", operation.TypeStop, target, fingerprint)
			if eligibilityErr != nil {
				releaseTarget()
				return recorded, eligibilityErr
			}
		}
		if _, err := engine.recordObservedTerminalLocked(ctx, operationID, pair.Container.ID, observation, observationMeasurement, operationStartedAt); err != nil {
			releaseTarget()
			return recorded, err
		}
		releaseTarget()
		recorded = append(recorded, operationID)
	}
	return recorded, nil
}

// planTerminalWatch refreshes phase and active-operation ownership while the
// caller holds the Container lock. It skips client mutations but resumes the
// engine-owned two-stage Stop left active by a transient observation outage.
func (engine *Engine) planTerminalWatch(ctx context.Context, containerID domain.ContainerID) (terminalWatchPlan, error) {
	plan := terminalWatchPlan{}
	err := engine.store.View(ctx, func(reader state.Reader) error {
		record, err := reader.GetContainerAttempt(containerID)
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if record.ContainerAttempt.Attempt.Phase != domain.AttemptRunning {
			return nil
		}
		target := operation.Target{Kind: operation.TargetContainer, ID: string(containerID)}
		operations, err := reader.ListOperations()
		if err != nil {
			return err
		}
		for _, candidate := range operations {
			if candidate.Operation.State.Active() && candidate.Operation.Target.Equal(target) {
				if candidate.Operation.Type == operation.TypeStop && !terminalStopWatchBlocked(record.ContainerAttempt.Attempt.Conditions) {
					plan = terminalWatchPlan{Observe: true, OperationID: candidate.Operation.ID}
				}
				return nil
			}
		}
		if terminalWatchBlockedByCondition(record.ContainerAttempt.Attempt.Conditions) {
			return nil
		}
		plan.Observe = true
		return nil
	})
	return plan, err
}

// terminalStopWatchBlocked prevents an active natural-stop retry when durable
// ownership itself is absent or unknown. Only the explicit transient
// supervisor-observation condition permits online re-observation.
func terminalStopWatchBlocked(conditions []domain.Condition) bool {
	for _, condition := range conditions {
		switch condition.Type {
		case domain.ConditionCleanupPending:
			return true
		case domain.ConditionProcessIdentityUnknown:
			if condition.Reason != "SupervisorObservationUnavailable" {
				return true
			}
		}
	}
	return false
}

// terminalWatchBlockedByCondition prevents a target already classified as
// unobservable or pending cleanup from turning one target outage into a
// daemon-wide watcher failure; recovery owns clearing these conditions.
func terminalWatchBlockedByCondition(conditions []domain.Condition) bool {
	for _, condition := range conditions {
		if condition.Type == domain.ConditionProcessIdentityUnknown || condition.Type == domain.ConditionCleanupPending {
			return true
		}
	}
	return false
}

// WatchTerminals keeps the daemon's Running projection synchronized with
// wrapper exit facts until cancellation, without using timing as evidence.
func (engine *Engine) WatchTerminals(ctx context.Context, interval time.Duration) error {
	if ctx == nil {
		return errors.New("terminal watcher context must not be nil")
	}
	if interval <= 0 {
		return errors.New("terminal watcher interval must be positive")
	}
	if ctx.Err() != nil {
		return nil
	}
	if _, err := engine.SynchronizeTerminals(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := engine.SynchronizeTerminals(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

// SynchronizeKillDeadlines advances only durable Kill intents that need an
// initial delivery checkpoint or whose absolute escalation deadline is due.
// Full recovery remains a startup barrier and is never replayed concurrently
// with public lifecycle mutations.
func (engine *Engine) SynchronizeKillDeadlines(ctx context.Context) ([]operation.OperationID, error) {
	var records []state.OperationRecord
	if err := engine.store.View(ctx, func(reader state.Reader) error {
		var err error
		records, err = reader.ListOperations()
		return err
	}); err != nil {
		return nil, err
	}
	reconciled := make([]operation.OperationID, 0)
	for _, record := range records {
		active := record.Operation
		if !active.State.Active() || active.Type != operation.TypeKill {
			continue
		}
		policy, err := killPolicyFromOperation(active)
		if err != nil {
			return reconciled, err
		}
		if active.Stage == operation.StagePersistIntent {
			if err := engine.checkpointInitialKillDuringRecovery(ctx, active, policy); err != nil {
				if provider.IsObservationUnavailable(err) {
					continue
				}
				return reconciled, err
			}
			reconciled = append(reconciled, active.ID)
			continue
		}
		if record.KillEscalationDeadline == nil {
			return reconciled, fmt.Errorf("active Kill %q is missing its durable escalation deadline", active.ID)
		}
		if engine.diagnosticNow().Before(*record.KillEscalationDeadline) {
			continue
		}
		if _, err := engine.KillContainer(ctx, lifecycle.KillRequest{
			OperationID: active.ID, ContainerID: domain.ContainerID(active.Target.ID), Policy: policy,
		}); err != nil {
			if provider.IsObservationUnavailable(err) {
				continue
			}
			terminal, terminalErr := engine.operationAlreadyTerminal(ctx, active.ID)
			if terminalErr != nil {
				return reconciled, errors.Join(err, terminalErr)
			}
			if terminal {
				reconciled = append(reconciled, active.ID)
				continue
			}
			return reconciled, err
		}
		reconciled = append(reconciled, active.ID)
	}
	return reconciled, nil
}

// operationAlreadyTerminal distinguishes a benign race with a completed
// same-ID client request from a watcher failure; an expired tombstone proves
// the operation became terminal even though its exact response was compacted.
func (engine *Engine) operationAlreadyTerminal(ctx context.Context, operationID operation.OperationID) (bool, error) {
	current, err := engine.lifecycle.GetOperation(ctx, operationID)
	if errors.Is(err, state.ErrOperationExpired) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return current.State.Terminal(), nil
}

// killPolicyFromOperation restores the immutable termination policy retained
// in an active Kill response without deriving it from mutable Container state.
func killPolicyFromOperation(active operation.Operation) (domain.TerminationPolicy, error) {
	var response struct {
		Plan domain.KillPlan `json:"plan"`
	}
	if err := json.Unmarshal(active.Response, &response); err != nil {
		return domain.TerminationPolicy{}, fmt.Errorf("decode active Kill %q plan: %w", active.ID, err)
	}
	policy := domain.TerminationPolicy{
		Signal: response.Plan.Signal, GracePeriod: response.Plan.GracePeriod,
		EscalationSignal: response.Plan.EscalationSignal,
	}
	if err := policy.Validate(); err != nil {
		return domain.TerminationPolicy{}, fmt.Errorf("validate active Kill %q policy: %w", active.ID, err)
	}
	return policy, nil
}

// WatchKillDeadlines periodically advances durable Kill deadlines after the
// startup barrier without applying a stale daemon-wide recovery snapshot to
// concurrent public lifecycle mutations.
func (engine *Engine) WatchKillDeadlines(ctx context.Context, interval time.Duration) error {
	if ctx == nil {
		return errors.New("Kill deadline watcher context must not be nil")
	}
	if interval <= 0 {
		return errors.New("Kill deadline watcher interval must be positive")
	}
	if ctx.Err() != nil {
		return nil
	}
	if _, err := engine.SynchronizeKillDeadlines(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := engine.SynchronizeKillDeadlines(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

// WatchRuntime runs terminal projection and deadline-driven Kill recovery
// together, canceling the sibling loop if either one reports a fatal error.
func (engine *Engine) WatchRuntime(ctx context.Context, interval time.Duration) error {
	if ctx == nil {
		return errors.New("runtime watcher context must not be nil")
	}
	watchContext, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	go func() {
		results <- engine.WatchTerminals(watchContext, interval)
	}()
	go func() {
		results <- engine.WatchKillDeadlines(watchContext, interval)
	}()
	first := <-results
	cancel()
	second := <-results
	if ctx.Err() != nil {
		return nil
	}
	if first == nil {
		first = errors.New("runtime lifecycle watcher stopped unexpectedly")
	}
	return errors.Join(first, second)
}
