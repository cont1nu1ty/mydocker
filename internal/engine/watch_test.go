package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/lifecycle"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/state"
)

// TestSynchronizeTerminalsRecordsNaturalExit verifies the daemon watcher
// converts a durable wrapper exit into one stopped lifecycle projection.
func TestSynchronizeTerminalsRecordsNaturalExit(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	prepareCreatedContainer(t, engine)
	if _, err := engine.StartContainer(context.Background(), lifecycle.ContainerActionRequest{
		OperationID: "op-start-before-watch", ContainerID: "container-start-test",
	}); err != nil {
		t.Fatalf("StartContainer() error = %v", err)
	}
	started := fixedClock{}.Now()
	finished := started.Add(time.Second)
	host.mu.Lock()
	host.attempt = AttemptObservation{
		Terminal: true, Evidence: "watch-terminal-evidence",
		Outcome: domain.ExitOutcome(0, domain.EvidenceFalse, started, finished, time.Second),
	}
	host.mu.Unlock()
	recorded, err := engine.SynchronizeTerminals(context.Background())
	if err != nil || len(recorded) != 1 {
		t.Fatalf("SynchronizeTerminals() = (%v, %v)", recorded, err)
	}
	pair, err := engine.Coordinator().GetContainer(context.Background(), "container-start-test")
	if err != nil || pair.Attempt.Phase != domain.AttemptStopped || pair.Attempt.Outcome.ExitCode == nil || *pair.Attempt.Outcome.ExitCode != 0 {
		t.Fatalf("GetContainer() = (%#v, %v)", pair, err)
	}
	replayed, err := engine.SynchronizeTerminals(context.Background())
	if err != nil || len(replayed) != 0 {
		t.Fatalf("SynchronizeTerminals(retry) = (%v, %v), want no new operation", replayed, err)
	}
}

// TestSynchronizeTerminalsCompleteDurationIncludesObservation verifies the
// watcher starts a new natural-stop span before reading the supervisor, so the
// complete duration is strictly wider than its nested observation stage.
func TestSynchronizeTerminalsCompleteDurationIncludesObservation(t *testing.T) {
	host := newFakeHost()
	store := state.NewMemoryStore()
	step := 3 * time.Millisecond
	clock := &scriptedClock{now: time.Date(2026, 8, 24, 13, 0, 0, 0, time.UTC), step: step}
	engine, err := NewWithClock(store, testProviders(t, host), clock)
	if err != nil {
		t.Fatalf("NewWithClock() error = %v", err)
	}
	prepareCreatedContainer(t, engine)
	if _, err := engine.StartContainer(context.Background(), lifecycle.ContainerActionRequest{
		OperationID: "op-start-before-measured-watch", ContainerID: "container-start-test",
	}); err != nil {
		t.Fatalf("StartContainer() error = %v", err)
	}
	started := time.Date(2026, 8, 24, 13, 30, 0, 0, time.UTC)
	host.mu.Lock()
	host.attempt = AttemptObservation{
		Terminal: true, Evidence: "measured-watch-terminal-evidence",
		Outcome: domain.ExitOutcome(0, domain.EvidenceFalse, started, started.Add(time.Second), time.Second),
	}
	host.mu.Unlock()
	recorded, err := engine.SynchronizeTerminals(context.Background())
	if err != nil || len(recorded) != 1 {
		t.Fatalf("SynchronizeTerminals() = (%v, %v), want one natural-stop operation", recorded, err)
	}
	var observationDuration operation.Duration
	var completeDuration operation.Duration
	var foundObservation bool
	var foundComplete bool
	if err := store.View(context.Background(), func(reader state.Reader) error {
		events, readErr := reader.EventsAfter(0, 0)
		if readErr != nil {
			return readErr
		}
		for _, event := range events {
			if event.OperationID != recorded[0] {
				continue
			}
			switch event.Stage {
			case operation.StageObserveProcess:
				if event.Duration == nil {
					t.Fatal("watcher observation duration is unavailable")
				}
				observationDuration = *event.Duration
				foundObservation = true
			case operation.StageComplete:
				if event.Duration == nil {
					t.Fatal("watcher complete duration is unavailable")
				}
				completeDuration = *event.Duration
				foundComplete = true
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("EventsAfter() error = %v", err)
	}
	if !foundObservation || observationDuration.Value() != step {
		t.Fatalf("observation duration = %v/found=%t, want %v", observationDuration.Value(), foundObservation, step)
	}
	if !foundComplete || completeDuration <= observationDuration {
		t.Fatalf("complete duration = %v/found=%t, want greater than observation %v", completeDuration.Value(), foundComplete, observationDuration.Value())
	}
}

// TestWatchTerminalsStopsOnCancellation verifies daemon shutdown does not leak a polling goroutine.
func TestWatchTerminalsStopsOnCancellation(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := engine.WatchTerminals(ctx, time.Millisecond); err != nil {
		t.Fatalf("WatchTerminals(canceled) error = %v", err)
	}
}

// TestWatchRuntimeStopsBothLoopsOnCancellation verifies the production
// background supervisor treats daemon shutdown as a normal terminal condition.
func TestWatchRuntimeStopsBothLoopsOnCancellation(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := engine.WatchRuntime(ctx, time.Millisecond); err != nil {
		t.Fatalf("WatchRuntime(canceled) error = %v", err)
	}
}

// TestOperationAlreadyTerminalAcceptsExpiredTombstone verifies a deadline
// watcher snapshot cannot turn concurrent terminal compaction into a fatal
// daemon error after the same operation leaves the exact replay window.
func TestOperationAlreadyTerminalAcceptsExpiredTombstone(t *testing.T) {
	host := newFakeHost()
	store, err := state.NewMemoryStoreWithRetention(state.RetentionPolicy{
		TerminalOperationLimit: 1, OperationIdentityLimit: 8, EventLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewWithClock(store, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []lifecycle.SandboxActionRequest{
		{OperationID: "op-expired-before-deadline-reread", SandboxID: "sandbox-expired-before-deadline-reread"},
		{OperationID: "op-newer-terminal", SandboxID: "sandbox-newer-terminal"},
	} {
		if _, err := engine.RemoveSandbox(context.Background(), request); err != nil {
			t.Fatalf("RemoveSandbox(%q) error = %v", request.OperationID, err)
		}
	}
	terminal, err := engine.operationAlreadyTerminal(context.Background(), "op-expired-before-deadline-reread")
	if err != nil || !terminal {
		t.Fatalf("operationAlreadyTerminal(expired) = (%t,%v), want true,nil", terminal, err)
	}
}

// TestSynchronizeTerminalsSkipsActiveOperation verifies the watcher cannot
// race a client-owned lifecycle mutation on the same Container.
func TestSynchronizeTerminalsSkipsActiveOperation(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	prepareCreatedContainer(t, engine)
	if _, err := engine.StartContainer(context.Background(), lifecycle.ContainerActionRequest{
		OperationID: "op-start-before-active-watch", ContainerID: "container-start-test",
	}); err != nil {
		t.Fatalf("StartContainer() error = %v", err)
	}
	if _, err := engine.Coordinator().PlanKill(context.Background(), lifecycle.KillRequest{
		OperationID: "op-active-watch-kill", ContainerID: "container-start-test",
		Policy: domain.TerminationPolicy{Signal: "SIGTERM", GracePeriod: time.Second, EscalationSignal: "SIGKILL"},
	}); err != nil {
		t.Fatalf("PlanKill() error = %v", err)
	}
	host.mu.Lock()
	host.attempt = AttemptObservation{Terminal: true, Evidence: "terminal-hidden-by-active-operation", Outcome: domain.NotApplicableOutcome()}
	host.mu.Unlock()
	recorded, err := engine.SynchronizeTerminals(context.Background())
	if err != nil || len(recorded) != 0 {
		t.Fatalf("SynchronizeTerminals() = (%v, %v), want active-operation skip", recorded, err)
	}
	pair, err := engine.Coordinator().GetContainer(context.Background(), "container-start-test")
	if err != nil || pair.Attempt.Phase != domain.AttemptRunning {
		t.Fatalf("GetContainer() = (%#v, %v), want Running", pair, err)
	}
}

// TestSynchronizeTerminalsRetriesUnavailableObservation verifies one temporary
// wrapper transport outage does not terminate the daemon watcher.
func TestSynchronizeTerminalsRetriesUnavailableObservation(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	prepareCreatedContainer(t, engine)
	if _, err := engine.StartContainer(context.Background(), lifecycle.ContainerActionRequest{
		OperationID: "op-start-before-unavailable-watch", ContainerID: "container-start-test",
	}); err != nil {
		t.Fatalf("StartContainer() error = %v", err)
	}
	host.mu.Lock()
	host.observeUnavailable = true
	host.mu.Unlock()
	recorded, err := engine.SynchronizeTerminals(context.Background())
	if err != nil || len(recorded) != 0 {
		t.Fatalf("SynchronizeTerminals() = (%v, %v), want retryable skip", recorded, err)
	}
}

// TestSynchronizeTerminalsResumesInterruptedNaturalStop verifies a startup
// observation outage leaves the engine-owned Stop active and the online
// watcher later completes that exact operation instead of waiting for restart.
func TestSynchronizeTerminalsResumesInterruptedNaturalStop(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	createRunningEngineContainer(t, engine, "watch-resume-stop")
	startedAt := fixedClock{}.Now()
	observation := AttemptObservation{
		Terminal: true, Evidence: "watch-resume-stop-evidence",
		Outcome: domain.ExitOutcome(17, domain.EvidenceFalse, startedAt, startedAt.Add(time.Second), time.Second),
	}
	host.mu.Lock()
	host.attempt = observation
	host.mu.Unlock()
	containerID := domain.ContainerID("container-watch-resume-stop")
	operationID := operation.OperationID("op-watch-resume-stop")
	if _, err := engine.Coordinator().BeginRecordStopped(context.Background(), lifecycle.RecordStoppedRequest{
		OperationID: operationID, ContainerID: containerID, Outcome: observation.Outcome,
		Conditions: terminalConditions(observation.Outcome),
		Verification: lifecycle.Verification{
			Kind: lifecycle.VerificationAttemptStopped, Verified: true,
			Evidence: observation.Evidence, ObservedAt: fixedClock{}.Now(),
		},
	}); err != nil {
		t.Fatalf("BeginRecordStopped() error = %v", err)
	}
	host.mu.Lock()
	host.observeUnavailable = true
	host.mu.Unlock()
	if _, err := engine.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile(unavailable) error = %v", err)
	}
	pending, err := engine.Coordinator().GetContainer(context.Background(), containerID)
	if err != nil || !hasEngineConditionReason(pending.Attempt.Conditions, domain.ConditionProcessIdentityUnknown, "SupervisorObservationUnavailable") {
		t.Fatalf("GetContainer(unavailable) = (%#v,%v), want retryable supervisor condition", pending, err)
	}
	host.mu.Lock()
	host.observeUnavailable = false
	host.mu.Unlock()
	recorded, err := engine.SynchronizeTerminals(context.Background())
	if err != nil || len(recorded) != 1 || recorded[0] != operationID {
		t.Fatalf("SynchronizeTerminals() = (%v,%v), want resumed %q", recorded, err, operationID)
	}
	completed, err := engine.Coordinator().GetOperation(context.Background(), operationID)
	if err != nil || completed.State != operation.StateSucceeded {
		t.Fatalf("GetOperation() = (%#v,%v), want succeeded", completed, err)
	}
	pair, err := engine.Coordinator().GetContainer(context.Background(), containerID)
	if err != nil || pair.Attempt.Phase != domain.AttemptStopped || hasEngineCondition(pair.Attempt.Conditions, domain.ConditionProcessIdentityUnknown) {
		t.Fatalf("GetContainer() = (%#v,%v), want stopped without recovery condition", pair, err)
	}
}

// TestSynchronizeTerminalsBlocksNaturalStopWithoutOwnedProcess verifies an
// absent init receipt remains fail-closed: the online watcher neither contacts
// an untrusted replacement wrapper nor turns that target into daemon failure.
func TestSynchronizeTerminalsBlocksNaturalStopWithoutOwnedProcess(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	createRunningEngineContainer(t, engine, "watch-block-stop")
	startedAt := fixedClock{}.Now()
	observation := AttemptObservation{
		Terminal: true, Evidence: "watch-block-stop-evidence",
		Outcome: domain.ExitOutcome(19, domain.EvidenceFalse, startedAt, startedAt.Add(time.Second), time.Second),
	}
	host.mu.Lock()
	host.attempt = observation
	host.mu.Unlock()
	containerID := domain.ContainerID("container-watch-block-stop")
	operationID := operation.OperationID("op-watch-block-stop")
	if _, err := engine.Coordinator().BeginRecordStopped(context.Background(), lifecycle.RecordStoppedRequest{
		OperationID: operationID, ContainerID: containerID, Outcome: observation.Outcome,
		Conditions: terminalConditions(observation.Outcome),
		Verification: lifecycle.Verification{
			Kind: lifecycle.VerificationAttemptStopped, Verified: true,
			Evidence: observation.Evidence, ObservedAt: fixedClock{}.Now(),
		},
	}); err != nil {
		t.Fatalf("BeginRecordStopped() error = %v", err)
	}
	host.mu.Lock()
	host.present[ownership.KindInitProcess] = false
	host.mu.Unlock()
	if _, err := engine.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile(absent init) error = %v", err)
	}
	pending, err := engine.Coordinator().GetContainer(context.Background(), containerID)
	if err != nil || !hasEngineConditionReason(pending.Attempt.Conditions, domain.ConditionProcessIdentityUnknown, "DaemonRecoveryEvidenceMissing") {
		t.Fatalf("GetContainer(absent init) = (%#v,%v), want blocking identity condition", pending, err)
	}
	host.mu.Lock()
	host.observeErr = errors.New("wrapper socket is absent")
	host.mu.Unlock()
	recorded, err := engine.SynchronizeTerminals(context.Background())
	if err != nil || len(recorded) != 0 {
		t.Fatalf("SynchronizeTerminals(absent init) = (%v,%v), want safe skip", recorded, err)
	}
	active, err := engine.Coordinator().GetOperation(context.Background(), operationID)
	if err != nil || !active.State.Active() {
		t.Fatalf("GetOperation(absent init) = (%#v,%v), want active", active, err)
	}
}

// TestSynchronizeTerminalsSerializesConcurrentKill verifies a terminal watcher
// and client Kill cannot turn a benign superseded observation into daemon failure.
func TestSynchronizeTerminalsSerializesConcurrentKill(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	prepareCreatedContainer(t, engine)
	if _, err := engine.StartContainer(context.Background(), lifecycle.ContainerActionRequest{
		OperationID: "op-start-before-watch-race", ContainerID: "container-start-test",
	}); err != nil {
		t.Fatalf("StartContainer() error = %v", err)
	}
	started := fixedClock{}.Now()
	host.mu.Lock()
	host.attempt = AttemptObservation{
		Terminal: true, Evidence: "terminal-watch-race",
		Outcome: domain.ExitOutcome(0, domain.EvidenceFalse, started, started.Add(time.Second), time.Second),
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	host.observeEntered = entered
	host.observeRelease = release
	host.mu.Unlock()
	watchResult := make(chan error, 1)
	go func() {
		_, err := engine.SynchronizeTerminals(context.Background())
		watchResult <- err
	}()
	<-entered
	killResult := make(chan error, 1)
	go func() {
		_, err := engine.KillContainer(context.Background(), lifecycle.KillRequest{
			OperationID: "op-kill-during-watch", ContainerID: "container-start-test",
			Policy: domain.TerminationPolicy{Signal: "SIGTERM", GracePeriod: time.Second, EscalationSignal: "SIGKILL"},
		})
		killResult <- err
	}()
	close(release)
	if err := <-watchResult; err != nil {
		t.Fatalf("SynchronizeTerminals() error = %v", err)
	}
	if err := <-killResult; err != nil {
		t.Fatalf("KillContainer(after watcher) error = %v", err)
	}
	host.mu.Lock()
	host.observeEntered = nil
	host.observeRelease = nil
	host.mu.Unlock()
	pair, err := engine.Coordinator().GetContainer(context.Background(), "container-start-test")
	if err != nil || pair.Attempt.Phase != domain.AttemptStopped {
		t.Fatalf("GetContainer() = (%#v, %v), want Stopped", pair, err)
	}
}

// hasEngineConditionReason reports whether one test projection retains the
// exact bounded condition type/reason pair used to authorize watcher retry.
func hasEngineConditionReason(conditions []domain.Condition, conditionType, reason string) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType && condition.Reason == reason {
			return true
		}
	}
	return false
}
