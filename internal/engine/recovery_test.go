package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/lifecycle"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
	"mydocker/internal/state"
)

// TestDiscoverFailureLeavesStateAndRuntimeRegistryUntouched verifies the daemon-wide read barrier fails before publishing identity or authorizing mutation.
func TestDiscoverFailureLeavesStateAndRuntimeRegistryUntouched(t *testing.T) {
	host := newFakeHost()
	first, store := testEngine(t, host)
	_, err := first.CreateSandbox(context.Background(), lifecycle.SandboxCreateRequest{
		OperationID: "op-discovery-barrier-create", SandboxID: "sandbox-discovery-barrier",
		Spec: domain.SandboxSpec{Network: domain.NetworkIntent{Mode: "none"}},
	})
	if err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	beforeEvents := engineEventCount(t, store)
	host.mu.Lock()
	beforeCreated := cloneKindCounts(host.created)
	host.failInspectKind = ownership.KindKeeperProcess
	host.mu.Unlock()
	restarted, err := NewWithClock(store, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatalf("NewWithClock(restarted) error = %v", err)
	}
	if _, err := restarted.Discover(context.Background()); err == nil {
		t.Fatal("Discover() error = nil, want injected read-only failure")
	}
	if len(restarted.identities) != 0 {
		t.Fatalf("identity registry published after failed barrier: %#v", restarted.identities)
	}
	if got := engineEventCount(t, store); got != beforeEvents {
		t.Fatalf("event count after failed discovery = %d, want %d", got, beforeEvents)
	}
	host.mu.Lock()
	afterCreated := cloneKindCounts(host.created)
	host.mu.Unlock()
	if !reflect.DeepEqual(afterCreated, beforeCreated) {
		t.Fatalf("provider acquisition counts changed during discovery: got %v want %v", afterCreated, beforeCreated)
	}
}

// TestStaleIdentityDiscoveryCannotOverwriteConcurrentRegistration verifies a
// slow recovery publication yields to a process identity registered after its snapshot.
func TestStaleIdentityDiscoveryCannotOverwriteConcurrentRegistration(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	prepareCreatedContainer(t, engine)
	revision := engine.identityRevision()
	engine.identityMu.RLock()
	stale := make(map[string]ownership.Receipt, len(engine.identities))
	var concurrent ownership.Receipt
	for handle, receipt := range engine.identities {
		stale[handle] = receipt.Clone()
		concurrent = receipt.Clone()
	}
	engine.identityMu.RUnlock()
	concurrent.LocalID += "-concurrent"
	concurrent.EvidenceSHA256 = strings.Repeat("a", 64)
	if err := engine.registerProcessIdentity(concurrent); err != nil {
		t.Fatalf("registerProcessIdentity(concurrent) error = %v", err)
	}
	if engine.publishDiscoveredIdentities(revision, stale) {
		t.Fatal("stale discovery unexpectedly replaced a concurrently changed identity registry")
	}
	engine.identityMu.RLock()
	_, found := engine.identities[processIdentityHandle(concurrent)]
	engine.identityMu.RUnlock()
	if !found {
		t.Fatal("stale discovery erased the concurrently registered process identity")
	}
}

// TestSynchronizeKillDeadlinesDoesNotRunFullRecovery verifies the online
// deadline loop never applies provider observations from a daemon-wide stale snapshot.
func TestSynchronizeKillDeadlinesDoesNotRunFullRecovery(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	if _, err := engine.CreateSandbox(context.Background(), lifecycle.SandboxCreateRequest{
		OperationID: "op-deadline-loop-sandbox", SandboxID: "sandbox-deadline-loop",
		Spec: domain.SandboxSpec{Network: domain.NetworkIntent{Mode: "none"}},
	}); err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	host.mu.Lock()
	host.failInspectKind = ownership.KindSandboxCgroup
	host.mu.Unlock()
	reconciled, err := engine.SynchronizeKillDeadlines(context.Background())
	if err != nil || len(reconciled) != 0 {
		t.Fatalf("SynchronizeKillDeadlines() = (%v, %v), want no full recovery or Kill work", reconciled, err)
	}
}

// TestReconcileResumesResponseLossAfterDaemonRestart verifies an uncheckpointed deterministic side effect is rediscovered through Ensure only after full discovery.
func TestReconcileResumesResponseLossAfterDaemonRestart(t *testing.T) {
	host := newFakeHost()
	host.failAfterEnsureKind = ownership.KindKeeperProcess
	first, store := testEngine(t, host)
	request := lifecycle.SandboxCreateRequest{
		OperationID: "op-restart-resume", SandboxID: "sandbox-restart-resume",
		Spec: domain.SandboxSpec{Network: domain.NetworkIntent{Mode: "none"}},
	}
	if _, err := first.CreateSandbox(context.Background(), request); err == nil {
		t.Fatal("CreateSandbox() error = nil, want injected response loss")
	}
	restarted, err := NewWithClock(store, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatalf("NewWithClock(restarted) error = %v", err)
	}
	report, err := restarted.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !reflect.DeepEqual(report.Reconciled, []operation.OperationID{request.OperationID}) {
		t.Fatalf("Reconcile().Reconciled = %v", report.Reconciled)
	}
	sandbox, err := restarted.Coordinator().GetSandbox(context.Background(), request.SandboxID)
	if err != nil || sandbox.Status.Phase != domain.SandboxReady {
		t.Fatalf("GetSandbox() = (%#v, %v), want Ready", sandbox, err)
	}
	host.mu.Lock()
	created := host.created[ownership.KindKeeperProcess]
	host.mu.Unlock()
	if created != 1 {
		t.Fatalf("keeper physical creation count = %d, want 1", created)
	}
}

// TestReconcileMarksRunningIdentityUnknownExactlyOnce verifies M3 restart preserves Running phase while making its reconnect limitation visible and replayable.
func TestReconcileMarksRunningIdentityUnknownExactlyOnce(t *testing.T) {
	host := newFakeHost()
	first, store := testEngine(t, host)
	createRunningEngineContainer(t, first, "recovery-running")
	restarted, err := NewWithClock(store, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatalf("NewWithClock(restarted) error = %v", err)
	}
	firstReport, err := restarted.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(firstReport.Recorded) != 1 {
		t.Fatalf("Reconcile().Recorded = %v, want one recovery condition", firstReport.Recorded)
	}
	pair, err := restarted.Coordinator().GetContainer(context.Background(), "container-recovery-running")
	if err != nil {
		t.Fatalf("GetContainer() error = %v", err)
	}
	if pair.Attempt.Phase != domain.AttemptRunning || !hasEngineCondition(pair.Attempt.Conditions, domain.ConditionProcessIdentityUnknown) {
		t.Fatalf("reconciled Attempt status = %#v", pair.Attempt)
	}
	if pair.Attempt.ProcessIdentity == nil {
		t.Fatal("reconciled Running Attempt lost its persisted strong identity projection")
	}
	if err := restarted.Verify(context.Background(), operation.Target{Kind: operation.TargetContainer, ID: string(pair.Container.ID)}, *pair.Attempt.ProcessIdentity); err != nil {
		t.Fatalf("Verify(rediscovered identity) error = %v", err)
	}
	beforeEvents := engineEventCount(t, store)
	secondReport, err := restarted.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile(replay) error = %v", err)
	}
	if !reflect.DeepEqual(secondReport.Recorded, firstReport.Recorded) {
		t.Fatalf("Reconcile(replay).Recorded = %v, want %v", secondReport.Recorded, firstReport.Recorded)
	}
	if got := engineEventCount(t, store); got != beforeEvents {
		t.Fatalf("event count after exact recovery replay = %d, want %d", got, beforeEvents)
	}
}

// TestReconcileRecordsMissingRunningProcessEvidence verifies an absent durable init receipt cannot leave a Running Attempt silently authoritative.
func TestReconcileRecordsMissingRunningProcessEvidence(t *testing.T) {
	host := newFakeHost()
	first, store := testEngine(t, host)
	createRunningEngineContainer(t, first, "recovery-missing-init")
	host.mu.Lock()
	host.present[ownership.KindInitProcess] = false
	host.mu.Unlock()
	restarted, err := NewWithClock(store, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatalf("NewWithClock(restarted) error = %v", err)
	}
	firstReport, err := restarted.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(firstReport.Plan.Issues) == 0 || len(firstReport.Recorded) != 1 {
		t.Fatalf("Reconcile() issues/recorded = %#v/%v, want visible missing evidence", firstReport.Plan.Issues, firstReport.Recorded)
	}
	pair, err := restarted.Coordinator().GetContainer(context.Background(), "container-recovery-missing-init")
	if err != nil {
		t.Fatalf("GetContainer() error = %v", err)
	}
	if pair.Attempt.Phase != domain.AttemptRunning || !hasEngineCondition(pair.Attempt.Conditions, domain.ConditionProcessIdentityUnknown) {
		t.Fatalf("Attempt after missing process discovery = %#v", pair.Attempt)
	}
	beforeEvents := engineEventCount(t, store)
	secondReport, err := restarted.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile(retry) error = %v", err)
	}
	if !reflect.DeepEqual(secondReport.Recorded, firstReport.Recorded) || engineEventCount(t, store) != beforeEvents {
		t.Fatalf("Reconcile(retry) recorded/events = %v/%d, want replay %v/%d", secondReport.Recorded, engineEventCount(t, store), firstReport.Recorded, beforeEvents)
	}
}

// TestTerminalWatcherSkipsRecoveryUnknownAttempt verifies a target already
// marked unobservable cannot make the daemon-wide terminal watcher fatal on
// the next ordinary supervisor error.
func TestTerminalWatcherSkipsRecoveryUnknownAttempt(t *testing.T) {
	host := newFakeHost()
	first, store := testEngine(t, host)
	createRunningEngineContainer(t, first, "watcher-unknown-init")
	host.mu.Lock()
	host.present[ownership.KindInitProcess] = false
	host.mu.Unlock()
	restarted, err := NewWithClock(store, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	host.mu.Lock()
	host.observeErr = errors.New("wrapper socket is absent")
	host.mu.Unlock()
	recorded, err := restarted.SynchronizeTerminals(context.Background())
	if err != nil || len(recorded) != 0 {
		t.Fatalf("SynchronizeTerminals() = (%v,%v), want skipped unknown Attempt", recorded, err)
	}
}

// TestReconcileRecordsTemporarySupervisorOutageWithoutFailingDaemon verifies
// unavailable read-only control evidence becomes an Unknown condition while
// preserving the authoritative Running phase for later retry.
func TestReconcileRecordsTemporarySupervisorOutageWithoutFailingDaemon(t *testing.T) {
	host := newFakeHost()
	first, store := testEngine(t, host)
	createRunningEngineContainer(t, first, "recovery-temporary-outage")
	host.mu.Lock()
	host.observeUnavailable = true
	host.mu.Unlock()
	restarted, err := NewWithClock(store, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatalf("NewWithClock(restarted) error = %v", err)
	}
	report, err := restarted.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want non-fatal Unknown projection", err)
	}
	if len(report.Plan.Issues) == 0 || len(report.Recorded) != 1 {
		t.Fatalf("Reconcile() issues/recorded = %#v/%v", report.Plan.Issues, report.Recorded)
	}
	pair, err := restarted.Coordinator().GetContainer(context.Background(), "container-recovery-temporary-outage")
	if err != nil {
		t.Fatalf("GetContainer() error = %v", err)
	}
	if pair.Attempt.Phase != domain.AttemptRunning || !hasEngineCondition(pair.Attempt.Conditions, domain.ConditionProcessIdentityUnknown) {
		t.Fatalf("Attempt after temporary outage = %#v", pair.Attempt)
	}
}

// TestReconcileAllocatesFreshInternalIdentityAfterRetirement verifies both
// resource-issue and identity-unknown observations replay while retained, keep
// public tombstone reuse forbidden, and receive a new durable-state epoch ID
// after small-policy compaction and FileStore reopen.
func TestReconcileAllocatesFreshInternalIdentityAfterRetirement(t *testing.T) {
	tests := []struct {
		name               string
		suffix             string
		observeUnavailable bool
	}{
		{name: "resource issue", suffix: "retired-resource-issue", observeUnavailable: true},
		{name: "identity unknown", suffix: "retired-identity-unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			host := newFakeHost()
			path := filepath.Join(secureEngineStateDirectory(t), "state.json")
			policy := state.RetentionPolicy{TerminalOperationLimit: 1, OperationIdentityLimit: 32, EventLimit: 128}
			store, err := state.NewFileStoreWithRetention(path, policy)
			if err != nil {
				t.Fatal(err)
			}
			first, err := NewWithClock(store, testProviders(t, host), fixedClock{})
			if err != nil {
				t.Fatal(err)
			}
			createRunningEngineContainer(t, first, test.suffix)
			host.mu.Lock()
			host.observeUnavailable = test.observeUnavailable
			host.mu.Unlock()

			initial, err := first.Reconcile(ctx)
			if err != nil || len(initial.Recorded) != 1 {
				t.Fatalf("Reconcile(initial) = (%#v, %v), want one internal operation", initial, err)
			}
			if test.observeUnavailable {
				if len(initial.Plan.Issues) != 1 || initial.Plan.Issues[0].Receipt == nil ||
					initial.Plan.Issues[0].AttemptID != domain.AttemptID("attempt-"+test.suffix) ||
					initial.Plan.Issues[0].Receipt.Owner.OperationID != operation.OperationID("op-container-"+test.suffix) ||
					initial.Plan.Issues[0].Receipt.Owner.Token == "" || initial.Plan.Issues[0].Receipt.LocalID == "" ||
					initial.Plan.Issues[0].Receipt.EvidenceSHA256 == "" {
					t.Fatalf("Reconcile(initial) issue lacks exact owner/Attempt evidence: %#v", initial.Plan.Issues)
				}
			}
			retiredID := initial.Recorded[0]
			replayed, err := first.Reconcile(ctx)
			if err != nil || len(replayed.Recorded) != 1 || replayed.Recorded[0] != retiredID {
				t.Fatalf("Reconcile(retained replay) = (%#v, %v), want %q", replayed, err, retiredID)
			}

			containerID := domain.ContainerID("container-" + test.suffix)
			for index := 1; index <= 2; index++ {
				operationID := operation.OperationID(fmt.Sprintf("op-recovery-retention-churn-%s-%d", test.suffix, index))
				if _, err := first.StartContainer(ctx, lifecycle.ContainerActionRequest{OperationID: operationID, ContainerID: containerID}); err != nil {
					t.Fatalf("StartContainer(retention churn %d) error = %v", index, err)
				}
			}
			if _, err := first.Coordinator().GetOperation(ctx, retiredID); !errors.Is(err, state.ErrOperationExpired) {
				t.Fatalf("GetOperation(retired recovery ID) error = %v, want ErrOperationExpired", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := state.NewFileStoreWithRetention(path, policy)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Errorf("FileStore.Close() error = %v", err)
				}
			})
			restarted, err := NewWithClock(reopened, testProviders(t, host), fixedClock{})
			if err != nil {
				t.Fatal(err)
			}

			publicRetry := recoveryConditionRequestForTest(t, initial, retiredID)
			if _, err := restarted.lifecycle.ReconcileCondition(ctx, publicRetry); !errors.Is(err, state.ErrOperationExpired) {
				t.Fatalf("ReconcileCondition(public retired ID reuse) error = %v, want ErrOperationExpired", err)
			}
			continued, err := restarted.Reconcile(ctx)
			if err != nil || len(continued.Recorded) != 1 {
				t.Fatalf("Reconcile(after tombstone reopen) = (%#v, %v), want one fresh operation", continued, err)
			}
			if continued.Recorded[0] == retiredID {
				t.Fatalf("Reconcile(after tombstone reopen) reused retired ID %q", retiredID)
			}
		})
	}
}

// recoveryConditionRequestForTest reconstructs the exact internal condition
// decision from a discovery report so a test can prove public reuse of its
// retired operation ID remains fail-closed.
func recoveryConditionRequestForTest(t *testing.T, report RecoveryReport, operationID operation.OperationID) lifecycle.ReconcileConditionRequest {
	t.Helper()
	if len(report.Plan.Issues) != 0 {
		target := report.Plan.Issues[0].Target
		return lifecycle.ReconcileConditionRequest{
			OperationID: operationID, Target: target,
			Condition: conditionPointer(recoveryIssueCondition(target, report.Plan.Issues)),
			Evidence:  recoveryIssueEvidence(report.Plan.Issues), ObservedAt: fixedClock{}.Now(),
		}
	}
	if len(report.Plan.Running) != 1 {
		t.Fatalf("recovery report has issues/running = %d/%d, want one reconstructable decision", len(report.Plan.Issues), len(report.Plan.Running))
	}
	running := report.Plan.Running[0]
	target := operation.Target{Kind: operation.TargetContainer, ID: string(running.ContainerID)}
	return lifecycle.ReconcileConditionRequest{
		OperationID: operationID, Target: target,
		Condition: &domain.Condition{
			Type: domain.ConditionProcessIdentityUnknown, Reason: "DaemonRestart",
			Message: "workload state was observed read-only, but M3 does not claim a reconnectable supervisor channel",
		},
		Evidence:   "M3 restart observed the retained init wrapper but supervisor reconnection is deferred to M5: " + running.Observation.Evidence,
		ObservedAt: fixedClock{}.Now(),
	}
}

// TestReconcileSeparatesRecreatedContainerAttemptIdentity verifies an
// unchanged public Container ID and identical running observation cannot replay
// the old incarnation's internal condition response after verified deletion
// and recreation with a different durable Attempt and create owner.
func TestReconcileSeparatesRecreatedContainerAttemptIdentity(t *testing.T) {
	ctx := context.Background()
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	sandboxID := domain.SandboxID("sandbox-recovery-incarnation")
	containerID := domain.ContainerID("container-recovery-incarnation")
	if _, err := engine.CreateSandbox(ctx, lifecycle.SandboxCreateRequest{
		OperationID: "op-sandbox-recovery-incarnation", SandboxID: sandboxID,
		Spec: domain.SandboxSpec{Network: domain.NetworkIntent{Mode: "none"}},
	}); err != nil {
		t.Fatal(err)
	}
	firstAttemptID := domain.AttemptID("attempt-recovery-incarnation-1")
	if _, err := engine.CreateContainer(ctx, lifecycle.ContainerCreateRequest{
		OperationID: "op-container-recovery-incarnation-1", SandboxID: sandboxID,
		ContainerID: containerID, AttemptID: firstAttemptID,
		Process: domain.ProcessSpec{Argv: []string{"/bin/workload"}}, RootFS: "prepared-rootfs-recovery-incarnation-1",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.StartContainer(ctx, lifecycle.ContainerActionRequest{
		OperationID: "op-start-recovery-incarnation-1", ContainerID: containerID,
	}); err != nil {
		t.Fatal(err)
	}
	firstReport, err := engine.Reconcile(ctx)
	if err != nil || len(firstReport.Recorded) != 1 {
		t.Fatalf("Reconcile(first incarnation) = (%#v, %v), want one condition operation", firstReport, err)
	}
	firstRecoveryID := firstReport.Recorded[0]

	startedAt := fixedClock{}.Now()
	finishedAt := startedAt.Add(time.Second)
	host.mu.Lock()
	host.attempt = AttemptObservation{
		Terminal: true, Evidence: "recovery-incarnation-terminal",
		Outcome: domain.ExitOutcome(0, domain.EvidenceFalse, startedAt, finishedAt, time.Second),
	}
	host.mu.Unlock()
	if _, err := engine.RecordTerminal(ctx, "op-terminal-recovery-incarnation-1", containerID); err != nil {
		t.Fatal(err)
	}
	if result, err := engine.DeleteContainer(ctx, lifecycle.ContainerActionRequest{
		OperationID: "op-delete-recovery-incarnation-1", ContainerID: containerID,
	}); err != nil || !result.Removed {
		t.Fatalf("DeleteContainer(first incarnation) = (%#v, %v), want removed", result, err)
	}
	host.mu.Lock()
	host.attempt = AttemptObservation{Prepared: true, Evidence: "prepared-evidence"}
	host.mu.Unlock()

	secondAttemptID := domain.AttemptID("attempt-recovery-incarnation-2")
	secondCreate, err := engine.CreateContainer(ctx, lifecycle.ContainerCreateRequest{
		OperationID: "op-container-recovery-incarnation-2", SandboxID: sandboxID,
		ContainerID: containerID, AttemptID: secondAttemptID,
		Process: domain.ProcessSpec{Argv: []string{"/bin/workload"}}, RootFS: "prepared-rootfs-recovery-incarnation-2",
	})
	if err != nil {
		t.Fatalf("CreateContainer(second incarnation) = (%#v, %v)", secondCreate, err)
	}
	if _, err := engine.StartContainer(ctx, lifecycle.ContainerActionRequest{
		OperationID: "op-start-recovery-incarnation-2", ContainerID: containerID,
	}); err != nil {
		t.Fatal(err)
	}
	secondReport, err := engine.Reconcile(ctx)
	if err != nil || len(secondReport.Recorded) != 1 {
		t.Fatalf("Reconcile(second incarnation) = (%#v, %v), want one condition operation", secondReport, err)
	}
	if secondReport.Recorded[0] == firstRecoveryID {
		t.Fatalf("second incarnation reused internal operation %q", firstRecoveryID)
	}
	current, err := engine.Coordinator().GetContainer(ctx, containerID)
	if err != nil || current.Attempt.ID != secondAttemptID || current.Attempt.LastObservation.OperationID != string(secondReport.Recorded[0]) {
		t.Fatalf("GetContainer(second incarnation) = (%#v, %v), want Attempt %q observed by %q", current, err, secondAttemptID, secondReport.Recorded[0])
	}

	oldRequest := recoveryConditionRequestForTest(t, secondReport, firstRecoveryID)
	oldReplay, err := engine.lifecycle.ReconcileCondition(ctx, oldRequest)
	if err != nil || oldReplay.ContainerAttempt == nil || oldReplay.ContainerAttempt.Attempt.ID != firstAttemptID {
		t.Fatalf("ReconcileCondition(old exact identity) = (%#v, %v), want immutable first Attempt %q response", oldReplay, err, firstAttemptID)
	}
}

// TestRecordTerminalRetainsCreateOOMBaselineAcrossCompaction verifies live
// HostResources pin their exact create owner within the hard identity budget,
// allowing a FileStore-reopened Running Attempt to attribute terminal OOM after
// older ordinary replay records have already become tombstones.
func TestRecordTerminalRetainsCreateOOMBaselineAcrossCompaction(t *testing.T) {
	ctx := context.Background()
	host := newFakeHost()
	path := filepath.Join(secureEngineStateDirectory(t), "state.json")
	policy := state.RetentionPolicy{TerminalOperationLimit: 1, OperationIdentityLimit: 16, EventLimit: 128}
	store, err := state.NewFileStoreWithRetention(path, policy)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewWithClock(store, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatal(err)
	}
	suffix := "pinned-oom-baseline"
	createRunningEngineContainer(t, first, suffix)
	containerID := domain.ContainerID("container-" + suffix)
	createOperationID := operation.OperationID("op-container-" + suffix)
	startOperationID := operation.OperationID("op-start-" + suffix)
	for index := 1; index <= 2; index++ {
		operationID := operation.OperationID(fmt.Sprintf("op-oom-retention-churn-%d", index))
		if _, err := first.StartContainer(ctx, lifecycle.ContainerActionRequest{OperationID: operationID, ContainerID: containerID}); err != nil {
			t.Fatalf("StartContainer(retention churn %d) error = %v", index, err)
		}
	}
	if _, err := first.Coordinator().GetOperation(ctx, startOperationID); !errors.Is(err, state.ErrOperationExpired) {
		t.Fatalf("GetOperation(unpinned Start) error = %v, want ErrOperationExpired", err)
	}
	if err := assertPinnedOOMBaseline(ctx, store, createOperationID); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := state.NewFileStoreWithRetention(path, policy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("FileStore.Close() error = %v", err)
		}
	})
	if err := assertPinnedOOMBaseline(ctx, reopened, createOperationID); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewWithClock(reopened, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatal(err)
	}
	startedAt := fixedClock{}.Now()
	finishedAt := startedAt.Add(time.Second)
	host.mu.Lock()
	host.oomKill = 1
	host.attempt = AttemptObservation{
		Terminal: true, Evidence: "pinned-create-terminal-oom",
		Outcome: domain.ExitOutcome(137, domain.EvidenceUnknown, startedAt, finishedAt, time.Second),
	}
	host.mu.Unlock()
	result, err := restarted.RecordTerminal(ctx, "op-record-pinned-oom", containerID)
	if err != nil {
		t.Fatalf("RecordTerminal() error = %v", err)
	}
	if result.ContainerAttempt == nil || result.ContainerAttempt.Attempt.Outcome.OOM != domain.EvidenceTrue {
		t.Fatalf("RecordTerminal() outcome = %#v, want attributed OOM", result.ContainerAttempt)
	}
	removed, err := restarted.DeleteContainer(ctx, lifecycle.ContainerActionRequest{
		OperationID: "op-delete-pinned-oom", ContainerID: containerID,
	})
	if err != nil || !removed.Removed {
		t.Fatalf("DeleteContainer() = (%#v, %v), want removed", removed, err)
	}
	if _, err := restarted.Coordinator().GetOperation(ctx, createOperationID); !errors.Is(err, state.ErrOperationExpired) {
		t.Fatalf("GetOperation(create owner after verified delete) error = %v, want ErrOperationExpired", err)
	}
}

// assertPinnedOOMBaseline verifies a retained create owner still carries the
// pre-start cgroup counters needed by terminal attribution.
func assertPinnedOOMBaseline(ctx context.Context, store state.Store, operationID operation.OperationID) error {
	return store.View(ctx, func(reader state.Reader) error {
		record, err := reader.GetOperation(operationID)
		if err != nil {
			return fmt.Errorf("get pinned create operation %q: %w", operationID, err)
		}
		if record.OOMBaseline == nil {
			return fmt.Errorf("pinned create operation %q has no OOM baseline", operationID)
		}
		return nil
	})
}

// TestReconcileKeepsInterruptedSandboxStopResumable verifies missing owned
// evidence cannot terminal-fail a Stopping Sandbox into an unreachable phase.
func TestReconcileKeepsInterruptedSandboxStopResumable(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	request := lifecycle.SandboxCreateRequest{
		OperationID: "op-create-before-stop-recovery", SandboxID: "sandbox-stop-recovery",
		Spec: domain.SandboxSpec{Network: domain.NetworkIntent{Mode: "none"}},
	}
	if _, err := engine.CreateSandbox(context.Background(), request); err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	stop := lifecycle.SandboxActionRequest{OperationID: "op-stop-recovery", SandboxID: request.SandboxID}
	if _, err := engine.Coordinator().BeginSandboxStop(context.Background(), stop); err != nil {
		t.Fatalf("BeginSandboxStop() error = %v", err)
	}
	host.mu.Lock()
	host.present[ownership.KindKeeperProcess] = false
	host.mu.Unlock()
	if _, err := engine.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	active, err := engine.Coordinator().GetOperation(context.Background(), stop.OperationID)
	if err != nil || !active.State.Active() {
		t.Fatalf("GetOperation(stop) = (%#v, %v), want active", active, err)
	}
	sandbox, err := engine.Coordinator().GetSandbox(context.Background(), stop.SandboxID)
	if err != nil || sandbox.Status.Phase != domain.SandboxStopping || !hasEngineCondition(sandbox.Status.Conditions, domain.ConditionCleanupPending) {
		t.Fatalf("GetSandbox(after issue) = (%#v, %v)", sandbox, err)
	}
	host.mu.Lock()
	host.present[ownership.KindKeeperProcess] = true
	host.mu.Unlock()
	result, err := engine.StopSandbox(context.Background(), stop)
	if err != nil || result.Sandbox == nil || result.Sandbox.Status.Phase != domain.SandboxStopped {
		t.Fatalf("StopSandbox(retry) = (%#v, %v)", result, err)
	}
	if hasEngineCondition(result.Sandbox.Status.Conditions, domain.ConditionCleanupPending) {
		t.Fatalf("StopSandbox(retry) retained stale cleanup condition: %#v", result.Sandbox.Status.Conditions)
	}
}

// TestReconcileLeavesConsumedGateStartPending verifies a daemon restart during
// wrapper child launch records a resumable condition instead of aborting startup.
func TestReconcileLeavesConsumedGateStartPending(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	prepareCreatedContainer(t, engine)
	request := lifecycle.ContainerActionRequest{OperationID: "op-start-in-progress-recovery", ContainerID: "container-start-test"}
	begin, err := engine.Coordinator().BeginContainerStart(context.Background(), request)
	if err != nil {
		t.Fatalf("BeginContainerStart() error = %v", err)
	}
	inventory, err := engine.containerInventory(context.Background(), request.ContainerID)
	if err != nil {
		t.Fatalf("containerInventory() error = %v", err)
	}
	attachment, err := provider.NewAttachmentObservation(provider.AttachProcessRequest{
		Owner: inventory[0].Owner, Cgroup: inventory[0], Process: inventory[3],
	})
	if err != nil {
		t.Fatalf("NewAttachmentObservation() error = %v", err)
	}
	if _, err := engine.checkpointProgress(context.Background(), request.OperationID,
		operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}, begin.Fingerprint,
		operation.StageAttachCgroup, attachment); err != nil {
		t.Fatalf("checkpointProgress(attach) error = %v", err)
	}
	starting := AttemptObservation{Starting: true, Evidence: "starting-after-consumed-gate"}
	host.mu.Lock()
	host.attempt = starting
	host.mu.Unlock()
	if _, err := engine.checkpointProgress(context.Background(), request.OperationID,
		operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}, begin.Fingerprint,
		operation.StageReleaseStartGate, starting); err != nil {
		t.Fatalf("checkpointProgress(release) error = %v", err)
	}
	report, err := engine.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want resumable in-progress Start", err)
	}
	if !containsOperationID(report.Reconciled, request.OperationID) {
		t.Fatalf("Reconcile().Reconciled = %v", report.Reconciled)
	}
	active, err := engine.Coordinator().GetOperation(context.Background(), request.OperationID)
	if err != nil || !active.State.Active() || active.Stage != operation.StageObserveProcess {
		t.Fatalf("GetOperation(start) = (%#v, %v), want active observe stage", active, err)
	}
	pair, err := engine.Coordinator().GetContainer(context.Background(), request.ContainerID)
	if err != nil || pair.Attempt.Phase != domain.AttemptCreated || !hasEngineCondition(pair.Attempt.Conditions, domain.ConditionProcessIdentityUnknown) {
		t.Fatalf("GetContainer(starting) = (%#v, %v)", pair, err)
	}
	host.mu.Lock()
	host.attempt = AttemptObservation{Running: true, Evidence: "running-after-recovery"}
	host.mu.Unlock()
	result, err := engine.StartContainer(context.Background(), request)
	if err != nil || result.ContainerAttempt == nil || result.ContainerAttempt.Attempt.Phase != domain.AttemptRunning {
		t.Fatalf("StartContainer(retry) = (%#v, %v)", result, err)
	}
	if hasEngineCondition(result.ContainerAttempt.Attempt.Conditions, domain.ConditionProcessIdentityUnknown) {
		t.Fatalf("StartContainer(retry) retained stale unknown condition: %#v", result.ContainerAttempt.Attempt.Conditions)
	}
}

// TestReconcileClosesStartingToRunningObservationRace verifies a second
// observation that reaches Running is completed with one bounded same-ID retry.
func TestReconcileClosesStartingToRunningObservationRace(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	prepareCreatedContainer(t, engine)
	request := lifecycle.ContainerActionRequest{OperationID: "op-start-race-recovery", ContainerID: "container-start-test"}
	begin, err := engine.Coordinator().BeginContainerStart(context.Background(), request)
	if err != nil {
		t.Fatalf("BeginContainerStart() error = %v", err)
	}
	inventory, err := engine.containerInventory(context.Background(), request.ContainerID)
	if err != nil {
		t.Fatalf("containerInventory() error = %v", err)
	}
	attachment, err := provider.NewAttachmentObservation(provider.AttachProcessRequest{Owner: inventory[0].Owner, Cgroup: inventory[0], Process: inventory[3]})
	if err != nil {
		t.Fatalf("NewAttachmentObservation() error = %v", err)
	}
	target := operation.Target{Kind: operation.TargetContainer, ID: string(request.ContainerID)}
	if _, err := engine.checkpointProgress(context.Background(), request.OperationID, target, begin.Fingerprint, operation.StageAttachCgroup, attachment); err != nil {
		t.Fatalf("checkpointProgress(attach) error = %v", err)
	}
	if _, err := engine.checkpointProgress(context.Background(), request.OperationID, target, begin.Fingerprint, operation.StageReleaseStartGate,
		AttemptObservation{Starting: true, Evidence: "starting-release"}); err != nil {
		t.Fatalf("checkpointProgress(release) error = %v", err)
	}
	host.mu.Lock()
	host.attemptSequence = []AttemptObservation{
		{Starting: true, Evidence: "starting-first"},
		{Running: true, Evidence: "running-second"},
		{Running: true, Evidence: "running-final"},
	}
	host.mu.Unlock()
	if _, err := engine.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	pair, err := engine.Coordinator().GetContainer(context.Background(), request.ContainerID)
	if err != nil || pair.Attempt.Phase != domain.AttemptRunning {
		t.Fatalf("GetContainer() = (%#v, %v), want Running", pair, err)
	}
}

// TestReconcileDoesNotBlockOnFutureKillDeadline verifies daemon startup leaves
// a grace-period Kill active, then a later reconciliation escalates at the
// exact persisted wall deadline without resending the initial signal.
func TestReconcileDoesNotBlockOnFutureKillDeadline(t *testing.T) {
	host := newFakeHost()
	deliveredAt := fixedClock{}.Now()
	clock := &manualClock{now: deliveredAt}
	store := state.NewMemoryStore()
	engine, err := NewWithClock(store, testProviders(t, host), clock)
	if err != nil {
		t.Fatalf("NewWithClock() error = %v", err)
	}
	prepareCreatedContainer(t, engine)
	if _, err := engine.StartContainer(context.Background(), lifecycle.ContainerActionRequest{
		OperationID: "op-start-before-recovery-kill", ContainerID: "container-start-test",
	}); err != nil {
		t.Fatalf("StartContainer() error = %v", err)
	}
	request := lifecycle.KillRequest{
		OperationID: "op-kill-future-recovery", ContainerID: "container-start-test",
		Policy: domain.TerminationPolicy{Signal: "SIGTERM", GracePeriod: time.Hour, EscalationSignal: "SIGKILL"},
	}
	requestContext, cancel := context.WithCancel(context.Background())
	host.mu.Lock()
	host.keepRunningAfterInitial = true
	host.signalDeliveredAt = deliveredAt
	host.cancelNextObservation = cancel
	host.mu.Unlock()
	if _, err := engine.KillContainer(requestContext, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("KillContainer(canceled) error = %v, want context.Canceled", err)
	}
	restarted, err := NewWithClock(store, testProviders(t, host), clock)
	if err != nil {
		t.Fatalf("NewWithClock(restarted) error = %v", err)
	}
	if _, err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile(before deadline) error = %v", err)
	}
	host.mu.Lock()
	deliveriesBefore := host.signalDeliveries
	host.mu.Unlock()
	if deliveriesBefore != 1 {
		t.Fatalf("signal deliveries before deadline = %d, want initial only", deliveriesBefore)
	}
	active, err := restarted.Coordinator().GetOperation(context.Background(), request.OperationID)
	if err != nil || !active.State.Active() {
		t.Fatalf("GetOperation(before deadline) = (%#v, %v), want active", active, err)
	}
	clock.Set(deliveredAt.Add(request.Policy.GracePeriod))
	if _, err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile(at deadline) error = %v", err)
	}
	completed, err := restarted.Coordinator().GetOperation(context.Background(), request.OperationID)
	if err != nil || completed.State != operation.StateSucceeded {
		t.Fatalf("GetOperation(after deadline) = (%#v, %v), want succeeded", completed, err)
	}
	host.mu.Lock()
	deliveriesAfter := host.signalDeliveries
	host.mu.Unlock()
	if deliveriesAfter != 2 {
		t.Fatalf("signal deliveries after deadline = %d, want initial plus escalation", deliveriesAfter)
	}
}

// TestReconcileCheckpointsInitialKillWithoutWaiting verifies a crash after the
// durable intent cannot block daemon startup for the request's full grace period.
func TestReconcileCheckpointsInitialKillWithoutWaiting(t *testing.T) {
	host := newFakeHost()
	deliveredAt := fixedClock{}.Now()
	clock := &manualClock{now: deliveredAt}
	store := state.NewMemoryStore()
	engine, err := NewWithClock(store, testProviders(t, host), clock)
	if err != nil {
		t.Fatalf("NewWithClock() error = %v", err)
	}
	prepareCreatedContainer(t, engine)
	if _, err := engine.StartContainer(context.Background(), lifecycle.ContainerActionRequest{
		OperationID: "op-start-before-intent-kill", ContainerID: "container-start-test",
	}); err != nil {
		t.Fatalf("StartContainer() error = %v", err)
	}
	request := lifecycle.KillRequest{
		OperationID: "op-kill-persisted-intent", ContainerID: "container-start-test",
		Policy: domain.TerminationPolicy{Signal: "SIGTERM", GracePeriod: time.Hour, EscalationSignal: "SIGKILL"},
	}
	host.mu.Lock()
	host.keepRunningAfterInitial = true
	host.signalDeliveredAt = deliveredAt
	host.mu.Unlock()
	if _, err := engine.Coordinator().PlanKill(context.Background(), request); err != nil {
		t.Fatalf("PlanKill() error = %v", err)
	}
	if _, err := engine.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	progress, err := engine.Coordinator().GetOperationProgress(context.Background(), request.OperationID)
	if err != nil {
		t.Fatalf("GetOperationProgress() error = %v", err)
	}
	wantDeadline := deliveredAt.Add(request.Policy.GracePeriod)
	if progress.Operation.Stage != operation.StageSignalProcess || progress.KillEscalationDeadline == nil || !progress.KillEscalationDeadline.Equal(wantDeadline) {
		t.Fatalf("Kill progress = %#v, want active signal checkpoint at %s", progress, wantDeadline)
	}
	pair, err := engine.Coordinator().GetContainer(context.Background(), request.ContainerID)
	if err != nil || pair.Attempt.Phase != domain.AttemptRunning {
		t.Fatalf("GetContainer() = (%#v, %v), want still Running", pair, err)
	}
}

// TestWatchKillDeadlinesEscalatesDueKillWithoutClientRetry verifies the daemon
// background loop owns a persisted grace deadline after the public API is up.
func TestWatchKillDeadlinesEscalatesDueKillWithoutClientRetry(t *testing.T) {
	host := newFakeHost()
	deliveredAt := fixedClock{}.Now()
	clock := &manualClock{now: deliveredAt}
	store := state.NewMemoryStore()
	engine, err := NewWithClock(store, testProviders(t, host), clock)
	if err != nil {
		t.Fatalf("NewWithClock() error = %v", err)
	}
	prepareCreatedContainer(t, engine)
	if _, err := engine.StartContainer(context.Background(), lifecycle.ContainerActionRequest{
		OperationID: "op-start-before-background-kill", ContainerID: "container-start-test",
	}); err != nil {
		t.Fatalf("StartContainer() error = %v", err)
	}
	request := lifecycle.KillRequest{
		OperationID: "op-kill-background-deadline", ContainerID: "container-start-test",
		Policy: domain.TerminationPolicy{Signal: "SIGTERM", GracePeriod: time.Hour, EscalationSignal: "SIGKILL"},
	}
	steps := make(chan provider.SignalStep, 2)
	host.mu.Lock()
	host.keepRunningAfterInitial = true
	host.signalDeliveredAt = deliveredAt
	host.signalSteps = steps
	host.mu.Unlock()
	if _, err := engine.Coordinator().PlanKill(context.Background(), request); err != nil {
		t.Fatalf("PlanKill() error = %v", err)
	}
	if _, err := engine.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile(initial checkpoint) error = %v", err)
	}
	if step := <-steps; step != provider.SignalStepInitial {
		t.Fatalf("first signal step = %q, want initial", step)
	}
	watchContext, cancel := context.WithCancel(context.Background())
	watchResult := make(chan error, 1)
	go func() {
		watchResult <- engine.WatchKillDeadlines(watchContext, time.Millisecond)
	}()
	clock.Set(deliveredAt.Add(request.Policy.GracePeriod))
	if step := <-steps; step != provider.SignalStepEscalation {
		cancel()
		t.Fatalf("second signal step = %q, want escalation", step)
	}
	var completed operation.Operation
	for {
		completed, err = engine.Coordinator().GetOperation(context.Background(), request.OperationID)
		if err != nil {
			cancel()
			t.Fatalf("GetOperation() error = %v", err)
		}
		if completed.State.Terminal() {
			break
		}
		runtime.Gosched()
	}
	cancel()
	if err := <-watchResult; err != nil {
		t.Fatalf("WatchKillDeadlines() error = %v", err)
	}
	if completed.State != operation.StateSucceeded {
		t.Fatalf("GetOperation() = %#v, want succeeded", completed)
	}
}

// TestReconcileKeepsUnknownContainerDeleteActive verifies temporary receipt
// unavailability never authorizes cleanup or makes daemon startup fail.
func TestReconcileKeepsUnknownContainerDeleteActive(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	prepareCreatedContainer(t, engine)
	request := lifecycle.ContainerActionRequest{OperationID: "op-delete-unknown-recovery", ContainerID: "container-start-test"}
	if _, err := engine.Coordinator().BeginContainerDelete(context.Background(), request); err != nil {
		t.Fatalf("BeginContainerDelete() error = %v", err)
	}
	host.mu.Lock()
	host.inspectUnavailableKind = ownership.KindRootfsMount
	host.mu.Unlock()
	if _, err := engine.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	active, err := engine.Coordinator().GetOperation(context.Background(), request.OperationID)
	if err != nil || !active.State.Active() {
		t.Fatalf("GetOperation(delete) = (%#v, %v), want active", active, err)
	}
	pair, err := engine.Coordinator().GetContainer(context.Background(), request.ContainerID)
	if err != nil || !hasEngineCondition(pair.Attempt.Conditions, domain.ConditionCleanupPending) {
		t.Fatalf("GetContainer(delete unknown) = (%#v, %v)", pair, err)
	}
	host.mu.Lock()
	rootfsPresent := host.present[ownership.KindRootfsMount]
	host.mu.Unlock()
	if !rootfsPresent {
		t.Fatal("unknown recovery observation authorized rootfs cleanup")
	}
}

// TestReconcileKeepsUnknownSandboxDeleteActive verifies the same fail-closed
// rule for keeper-owned Sandbox inventory.
func TestReconcileKeepsUnknownSandboxDeleteActive(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	create := lifecycle.SandboxCreateRequest{
		OperationID: "op-create-before-unknown-remove", SandboxID: "sandbox-unknown-remove",
		Spec: domain.SandboxSpec{Network: domain.NetworkIntent{Mode: "none"}},
	}
	if _, err := engine.CreateSandbox(context.Background(), create); err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	if _, err := engine.StopSandbox(context.Background(), lifecycle.SandboxActionRequest{
		OperationID: "op-stop-before-unknown-remove", SandboxID: create.SandboxID,
	}); err != nil {
		t.Fatalf("StopSandbox() error = %v", err)
	}
	remove := lifecycle.SandboxActionRequest{OperationID: "op-remove-unknown-recovery", SandboxID: create.SandboxID}
	if _, err := engine.Coordinator().BeginSandboxRemove(context.Background(), remove); err != nil {
		t.Fatalf("BeginSandboxRemove() error = %v", err)
	}
	host.mu.Lock()
	host.inspectUnavailableKind = ownership.KindKeeperProcess
	host.mu.Unlock()
	if _, err := engine.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	active, err := engine.Coordinator().GetOperation(context.Background(), remove.OperationID)
	if err != nil || !active.State.Active() {
		t.Fatalf("GetOperation(remove) = (%#v, %v), want active", active, err)
	}
	sandbox, err := engine.Coordinator().GetSandbox(context.Background(), remove.SandboxID)
	if err != nil || !hasEngineCondition(sandbox.Status.Conditions, domain.ConditionCleanupPending) {
		t.Fatalf("GetSandbox(remove unknown) = (%#v, %v)", sandbox, err)
	}
	host.mu.Lock()
	keeperPresent := host.present[ownership.KindKeeperProcess]
	host.mu.Unlock()
	if !keeperPresent {
		t.Fatal("unknown recovery observation authorized keeper cleanup")
	}
}

// TestRollbackCreatePersistsLIFOProgressBeforeTerminalFailure verifies a definite acquisition failure automatically cleans and replays after all absence facts are proven.
func TestRollbackCreatePersistsLIFOProgressBeforeTerminalFailure(t *testing.T) {
	host := newFakeHost()
	host.failBeforeEnsureKind = ownership.KindUTSNamespace
	engine, _ := testEngine(t, host)
	request := lifecycle.SandboxCreateRequest{
		OperationID: "op-rollback-create", SandboxID: "sandbox-rollback-create",
		Spec: domain.SandboxSpec{Network: domain.NetworkIntent{Mode: "none"}},
	}
	if _, err := engine.CreateSandbox(context.Background(), request); err == nil {
		t.Fatal("CreateSandbox() error = nil, want injected definite failure")
	}
	failed, err := engine.Coordinator().GetOperation(context.Background(), request.OperationID)
	if err != nil {
		t.Fatalf("GetOperation() error = %v", err)
	}
	if failed.State != operation.StateFailed || failed.Stage != operation.StageComplete {
		t.Fatalf("automatic rollback operation = %#v", failed)
	}
	if _, err := engine.Coordinator().GetSandbox(context.Background(), request.SandboxID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetSandbox(after rollback) error = %v, want ErrNotFound", err)
	}
	replayed, err := engine.RollbackCreate(context.Background(), request.OperationID, lifecycle.Failure{
		Reason: operation.ReasonInternal, Message: "namespace acquisition failed before side effect",
	})
	if err != nil || !reflect.DeepEqual(replayed, failed) {
		t.Fatalf("RollbackCreate(replay) = (%#v, %v), want %#v", replayed, err, failed)
	}
}

// TestCreateRetryResumesPersistedRollback verifies a failed inverse preserves
// the original cause and step diagnostic, then retries cleanup without acquiring new resources.
func TestCreateRetryResumesPersistedRollback(t *testing.T) {
	host := newFakeHost()
	host.failBeforeEnsureKind = ownership.KindUTSNamespace
	host.failCleanupKind = ownership.KindKeeperProcess
	engine, _ := testEngine(t, host)
	request := lifecycle.SandboxCreateRequest{
		OperationID: "op-rollback-retry", SandboxID: "sandbox-rollback-retry",
		Spec: domain.SandboxSpec{Network: domain.NetworkIntent{Mode: "none"}},
	}
	if _, err := engine.CreateSandbox(context.Background(), request); err == nil {
		t.Fatal("CreateSandbox(first) error = nil, want persisted rollback failure")
	}
	progress, err := engine.Coordinator().GetOperationProgress(context.Background(), request.OperationID)
	if err != nil {
		t.Fatalf("GetOperationProgress() error = %v", err)
	}
	if progress.Operation.Stage != operation.StageRollback || progress.RollbackCause == nil ||
		!strings.Contains(progress.RollbackCause.Message, "injected provider failure before acquisition") {
		t.Fatalf("rollback primary progress = %#v", progress)
	}
	foundFailure := false
	for _, record := range progress.Rollback {
		if strings.HasPrefix(record.Descriptor.Name, string(ownership.KindKeeperProcess)+":") && record.Attempts == 1 && record.LastError == "injected rollback cleanup failure" {
			foundFailure = true
		}
	}
	if !foundFailure {
		t.Fatalf("rollback records = %#v, want persisted keeper failure", progress.Rollback)
	}
	host.mu.Lock()
	createdBefore := cloneKindCounts(host.created)
	host.mu.Unlock()
	replayed, err := engine.CreateSandbox(context.Background(), request)
	if err == nil || replayed.Operation.State != operation.StateFailed {
		t.Fatalf("CreateSandbox(retry) = (%#v, %v), want stable failed replay", replayed, err)
	}
	host.mu.Lock()
	createdAfter := cloneKindCounts(host.created)
	host.mu.Unlock()
	if !reflect.DeepEqual(createdAfter, createdBefore) {
		t.Fatalf("create counts after rollback retry = %v, want %v", createdAfter, createdBefore)
	}
}

// TestReconcileRollsBackPartialRootfsFailure verifies a one-shot pivot failure
// with possible mount effects is sealed, persisted, and recovered by stopping
// the already-checkpointed init owner instead of retrying rootfs preparation.
func TestReconcileRollsBackPartialRootfsFailure(t *testing.T) {
	host := newFakeHost()
	engine, store := testEngine(t, host)
	ctx := context.Background()
	if _, err := engine.CreateSandbox(ctx, lifecycle.SandboxCreateRequest{
		OperationID: "op-partial-rootfs-sandbox", SandboxID: "sandbox-partial-rootfs",
		Spec: domain.SandboxSpec{Network: domain.NetworkIntent{Mode: "none"}},
	}); err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	host.mu.Lock()
	host.rollbackRequiredKind = ownership.KindRootfsMount
	host.failCleanupKind = ownership.KindInitProcess
	host.mu.Unlock()
	request := lifecycle.ContainerCreateRequest{
		OperationID: "op-partial-rootfs-container", SandboxID: "sandbox-partial-rootfs",
		ContainerID: "container-partial-rootfs", AttemptID: "attempt-partial-rootfs",
		Process: domain.ProcessSpec{Argv: []string{"/bin/workload"}}, RootFS: "prepared-rootfs-partial",
	}
	if _, err := engine.CreateContainer(ctx, request); err == nil {
		t.Fatal("CreateContainer() error = nil, want persisted partial-rootfs rollback failure")
	}
	progress, err := engine.Coordinator().GetOperationProgress(ctx, request.OperationID)
	if err != nil {
		t.Fatalf("GetOperationProgress() error = %v", err)
	}
	if progress.Operation.Stage != operation.StageRollback || progress.RollbackCause == nil ||
		!strings.Contains(progress.RollbackCause.Message, "rollback-contained") {
		t.Fatalf("partial-rootfs rollback progress = %#v", progress)
	}
	host.mu.Lock()
	rootfsPresentBefore := host.present[ownership.KindRootfsMount]
	rootfsCreatesBefore := host.created[ownership.KindRootfsMount]
	host.mu.Unlock()
	if !rootfsPresentBefore || rootfsCreatesBefore != 1 {
		t.Fatalf("partial rootfs presence/creates = %t/%d, want true/1", rootfsPresentBefore, rootfsCreatesBefore)
	}

	restarted, err := NewWithClock(store, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatalf("NewWithClock(restart) error = %v", err)
	}
	if _, err := restarted.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	failed, err := restarted.Coordinator().GetOperation(ctx, request.OperationID)
	if err != nil || failed.State != operation.StateFailed || failed.Stage != operation.StageComplete {
		t.Fatalf("reconciled operation = (%#v, %v), want terminal failed", failed, err)
	}
	pair, err := restarted.Coordinator().GetContainer(ctx, request.ContainerID)
	if err != nil || pair.Attempt.Phase != domain.AttemptStopped || pair.Attempt.Outcome.Presence != domain.OutcomeNotApplicable {
		t.Fatalf("reconciled Container Attempt = (%#v, %v), want stopped/not-applicable", pair, err)
	}
	host.mu.Lock()
	rootfsPresentAfter := host.present[ownership.KindRootfsMount]
	rootfsCreatesAfter := host.created[ownership.KindRootfsMount]
	host.mu.Unlock()
	if rootfsPresentAfter || rootfsCreatesAfter != rootfsCreatesBefore {
		t.Fatalf("rootfs after restart = present:%t creates:%d, want absent and no replay", rootfsPresentAfter, rootfsCreatesAfter)
	}
}

// TestReconcileResumesDeleteAfterEffectBeforeCheckpoint verifies expected
// absence in the teardown crash window is journaled by the delete path rather than failed as lost ownership.
func TestReconcileResumesDeleteAfterEffectBeforeCheckpoint(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	prepareCreatedContainer(t, engine)
	request := lifecycle.ContainerActionRequest{OperationID: "op-delete-crash-window", ContainerID: "container-start-test"}
	begin, err := engine.Coordinator().BeginContainerDelete(context.Background(), request)
	if err != nil {
		t.Fatalf("BeginContainerDelete() error = %v", err)
	}
	inventory, err := engine.containerInventory(context.Background(), request.ContainerID)
	if err != nil {
		t.Fatalf("containerInventory() error = %v", err)
	}
	rootfs, err := receiptByKind(inventory, ownership.KindRootfsMount)
	if err != nil {
		t.Fatalf("receiptByKind(rootfs) error = %v", err)
	}
	if _, err := host.remove(provider.OwnedReceiptRequest{Owner: rootfs.Owner, Receipt: rootfs}, ownership.KindRootfsMount); err != nil {
		t.Fatalf("remove(rootfs before checkpoint) error = %v", err)
	}
	report, err := engine.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !containsOperationID(report.Reconciled, request.OperationID) {
		t.Fatalf("Reconcile() operations = %v, want %q", report.Reconciled, request.OperationID)
	}
	operationValue, err := engine.Coordinator().GetOperation(context.Background(), request.OperationID)
	if err != nil || operationValue.State != operation.StateSucceeded {
		t.Fatalf("GetOperation(delete) = (%#v, %v)", operationValue, err)
	}
	if operationValue.Fingerprint != begin.Fingerprint {
		t.Fatalf("delete fingerprint = %#v, want %#v", operationValue.Fingerprint, begin.Fingerprint)
	}
	if _, err := engine.Coordinator().GetContainer(context.Background(), request.ContainerID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetContainer(after reconcile delete) error = %v, want ErrNotFound", err)
	}
}

// TestReconcileResumesNaturalTerminalIntentAfterFileStoreReopen verifies a
// crash after BeginRecordStopped re-observes the same wrapper fact and closes
// the exact active Container Stop operation instead of blocking daemon start.
func TestReconcileResumesNaturalTerminalIntentAfterFileStoreReopen(t *testing.T) {
	ctx := context.Background()
	host := newFakeHost()
	path := filepath.Join(secureEngineStateDirectory(t), "state.json")
	store, err := state.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewWithClock(store, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatal(err)
	}
	createRunningEngineContainer(t, first, "terminal-intent-reopen")
	startedAt := fixedClock{}.Now()
	finishedAt := startedAt.Add(time.Second)
	host.mu.Lock()
	host.attempt = AttemptObservation{
		Terminal: true, Evidence: "terminal-intent-reopen-evidence",
		Outcome: domain.ExitOutcome(23, domain.EvidenceFalse, startedAt, finishedAt, time.Second),
	}
	host.mu.Unlock()
	containerID := domain.ContainerID("container-terminal-intent-reopen")
	operationID := operation.OperationID("op-terminal-intent-reopen")
	observation, err := first.observeRetainedAttemptLocked(ctx, containerID)
	if err != nil {
		t.Fatalf("observeRetainedAttemptLocked() error = %v", err)
	}
	begin, err := first.lifecycle.BeginRecordStopped(ctx, lifecycle.RecordStoppedRequest{
		OperationID: operationID, ContainerID: containerID, Outcome: observation.Outcome,
		Conditions: terminalConditions(observation.Outcome),
		Verification: lifecycle.Verification{
			Kind: lifecycle.VerificationAttemptStopped, Verified: true,
			Evidence: observation.Evidence, ObservedAt: first.clock.Now(),
		},
	})
	if err != nil || begin.Operation.State != operation.StateRunning {
		t.Fatalf("BeginRecordStopped() = (%#v, %v), want active intent", begin.Operation, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := state.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("FileStore.Close() error = %v", err)
		}
	})
	restarted, err := NewWithClock(reopened, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatal(err)
	}
	report, err := restarted.Reconcile(ctx)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !containsOperationID(report.Reconciled, operationID) {
		t.Fatalf("Reconcile().Reconciled = %v, want %q", report.Reconciled, operationID)
	}
	pair, err := restarted.Coordinator().GetContainer(ctx, containerID)
	if err != nil || pair.Attempt.Phase != domain.AttemptStopped || pair.Attempt.Outcome.ExitCode == nil || *pair.Attempt.Outcome.ExitCode != 23 {
		t.Fatalf("GetContainer() = (%#v, %v), want stopped exit 23", pair, err)
	}
	completed, err := restarted.Coordinator().GetOperation(ctx, operationID)
	if err != nil || completed.State != operation.StateSucceeded {
		t.Fatalf("GetOperation() = (%#v, %v), want succeeded", completed, err)
	}
}

// TestFileStoreReopensRecreatedSandboxGeneration verifies historical 1/1
// events from a deleted incarnation do not invalidate a new same-ID create
// intent whose durable projection has correctly restarted at generation 1/0.
func TestFileStoreReopensRecreatedSandboxGeneration(t *testing.T) {
	ctx := context.Background()
	host := newFakeHost()
	path := filepath.Join(secureEngineStateDirectory(t), "state.json")
	store, err := state.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewWithClock(store, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatal(err)
	}
	sandboxID := domain.SandboxID("sandbox-recreated-generation")
	spec := domain.SandboxSpec{Network: domain.NetworkIntent{Mode: "none"}}
	if _, err := first.CreateSandbox(ctx, lifecycle.SandboxCreateRequest{
		OperationID: "op-create-old-incarnation", SandboxID: sandboxID, Spec: spec,
	}); err != nil {
		t.Fatalf("CreateSandbox(old incarnation) error = %v", err)
	}
	if _, err := first.StopSandbox(ctx, lifecycle.SandboxActionRequest{
		OperationID: "op-stop-old-incarnation", SandboxID: sandboxID,
	}); err != nil {
		t.Fatalf("StopSandbox(old incarnation) error = %v", err)
	}
	if _, err := first.RemoveSandbox(ctx, lifecycle.SandboxActionRequest{
		OperationID: "op-delete-old-incarnation", SandboxID: sandboxID,
	}); err != nil {
		t.Fatalf("RemoveSandbox(old incarnation) error = %v", err)
	}
	begin, err := first.Coordinator().BeginSandboxCreate(ctx, lifecycle.SandboxCreateRequest{
		OperationID: "op-create-new-incarnation", SandboxID: sandboxID, Spec: spec,
	})
	if err != nil {
		t.Fatalf("BeginSandboxCreate(new incarnation) error = %v", err)
	}
	if begin.Sandbox == nil || begin.Sandbox.Status.Phase != domain.SandboxCreating || begin.Sandbox.Status.ObservedGeneration != 0 {
		t.Fatalf("new incarnation projection = %#v, want Creating generation 1/0", begin.Sandbox)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := state.NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(recreated incarnation reopen) error = %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("FileStore.Close() error = %v", err)
		}
	}()
	restarted, err := NewWithClock(reopened, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatal(err)
	}
	recreated, err := restarted.Coordinator().GetSandbox(ctx, sandboxID)
	if err != nil || recreated.Status.Phase != domain.SandboxCreating ||
		recreated.Status.Generation != domain.InitialGeneration || recreated.Status.ObservedGeneration != 0 {
		t.Fatalf("GetSandbox(recreated) = (%#v, %v), want Creating generation 1/0", recreated, err)
	}
}

// secureEngineStateDirectory creates owner-only state ancestry under the
// package workspace and removes FileStore's stable sibling lock at test end.
func secureEngineStateDirectory(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp(".", ".engine-state-test-")
	if err != nil {
		t.Fatalf("MkdirTemp(secure engine state directory) error = %v", err)
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		_ = os.RemoveAll(directory)
		t.Fatalf("Abs(secure engine state directory) error = %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(absolute); err != nil {
			t.Errorf("RemoveAll(secure engine state directory) error = %v", err)
		}
		pattern := filepath.Join(filepath.Dir(absolute), "."+filepath.Base(absolute)+"-*.state.lock")
		anchors, err := filepath.Glob(pattern)
		if err != nil {
			t.Errorf("Glob(engine state lock anchors) error = %v", err)
			return
		}
		for _, anchor := range anchors {
			if err := os.Remove(anchor); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Errorf("Remove(engine state lock anchor %q) error = %v", anchor, err)
			}
		}
	})
	return absolute
}

// createRunningEngineContainer prepares and starts one fake-provider Attempt for recovery tests.
func createRunningEngineContainer(t *testing.T, engine *Engine, suffix string) {
	t.Helper()
	sandboxID := domain.SandboxID("sandbox-" + suffix)
	containerID := domain.ContainerID("container-" + suffix)
	if _, err := engine.CreateSandbox(context.Background(), lifecycle.SandboxCreateRequest{
		OperationID: operation.OperationID("op-sandbox-" + suffix), SandboxID: sandboxID,
		Spec: domain.SandboxSpec{Network: domain.NetworkIntent{Mode: "none"}},
	}); err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	if _, err := engine.CreateContainer(context.Background(), lifecycle.ContainerCreateRequest{
		OperationID: operation.OperationID("op-container-" + suffix), SandboxID: sandboxID,
		ContainerID: containerID, AttemptID: domain.AttemptID("attempt-" + suffix),
		Process: domain.ProcessSpec{Argv: []string{"/bin/workload"}}, RootFS: "prepared-rootfs-" + suffix,
	}); err != nil {
		t.Fatalf("CreateContainer() error = %v", err)
	}
	if _, err := engine.StartContainer(context.Background(), lifecycle.ContainerActionRequest{
		OperationID: operation.OperationID("op-start-" + suffix), ContainerID: containerID,
	}); err != nil {
		t.Fatalf("StartContainer() error = %v", err)
	}
}

// engineEventCount returns the durable global event count for mutation-barrier assertions.
func engineEventCount(t *testing.T, store state.Store) int {
	t.Helper()
	count := 0
	if err := store.View(context.Background(), func(reader state.Reader) error {
		events, err := reader.EventsAfter(0, 0)
		count = len(events)
		return err
	}); err != nil {
		t.Fatalf("EventsAfter() error = %v", err)
	}
	return count
}

// cloneKindCounts copies fake acquisition counters without retaining the provider map.
func cloneKindCounts(values map[ownership.Kind]int) map[ownership.Kind]int {
	clone := make(map[ownership.Kind]int, len(values))
	for kind, count := range values {
		clone[kind] = count
	}
	return clone
}

// containsOperationID reports whether one deterministic recovery result includes the requested operation.
func containsOperationID(values []operation.OperationID, expected operation.OperationID) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

// hasEngineCondition reports whether one recovery condition type is present in a caller-owned status slice.
func hasEngineCondition(conditions []domain.Condition, conditionType string) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			return true
		}
	}
	return false
}
