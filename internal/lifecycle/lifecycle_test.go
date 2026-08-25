package lifecycle

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/state"
)

// fixedClock provides deterministic non-zero event wall facts without timing thresholds.
type fixedClock struct{ now time.Time }

// Now returns the fixed diagnostic timestamp selected by a test.
func (clock fixedClock) Now() time.Time { return clock.now }

// acceptingVerifier records action-time rechecks while accepting valid opaque test evidence.
type acceptingVerifier struct{ calls int }

// Verify validates the target and evidence and records that PlanKill did not trust persistence alone.
func (verifier *acceptingVerifier) Verify(ctx context.Context, target operation.Target, identity domain.ProcessIdentity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := target.Validate(); err != nil {
		return err
	}
	if err := identity.Validate(); err != nil {
		return err
	}
	verifier.calls++
	return nil
}

// TestSandboxLifecycleTwoPhaseRetryAndRemoval verifies create, stop, and remove require confirmation and replay exactly.
func TestSandboxLifecycleTwoPhaseRetryAndRemoval(t *testing.T) {
	coordinator, store := testCoordinator(t)
	createRequest := SandboxCreateRequest{OperationID: "op-sandbox-create", SandboxID: "sandbox-1", Spec: testSandboxSpec()}

	created, err := coordinator.BeginSandboxCreate(context.Background(), createRequest)
	if err != nil {
		t.Fatalf("BeginSandboxCreate() error = %v", err)
	}
	if created.Resolution != operation.ResolutionNew || created.Sandbox == nil || created.Sandbox.Status.Phase != domain.SandboxCreating || created.Operation.State != operation.StateRunning {
		t.Fatalf("BeginSandboxCreate() = %#v", created)
	}
	resumed, err := coordinator.BeginSandboxCreate(context.Background(), createRequest)
	if err != nil {
		t.Fatalf("BeginSandboxCreate(retry) error = %v", err)
	}
	if resumed.Resolution != operation.ResolutionResume || resumed.Fingerprint != created.Fingerprint {
		t.Fatalf("BeginSandboxCreate(retry) = %#v", resumed)
	}
	mismatched := createRequest
	mismatched.Spec.Hostname = "different"
	if _, err := coordinator.BeginSandboxCreate(context.Background(), mismatched); !errors.Is(err, operation.ErrBindingMismatch) {
		t.Fatalf("BeginSandboxCreate(mismatched retry) error = %v, want binding mismatch", err)
	}

	invalidConfirmation := SandboxConfirmRequest{OperationID: createRequest.OperationID, SandboxID: createRequest.SandboxID, Fingerprint: created.Fingerprint}
	if _, err := coordinator.ConfirmSandboxCreate(context.Background(), invalidConfirmation); !errors.Is(err, ErrVerificationRequired) {
		t.Fatalf("ConfirmSandboxCreate(without verification) error = %v", err)
	}
	stillCreating, err := coordinator.GetSandbox(context.Background(), createRequest.SandboxID)
	if err != nil || stillCreating.Status.Phase != domain.SandboxCreating || stillCreating.Status.ObservedGeneration != 0 {
		t.Fatalf("GetSandbox() before verification = (%#v, %v)", stillCreating, err)
	}

	confirmation := invalidConfirmation
	confirmation.Verification = testVerification(VerificationSandboxReady, nil)
	ready, err := coordinator.ConfirmSandboxCreate(context.Background(), confirmation)
	if err != nil {
		t.Fatalf("ConfirmSandboxCreate() error = %v", err)
	}
	if ready.Sandbox == nil || ready.Sandbox.Status.Phase != domain.SandboxReady || ready.Sandbox.Status.ObservedGeneration != domain.InitialGeneration || ready.Operation.State != operation.StateSucceeded {
		t.Fatalf("ConfirmSandboxCreate() = %#v", ready)
	}
	if ready.Sandbox.Status.LastObservation.OperationID != string(createRequest.OperationID) || ready.Sandbox.Status.LastObservation.EventSequence != 2 || ready.Sandbox.Status.LastObservation.Reason != string(operation.ReasonNone) {
		t.Fatalf("Ready LastObservation = %#v", ready.Sandbox.Status.LastObservation)
	}
	replayedReady, err := coordinator.ConfirmSandboxCreate(context.Background(), confirmation)
	if err != nil {
		t.Fatalf("ConfirmSandboxCreate(retry) error = %v", err)
	}
	if replayedReady.Resolution != operation.ResolutionReplay || !reflect.DeepEqual(replayedReady.Sandbox, ready.Sandbox) {
		t.Fatalf("ConfirmSandboxCreate(retry) = %#v, want replay %#v", replayedReady, ready)
	}

	stopRequest := SandboxActionRequest{OperationID: "op-sandbox-stop", SandboxID: createRequest.SandboxID}
	stopping, err := coordinator.BeginSandboxStop(context.Background(), stopRequest)
	if err != nil {
		t.Fatalf("BeginSandboxStop() error = %v", err)
	}
	if stopping.Sandbox == nil || stopping.Sandbox.Status.Phase != domain.SandboxStopping {
		t.Fatalf("BeginSandboxStop() = %#v", stopping)
	}
	conflictRequest := SandboxActionRequest{OperationID: "op-sandbox-stop-conflict", SandboxID: createRequest.SandboxID}
	conflictResult, err := coordinator.BeginSandboxStop(context.Background(), conflictRequest)
	assertDurableConflictError(t, err)
	if conflictResult.Operation.State != operation.StateFailed || conflictResult.Operation.Reason != operation.ReasonConflict {
		t.Fatalf("persisted conflict operation = %#v", conflictResult.Operation)
	}
	replayedConflict, err := coordinator.BeginSandboxStop(context.Background(), conflictRequest)
	assertDurableConflictError(t, err)
	if replayedConflict.Resolution != operation.ResolutionReplay || !reflect.DeepEqual(replayedConflict.Operation.Response, conflictResult.Operation.Response) {
		t.Fatalf("BeginSandboxStop(conflict retry) = (%#v, %v)", replayedConflict, err)
	}
	stopped, err := coordinator.ConfirmSandboxStop(context.Background(), SandboxConfirmRequest{
		OperationID: stopRequest.OperationID, SandboxID: stopRequest.SandboxID,
		Fingerprint: stopping.Fingerprint, Verification: testVerification(VerificationSandboxStopped, nil),
	})
	if err != nil {
		t.Fatalf("ConfirmSandboxStop() error = %v", err)
	}
	if stopped.Sandbox == nil || stopped.Sandbox.Status.Phase != domain.SandboxStopped {
		t.Fatalf("ConfirmSandboxStop() = %#v", stopped)
	}
	replayedStop, err := coordinator.ConfirmSandboxStop(context.Background(), SandboxConfirmRequest{
		OperationID: stopRequest.OperationID, SandboxID: stopRequest.SandboxID,
		Fingerprint: stopping.Fingerprint, Verification: testVerification(VerificationSandboxStopped, nil),
	})
	if err != nil || replayedStop.Resolution != operation.ResolutionReplay || !reflect.DeepEqual(replayedStop.Sandbox, stopped.Sandbox) {
		t.Fatalf("ConfirmSandboxStop(retry) = (%#v, %v), want exact stopped replay", replayedStop, err)
	}

	removeRequest := SandboxActionRequest{OperationID: "op-sandbox-remove", SandboxID: createRequest.SandboxID}
	removing, err := coordinator.BeginSandboxRemove(context.Background(), removeRequest)
	if err != nil {
		t.Fatalf("BeginSandboxRemove() error = %v", err)
	}
	if removing.Operation.State != operation.StateRunning || removing.Sandbox == nil {
		t.Fatalf("BeginSandboxRemove() = %#v", removing)
	}
	removed, err := coordinator.ConfirmSandboxRemove(context.Background(), SandboxConfirmRequest{
		OperationID: removeRequest.OperationID, SandboxID: removeRequest.SandboxID,
		Fingerprint: removing.Fingerprint, Verification: testVerification(VerificationSandboxAbsent, nil),
	})
	if err != nil {
		t.Fatalf("ConfirmSandboxRemove() error = %v", err)
	}
	if !removed.Removed || removed.Sandbox != nil || removed.Operation.State != operation.StateSucceeded {
		t.Fatalf("ConfirmSandboxRemove() = %#v", removed)
	}
	if _, err := coordinator.GetSandbox(context.Background(), createRequest.SandboxID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetSandbox(removed) error = %v, want not found", err)
	}
	replayedRemoval, err := coordinator.ConfirmSandboxRemove(context.Background(), SandboxConfirmRequest{
		OperationID: removeRequest.OperationID, SandboxID: removeRequest.SandboxID,
		Fingerprint: removing.Fingerprint, Verification: testVerification(VerificationSandboxAbsent, nil),
	})
	if err != nil || replayedRemoval.Resolution != operation.ResolutionReplay || !replayedRemoval.Removed {
		t.Fatalf("ConfirmSandboxRemove(retry) = (%#v, %v)", replayedRemoval, err)
	}
	replayedCreate, err := coordinator.ConfirmSandboxCreate(context.Background(), confirmation)
	if err != nil || replayedCreate.Resolution != operation.ResolutionReplay || !reflect.DeepEqual(replayedCreate.Sandbox, ready.Sandbox) {
		t.Fatalf("ConfirmSandboxCreate(after removal) = (%#v, %v), want original ready result", replayedCreate, err)
	}

	events, err := coordinator.ListEvents(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(events) != 7 {
		t.Fatalf("event count = %d, want 7 including persisted conflict", len(events))
	}
	for index, event := range events {
		if event.Sequence != operation.Sequence(index+1) {
			t.Fatalf("event[%d].Sequence = %d, want %d", index, event.Sequence, index+1)
		}
	}
	storedOperation, err := coordinator.GetOperation(context.Background(), removeRequest.OperationID)
	if err != nil || storedOperation.State != operation.StateSucceeded {
		t.Fatalf("GetOperation() = (%#v, %v)", storedOperation, err)
	}
	assertStoreReadable(t, store)
}

// TestContainerLifecycleStructuredInputKillAndDelete verifies pair fidelity, start verification, kill planning, outcome, and deletion.
func TestContainerLifecycleStructuredInputKillAndDelete(t *testing.T) {
	coordinator, _ := testCoordinator(t)
	sandbox := createReadySandbox(t, coordinator, "sandbox-container")
	process := domain.ProcessSpec{
		Argv:             []string{"/bin/tool", "", "space value", `quote"value`},
		Environment:      []domain.EnvVar{{Name: "TOKEN", Value: "a=b=c"}, {Name: "EMPTY", Value: ""}},
		WorkingDirectory: "/work dir",
		Termination:      domain.TerminationPolicy{Signal: "SIGTERM", GracePeriod: 3 * time.Second, EscalationSignal: "SIGKILL"},
	}
	createRequest := ContainerCreateRequest{
		OperationID: "op-container-create", SandboxID: sandbox.ID, ContainerID: "container-1", AttemptID: "attempt-1",
		Process: process, ImageDigest: "sha256:verified", RootFS: "snapshot-1",
	}
	creating, err := coordinator.BeginContainerCreate(context.Background(), createRequest)
	if err != nil {
		t.Fatalf("BeginContainerCreate() error = %v", err)
	}
	if creating.ContainerAttempt == nil || creating.ContainerAttempt.Attempt.Phase != domain.AttemptCreating {
		t.Fatalf("BeginContainerCreate() = %#v", creating)
	}
	if !reflect.DeepEqual(creating.ContainerAttempt.Container.Spec.Process, process) {
		t.Fatalf("structured ProcessSpec = %#v, want %#v", creating.ContainerAttempt.Container.Spec.Process, process)
	}
	if creating.ContainerAttempt.Container.Spec.Limits.CPULimitMilli == nil || *creating.ContainerAttempt.Container.Spec.Limits.CPULimitMilli != 500 {
		t.Fatalf("Container limits = %#v", creating.ContainerAttempt.Container.Spec.Limits)
	}
	created, err := coordinator.ConfirmContainerCreate(context.Background(), ContainerConfirmRequest{
		OperationID: createRequest.OperationID, ContainerID: createRequest.ContainerID,
		Fingerprint: creating.Fingerprint, Verification: testVerification(VerificationAttemptCreated, nil),
	})
	if err != nil {
		t.Fatalf("ConfirmContainerCreate() error = %v", err)
	}
	if created.ContainerAttempt == nil || created.ContainerAttempt.Attempt.Phase != domain.AttemptCreated || created.ContainerAttempt.Container.Status.ObservedGeneration != domain.InitialGeneration {
		t.Fatalf("ConfirmContainerCreate() = %#v", created)
	}

	startRequest := ContainerActionRequest{OperationID: "op-container-start", ContainerID: createRequest.ContainerID}
	starting, err := coordinator.BeginContainerStart(context.Background(), startRequest)
	if err != nil {
		t.Fatalf("BeginContainerStart() error = %v", err)
	}
	if starting.ContainerAttempt == nil || starting.ContainerAttempt.Attempt.Phase != domain.AttemptCreated {
		t.Fatalf("BeginContainerStart() changed phase before verification: %#v", starting)
	}
	if _, err := coordinator.ConfirmContainerStart(context.Background(), ContainerConfirmRequest{
		OperationID: startRequest.OperationID, ContainerID: startRequest.ContainerID,
		Fingerprint: starting.Fingerprint, Verification: testVerification(VerificationAttemptRunning, nil),
	}); !errors.Is(err, ErrVerificationRequired) {
		t.Fatalf("ConfirmContainerStart(without identity) error = %v", err)
	}
	identity := testIdentity()
	running, err := coordinator.ConfirmContainerStart(context.Background(), ContainerConfirmRequest{
		OperationID: startRequest.OperationID, ContainerID: startRequest.ContainerID,
		Fingerprint: starting.Fingerprint, Verification: testVerification(VerificationAttemptRunning, &identity),
	})
	if err != nil {
		t.Fatalf("ConfirmContainerStart() error = %v", err)
	}
	if running.ContainerAttempt == nil || running.ContainerAttempt.Attempt.Phase != domain.AttemptRunning || running.ContainerAttempt.Attempt.ProcessIdentity == nil {
		t.Fatalf("ConfirmContainerStart() = %#v", running)
	}
	if running.ContainerAttempt.Attempt.LastObservation.OperationID != string(startRequest.OperationID) || running.ContainerAttempt.Container.Status.LastObservation != running.ContainerAttempt.Attempt.LastObservation {
		t.Fatalf("Running LastObservation projection = %#v / %#v", running.ContainerAttempt.Container.Status.LastObservation, running.ContainerAttempt.Attempt.LastObservation)
	}
	if _, err := coordinator.PlanKill(context.Background(), KillRequest{OperationID: "op-container-kill-missing-policy", ContainerID: createRequest.ContainerID}); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("PlanKill(running without policy) error = %v, want invalid argument", err)
	}
	if _, err := coordinator.GetOperation(context.Background(), "op-container-kill-missing-policy"); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetOperation(rejected missing-policy kill) error = %v, want not found", err)
	}

	policy := process.Termination
	killRequest := KillRequest{OperationID: "op-container-kill", ContainerID: createRequest.ContainerID, Policy: policy}
	planned, err := coordinator.PlanKill(context.Background(), killRequest)
	if err != nil {
		t.Fatalf("PlanKill() error = %v", err)
	}
	if planned.Operation.State != operation.StateRunning || !planned.Actionable || planned.Plan.Signal != "SIGTERM" || planned.ProcessIdentity != identity {
		t.Fatalf("PlanKill() = %#v", planned)
	}
	restoredPolicy, err := ActiveKillPolicy(planned.Operation)
	if err != nil || !reflect.DeepEqual(restoredPolicy, policy) {
		t.Fatalf("ActiveKillPolicy() = (%#v, %v), want original policy", restoredPolicy, err)
	}
	tamperedOperation := planned.Operation.Clone()
	tamperedResponse, err := decodeKillResponse(tamperedOperation.Response)
	if err != nil {
		t.Fatal(err)
	}
	tamperedResponse.Plan.Signal = "SIGUSR1"
	tamperedOperation.Response, err = encodeResponse(tamperedResponse)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ActiveKillPolicy(tamperedOperation); err == nil {
		t.Fatal("ActiveKillPolicy() accepted a plan that differed from the immutable request fingerprint")
	}
	verifier, ok := coordinator.verifier.(*acceptingVerifier)
	if !ok || verifier.calls != 1 {
		t.Fatalf("action-time verifier = %#v, want one call", coordinator.verifier)
	}
	resumed, err := coordinator.PlanKill(context.Background(), killRequest)
	if err != nil || resumed.Resolution != operation.ResolutionResume {
		t.Fatalf("PlanKill(retry) = (%#v, %v)", resumed, err)
	}
	if verifier.calls != 2 {
		t.Fatalf("action-time verifier calls after PlanKill retry = %d, want 2", verifier.calls)
	}
	mismatchedKill := killRequest
	mismatchedKill.Policy.GracePeriod++
	if _, err := coordinator.PlanKill(context.Background(), mismatchedKill); !errors.Is(err, operation.ErrBindingMismatch) {
		t.Fatalf("PlanKill(mismatched retry) error = %v", err)
	}
	outcome := testSignalOutcome()
	killed, err := coordinator.RecordKillStopped(context.Background(), KillStoppedRequest{
		OperationID: killRequest.OperationID, ContainerID: killRequest.ContainerID, Fingerprint: planned.Fingerprint,
		Outcome: outcome, Verification: testVerification(VerificationAttemptStopped, nil),
	})
	if err != nil {
		t.Fatalf("RecordKillStopped() error = %v", err)
	}
	if killed.ContainerAttempt == nil || killed.ContainerAttempt.Attempt.Phase != domain.AttemptStopped || !reflect.DeepEqual(killed.ContainerAttempt.Attempt.Outcome, outcome) {
		t.Fatalf("RecordKillStopped() = %#v", killed)
	}
	if killed.ContainerAttempt.Attempt.LastObservation.OperationID != string(killRequest.OperationID) {
		t.Fatalf("Stopped LastObservation = %#v", killed.ContainerAttempt.Attempt.LastObservation)
	}
	alreadyStopped, err := coordinator.PlanKill(context.Background(), KillRequest{
		OperationID: "op-container-kill-already-stopped", ContainerID: killRequest.ContainerID,
	})
	if err != nil || alreadyStopped.Actionable || alreadyStopped.Plan != (domain.KillPlan{}) || alreadyStopped.Operation.Result != operation.ResultNoop || alreadyStopped.ContainerAttempt == nil || !reflect.DeepEqual(alreadyStopped.ContainerAttempt.Attempt.Outcome, outcome) {
		t.Fatalf("PlanKill(already stopped) = (%#v, %v)", alreadyStopped, err)
	}
	if verifier.calls != 2 {
		t.Fatalf("action-time verifier calls after stopped no-op = %d, want 2", verifier.calls)
	}
	replayedKill, err := coordinator.PlanKill(context.Background(), killRequest)
	if err != nil || replayedKill.Actionable || replayedKill.Resolution != operation.ResolutionReplay || !reflect.DeepEqual(replayedKill.ContainerAttempt, killed.ContainerAttempt) {
		t.Fatalf("PlanKill(after completion) = (%#v, %v)", replayedKill, err)
	}

	deleteRequest := ContainerActionRequest{OperationID: "op-container-delete", ContainerID: createRequest.ContainerID}
	deleting, err := coordinator.BeginContainerDelete(context.Background(), deleteRequest)
	if err != nil {
		t.Fatalf("BeginContainerDelete() error = %v", err)
	}
	deleted, err := coordinator.ConfirmContainerDelete(context.Background(), ContainerConfirmRequest{
		OperationID: deleteRequest.OperationID, ContainerID: deleteRequest.ContainerID,
		Fingerprint: deleting.Fingerprint, Verification: testVerification(VerificationAttemptAbsent, nil),
	})
	if err != nil {
		t.Fatalf("ConfirmContainerDelete() error = %v", err)
	}
	if !deleted.Removed {
		t.Fatalf("ConfirmContainerDelete() = %#v", deleted)
	}
	if _, err := coordinator.GetContainer(context.Background(), createRequest.ContainerID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetContainer(deleted) error = %v", err)
	}
	replayedStart, err := coordinator.ConfirmContainerStart(context.Background(), ContainerConfirmRequest{
		OperationID: startRequest.OperationID, ContainerID: startRequest.ContainerID,
		Fingerprint: starting.Fingerprint, Verification: testVerification(VerificationAttemptRunning, &identity),
	})
	if err != nil || replayedStart.Resolution != operation.ResolutionReplay || !reflect.DeepEqual(replayedStart.ContainerAttempt, running.ContainerAttempt) {
		t.Fatalf("ConfirmContainerStart(after deletion) = (%#v, %v), want original running result", replayedStart, err)
	}
	containers, err := coordinator.ListContainers(context.Background(), sandbox.ID)
	if err != nil || len(containers) != 0 {
		t.Fatalf("ListContainers() = (%#v, %v)", containers, err)
	}
	updatedSandbox, err := coordinator.GetSandbox(context.Background(), sandbox.ID)
	if err != nil || updatedSandbox.Status.CurrentContainerID != nil || updatedSandbox.Status.CurrentAttemptID != nil {
		t.Fatalf("Sandbox current refs after delete = (%#v, %v)", updatedSandbox.Status, err)
	}
	events, err := coordinator.ListEvents(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	for _, event := range events {
		if event.Target.Kind != operation.TargetContainer {
			continue
		}
		if len(event.Resources) != 3 || !containsEventResource(event.Resources, operation.TargetSandbox, string(sandbox.ID)) || !containsEventResource(event.Resources, operation.TargetContainer, string(createRequest.ContainerID)) || !containsEventResource(event.Resources, operation.TargetAttempt, string(createRequest.AttemptID)) {
			t.Fatalf("Container event resources = %#v", event.Resources)
		}
	}
}

// TestDeleteAbsentWithNewOperationIDs verifies each new absent delete intent still requires verification and then replays a no-op.
func TestDeleteAbsentWithNewOperationIDs(t *testing.T) {
	coordinator, _ := testCoordinator(t)
	containerRequest := ContainerActionRequest{OperationID: "op-delete-missing-container-1", ContainerID: "missing-container"}
	containerBegin, err := coordinator.BeginContainerDelete(context.Background(), containerRequest)
	if err != nil || containerBegin.Removed || containerBegin.Operation.State != operation.StateRunning {
		t.Fatalf("BeginContainerDelete(absent intent) = (%#v, %v)", containerBegin, err)
	}
	conflictingContainerRequest := ContainerActionRequest{OperationID: "op-delete-missing-container-conflict", ContainerID: containerRequest.ContainerID}
	conflictingContainer, conflictErr := coordinator.BeginContainerDelete(context.Background(), conflictingContainerRequest)
	assertDurableConflictError(t, conflictErr)
	if conflictingContainer.Operation.State != operation.StateFailed {
		t.Fatalf("BeginContainerDelete(absent conflict) = (%#v, %v)", conflictingContainer, conflictErr)
	}
	replayedConflict, conflictErr := coordinator.BeginContainerDelete(context.Background(), conflictingContainerRequest)
	assertDurableConflictError(t, conflictErr)
	if replayedConflict.Resolution != operation.ResolutionReplay {
		t.Fatalf("BeginContainerDelete(absent conflict retry) = (%#v, %v)", replayedConflict, conflictErr)
	}
	firstContainer, err := coordinator.ConfirmContainerDelete(context.Background(), ContainerConfirmRequest{
		OperationID: containerRequest.OperationID, ContainerID: containerRequest.ContainerID,
		Fingerprint: containerBegin.Fingerprint, Verification: testVerification(VerificationAttemptAbsent, nil),
	})
	if err != nil || !firstContainer.Removed || firstContainer.Operation.State != operation.StateSucceeded || firstContainer.Operation.Result != operation.ResultNoop {
		t.Fatalf("ConfirmContainerDelete(absent) = (%#v, %v)", firstContainer, err)
	}
	replayedContainer, err := coordinator.BeginContainerDelete(context.Background(), containerRequest)
	if err != nil || replayedContainer.Resolution != operation.ResolutionReplay || !reflect.DeepEqual(replayedContainer.Operation.Response, firstContainer.Operation.Response) {
		t.Fatalf("BeginContainerDelete(absent retry) = (%#v, %v)", replayedContainer, err)
	}
	secondContainerBegin, err := coordinator.BeginContainerDelete(context.Background(), ContainerActionRequest{OperationID: "op-delete-missing-container-2", ContainerID: containerRequest.ContainerID})
	if err != nil || secondContainerBegin.Operation.State != operation.StateRunning {
		t.Fatalf("BeginContainerDelete(absent new ID) = (%#v, %v)", secondContainerBegin, err)
	}
	secondContainer, err := coordinator.ConfirmContainerDelete(context.Background(), ContainerConfirmRequest{
		OperationID: secondContainerBegin.Operation.ID, ContainerID: containerRequest.ContainerID,
		Fingerprint: secondContainerBegin.Fingerprint, Verification: testVerification(VerificationAttemptAbsent, nil),
	})
	if err != nil || !secondContainer.Removed || secondContainer.Operation.Result != operation.ResultNoop {
		t.Fatalf("ConfirmContainerDelete(absent new ID) = (%#v, %v)", secondContainer, err)
	}

	sandboxRequest := SandboxActionRequest{OperationID: "op-remove-missing-sandbox-1", SandboxID: "missing-sandbox"}
	sandboxBegin, err := coordinator.BeginSandboxRemove(context.Background(), sandboxRequest)
	if err != nil || sandboxBegin.Removed || sandboxBegin.Operation.State != operation.StateRunning {
		t.Fatalf("BeginSandboxRemove(absent intent) = (%#v, %v)", sandboxBegin, err)
	}
	conflictingSandboxRequest := SandboxActionRequest{OperationID: "op-remove-missing-sandbox-conflict", SandboxID: sandboxRequest.SandboxID}
	conflictingSandbox, conflictErr := coordinator.BeginSandboxRemove(context.Background(), conflictingSandboxRequest)
	assertDurableConflictError(t, conflictErr)
	if conflictingSandbox.Operation.State != operation.StateFailed {
		t.Fatalf("BeginSandboxRemove(absent conflict) = (%#v, %v)", conflictingSandbox, conflictErr)
	}
	replayedSandboxConflict, conflictErr := coordinator.BeginSandboxRemove(context.Background(), conflictingSandboxRequest)
	assertDurableConflictError(t, conflictErr)
	if replayedSandboxConflict.Resolution != operation.ResolutionReplay {
		t.Fatalf("BeginSandboxRemove(absent conflict retry) = (%#v, %v)", replayedSandboxConflict, conflictErr)
	}
	firstSandbox, err := coordinator.ConfirmSandboxRemove(context.Background(), SandboxConfirmRequest{
		OperationID: sandboxRequest.OperationID, SandboxID: sandboxRequest.SandboxID,
		Fingerprint: sandboxBegin.Fingerprint, Verification: testVerification(VerificationSandboxAbsent, nil),
	})
	if err != nil || !firstSandbox.Removed || firstSandbox.Operation.State != operation.StateSucceeded || firstSandbox.Operation.Result != operation.ResultNoop {
		t.Fatalf("ConfirmSandboxRemove(absent) = (%#v, %v)", firstSandbox, err)
	}
	secondSandboxBegin, err := coordinator.BeginSandboxRemove(context.Background(), SandboxActionRequest{OperationID: "op-remove-missing-sandbox-2", SandboxID: sandboxRequest.SandboxID})
	if err != nil || secondSandboxBegin.Operation.State != operation.StateRunning {
		t.Fatalf("BeginSandboxRemove(absent new ID) = (%#v, %v)", secondSandboxBegin, err)
	}
	secondSandbox, err := coordinator.ConfirmSandboxRemove(context.Background(), SandboxConfirmRequest{
		OperationID: secondSandboxBegin.Operation.ID, SandboxID: sandboxRequest.SandboxID,
		Fingerprint: secondSandboxBegin.Fingerprint, Verification: testVerification(VerificationSandboxAbsent, nil),
	})
	if err != nil || !secondSandbox.Removed || secondSandbox.Operation.Result != operation.ResultNoop {
		t.Fatalf("ConfirmSandboxRemove(absent new ID) = (%#v, %v)", secondSandbox, err)
	}
}

// TestRecordStoppedStandaloneReplaysExitZero verifies explicit exit-zero presence and canonical mismatch handling.
func TestRecordStoppedStandaloneReplaysExitZero(t *testing.T) {
	coordinator, _ := testCoordinator(t)
	sandbox := createReadySandbox(t, coordinator, "sandbox-exit")
	pair := createRunningContainer(t, coordinator, sandbox.ID, "container-exit", "attempt-exit", "exit")
	outcome := testExitOutcome(0)
	request := RecordStoppedRequest{
		OperationID: "op-record-exit", ContainerID: pair.Container.ID, Outcome: outcome,
		Verification: testVerification(VerificationAttemptStopped, nil),
	}
	stopped, err := coordinator.RecordStopped(context.Background(), request)
	if err != nil {
		t.Fatalf("RecordStopped() error = %v", err)
	}
	if stopped.ContainerAttempt == nil || stopped.ContainerAttempt.Attempt.Outcome.ExitCode == nil || *stopped.ContainerAttempt.Attempt.Outcome.ExitCode != 0 {
		t.Fatalf("RecordStopped(exit zero) = %#v", stopped)
	}
	replayed, err := coordinator.RecordStopped(context.Background(), request)
	if err != nil || replayed.Resolution != operation.ResolutionReplay || replayed.ContainerAttempt.Attempt.Outcome.ExitCode == nil {
		t.Fatalf("RecordStopped(retry) = (%#v, %v)", replayed, err)
	}
	mismatch := request
	mismatch.Outcome = testExitOutcome(1)
	if _, err := coordinator.RecordStopped(context.Background(), mismatch); !errors.Is(err, operation.ErrBindingMismatch) {
		t.Fatalf("RecordStopped(mismatched retry) error = %v", err)
	}
}

// TestSequentialAttemptHistoryAdvancesCurrentRefs verifies a terminal pair remains immutable while the Sandbox points at one new active pair.
func TestSequentialAttemptHistoryAdvancesCurrentRefs(t *testing.T) {
	coordinator, _ := testCoordinator(t)
	sandbox := createReadySandbox(t, coordinator, "sandbox-sequential")
	first := createRunningContainer(t, coordinator, sandbox.ID, "container-sequential-1", "attempt-sequential-1", "sequential-1")
	firstOutcome := testExitOutcome(0)
	_, err := coordinator.RecordStopped(context.Background(), RecordStoppedRequest{
		OperationID: "op-stop-sequential-1", ContainerID: first.Container.ID, Outcome: firstOutcome,
		Verification: testVerification(VerificationAttemptStopped, nil),
	})
	if err != nil {
		t.Fatalf("RecordStopped(first sequential pair) error = %v", err)
	}
	secondRequest := ContainerCreateRequest{
		OperationID: "op-create-sequential-2", SandboxID: sandbox.ID,
		ContainerID: "container-sequential-2", AttemptID: "attempt-sequential-2", Process: testProcessSpec(),
	}
	secondBegin, err := coordinator.BeginContainerCreate(context.Background(), secondRequest)
	if err != nil {
		t.Fatalf("BeginContainerCreate(second sequential pair) error = %v", err)
	}
	second, err := coordinator.ConfirmContainerCreate(context.Background(), ContainerConfirmRequest{
		OperationID: secondRequest.OperationID, ContainerID: secondRequest.ContainerID,
		Fingerprint: secondBegin.Fingerprint, Verification: testVerification(VerificationAttemptCreated, nil),
	})
	if err != nil {
		t.Fatalf("ConfirmContainerCreate(second sequential pair) error = %v", err)
	}
	if _, err := coordinator.BeginContainerCreate(context.Background(), ContainerCreateRequest{
		OperationID: "op-create-sequential-blocked", SandboxID: sandbox.ID,
		ContainerID: "container-sequential-3", AttemptID: "attempt-sequential-3", Process: testProcessSpec(),
	}); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("BeginContainerCreate(while second active) error = %v, want failed precondition", err)
	}
	history, err := coordinator.ListContainers(context.Background(), sandbox.ID)
	if err != nil || len(history) != 2 {
		t.Fatalf("ListContainers(sequential history) = (%#v, %v)", history, err)
	}
	if !reflect.DeepEqual(history[0].Attempt.Outcome, firstOutcome) || history[0].Attempt.Phase != domain.AttemptStopped || history[1].Attempt.ID != second.ContainerAttempt.Attempt.ID {
		t.Fatalf("sequential history = %#v", history)
	}
	current, err := coordinator.GetSandbox(context.Background(), sandbox.ID)
	if err != nil || current.Status.CurrentContainerID == nil || *current.Status.CurrentContainerID != second.ContainerAttempt.Container.ID || current.Status.CurrentAttemptID == nil || *current.Status.CurrentAttemptID != second.ContainerAttempt.Attempt.ID {
		t.Fatalf("Sandbox current refs after sequential create = (%#v, %v)", current.Status, err)
	}
}

// TestContainerDeleteBeforeStartAndSandboxRemoveGuards verifies not-run outcome and non-cascading Sandbox removal.
func TestContainerDeleteBeforeStartAndSandboxRemoveGuards(t *testing.T) {
	coordinator, _ := testCoordinator(t)
	sandbox := createReadySandbox(t, coordinator, "sandbox-guard")
	createRequest := ContainerCreateRequest{
		OperationID: "op-guard-create", SandboxID: sandbox.ID, ContainerID: "container-guard", AttemptID: "attempt-guard",
		Process: testProcessSpec(),
	}
	creating, err := coordinator.BeginContainerCreate(context.Background(), createRequest)
	if err != nil {
		t.Fatalf("BeginContainerCreate() error = %v", err)
	}
	_, err = coordinator.ConfirmContainerCreate(context.Background(), ContainerConfirmRequest{
		OperationID: createRequest.OperationID, ContainerID: createRequest.ContainerID,
		Fingerprint: creating.Fingerprint, Verification: testVerification(VerificationAttemptCreated, nil),
	})
	if err != nil {
		t.Fatalf("ConfirmContainerCreate() error = %v", err)
	}
	if _, err := coordinator.BeginSandboxStop(context.Background(), SandboxActionRequest{OperationID: "op-guard-stop-active", SandboxID: sandbox.ID}); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("BeginSandboxStop(active Attempt) error = %v", err)
	}
	deleting, err := coordinator.BeginContainerDelete(context.Background(), ContainerActionRequest{OperationID: "op-guard-delete", ContainerID: createRequest.ContainerID})
	if err != nil {
		t.Fatalf("BeginContainerDelete(created) error = %v", err)
	}
	if deleting.ContainerAttempt == nil || deleting.ContainerAttempt.Attempt.Outcome.Presence != domain.OutcomeNotApplicable {
		t.Fatalf("BeginContainerDelete(created) outcome = %#v", deleting)
	}
	stopping, err := coordinator.BeginSandboxStop(context.Background(), SandboxActionRequest{OperationID: "op-guard-stop", SandboxID: sandbox.ID})
	if err != nil {
		t.Fatalf("BeginSandboxStop() error = %v", err)
	}
	_, err = coordinator.ConfirmSandboxStop(context.Background(), SandboxConfirmRequest{
		OperationID: "op-guard-stop", SandboxID: sandbox.ID, Fingerprint: stopping.Fingerprint,
		Verification: testVerification(VerificationSandboxStopped, nil),
	})
	if err != nil {
		t.Fatalf("ConfirmSandboxStop() error = %v", err)
	}
	if _, err := coordinator.BeginSandboxRemove(context.Background(), SandboxActionRequest{OperationID: "op-guard-remove-too-soon", SandboxID: sandbox.ID}); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("BeginSandboxRemove(with retained Container) error = %v", err)
	}
	_, err = coordinator.ConfirmContainerDelete(context.Background(), ContainerConfirmRequest{
		OperationID: "op-guard-delete", ContainerID: createRequest.ContainerID, Fingerprint: deleting.Fingerprint,
		Verification: testVerification(VerificationAttemptAbsent, nil),
	})
	if err != nil {
		t.Fatalf("ConfirmContainerDelete() error = %v", err)
	}
	removing, err := coordinator.BeginSandboxRemove(context.Background(), SandboxActionRequest{OperationID: "op-guard-remove", SandboxID: sandbox.ID})
	if err != nil || removing.Sandbox == nil {
		t.Fatalf("BeginSandboxRemove(after explicit delete) = (%#v, %v)", removing, err)
	}
}

// TestLifecycleTransactionRollsBackWhenEventFails verifies resource and operation writes never escape a failed event append.
func TestLifecycleTransactionRollsBackWhenEventFails(t *testing.T) {
	store := state.NewMemoryStore()
	coordinator, err := NewCoordinatorWithClock(store, fixedClock{})
	if err != nil {
		t.Fatalf("NewCoordinatorWithClock() error = %v", err)
	}
	request := SandboxCreateRequest{OperationID: "op-atomic", SandboxID: "sandbox-atomic", Spec: testSandboxSpec()}
	if _, err := coordinator.BeginSandboxCreate(context.Background(), request); err == nil {
		t.Fatal("BeginSandboxCreate() error = nil, want invalid zero event timestamp")
	}
	if _, err := coordinator.GetSandbox(context.Background(), request.SandboxID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetSandbox(after rollback) error = %v", err)
	}
	if _, err := coordinator.GetOperation(context.Background(), request.OperationID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetOperation(after rollback) error = %v", err)
	}
	events, err := coordinator.ListEvents(context.Background(), 0, 0)
	if err != nil || len(events) != 0 {
		t.Fatalf("ListEvents(after rollback) = (%#v, %v)", events, err)
	}
}

// TestVerificationKindAndFingerprintCannotBeSubstituted verifies Confirm rejects wrong evidence and wrong binding without mutation.
func TestVerificationKindAndFingerprintCannotBeSubstituted(t *testing.T) {
	coordinator, _ := testCoordinator(t)
	request := SandboxCreateRequest{OperationID: "op-verify", SandboxID: "sandbox-verify", Spec: testSandboxSpec()}
	begin, err := coordinator.BeginSandboxCreate(context.Background(), request)
	if err != nil {
		t.Fatalf("BeginSandboxCreate() error = %v", err)
	}
	wrongKind := testVerification(VerificationSandboxStopped, nil)
	if _, err := coordinator.ConfirmSandboxCreate(context.Background(), SandboxConfirmRequest{
		OperationID: request.OperationID, SandboxID: request.SandboxID, Fingerprint: begin.Fingerprint, Verification: wrongKind,
	}); !errors.Is(err, ErrVerificationRequired) {
		t.Fatalf("ConfirmSandboxCreate(wrong kind) error = %v", err)
	}
	wrongFingerprint, err := operation.CanonicalRequestFingerprint(map[string]string{"different": "request"})
	if err != nil {
		t.Fatalf("CanonicalRequestFingerprint() error = %v", err)
	}
	if _, err := coordinator.ConfirmSandboxCreate(context.Background(), SandboxConfirmRequest{
		OperationID: request.OperationID, SandboxID: request.SandboxID, Fingerprint: wrongFingerprint,
		Verification: testVerification(VerificationSandboxReady, nil),
	}); !errors.Is(err, operation.ErrBindingMismatch) {
		t.Fatalf("ConfirmSandboxCreate(wrong fingerprint) error = %v", err)
	}
	sandbox, err := coordinator.GetSandbox(context.Background(), request.SandboxID)
	if err != nil || sandbox.Status.Phase != domain.SandboxCreating {
		t.Fatalf("GetSandbox(after rejected confirmations) = (%#v, %v)", sandbox, err)
	}
}

// TestPlanKillFailsClosedWithoutActionVerifier verifies persisted identity shape never authorizes a kill plan alone.
func TestPlanKillFailsClosedWithoutActionVerifier(t *testing.T) {
	coordinator, store := testCoordinator(t)
	sandbox := createReadySandbox(t, coordinator, "sandbox-no-verifier")
	pair := createRunningContainer(t, coordinator, sandbox.ID, "container-no-verifier", "attempt-no-verifier", "no-verifier")
	withoutVerifier, err := NewCoordinatorWithClock(store, fixedClock{now: testWallTime()})
	if err != nil {
		t.Fatalf("NewCoordinatorWithClock() error = %v", err)
	}
	request := KillRequest{OperationID: "op-no-verifier-kill", ContainerID: pair.Container.ID, Policy: pair.Container.Spec.Process.Termination}
	if _, err := withoutVerifier.PlanKill(context.Background(), request); !errors.Is(err, ErrProcessVerificationUnavailable) {
		t.Fatalf("PlanKill(without action verifier) error = %v", err)
	}
	if _, err := withoutVerifier.GetOperation(context.Background(), request.OperationID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetOperation(rejected kill plan) error = %v", err)
	}
}

// testCoordinator builds a deterministic coordinator and returns its inspectable memory store.
func testCoordinator(t *testing.T) (*Coordinator, *state.MemoryStore) {
	t.Helper()
	store := state.NewMemoryStore()
	coordinator := testCoordinatorForStoreProfile(t, store, state.HostProfileAbstractM1)
	return coordinator, store
}

// testCoordinatorForStoreProfile constructs a deterministic coordinator over an existing store and explicit host contract.
func testCoordinatorForStoreProfile(t *testing.T, store *state.MemoryStore, profile state.HostProfile) *Coordinator {
	t.Helper()
	coordinator, err := NewCoordinatorWithClockForProfile(store, fixedClock{now: testWallTime()}, profile, &acceptingVerifier{})
	if err != nil {
		t.Fatalf("NewCoordinatorWithClockForProfile() error = %v", err)
	}
	return coordinator
}

// createReadySandbox runs both create phases so Container tests start from a verified Ready environment.
func createReadySandbox(t *testing.T, coordinator *Coordinator, id domain.SandboxID) domain.Sandbox {
	t.Helper()
	operationID := operation.OperationID("op-create-" + string(id))
	begin, err := coordinator.BeginSandboxCreate(context.Background(), SandboxCreateRequest{OperationID: operationID, SandboxID: id, Spec: testSandboxSpec()})
	if err != nil {
		t.Fatalf("BeginSandboxCreate(%s) error = %v", id, err)
	}
	confirmed, err := coordinator.ConfirmSandboxCreate(context.Background(), SandboxConfirmRequest{
		OperationID: operationID, SandboxID: id, Fingerprint: begin.Fingerprint,
		Verification: testVerification(VerificationSandboxReady, nil),
	})
	if err != nil {
		t.Fatalf("ConfirmSandboxCreate(%s) error = %v", id, err)
	}
	return confirmed.Sandbox.Clone()
}

// createRunningContainer runs create and start confirmation for terminal-observation tests.
func createRunningContainer(t *testing.T, coordinator *Coordinator, sandboxID domain.SandboxID, containerID domain.ContainerID, attemptID domain.AttemptID, suffix string) domain.ContainerAttempt {
	t.Helper()
	createID := operation.OperationID("op-create-container-" + suffix)
	begin, err := coordinator.BeginContainerCreate(context.Background(), ContainerCreateRequest{
		OperationID: createID, SandboxID: sandboxID, ContainerID: containerID, AttemptID: attemptID, Process: testProcessSpec(),
	})
	if err != nil {
		t.Fatalf("BeginContainerCreate(%s) error = %v", containerID, err)
	}
	_, err = coordinator.ConfirmContainerCreate(context.Background(), ContainerConfirmRequest{
		OperationID: createID, ContainerID: containerID, Fingerprint: begin.Fingerprint,
		Verification: testVerification(VerificationAttemptCreated, nil),
	})
	if err != nil {
		t.Fatalf("ConfirmContainerCreate(%s) error = %v", containerID, err)
	}
	startID := operation.OperationID("op-start-container-" + suffix)
	starting, err := coordinator.BeginContainerStart(context.Background(), ContainerActionRequest{OperationID: startID, ContainerID: containerID})
	if err != nil {
		t.Fatalf("BeginContainerStart(%s) error = %v", containerID, err)
	}
	identity := testIdentity()
	running, err := coordinator.ConfirmContainerStart(context.Background(), ContainerConfirmRequest{
		OperationID: startID, ContainerID: containerID, Fingerprint: starting.Fingerprint,
		Verification: testVerification(VerificationAttemptRunning, &identity),
	})
	if err != nil {
		t.Fatalf("ConfirmContainerStart(%s) error = %v", containerID, err)
	}
	return running.ContainerAttempt.Clone()
}

// testSandboxSpec returns nested immutable input with one enforcement limit for copy checks.
func testSandboxSpec() domain.SandboxSpec {
	cpu := int64(500)
	return domain.SandboxSpec{
		Hostname: "workload", DNS: []string{"1.1.1.1"}, Labels: map[string]string{"app": "test"},
		Resources: domain.Resources{Limits: domain.ResourceLimits{CPULimitMilli: &cpu}},
	}
}

// testProcessSpec returns structured argv, environment, and explicit graceful-stop intent.
func testProcessSpec() domain.ProcessSpec {
	return domain.ProcessSpec{
		Argv: []string{"/bin/work", "argument"}, Environment: []domain.EnvVar{{Name: "MODE", Value: "test"}},
		Termination: domain.TerminationPolicy{Signal: "SIGTERM", GracePeriod: time.Second, EscalationSignal: "SIGKILL"},
	}
}

// testIdentity returns verified opaque evidence with no numeric or bare PID representation.
func testIdentity() domain.ProcessIdentity {
	return domain.ProcessIdentity{Verified: true, Handle: "strong-handle-token", Evidence: "independent-owner-proof"}
}

// testVerification returns one explicit external observation at deterministic time and duration.
func testVerification(kind VerificationKind, identity *domain.ProcessIdentity) Verification {
	duration := operation.Duration(5 * time.Millisecond)
	return Verification{
		Kind: kind, Verified: true, Evidence: "test-provider-observed", ObservedAt: testWallTime(),
		Duration: &duration, ProcessIdentity: identity,
		Streams: domain.StreamReferences{Stdout: "stream://stdout", Stderr: "stream://stderr"},
	}
}

// testExitOutcome returns captured normal-exit facts while preserving an explicit code pointer.
func testExitOutcome(code int32) domain.Outcome {
	started := testWallTime().Add(-time.Second)
	finished := testWallTime()
	return domain.ExitOutcome(code, domain.EvidenceFalse, started, finished, time.Second)
}

// testSignalOutcome returns captured signal and OOM evidence without performing a signal operation.
func testSignalOutcome() domain.Outcome {
	started := testWallTime().Add(-2 * time.Second)
	finished := testWallTime()
	return domain.SignalOutcome("SIGKILL", domain.EvidenceTrue, started, finished, 2*time.Second)
}

// testWallTime returns the stable non-zero wall fact used by events and outcomes.
func testWallTime() time.Time {
	return time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
}

// containsEventResource reports whether an event's related identities contain one exact typed target.
func containsEventResource(resources []operation.Target, kind operation.TargetKind, id string) bool {
	for _, resource := range resources {
		if resource.Kind == kind && resource.ID == id {
			return true
		}
	}
	return false
}

// assertDurableConflictError verifies a conflict already committed as a
// terminal operation uses stable failure classification instead of a transient
// active-operation retry signal.
func assertDurableConflictError(t *testing.T, err error) {
	t.Helper()
	var failure *OperationFailureError
	if !errors.As(err, &failure) || failure.Reason != operation.ReasonConflict {
		t.Fatalf("error = %v, want durable conflict failure", err)
	}
}

// assertStoreReadable verifies the coordinator left the shared store in a valid committed state.
func assertStoreReadable(t *testing.T, store state.Store) {
	t.Helper()
	if err := store.View(context.Background(), func(reader state.Reader) error {
		_, err := reader.ListSandboxes()
		return err
	}); err != nil {
		t.Fatalf("Store.View() after lifecycle operations error = %v", err)
	}
}
