package domain

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestSandboxAggregateTransitions verifies explicit confirmation, immutable generation, and atomic current references.
func TestSandboxAggregateTransitions(t *testing.T) {
	sandbox, err := NewSandbox("sandbox-transition", SandboxSpec{Network: NetworkIntent{Mode: "isolated"}})
	if err != nil {
		t.Fatalf("NewSandbox() error = %v", err)
	}
	if err := sandbox.Transition(SandboxReady); err != nil {
		t.Fatalf("Transition(Ready) error = %v", err)
	}
	if sandbox.Status.ObservedGeneration != InitialGeneration {
		t.Fatalf("observed generation = %d, want %d", sandbox.Status.ObservedGeneration, InitialGeneration)
	}
	if err := sandbox.SetCurrentPair("container-transition", "attempt-transition"); err != nil {
		t.Fatalf("SetCurrentPair() error = %v", err)
	}
	clone := sandbox.Clone()
	*clone.Status.CurrentContainerID = "container-mutated"
	if *sandbox.Status.CurrentContainerID != "container-transition" {
		t.Fatal("Sandbox.Clone() retained current-reference aliases")
	}
	if err := sandbox.ClearCurrentPair("other", "attempt-transition"); !IsCode(err, CodeFailedPrecondition) {
		t.Fatalf("ClearCurrentPair(mismatch) error = %v", err)
	}
	if err := sandbox.ClearCurrentPair("container-transition", "attempt-transition"); err != nil {
		t.Fatalf("ClearCurrentPair() error = %v", err)
	}
	if err := sandbox.Transition(SandboxStopping); err != nil {
		t.Fatalf("Transition(Stopping) error = %v", err)
	}
	if err := sandbox.Transition(SandboxStopped); err != nil {
		t.Fatalf("Transition(Stopped) error = %v", err)
	}
	if sandbox.Status.Generation != InitialGeneration {
		t.Fatalf("lifecycle transitions changed generation to %d", sandbox.Status.Generation)
	}
}

// TestContainerAttemptVerifiedLifecycle verifies two-phase created/running confirmations and terminal outcome projection.
func TestContainerAttemptVerifiedLifecycle(t *testing.T) {
	pair := testPair(t, AttemptCreating, "container-lifecycle", "attempt-lifecycle")
	streams := StreamReferences{Stdout: "stream://stdout", Stderr: "stream://stderr"}
	if err := pair.SetStreams(streams); err != nil {
		t.Fatalf("SetStreams() error = %v", err)
	}
	if err := pair.Transition(AttemptCreated, PendingOutcome()); err != nil {
		t.Fatalf("Transition(Created) error = %v", err)
	}
	identity := ProcessIdentity{Verified: true, Handle: "pidfd-token", Evidence: "owner-proof"}
	if err := pair.SetProcessIdentity(identity); err != nil {
		t.Fatalf("SetProcessIdentity() error = %v", err)
	}
	replacement := ProcessIdentity{Verified: true, Handle: "different-handle", Evidence: "different-proof"}
	if err := pair.SetProcessIdentity(replacement); !IsCode(err, CodeFailedPrecondition) {
		t.Fatalf("SetProcessIdentity(replacement) error = %v, want failed precondition", err)
	}
	if err := pair.Transition(AttemptRunning, PendingOutcome()); err != nil {
		t.Fatalf("Transition(Running) error = %v", err)
	}
	started := time.Now()
	finished := started.Add(2 * time.Second)
	outcome := ExitOutcome(0, EvidenceFalse, started, finished, 2*time.Second)
	if err := pair.Transition(AttemptStopped, outcome); err != nil {
		t.Fatalf("Transition(Stopped) error = %v", err)
	}
	if pair.Container.Status.Outcome.ExitCode == nil || *pair.Container.Status.Outcome.ExitCode != 0 {
		t.Fatalf("projected outcome = %#v, want explicit exit zero", pair.Container.Status.Outcome)
	}
	if pair.Container.Status.Generation != InitialGeneration || pair.Container.Status.ObservedGeneration != InitialGeneration {
		t.Fatalf("Container generation changed across lifecycle: %#v", pair.Container.Status)
	}
	if pair.Container.Status.Streams != streams || pair.Container.Status.ProcessIdentity == nil {
		t.Fatalf("Container status did not project streams/identity: %#v", pair.Container.Status)
	}
}

// TestContainerAttemptRejectsUnverifiedRunning verifies a start confirmation cannot invent Running without strong evidence.
func TestContainerAttemptRejectsUnverifiedRunning(t *testing.T) {
	pair := testPair(t, AttemptCreating, "container-unverified", "attempt-unverified")
	if err := pair.Transition(AttemptCreated, PendingOutcome()); err != nil {
		t.Fatalf("Transition(Created) error = %v", err)
	}
	before := pair.Clone()
	if err := pair.Transition(AttemptRunning, PendingOutcome()); !IsCode(err, CodeUnsafeIdentity) {
		t.Fatalf("Transition(Running without identity) error = %v", err)
	}
	if pair.Attempt.Phase != before.Attempt.Phase || pair.Container.Status.Phase != before.Container.Status.Phase {
		t.Fatalf("failed transition mutated aggregate: %#v", pair)
	}
}

// TestUnknownOutcomeRequiresCondition verifies missing terminal evidence remains explicit instead of becoming success.
func TestUnknownOutcomeRequiresCondition(t *testing.T) {
	pair := testPair(t, AttemptRunning, "container-unknown", "attempt-unknown")
	if err := pair.Transition(AttemptStopped, UnknownOutcome(EvidenceUnknown)); !IsCode(err, CodeOutcomeUnknown) {
		t.Fatalf("Transition(Unknown without condition) error = %v", err)
	}
	condition := Condition{Type: ConditionOutcomeUnknown, Reason: "SupervisorEvidenceMissing"}
	if err := pair.SetConditions([]Condition{condition}); err != nil {
		t.Fatalf("SetConditions() error = %v", err)
	}
	if err := pair.Transition(AttemptStopped, UnknownOutcome(EvidenceUnknown)); err != nil {
		t.Fatalf("Transition(Unknown with condition) error = %v", err)
	}
	if pair.Container.Status.Outcome.Presence != OutcomeUnknown {
		t.Fatalf("unknown outcome projection = %#v", pair.Container.Status.Outcome)
	}
}

// TestSetConditionsFailureLeavesAggregateUnchanged verifies reconcilers cannot corrupt a valid terminal projection when a replacement omits a required condition.
func TestSetConditionsFailureLeavesAggregateUnchanged(t *testing.T) {
	pair := testPair(t, AttemptRunning, "container-condition-atomic", "attempt-condition-atomic")
	conditions := []Condition{{Type: ConditionOutcomeUnknown, Reason: "terminal-evidence-missing"}}
	if err := pair.SetConditions(conditions); err != nil {
		t.Fatalf("SetConditions(required unknown condition) error = %v", err)
	}
	if err := pair.Transition(AttemptStopped, UnknownOutcome(EvidenceUnknown)); err != nil {
		t.Fatalf("Transition(Stopped unknown) error = %v", err)
	}
	before := pair.Clone()
	if err := pair.SetConditions(nil); !IsCode(err, CodeOutcomeUnknown) {
		t.Fatalf("SetConditions(without required unknown condition) error = %v, want outcome unknown", err)
	}
	if !reflect.DeepEqual(pair, before) {
		t.Fatalf("SetConditions() mutated aggregate after rejection: got %#v want %#v", pair, before)
	}
}

// TestConditionUpsertAndClearPreserveUnrelatedFacts verifies reconcilers update one condition type without clobbering another.
func TestConditionUpsertAndClearPreserveUnrelatedFacts(t *testing.T) {
	sandbox, err := NewSandbox("sandbox-conditions", SandboxSpec{})
	if err != nil {
		t.Fatalf("NewSandbox() error = %v", err)
	}
	failure := Condition{Type: ConditionFailure, Reason: "HostPreflight", Message: "rootful capability missing"}
	cleanup := Condition{Type: ConditionCleanupPending, Reason: "CgroupBusy"}
	if err := sandbox.UpsertCondition(failure); err != nil {
		t.Fatalf("Sandbox.UpsertCondition(failure) error = %v", err)
	}
	if err := sandbox.UpsertCondition(cleanup); err != nil {
		t.Fatalf("Sandbox.UpsertCondition(cleanup) error = %v", err)
	}
	replacement := failure
	replacement.Reason = "NamespaceUnavailable"
	if err := sandbox.UpsertCondition(replacement); err != nil {
		t.Fatalf("Sandbox.UpsertCondition(replacement) error = %v", err)
	}
	if len(sandbox.Status.Conditions) != 2 || sandbox.Status.Conditions[0].Reason != replacement.Reason {
		t.Fatalf("Sandbox conditions = %#v", sandbox.Status.Conditions)
	}
	if err := sandbox.ClearCondition(ConditionFailure); err != nil {
		t.Fatalf("Sandbox.ClearCondition() error = %v", err)
	}
	if len(sandbox.Status.Conditions) != 1 || sandbox.Status.Conditions[0].Type != ConditionCleanupPending {
		t.Fatalf("Sandbox conditions after clear = %#v", sandbox.Status.Conditions)
	}

	pair := testPair(t, AttemptCreating, "container-conditions", "attempt-conditions")
	if err := pair.UpsertCondition(failure); err != nil {
		t.Fatalf("ContainerAttempt.UpsertCondition() error = %v", err)
	}
	if err := pair.ClearCondition(ConditionFailure); err != nil {
		t.Fatalf("ContainerAttempt.ClearCondition() error = %v", err)
	}
	if len(pair.Attempt.Conditions) != 0 || len(pair.Container.Status.Conditions) != 0 {
		t.Fatalf("Container/Attempt condition projection = %#v / %#v", pair.Container.Status.Conditions, pair.Attempt.Conditions)
	}
}

// TestRequiredUnknownConditionCannotBeCleared verifies a targeted condition mutation remains atomic when terminal evidence depends on it.
func TestRequiredUnknownConditionCannotBeCleared(t *testing.T) {
	pair := testPair(t, AttemptRunning, "container-required-condition", "attempt-required-condition")
	condition := Condition{Type: ConditionOutcomeUnknown, Reason: "RecoveryEvidenceMissing"}
	if err := pair.UpsertCondition(condition); err != nil {
		t.Fatalf("UpsertCondition() error = %v", err)
	}
	if err := pair.Transition(AttemptStopped, UnknownOutcome(EvidenceUnknown)); err != nil {
		t.Fatalf("Transition(Stopped unknown) error = %v", err)
	}
	before := pair.Clone()
	if err := pair.ClearCondition(ConditionOutcomeUnknown); !IsCode(err, CodeOutcomeUnknown) {
		t.Fatalf("ClearCondition(required) error = %v, want CodeOutcomeUnknown", err)
	}
	if !reflect.DeepEqual(pair, before) {
		t.Fatalf("ClearCondition(required) mutated aggregate: got %#v want %#v", pair, before)
	}
}

// TestCreateRollbackUsesNotApplicableOutcome verifies preparation failure can terminate without inventing process facts.
func TestCreateRollbackUsesNotApplicableOutcome(t *testing.T) {
	pair := testPair(t, AttemptCreating, "container-rollback", "attempt-rollback")
	if err := pair.Transition(AttemptStopped, NotApplicableOutcome()); err != nil {
		t.Fatalf("Transition(create rollback) error = %v", err)
	}
	if pair.Attempt.Outcome.Presence != OutcomeNotApplicable || pair.Attempt.ProcessIdentity != nil {
		t.Fatalf("create rollback outcome = %#v, identity = %#v", pair.Attempt.Outcome, pair.Attempt.ProcessIdentity)
	}
}

// TestAttemptTransitionOutcomeGuards verifies terminal presence agrees with whether the workload crossed Running.
func TestAttemptTransitionOutcomeGuards(t *testing.T) {
	creating := testPair(t, AttemptCreating, "container-edge-creating", "attempt-edge-creating")
	started := time.Now()
	finished := started.Add(time.Second)
	if err := creating.Transition(AttemptStopped, ExitOutcome(0, EvidenceFalse, started, finished, time.Second)); !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("creating -> stopped captured outcome error = %v", err)
	}
	running := testPair(t, AttemptRunning, "container-edge-running", "attempt-edge-running")
	if err := running.Transition(AttemptStopped, NotApplicableOutcome()); !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("running -> stopped not-applicable outcome error = %v", err)
	}
}

// TestOutcomeValidationMatrix verifies explicit presence rejects contradictory or incomplete result facts.
func TestOutcomeValidationMatrix(t *testing.T) {
	now := time.Now()
	duration := time.Second
	zero := int32(0)
	tests := []Outcome{
		{},
		{Presence: OutcomePending, OOM: EvidenceTrue},
		{Presence: OutcomeNotApplicable, OOM: EvidenceUnknown, StartedAt: &now},
		{Presence: OutcomeCaptured, ExitCode: &zero, Signal: "SIGTERM", OOM: EvidenceFalse, StartedAt: &now, FinishedAt: &now, RunningDuration: &duration},
		{Presence: OutcomeUnknown, Signal: "SIGKILL", OOM: EvidenceUnknown},
	}
	for index, outcome := range tests {
		if err := outcome.Validate(); !IsCode(err, CodeInvalidArgument) {
			t.Fatalf("Outcome[%d].Validate() error = %v, want invalid argument", index, err)
		}
	}
}

// TestSandboxSpecValidation verifies future network intent and current references remain data-only and persistence-safe.
func TestSandboxSpecValidation(t *testing.T) {
	invalidSpecs := []SandboxSpec{
		{Hostname: "bad\x00host"},
		{Hostname: strings.Repeat("x", 65)},
		{Hostname: string([]byte{0xff})},
		{DNS: []string{""}},
		{DNS: []string{"resolver.example"}},
		{DNS: []string{"1.1.1.1", "8.8.8.8", "9.9.9.9", "2001:4860:4860::8888"}},
		{Labels: map[string]string{"": "value"}},
		{Network: NetworkIntent{Attachments: []string{""}}},
	}
	for index, spec := range invalidSpecs {
		if err := spec.Validate(); !IsCode(err, CodeInvalidArgument) {
			t.Fatalf("SandboxSpec[%d].Validate() error = %v", index, err)
		}
	}
}
