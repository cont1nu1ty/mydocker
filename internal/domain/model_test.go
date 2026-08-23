package domain

import (
	"encoding/json"
	"testing"
	"time"
)

// TestNewSandboxCopiesSpecAndInitializesGeneration verifies immutable create intent semantics.
func TestNewSandboxCopiesSpecAndInitializesGeneration(t *testing.T) {
	cpu := int64(500)
	spec := SandboxSpec{
		DNS: []string{"1.1.1.1"}, Labels: map[string]string{"app": "demo"},
		Resources: Resources{Limits: ResourceLimits{CPULimitMilli: &cpu}},
	}
	sandbox, err := NewSandbox("sandbox-1", spec)
	if err != nil {
		t.Fatalf("NewSandbox() error = %v", err)
	}
	if sandbox.Status.Phase != SandboxCreating || sandbox.Status.Generation != 1 || sandbox.Status.ObservedGeneration != 0 {
		t.Fatalf("NewSandbox() status = %#v", sandbox.Status)
	}
	spec.DNS[0] = "changed"
	spec.Labels["app"] = "changed"
	*spec.Resources.Limits.CPULimitMilli = 900
	if sandbox.Spec.DNS[0] != "1.1.1.1" || sandbox.Spec.Labels["app"] != "demo" ||
		*sandbox.Spec.Resources.Limits.CPULimitMilli != 500 {
		t.Fatal("NewSandbox() retained caller-owned aliases")
	}
}

// TestNewContainerAttemptCopiesLimits verifies requests remain scheduling-only and limits are immutable.
func TestNewContainerAttemptCopiesLimits(t *testing.T) {
	request, limit := int64(250), int64(500)
	sandbox, err := NewSandbox("sandbox-1", SandboxSpec{Resources: Resources{
		Requests: ResourceRequests{CPURequestMilli: &request},
		Limits:   ResourceLimits{CPULimitMilli: &limit},
	}})
	if err != nil {
		t.Fatalf("NewSandbox() error = %v", err)
	}
	sandbox.Status.Phase = SandboxReady
	sandbox.Status.ObservedGeneration = 1
	pair, err := NewContainerAttempt(sandbox, "container-1", "attempt-1",
		ProcessSpec{Argv: []string{"/bin/true"}}, "", "/prepared/rootfs")
	if err != nil {
		t.Fatalf("NewContainerAttempt() error = %v", err)
	}
	if pair.Container.Status.Generation != 1 || pair.Container.Status.ObservedGeneration != 0 {
		t.Fatalf("Container generation = %#v", pair.Container.Status)
	}
	if pair.Container.Spec.Limits.CPUUnlimited || pair.Container.Spec.Limits.CPULimitMilli == nil ||
		*pair.Container.Spec.Limits.CPULimitMilli != 500 || !pair.Container.Spec.Limits.MemoryUnlimited ||
		pair.Container.Spec.Limits.PidsLimit != DefaultPidsLimit {
		t.Fatalf("copied limits = %#v", pair.Container.Spec.Limits)
	}
	*sandbox.Spec.Resources.Limits.CPULimitMilli = 900
	if *pair.Container.Spec.Limits.CPULimitMilli != 500 {
		t.Fatal("Container limits alias Sandbox spec")
	}
}

// TestContainerSpecPersistsResolvedDefaults verifies immutable Container JSON and clones retain explicit max and default policy.
func TestContainerSpecPersistsResolvedDefaults(t *testing.T) {
	sandbox, err := NewSandbox("sandbox-defaults", SandboxSpec{})
	if err != nil {
		t.Fatalf("NewSandbox() error = %v", err)
	}
	sandbox.Status.Phase = SandboxReady
	sandbox.Status.ObservedGeneration = InitialGeneration
	pair, err := NewContainerAttempt(sandbox, "container-defaults", "attempt-defaults",
		ProcessSpec{Argv: []string{"/bin/true"}}, "", "/prepared/rootfs")
	if err != nil {
		t.Fatalf("NewContainerAttempt() error = %v", err)
	}
	encoded, err := json.Marshal(pair.Container.Spec)
	if err != nil {
		t.Fatalf("json.Marshal(ContainerSpec) error = %v", err)
	}
	var restored ContainerSpec
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatalf("json.Unmarshal(ContainerSpec) error = %v", err)
	}
	if err := restored.Validate(); err != nil {
		t.Fatalf("restored ContainerSpec validation error = %v", err)
	}
	if !restored.Limits.CPUUnlimited || !restored.Limits.MemoryUnlimited ||
		restored.Limits.PidsLimit != DefaultPidsLimit {
		t.Fatalf("restored ContainerSpec limits = %#v", restored.Limits)
	}
	clone := pair.Clone()
	clone.Container.Spec.Limits.PidsLimit = 99
	if pair.Container.Spec.Limits.PidsLimit != DefaultPidsLimit {
		t.Fatal("ContainerAttempt.Clone() aliased resolved limits")
	}
}

// TestContainerAttemptOneToOne verifies identity mismatches and phase projection are rejected.
func TestContainerAttemptOneToOne(t *testing.T) {
	pair := testPair(t, AttemptCreating, "container-1", "attempt-1")
	pair.Attempt.ContainerID = "other-container"
	if err := pair.Validate(); !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("mismatched ContainerAttempt.Validate() error = %v", err)
	}
	pair = testPair(t, AttemptCreating, "container-1", "attempt-1")
	pair.Container.Status.Phase = AttemptCreated
	if err := pair.Validate(); !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("mismatched phase Validate() error = %v", err)
	}
}

// TestValidateOneActiveAttempt verifies all active phases block a second pair while history remains valid.
func TestValidateOneActiveAttempt(t *testing.T) {
	activePhases := []AttemptPhase{AttemptCreating, AttemptCreated, AttemptRunning}
	for _, phase := range activePhases {
		first := testPair(t, phase, "container-1", "attempt-1")
		second := testPair(t, AttemptCreating, "container-2", "attempt-2")
		if err := ValidateOneActiveAttempt([]ContainerAttempt{first, second}); !IsCode(err, CodeFailedPrecondition) {
			t.Fatalf("ValidateOneActiveAttempt(%s + creating) error = %v", phase, err)
		}
	}
	stopped := testPair(t, AttemptStopped, "container-1", "attempt-1")
	creating := testPair(t, AttemptCreating, "container-2", "attempt-2")
	if err := ValidateOneActiveAttempt([]ContainerAttempt{stopped, creating}); err != nil {
		t.Fatalf("ValidateOneActiveAttempt(stopped history + creating) error = %v", err)
	}
}

// TestOutcomePresence verifies exit zero, signal, OOM evidence, and unknown remain distinct facts.
func TestOutcomePresence(t *testing.T) {
	now := time.Now()
	started := now.Add(-time.Second)
	exit := ExitOutcome(0, EvidenceFalse, started, now, time.Second)
	if !exit.Known() || exit.Validate() != nil {
		t.Fatalf("exit-zero outcome invalid: %#v", exit)
	}
	signal := SignalOutcome("SIGKILL", EvidenceTrue, started, now, time.Second)
	if !signal.Known() || signal.Validate() != nil {
		t.Fatalf("signal/OOM outcome invalid: %#v", signal)
	}
	unknownOOM := ExitOutcome(137, EvidenceUnknown, started, now, time.Second)
	if !unknownOOM.Known() || unknownOOM.Validate() != nil || unknownOOM.OOM != EvidenceUnknown {
		t.Fatalf("captured exit with independent unknown OOM evidence invalid: %#v", unknownOOM)
	}
	unknown := UnknownOutcome(EvidenceUnknown)
	if unknown.Known() || unknown.Validate() != nil {
		t.Fatalf("unknown outcome = %#v", unknown)
	}
	zero := int32(0)
	duration := time.Second
	invalid := Outcome{Presence: OutcomeCaptured, ExitCode: &zero, Signal: "SIGTERM", OOM: EvidenceFalse, StartedAt: &started, FinishedAt: &now, RunningDuration: &duration}
	if err := invalid.Validate(); !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("contradictory outcome error = %v", err)
	}
}

// TestProcessIdentityRequiresStrongEvidence verifies persisted identity shape rejects bare or partial evidence.
func TestProcessIdentityRequiresStrongEvidence(t *testing.T) {
	unsafe := []ProcessIdentity{{}, {Verified: true}, {Verified: true, Handle: "pidfd-token"}}
	for _, identity := range unsafe {
		if err := identity.Validate(); !IsCode(err, CodeUnsafeIdentity) {
			t.Fatalf("ProcessIdentity.Validate(%#v) error = %v", identity, err)
		}
	}
	verified := ProcessIdentity{Verified: true, Handle: "pidfd-token", Evidence: "owner-proof"}
	if err := verified.Validate(); err != nil {
		t.Fatalf("verified ProcessIdentity.Validate() error = %v", err)
	}
}

// testPair creates a valid pair in a requested phase for aggregate-invariant tests.
func testPair(t *testing.T, phase AttemptPhase, containerID ContainerID, attemptID AttemptID) ContainerAttempt {
	t.Helper()
	sandbox, err := NewSandbox("sandbox-1", SandboxSpec{})
	if err != nil {
		t.Fatalf("NewSandbox() error = %v", err)
	}
	sandbox.Status.Phase = SandboxReady
	sandbox.Status.ObservedGeneration = 1
	pair, err := NewContainerAttempt(sandbox, containerID, attemptID,
		ProcessSpec{Argv: []string{"/bin/true"}}, "", "/prepared/rootfs")
	if err != nil {
		t.Fatalf("NewContainerAttempt() error = %v", err)
	}
	pair.Attempt.Phase = phase
	pair.Container.Status.Phase = phase
	if phase == AttemptCreated || phase == AttemptRunning || phase == AttemptStopped {
		pair.Container.Status.ObservedGeneration = 1
	}
	if phase == AttemptRunning {
		identity := &ProcessIdentity{Verified: true, Handle: "pidfd-token", Evidence: "owner-proof"}
		pair.Attempt.ProcessIdentity = identity
		pair.Container.Status.ProcessIdentity = cloneProcessIdentity(identity)
	}
	if phase == AttemptStopped {
		pair.Attempt.Outcome = NotApplicableOutcome()
		pair.Container.Status.Outcome = NotApplicableOutcome()
	}
	return pair
}
