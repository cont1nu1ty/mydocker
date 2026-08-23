package operation

import (
	"testing"
	"time"
)

// validEvent constructs a fully populated event for sequence and validation tests.
func validEvent(sequence Sequence) Event {
	return Event{
		SchemaVersion:      SchemaVersion,
		Sequence:           sequence,
		OperationID:        "op-1",
		Type:               TypeCreate,
		Target:             Target{Kind: TargetSandbox, ID: "sandbox-1"},
		Resources:          []Target{{Kind: TargetSandbox, ID: "sandbox-1"}},
		Stage:              StageTransition,
		Result:             ResultSucceeded,
		Reason:             ReasonNone,
		OccurredAt:         time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC),
		Duration:           Duration(25 * time.Millisecond),
		Generation:         1,
		ObservedGeneration: 1,
		Details:            []byte(`{"from":"creating","to":"ready"}`),
	}
}

// TestEventValidateAndSequence verifies a valid monotonic-duration event and an
// immediately following event pass ordering checks.
func TestEventValidateAndSequence(t *testing.T) {
	first := validEvent(1)
	second := validEvent(2)
	if err := first.Validate(); err != nil {
		t.Fatalf("first.Validate() error = %v", err)
	}
	if err := second.ValidateAfter(first); err != nil {
		t.Fatalf("second.ValidateAfter(first) error = %v", err)
	}
	if got, want := first.Duration.Value(), 25*time.Millisecond; got != want {
		t.Fatalf("Duration.Value() = %v, want %v", got, want)
	}
}

// TestHostLifecycleStagesRemainBounded verifies every M2/M3 host checkpoint is valid for durable operations and events.
func TestHostLifecycleStagesRemainBounded(t *testing.T) {
	stages := []Stage{
		StageHostPreflight, StagePrepareCgroup, StagePrepareStartGate, StagePrepareStreams,
		StageCreateProcess, StagePrepareNamespaces, StageJoinNamespaces, StagePrepareRootfs,
		StageAttachCgroup, StageReleaseStartGate, StageSignalProcess,
		StageObserveProcess, StageTeardown,
	}
	for _, stage := range stages {
		if !stage.Valid() {
			t.Fatalf("Stage.Valid(%q) = false, want a durable host checkpoint", stage)
		}
		event := validEvent(1)
		event.Stage = stage
		if err := event.Validate(); err != nil {
			t.Fatalf("Event.Validate(stage %q) error = %v", stage, err)
		}
	}
}

// TestEventValidateRejectsInvalidFields verifies sequence, duration, generation,
// result/reason consistency, stage vocabulary, timestamp, and JSON details.
func TestEventValidateRejectsInvalidFields(t *testing.T) {
	tests := map[string]func(*Event){
		"zero sequence":       func(event *Event) { event.Sequence = 0 },
		"negative duration":   func(event *Event) { event.Duration = -1 },
		"observed generation": func(event *Event) { event.ObservedGeneration = 2 },
		"missing timestamp":   func(event *Event) { event.OccurredAt = time.Time{} },
		"unbounded stage":     func(event *Event) { event.Stage = "future_stage" },
		"invalid type target": func(event *Event) { event.Type = TypeStart },
		"failure no reason": func(event *Event) {
			event.Result = ResultFailed
			event.Reason = ReasonNone
		},
		"success with reason": func(event *Event) { event.Reason = ReasonInternal },
		"invalid details":     func(event *Event) { event.Details = []byte("{") },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			event := validEvent(1)
			mutate(&event)
			if err := event.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want validation error")
			}
		})
	}
}

// TestExecutionEventRequiresAllResourceIdentities verifies execution facts always carry Sandbox, Container, and Attempt IDs.
func TestExecutionEventRequiresAllResourceIdentities(t *testing.T) {
	event := validEvent(1)
	event.Type = TypeStart
	event.Target = Target{Kind: TargetContainer, ID: "container-1"}
	event.Resources = []Target{event.Target}
	if err := event.Validate(); err == nil {
		t.Fatal("Event.Validate() accepted execution event without Sandbox and Attempt identities")
	}
	event.Resources = []Target{
		{Kind: TargetSandbox, ID: "sandbox-1"},
		event.Target,
		{Kind: TargetAttempt, ID: "attempt-1"},
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("Event.Validate() complete execution identities error = %v", err)
	}
}

// TestEventValidateAfterRejectsGap verifies event ordering does not silently
// accept duplicate sequence numbers or gaps in the selected ordering scope.
func TestEventValidateAfterRejectsGap(t *testing.T) {
	previous := validEvent(4)
	for _, sequence := range []Sequence{4, 6} {
		event := validEvent(sequence)
		if err := event.ValidateAfter(previous); err == nil {
			t.Fatalf("ValidateAfter() accepted sequence %d after %d", sequence, previous.Sequence)
		}
	}
}

// TestEventCloneDeepCopiesDetails verifies observers cannot mutate stored event
// details through a returned clone.
func TestEventCloneDeepCopiesDetails(t *testing.T) {
	original := validEvent(1)
	clone := original.Clone()
	clone.Details[2] = 'X'
	clone.Resources[0].ID = "sandbox-mutated"
	if string(clone.Details) == string(original.Details) {
		t.Fatal("Clone() details share backing storage")
	}
	if original.Resources[0].ID != "sandbox-1" {
		t.Fatal("Clone() resources share backing storage")
	}
}

// TestEventValidateResourceIdentities verifies the primary resource is explicit and related identities are unique.
func TestEventValidateResourceIdentities(t *testing.T) {
	missing := validEvent(1)
	missing.Resources = nil
	if err := missing.Validate(); err == nil {
		t.Fatal("Event.Validate() accepted missing resource identities")
	}
	duplicate := validEvent(1)
	duplicate.Resources = append(duplicate.Resources, duplicate.Target)
	if err := duplicate.Validate(); err == nil {
		t.Fatal("Event.Validate() accepted duplicate resource identity")
	}
	wrong := validEvent(1)
	wrong.Resources = []Target{{Kind: TargetSandbox, ID: "other"}}
	if err := wrong.Validate(); err == nil {
		t.Fatal("Event.Validate() accepted resources without its primary target")
	}
}

// TestSequenceNextRejectsOverflow verifies the reserved ordering type fails
// explicitly instead of wrapping to zero.
func TestSequenceNextRejectsOverflow(t *testing.T) {
	if _, err := Sequence(^uint64(0)).Next(); err == nil {
		t.Fatal("Next() error = nil, want overflow error")
	}
}
