package daemon

import (
	"testing"
	"time"

	"mydocker/internal/operation"
)

// TestProjectEventPreservesOptionalDuration verifies daemon projection neither invents zero for missing evidence nor drops a measured zero sample.
func TestProjectEventPreservesOptionalDuration(t *testing.T) {
	event := operation.Event{
		SchemaVersion: operation.EventSchemaVersion,
		Sequence:      1,
		OperationID:   "operation-duration-projection",
		Type:          operation.TypeCreate,
		Target:        operation.Target{Kind: operation.TargetSandbox, ID: "sandbox-duration-projection"},
		Resources:     []operation.Target{{Kind: operation.TargetSandbox, ID: "sandbox-duration-projection"}},
		Stage:         operation.StagePersistIntent,
		Result:        operation.ResultPending,
		Reason:        operation.ReasonNone,
		OccurredAt:    time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC),
		Details:       []byte(`{"phase":"creating"}`),
	}
	projected, err := projectEvent(event)
	if err != nil {
		t.Fatalf("projectEvent(missing) error = %v", err)
	}
	if projected.DurationNanoseconds != nil {
		t.Fatalf("projected missing duration = %d, want nil", *projected.DurationNanoseconds)
	}
	zero := operation.Duration(0)
	event.Duration = &zero
	projected, err = projectEvent(event)
	if err != nil {
		t.Fatalf("projectEvent(zero) error = %v", err)
	}
	if projected.DurationNanoseconds == nil || *projected.DurationNanoseconds != 0 {
		t.Fatalf("projected measured zero duration = %#v, want explicit zero", projected.DurationNanoseconds)
	}
}
