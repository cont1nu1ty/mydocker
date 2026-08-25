package operation

import (
	"errors"
	"testing"
)

// validOperation constructs an internally consistent operation record for
// resolution and validation tests.
func validOperation(t *testing.T, state State) Operation {
	t.Helper()
	result := ResultPending
	reason := ReasonNone
	stage := StageTransition
	switch state {
	case StatePending:
		stage = StageValidate
	case StateSucceeded:
		result = ResultSucceeded
		stage = StageComplete
	case StateFailed:
		result = ResultFailed
		reason = ReasonInternal
		stage = StageComplete
	}
	return Operation{
		SchemaVersion: SchemaVersion,
		ID:            "op-1",
		Type:          TypeCreate,
		Target:        Target{Kind: TargetSandbox, ID: "sandbox-1"},
		Fingerprint:   mustFingerprint(t, map[string]string{"hostname": "demo"}),
		State:         state,
		Stage:         stage,
		Result:        result,
		Reason:        reason,
		Response:      []byte(`{"sandbox_id":"sandbox-1"}`),
	}
}

// TestResolveMatrix verifies deterministic New, Resume, and Replay selection.
func TestResolveMatrix(t *testing.T) {
	base := validOperation(t, StatePending)
	tests := []struct {
		name     string
		existing *Operation
		want     Resolution
	}{
		{name: "new", existing: nil, want: ResolutionNew},
		{name: "resume pending", existing: operationPointer(validOperation(t, StatePending)), want: ResolutionResume},
		{name: "resume running", existing: operationPointer(validOperation(t, StateRunning)), want: ResolutionResume},
		{name: "replay succeeded", existing: operationPointer(validOperation(t, StateSucceeded)), want: ResolutionReplay},
		{name: "replay failed", existing: operationPointer(validOperation(t, StateFailed)), want: ResolutionReplay},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Resolve(test.existing, base.Binding())
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("Resolve() = %q, want %q", got, test.want)
			}
		})
	}
}

// operationPointer returns a distinct address for a table-driven operation value.
func operationPointer(operation Operation) *Operation {
	return &operation
}

// TestResolveRejectsBindingMismatch verifies each immutable binding field yields
// a typed mismatch and never resumes or replays the wrong request.
func TestResolveRejectsBindingMismatch(t *testing.T) {
	existing := validOperation(t, StateRunning)
	tests := []struct {
		name   string
		change func(*Binding)
		field  string
	}{
		{name: "operation ID", field: "operation_id", change: func(binding *Binding) { binding.ID = "op-2" }},
		{name: "type", field: "operation_type", change: func(binding *Binding) { binding.Type = TypeDelete }},
		{name: "target", field: "target", change: func(binding *Binding) { binding.Target.ID = "sandbox-2" }},
		{name: "fingerprint", field: "request_fingerprint", change: func(binding *Binding) {
			binding.Fingerprint = mustFingerprint(t, map[string]string{"hostname": "other"})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binding := existing.Binding()
			test.change(&binding)
			if _, err := Resolve(&existing, binding); err == nil {
				t.Fatal("Resolve() error = nil, want binding mismatch")
			} else {
				if !errors.Is(err, ErrBindingMismatch) {
					t.Fatalf("Resolve() error = %v, want ErrBindingMismatch", err)
				}
				var mismatch *BindingMismatchError
				if !errors.As(err, &mismatch) || mismatch.Field != test.field {
					t.Fatalf("Resolve() mismatch = %#v, want field %q", mismatch, test.field)
				}
			}
		})
	}
}

// TestCheckActiveConflict verifies different active IDs conflict only when they
// address the same target, while the same ID remains a transport retry.
func TestCheckActiveConflict(t *testing.T) {
	active := validOperation(t, StateRunning)
	requested := active.Binding()
	requested.ID = "op-2"
	if err := CheckActiveConflict(&active, requested); !errors.Is(err, ErrActiveConflict) {
		t.Fatalf("CheckActiveConflict() error = %v, want ErrActiveConflict", err)
	}

	requested.ID = active.ID
	if err := CheckActiveConflict(&active, requested); err != nil {
		t.Fatalf("same operation retry rejected: %v", err)
	}
	requested.ID = "op-2"
	requested.Target.ID = "sandbox-2"
	if err := CheckActiveConflict(&active, requested); err != nil {
		t.Fatalf("different target rejected: %v", err)
	}
	terminal := validOperation(t, StateSucceeded)
	requested.Target = terminal.Target
	if err := CheckActiveConflict(&terminal, requested); err != nil {
		t.Fatalf("terminal operation rejected a new operation: %v", err)
	}
}

// TestOperationCloneDeepCopiesResponse verifies store callers cannot mutate the
// persisted replay payload through a returned operation value.
func TestOperationCloneDeepCopiesResponse(t *testing.T) {
	original := validOperation(t, StateSucceeded)
	clone := original.Clone()
	clone.Response[2] = 'X'
	if string(clone.Response) == string(original.Response) {
		t.Fatal("Clone() response shares backing storage")
	}
}

// TestOperationValidateStateResultMatrix verifies active and terminal records
// cannot contain contradictory bounded results or reason classes.
func TestOperationValidateStateResultMatrix(t *testing.T) {
	if err := validOperation(t, StateRunning).Validate(); err != nil {
		t.Fatalf("valid running operation rejected: %v", err)
	}
	if err := validOperation(t, StateSucceeded).Validate(); err != nil {
		t.Fatalf("valid succeeded operation rejected: %v", err)
	}
	if err := validOperation(t, StateFailed).Validate(); err != nil {
		t.Fatalf("valid failed operation rejected: %v", err)
	}

	tests := map[string]func(*Operation){
		"active terminal result": func(operation *Operation) { operation.Result = ResultSucceeded },
		"success failure result": func(operation *Operation) {
			operation.State = StateSucceeded
			operation.Result = ResultFailed
			operation.Reason = ReasonInternal
		},
		"failure missing reason": func(operation *Operation) {
			operation.State = StateFailed
			operation.Result = ResultFailed
		},
		"invalid response": func(operation *Operation) { operation.Response = []byte("{") },
		"duplicate response key": func(operation *Operation) {
			operation.Response = []byte(`{"sandbox_id":"sandbox-1","sandbox_id":"sandbox-2"}`)
		},
		"invalid response UTF-8": func(operation *Operation) {
			operation.Response = []byte{'"', 0xff, '"'}
		},
		"multiple response values": func(operation *Operation) {
			operation.Response = []byte(`{"sandbox_id":"sandbox-1"} null`)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			operation := validOperation(t, StateRunning)
			mutate(&operation)
			if err := operation.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want inconsistent operation error")
			}
		})
	}
}

// TestOperationValidateIdentityVerbAndStage verifies durable records reject unsafe IDs and impossible combinations.
func TestOperationValidateIdentityVerbAndStage(t *testing.T) {
	tests := map[string]func(*Operation){
		"operation ID whitespace": func(value *Operation) { value.ID = "op with space" },
		"target control":          func(value *Operation) { value.Target.ID = "sandbox\nother" },
		"start sandbox":           func(value *Operation) { value.Type = TypeStart },
		"pending complete":        func(value *Operation) { value.Stage = StageComplete },
		"running validate": func(value *Operation) {
			value.State = StateRunning
			value.Stage = StageValidate
		},
		"terminal transition": func(value *Operation) {
			value.State = StateSucceeded
			value.Result = ResultSucceeded
			value.Stage = StageTransition
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := validOperation(t, StatePending)
			mutate(&value)
			if err := value.Validate(); err == nil {
				t.Fatal("Operation.Validate() error = nil, want invalid durable combination")
			}
		})
	}
}
