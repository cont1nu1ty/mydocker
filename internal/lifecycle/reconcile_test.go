package lifecycle

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
)

// TestReconcileConditionPersistsPostEventReplay verifies recovery facts and their observation cursor replay exactly after later state changes.
func TestReconcileConditionPersistsPostEventReplay(t *testing.T) {
	coordinator, _ := testCoordinator(t)
	sandbox := createReadySandbox(t, coordinator, "sandbox-reconcile-condition")
	target := operation.Target{Kind: operation.TargetSandbox, ID: string(sandbox.ID)}
	request := ReconcileConditionRequest{
		OperationID: "op-reconcile-condition-first",
		Target:      target,
		Condition: &domain.Condition{
			Type: domain.ConditionProcessIdentityUnknown, Reason: "DaemonRestart",
			Message: "runtime supervisor cannot be reconnected in M3",
		},
		Evidence:   "read-only discovery found retained runtime ownership",
		ObservedAt: testWallTime(),
	}
	first, err := coordinator.ReconcileCondition(context.Background(), request)
	if err != nil {
		t.Fatalf("ReconcileCondition() error = %v", err)
	}
	if first.Sandbox == nil || first.Operation.State != operation.StateSucceeded ||
		first.Sandbox.Status.LastObservation.OperationID != string(request.OperationID) {
		t.Fatalf("ReconcileCondition() = %#v", first)
	}
	original := first.Sandbox.Clone()
	secondRequest := ReconcileConditionRequest{
		OperationID: "op-reconcile-condition-second", Target: target,
		Condition: &domain.Condition{Type: domain.ConditionFailure, Reason: "LaterFact"},
		Evidence:  "a later independent recovery observation", ObservedAt: testWallTime(),
	}
	if _, err := coordinator.ReconcileCondition(context.Background(), secondRequest); err != nil {
		t.Fatalf("ReconcileCondition(second) error = %v", err)
	}
	replayed, err := coordinator.ReconcileCondition(context.Background(), request)
	if err != nil {
		t.Fatalf("ReconcileCondition(replay) error = %v", err)
	}
	if !reflect.DeepEqual(replayed.Sandbox, &original) || replayed.Operation.Response == nil {
		t.Fatalf("ReconcileCondition(replay) = %#v, want exact %#v", replayed, original)
	}
}

// TestDecodeReconcileResponseAcceptsExactTargetProjection verifies both supported target kinds restore one valid, identity-bound resource and clear the opposite branch.
func TestDecodeReconcileResponseAcceptsExactTargetProjection(t *testing.T) {
	coordinator, _ := testCoordinator(t)
	sandbox := createReadySandbox(t, coordinator, "sandbox-reconcile-decode-valid")
	pair := createRunningContainer(
		t, coordinator, sandbox.ID,
		"container-reconcile-decode-valid", "attempt-reconcile-decode-valid", "reconcile-decode-valid",
	)

	tests := []struct {
		name       string
		target     operation.Target
		response   reconcileResponse
		wantResult ReconcileConditionResult
	}{
		{
			name:     "Sandbox",
			target:   operation.Target{Kind: operation.TargetSandbox, ID: string(sandbox.ID)},
			response: reconcileResponse{Sandbox: sandboxPointer(sandbox)},
			wantResult: ReconcileConditionResult{
				Sandbox: sandboxPointer(sandbox),
			},
		},
		{
			name:     "Container Attempt",
			target:   operation.Target{Kind: operation.TargetContainer, ID: string(pair.Container.ID)},
			response: reconcileResponse{ContainerAttempt: pairPointer(pair)},
			wantResult: ReconcileConditionResult{
				ContainerAttempt: pairPointer(pair),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := ReconcileConditionResult{Sandbox: sandboxPointer(sandbox), ContainerAttempt: pairPointer(pair)}
			encoded := encodeReconcileTestResponse(t, test.response)
			if err := decodeReconcileResponse(encoded, test.target, &result); err != nil {
				t.Fatalf("decodeReconcileResponse() error = %v", err)
			}
			if !reflect.DeepEqual(result, test.wantResult) {
				t.Fatalf("decodeReconcileResponse() = %#v, want %#v", result, test.wantResult)
			}
		})
	}
}

// TestDecodeReconcileResponseRejectsAmbiguousOrUnboundPayload verifies terminal replay fails closed for noncanonical JSON, invalid resource models, branch ambiguity, and target drift.
func TestDecodeReconcileResponseRejectsAmbiguousOrUnboundPayload(t *testing.T) {
	coordinator, _ := testCoordinator(t)
	sandbox := createReadySandbox(t, coordinator, "sandbox-reconcile-decode-reject")
	pair := createRunningContainer(
		t, coordinator, sandbox.ID,
		"container-reconcile-decode-reject", "attempt-reconcile-decode-reject", "reconcile-decode-reject",
	)
	sandboxJSON, err := json.Marshal(sandbox)
	if err != nil {
		t.Fatalf("json.Marshal(Sandbox) error = %v", err)
	}
	sandboxPayload := encodeReconcileTestResponse(t, reconcileResponse{Sandbox: sandboxPointer(sandbox)})
	pairPayload := encodeReconcileTestResponse(t, reconcileResponse{ContainerAttempt: pairPointer(pair)})
	bothPayload := encodeReconcileTestResponse(t, reconcileResponse{
		Sandbox: sandboxPointer(sandbox), ContainerAttempt: pairPointer(pair),
	})
	invalidSandbox := sandbox.Clone()
	invalidSandbox.SchemaVersion++
	invalidPair := pair.Clone()
	invalidPair.Attempt.ContainerID = "container-reconcile-decode-other"

	tests := []struct {
		name    string
		target  operation.Target
		payload json.RawMessage
	}{
		{
			name:   "empty response",
			target: operation.Target{Kind: operation.TargetSandbox, ID: string(sandbox.ID)},
		},
		{
			name:   "decoded duplicate key",
			target: operation.Target{Kind: operation.TargetSandbox, ID: string(sandbox.ID)},
			payload: json.RawMessage(`{"sandbox":` + string(sandboxJSON) +
				`,"\u0073andbox":` + string(sandboxJSON) + `}`),
		},
		{
			name:    "case alias",
			target:  operation.Target{Kind: operation.TargetSandbox, ID: string(sandbox.ID)},
			payload: json.RawMessage(`{"Sandbox":` + string(sandboxJSON) + `}`),
		},
		{
			name:   "unknown field",
			target: operation.Target{Kind: operation.TargetSandbox, ID: string(sandbox.ID)},
			payload: json.RawMessage(`{"sandbox":` + string(sandboxJSON) +
				`,"unknown":true}`),
		},
		{
			name:    "trailing value",
			target:  operation.Target{Kind: operation.TargetSandbox, ID: string(sandbox.ID)},
			payload: append(append(json.RawMessage(nil), sandboxPayload...), []byte(`{}`)...),
		},
		{
			name:    "both resource branches",
			target:  operation.Target{Kind: operation.TargetSandbox, ID: string(sandbox.ID)},
			payload: bothPayload,
		},
		{
			name:    "neither resource branch",
			target:  operation.Target{Kind: operation.TargetSandbox, ID: string(sandbox.ID)},
			payload: json.RawMessage(`{}`),
		},
		{
			name:    "Container branch for Sandbox target",
			target:  operation.Target{Kind: operation.TargetSandbox, ID: string(sandbox.ID)},
			payload: pairPayload,
		},
		{
			name:    "Sandbox branch for Container target",
			target:  operation.Target{Kind: operation.TargetContainer, ID: string(pair.Container.ID)},
			payload: sandboxPayload,
		},
		{
			name:    "wrong Sandbox ID",
			target:  operation.Target{Kind: operation.TargetSandbox, ID: "sandbox-reconcile-decode-other"},
			payload: sandboxPayload,
		},
		{
			name:    "wrong Container ID",
			target:  operation.Target{Kind: operation.TargetContainer, ID: "container-reconcile-decode-other"},
			payload: pairPayload,
		},
		{
			name:   "invalid Sandbox model",
			target: operation.Target{Kind: operation.TargetSandbox, ID: string(invalidSandbox.ID)},
			payload: encodeReconcileTestResponse(t, reconcileResponse{
				Sandbox: sandboxPointer(invalidSandbox),
			}),
		},
		{
			name:   "invalid Container Attempt model",
			target: operation.Target{Kind: operation.TargetContainer, ID: string(invalidPair.Container.ID)},
			payload: encodeReconcileTestResponse(t, reconcileResponse{
				ContainerAttempt: pairPointer(invalidPair),
			}),
		},
		{
			name:    "unsupported target kind",
			target:  operation.Target{Kind: operation.TargetAttempt, ID: string(pair.Attempt.ID)},
			payload: pairPayload,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var result ReconcileConditionResult
			if err := decodeReconcileResponse(test.payload, test.target, &result); err == nil {
				t.Fatalf("decodeReconcileResponse() error = nil, result = %#v", result)
			}
			if result.Sandbox != nil || result.ContainerAttempt != nil {
				t.Fatalf("decodeReconcileResponse() partially populated result = %#v", result)
			}
		})
	}
}

// encodeReconcileTestResponse serializes one test replay projection through the production response encoder.
func encodeReconcileTestResponse(t *testing.T, response reconcileResponse) json.RawMessage {
	t.Helper()
	encoded, err := encodeResponse(response)
	if err != nil {
		t.Fatalf("encodeResponse() error = %v", err)
	}
	return encoded
}

// TestFailActiveOperationStoresConditionAndExactResponse verifies an unrecoverable non-create intent becomes a stable failed result without inventing a phase transition.
func TestFailActiveOperationStoresConditionAndExactResponse(t *testing.T) {
	coordinator, _ := testCoordinator(t)
	sandbox := createReadySandbox(t, coordinator, "sandbox-reconcile-failure")
	begin, err := coordinator.BeginSandboxStop(context.Background(), SandboxActionRequest{
		OperationID: "op-reconcile-stop-failure", SandboxID: sandbox.ID,
	})
	if err != nil {
		t.Fatalf("BeginSandboxStop() error = %v", err)
	}
	request := FailOperationRequest{
		OperationID: begin.Operation.ID,
		Target:      begin.Operation.Target,
		Fingerprint: begin.Fingerprint,
		Failure:     Failure{Reason: operation.ReasonInternal, Message: "provider observation is permanently invalid"},
		Condition:   domain.Condition{Type: domain.ConditionFailure, Reason: "RecoveryRejected", Message: "operator action required"},
		ObservedAt:  testWallTime(),
	}
	failed, err := coordinator.FailActiveOperation(context.Background(), request)
	if err != nil {
		t.Fatalf("FailActiveOperation() error = %v", err)
	}
	if failed.State != operation.StateFailed || failed.Reason != operation.ReasonInternal || len(failed.Response) == 0 {
		t.Fatalf("FailActiveOperation() = %#v", failed)
	}
	current, err := coordinator.GetSandbox(context.Background(), sandbox.ID)
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if current.Status.Phase != domain.SandboxStopping || current.Status.LastObservation.OperationID != string(request.OperationID) {
		t.Fatalf("failed stop Sandbox = %#v", current.Status)
	}
	replayed, err := coordinator.FailActiveOperation(context.Background(), request)
	if err != nil {
		t.Fatalf("FailActiveOperation(replay) error = %v", err)
	}
	if !reflect.DeepEqual(replayed, failed) {
		t.Fatalf("FailActiveOperation(replay) = %#v, want %#v", replayed, failed)
	}
}
