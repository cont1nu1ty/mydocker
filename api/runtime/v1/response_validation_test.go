package v1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestResponseValidationAcceptsBoundedDTOs verifies each lifecycle resource and
// list response accepts complete v1 identity, phase, generation, and operation data.
func TestResponseValidationAcceptsBoundedDTOs(t *testing.T) {
	sandbox := validSandboxResponse("sandbox-one", "operation-sandbox")
	container := validContainerResponse("container-one", "sandbox-one", "attempt-one", "operation-container")
	event := validResponseEvent(1)
	frame := validResponseLogFrame(1, 1, "stdout", []byte("hello"))
	logCursor, err := NewLogCursor(frame.ContainerID, frame.AttemptID, frame.Cursor)
	if err != nil {
		t.Fatalf("NewLogCursor() error = %v", err)
	}
	responses := []struct {
		name     string
		validate func() error
	}{
		{name: "Sandbox", validate: sandbox.Validate},
		{name: "Sandbox list", validate: (SandboxListResponse{Sandboxes: []Sandbox{sandbox.Sandbox}}).Validate},
		{name: "Container", validate: container.Validate},
		{name: "Container list", validate: (ContainerListResponse{Containers: []Container{container.Container}}).Validate},
		{name: "Operation", validate: (OperationResponse{Operation: *container.Operation}).Validate},
		{name: "Event", validate: event.Validate},
		{name: "Event list", validate: (EventListResponse{Events: []Event{event}, NextResumeToken: NewResumeToken(event.Sequence)}).Validate},
		{name: "Log frame", validate: frame.Validate},
		{name: "Log list", validate: (LogListResponse{Frames: []LogFrame{frame}, NextCursor: logCursor}).Validate},
	}
	for _, response := range responses {
		t.Run(response.name, func(t *testing.T) {
			if err := response.validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

// TestResponseValidationRejectsZeroIdentityPhaseGenerationAndTarget verifies
// complete JSON with unsafe lifecycle semantics cannot pass DTO validation.
func TestResponseValidationRejectsZeroIdentityPhaseGenerationAndTarget(t *testing.T) {
	tests := []struct {
		name     string
		validate func() error
	}{
		{name: "zero Sandbox", validate: (SandboxResponse{}).Validate},
		{name: "zero Container", validate: (ContainerResponse{}).Validate},
		{name: "zero Operation", validate: (OperationResponse{}).Validate},
		{name: "Sandbox phase", validate: func() error {
			response := validSandboxResponse("sandbox-one", "operation-one")
			response.Sandbox.Status.Phase = "future"
			return response.Validate()
		}},
		{name: "Sandbox generation", validate: func() error {
			response := validSandboxResponse("sandbox-one", "operation-one")
			response.Sandbox.Status.Generation = 0
			return response.Validate()
		}},
		{name: "Sandbox operation target", validate: func() error {
			response := validSandboxResponse("sandbox-one", "operation-one")
			response.Operation.Target = ResourceRef{Kind: "sandbox", ID: "sandbox-other"}
			return response.Validate()
		}},
		{name: "Container phase", validate: func() error {
			response := validContainerResponse("container-one", "sandbox-one", "attempt-one", "operation-one")
			response.Container.Status.Phase = "future"
			return response.Validate()
		}},
		{name: "Container generation", validate: func() error {
			response := validContainerResponse("container-one", "sandbox-one", "attempt-one", "operation-one")
			response.Container.Status.ObservedGeneration = 2
			return response.Validate()
		}},
		{name: "Container operation target", validate: func() error {
			response := validContainerResponse("container-one", "sandbox-one", "attempt-one", "operation-one")
			response.Operation.Target = ResourceRef{Kind: "container", ID: "container-other"}
			return response.Validate()
		}},
		{name: "unordered Sandbox list", validate: func() error {
			first := validSandboxResponse("sandbox-two", "operation-two").Sandbox
			second := validSandboxResponse("sandbox-one", "operation-one").Sandbox
			return (SandboxListResponse{Sandboxes: []Sandbox{first, second}}).Validate()
		}},
		{name: "unordered Container list", validate: func() error {
			first := validContainerResponse("container-two", "sandbox-one", "attempt-two", "operation-two").Container
			second := validContainerResponse("container-one", "sandbox-one", "attempt-one", "operation-one").Container
			return (ContainerListResponse{Containers: []Container{first, second}}).Validate()
		}},
		{name: "event missing resource", validate: func() error {
			event := validResponseEvent(1)
			event.Resources = nil
			return event.Validate()
		}},
		{name: "event generation", validate: func() error {
			event := validResponseEvent(1)
			event.ObservedGeneration = 2
			return event.Validate()
		}},
		{name: "negative event duration", validate: func() error {
			event := validResponseEvent(1)
			duration := int64(-1)
			event.DurationNanoseconds = &duration
			return event.Validate()
		}},
		{name: "event token", validate: func() error {
			event := validResponseEvent(2)
			return (EventListResponse{Events: []Event{event}, NextResumeToken: NewResumeToken(1)}).Validate()
		}},
		{name: "unordered event page", validate: func() error {
			return (EventListResponse{
				Events:          []Event{validResponseEvent(2), validResponseEvent(1)},
				NextResumeToken: NewResumeToken(1),
			}).Validate()
		}},
		{name: "gapped event page", validate: func() error {
			return (EventListResponse{
				Events:          []Event{validResponseEvent(1), validResponseEvent(3)},
				NextResumeToken: NewResumeToken(3),
			}).Validate()
		}},
		{name: "log checksum", validate: func() error {
			frame := validResponseLogFrame(1, 1, "stdout", []byte("hello"))
			frame.PayloadSHA256 = strings.Repeat("0", 64)
			return frame.Validate()
		}},
		{name: "mixed log identity", validate: func() error {
			first := validResponseLogFrame(1, 1, "stdout", []byte("first"))
			second := validResponseLogFrame(2, 1, "stderr", []byte("second"))
			second.ContainerID = "container-other"
			cursor, err := NewLogCursor(second.ContainerID, second.AttemptID, second.Cursor)
			if err != nil {
				return err
			}
			return (LogListResponse{Frames: []LogFrame{first, second}, NextCursor: cursor}).Validate()
		}},
		{name: "log cursor", validate: func() error {
			frame := validResponseLogFrame(1, 1, "stdout", []byte("hello"))
			cursor, err := NewLogCursor(frame.ContainerID, frame.AttemptID, 2)
			if err != nil {
				return err
			}
			return (LogListResponse{Frames: []LogFrame{frame}, NextCursor: cursor}).Validate()
		}},
		{name: "gapped log page", validate: func() error {
			first := validResponseLogFrame(1, 1, "stdout", []byte("first"))
			second := validResponseLogFrame(3, 1, "stderr", []byte("second"))
			cursor, err := NewLogCursor(second.ContainerID, second.AttemptID, second.Cursor)
			if err != nil {
				return err
			}
			return (LogListResponse{Frames: []LogFrame{first, second}, NextCursor: cursor}).Validate()
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.validate(); err == nil {
				t.Fatal("Validate() error = nil, want semantic rejection")
			}
		})
	}
}

// TestEventDurationJSONDistinguishesMissingAndZero verifies v1 omits unavailable timing evidence while preserving a real measured zero sample.
func TestEventDurationJSONDistinguishesMissingAndZero(t *testing.T) {
	event := validResponseEvent(1)
	missing, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("json.Marshal(missing) error = %v", err)
	}
	if strings.Contains(string(missing), `"duration_ns"`) {
		t.Fatalf("missing duration JSON = %s, want field omitted", missing)
	}
	zero := int64(0)
	event.DurationNanoseconds = &zero
	measured, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("json.Marshal(zero) error = %v", err)
	}
	if !strings.Contains(string(measured), `"duration_ns":0`) {
		t.Fatalf("measured zero duration JSON = %s, want explicit zero", measured)
	}
}

// validSandboxResponse constructs one complete Ready Sandbox mutation projection.
func validSandboxResponse(sandboxID, operationID string) SandboxResponse {
	operation := validResponseOperation(operationID, "create", "sandbox", sandboxID)
	return SandboxResponse{
		Sandbox: Sandbox{
			ID: sandboxID, Spec: SandboxSpec{Network: NetworkIntent{Mode: "none"}},
			Status: SandboxStatus{
				Phase: "ready", Generation: 1, ObservedGeneration: 1,
				LastObservation: LifecycleObservation{OperationID: operationID, EventSequence: 1, Reason: "none"},
			},
		},
		Operation: &operation,
	}
}

// validContainerResponse constructs one complete Created Container Attempt mutation projection.
func validContainerResponse(containerID, sandboxID, attemptID, operationID string) ContainerResponse {
	operation := validResponseOperation(operationID, "create", "container", containerID)
	return ContainerResponse{
		Container: Container{
			ID: containerID, SandboxID: sandboxID, AttemptID: attemptID,
			Spec: ContainerSpec{
				Process: ProcessSpec{Argv: []string{"/bin/workload"}}, RootFS: "rootfs-one",
				Limits: ResolvedResourceLimits{CPUUnlimited: true, MemoryUnlimited: true, PidsLimit: 1024},
			},
			Status: ContainerStatus{
				Phase: "created", Generation: 1, ObservedGeneration: 1,
				Outcome:         Outcome{Presence: "pending", OOM: "unknown"},
				LastObservation: LifecycleObservation{OperationID: operationID, EventSequence: 1, Reason: "none"},
			},
		},
		Operation: &operation,
	}
}

// validResponseOperation constructs one terminal successful operation with a canonical fingerprint.
func validResponseOperation(operationID, operationType, targetKind, targetID string) Operation {
	return Operation{
		ID: operationID, Type: operationType, Target: ResourceRef{Kind: targetKind, ID: targetID},
		Fingerprint: RequestFingerprint{Version: 1, SHA256: strings.Repeat("a", 64)},
		State:       "succeeded", Stage: "complete", Result: "succeeded", Reason: "none",
	}
}

// validResponseEvent constructs one complete public lifecycle event projection.
func validResponseEvent(sequence uint64) Event {
	return Event{
		Sequence: sequence, OperationID: "operation-event", Type: "create",
		Target:    ResourceRef{Kind: "sandbox", ID: "sandbox-one"},
		Resources: []ResourceRef{{Kind: "sandbox", ID: "sandbox-one"}},
		Stage:     "complete", Result: "succeeded", Reason: "none",
		OccurredAt: time.Unix(1, 0).UTC(), Generation: 1, ObservedGeneration: 1,
	}
}

// validResponseLogFrame constructs one checksum-bound public workload-log frame.
func validResponseLogFrame(cursor, sequence uint64, stream string, payload []byte) LogFrame {
	digest := sha256.Sum256(payload)
	return LogFrame{
		ContainerID: "container-one", AttemptID: "attempt-one", Stream: stream,
		Cursor: cursor, Sequence: sequence, Payload: append([]byte(nil), payload...),
		PayloadSHA256: hex.EncodeToString(digest[:]),
	}
}
