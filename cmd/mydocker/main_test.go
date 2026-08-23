package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	v1 "mydocker/api/runtime/v1"
	"mydocker/pkg/client"
)

type fakeRuntimeClient struct {
	called          string
	operationID     string
	sandboxID       string
	containerID     string
	attemptID       string
	createSandbox   v1.CreateSandboxRequest
	createContainer v1.CreateContainerRequest
	killPolicy      v1.TerminationPolicy
	err             error
}

// CreateSandbox records the exact structured request passed through the CLI create form.
func (fake *fakeRuntimeClient) CreateSandbox(_ context.Context, operationID string, request v1.CreateSandboxRequest) (v1.SandboxResponse, error) {
	fake.called, fake.operationID, fake.createSandbox = "sandbox.create", operationID, request
	return v1.SandboxResponse{Sandbox: v1.Sandbox{ID: request.SandboxID}}, fake.err
}

// StopSandbox records one operation-scoped Sandbox stop without performing host work.
func (fake *fakeRuntimeClient) StopSandbox(_ context.Context, operationID, sandboxID string) (v1.SandboxResponse, error) {
	fake.called, fake.operationID, fake.sandboxID = "sandbox.stop", operationID, sandboxID
	return v1.SandboxResponse{Sandbox: v1.Sandbox{ID: sandboxID}}, fake.err
}

// DeleteSandbox records one operation-scoped Sandbox deletion without modifying persistent state.
func (fake *fakeRuntimeClient) DeleteSandbox(_ context.Context, operationID, sandboxID string) (v1.OperationResponse, error) {
	fake.called, fake.operationID, fake.sandboxID = "sandbox.delete", operationID, sandboxID
	return operationResponse(operationID), fake.err
}

// GetSandbox records one read-only Sandbox lookup for command dispatch coverage.
func (fake *fakeRuntimeClient) GetSandbox(_ context.Context, sandboxID string) (v1.SandboxResponse, error) {
	fake.called, fake.sandboxID = "sandbox.get", sandboxID
	return v1.SandboxResponse{Sandbox: v1.Sandbox{ID: sandboxID}}, fake.err
}

// ListSandboxes records one deterministic Sandbox collection request.
func (fake *fakeRuntimeClient) ListSandboxes(context.Context) (v1.SandboxListResponse, error) {
	fake.called = "sandbox.list"
	return v1.SandboxListResponse{Sandboxes: []v1.Sandbox{}}, fake.err
}

// CreateContainer records the parent identity and preserves argv/environment as JSON arrays.
func (fake *fakeRuntimeClient) CreateContainer(_ context.Context, operationID, sandboxID string, request v1.CreateContainerRequest) (v1.ContainerResponse, error) {
	fake.called, fake.operationID, fake.sandboxID, fake.createContainer = "container.create", operationID, sandboxID, request
	return v1.ContainerResponse{Container: v1.Container{ID: request.ContainerID, SandboxID: sandboxID, AttemptID: request.AttemptID}}, fake.err
}

// StartContainer records one operation-scoped start-gate release request.
func (fake *fakeRuntimeClient) StartContainer(_ context.Context, operationID, containerID string) (v1.ContainerResponse, error) {
	fake.called, fake.operationID, fake.containerID = "container.start", operationID, containerID
	return v1.ContainerResponse{Container: v1.Container{ID: containerID}}, fake.err
}

// KillContainer records the complete explicit signal, grace, and escalation policy.
func (fake *fakeRuntimeClient) KillContainer(_ context.Context, operationID, containerID string, policy v1.TerminationPolicy) (v1.ContainerResponse, error) {
	fake.called, fake.operationID, fake.containerID, fake.killPolicy = "container.kill", operationID, containerID, policy
	return v1.ContainerResponse{Container: v1.Container{ID: containerID}}, fake.err
}

// DeleteContainer records one operation-scoped Attempt teardown request.
func (fake *fakeRuntimeClient) DeleteContainer(_ context.Context, operationID, containerID string) (v1.OperationResponse, error) {
	fake.called, fake.operationID, fake.containerID = "container.delete", operationID, containerID
	return operationResponse(operationID), fake.err
}

// GetContainer records one read-only Container/Attempt lookup.
func (fake *fakeRuntimeClient) GetContainer(_ context.Context, containerID string) (v1.ContainerResponse, error) {
	fake.called, fake.containerID = "container.get", containerID
	return v1.ContainerResponse{Container: v1.Container{ID: containerID}}, fake.err
}

// ListContainers records the stable Sandbox scope used by the list request.
func (fake *fakeRuntimeClient) ListContainers(_ context.Context, sandboxID string) (v1.ContainerListResponse, error) {
	fake.called, fake.sandboxID = "container.list", sandboxID
	return v1.ContainerListResponse{Containers: []v1.Container{}}, fake.err
}

// GetOperation records one durable operation lookup without creating a new operation.
func (fake *fakeRuntimeClient) GetOperation(_ context.Context, operationID string) (v1.OperationResponse, error) {
	fake.called, fake.operationID = "operation.get", operationID
	return operationResponse(operationID), fake.err
}

// Events records event pagination dispatch while returning an empty canonical page.
func (fake *fakeRuntimeClient) Events(context.Context, v1.ResumeToken, int) (v1.EventListResponse, error) {
	fake.called = "events"
	return v1.EventListResponse{}, fake.err
}

// Logs records exact Container/Attempt log identity while returning an empty page.
func (fake *fakeRuntimeClient) Logs(_ context.Context, containerID, attemptID string, _ v1.LogCursor, _ int) (v1.LogListResponse, error) {
	fake.called, fake.containerID, fake.attemptID = "logs", containerID, attemptID
	return v1.LogListResponse{}, fake.err
}

// operationResponse creates the smallest machine-readable response needed by command tests.
func operationResponse(operationID string) v1.OperationResponse {
	return v1.OperationResponse{Operation: v1.Operation{ID: operationID}}
}

// TestRunSupportsCompleteCommandVocabulary verifies every required M3 command reaches only its public-client method.
func TestRunSupportsCompleteCommandVocabulary(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		stdin      string
		wantCalled string
	}{
		{"sandbox create", []string{"--operation-id", "operation-one", "sandbox", "create"}, `{"sandbox_id":"sandbox-one","spec":{"network":{"mode":"none"},"resources":{"requests":{},"limits":{}}}}`, "sandbox.create"},
		{"sandbox get", []string{"sandbox", "get", "sandbox-one"}, "", "sandbox.get"},
		{"sandbox list", []string{"sandbox", "list"}, "", "sandbox.list"},
		{"sandbox stop", []string{"--operation-id", "operation-one", "sandbox", "stop", "sandbox-one"}, "", "sandbox.stop"},
		{"sandbox delete", []string{"--operation-id", "operation-one", "sandbox", "delete", "sandbox-one"}, "", "sandbox.delete"},
		{"container create", []string{"--operation-id", "operation-one", "container", "create", "sandbox-one"}, `{"container_id":"container-one","attempt_id":"attempt-one","process":{"argv":["/bin/echo","hello world"]},"rootfs":"prepared-rootfs-one"}`, "container.create"},
		{"container get", []string{"container", "get", "container-one"}, "", "container.get"},
		{"container list", []string{"container", "list", "sandbox-one"}, "", "container.list"},
		{"container start", []string{"--operation-id", "operation-one", "container", "start", "container-one"}, "", "container.start"},
		{"container kill", []string{"--operation-id", "operation-one", "container", "kill", "--signal", "SIGTERM", "--grace-period", "5s", "--escalation-signal", "SIGKILL", "container-one"}, "", "container.kill"},
		{"container delete", []string{"--operation-id", "operation-one", "container", "delete", "container-one"}, "", "container.delete"},
		{"operation get", []string{"operation", "get", "operation-one"}, "", "operation.get"},
		{"events", []string{"events", "--limit", "10"}, "", "events"},
		{"logs", []string{"logs", "--attempt-id", "attempt-one", "container-one"}, "", "logs"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeRuntimeClient{}
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			status := run(context.Background(), test.args, strings.NewReader(test.stdin), &stdout, &stderr,
				func(client.Config) (runtimeClient, error) { return fake, nil },
				func() (string, error) { return "generated-operation", nil })
			if status != 0 {
				t.Fatalf("status = %d, stderr = %s", status, stderr.String())
			}
			if fake.called != test.wantCalled {
				t.Fatalf("called = %q, want %q", fake.called, test.wantCalled)
			}
			if test.name == "container kill" {
				wantPolicy := v1.TerminationPolicy{Signal: "SIGTERM", GracePeriodNanoseconds: int64(5 * time.Second), EscalationSignal: "SIGKILL"}
				if fake.killPolicy != wantPolicy {
					t.Fatalf("kill policy = %#v, want %#v", fake.killPolicy, wantPolicy)
				}
			}
			if !json.Valid(stdout.Bytes()) {
				t.Fatalf("stdout is not JSON: %q", stdout.String())
			}
		})
	}
}

// TestContainerCreatePreservesStructuredProcessInput verifies no shell join/split changes argv ordering, spaces, or duplicate environment names.
func TestContainerCreatePreservesStructuredProcessInput(t *testing.T) {
	fake := &fakeRuntimeClient{}
	input := `{"container_id":"container-one","attempt_id":"attempt-one","process":{"argv":["/bin/echo","a b","$(not-executed)"],"environment":[{"name":"A","value":"one"},{"name":"A","value":"two words"}],"working_directory":"/work"},"rootfs":"prepared-one"}`
	status := run(context.Background(), []string{"container", "create", "sandbox-one"}, strings.NewReader(input), io.Discard, io.Discard,
		func(client.Config) (runtimeClient, error) { return fake, nil },
		func() (string, error) { return "generated-operation", nil })
	if status != 0 {
		t.Fatalf("status = %d", status)
	}
	wantArgv := []string{"/bin/echo", "a b", "$(not-executed)"}
	wantEnvironment := []v1.EnvVar{{Name: "A", Value: "one"}, {Name: "A", Value: "two words"}}
	if !reflect.DeepEqual(fake.createContainer.Process.Argv, wantArgv) || !reflect.DeepEqual(fake.createContainer.Process.Environment, wantEnvironment) {
		t.Fatalf("structured process changed: %#v", fake.createContainer.Process)
	}
	if fake.operationID != "generated-operation" {
		t.Fatalf("operation ID = %q, want generated identity", fake.operationID)
	}
}

// TestKillRequiresCompleteExplicitPolicy verifies the CLI never invents a grace period or escalation signal.
func TestKillRequiresCompleteExplicitPolicy(t *testing.T) {
	fake := &fakeRuntimeClient{}
	var stderr bytes.Buffer
	status := run(context.Background(), []string{"container", "kill", "--signal", "SIGTERM", "container-one"}, strings.NewReader(""), io.Discard, &stderr,
		func(client.Config) (runtimeClient, error) { return fake, nil },
		func() (string, error) { return "generated-operation", nil })
	if status != v1.ExitStatus(v1.CodeInvalidArgument) || fake.called != "" {
		t.Fatalf("status/call = %d/%q, stderr = %s", status, fake.called, stderr.String())
	}
}

// TestReadRejectsExplicitOperationID verifies read calls cannot silently consume or ignore a mutation identity.
func TestReadRejectsExplicitOperationID(t *testing.T) {
	fake := &fakeRuntimeClient{}
	status := run(context.Background(), []string{"--operation-id", "operation-one", "sandbox", "get", "sandbox-one"}, strings.NewReader(""), io.Discard, io.Discard,
		func(client.Config) (runtimeClient, error) { return fake, nil },
		func() (string, error) { return "generated-operation", nil })
	if status != 2 || fake.called != "" {
		t.Fatalf("status/call = %d/%q, want invalid argument before API call", status, fake.called)
	}
}

// TestUnknownJSONFieldFailsBeforeMutation verifies schema drift is rejected before an operation ID reaches the client.
func TestUnknownJSONFieldFailsBeforeMutation(t *testing.T) {
	fake := &fakeRuntimeClient{}
	input := `{"sandbox_id":"sandbox-one","spec":{"network":{"mode":"none"},"resources":{"requests":{},"limits":{}}},"future":true}`
	status := run(context.Background(), []string{"sandbox", "create"}, strings.NewReader(input), io.Discard, io.Discard,
		func(client.Config) (runtimeClient, error) { return fake, nil },
		func() (string, error) { return "generated-operation", nil })
	if status != 2 || fake.called != "" {
		t.Fatalf("status/call = %d/%q, want local schema rejection", status, fake.called)
	}
}

// TestRemoteErrorFromFakeUnixServer verifies the real public client maps a fake daemon envelope to stable JSON and status three.
func TestRemoteErrorFromFakeUnixServer(t *testing.T) {
	directory := t.TempDir()
	socketPath := filepath.Join(directory, "fake.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen fake UDS: %v", err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", v1.MediaTypeJSON)
		writer.Header().Set(v1.HeaderRequestID, request.Header.Get(v1.HeaderRequestID))
		writer.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(writer).Encode(v1.ErrorEnvelope{
			Error:     v1.ErrorDetail{Code: v1.CodeNotFound, Message: "sandbox does not exist"},
			RequestID: request.Header.Get(v1.HeaderRequestID),
		})
	})}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
		<-done
	})
	var stderr bytes.Buffer
	status := run(context.Background(), []string{"--socket", socketPath, "sandbox", "get", "missing"}, strings.NewReader(""), io.Discard, &stderr, newAPIClient, newOperationID)
	if status != 3 {
		t.Fatalf("status = %d, stderr = %s", status, stderr.String())
	}
	var output errorOutput
	if err := json.Unmarshal(stderr.Bytes(), &output); err != nil {
		t.Fatalf("decode CLI error: %v", err)
	}
	if output.Error.Code != v1.CodeNotFound || output.ExitStatus != 3 {
		t.Fatalf("error output = %#v", output)
	}
}

// TestTransportFailureUsesUnavailableStatus verifies a fake client outage maps through v1 ExitStatus rather than ad hoc command codes.
func TestTransportFailureUsesUnavailableStatus(t *testing.T) {
	fake := &fakeRuntimeClient{err: &client.TransportError{Cause: errors.New("fake dial failure")}}
	var stderr bytes.Buffer
	status := run(context.Background(), []string{"sandbox", "get", "sandbox-one"}, strings.NewReader(""), io.Discard, &stderr,
		func(client.Config) (runtimeClient, error) { return fake, nil },
		func() (string, error) { return "generated-operation", nil })
	if status != v1.ExitStatus(v1.CodeUnavailable) || !strings.Contains(stderr.String(), `"code":"unavailable"`) {
		t.Fatalf("status/error = %d/%s", status, stderr.String())
	}
}

// TestNewOperationIDProducesCanonicalRandomIdentity verifies generated IDs satisfy the public bounded identifier contract.
func TestNewOperationIDProducesCanonicalRandomIdentity(t *testing.T) {
	first, err := newOperationID()
	if err != nil {
		t.Fatalf("newOperationID: %v", err)
	}
	second, err := newOperationID()
	if err != nil {
		t.Fatalf("newOperationID second: %v", err)
	}
	if first == second || !strings.HasPrefix(first, "op-") {
		t.Fatalf("generated IDs are not distinct/canonical: %q %q", first, second)
	}
	if err := v1.ValidateOperationID(first); err != nil {
		t.Fatalf("generated operation ID validation: %v", err)
	}
}

// TestInputFileReadsStrictJSON verifies explicit files use the same bounded decoder as stdin.
func TestInputFileReadsStrictJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "request.json")
	payload := []byte(`{"sandbox_id":"sandbox-one","spec":{"network":{"mode":"none"},"resources":{"requests":{},"limits":{}}}}`)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write request fixture: %v", err)
	}
	fake := &fakeRuntimeClient{}
	status := run(context.Background(), []string{"sandbox", "create", "--input", path}, strings.NewReader(""), io.Discard, io.Discard,
		func(client.Config) (runtimeClient, error) { return fake, nil },
		func() (string, error) { return "generated-operation", nil })
	if status != 0 || fake.createSandbox.SandboxID != "sandbox-one" {
		t.Fatalf("status/request = %d/%#v", status, fake.createSandbox)
	}
}
