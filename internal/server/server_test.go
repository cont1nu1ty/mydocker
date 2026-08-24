package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "mydocker/api/runtime/v1"
	"mydocker/pkg/client"
)

// fakeService provides deterministic idempotency and injectable read/cancellation behavior for transport tests.
type fakeService struct {
	mu              sync.Mutex
	createResponses map[string]v1.SandboxResponse
	createCalls     int
	createEffects   int
	createHook      func(context.Context, v1.RequestContext, v1.CreateSandboxRequest) (v1.SandboxResponse, error)
	getSandboxHook  func(context.Context, v1.RequestContext, string) (v1.SandboxResponse, error)
	eventsHook      func(context.Context, v1.RequestContext, v1.ListEventsRequest) ([]v1.Event, error)
	logsHook        func(context.Context, v1.RequestContext, v1.ListLogsRequest) ([]v1.LogFrame, error)
	events          []v1.Event
	logs            []v1.LogFrame
}

// newFakeService initializes the replay map used to model service-level operation idempotency.
func newFakeService() *fakeService {
	return &fakeService{createResponses: make(map[string]v1.SandboxResponse)}
}

// CreateSandbox replays a prior operation result or records exactly one fake side effect.
func (service *fakeService) CreateSandbox(ctx context.Context, requestContext v1.RequestContext, input v1.CreateSandboxRequest) (v1.SandboxResponse, error) {
	if service.createHook != nil {
		return service.createHook(ctx, requestContext, input)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	service.createCalls++
	if response, ok := service.createResponses[requestContext.OperationID]; ok {
		return response, nil
	}
	service.createEffects++
	operation := v1.Operation{
		ID: requestContext.OperationID, Type: "create", Target: v1.ResourceRef{Kind: "sandbox", ID: input.SandboxID},
		Fingerprint: v1.RequestFingerprint{Version: 1, SHA256: strings.Repeat("a", 64)},
		State:       "succeeded", Stage: "complete", Result: "succeeded", Reason: "none",
	}
	response := v1.SandboxResponse{
		Sandbox: v1.Sandbox{ID: input.SandboxID, Spec: input.Spec, Status: v1.SandboxStatus{
			Phase: "ready", Generation: 1, ObservedGeneration: 1,
			LastObservation: v1.LifecycleObservation{OperationID: requestContext.OperationID, EventSequence: 1, Reason: "none"},
		}},
		Operation: &operation,
	}
	service.createResponses[requestContext.OperationID] = response
	return response, nil
}

// StopSandbox returns a bounded not-implemented failure for routes not exercised by these transport tests.
func (service *fakeService) StopSandbox(context.Context, v1.RequestContext, v1.StopSandboxRequest) (v1.SandboxResponse, error) {
	return v1.SandboxResponse{}, fakeNotImplemented()
}

// DeleteSandbox returns a bounded not-implemented failure for routes not exercised by these transport tests.
func (service *fakeService) DeleteSandbox(context.Context, v1.RequestContext, v1.DeleteSandboxRequest) (v1.OperationResponse, error) {
	return v1.OperationResponse{}, fakeNotImplemented()
}

// GetSandbox delegates to an injected lookup or returns a typed missing-resource failure.
func (service *fakeService) GetSandbox(ctx context.Context, requestContext v1.RequestContext, input v1.GetSandboxRequest) (v1.SandboxResponse, error) {
	if service.getSandboxHook != nil {
		return service.getSandboxHook(ctx, requestContext, input.SandboxID)
	}
	return v1.SandboxResponse{}, v1.NewError(v1.CodeNotFound, "sandbox_id", "sandbox does not exist")
}

// ListSandboxes returns an empty deterministic snapshot for transport coverage.
func (service *fakeService) ListSandboxes(context.Context, v1.RequestContext, v1.ListSandboxesRequest) (v1.SandboxListResponse, error) {
	return v1.SandboxListResponse{Sandboxes: []v1.Sandbox{}}, nil
}

// CreateContainer returns a bounded not-implemented failure for routes not exercised by these transport tests.
func (service *fakeService) CreateContainer(context.Context, v1.RequestContext, v1.CreateContainerRequest) (v1.ContainerResponse, error) {
	return v1.ContainerResponse{}, fakeNotImplemented()
}

// StartContainer returns a bounded not-implemented failure for routes not exercised by these transport tests.
func (service *fakeService) StartContainer(context.Context, v1.RequestContext, v1.StartContainerRequest) (v1.ContainerResponse, error) {
	return v1.ContainerResponse{}, fakeNotImplemented()
}

// KillContainer returns a bounded not-implemented failure for routes not exercised by these transport tests.
func (service *fakeService) KillContainer(context.Context, v1.RequestContext, v1.KillContainerRequest) (v1.ContainerResponse, error) {
	return v1.ContainerResponse{}, fakeNotImplemented()
}

// DeleteContainer returns a bounded not-implemented failure for routes not exercised by these transport tests.
func (service *fakeService) DeleteContainer(context.Context, v1.RequestContext, v1.DeleteContainerRequest) (v1.OperationResponse, error) {
	return v1.OperationResponse{}, fakeNotImplemented()
}

// GetContainer returns a bounded not-implemented failure for routes not exercised by these transport tests.
func (service *fakeService) GetContainer(context.Context, v1.RequestContext, v1.GetContainerRequest) (v1.ContainerResponse, error) {
	return v1.ContainerResponse{}, fakeNotImplemented()
}

// ListContainers returns an empty deterministic snapshot for transport coverage.
func (service *fakeService) ListContainers(context.Context, v1.RequestContext, v1.ListContainersRequest) (v1.ContainerListResponse, error) {
	return v1.ContainerListResponse{Containers: []v1.Container{}}, nil
}

// GetOperation returns a bounded not-implemented failure for routes not exercised by these transport tests.
func (service *fakeService) GetOperation(context.Context, v1.RequestContext, v1.GetOperationRequest) (v1.OperationResponse, error) {
	return v1.OperationResponse{}, fakeNotImplemented()
}

// EventsAfter returns a bounded ordered suffix suitable for server-side has-more derivation.
func (service *fakeService) EventsAfter(ctx context.Context, requestContext v1.RequestContext, input v1.ListEventsRequest) ([]v1.Event, error) {
	if service.eventsHook != nil {
		return service.eventsHook(ctx, requestContext, input)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	result := make([]v1.Event, 0, input.Limit)
	for _, event := range service.events {
		if event.Sequence > input.AfterSequence {
			result = append(result, event)
		}
		if len(result) == input.Limit {
			break
		}
	}
	return result, nil
}

// LogsAfter delegates to an injected cursor outcome or returns a bounded identity-scoped suffix suitable for API pagination tests.
func (service *fakeService) LogsAfter(ctx context.Context, requestContext v1.RequestContext, input v1.ListLogsRequest) ([]v1.LogFrame, error) {
	if service.logsHook != nil {
		return service.logsHook(ctx, requestContext, input)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	result := make([]v1.LogFrame, 0, input.Limit)
	for _, frame := range service.logs {
		if frame.ContainerID == input.ContainerID && frame.AttemptID == input.AttemptID && frame.Cursor > input.AfterCursor {
			result = append(result, frame)
		}
		if len(result) == input.Limit {
			break
		}
	}
	return result, nil
}

// fakeNotImplemented classifies an unused fake route without exposing an arbitrary Go error.
func fakeNotImplemented() error {
	return v1.NewError(v1.CodeFailedPrecondition, "test_service", "route is not implemented by this fake")
}

// TestUDSSocketModeAndIdempotentReplay verifies exact permissions and one fake side effect for duplicate operation identity.
func TestUDSSocketModeAndIdempotentReplay(t *testing.T) {
	service := newFakeService()
	server, apiClient, socketPath := startTestServer(t, service, Config{SocketMode: 0o660})
	_ = server
	info, err := os.Lstat(socketPath)
	if err != nil {
		t.Fatalf("lstat socket: %v", err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o660 {
		t.Fatalf("socket mode = %v, want socket 0660", info.Mode())
	}
	input := v1.CreateSandboxRequest{SandboxID: "sandbox-one", Spec: v1.SandboxSpec{Network: v1.NetworkIntent{Mode: "none"}}}
	first, err := apiClient.CreateSandbox(context.Background(), "operation-one", input)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	second, err := apiClient.CreateSandbox(context.Background(), "operation-one", input)
	if err != nil {
		t.Fatalf("retry create: %v", err)
	}
	if first.Sandbox.ID != second.Sandbox.ID || first.Operation == nil || second.Operation == nil || first.Operation.ID != second.Operation.ID {
		t.Fatalf("replayed responses differ: %#v %#v", first, second)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.createCalls != 2 || service.createEffects != 1 {
		t.Fatalf("calls/effects = %d/%d, want 2/1", service.createCalls, service.createEffects)
	}
}

// TestUnsupportedVersionReturnsTypedJSON verifies non-v1 paths never fall through to a text 404.
func TestUnsupportedVersionReturnsTypedJSON(t *testing.T) {
	_, _, socketPath := startTestServer(t, newFakeService(), Config{})
	request, err := http.NewRequest(http.MethodGet, "http://mydocker/v2/sandboxes", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	request.Header.Set(v1.HeaderRequestID, "request-one")
	response, err := rawUnixHTTPClient(socketPath).Do(request)
	if err != nil {
		t.Fatalf("version request: %v", err)
	}
	defer response.Body.Close()
	var envelope v1.ErrorEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if response.StatusCode != http.StatusNotFound || envelope.Error.Code != v1.CodeUnsupportedVersion {
		t.Fatalf("status/code = %d/%q", response.StatusCode, envelope.Error.Code)
	}
}

// TestStrictJSONRejectsUnknownAndTrailingData verifies malformed bodies never reach the lifecycle service.
func TestStrictJSONRejectsUnknownAndTrailingData(t *testing.T) {
	service := newFakeService()
	_, _, socketPath := startTestServer(t, service, Config{})
	valid := `{"sandbox_id":"sandbox-one","spec":{"network":{"mode":"none"},"resources":{"requests":{},"limits":{}}}}`
	tests := map[string]string{
		"unknown field":  strings.TrimSuffix(valid, "}") + `,"future":true}`,
		"trailing value": valid + `{}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			response := postRawSandbox(t, socketPath, body)
			defer response.Body.Close()
			var envelope v1.ErrorEnvelope
			if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}
			if response.StatusCode != http.StatusBadRequest || envelope.Error.Code != v1.CodeInvalidArgument {
				t.Fatalf("status/code = %d/%q", response.StatusCode, envelope.Error.Code)
			}
		})
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.createCalls != 0 {
		t.Fatalf("malformed requests reached service %d times", service.createCalls)
	}
}

// TestRequestSizeLimitReturnsTypedError verifies oversized JSON is bounded before service dispatch.
func TestRequestSizeLimitReturnsTypedError(t *testing.T) {
	service := newFakeService()
	_, _, socketPath := startTestServer(t, service, Config{MaxRequestBytes: 64})
	body := `{"sandbox_id":"sandbox-one","spec":{"hostname":"` + strings.Repeat("x", 128) + `","network":{"mode":"none"},"resources":{"requests":{},"limits":{}}}}`
	response := postRawSandbox(t, socketPath, body)
	defer response.Body.Close()
	var envelope v1.ErrorEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if response.StatusCode != http.StatusRequestEntityTooLarge || envelope.Error.Code != v1.CodeRequestTooLarge {
		t.Fatalf("status/code = %d/%q", response.StatusCode, envelope.Error.Code)
	}
}

// TestClientReceivesStableServiceError verifies UDS transport preserves code, correlation, and CLI mapping.
func TestClientReceivesStableServiceError(t *testing.T) {
	service := newFakeService()
	service.getSandboxHook = func(context.Context, v1.RequestContext, string) (v1.SandboxResponse, error) {
		return v1.SandboxResponse{}, v1.NewError(v1.CodeNotFound, "sandbox_id", "sandbox does not exist")
	}
	_, apiClient, _ := startTestServer(t, service, Config{})
	_, err := apiClient.GetSandbox(context.Background(), "missing")
	if client.CodeOf(err) != v1.CodeNotFound || client.ExitStatus(err) != 3 {
		t.Fatalf("code/status = %q/%d for %v", client.CodeOf(err), client.ExitStatus(err), err)
	}
}

// TestEventPaginationResumeToken verifies pages neither overlap nor expose raw sequence tokens.
func TestEventPaginationResumeToken(t *testing.T) {
	service := newFakeService()
	service.events = []v1.Event{newTestEvent(1), newTestEvent(2), newTestEvent(3)}
	_, apiClient, _ := startTestServer(t, service, Config{})
	first, err := apiClient.Events(context.Background(), "", 2)
	if err != nil {
		t.Fatalf("first event page: %v", err)
	}
	if len(first.Events) != 2 || !first.HasMore || first.NextResumeToken == "" || first.NextResumeToken == "2" {
		t.Fatalf("first page = %#v", first)
	}
	second, err := apiClient.Events(context.Background(), first.NextResumeToken, 2)
	if err != nil {
		t.Fatalf("second event page: %v", err)
	}
	if len(second.Events) != 1 || second.Events[0].Sequence != 3 || second.HasMore {
		t.Fatalf("second page = %#v", second)
	}
}

// newTestEvent builds one complete public lifecycle fact for server and client pagination tests.
func newTestEvent(sequence uint64) v1.Event {
	target := v1.ResourceRef{Kind: "sandbox", ID: "sandbox-one"}
	return v1.Event{
		Sequence: sequence, OperationID: fmt.Sprintf("operation-event-%d", sequence),
		Type: "create", Target: target, Resources: []v1.ResourceRef{target},
		Stage: "complete", Result: "succeeded", Reason: "none",
		OccurredAt: time.Unix(int64(sequence), 0).UTC(), Generation: 1, ObservedGeneration: 1,
	}
}

// TestEventResumeGapCrossesUDSTransport verifies HTTP 410 and the versioned
// resume_gap envelope remain a typed client outcome instead of a decode failure.
func TestEventResumeGapCrossesUDSTransport(t *testing.T) {
	service := newFakeService()
	service.eventsHook = func(context.Context, v1.RequestContext, v1.ListEventsRequest) ([]v1.Event, error) {
		return nil, v1.NewError(v1.CodeResumeGap, "resume_token", "restart with an empty resume token")
	}
	_, apiClient, _ := startTestServer(t, service, Config{})
	_, err := apiClient.Events(context.Background(), v1.NewResumeToken(1), 2)
	if !client.IsResumeGap(err) || client.ExitStatus(err) != 4 {
		t.Fatalf("Events(stale) code/status = %q/%d for %v, want resume_gap/4", client.CodeOf(err), client.ExitStatus(err), err)
	}
}

// TestLogPaginationBindsAttemptAndPreservesStreams verifies payload, cursor, and per-stream sequence projection.
func TestLogPaginationBindsAttemptAndPreservesStreams(t *testing.T) {
	service := newFakeService()
	service.logs = []v1.LogFrame{
		newTestLogFrame("container-one", "attempt-one", "stdout", 1, 1, []byte("first stdout")),
		newTestLogFrame("container-one", "attempt-one", "stderr", 2, 1, []byte("first stderr")),
		newTestLogFrame("container-one", "attempt-one", "stdout", 3, 2, []byte("second stdout")),
	}
	_, apiClient, _ := startTestServer(t, service, Config{})
	first, err := apiClient.Logs(context.Background(), "container-one", "attempt-one", "", 2)
	if err != nil {
		t.Fatalf("first log page: %v", err)
	}
	if len(first.Frames) != 2 || !first.HasMore || string(first.Frames[0].Payload) != "first stdout" || first.NextCursor == "" {
		t.Fatalf("first log page = %#v", first)
	}
	second, err := apiClient.Logs(context.Background(), "container-one", "attempt-one", first.NextCursor, 2)
	if err != nil {
		t.Fatalf("second log page: %v", err)
	}
	if len(second.Frames) != 1 || second.Frames[0].Cursor != 3 || second.Frames[0].Sequence != 2 || second.HasMore {
		t.Fatalf("second log page = %#v", second)
	}
	if _, err := apiClient.Logs(context.Background(), "container-one", "attempt-two", first.NextCursor, 2); client.CodeOf(err) != v1.CodeInvalidArgument {
		t.Fatalf("cross-Attempt cursor error = %v, want invalid_argument", err)
	}
}

// TestLogFutureCursorGapCrossesUDSTransport verifies a valid identity-bound but ahead-of-stream cursor remains resume_gap while a cross-Attempt cursor remains invalid_argument.
func TestLogFutureCursorGapCrossesUDSTransport(t *testing.T) {
	service := newFakeService()
	service.logsHook = func(context.Context, v1.RequestContext, v1.ListLogsRequest) ([]v1.LogFrame, error) {
		return nil, v1.NewError(v1.CodeResumeGap, "after", "restart with an empty log cursor")
	}
	_, apiClient, _ := startTestServer(t, service, Config{})
	future, err := v1.NewLogCursor("container-one", "attempt-one", 2)
	if err != nil {
		t.Fatalf("NewLogCursor(future) error = %v", err)
	}
	_, err = apiClient.Logs(context.Background(), "container-one", "attempt-one", future, 2)
	if !client.IsResumeGap(err) || client.CodeOf(err) != v1.CodeResumeGap || client.ExitStatus(err) != 4 {
		t.Fatalf("Logs(future) code/status = %q/%d for %v, want resume_gap/4", client.CodeOf(err), client.ExitStatus(err), err)
	}
	_, err = apiClient.Logs(context.Background(), "container-one", "attempt-two", future, 2)
	if client.CodeOf(err) != v1.CodeInvalidArgument || client.ExitStatus(err) != 2 {
		t.Fatalf("Logs(cross-Attempt cursor) code/status = %q/%d for %v, want invalid_argument/2", client.CodeOf(err), client.ExitStatus(err), err)
	}
}

// TestLogsRequireAttemptIdentity verifies the public route cannot fall back to a Container-only log lookup.
func TestLogsRequireAttemptIdentity(t *testing.T) {
	_, _, socketPath := startTestServer(t, newFakeService(), Config{})
	request, err := http.NewRequest(http.MethodGet, "http://mydocker/v1/containers/container-one/logs?limit=10", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	request.Header.Set(v1.HeaderRequestID, "request-one")
	response, err := rawUnixHTTPClient(socketPath).Do(request)
	if err != nil {
		t.Fatalf("logs request: %v", err)
	}
	defer response.Body.Close()
	var envelope v1.ErrorEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if response.StatusCode != http.StatusBadRequest || envelope.Error.Code != v1.CodeInvalidArgument {
		t.Fatalf("status/code = %d/%q", response.StatusCode, envelope.Error.Code)
	}
}

// TestClientDisconnectCancelsServiceContext verifies abandoned requests do not leave handlers running until timeout.
func TestClientDisconnectCancelsServiceContext(t *testing.T) {
	service := newFakeService()
	started := make(chan struct{})
	canceled := make(chan error, 1)
	service.createHook = func(ctx context.Context, _ v1.RequestContext, _ v1.CreateSandboxRequest) (v1.SandboxResponse, error) {
		close(started)
		<-ctx.Done()
		canceled <- ctx.Err()
		return v1.SandboxResponse{}, ctx.Err()
	}
	_, _, socketPath := startTestServer(t, service, Config{HandlerTimeout: 10 * time.Second})
	connection, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial Unix socket: %v", err)
	}
	body := `{"sandbox_id":"sandbox-one","spec":{"network":{"mode":"none"},"resources":{"requests":{},"limits":{}}}}`
	request := fmt.Sprintf("POST /v1/sandboxes HTTP/1.1\r\nHost: mydocker\r\nContent-Type: application/json\r\n%s: request-one\r\n%s: operation-one\r\nContent-Length: %d\r\n\r\n%s", v1.HeaderRequestID, v1.HeaderOperationID, len(body), body)
	if _, err := io.WriteString(connection, request); err != nil {
		t.Fatalf("write raw request: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("service did not receive request")
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close client connection: %v", err)
	}
	select {
	case err := <-canceled:
		if err != context.Canceled {
			t.Fatalf("service context error = %v, want canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("service context was not canceled after client disconnect")
	}
}

// TestStartRefusesExistingNonSocket verifies startup never overwrites an unrelated filesystem object.
func TestStartRefusesExistingNonSocket(t *testing.T) {
	directory := t.TempDir()
	socketPath := filepath.Join(directory, "mydockerd.sock")
	if err := os.WriteFile(socketPath, []byte("keep"), 0o600); err != nil {
		t.Fatalf("create sentinel: %v", err)
	}
	server, err := New(Config{SocketPath: socketPath}, newFakeService())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := server.Start(); err == nil {
		t.Fatal("Start succeeded over a regular file")
	}
	payload, err := os.ReadFile(socketPath)
	if err != nil || string(payload) != "keep" {
		t.Fatalf("sentinel changed: %q, %v", payload, err)
	}
}

// TestStartRefusesActiveOwnedSocket verifies stale cleanup never replaces a live daemon listener.
func TestStartRefusesActiveOwnedSocket(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "mydockerd.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatalf("listen sentinel socket: %v", err)
	}
	defer listener.Close()
	server, err := New(Config{SocketPath: socketPath}, newFakeService())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := server.Start(); err == nil {
		t.Fatal("Start replaced an active Unix listener")
	}
	connection, err := net.DialTimeout("unix", socketPath, time.Second)
	if err != nil {
		t.Fatalf("original listener is no longer reachable: %v", err)
	}
	_ = connection.Close()
}

// startTestServer binds a temporary UDS synchronously and registers bounded graceful cleanup.
func startTestServer(t *testing.T, service Service, partial Config) (*Server, *client.Client, string) {
	t.Helper()
	socketPath := filepath.Join(t.TempDir(), "mydockerd.sock")
	partial.SocketPath = socketPath
	server, err := New(partial, service)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := server.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	apiClient, err := client.New(client.Config{SocketPath: socketPath, Timeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() {
		apiClient.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("shutdown server: %v", err)
		}
	})
	return server, apiClient, socketPath
}

// rawUnixHTTPClient constructs an HTTP transport dedicated to one temporary Unix socket.
func rawUnixHTTPClient(socketPath string) *http.Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
	}
	return &http.Client{Transport: transport, Timeout: 3 * time.Second}
}

// postRawSandbox sends caller-controlled bytes so malformed JSON behavior can be tested below pkg/client validation.
func postRawSandbox(t *testing.T, socketPath, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, "http://mydocker/v1/sandboxes", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	request.Header.Set("Content-Type", v1.MediaTypeJSON)
	request.Header.Set(v1.HeaderRequestID, "request-one")
	request.Header.Set(v1.HeaderOperationID, "operation-one")
	response, err := rawUnixHTTPClient(socketPath).Do(request)
	if err != nil {
		t.Fatalf("post raw sandbox: %v", err)
	}
	return response
}

// newTestLogFrame builds checksum-valid output evidence for server and client pagination tests.
func newTestLogFrame(containerID, attemptID, stream string, cursor, sequence uint64, payload []byte) v1.LogFrame {
	digest := sha256.Sum256(payload)
	return v1.LogFrame{
		ContainerID: containerID, AttemptID: attemptID, Stream: stream,
		Cursor: cursor, Sequence: sequence, Payload: append([]byte(nil), payload...),
		PayloadSHA256: hex.EncodeToString(digest[:]),
	}
}
