package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "mydocker/api/runtime/v1"
	"mydocker/internal/cgroupv2"
	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/lifecycle"
	"mydocker/internal/logstore"
	"mydocker/internal/operation"
	"mydocker/internal/provider"
	"mydocker/internal/shim"
	"mydocker/internal/slim"
	"mydocker/internal/state"
)

var errInjectedMutation = errors.New("injected mutation failure")

// recordingMutator captures semantic requests while returning a deterministic error before any host effect.
type recordingMutator struct {
	createContainerCalls int
	lastCreateContainer  lifecycle.ContainerCreateRequest
	lastKill             lifecycle.KillRequest
}

// coordinatorStopMutator routes Sandbox stop through a real Coordinator while
// retaining no-effect implementations for unrelated adapter methods.
type coordinatorStopMutator struct {
	*recordingMutator
	coordinator *lifecycle.Coordinator
}

// StopSandbox delegates to the durable Coordinator so tests exercise actual
// conflict persistence and replay before the Service maps the returned error.
func (mutator *coordinatorStopMutator) StopSandbox(ctx context.Context, request lifecycle.SandboxActionRequest) (lifecycle.SandboxResult, error) {
	return mutator.coordinator.BeginSandboxStop(ctx, request)
}

// CreateSandbox records no host effects because adapter tests do not exercise Sandbox provider orchestration.
func (mutator *recordingMutator) CreateSandbox(context.Context, lifecycle.SandboxCreateRequest) (lifecycle.SandboxResult, error) {
	return lifecycle.SandboxResult{}, errInjectedMutation
}

// StopSandbox records no host effects because adapter tests use the Coordinator for reads.
func (mutator *recordingMutator) StopSandbox(context.Context, lifecycle.SandboxActionRequest) (lifecycle.SandboxResult, error) {
	return lifecycle.SandboxResult{}, errInjectedMutation
}

// RemoveSandbox records no host effects because teardown projection is covered by operation helpers.
func (mutator *recordingMutator) RemoveSandbox(context.Context, lifecycle.SandboxActionRequest) (lifecycle.SandboxResult, error) {
	return lifecycle.SandboxResult{}, errInjectedMutation
}

// CreateContainer captures structured input and returns before provider work.
func (mutator *recordingMutator) CreateContainer(_ context.Context, request lifecycle.ContainerCreateRequest) (lifecycle.ContainerResult, error) {
	mutator.createContainerCalls++
	mutator.lastCreateContainer = request
	return lifecycle.ContainerResult{}, errInjectedMutation
}

// StartContainer records no host effects because start orchestration belongs to Engine tests.
func (mutator *recordingMutator) StartContainer(context.Context, lifecycle.ContainerActionRequest) (lifecycle.ContainerResult, error) {
	return lifecycle.ContainerResult{}, errInjectedMutation
}

// KillContainer captures the complete policy and returns before signal delivery.
func (mutator *recordingMutator) KillContainer(_ context.Context, request lifecycle.KillRequest) (lifecycle.KillResult, error) {
	mutator.lastKill = request
	return lifecycle.KillResult{}, errInjectedMutation
}

// DeleteContainer records no host effects because exact cleanup belongs to Engine tests.
func (mutator *recordingMutator) DeleteContainer(context.Context, lifecycle.ContainerActionRequest) (lifecycle.ContainerResult, error) {
	return lifecycle.ContainerResult{}, errInjectedMutation
}

// fakeLogSource provides immutable identity-scoped frames without opening a host file.
type fakeLogSource struct {
	identity logstore.Identity
	frames   []logstore.Frame
	err      error
}

// Identity returns the fixed Container/Attempt binding used by registry tests.
func (source *fakeLogSource) Identity() logstore.Identity {
	return source.identity
}

// Read returns an ordered cursor suffix and clones payloads to model logstore ownership.
func (source *fakeLogSource) Read(after logstore.Cursor, limit int) ([]logstore.Frame, error) {
	if source.err != nil {
		return nil, source.err
	}
	result := make([]logstore.Frame, 0, len(source.frames))
	for _, frame := range source.frames {
		if frame.Cursor <= after {
			continue
		}
		result = append(result, frame.Clone())
		if limit > 0 && len(result) == limit {
			break
		}
	}
	return result, nil
}

// TestServiceQueriesProjectMemoryStoreFacts verifies Coordinator reads preserve public state while hiding internal response, event, stream, and process data.
func TestServiceQueriesProjectMemoryStoreFacts(t *testing.T) {
	coordinator, pair := seededCoordinator(t)
	service, err := newService(&recordingMutator{}, coordinator, NewLogRegistry())
	if err != nil {
		t.Fatalf("newService() error = %v", err)
	}
	requestContext := v1.RequestContext{RequestID: "request-read-1"}

	sandboxResponse, err := service.GetSandbox(context.Background(), requestContext, v1.GetSandboxRequest{SandboxID: "sandbox-1"})
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if sandboxResponse.Sandbox.Status.Phase != "ready" || sandboxResponse.Operation != nil {
		t.Fatalf("GetSandbox() response = %#v", sandboxResponse)
	}

	containerResponse, err := service.GetContainer(context.Background(), requestContext, v1.GetContainerRequest{ContainerID: "container-1"})
	if err != nil {
		t.Fatalf("GetContainer() error = %v", err)
	}
	container := containerResponse.Container
	if container.AttemptID != string(pair.Attempt.ID) || container.Spec.RootFS != "prepared-rootfs-1" {
		t.Fatalf("GetContainer() identity/spec = %#v", container)
	}
	for name, reference := range map[string]string{"stdout": container.Status.Streams.Stdout, "stderr": container.Status.Streams.Stderr} {
		if reference == "" || strings.Contains(reference, "/run/mydocker") {
			t.Fatalf("public %s stream reference = %q, want opaque non-host token", name, reference)
		}
	}
	if identity := container.Status.ProcessIdentity; identity == nil || strings.Contains(identity.Handle, "/proc/") || !strings.HasPrefix(identity.Handle, "v1:process:") {
		t.Fatalf("public process identity = %#v, want hashed opaque handle", identity)
	}

	operationResponse, err := service.GetOperation(context.Background(), requestContext, v1.GetOperationRequest{OperationID: "op-container-create"})
	if err != nil {
		t.Fatalf("GetOperation() error = %v", err)
	}
	if len(operationResponse.Operation.Response) != 0 || operationResponse.Operation.State != "succeeded" {
		t.Fatalf("GetOperation() = %#v, want terminal operation without internal replay JSON", operationResponse.Operation)
	}

	events, err := service.EventsAfter(context.Background(), requestContext, v1.ListEventsRequest{Limit: 100})
	if err != nil {
		t.Fatalf("EventsAfter() error = %v", err)
	}
	if len(events) == 0 {
		t.Fatal("EventsAfter() returned no lifecycle facts")
	}
	for _, event := range events {
		if len(event.Details) != 0 {
			t.Fatalf("event %d exposed internal details %s", event.Sequence, event.Details)
		}
	}
}

// TestServiceMutationConversionRejectsPathsAndPreservesPolicy verifies API-only validation happens before Engine and structured fields remain exact.
func TestServiceMutationConversionRejectsPathsAndPreservesPolicy(t *testing.T) {
	coordinator, _ := seededCoordinator(t)
	mutations := &recordingMutator{}
	service, err := newService(mutations, coordinator, NewLogRegistry())
	if err != nil {
		t.Fatalf("newService() error = %v", err)
	}
	requestContext := v1.RequestContext{RequestID: "request-mutate-1", OperationID: "op-api-mutate-1"}
	process := v1.ProcessSpec{
		Argv: []string{"/bin/demo", "", "value with spaces"},
		Environment: []v1.EnvVar{
			{Name: "DUPLICATE", Value: "first"},
			{Name: "DUPLICATE", Value: "second"},
		},
		Termination: v1.TerminationPolicy{Signal: "SIGTERM", GracePeriodNanoseconds: 17, EscalationSignal: "SIGKILL"},
	}
	_, err = service.CreateContainer(context.Background(), requestContext, v1.CreateContainerRequest{
		SandboxID: "sandbox-1", ContainerID: "container-path", AttemptID: "attempt-path", Process: process, RootFS: "/srv/rootfs",
	})
	assertV1Error(t, err, v1.CodeInvalidArgument, false)
	if mutations.createContainerCalls != 0 {
		t.Fatalf("path-shaped rootfs reached mutator %d time(s)", mutations.createContainerCalls)
	}

	_, err = service.CreateContainer(context.Background(), requestContext, v1.CreateContainerRequest{
		SandboxID: "sandbox-1", ContainerID: "container-2", AttemptID: "attempt-2", Process: process, RootFS: "prepared-rootfs-2",
	})
	assertV1Error(t, err, v1.CodeInternal, false)
	if mutations.createContainerCalls != 1 {
		t.Fatalf("valid opaque rootfs mutator calls = %d, want 1", mutations.createContainerCalls)
	}
	gotProcess := mutations.lastCreateContainer.Process
	if len(gotProcess.Argv) != 3 || gotProcess.Argv[1] != "" || len(gotProcess.Environment) != 2 || gotProcess.Environment[1].Value != "second" {
		t.Fatalf("structured process conversion = %#v", gotProcess)
	}
	if gotProcess.Termination.GracePeriod != 17*time.Nanosecond {
		t.Fatalf("process grace = %s, want 17ns", gotProcess.Termination.GracePeriod)
	}

	killPolicy := v1.TerminationPolicy{Signal: "SIGINT", GracePeriodNanoseconds: 29, EscalationSignal: "SIGQUIT"}
	_, err = service.KillContainer(context.Background(), requestContext, v1.KillContainerRequest{ContainerID: "container-1", Policy: killPolicy})
	assertV1Error(t, err, v1.CodeInternal, false)
	if got := mutations.lastKill.Policy; got.Signal != "SIGINT" || got.GracePeriod != 29*time.Nanosecond || got.EscalationSignal != "SIGQUIT" {
		t.Fatalf("KillContainer policy = %#v, want exact API values", got)
	}
}

// TestLogsAfterUsesAuthoritativeAttemptAndIdentityRegistry verifies frames are resolved without any API host path and remain checksum-bound.
func TestLogsAfterUsesAuthoritativeAttemptAndIdentityRegistry(t *testing.T) {
	coordinator, pair := seededCoordinator(t)
	identity := logstore.Identity{ContainerID: pair.Container.ID, AttemptID: pair.Attempt.ID}
	payload := []byte("hello from stdout\n")
	digest := sha256.Sum256(payload)
	frame := logstore.Frame{
		SchemaVersion: logstore.SchemaVersion,
		Identity:      identity,
		Stream:        logstore.StreamStdout,
		Cursor:        1,
		Sequence:      1,
		Payload:       payload,
		PayloadSHA256: hex.EncodeToString(digest[:]),
	}
	registry := NewLogRegistry()
	source := &fakeLogSource{identity: identity, frames: []logstore.Frame{frame}}
	if err := registry.Register(source); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	service, err := newService(&recordingMutator{}, coordinator, registry)
	if err != nil {
		t.Fatalf("newService() error = %v", err)
	}
	requestContext := v1.RequestContext{RequestID: "request-logs-1"}
	frames, err := service.LogsAfter(context.Background(), requestContext, v1.ListLogsRequest{
		ContainerID: "container-1", AttemptID: "attempt-1", Limit: 2,
	})
	if err != nil {
		t.Fatalf("LogsAfter() error = %v", err)
	}
	if len(frames) != 1 || frames[0].PayloadSHA256 != frame.PayloadSHA256 || string(frames[0].Payload) != string(payload) {
		t.Fatalf("LogsAfter() = %#v", frames)
	}
	frames[0].Payload[0] = 'X'
	if source.frames[0].Payload[0] == 'X' {
		t.Fatal("public log payload aliases registered source memory")
	}

	_, err = service.LogsAfter(context.Background(), requestContext, v1.ListLogsRequest{
		ContainerID: "container-1", AttemptID: "attempt-other", Limit: 1,
	})
	assertV1Error(t, err, v1.CodeFailedPrecondition, false)
}

// TestLogRegistryRejectsReplacementAndSupportsIdempotentRemoval verifies identity collisions never switch readers silently.
func TestLogRegistryRejectsReplacementAndSupportsIdempotentRemoval(t *testing.T) {
	registry := NewLogRegistry()
	identity := logstore.Identity{ContainerID: "container-1", AttemptID: "attempt-1"}
	source := &fakeLogSource{identity: identity}
	if err := registry.Register(source); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := registry.Register(&fakeLogSource{identity: identity}); !errors.Is(err, ErrLogAlreadyRegistered) {
		t.Fatalf("duplicate Register() error = %v, want ErrLogAlreadyRegistered", err)
	}
	located, err := registry.Locate(context.Background(), identity)
	if err != nil || located != source {
		t.Fatalf("Locate() = %#v, %v", located, err)
	}
	registration, found, err := registry.CaptureRegistration(identity)
	if err != nil || !found {
		t.Fatalf("CaptureRegistration() = (%#v, %t, %v)", registration, found, err)
	}
	if err := registry.Unregister(identity); err != nil {
		t.Fatalf("Unregister() error = %v", err)
	}
	if err := registry.Unregister(identity); err != nil {
		t.Fatalf("idempotent Unregister() error = %v", err)
	}
	if _, err := registry.Locate(context.Background(), identity); !errors.Is(err, ErrLogNotFound) {
		t.Fatalf("Locate() after unregister error = %v, want ErrLogNotFound", err)
	}
	replacement := &fakeLogSource{identity: identity}
	if err := registry.Register(replacement); err != nil {
		t.Fatalf("Register() replacement error = %v", err)
	}
	if err := registry.UnregisterRegistration(registration); err != nil {
		t.Fatalf("UnregisterRegistration() stale revision error = %v", err)
	}
	located, err = registry.Locate(context.Background(), identity)
	if err != nil || located != replacement {
		t.Fatalf("Locate() after stale unregister = %#v, %v", located, err)
	}
}

// TestMapErrorClassifiesStableInternalFamilies verifies callers never need to parse implementation messages.
func TestMapErrorClassifiesStableInternalFamilies(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		code      v1.ErrorCode
		retryable bool
	}{
		{name: "domain validation", err: domain.NewError(domain.CodeInvalidArgument, "field", "bad"), code: v1.CodeInvalidArgument},
		{name: "domain transition", err: domain.NewError(domain.CodeInvalidTransition, "phase", "bad"), code: v1.CodeFailedPrecondition},
		{name: "durable invalid request", err: &lifecycle.OperationFailureError{OperationID: "op-invalid", Reason: operation.ReasonInvalidRequest}, code: v1.CodeInvalidArgument},
		{name: "durable absent", err: &lifecycle.OperationFailureError{OperationID: "op-absent", Reason: operation.ReasonNotFound}, code: v1.CodeNotFound},
		{name: "durable precondition", err: &lifecycle.OperationFailureError{OperationID: "op-precondition", Reason: operation.ReasonPrecondition}, code: v1.CodeFailedPrecondition},
		{name: "durable conflict", err: &lifecycle.OperationFailureError{OperationID: "op-conflict", Reason: operation.ReasonConflict}, code: v1.CodeConflict},
		{name: "durable cleanup", err: &lifecycle.OperationFailureError{OperationID: "op-cleanup", Reason: operation.ReasonCleanup}, code: v1.CodeUnavailable},
		{name: "durable internal", err: &lifecycle.OperationFailureError{OperationID: "op-internal", Reason: operation.ReasonInternal}, code: v1.CodeInternal},
		{name: "state absent", err: state.ErrNotFound, code: v1.CodeNotFound},
		{name: "state conflict", err: state.ErrRevisionConflict, code: v1.CodeConflict, retryable: true},
		{name: "operation replay expired", err: state.ErrOperationExpired, code: v1.CodeOperationExpired},
		{name: "event resume gap", err: &state.EventResumeGapError{Requested: 1, FirstAvailable: 3}, code: v1.CodeResumeGap},
		{name: "retention capacity", err: state.ErrRetentionCapacity, code: v1.CodeResourceExhausted},
		{name: "operation binding", err: operation.ErrBindingMismatch, code: v1.CodeConflict},
		{name: "deadline", err: context.DeadlineExceeded, code: v1.CodeDeadlineExceeded, retryable: true},
		{name: "provider ownership", err: provider.ErrRollbackOwnerMismatch, code: v1.CodeUnsafeIdentity},
		{name: "isolation ownership", err: isolation.ErrUnsafeIdentity, code: v1.CodeUnsafeIdentity},
		{name: "cgroup unknown", err: cgroupv2.ErrUnknownState, code: v1.CodeUnavailable, retryable: true},
		{name: "shim unavailable", err: &shim.Error{Code: shim.CodeUnavailable, Message: "socket lost"}, code: v1.CodeUnavailable, retryable: true},
		{name: "launcher incomplete", err: slim.ErrLauncherIncomplete, code: v1.CodeUnavailable},
		{name: "log commit pending", err: logstore.ErrReadUnavailable, code: v1.CodeUnavailable, retryable: true},
		{name: "log corruption", err: logstore.ErrCorrupt, code: v1.CodeInternal},
		{name: "unknown", err: errors.New("private implementation detail"), code: v1.CodeInternal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertV1Error(t, MapError(test.err), test.code, test.retryable)
		})
	}
}

// TestServiceDurableConflictIsNeverRetryable verifies a persisted conflict is
// terminal on its first response, exact same-ID replay, and FileStore reopen,
// while the adapter reserves retryable conflicts for transient state races.
func TestServiceDurableConflictIsNeverRetryable(t *testing.T) {
	ctx := context.Background()
	path := secureDaemonStatePath(t)
	store, err := state.NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	coordinator, err := lifecycle.NewCoordinator(store)
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}
	create := lifecycle.SandboxCreateRequest{
		OperationID: "op-service-conflict-create", SandboxID: "sandbox-service-conflict",
		Spec: domain.SandboxSpec{Network: domain.NetworkIntent{Mode: "none"}},
	}
	begin, err := coordinator.BeginSandboxCreate(ctx, create)
	if err != nil {
		t.Fatalf("BeginSandboxCreate() error = %v", err)
	}
	if _, err := coordinator.ConfirmSandboxCreate(ctx, lifecycle.SandboxConfirmRequest{
		OperationID: create.OperationID, SandboxID: create.SandboxID, Fingerprint: begin.Fingerprint,
		Verification: lifecycle.Verification{
			Kind: lifecycle.VerificationSandboxReady, Verified: true,
			Evidence: "service-conflict-ready", ObservedAt: time.Unix(1, 0).UTC(),
		},
	}); err != nil {
		t.Fatalf("ConfirmSandboxCreate() error = %v", err)
	}
	if _, err := coordinator.BeginSandboxStop(ctx, lifecycle.SandboxActionRequest{
		OperationID: "op-service-conflict-active", SandboxID: create.SandboxID,
	}); err != nil {
		t.Fatalf("BeginSandboxStop(active) error = %v", err)
	}

	conflictOperationID := "op-service-conflict-terminal"
	assertConflict := func(label string, service *Service, requestID string) {
		t.Helper()
		_, callErr := service.StopSandbox(ctx, v1.RequestContext{
			RequestID: requestID, OperationID: conflictOperationID,
		}, v1.StopSandboxRequest{SandboxID: string(create.SandboxID)})
		if callErr == nil {
			t.Fatalf("%s conflict error = nil", label)
		}
		assertV1Error(t, callErr, v1.CodeConflict, false)
	}
	service, err := newService(&coordinatorStopMutator{recordingMutator: &recordingMutator{}, coordinator: coordinator}, coordinator, NewLogRegistry())
	if err != nil {
		t.Fatalf("newService() error = %v", err)
	}
	assertConflict("first", service, "request-service-conflict-first")
	assertConflict("same-process replay", service, "request-service-conflict-replay")
	if err := store.Close(); err != nil {
		t.Fatalf("FileStore.Close() error = %v", err)
	}

	reopened, err := state.NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(reopen) error = %v", err)
	}
	defer func() {
		if closeErr := reopened.Close(); closeErr != nil {
			t.Errorf("FileStore.Close(reopened) error = %v", closeErr)
		}
	}()
	coordinator, err = lifecycle.NewCoordinator(reopened)
	if err != nil {
		t.Fatalf("NewCoordinator(reopen) error = %v", err)
	}
	service, err = newService(&coordinatorStopMutator{recordingMutator: &recordingMutator{}, coordinator: coordinator}, coordinator, NewLogRegistry())
	if err != nil {
		t.Fatalf("newService(reopen) error = %v", err)
	}
	assertConflict("reopen replay", service, "request-service-conflict-reopen")

	assertV1Error(t, MapError(&operation.ActiveConflictError{
		Target:   operation.Target{Kind: operation.TargetSandbox, ID: string(create.SandboxID)},
		ActiveID: "op-transient-active", RequestedID: "op-transient-requested",
	}), v1.CodeConflict, true)
}

// secureDaemonStatePath creates an owner-only state location and removes the
// stable sibling lock that FileStore intentionally retains after Close.
func secureDaemonStatePath(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp(".", ".daemon-state-test-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		_ = os.RemoveAll(directory)
		t.Fatalf("Abs() error = %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(absolute); err != nil {
			t.Errorf("RemoveAll() error = %v", err)
		}
		pattern := filepath.Join(filepath.Dir(absolute), "."+filepath.Base(absolute)+"-*.state.lock")
		anchors, globErr := filepath.Glob(pattern)
		if globErr != nil {
			t.Errorf("Glob(state lock) error = %v", globErr)
			return
		}
		for _, anchor := range anchors {
			if err := os.Remove(anchor); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Errorf("Remove(state lock) error = %v", err)
			}
		}
	})
	return filepath.Join(absolute, "state.json")
}

// TestMapErrorResumeGapMessageCoversFutureCursor verifies the public diagnostic
// describes both compacted and ahead-of-stream positions without a false cause.
func TestMapErrorResumeGapMessageCoversFutureCursor(t *testing.T) {
	mapped := MapError(&state.EventResumeGapError{Requested: 9, FirstAvailable: 3, LastAvailable: 4})
	var typed *v1.Error
	if !errors.As(mapped, &typed) {
		t.Fatalf("MapError(future cursor) = %v, want v1.Error", mapped)
	}
	if typed.Code != v1.CodeResumeGap || typed.Retryable || !strings.Contains(typed.Message, "outside the retained committed event stream") {
		t.Fatalf("MapError(future cursor) = %#v, want non-retryable generic resume gap", typed)
	}
}

// seededCoordinator creates fully confirmed M1 Sandbox and Container facts in MemoryStore without invoking Linux providers.
func seededCoordinator(t *testing.T) (*lifecycle.Coordinator, domain.ContainerAttempt) {
	t.Helper()
	store := state.NewMemoryStore()
	coordinator, err := lifecycle.NewCoordinator(store)
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}
	ctx := context.Background()
	sandboxBegin, err := coordinator.BeginSandboxCreate(ctx, lifecycle.SandboxCreateRequest{
		OperationID: "op-sandbox-create",
		SandboxID:   "sandbox-1",
		Spec: domain.SandboxSpec{
			Hostname: "demo",
			Network:  domain.NetworkIntent{Mode: "loopback"},
		},
	})
	if err != nil {
		t.Fatalf("BeginSandboxCreate() error = %v", err)
	}
	_, err = coordinator.ConfirmSandboxCreate(ctx, lifecycle.SandboxConfirmRequest{
		OperationID: "op-sandbox-create",
		SandboxID:   "sandbox-1",
		Fingerprint: sandboxBegin.Fingerprint,
		Verification: lifecycle.Verification{
			Kind: lifecycle.VerificationSandboxReady, Verified: true,
			Evidence: "sandbox-ready-evidence", ObservedAt: time.Unix(10, 0).UTC(),
		},
	})
	if err != nil {
		t.Fatalf("ConfirmSandboxCreate() error = %v", err)
	}
	containerBegin, err := coordinator.BeginContainerCreate(ctx, lifecycle.ContainerCreateRequest{
		OperationID: "op-container-create",
		SandboxID:   "sandbox-1",
		ContainerID: "container-1",
		AttemptID:   "attempt-1",
		Process: domain.ProcessSpec{
			Argv: []string{"/bin/demo", "argument"},
			Termination: domain.TerminationPolicy{
				Signal: "SIGTERM", GracePeriod: time.Second, EscalationSignal: "SIGKILL",
			},
		},
		RootFS: "prepared-rootfs-1",
	})
	if err != nil {
		t.Fatalf("BeginContainerCreate() error = %v", err)
	}
	identity := domain.ProcessIdentity{Verified: true, Handle: "/proc/123/fd/9", Evidence: "provider-process-evidence"}
	containerResult, err := coordinator.ConfirmContainerCreate(ctx, lifecycle.ContainerConfirmRequest{
		OperationID: "op-container-create",
		ContainerID: "container-1",
		Fingerprint: containerBegin.Fingerprint,
		Verification: lifecycle.Verification{
			Kind: lifecycle.VerificationAttemptCreated, Verified: true,
			Evidence: "attempt-created-evidence", ObservedAt: time.Unix(20, 0).UTC(),
			ProcessIdentity: &identity,
			Streams: domain.StreamReferences{
				Stdout: "/run/mydocker/attempt-1/stdout", Stderr: "/run/mydocker/attempt-1/stderr",
			},
		},
	})
	if err != nil {
		t.Fatalf("ConfirmContainerCreate() error = %v", err)
	}
	if containerResult.ContainerAttempt == nil {
		t.Fatal("ConfirmContainerCreate() omitted retained pair")
	}
	return coordinator, containerResult.ContainerAttempt.Clone()
}

// assertV1Error verifies code and retry semantics without depending on private diagnostic strings.
func assertV1Error(t *testing.T, err error, code v1.ErrorCode, retryable bool) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want v1 code %q", code)
	}
	var typed *v1.Error
	if !errors.As(err, &typed) {
		t.Fatalf("error = %T %v, want *v1.Error", err, err)
	}
	if typed.Code != code || typed.Retryable != retryable {
		t.Fatalf("v1 error = %#v, want code %q retryable=%t", typed, code, retryable)
	}
}
