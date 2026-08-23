package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/lifecycle"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
	"mydocker/internal/state"
)

// fakeHost is a deterministic in-memory implementation of every M3 provider boundary.
type fakeHost struct {
	mu                      sync.Mutex
	present                 map[ownership.Kind]bool
	receipts                map[ownership.Kind]ownership.Receipt
	created                 map[ownership.Kind]int
	callOrder               []ownership.Kind
	namespaceRequests       []provider.NamespaceRequest
	rootfsRequests          []provider.RootfsRequest
	attempt                 AttemptObservation
	attemptSequence         []AttemptObservation
	failBeforeEnsureKind    ownership.Kind
	failAfterEnsureKind     ownership.Kind
	rollbackRequiredKind    ownership.Kind
	failInspectKind         ownership.Kind
	inspectUnavailableKind  ownership.Kind
	failCleanupKind         ownership.Kind
	failedOnce              bool
	cleanupFailedOnce       bool
	failAfterRelease        bool
	releaseFailedOnce       bool
	releaseCalls            int
	terminalAfterRelease    bool
	signalResponses         map[string]provider.SignalObservation
	signalDeliveries        int
	failAfterSignal         bool
	signalFailedOnce        bool
	signalNotRunning        bool
	signalSteps             chan<- provider.SignalStep
	observeUnavailable      bool
	observeErr              error
	observeEntered          chan<- struct{}
	observeRelease          <-chan struct{}
	keepRunningAfterInitial bool
	cancelNextObservation   func()
	signalDeliveredAt       time.Time
	oom                     uint64
	oomKill                 uint64
	oomGroupKill            uint64
}

// newFakeHost returns a provider whose shim starts prepared and changes state only through gate or signal calls.
func newFakeHost() *fakeHost {
	return &fakeHost{
		present:         make(map[ownership.Kind]bool),
		receipts:        make(map[ownership.Kind]ownership.Receipt),
		created:         make(map[ownership.Kind]int),
		attempt:         AttemptObservation{Prepared: true, Evidence: "prepared-evidence"},
		signalResponses: make(map[string]provider.SignalObservation),
	}
}

// testEngine builds a Linux-profile engine and bounded rollback registry over the fake host.
func testEngine(t *testing.T, host *fakeHost) (*Engine, *state.MemoryStore) {
	t.Helper()
	providers := testProviders(t, host)
	store := state.NewMemoryStore()
	engine, err := NewWithClock(store, providers, fixedClock{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return engine, store
}

// testProviders wires every bounded inverse back to the owner-checking fake cleanup path.
func testProviders(t *testing.T, host *fakeHost) Providers {
	t.Helper()
	registry, err := provider.NewRollbackRegistry(
		provider.RollbackRegistration{Provider: ownership.ProviderCgroupV2, Action: ownership.ActionRemoveCgroup, Handler: func(_ context.Context, _ ownership.OwnerKey, receipt ownership.Receipt) (provider.CleanupObservation, error) {
			return host.remove(provider.OwnedReceiptRequest{Owner: receipt.Owner, Receipt: receipt}, receipt.Kind)
		}},
		provider.RollbackRegistration{Provider: ownership.ProviderLinux, Action: ownership.ActionStopProcess, Handler: func(_ context.Context, _ ownership.OwnerKey, receipt ownership.Receipt) (provider.CleanupObservation, error) {
			return host.remove(provider.OwnedReceiptRequest{Owner: receipt.Owner, Receipt: receipt}, receipt.Kind)
		}},
		provider.RollbackRegistration{Provider: ownership.ProviderLinux, Action: ownership.ActionUnmountRoot, Handler: func(_ context.Context, _ ownership.OwnerKey, receipt ownership.Receipt) (provider.CleanupObservation, error) {
			return host.remove(provider.OwnedReceiptRequest{Owner: receipt.Owner, Receipt: receipt}, receipt.Kind)
		}},
		provider.RollbackRegistration{Provider: ownership.ProviderLinux, Action: ownership.ActionCloseGate, Handler: func(_ context.Context, _ ownership.OwnerKey, receipt ownership.Receipt) (provider.CleanupObservation, error) {
			return host.remove(provider.OwnedReceiptRequest{Owner: receipt.Owner, Receipt: receipt}, receipt.Kind)
		}},
		provider.RollbackRegistration{Provider: ownership.ProviderLinux, Action: ownership.ActionCloseStreams, Handler: func(_ context.Context, _ ownership.OwnerKey, receipt ownership.Receipt) (provider.CleanupObservation, error) {
			return host.remove(provider.OwnedReceiptRequest{Owner: receipt.Owner, Receipt: receipt}, receipt.Kind)
		}},
	)
	if err != nil {
		t.Fatalf("new rollback registry: %v", err)
	}
	return Providers{Cgroup: host, Isolation: host, Supervisor: host, Rollback: registry}
}

// prepareCreatedContainer drives the canonical fake Sandbox and Container acquisition path so Start tests begin at the durable Created boundary.
func prepareCreatedContainer(t *testing.T, engine *Engine) {
	t.Helper()
	ctx := context.Background()
	if _, err := engine.CreateSandbox(ctx, lifecycle.SandboxCreateRequest{
		OperationID: "op-setup-sandbox", SandboxID: "sandbox-start-test",
		Spec: domain.SandboxSpec{Hostname: "sandbox-start-test", Network: domain.NetworkIntent{Mode: "none"}},
	}); err != nil {
		t.Fatalf("prepare Sandbox: %v", err)
	}
	if _, err := engine.CreateContainer(ctx, lifecycle.ContainerCreateRequest{
		OperationID: "op-setup-container", SandboxID: "sandbox-start-test", ContainerID: "container-start-test", AttemptID: "attempt-start-test",
		Process: domain.ProcessSpec{Argv: []string{"/bin/workload"}}, RootFS: "prepared-rootfs-start-test",
	}); err != nil {
		t.Fatalf("prepare Container: %v", err)
	}
}

// fixedClock makes event and verification wall facts deterministic without timing assertions.
type fixedClock struct{}

// Now returns one stable diagnostic timestamp for engine tests.
func (fixedClock) Now() time.Time {
	return time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)
}

// manualClock exposes an explicitly advanced wall instant so deadline recovery tests never wait on real time.
type manualClock struct {
	mu  sync.RWMutex
	now time.Time
}

// Now returns the current test-controlled instant without advancing it implicitly.
func (clock *manualClock) Now() time.Time {
	clock.mu.RLock()
	defer clock.mu.RUnlock()
	return clock.now
}

// Set moves the test-controlled wall instant to an explicitly selected recovery point.
func (clock *manualClock) Set(now time.Time) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = now
}

// TestEngineRunsFullLifecycleThroughCheckpointedProviders verifies the public engine path and reverse cleanup order.
func TestEngineRunsFullLifecycleThroughCheckpointedProviders(t *testing.T) {
	host := newFakeHost()
	engine, store := testEngine(t, host)
	ctx := context.Background()
	sandboxRequest := lifecycle.SandboxCreateRequest{
		OperationID: "op-sandbox-create", SandboxID: "sandbox-1",
		Spec: domain.SandboxSpec{Hostname: "sandbox-1", Network: domain.NetworkIntent{Mode: "none"}},
	}
	sandboxResult, err := engine.CreateSandbox(ctx, sandboxRequest)
	if err != nil {
		t.Fatalf("create Sandbox: %v", err)
	}
	if sandboxResult.Sandbox == nil || sandboxResult.Sandbox.Status.Phase != domain.SandboxReady {
		t.Fatalf("Sandbox result = %#v, want Ready", sandboxResult.Sandbox)
	}
	containerRequest := lifecycle.ContainerCreateRequest{
		OperationID: "op-container-create", SandboxID: "sandbox-1", ContainerID: "container-1", AttemptID: "attempt-1",
		Process: domain.ProcessSpec{Argv: []string{"/bin/workload"}}, RootFS: "prepared-rootfs-1",
	}
	containerResult, err := engine.CreateContainer(ctx, containerRequest)
	if err != nil {
		t.Fatalf("create Container: %v", err)
	}
	if containerResult.ContainerAttempt == nil || containerResult.ContainerAttempt.Attempt.Phase != domain.AttemptCreated {
		t.Fatalf("Container result = %#v, want Created", containerResult.ContainerAttempt)
	}
	if containerResult.HostBinding == nil || containerResult.HostBinding.Owner.OperationID != containerRequest.OperationID ||
		containerResult.HostBinding.AttemptID != containerRequest.AttemptID {
		t.Fatalf("Container host binding = %#v, want original create owner", containerResult.HostBinding)
	}
	noopRequest := containerRequest
	noopRequest.OperationID = "op-container-create-noop"
	noopResult, err := engine.CreateContainer(ctx, noopRequest)
	if err != nil || noopResult.Operation.Result != operation.ResultNoop || noopResult.HostBinding == nil || *noopResult.HostBinding != *containerResult.HostBinding {
		t.Fatalf("Container create noop = (%#v,%v), want original host binding", noopResult, err)
	}
	startResult, err := engine.StartContainer(ctx, lifecycle.ContainerActionRequest{OperationID: "op-start", ContainerID: "container-1"})
	if err != nil {
		t.Fatalf("start Container: %v", err)
	}
	if startResult.ContainerAttempt == nil || startResult.ContainerAttempt.Attempt.Phase != domain.AttemptRunning {
		t.Fatalf("start result = %#v, want Running", startResult.ContainerAttempt)
	}
	killResult, err := engine.KillContainer(ctx, lifecycle.KillRequest{
		OperationID: "op-kill", ContainerID: "container-1",
		Policy: domain.TerminationPolicy{Signal: "SIGTERM", EscalationSignal: "SIGKILL"},
	})
	if err != nil {
		t.Fatalf("kill Container: %v", err)
	}
	if killResult.ContainerAttempt == nil || killResult.ContainerAttempt.Attempt.Phase != domain.AttemptStopped {
		t.Fatalf("kill result = %#v, want Stopped", killResult.ContainerAttempt)
	}
	deleteResult, err := engine.DeleteContainer(ctx, lifecycle.ContainerActionRequest{OperationID: "op-delete", ContainerID: "container-1"})
	if err != nil {
		t.Fatalf("delete Container: %v", err)
	}
	if !deleteResult.Removed {
		t.Fatalf("delete result = %#v, want removed", deleteResult)
	}
	if deleteResult.HostBinding == nil || *deleteResult.HostBinding != *containerResult.HostBinding {
		t.Fatalf("delete host binding = %#v, want immutable %#v", deleteResult.HostBinding, containerResult.HostBinding)
	}
	deleteReplay, err := engine.DeleteContainer(ctx, lifecycle.ContainerActionRequest{OperationID: "op-delete", ContainerID: "container-1"})
	if err != nil || deleteReplay.Resolution != operation.ResolutionReplay || deleteReplay.HostBinding == nil || *deleteReplay.HostBinding != *deleteResult.HostBinding {
		t.Fatalf("delete replay = (%#v,%v), want durable removed host binding", deleteReplay, err)
	}
	stopResult, err := engine.StopSandbox(ctx, lifecycle.SandboxActionRequest{OperationID: "op-stop", SandboxID: "sandbox-1"})
	if err != nil {
		t.Fatalf("stop Sandbox: %v", err)
	}
	if stopResult.Sandbox == nil || stopResult.Sandbox.Status.Phase != domain.SandboxStopped {
		t.Fatalf("stop result = %#v, want Stopped", stopResult.Sandbox)
	}
	removeResult, err := engine.RemoveSandbox(ctx, lifecycle.SandboxActionRequest{OperationID: "op-remove", SandboxID: "sandbox-1"})
	if err != nil {
		t.Fatalf("remove Sandbox: %v", err)
	}
	if !removeResult.Removed {
		t.Fatalf("remove result = %#v, want removed", removeResult)
	}
	var eventCount int
	if err := store.View(ctx, func(reader state.Reader) error {
		events, listErr := reader.EventsAfter(0, 0)
		eventCount = len(events)
		return listErr
	}); err != nil {
		t.Fatalf("list events: %v", err)
	}
	if eventCount < 20 {
		t.Fatalf("event count = %d, want checkpointed lifecycle history", eventCount)
	}
}

// TestEnginePassesRetainedSandboxEnvironmentToHostEffects verifies UTS hostname,
// network mode, and ordered DNS come from the immutable stored Sandbox spec at
// their respective namespace and Attempt-rootfs execution boundaries.
func TestEnginePassesRetainedSandboxEnvironmentToHostEffects(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	ctx := context.Background()
	request := lifecycle.SandboxCreateRequest{
		OperationID: "op-sandbox-environment", SandboxID: "sandbox-environment",
		Spec: domain.SandboxSpec{
			Hostname: "retained-host", DNS: []string{"1.1.1.1", "8.8.8.8"},
			Network: domain.NetworkIntent{Mode: "loopback"},
		},
	}
	if _, err := engine.CreateSandbox(ctx, request); err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	request.Spec.Hostname = "caller-mutated-host"
	request.Spec.Network.Mode = "none"
	request.Spec.DNS[0] = "9.9.9.9"
	if _, err := engine.CreateContainer(ctx, lifecycle.ContainerCreateRequest{
		OperationID: "op-container-environment", SandboxID: "sandbox-environment",
		ContainerID: "container-environment", AttemptID: "attempt-environment",
		Process: domain.ProcessSpec{Argv: []string{"/bin/workload"}}, RootFS: "prepared-environment",
	}); err != nil {
		t.Fatalf("CreateContainer() error = %v", err)
	}

	host.mu.Lock()
	namespaceRequests := append([]provider.NamespaceRequest(nil), host.namespaceRequests...)
	rootfsRequests := append([]provider.RootfsRequest(nil), host.rootfsRequests...)
	host.mu.Unlock()
	var utsRequest, networkRequest *provider.NamespaceRequest
	for index := range namespaceRequests {
		request := namespaceRequests[index]
		switch request.Namespace {
		case isolation.NamespaceUTS:
			utsRequest = &request
		case isolation.NamespaceNetwork:
			networkRequest = &request
		}
	}
	if utsRequest == nil || utsRequest.Hostname != "retained-host" {
		t.Fatalf("UTS request = %#v, want retained hostname", utsRequest)
	}
	if networkRequest == nil || networkRequest.NetworkMode != provider.SandboxNetworkLoopback {
		t.Fatalf("network request = %#v, want retained loopback mode", networkRequest)
	}
	if len(rootfsRequests) != 1 || !reflect.DeepEqual(rootfsRequests[0].DNS, []string{"1.1.1.1", "8.8.8.8"}) {
		t.Fatalf("rootfs requests = %#v, want retained ordered DNS", rootfsRequests)
	}
}

// TestEngineRejectsUnsupportedNetworkBeforeHostAcquisition verifies direct
// engine callers cannot reach cgroup, keeper, or namespace effects with a mode
// outside the M3 none/loopback contract even though the broad domain model is additive.
func TestEngineRejectsUnsupportedNetworkBeforeHostAcquisition(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	request := lifecycle.SandboxCreateRequest{
		OperationID: "op-unsupported-network", SandboxID: "sandbox-unsupported-network",
		Spec: domain.SandboxSpec{Network: domain.NetworkIntent{Mode: "bridge"}},
	}
	if _, err := engine.CreateSandbox(context.Background(), request); err == nil {
		t.Fatal("CreateSandbox() accepted a mode outside the M3 provider contract")
	}
	host.mu.Lock()
	created := cloneKindCounts(host.created)
	host.mu.Unlock()
	if len(created) != 0 {
		t.Fatalf("unsupported network mode reached host acquisitions: %v", created)
	}
	if _, err := engine.Coordinator().GetSandbox(context.Background(), request.SandboxID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetSandbox() error = %v, want failed pre-effect intent removed", err)
	}
}

// TestEngineRecoversSideEffectBeforeCheckpoint verifies retry rediscovers one deterministic resource instead of creating a duplicate.
func TestEngineRecoversSideEffectBeforeCheckpoint(t *testing.T) {
	host := newFakeHost()
	host.failAfterEnsureKind = ownership.KindKeeperProcess
	engine, _ := testEngine(t, host)
	request := lifecycle.SandboxCreateRequest{
		OperationID: "op-recover", SandboxID: "sandbox-recover",
		Spec: domain.SandboxSpec{Network: domain.NetworkIntent{Mode: "none"}},
	}
	if _, err := engine.CreateSandbox(context.Background(), request); err == nil {
		t.Fatal("expected injected response loss after keeper creation")
	}
	result, err := engine.CreateSandbox(context.Background(), request)
	if err != nil {
		t.Fatalf("resume Sandbox create: %v", err)
	}
	if result.Sandbox == nil || result.Sandbox.Status.Phase != domain.SandboxReady {
		t.Fatalf("resume result = %#v, want Ready", result.Sandbox)
	}
	host.mu.Lock()
	created := host.created[ownership.KindKeeperProcess]
	host.mu.Unlock()
	if created != 1 {
		t.Fatalf("keeper physical creations = %d, want one", created)
	}
}

// TestStartContainerRecoversGateResponseLoss proves a delivered one-shot release is observed and never issued twice when the provider response is lost.
func TestStartContainerRecoversGateResponseLoss(t *testing.T) {
	host := newFakeHost()
	host.failAfterRelease = true
	engine, _ := testEngine(t, host)
	prepareCreatedContainer(t, engine)
	request := lifecycle.ContainerActionRequest{OperationID: "op-start-response-loss", ContainerID: "container-start-test"}
	first, err := engine.StartContainer(context.Background(), request)
	if err != nil {
		t.Fatalf("StartContainer() after response loss error = %v", err)
	}
	if first.ContainerAttempt == nil || first.ContainerAttempt.Attempt.Phase != domain.AttemptRunning {
		t.Fatalf("StartContainer() result = %#v, want Running", first.ContainerAttempt)
	}
	replay, err := engine.StartContainer(context.Background(), request)
	if err != nil {
		t.Fatalf("StartContainer() replay error = %v", err)
	}
	if !reflect.DeepEqual(first.ContainerAttempt, replay.ContainerAttempt) {
		t.Fatalf("StartContainer() replay = %#v, want exact %#v", replay.ContainerAttempt, first.ContainerAttempt)
	}
	host.mu.Lock()
	releaseCalls := host.releaseCalls
	host.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("ReleaseStartGate() calls = %d, want one", releaseCalls)
	}
}

// TestStartContainerRecordsTerminalBeforeRunning proves a wrapper failure after release becomes a stable failed Start result instead of a permanently active operation.
func TestStartContainerRecordsTerminalBeforeRunning(t *testing.T) {
	host := newFakeHost()
	host.terminalAfterRelease = true
	engine, _ := testEngine(t, host)
	prepareCreatedContainer(t, engine)
	request := lifecycle.ContainerActionRequest{OperationID: "op-start-terminal", ContainerID: "container-start-test"}
	first, err := engine.StartContainer(context.Background(), request)
	if err == nil {
		t.Fatal("StartContainer() terminal error = nil, want stable failed-operation error")
	}
	if first.ContainerAttempt == nil || first.ContainerAttempt.Attempt.Phase != domain.AttemptStopped {
		t.Fatalf("StartContainer() terminal result = %#v, want Stopped", first.ContainerAttempt)
	}
	if first.Operation.Result != operation.ResultFailed || first.ContainerAttempt.Attempt.Outcome.Presence != domain.OutcomeNotApplicable {
		t.Fatalf("StartContainer() terminal operation = %#v outcome = %#v, want failed/not-applicable", first.Operation, first.ContainerAttempt.Attempt.Outcome)
	}
	replay, err := engine.StartContainer(context.Background(), request)
	if err == nil {
		t.Fatal("StartContainer() terminal replay error = nil, want stable failed-operation error")
	}
	if !reflect.DeepEqual(first.ContainerAttempt, replay.ContainerAttempt) || replay.Operation.Result != operation.ResultFailed {
		t.Fatalf("StartContainer() terminal replay = %#v, want exact stopped failure", replay)
	}
	host.mu.Lock()
	releaseCalls := host.releaseCalls
	host.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("ReleaseStartGate() terminal calls = %d, want one", releaseCalls)
	}
}

// TestKillContainerRecoversSignalResponseLoss proves a transport retry reuses the same action key and never delivers the initial signal twice.
func TestKillContainerRecoversSignalResponseLoss(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	prepareCreatedContainer(t, engine)
	if _, err := engine.StartContainer(context.Background(), lifecycle.ContainerActionRequest{
		OperationID: "op-start-before-kill-loss", ContainerID: "container-start-test",
	}); err != nil {
		t.Fatalf("StartContainer() error = %v", err)
	}
	host.mu.Lock()
	host.failAfterSignal = true
	host.mu.Unlock()
	request := lifecycle.KillRequest{
		OperationID: "op-kill-response-loss", ContainerID: "container-start-test",
		Policy: domain.TerminationPolicy{Signal: "SIGTERM", EscalationSignal: "SIGKILL"},
	}
	result, err := engine.KillContainer(context.Background(), request)
	if err != nil {
		t.Fatalf("KillContainer(response loss recovery) error = %v", err)
	}
	if result.ContainerAttempt == nil || result.ContainerAttempt.Attempt.Phase != domain.AttemptStopped {
		t.Fatalf("KillContainer(response loss recovery) = %#v, want Stopped", result.ContainerAttempt)
	}
	if _, err := engine.KillContainer(context.Background(), request); err != nil {
		t.Fatalf("KillContainer(replay) error = %v", err)
	}
	host.mu.Lock()
	deliveries := host.signalDeliveries
	host.mu.Unlock()
	if deliveries != 1 {
		t.Fatalf("physical signal deliveries = %d, want one", deliveries)
	}
}

// TestKillDeadlineSurvivesCancellationAndDaemonRestart verifies a caller timeout cannot reset grace and a restarted engine escalates immediately after the persisted absolute deadline.
func TestKillDeadlineSurvivesCancellationAndDaemonRestart(t *testing.T) {
	host := newFakeHost()
	deliveredAt := fixedClock{}.Now()
	clock := &manualClock{now: deliveredAt}
	store := state.NewMemoryStore()
	engine, err := NewWithClock(store, testProviders(t, host), clock)
	if err != nil {
		t.Fatalf("NewWithClock() error = %v", err)
	}
	prepareCreatedContainer(t, engine)
	if _, err := engine.StartContainer(context.Background(), lifecycle.ContainerActionRequest{
		OperationID: "op-start-before-deadline", ContainerID: "container-start-test",
	}); err != nil {
		t.Fatalf("StartContainer() error = %v", err)
	}
	request := lifecycle.KillRequest{
		OperationID: "op-kill-deadline", ContainerID: "container-start-test",
		Policy: domain.TerminationPolicy{Signal: "SIGTERM", GracePeriod: 2 * time.Hour, EscalationSignal: "SIGKILL"},
	}
	requestContext, cancel := context.WithCancel(context.Background())
	host.mu.Lock()
	host.keepRunningAfterInitial = true
	host.signalDeliveredAt = deliveredAt
	host.cancelNextObservation = cancel
	host.mu.Unlock()
	if _, err := engine.KillContainer(requestContext, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("KillContainer(canceled wait) error = %v, want context.Canceled", err)
	}
	progress, err := engine.Coordinator().GetOperationProgress(context.Background(), request.OperationID)
	if err != nil {
		t.Fatalf("GetOperationProgress() error = %v", err)
	}
	wantDeadline := deliveredAt.Add(request.Policy.GracePeriod)
	if progress.Operation.Stage != operation.StageSignalProcess || progress.KillEscalationDeadline == nil ||
		!progress.KillEscalationDeadline.Equal(wantDeadline) {
		t.Fatalf("durable Kill progress = %#v, want deadline %s", progress, wantDeadline)
	}
	host.mu.Lock()
	deliveriesBeforeRestart := host.signalDeliveries
	host.mu.Unlock()
	if deliveriesBeforeRestart != 1 {
		t.Fatalf("signal deliveries before restart = %d, want initial only", deliveriesBeforeRestart)
	}

	clock.Set(wantDeadline.Add(time.Nanosecond))
	restarted, err := NewWithClock(store, testProviders(t, host), clock)
	if err != nil {
		t.Fatalf("NewWithClock(restarted) error = %v", err)
	}
	if _, err := restarted.Discover(context.Background()); err != nil {
		t.Fatalf("Discover(restarted) error = %v", err)
	}
	result, err := restarted.KillContainer(context.Background(), request)
	if err != nil {
		t.Fatalf("KillContainer(restarted after deadline) error = %v", err)
	}
	if result.ContainerAttempt == nil || result.ContainerAttempt.Attempt.Phase != domain.AttemptStopped {
		t.Fatalf("KillContainer(restarted) result = %#v, want Stopped", result.ContainerAttempt)
	}
	host.mu.Lock()
	deliveriesAfterRestart := host.signalDeliveries
	host.mu.Unlock()
	if deliveriesAfterRestart != 2 {
		t.Fatalf("signal deliveries after restart = %d, want one initial plus one escalation", deliveriesAfterRestart)
	}
}

// InspectCgroupCapabilities returns every requirement from the pure fake provider.
func (host *fakeHost) InspectCgroupCapabilities(_ context.Context, requirements provider.CgroupRequirements) (provider.CgroupCapabilities, error) {
	return provider.CgroupCapabilities{UnifiedV2: true, Delegated: true, Controllers: append([]provider.CgroupController(nil), requirements.Controllers...)}, nil
}

// InspectIsolationCapabilities returns every requested namespace and isolation feature.
func (host *fakeHost) InspectIsolationCapabilities(_ context.Context, requirements provider.IsolationRequirements) (provider.IsolationCapabilities, error) {
	return provider.IsolationCapabilities{Rootful: true, PIDFD: true, PivotRoot: true, StartGate: true, Streams: true, Namespaces: append([]isolation.NamespaceType(nil), requirements.Namespaces...)}, nil
}

// EnsureSandboxCgroup creates or rediscovers the deterministic fake Sandbox parent.
func (host *fakeHost) EnsureSandboxCgroup(_ context.Context, request provider.SandboxCgroupRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	return host.ensure(request.Owner, ownership.KindSandboxCgroup)
}

// EnsureKeeperCgroup creates or rediscovers the deterministic fake keeper leaf.
func (host *fakeHost) EnsureKeeperCgroup(_ context.Context, request provider.KeeperCgroupRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	return host.ensure(request.Owner, ownership.KindKeeperCgroup)
}

// EnsureAttemptCgroup creates or rediscovers the deterministic fake Attempt leaf.
func (host *fakeHost) EnsureAttemptCgroup(_ context.Context, request provider.AttemptCgroupRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	return host.ensure(request.Owner, ownership.KindAttemptCgroup)
}

// EnsureKeeperProcess creates or rediscovers the stable fake keeper process.
func (host *fakeHost) EnsureKeeperProcess(_ context.Context, request provider.KeeperProcessRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	return host.ensure(request.Owner, ownership.KindKeeperProcess)
}

// EnsureInitProcess creates or rediscovers the stable fake init wrapper.
func (host *fakeHost) EnsureInitProcess(_ context.Context, request provider.InitProcessRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	return host.ensure(request.Owner, ownership.KindInitProcess)
}

// EnsureNamespace creates or rediscovers one typed fake namespace receipt.
func (host *fakeHost) EnsureNamespace(_ context.Context, request provider.NamespaceRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	request.Process = request.Process.Clone()
	host.mu.Lock()
	host.namespaceRequests = append(host.namespaceRequests, request)
	host.mu.Unlock()
	kind, err := request.ReceiptKind()
	if err != nil {
		return ownership.Receipt{}, err
	}
	return host.ensure(request.Owner, kind)
}

// EnsureRootfs creates or rediscovers one fake prepared rootfs view.
func (host *fakeHost) EnsureRootfs(_ context.Context, request provider.RootfsRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	request.Process = request.Process.Clone()
	request.PID = request.PID.Clone()
	request.Mount = request.Mount.Clone()
	request.DNS = append([]string(nil), request.DNS...)
	host.mu.Lock()
	host.rootfsRequests = append(host.rootfsRequests, request)
	host.mu.Unlock()
	return host.ensure(request.Owner, ownership.KindRootfsMount)
}

// EnsureStartGate creates or rediscovers one fake closed start gate.
func (host *fakeHost) EnsureStartGate(_ context.Context, request provider.AttemptResourceRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	return host.ensure(request.Owner, ownership.KindStartGate)
}

// EnsureStreams creates or rediscovers fake persistent stdout and stderr references.
func (host *fakeHost) EnsureStreams(_ context.Context, request provider.AttemptResourceRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	receipt, err := host.ensure(request.Owner, ownership.KindStreams)
	if err == nil {
		receipt.Attributes["stdout"] = "stdout-log"
		receipt.Attributes["stderr"] = "stderr-log"
		host.mu.Lock()
		host.receipts[ownership.KindStreams] = receipt.Clone()
		host.mu.Unlock()
	}
	return receipt, err
}

// ensure implements deterministic idempotent acquisition and one response-loss injection point.
func (host *fakeHost) ensure(owner ownership.OwnerKey, kind ownership.Kind) (ownership.Receipt, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	if host.failBeforeEnsureKind == kind && !host.failedOnce {
		host.failedOnce = true
		return ownership.Receipt{}, provider.MarkNoEffect(errors.New("injected provider failure before acquisition"))
	}
	if receipt, exists := host.receipts[kind]; exists && host.present[kind] {
		return receipt.Clone(), nil
	}
	providerName := ownership.ProviderLinux
	if kind == ownership.KindSandboxCgroup || kind == ownership.KindKeeperCgroup || kind == ownership.KindAttemptCgroup {
		providerName = ownership.ProviderCgroupV2
	}
	evidence, err := ownership.EvidenceDigest(struct {
		Owner ownership.OwnerKey `json:"owner"`
		Kind  ownership.Kind     `json:"kind"`
	}{owner, kind})
	if err != nil {
		return ownership.Receipt{}, err
	}
	receipt := ownership.Receipt{
		SchemaVersion: ownership.SchemaVersion, Provider: providerName, Kind: kind,
		LocalID: string(kind), Owner: owner, EvidenceSHA256: evidence, Attributes: map[string]string{},
	}
	host.present[kind] = true
	host.receipts[kind] = receipt.Clone()
	host.created[kind]++
	host.callOrder = append(host.callOrder, kind)
	if host.failAfterEnsureKind == kind && !host.failedOnce {
		host.failedOnce = true
		return ownership.Receipt{}, errors.New("injected provider response loss")
	}
	if host.rollbackRequiredKind == kind && !host.failedOnce {
		host.failedOnce = true
		return ownership.Receipt{}, provider.MarkRollbackRequired(errors.New("injected rollback-contained provider failure"))
	}
	return receipt, nil
}

// InspectSandboxCgroup reports owner-verified fake presence for the Sandbox parent.
func (host *fakeHost) InspectSandboxCgroup(_ context.Context, request provider.OwnedReceiptRequest) (provider.ResourceObservation, error) {
	return host.inspect(request, ownership.KindSandboxCgroup)
}

// InspectKeeperCgroup reports owner-verified fake presence for the keeper leaf.
func (host *fakeHost) InspectKeeperCgroup(_ context.Context, request provider.OwnedReceiptRequest) (provider.ResourceObservation, error) {
	return host.inspect(request, ownership.KindKeeperCgroup)
}

// InspectAttemptCgroup reports owner-verified fake presence for the Attempt leaf.
func (host *fakeHost) InspectAttemptCgroup(_ context.Context, request provider.OwnedReceiptRequest) (provider.ResourceObservation, error) {
	return host.inspect(request, ownership.KindAttemptCgroup)
}

// InspectProcess reports owner-verified fake keeper or init presence.
func (host *fakeHost) InspectProcess(_ context.Context, request provider.OwnedReceiptRequest) (provider.ResourceObservation, error) {
	return host.inspect(request, request.Receipt.Kind)
}

// InspectNamespace reports owner-verified fake namespace presence.
func (host *fakeHost) InspectNamespace(_ context.Context, request provider.OwnedReceiptRequest) (provider.ResourceObservation, error) {
	return host.inspect(request, request.Receipt.Kind)
}

// InspectRootfs reports owner-verified fake rootfs presence.
func (host *fakeHost) InspectRootfs(_ context.Context, request provider.OwnedReceiptRequest) (provider.ResourceObservation, error) {
	return host.inspect(request, ownership.KindRootfsMount)
}

// InspectStartGate reports owner-verified fake gate presence.
func (host *fakeHost) InspectStartGate(_ context.Context, request provider.OwnedReceiptRequest) (provider.ResourceObservation, error) {
	return host.inspect(request, ownership.KindStartGate)
}

// InspectStreams reports owner-verified fake stream presence.
func (host *fakeHost) InspectStreams(_ context.Context, request provider.OwnedReceiptRequest) (provider.ResourceObservation, error) {
	return host.inspect(request, ownership.KindStreams)
}

// inspect validates exact receipt authority before returning deterministic presence.
func (host *fakeHost) inspect(request provider.OwnedReceiptRequest, kind ownership.Kind) (provider.ResourceObservation, error) {
	if request.Receipt.Kind != kind || request.Owner != request.Receipt.Owner {
		return provider.ResourceObservation{}, errors.New("fake inspect owner or kind mismatch")
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if host.failInspectKind == kind {
		return provider.ResourceObservation{}, errors.New("injected read-only discovery failure")
	}
	if host.inspectUnavailableKind == kind {
		return provider.ResourceObservation{}, provider.MarkObservationUnavailable(errors.New("injected temporary observation outage"))
	}
	if host.present[kind] {
		return provider.ResourceObservation{Presence: provider.PresencePresent, Verified: true, EvidenceSHA256: request.Receipt.EvidenceSHA256}, nil
	}
	return provider.ResourceObservation{Presence: provider.PresenceAbsent, Verified: true}, nil
}

// RemoveSandboxCgroup proves the fake Sandbox parent absent.
func (host *fakeHost) RemoveSandboxCgroup(_ context.Context, request provider.OwnedReceiptRequest) (provider.CleanupObservation, error) {
	return host.remove(request, ownership.KindSandboxCgroup)
}

// RemoveKeeperCgroup proves the fake keeper leaf absent.
func (host *fakeHost) RemoveKeeperCgroup(_ context.Context, request provider.OwnedReceiptRequest) (provider.CleanupObservation, error) {
	return host.remove(request, ownership.KindKeeperCgroup)
}

// RemoveAttemptCgroup proves the fake Attempt leaf absent.
func (host *fakeHost) RemoveAttemptCgroup(_ context.Context, request provider.OwnedReceiptRequest) (provider.CleanupObservation, error) {
	return host.remove(request, ownership.KindAttemptCgroup)
}

// RemoveProcess proves the selected fake keeper or init process absent.
func (host *fakeHost) RemoveProcess(_ context.Context, request provider.OwnedReceiptRequest) (provider.CleanupObservation, error) {
	return host.remove(request, request.Receipt.Kind)
}

// RemoveNamespace proves the selected fake namespace absent.
func (host *fakeHost) RemoveNamespace(_ context.Context, request provider.OwnedReceiptRequest) (provider.CleanupObservation, error) {
	return host.remove(request, request.Receipt.Kind)
}

// RemoveRootfs proves the fake rootfs absent.
func (host *fakeHost) RemoveRootfs(_ context.Context, request provider.OwnedReceiptRequest) (provider.CleanupObservation, error) {
	return host.remove(request, ownership.KindRootfsMount)
}

// RemoveStartGate proves the fake start gate absent.
func (host *fakeHost) RemoveStartGate(_ context.Context, request provider.OwnedReceiptRequest) (provider.CleanupObservation, error) {
	return host.remove(request, ownership.KindStartGate)
}

// RemoveStreams proves the fake stream endpoints absent.
func (host *fakeHost) RemoveStreams(_ context.Context, request provider.OwnedReceiptRequest) (provider.CleanupObservation, error) {
	return host.remove(request, ownership.KindStreams)
}

// remove validates exact receipt authority and implements idempotent fake cleanup.
func (host *fakeHost) remove(request provider.OwnedReceiptRequest, kind ownership.Kind) (provider.CleanupObservation, error) {
	if request.Receipt.Kind != kind || request.Owner != request.Receipt.Owner {
		return provider.CleanupObservation{}, errors.New("fake cleanup owner or kind mismatch")
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if host.failCleanupKind == kind && !host.cleanupFailedOnce {
		host.cleanupFailedOnce = true
		return provider.CleanupObservation{}, errors.New("injected rollback cleanup failure")
	}
	disposition := provider.CleanupAlreadyAbsent
	if host.present[kind] {
		disposition = provider.CleanupRemoved
		host.present[kind] = false
		if kind == ownership.KindKeeperProcess {
			host.present[ownership.KindUTSNamespace] = false
			host.present[ownership.KindIPCNamespace] = false
			host.present[ownership.KindNetworkNamespace] = false
		}
		if kind == ownership.KindInitProcess {
			host.present[ownership.KindPIDNamespace] = false
			host.present[ownership.KindMountNamespace] = false
			host.present[ownership.KindRootfsMount] = false
		}
	}
	return provider.CleanupObservation{Disposition: disposition, After: provider.ResourceObservation{Presence: provider.PresenceAbsent, Verified: true}}, nil
}

// AttachAttemptProcess returns canonical evidence for the exact fake cgroup and init receipts.
func (host *fakeHost) AttachAttemptProcess(_ context.Context, request provider.AttachProcessRequest) (provider.AttachmentObservation, error) {
	return provider.NewAttachmentObservation(request)
}

// SnapshotAttemptOOM returns a scoped zero-delta fake memory event snapshot.
func (host *fakeHost) SnapshotAttemptOOM(_ context.Context, request provider.OwnedReceiptRequest) (provider.OOMSnapshot, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	return provider.NewOOMSnapshot(request, host.oom, host.oomKill, host.oomGroupKill)
}

// ReleaseStartGate moves the fake wrapper from prepared to running after validating exact dependencies.
func (host *fakeHost) ReleaseStartGate(_ context.Context, request provider.ReleaseGateRequest) (provider.ResourceObservation, error) {
	if err := request.Validate(); err != nil {
		return provider.ResourceObservation{}, err
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	host.releaseCalls++
	if host.terminalAfterRelease {
		host.attempt = AttemptObservation{Terminal: true, Outcome: domain.NotApplicableOutcome(), Evidence: "terminal-before-running-evidence"}
	} else {
		host.attempt = AttemptObservation{Running: true, Evidence: "running-evidence"}
	}
	if host.failAfterRelease && !host.releaseFailedOnce {
		host.releaseFailedOnce = true
		return provider.ResourceObservation{}, errors.New("injected start-gate response loss")
	}
	return provider.ResourceObservation{Presence: provider.PresencePresent, Verified: true, EvidenceSHA256: request.Gate.EvidenceSHA256}, nil
}

// SignalVerified exactly replays keyed fake delivery evidence and can leave the initial signal non-terminal for grace-deadline tests.
func (host *fakeHost) SignalVerified(_ context.Context, request provider.SignalRequest) (provider.SignalObservation, error) {
	if err := request.Validate(); err != nil {
		return provider.SignalObservation{}, err
	}
	key := fmt.Sprintf("%s/%s/%s", request.ActionOperationID, request.Step, request.Signal)
	host.mu.Lock()
	if response, found := host.signalResponses[key]; found {
		host.mu.Unlock()
		return response, nil
	}
	started := time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)
	finished := started.Add(time.Second)
	if request.Step != provider.SignalStepInitial || !host.keepRunningAfterInitial {
		host.attempt = AttemptObservation{
			Terminal: true, Evidence: "terminal-evidence",
			Outcome: domain.SignalOutcome(string(request.Signal), domain.EvidenceFalse, started, finished, time.Second),
		}
	}
	deliveredAt := host.signalDeliveredAt
	if deliveredAt.IsZero() {
		deliveredAt = fixedClock{}.Now()
	}
	response := provider.SignalObservation{
		Signal: request.Signal, IdentityEvidenceSHA256: request.Process.EvidenceSHA256,
		Delivered: true, DeliveredAt: deliveredAt,
	}
	host.signalResponses[key] = response
	host.signalDeliveries++
	if host.signalSteps != nil {
		host.signalSteps <- request.Step
	}
	if host.signalNotRunning {
		host.mu.Unlock()
		return provider.SignalObservation{}, provider.ErrProcessNotRunning
	}
	if host.failAfterSignal && !host.signalFailedOnce {
		host.signalFailedOnce = true
		host.mu.Unlock()
		return provider.SignalObservation{}, errors.New("injected verified-signal response loss")
	}
	host.mu.Unlock()
	return response, nil
}

// TestKillContainerRecordsNaturalExitRace verifies a signal-side not-running
// result is reconciled from the wrapper's already durable terminal outcome.
func TestKillContainerRecordsNaturalExitRace(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	prepareCreatedContainer(t, engine)
	if _, err := engine.StartContainer(context.Background(), lifecycle.ContainerActionRequest{
		OperationID: "op-start-natural-race", ContainerID: "container-start-test",
	}); err != nil {
		t.Fatalf("StartContainer() error = %v", err)
	}
	host.signalNotRunning = true
	result, err := engine.KillContainer(context.Background(), lifecycle.KillRequest{
		OperationID: "op-kill-natural-race", ContainerID: "container-start-test",
		Policy: domain.TerminationPolicy{Signal: "SIGTERM", GracePeriod: time.Second, EscalationSignal: "SIGKILL"},
	})
	if err != nil {
		t.Fatalf("KillContainer() error = %v", err)
	}
	if result.ContainerAttempt == nil || result.ContainerAttempt.Attempt.Phase != domain.AttemptStopped {
		t.Fatalf("KillContainer() result = %#v, want Stopped", result.ContainerAttempt)
	}
}

// TestRecordTerminalAttributesOOMFromDurableBaseline verifies a restart-safe
// cgroup delta augments captured wait facts without inferring OOM from SIGKILL or exit code.
func TestRecordTerminalAttributesOOMFromDurableBaseline(t *testing.T) {
	host := newFakeHost()
	engine, store := testEngine(t, host)
	prepareCreatedContainer(t, engine)
	if _, err := engine.StartContainer(context.Background(), lifecycle.ContainerActionRequest{
		OperationID: "op-start-before-oom", ContainerID: "container-start-test",
	}); err != nil {
		t.Fatalf("StartContainer() error = %v", err)
	}
	started := fixedClock{}.Now()
	finished := started.Add(time.Second)
	host.mu.Lock()
	host.oomKill = 1
	host.attempt = AttemptObservation{
		Terminal: true, Evidence: "terminal-oom-evidence",
		Outcome: domain.ExitOutcome(137, domain.EvidenceUnknown, started, finished, time.Second),
	}
	host.mu.Unlock()
	restarted, err := NewWithClock(store, testProviders(t, host), fixedClock{})
	if err != nil {
		t.Fatalf("NewWithClock(restart) error = %v", err)
	}
	inventory, err := restarted.containerInventory(context.Background(), "container-start-test")
	if err != nil {
		t.Fatalf("containerInventory() error = %v", err)
	}
	cgroup, err := receiptByKind(inventory, ownership.KindAttemptCgroup)
	if err != nil {
		t.Fatalf("receiptByKind(cgroup) error = %v", err)
	}
	host.mu.Lock()
	rawObservation := host.attempt
	host.mu.Unlock()
	attributed, err := restarted.attributeTerminalOOM(context.Background(), cgroup, rawObservation)
	if err != nil || attributed.Evidence == rawObservation.Evidence {
		t.Fatalf("attributeTerminalOOM() = (%#v, %v), want newly bound evidence", attributed, err)
	}
	result, err := restarted.RecordTerminal(context.Background(), "op-record-oom", "container-start-test")
	if err != nil {
		t.Fatalf("RecordTerminal() error = %v", err)
	}
	if result.ContainerAttempt == nil || result.ContainerAttempt.Attempt.Outcome.ExitCode == nil ||
		*result.ContainerAttempt.Attempt.Outcome.ExitCode != 137 || result.ContainerAttempt.Attempt.Outcome.OOM != domain.EvidenceTrue {
		t.Fatalf("RecordTerminal() outcome = %#v", result.ContainerAttempt)
	}
}

// ObserveAttempt returns the current fake wrapper/workload state without mutating it.
func (host *fakeHost) ObserveAttempt(_ context.Context, request provider.OwnedReceiptRequest) (AttemptObservation, error) {
	if request.Receipt.Kind != ownership.KindInitProcess || request.Owner != request.Receipt.Owner {
		return AttemptObservation{}, errors.New("fake supervisor owner mismatch")
	}
	host.mu.Lock()
	if host.observeErr != nil {
		err := host.observeErr
		host.mu.Unlock()
		return AttemptObservation{}, err
	}
	if host.observeUnavailable {
		host.mu.Unlock()
		return AttemptObservation{}, provider.MarkObservationUnavailable(errors.New("injected temporary supervisor outage"))
	}
	observation := host.attempt
	if len(host.attemptSequence) != 0 {
		observation = host.attemptSequence[0]
		host.attemptSequence = host.attemptSequence[1:]
		host.attempt = observation
	}
	cancel := host.cancelNextObservation
	entered := host.observeEntered
	release := host.observeRelease
	host.cancelNextObservation = nil
	host.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if release != nil {
		<-release
	}
	if cancel != nil {
		cancel()
	}
	return observation, nil
}

// TestFakeHostCallOrderDocumentsCanonicalAcquisition verifies dependency order
// used by recovery assertions and the historical empty network mode's none canonicalization.
func TestFakeHostCallOrderDocumentsCanonicalAcquisition(t *testing.T) {
	host := newFakeHost()
	engine, _ := testEngine(t, host)
	_, err := engine.CreateSandbox(context.Background(), lifecycle.SandboxCreateRequest{
		OperationID: "op-order", SandboxID: "sandbox-order", Spec: domain.SandboxSpec{},
	})
	if err != nil {
		t.Fatalf("create Sandbox: %v", err)
	}
	host.mu.Lock()
	got := append([]ownership.Kind(nil), host.callOrder...)
	namespaceRequests := append([]provider.NamespaceRequest(nil), host.namespaceRequests...)
	host.mu.Unlock()
	want := []ownership.Kind{
		ownership.KindSandboxCgroup, ownership.KindKeeperCgroup, ownership.KindKeeperProcess,
		ownership.KindUTSNamespace, ownership.KindIPCNamespace, ownership.KindNetworkNamespace,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("acquisition order = %v, want %v", got, want)
	}
	if len(namespaceRequests) != 3 || namespaceRequests[2].NetworkMode != provider.SandboxNetworkNone {
		t.Fatalf("namespace requests = %#v, want canonical network none", namespaceRequests)
	}
}

// String formats a fake host for debugging without exposing mutable maps to test failures.
func (host *fakeHost) String() string {
	host.mu.Lock()
	defer host.mu.Unlock()
	return fmt.Sprintf("fakeHost{present:%v, created:%v}", host.present, host.created)
}
