package engine_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	v1 "mydocker/api/runtime/v1"
	"mydocker/internal/daemon"
	"mydocker/internal/engine"
	"mydocker/internal/operation"
	"mydocker/internal/server"
	"mydocker/internal/state"
	"mydocker/pkg/client"
)

const integrationRequestTimeout = 10 * time.Second

// v1LifecycleStack owns one real persistence, adapter, UDS, and client stack;
// only its host-provider boundary is replaced by IntegrationFakeHost.
type v1LifecycleStack struct {
	api       *client.Client
	server    *server.Server
	store     *state.FileStore
	closeOnce sync.Once
	closeErr  error
}

// Close drains transport before releasing the exclusive FileStore lease.
func (stack *v1LifecycleStack) Close() error {
	if stack == nil {
		return nil
	}
	stack.closeOnce.Do(func() {
		if stack.api != nil {
			stack.api.CloseIdleConnections()
		}
		var serverErr error
		if stack.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), integrationRequestTimeout)
			serverErr = stack.server.Shutdown(ctx)
			cancel()
		}
		var storeErr error
		if stack.store != nil {
			storeErr = stack.store.Close()
		}
		stack.closeErr = errors.Join(serverErr, storeErr)
	})
	return stack.closeErr
}

// TestV1UDSLifecycleSurvivesFileStoreRestart exercises the public protocol and
// durable lifecycle as one non-privileged stack. It deliberately uses opaque
// executable/rootfs values: the injected provider never executes or mounts them.
func TestV1UDSLifecycleSurvivesFileStoreRestart(t *testing.T) {
	paths := newV1LifecyclePaths(t)
	host := engine.NewIntegrationFakeHost()
	ctx := context.Background()

	first := startV1LifecycleStack(t, paths, host)
	sandboxRequest := v1.CreateSandboxRequest{
		SandboxID: "sandbox-stack-one",
		Spec: v1.SandboxSpec{
			Hostname: "sandbox-stack-one", DNS: []string{"1.1.1.1"},
			Network: v1.NetworkIntent{Mode: "none"}, Labels: map[string]string{"suite": "v1-uds"},
		},
	}
	containerRequest := v1.CreateContainerRequest{
		ContainerID: "container-stack-one", AttemptID: "attempt-stack-one",
		Process: v1.ProcessSpec{Argv: []string{"/bin/not-executed-by-fake-provider"}},
		RootFS:  "prepared-rootfs-stack-one",
	}
	if response, err := first.api.CreateSandbox(ctx, "op-stack-create-sandbox", sandboxRequest); err != nil || response.Sandbox.Status.Phase != "ready" {
		t.Fatalf("CreateSandbox() = (%#v, %v), want ready", response, err)
	}
	if response, err := first.api.CreateContainer(ctx, "op-stack-create-container", sandboxRequest.SandboxID, containerRequest); err != nil || response.Container.Status.Phase != "created" {
		t.Fatalf("CreateContainer() = (%#v, %v), want created", response, err)
	}
	if response, err := first.api.StartContainer(ctx, "op-stack-start-container", containerRequest.ContainerID); err != nil || response.Container.Status.Phase != "running" {
		t.Fatalf("StartContainer() = (%#v, %v), want running", response, err)
	}
	killPolicy := v1.TerminationPolicy{Signal: "SIGTERM", EscalationSignal: "SIGKILL"}
	if response, err := first.api.KillContainer(ctx, "op-stack-kill-container", containerRequest.ContainerID, killPolicy); err != nil || response.Container.Status.Phase != "stopped" {
		t.Fatalf("KillContainer() = (%#v, %v), want stopped", response, err)
	}
	beforeRestart := readAllLifecycleEvents(t, first.api)
	physicalBeforeRestart := engine.SnapshotIntegrationHost(host)
	if physicalBeforeRestart != (engine.IntegrationHostSnapshot{Created: 13, Present: 13, GateReleases: 1, SignalDeliveries: 1}) {
		t.Fatalf("provider snapshot before restart = %#v, want one complete fake acquisition/start/kill", physicalBeforeRestart)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first lifecycle stack: %v", err)
	}

	second := startV1LifecycleStack(t, paths, host)
	if got := readAllLifecycleEvents(t, second.api); !reflect.DeepEqual(got, beforeRestart) {
		t.Fatalf("events changed during FileStore reopen and reconciliation:\n got %#v\nwant %#v", got, beforeRestart)
	}
	assertSandboxReplay(t, second.api, sandboxRequest)
	assertContainerReplays(t, second.api, sandboxRequest.SandboxID, containerRequest, killPolicy)
	if got := readAllLifecycleEvents(t, second.api); !reflect.DeepEqual(got, beforeRestart) {
		t.Fatalf("exact operation replay appended events:\n got %#v\nwant %#v", got, beforeRestart)
	}
	if got := engine.SnapshotIntegrationHost(host); got != physicalBeforeRestart {
		t.Fatalf("exact operation replay changed provider effects: got %#v want %#v", got, physicalBeforeRestart)
	}

	deleteContainer, err := second.api.DeleteContainer(ctx, "op-stack-delete-container", containerRequest.ContainerID)
	if err != nil || deleteContainer.Operation.Result != string(operation.ResultSucceeded) {
		t.Fatalf("DeleteContainer() = (%#v, %v), want succeeded", deleteContainer, err)
	}
	if response, err := second.api.StopSandbox(ctx, "op-stack-stop-sandbox", sandboxRequest.SandboxID); err != nil || response.Sandbox.Status.Phase != "stopped" {
		t.Fatalf("StopSandbox() = (%#v, %v), want stopped", response, err)
	}
	deleteSandbox, err := second.api.DeleteSandbox(ctx, "op-stack-delete-sandbox", sandboxRequest.SandboxID)
	if err != nil || deleteSandbox.Operation.Result != string(operation.ResultSucceeded) {
		t.Fatalf("DeleteSandbox() = (%#v, %v), want succeeded", deleteSandbox, err)
	}
	finalEvents := readAllLifecycleEvents(t, second.api)
	assertOwnedCompleteEvents(t, finalEvents, sandboxRequest.SandboxID, containerRequest.ContainerID, containerRequest.AttemptID)

	beforeDeleteReplay := append([]v1.Event(nil), finalEvents...)
	if replay, err := second.api.DeleteContainer(ctx, "op-stack-delete-container", containerRequest.ContainerID); err != nil || !reflect.DeepEqual(replay.Operation, deleteContainer.Operation) {
		t.Fatalf("DeleteContainer(replay) = (%#v, %v), want exact %#v", replay, err, deleteContainer)
	}
	if replay, err := second.api.DeleteSandbox(ctx, "op-stack-delete-sandbox", sandboxRequest.SandboxID); err != nil || !reflect.DeepEqual(replay.Operation, deleteSandbox.Operation) {
		t.Fatalf("DeleteSandbox(replay) = (%#v, %v), want exact %#v", replay, err, deleteSandbox)
	}
	if got := readAllLifecycleEvents(t, second.api); !reflect.DeepEqual(got, beforeDeleteReplay) {
		t.Fatalf("delete replay appended events: got %d want %d", len(got), len(beforeDeleteReplay))
	}
	if _, err := second.api.GetContainer(ctx, containerRequest.ContainerID); client.CodeOf(err) != v1.CodeNotFound {
		t.Fatalf("GetContainer(after delete) error = %v, want not_found", err)
	}
	if _, err := second.api.GetSandbox(ctx, sandboxRequest.SandboxID); client.CodeOf(err) != v1.CodeNotFound {
		t.Fatalf("GetSandbox(after delete) error = %v, want not_found", err)
	}
}

// v1LifecyclePaths are private non-privileged filesystem locations for one test stack.
type v1LifecyclePaths struct {
	statePath   string
	runtimeRoot string
	socketPath  string
}

// newV1LifecyclePaths creates owner-only paths below the repository so strict
// FileStore ancestor validation never relies on a world-writable temporary root.
func newV1LifecyclePaths(t *testing.T) v1LifecyclePaths {
	t.Helper()
	directory, err := os.MkdirTemp(".", ".v1-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		_ = os.RemoveAll(directory)
		t.Fatalf("Abs() error = %v", err)
	}
	for _, child := range []string{"t", "r", "s"} {
		if err := os.Mkdir(filepath.Join(absolute, child), 0o700); err != nil {
			_ = os.RemoveAll(absolute)
			t.Fatalf("Mkdir(%s) error = %v", child, err)
		}
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(absolute); err != nil {
			t.Errorf("RemoveAll(test paths) error = %v", err)
		}
	})
	return v1LifecyclePaths{
		statePath:   filepath.Join(absolute, "t", "state.json"),
		runtimeRoot: filepath.Join(absolute, "r"),
		socketPath:  filepath.Join(absolute, "s", "d.sock"),
	}
}

// startV1LifecycleStack opens production persistence and transport around the injected fake host.
func startV1LifecycleStack(t *testing.T, paths v1LifecyclePaths, host *engine.IntegrationFakeHost) *v1LifecycleStack {
	t.Helper()
	store, err := state.NewFileStore(paths.statePath)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	stack := &v1LifecycleStack{store: store}
	t.Cleanup(func() {
		if err := stack.Close(); err != nil {
			t.Errorf("close lifecycle stack: %v", err)
		}
	})
	runtime, err := engine.New(store, engine.IntegrationProviders(t, host))
	if err != nil {
		t.Fatalf("engine.New() error = %v", err)
	}
	if _, err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatalf("Engine.Reconcile() error = %v", err)
	}
	logs, err := daemon.NewRuntimeLogRegistry(paths.runtimeRoot)
	if err != nil {
		t.Fatalf("NewRuntimeLogRegistry() error = %v", err)
	}
	service, err := daemon.NewService(runtime, logs)
	if err != nil {
		t.Fatalf("daemon.NewService() error = %v", err)
	}
	stack.server, err = server.New(server.Config{
		SocketPath: paths.socketPath, HandlerTimeout: integrationRequestTimeout,
	}, service)
	if err != nil {
		t.Fatalf("server.New() error = %v", err)
	}
	if err := stack.server.Start(); err != nil {
		t.Fatalf("Server.Start() error = %v", err)
	}
	stack.api, err = client.New(client.Config{
		SocketPath: paths.socketPath, Timeout: integrationRequestTimeout,
		DialTimeout: time.Second, TransportRetries: 1,
	})
	if err != nil {
		t.Fatalf("client.New() error = %v", err)
	}
	return stack
}

// assertSandboxReplay verifies the original public result survived FileStore close/reopen.
func assertSandboxReplay(t *testing.T, api *client.Client, request v1.CreateSandboxRequest) {
	t.Helper()
	response, err := api.CreateSandbox(context.Background(), "op-stack-create-sandbox", request)
	if err != nil || response.Operation == nil || response.Operation.Result != string(operation.ResultSucceeded) || response.Sandbox.Status.Phase != "ready" {
		t.Fatalf("CreateSandbox(replay) = (%#v, %v), want original ready success", response, err)
	}
}

// assertContainerReplays verifies create, start, and kill responses are durable history without provider redelivery.
func assertContainerReplays(t *testing.T, api *client.Client, sandboxID string, request v1.CreateContainerRequest, policy v1.TerminationPolicy) {
	t.Helper()
	if response, err := api.CreateContainer(context.Background(), "op-stack-create-container", sandboxID, request); err != nil || response.Container.Status.Phase != "created" {
		t.Fatalf("CreateContainer(replay) = (%#v, %v), want created", response, err)
	}
	if response, err := api.StartContainer(context.Background(), "op-stack-start-container", request.ContainerID); err != nil || response.Container.Status.Phase != "running" {
		t.Fatalf("StartContainer(replay) = (%#v, %v), want historical running result", response, err)
	}
	if response, err := api.KillContainer(context.Background(), "op-stack-kill-container", request.ContainerID, policy); err != nil || response.Container.Status.Phase != "stopped" {
		t.Fatalf("KillContainer(replay) = (%#v, %v), want stopped", response, err)
	}
}

// readAllLifecycleEvents exercises the public resume endpoint and returns its complete bounded page.
func readAllLifecycleEvents(t *testing.T, api *client.Client) []v1.Event {
	t.Helper()
	response, err := api.Events(context.Background(), "", 500)
	if err != nil {
		t.Fatalf("Events() error = %v", err)
	}
	if response.HasMore {
		t.Fatalf("Events() unexpectedly required another page after %d events", len(response.Events))
	}
	if len(response.Events) != 0 && response.NextResumeToken != v1.NewResumeToken(response.Events[len(response.Events)-1].Sequence) {
		t.Fatalf("Events() resume token = %q, want final sequence %d", response.NextResumeToken, response.Events[len(response.Events)-1].Sequence)
	}
	return append([]v1.Event(nil), response.Events...)
}

// assertOwnedCompleteEvents requires one immutable successful completion per
// operation and the exact resource identities owned by that lifecycle target.
func assertOwnedCompleteEvents(t *testing.T, events []v1.Event, sandboxID, containerID, attemptID string) {
	t.Helper()
	sandboxResources := []v1.ResourceRef{{Kind: string(operation.TargetSandbox), ID: sandboxID}}
	containerResources := []v1.ResourceRef{
		{Kind: string(operation.TargetSandbox), ID: sandboxID},
		{Kind: string(operation.TargetContainer), ID: containerID},
		{Kind: string(operation.TargetAttempt), ID: attemptID},
	}
	expected := map[string]struct {
		target    v1.ResourceRef
		resources []v1.ResourceRef
	}{
		"op-stack-create-sandbox":   {target: sandboxResources[0], resources: sandboxResources},
		"op-stack-create-container": {target: containerResources[1], resources: containerResources},
		"op-stack-start-container":  {target: containerResources[1], resources: containerResources},
		"op-stack-kill-container":   {target: containerResources[1], resources: containerResources},
		"op-stack-delete-container": {target: containerResources[1], resources: containerResources},
		"op-stack-stop-sandbox":     {target: sandboxResources[0], resources: sandboxResources},
		"op-stack-delete-sandbox":   {target: sandboxResources[0], resources: sandboxResources},
	}
	complete := make(map[string]int, len(expected))
	for index, event := range events {
		if event.Sequence != uint64(index+1) {
			t.Fatalf("event sequence[%d] = %d, want %d", index, event.Sequence, index+1)
		}
		if event.Stage != string(operation.StageComplete) {
			continue
		}
		want, found := expected[event.OperationID]
		if !found {
			t.Fatalf("unexpected complete event for operation %q", event.OperationID)
		}
		complete[event.OperationID]++
		if event.Result != string(operation.ResultSucceeded) || event.Target != want.target || !reflect.DeepEqual(event.Resources, want.resources) {
			t.Fatalf("complete event %q = %#v, want succeeded target %#v resources %#v", event.OperationID, event, want.target, want.resources)
		}
	}
	for operationID := range expected {
		if complete[operationID] != 1 {
			t.Fatalf("operation %q complete event count = %d, want 1", operationID, complete[operationID])
		}
	}
}
