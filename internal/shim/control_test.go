package shim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
)

// TestDecodeControlRequestRejectsAmbiguousJSON verifies the private shim
// protocol applies the same no-alias, no-duplicate, valid-UTF-8 framing rules
// as the public API and durable state loaders.
func TestDecodeControlRequestRejectsAmbiguousJSON(t *testing.T) {
	request := ControlRequest{
		SchemaVersion: SchemaVersion, RequestID: "request-strict-json",
		Owner: testKeeperSpec(t).Owner, Action: ActionInspect,
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	invalidUTF8 := append([]byte(nil), encoded...)
	requestIDOffset := bytes.Index(invalidUTF8, []byte("request-strict-json"))
	if requestIDOffset < 0 {
		t.Fatal("encoded request ID not found")
	}
	invalidUTF8[requestIDOffset] = 0xff
	tests := map[string][]byte{
		"duplicate decoded key": bytes.Replace(encoded, []byte(`"request_id":`), []byte(`"request_id":"duplicate","request_id":`), 1),
		"case alias":            bytes.Replace(encoded, []byte(`"request_id"`), []byte(`"Request_ID"`), 1),
		"invalid UTF-8":         invalidUTF8,
		"second value":          append(append([]byte(nil), encoded...), []byte(` {}`)...),
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeControlRequest(bytes.NewReader(payload)); err == nil {
				t.Fatal("ambiguous control request was accepted")
			}
		})
	}
}

// TestPrepareRootfsControlReplayAcrossIndependentDecodes verifies response-loss
// retry compares canonical command content and never repeats the privileged preparer.
func TestPrepareRootfsControlReplayAcrossIndependentDecodes(t *testing.T) {
	preparer := &fakeRootfsPreparer{}
	wrapper, err := NewInit(testInitSpec(t, "op-control-rootfs", "container-control-rootfs", "attempt-control-rootfs"), InitDependencies{
		Runner: &fakeRunner{child: newFakeChild()}, Stdout: io.Discard, Stderr: io.Discard,
		Terminal: &memoryTerminalStore{}, Clock: fixedClock{now: testTime()}, Rootfs: preparer,
	})
	if err != nil {
		t.Fatal(err)
	}
	rootfs := testRootfsRequest()
	original := ControlRequest{
		SchemaVersion: SchemaVersion, RequestID: "request-rootfs-response-loss", Owner: wrapper.Owner(),
		Action: ActionPrepareRootfs, Rootfs: &rootfs,
	}
	payload, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var firstRequest, retryRequest ControlRequest
	if err := json.Unmarshal(payload, &firstRequest); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, &retryRequest); err != nil {
		t.Fatal(err)
	}
	first := wrapper.HandleControl(firstRequest)
	replayed := wrapper.HandleControl(retryRequest)
	if first.Error != nil || replayed.Error != nil || first.Rootfs == nil || replayed.Rootfs == nil || *first.Rootfs != *replayed.Rootfs {
		t.Fatalf("first=%+v replayed=%+v", first, replayed)
	}
	if preparer.count() != 1 {
		t.Fatalf("preparer calls=%d, want one", preparer.count())
	}
}

// TestControlScopesOwnerAndExactlyReplaysRequest verifies exact retries replay while request-ID tampering fails closed.
func TestControlScopesOwnerAndExactlyReplaysRequest(t *testing.T) {
	wrapper, err := NewKeeper(testKeeperSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	wrongOwner, err := ownership.NewOwnerKey(
		operation.OperationID("op-wrong-owner"),
		operation.Target{Kind: operation.TargetSandbox, ID: "sandbox-other"},
		domain.InitialGeneration,
	)
	if err != nil {
		t.Fatal(err)
	}
	wrong := wrapper.HandleControl(ControlRequest{
		SchemaVersion: SchemaVersion, RequestID: "request-wrong-owner", Owner: wrongOwner, Action: ActionInspect,
	})
	if wrong.Error == nil || wrong.Error.Code != CodeOwnerMismatch {
		t.Fatalf("wrong-owner response=%+v", wrong)
	}
	request := ControlRequest{
		SchemaVersion: SchemaVersion, RequestID: "request-replay", Owner: wrapper.Owner(), Action: ActionInspect,
	}
	first := wrapper.HandleControl(request)
	if first.Error != nil || first.Observation == nil || first.Observation.State != StatePrepared {
		t.Fatalf("first response=%+v", first)
	}
	second := wrapper.HandleControl(request)
	if second.Error != nil || second.Observation == nil || second.Observation.EvidenceSHA256 != first.Observation.EvidenceSHA256 {
		t.Fatalf("exact replay response=%+v", second)
	}
}

// TestConcurrentSignalReplayInvokesChildOnce verifies response loss and concurrent retry cannot duplicate delivery or change its wrapper-stamped time.
func TestConcurrentSignalReplayInvokesChildOnce(t *testing.T) {
	spec := testInitSpec(t, "op-signal-replay", "container-signal-replay", "attempt-signal-replay")
	child := newFakeChild()
	wrapper, err := NewInit(spec, InitDependencies{
		Runner: &fakeRunner{child: child}, Stdout: io.Discard, Stderr: io.Discard,
		Terminal: &memoryTerminalStore{}, Clock: fixedClock{now: testTime()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapper.Release(); err != nil {
		t.Fatal(err)
	}
	request := ControlRequest{
		SchemaVersion: SchemaVersion, RequestID: "operation-signal-initial", Owner: wrapper.Owner(),
		Action: ActionSignal, Signal: SignalTERM,
	}
	const callers = 24
	responses := make(chan ControlResponse, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for index := 0; index < callers; index++ {
		go func() {
			defer wait.Done()
			responses <- wrapper.HandleControl(request)
		}()
	}
	wait.Wait()
	close(responses)
	for response := range responses {
		if response.Error != nil || response.Delivery == nil || response.Delivery.Signal != SignalTERM || !response.Delivery.DeliveredAt.Equal(testTime()) {
			t.Fatalf("signal replay response=%+v", response)
		}
	}
	if child.signals.Load() != 1 {
		t.Fatalf("verified child signal calls=%d, want 1", child.signals.Load())
	}
	tampered := request
	tampered.Signal = SignalKILL
	response := wrapper.HandleControl(tampered)
	if response.Error == nil || response.Error.Code != CodeDuplicateRequest || child.signals.Load() != 1 {
		t.Fatalf("tampered signal response=%+v calls=%d", response, child.signals.Load())
	}
}

// TestControlRejectsInvalidAndRepeatedActions verifies invalid signal fields and second release use typed failures.
func TestControlRejectsInvalidAndRepeatedActions(t *testing.T) {
	spec := testInitSpec(t, "op-control-release", "container-control-release", "attempt-control-release")
	child := newFakeChild()
	wrapper, err := NewInit(spec, InitDependencies{
		Runner: &fakeRunner{child: child}, Stdout: io.Discard, Stderr: io.Discard,
		Terminal: &memoryTerminalStore{}, Clock: fixedClock{now: testTime()},
	})
	if err != nil {
		t.Fatal(err)
	}
	invalid := wrapper.HandleControl(ControlRequest{
		SchemaVersion: SchemaVersion, RequestID: "request-invalid-signal", Owner: wrapper.Owner(),
		Action: ActionInspect, Signal: SignalTERM,
	})
	if invalid.Error == nil || invalid.Error.Code != CodeInvalidArgument {
		t.Fatalf("invalid response=%+v", invalid)
	}
	for index := 0; index < maxRememberedControlRequests+100; index++ {
		inspected := wrapper.HandleControl(ControlRequest{
			SchemaVersion: SchemaVersion, RequestID: fmt.Sprintf("request-inspect-%d", index),
			Owner: wrapper.Owner(), Action: ActionInspect,
		})
		if inspected.Error != nil || inspected.Observation == nil {
			t.Fatalf("inspection %d response=%+v", index, inspected)
		}
	}
	first := wrapper.HandleControl(ControlRequest{
		SchemaVersion: SchemaVersion, RequestID: "request-release-one", Owner: wrapper.Owner(), Action: ActionRelease,
	})
	if first.Error != nil || first.Observation == nil || first.Observation.State != StateRunning {
		t.Fatalf("first release=%+v", first)
	}
	second := wrapper.HandleControl(ControlRequest{
		SchemaVersion: SchemaVersion, RequestID: "request-release-two", Owner: wrapper.Owner(), Action: ActionRelease,
	})
	if second.Error == nil || second.Error.Code != CodeAlreadyReleased {
		t.Fatalf("second release=%+v", second)
	}
}

// TestUnixControlServerSurvivesClientDisconnect verifies inspection works across fresh daemon-like connections.
func TestUnixControlServerSurvivesClientDisconnect(t *testing.T) {
	wrapper, err := NewKeeper(testKeeperSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(directory, "keeper.sock")
	server, err := NewControlServer(socketPath, wrapper)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		served <- server.Serve(ctx)
	}()
	for index, requestID := range []string{"daemon-before-disconnect", "daemon-after-reconnect"} {
		response, requestErr := DoControl(context.Background(), socketPath, ControlRequest{
			SchemaVersion: SchemaVersion, RequestID: requestID, Owner: wrapper.Owner(), Action: ActionInspect,
		})
		if requestErr != nil {
			t.Fatalf("request %d: %v", index, requestErr)
		}
		if response.Error != nil || response.Observation == nil || response.Observation.State != StatePrepared {
			t.Fatalf("request %d response=%+v", index, response)
		}
	}
	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("control server did not stop after context cancellation")
	}
	if err := server.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
}

// TestRetainedDirectoryFDArtifactsWorkAcrossProcfs verifies the exact
// descriptor-backed paths used by PID1 can persist terminal facts and serve a
// private control socket after their original directory pathname is hidden by
// pivot_root.
func TestRetainedDirectoryFDArtifactsWorkAcrossProcfs(t *testing.T) {
	directory := privateTempDir(t)
	directoryFD, err := os.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer directoryFD.Close()
	retainedRoot := fmt.Sprintf("/proc/self/fd/%d", directoryFD.Fd())
	terminal, err := NewFileTerminalStore(filepath.Join(retainedRoot, "terminal.json"))
	if err != nil {
		t.Fatalf("NewFileTerminalStore(retained path) error = %v", err)
	}
	record := startFailureRecord(t)
	if err := terminal.Commit(record); err != nil {
		t.Fatalf("Commit(retained path) error = %v", err)
	}
	loaded, found, err := terminal.Load()
	if err != nil || !found || !reflect.DeepEqual(loaded, record) {
		t.Fatalf("Load(retained path) = (%+v, %t, %v), want exact record", loaded, found, err)
	}

	wrapper, err := NewKeeper(testKeeperSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(retainedRoot, "control.sock")
	server, err := NewControlServer(socketPath, wrapper)
	if err != nil {
		t.Fatalf("NewControlServer(retained path) error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx) }()
	response, err := DoControl(context.Background(), socketPath, ControlRequest{
		SchemaVersion: SchemaVersion, RequestID: "retained-directory-inspect", Owner: wrapper.Owner(), Action: ActionInspect,
	})
	if err != nil || response.Error != nil || response.Observation == nil || response.Observation.State != StatePrepared {
		t.Fatalf("retained control response = (%+v, %v)", response, err)
	}
	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retained control server did not stop after cancellation")
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(directory, "control.sock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control socket after Close() error = %v, want absent", err)
	}
}

// TestControlServerCloseReplaysSocketReplacementFailure verifies repeated
// cleanup cannot hide the first unsafe-inode diagnostic or delete a replacement.
func TestControlServerCloseReplaysSocketReplacementFailure(t *testing.T) {
	wrapper, err := NewKeeper(testKeeperSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(directory, "replaced.sock")
	server, err := NewControlServer(socketPath, wrapper)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(socketPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(socketPath, []byte("foreign replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	first := server.Close()
	second := server.Close()
	if !errors.Is(first, ErrUnsafeArtifact) || !errors.Is(second, ErrUnsafeArtifact) || first.Error() != second.Error() {
		t.Fatalf("Close() errors=(%v,%v), want stable ErrUnsafeArtifact replay", first, second)
	}
	payload, err := os.ReadFile(socketPath)
	if err != nil || string(payload) != "foreign replacement" {
		t.Fatalf("replacement after Close()=(%q,%v), want untouched file", payload, err)
	}
}

// TestDoControlWithPeerReturnsServingProcess verifies launcher recovery receives
// the kernel-authenticated PID of the shim serving the private Unix socket.
func TestDoControlWithPeerReturnsServingProcess(t *testing.T) {
	wrapper, err := NewKeeper(testKeeperSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	server, err := NewControlServer(filepath.Join(directory, "peer.sock"), wrapper)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx) }()
	response, peerPID, err := DoControlWithPeer(context.Background(), server.path, ControlRequest{
		SchemaVersion: SchemaVersion, RequestID: "request-peer", Owner: wrapper.Owner(), Action: ActionInspect,
	})
	if err != nil || response.Error != nil {
		t.Fatalf("peer exchange=(%+v,%d,%v)", response, peerPID, err)
	}
	if peerPID != os.Getpid() {
		t.Fatalf("peer PID=%d, want %d", peerPID, os.Getpid())
	}
}

// TestDoControlWithExpectedPeerRejectsBeforeRelease verifies socket
// replacement detection happens before request bytes can consume the wrapper's
// one-shot workload gate.
func TestDoControlWithExpectedPeerRejectsBeforeRelease(t *testing.T) {
	runner := &fakeRunner{child: newFakeChild()}
	wrapper, err := NewInit(testInitSpec(t, "op-expected-peer", "container-expected-peer", "attempt-expected-peer"), InitDependencies{
		Runner: runner, Stdout: io.Discard, Stderr: io.Discard,
		Terminal: &memoryTerminalStore{}, Clock: fixedClock{now: testTime()},
	})
	if err != nil {
		t.Fatal(err)
	}
	directory := privateTempDir(t)
	server, err := NewControlServer(filepath.Join(directory, "expected-peer.sock"), wrapper)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx) }()
	_, _, err = DoControlWithExpectedPeer(context.Background(), server.path, os.Getpid()+1, ControlRequest{
		SchemaVersion: SchemaVersion, RequestID: "wrong-expected-peer-release", Owner: wrapper.Owner(), Action: ActionRelease,
	})
	if !IsCode(err, CodeUnavailable) {
		t.Fatalf("DoControlWithExpectedPeer() error = %v, want unavailable", err)
	}
	observation, err := wrapper.Inspect()
	if err != nil || observation.State != StatePrepared || runner.starts.Load() != 0 {
		t.Fatalf("wrapper after rejected peer = (%+v, %v), starts=%d", observation, err, runner.starts.Load())
	}
	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("control server did not stop after expected-peer test")
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestDoControlReportsUnavailableDaemonPath verifies a missing wrapper endpoint is a typed transport failure.
func TestDoControlReportsUnavailableDaemonPath(t *testing.T) {
	wrapper, err := NewKeeper(testKeeperSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	_, err = DoControl(context.Background(), filepath.Join(directory, "absent.sock"), ControlRequest{
		SchemaVersion: SchemaVersion, RequestID: "request-absent", Owner: wrapper.Owner(), Action: ActionInspect,
	})
	if !IsCode(err, CodeUnavailable) {
		t.Fatalf("error=%v, want unavailable", err)
	}
}

// TestBoundedControlContextCapsBackgroundAndLongDeadlines verifies recovery
// traffic cannot inherit an unbounded or excessively long client wait.
func TestBoundedControlContextCapsBackgroundAndLongDeadlines(t *testing.T) {
	for _, parent := range []context.Context{context.Background(), mustLongControlContext(t)} {
		bounded, cancel := boundedControlContext(parent, 100*time.Millisecond)
		deadline, exists := bounded.Deadline()
		cancel()
		if !exists {
			t.Fatal("bounded control context has no deadline")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 || remaining > 100*time.Millisecond {
			t.Fatalf("remaining deadline=%s, want (0,100ms]", remaining)
		}
	}
}

// mustLongControlContext supplies a one-hour parent deadline for deadline-cap testing.
func mustLongControlContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	t.Cleanup(cancel)
	return ctx
}
