package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"mydocker/internal/provider"
	"mydocker/internal/server"
	"mydocker/internal/slim"
)

// fakeDaemonRuntime records startup ordering and injects failures without
// constructing any production host provider or persistence implementation.
type fakeDaemonRuntime struct {
	socketPath   string
	order        *[]string
	preflightErr error
	reconcileErr error
	closed       bool
}

// Preflight proves the public socket is still absent at the read-only host
// capability boundary and then returns its deterministic injected outcome.
func (runtime *fakeDaemonRuntime) Preflight(context.Context) error {
	*runtime.order = append(*runtime.order, "preflight")
	if _, err := os.Lstat(runtime.socketPath); !errors.Is(err, os.ErrNotExist) {
		return errors.New("public socket existed during preflight")
	}
	return runtime.preflightErr
}

// Reconcile proves recovery also precedes UDS construction or binding and
// returns its deterministic injected outcome.
func (runtime *fakeDaemonRuntime) Reconcile(context.Context) error {
	*runtime.order = append(*runtime.order, "reconcile")
	if _, err := os.Lstat(runtime.socketPath); !errors.Is(err, os.ErrNotExist) {
		return errors.New("public socket existed during recovery")
	}
	return runtime.reconcileErr
}

// APIService returns nil because the injected test endpoint deliberately does
// not decode HTTP or call lifecycle methods.
func (runtime *fakeDaemonRuntime) APIService() server.Service { return nil }

// Close records release of the injected runtime after every success or
// post-open failure path.
func (runtime *fakeDaemonRuntime) Close() error {
	*runtime.order = append(*runtime.order, "close")
	runtime.closed = true
	return nil
}

// fakeManagedServer owns only one temporary Unix listener and never creates an
// HTTP handler, cgroup, namespace, process, signal, or mount side effect.
type fakeManagedServer struct {
	path    string
	order   *[]string
	started chan struct{}
	done    chan struct{}
	once    sync.Once
	listen  *net.UnixListener
}

// Start binds the temporary UDS and announces readiness to the controlling
// test after recovery ordering has already been checked.
func (endpoint *fakeManagedServer) Start() error {
	*endpoint.order = append(*endpoint.order, "start")
	address := &net.UnixAddr{Name: endpoint.path, Net: "unix"}
	listener, err := net.ListenUnix("unix", address)
	if err != nil {
		return err
	}
	listener.SetUnlinkOnClose(true)
	endpoint.listen = listener
	close(endpoint.started)
	return nil
}

// Wait blocks until Shutdown closes the temporary endpoint.
func (endpoint *fakeManagedServer) Wait() error {
	<-endpoint.done
	return nil
}

// Shutdown closes only the temporary listener and makes concurrent Wait return.
func (endpoint *fakeManagedServer) Shutdown(context.Context) error {
	*endpoint.order = append(*endpoint.order, "shutdown")
	var closeErr error
	endpoint.once.Do(func() {
		if endpoint.listen != nil {
			closeErr = endpoint.listen.Close()
		}
		close(endpoint.done)
	})
	return closeErr
}

// fakeIsolationInspector supplies only the read-only isolation capability seam
// and records whether cgroup discovery could run afterward.
type fakeIsolationInspector struct {
	called       bool
	capabilities provider.IsolationCapabilities
	err          error
}

// InspectIsolationCapabilities returns the configured pure observation.
func (inspector *fakeIsolationInspector) InspectIsolationCapabilities(context.Context, provider.IsolationRequirements) (provider.IsolationCapabilities, error) {
	inspector.called = true
	return inspector.capabilities, inspector.err
}

// fakeCgroupInspector supplies only the read-only cgroup capability seam.
type fakeCgroupInspector struct {
	called       bool
	capabilities provider.CgroupCapabilities
	err          error
}

// InspectCgroupCapabilities returns the configured pure observation.
func (inspector *fakeCgroupInspector) InspectCgroupCapabilities(context.Context, provider.CgroupRequirements) (provider.CgroupCapabilities, error) {
	inspector.called = true
	return inspector.capabilities, inspector.err
}

// validDaemonArguments constructs complete explicit absolute configuration
// beneath one test-owned directory without requiring any path to be privileged.
func validDaemonArguments(root string) []string {
	return []string{
		"--state", filepath.Join(root, "state", "runtime.json"),
		"--runtime-root", filepath.Join(root, "runtime"),
		"--socket", filepath.Join(root, "api", "mydockerd.sock"),
		"--cgroup-root", filepath.Join(root, "delegated-cgroup"),
		"--shim", filepath.Join(root, "bin", "mydocker-shim"),
		"--prepared-rootfs", "base=" + filepath.Join(root, "images", "base", "rootfs"),
	}
}

// newFakeEndpointFactory returns an unstarted temporary UDS endpoint and
// exposes its readiness channel for deterministic graceful-shutdown tests.
func newFakeEndpointFactory(path string, order *[]string, started chan struct{}) serverFactory {
	return func(config server.Config, _ server.Service) (managedServer, error) {
		*order = append(*order, "construct-server")
		if config.SocketPath != path {
			return nil, errors.New("server received a different socket path")
		}
		return &fakeManagedServer{path: path, order: order, started: started, done: make(chan struct{})}, nil
	}
}

// requireJSONLogLines verifies startup diagnostics remain newline-delimited
// structured JSON even on fail-closed paths.
func requireJSONLogLines(t *testing.T, output []byte) {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(output), []byte{'\n'})
	if len(lines) == 0 || len(lines[0]) == 0 {
		t.Fatal("structured startup log is empty")
	}
	for _, line := range lines {
		var value map[string]any
		if err := json.Unmarshal(line, &value); err != nil {
			t.Fatalf("startup log line is not JSON: %q: %v", line, err)
		}
		if value["schema_version"] != float64(1) || value["message"] == "" {
			t.Fatalf("startup log record is incomplete: %#v", value)
		}
	}
}

// TestParseDaemonConfigRequiresExplicitAbsoluteOwnershipPaths verifies no
// production host boundary is inferred from the working directory or defaults.
func TestParseDaemonConfigRequiresExplicitAbsoluteOwnershipPaths(t *testing.T) {
	root := t.TempDir()
	config, err := parseDaemonConfig(validDaemonArguments(root))
	if err != nil {
		t.Fatalf("parse complete config: %v", err)
	}
	if config.statePath != filepath.Join(root, "state", "runtime.json") || config.shutdown != defaultShutdownTimeout {
		t.Fatalf("parsed config = %#v", config)
	}
	source, found := config.preparedRootFS["base"]
	if !found || source.Rootfs != filepath.Join(root, "images", "base", "rootfs") || source.AllowedRoot != filepath.Join(root, "images", "base") {
		t.Fatalf("prepared source = %#v, found = %v", source, found)
	}
	invalid := validDaemonArguments(root)
	invalid[1] = "relative-state.json"
	if _, err := parseDaemonConfig(invalid); err == nil {
		t.Fatal("relative state path was accepted")
	}
	if _, err := parseDaemonConfig(nil); err == nil {
		t.Fatal("missing explicit host configuration was accepted")
	}
}

// TestParseDaemonConfigRejectsDuplicatePreparedRootFS verifies catalog identity
// collisions fail before a runtime or FileStore is opened.
func TestParseDaemonConfigRejectsDuplicatePreparedRootFS(t *testing.T) {
	root := t.TempDir()
	arguments := append(validDaemonArguments(root), "--prepared-rootfs", "base="+filepath.Join(root, "images", "other", "rootfs"))
	if _, err := parseDaemonConfig(arguments); err == nil {
		t.Fatal("duplicate prepared-rootfs ID was accepted")
	}
}

// TestRunDaemonRecoversBeforeBindingAndShutsDown verifies the complete command
// ordering using only an injected runtime and a temporary Unix listener.
func TestRunDaemonRecoversBeforeBindingAndShutsDown(t *testing.T) {
	root := t.TempDir()
	arguments := validDaemonArguments(root)
	socketPath := filepath.Join(root, "api", "mydockerd.sock")
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		t.Fatal(err)
	}
	var order []string
	runtime := &fakeDaemonRuntime{socketPath: socketPath, order: &order}
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	var logs bytes.Buffer
	go func() {
		result <- runDaemon(ctx, arguments, &logs,
			func(context.Context, daemonConfig) (daemonRuntime, error) {
				order = append(order, "open")
				return runtime, nil
			},
			newFakeEndpointFactory(socketPath, &order, started))
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("temporary UDS did not start")
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("run daemon: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not complete graceful shutdown")
	}
	wantOrder := []string{"open", "preflight", "reconcile", "construct-server", "start", "shutdown", "close"}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("startup order = %v, want %v", order, wantOrder)
	}
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket remained after shutdown: %v", err)
	}
	if !runtime.closed {
		t.Fatal("runtime state lock was not released")
	}
	requireJSONLogLines(t, logs.Bytes())
}

// TestBackgroundExitErrorTreatsCanceledWatcherAsNormal verifies a watcher that
// observes cancellation first cannot turn graceful daemon shutdown into failure.
func TestBackgroundExitErrorTreatsCanceledWatcherAsNormal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := backgroundExitError(ctx, nil); err != nil {
		t.Fatalf("backgroundExitError(canceled, nil) = %v", err)
	}
	if err := backgroundExitError(ctx, errors.New("late watcher error")); err != nil {
		t.Fatalf("backgroundExitError(canceled, error) = %v", err)
	}
	if err := backgroundExitError(context.Background(), nil); err == nil {
		t.Fatal("backgroundExitError(active, nil) error = nil")
	}
}

// TestRunDaemonFailsClosedBeforeServerOnLauncherGap verifies an incomplete
// production-style isolation preflight never constructs or binds the UDS.
func TestRunDaemonFailsClosedBeforeServerOnLauncherGap(t *testing.T) {
	root := t.TempDir()
	arguments := validDaemonArguments(root)
	socketPath := filepath.Join(root, "api", "mydockerd.sock")
	var order []string
	runtime := &fakeDaemonRuntime{socketPath: socketPath, order: &order, preflightErr: slim.ErrLauncherIncomplete}
	serverCalled := false
	var logs bytes.Buffer
	err := runDaemon(context.Background(), arguments, &logs,
		func(context.Context, daemonConfig) (daemonRuntime, error) {
			order = append(order, "open")
			return runtime, nil
		},
		func(server.Config, server.Service) (managedServer, error) {
			serverCalled = true
			return nil, errors.New("must not be called")
		})
	if !errors.Is(err, slim.ErrLauncherIncomplete) {
		t.Fatalf("error = %v, want ErrLauncherIncomplete", err)
	}
	if serverCalled {
		t.Fatal("server was constructed after incomplete launcher preflight")
	}
	if !reflect.DeepEqual(order, []string{"open", "preflight", "close"}) {
		t.Fatalf("startup order = %v", order)
	}
	if _, statErr := os.Lstat(socketPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("socket exists after fail-closed startup: %v", statErr)
	}
	requireJSONLogLines(t, logs.Bytes())
}

// TestRunDaemonDoesNotBindWhenRecoveryFails verifies a global discovery or
// reconcile error leaves no API listener and still releases the runtime.
func TestRunDaemonDoesNotBindWhenRecoveryFails(t *testing.T) {
	root := t.TempDir()
	socketPath := filepath.Join(root, "api", "mydockerd.sock")
	var order []string
	runtime := &fakeDaemonRuntime{socketPath: socketPath, order: &order, reconcileErr: errors.New("injected recovery failure")}
	serverCalled := false
	err := runDaemon(context.Background(), validDaemonArguments(root), &bytes.Buffer{},
		func(context.Context, daemonConfig) (daemonRuntime, error) {
			order = append(order, "open")
			return runtime, nil
		},
		func(server.Config, server.Service) (managedServer, error) {
			serverCalled = true
			return nil, errors.New("must not be called")
		})
	if err == nil || serverCalled {
		t.Fatalf("error/serverCalled = %v/%v", err, serverCalled)
	}
	wantOrder := []string{"open", "preflight", "reconcile", "close"}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("startup order = %v, want %v", order, wantOrder)
	}
}

// TestPreflightHostChecksIncompleteIsolationBeforeCgroup verifies the known
// launcher gap is surfaced exactly and no cgroup probe can obscure it.
func TestPreflightHostChecksIncompleteIsolationBeforeCgroup(t *testing.T) {
	isolationInspector := &fakeIsolationInspector{err: slim.ErrLauncherIncomplete}
	cgroupInspector := &fakeCgroupInspector{err: errors.New("must not be reached")}
	err := preflightHost(context.Background(), cgroupInspector, isolationInspector)
	if !errors.Is(err, slim.ErrLauncherIncomplete) {
		t.Fatalf("error = %v, want ErrLauncherIncomplete", err)
	}
	if !isolationInspector.called || cgroupInspector.called {
		t.Fatalf("isolation/cgroup calls = %v/%v", isolationInspector.called, cgroupInspector.called)
	}
}
