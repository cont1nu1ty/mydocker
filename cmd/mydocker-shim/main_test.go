package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/shim"
)

// TestRunRequiresExactlyOneConfig verifies the production command rejects ambient or positional bootstrap input.
func TestRunRequiresExactlyOneConfig(t *testing.T) {
	if err := run(context.Background(), nil, io.Discard); err == nil {
		t.Fatal("missing -config was accepted")
	}
	if err := run(context.Background(), []string{"positional"}, io.Discard); err == nil {
		t.Fatal("positional bootstrap argument was accepted")
	}
}

// TestRunRejectsWorldReadableConfig verifies the command fails before binding a socket from unsafe metadata.
func TestRunRejectsWorldReadableConfig(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "shim.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{"-config", path}, io.Discard); err == nil {
		t.Fatal("world-readable config was accepted")
	}
}

// TestServePropagatesSocketReplacementCleanupFailure verifies a graceful
// cancellation cannot report success while an unsafe control path remains.
func TestServePropagatesSocketReplacementCleanupFailure(t *testing.T) {
	wrapper := commandTestKeeper(t)
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(directory, "keeper.sock")
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- serve(ctx, socketPath, wrapper)
	}()
	waitForCommandControl(t, socketPath, wrapper.Owner())
	if err := os.Remove(socketPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(socketPath, []byte("foreign replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, shim.ErrUnsafeArtifact) {
			t.Fatalf("serve() error=%v, want ErrUnsafeArtifact", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serve() did not stop after cancellation")
	}
}

// commandTestKeeper constructs the exact owner-bound keeper used by command serving tests.
func commandTestKeeper(t *testing.T) *shim.Wrapper {
	t.Helper()
	owner, err := ownership.NewOwnerKey(
		operation.OperationID("op-command-keeper"),
		operation.Target{Kind: operation.TargetSandbox, ID: "sandbox-command-keeper"},
		domain.InitialGeneration,
	)
	if err != nil {
		t.Fatal(err)
	}
	wrapper, err := shim.NewKeeper(shim.KeeperSpec{
		Owner: owner, SandboxID: "sandbox-command-keeper", WrapperEvidence: strings.Repeat("a", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	return wrapper
}

// waitForCommandControl waits for the temporary UDS to accept one owner-bound inspection request.
func waitForCommandControl(t *testing.T, socketPath string, owner ownership.OwnerKey) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		requestCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		response, err := shim.DoControl(requestCtx, socketPath, shim.ControlRequest{
			SchemaVersion: shim.SchemaVersion, RequestID: "command-ready", Owner: owner, Action: shim.ActionInspect,
		})
		cancel()
		if err == nil && response.Error == nil && response.Observation != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("control server did not become ready: response=%+v error=%v", response, err)
		}
	}
}
