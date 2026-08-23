package shim

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"mydocker/internal/domain"
)

// TestLoadRuntimeConfigPreservesStructuredProcess verifies private JSON bootstrap does not shell-flatten argv or environment.
func TestLoadRuntimeConfigPreservesStructuredProcess(t *testing.T) {
	directory := privateTempDir(t)
	spec := testInitSpec(t, "op-config", "container-config", "attempt-config")
	config := RuntimeConfig{
		SchemaVersion: SchemaVersion, Mode: ModeInit, Owner: spec.Owner,
		SandboxID: spec.SandboxID, ContainerID: spec.ContainerID, AttemptID: spec.AttemptID,
		WrapperEvidence: spec.WrapperEvidence, ControlSocket: filepath.Join(directory, "control.sock"),
		TerminalPath: filepath.Join(directory, "terminal.json"), LogPath: filepath.Join(directory, "workload.log"),
		Process: spec.Process,
	}
	config.Process.Environment = []domain.EnvVar{{Name: "GREETING", Value: "hello world"}, {Name: "EMPTY", Value: ""}}
	path := filepath.Join(directory, "shim.json")
	writeJSONFile(t, path, config)
	loaded, err := LoadRuntimeConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded, config) {
		t.Fatalf("loaded config differs:\n got %+v\nwant %+v", loaded, config)
	}
}

// TestLoadRuntimeConfigRejectsUnknownFieldsAndSymlinks verifies bootstrap input is strict and cannot be redirected.
func TestLoadRuntimeConfigRejectsUnknownFieldsAndSymlinks(t *testing.T) {
	directory := privateTempDir(t)
	spec := testKeeperSpec(t)
	config := RuntimeConfig{
		SchemaVersion: SchemaVersion, Mode: ModeKeeper, Owner: spec.Owner,
		SandboxID: spec.SandboxID, WrapperEvidence: spec.WrapperEvidence,
		ControlSocket: filepath.Join(directory, "keeper.sock"),
	}
	payload, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	payload[len(payload)-1] = ','
	payload = append(payload, []byte(`"unexpected":true}`)...)
	unknownPath := filepath.Join(directory, "unknown.json")
	if err := os.WriteFile(unknownPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRuntimeConfig(unknownPath); err == nil {
		t.Fatal("unknown config field was accepted")
	}
	validPath := filepath.Join(directory, "valid.json")
	writeJSONFile(t, validPath, config)
	linkPath := filepath.Join(directory, "link.json")
	if err := os.Symlink(validPath, linkPath); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRuntimeConfig(linkPath); !errors.Is(err, ErrUnsafeArtifact) {
		t.Fatalf("symlink config error=%v, want ErrUnsafeArtifact", err)
	}
}

// TestRuntimeConfigRejectsRelativeExecutable verifies the production command cannot inherit ambient PATH lookup.
func TestRuntimeConfigRejectsRelativeExecutable(t *testing.T) {
	directory := privateTempDir(t)
	spec := testInitSpec(t, "op-relative", "container-relative", "attempt-relative")
	spec.Process.Argv[0] = "workload"
	config := RuntimeConfig{
		SchemaVersion: SchemaVersion, Mode: ModeInit, Owner: spec.Owner,
		SandboxID: spec.SandboxID, ContainerID: spec.ContainerID, AttemptID: spec.AttemptID,
		WrapperEvidence: spec.WrapperEvidence, ControlSocket: filepath.Join(directory, "control.sock"),
		TerminalPath: filepath.Join(directory, "terminal.json"), LogPath: filepath.Join(directory, "workload.log"),
		Process: spec.Process,
	}
	if err := config.Validate(); err == nil {
		t.Fatal("relative executable was accepted")
	}
}

// writeJSONFile writes one mode-0600 config fixture for secure loader tests.
func writeJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
}
