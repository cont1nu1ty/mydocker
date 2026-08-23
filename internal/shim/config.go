package shim

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"mydocker/internal/domain"
	"mydocker/internal/ownership"
)

const maxRuntimeConfigBytes = 1 << 20

// RuntimeConfig is the restricted file-only bootstrap contract for the production shim command.
type RuntimeConfig struct {
	SchemaVersion     uint32             `json:"schema_version"`
	Mode              Mode               `json:"mode"`
	Owner             ownership.OwnerKey `json:"owner"`
	SandboxID         domain.SandboxID   `json:"sandbox_id"`
	ContainerID       domain.ContainerID `json:"container_id,omitempty"`
	AttemptID         domain.AttemptID   `json:"attempt_id,omitempty"`
	WrapperEvidence   string             `json:"wrapper_evidence_sha256"`
	BootstrapEvidence string             `json:"bootstrap_evidence_sha256,omitempty"`
	ControlSocket     string             `json:"control_socket"`
	TerminalPath      string             `json:"terminal_path,omitempty"`
	LogPath           string             `json:"log_path,omitempty"`
	Process           domain.ProcessSpec `json:"process,omitempty"`
}

// RuntimeConfigEvidence hashes every immutable bootstrap field except the
// self-describing WrapperEvidence slot; production launchers persist this
// digest in that slot and use it to reject configuration replacement.
func RuntimeConfigEvidence(config RuntimeConfig) (string, error) {
	config.WrapperEvidence = ""
	return ownership.EvidenceDigest(config)
}

// Validate requires exact role identity, private-artifact paths, and structured absolute workload argv.
func (config RuntimeConfig) Validate() error {
	if config.SchemaVersion != SchemaVersion || !config.Mode.Valid() {
		return errors.New("unsupported runtime config schema or mode")
	}
	if err := validateAbsolutePath("control socket", config.ControlSocket); err != nil {
		return err
	}
	switch config.Mode {
	case ModeKeeper:
		spec := KeeperSpec{Owner: config.Owner, SandboxID: config.SandboxID, WrapperEvidence: config.WrapperEvidence}
		if err := spec.Validate(); err != nil {
			return err
		}
		if config.ContainerID != "" || config.AttemptID != "" || config.BootstrapEvidence != "" || config.TerminalPath != "" || config.LogPath != "" ||
			len(config.Process.Argv) != 0 || len(config.Process.Environment) != 0 || config.Process.WorkingDirectory != "" ||
			config.Process.Termination.Signal != "" || config.Process.Termination.GracePeriod != 0 || config.Process.Termination.EscalationSignal != "" {
			return errors.New("keeper config must not contain Attempt process or artifact fields")
		}
	case ModeInit:
		spec := config.InitSpec()
		if err := spec.Validate(); err != nil {
			return err
		}
		if err := validateAbsolutePath("terminal path", config.TerminalPath); err != nil {
			return err
		}
		if err := validateAbsolutePath("log path", config.LogPath); err != nil {
			return err
		}
		if config.BootstrapEvidence != "" && !validDigest(config.BootstrapEvidence) {
			return errors.New("init bootstrap evidence must be a lowercase SHA-256 digest")
		}
		if config.ControlSocket == config.TerminalPath || config.ControlSocket == config.LogPath || config.TerminalPath == config.LogPath {
			return errors.New("control socket, terminal path, and log path must be distinct")
		}
		if !filepath.IsAbs(config.Process.Argv[0]) || filepath.Clean(config.Process.Argv[0]) != config.Process.Argv[0] {
			return errors.New("runtime config executable must be a clean absolute path")
		}
	}
	return nil
}

// InitSpec extracts the immutable gated wrapper inputs without retaining filesystem bootstrap paths.
func (config RuntimeConfig) InitSpec() InitSpec {
	return InitSpec{
		Owner: config.Owner, SandboxID: config.SandboxID, ContainerID: config.ContainerID,
		AttemptID: config.AttemptID, WrapperEvidence: config.WrapperEvidence, Process: config.Process.Clone(),
	}
}

// KeeperSpec extracts the immutable minimal keeper identity from a validated runtime configuration.
func (config RuntimeConfig) KeeperSpec() KeeperSpec {
	return KeeperSpec{Owner: config.Owner, SandboxID: config.SandboxID, WrapperEvidence: config.WrapperEvidence}
}

// LoadRuntimeConfig securely opens a same-owner mode-0600 file, rejects swaps and unknown JSON fields, and validates it.
func LoadRuntimeConfig(path string) (RuntimeConfig, error) {
	if err := validateAbsolutePath("runtime config", path); err != nil {
		return RuntimeConfig{}, err
	}
	if err := validatePrivateDirectory(osTerminalFS{}, filepath.Dir(path)); err != nil {
		return RuntimeConfig{}, err
	}
	before, err := os.Lstat(path)
	if err != nil {
		return RuntimeConfig{}, fmt.Errorf("inspect runtime config: %w", err)
	}
	if err := validatePrivateFile(before); err != nil {
		return RuntimeConfig{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return RuntimeConfig{}, fmt.Errorf("open runtime config: %w", err)
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil {
		return RuntimeConfig{}, fmt.Errorf("inspect opened runtime config: %w", err)
	}
	if !os.SameFile(before, after) {
		return RuntimeConfig{}, fmt.Errorf("%w: runtime config changed while opening", ErrUnsafeArtifact)
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxRuntimeConfigBytes+1))
	if err != nil {
		return RuntimeConfig{}, fmt.Errorf("read runtime config: %w", err)
	}
	if len(payload) > maxRuntimeConfigBytes {
		return RuntimeConfig{}, errors.New("runtime config exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var config RuntimeConfig
	if err := decoder.Decode(&config); err != nil {
		return RuntimeConfig{}, fmt.Errorf("decode runtime config: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return RuntimeConfig{}, fmt.Errorf("runtime config trailing data: %w", err)
	}
	if err := config.Validate(); err != nil {
		return RuntimeConfig{}, err
	}
	return config, nil
}
