package slim

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"mydocker/internal/cgroupv2"
	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
	"mydocker/internal/shim"
)

const (
	// SchemaVersion is the only slim artifact and launcher contract understood by M3.
	SchemaVersion             uint32 = 1
	launcherEvidenceAttribute        = "launcher_evidence_sha256"
	sandboxIDAttribute               = "sandbox_id"
	attemptIDAttribute               = "attempt_id"
)

var (
	// ErrLauncherIncomplete reports the fail-closed production gap between M2 helpers and a safe PID1 shim bootstrap.
	ErrLauncherIncomplete = errors.New("Linux shim launcher is not safely implementable with current M2 primitives")
	// ErrArtifactUnsafe reports a runtime-root, owner-directory, or artifact identity that cannot be trusted.
	ErrArtifactUnsafe = errors.New("unsafe slim runtime artifact")
	// ErrReceiptMismatch reports a receipt whose deterministic identity or evidence no longer matches its owner.
	ErrReceiptMismatch = errors.New("slim receipt evidence mismatch")
)

// ArtifactPaths are deterministic internal paths derived from RuntimeRoot and OwnerKey.Token only.
type ArtifactPaths struct {
	OwnerRoot     string
	ControlSocket string
	Config        string
	LaunchJournal string
	Terminal      string
	Log           string
}

// ValidateFor rejects any caller-constructed path set that differs from deterministic owner derivation.
func (paths ArtifactPaths) ValidateFor(runtimeRoot string, owner ownership.OwnerKey) error {
	expected, err := deriveArtifactPaths(runtimeRoot, owner)
	if err != nil {
		return err
	}
	if paths != expected {
		return fmt.Errorf("%w: launcher paths do not match runtime root and owner token", ErrArtifactUnsafe)
	}
	return nil
}

// deriveArtifactPaths constructs all wrapper artifacts without consuming receipt attributes or API path values.
func deriveArtifactPaths(runtimeRoot string, owner ownership.OwnerKey) (ArtifactPaths, error) {
	if err := owner.Validate(); err != nil {
		return ArtifactPaths{}, err
	}
	if !filepath.IsAbs(runtimeRoot) || filepath.Clean(runtimeRoot) != runtimeRoot || runtimeRoot == string(filepath.Separator) {
		return ArtifactPaths{}, fmt.Errorf("%w: runtime root must be a clean absolute non-root path", ErrArtifactUnsafe)
	}
	ownerRoot := filepath.Join(runtimeRoot, "owners", owner.Token)
	return ArtifactPaths{
		OwnerRoot: ownerRoot, ControlSocket: filepath.Join(ownerRoot, "control.sock"),
		Config: filepath.Join(ownerRoot, "shim.json"), LaunchJournal: filepath.Join(ownerRoot, "launch.json"),
		Terminal: filepath.Join(ownerRoot, "terminal.json"),
		Log:      filepath.Join(ownerRoot, "workload.log"),
	}, nil
}

// LaunchedProcess is the strong wrapper identity and executable evidence returned after readiness.
type LaunchedProcess struct {
	IdentityEvidenceSHA256 string
	WrapperEvidenceSHA256  string
	ProcessEvidence        isolation.ProcessEvidence
}

// Validate rejects a launcher result unless its serializable strong process
// evidence hashes to the independently returned identity used by receipts.
func (process LaunchedProcess) Validate() error {
	if !validDigest(process.IdentityEvidenceSHA256) || !validDigest(process.WrapperEvidenceSHA256) {
		return errors.New("launched process requires identity and wrapper SHA-256 evidence")
	}
	if err := process.ProcessEvidence.Validate(); err != nil {
		return fmt.Errorf("launched process evidence: %w", err)
	}
	digest, err := ownership.EvidenceDigest(process.ProcessEvidence)
	if err != nil {
		return err
	}
	if digest != process.IdentityEvidenceSHA256 {
		return errors.New("launched process identity digest does not match its persisted strong evidence")
	}
	if _, err := encodeProcessEvidence(process.ProcessEvidence); err != nil {
		return fmt.Errorf("launched process evidence is not checkpointable: %w", err)
	}
	return nil
}

// ResourceReference is action-time launcher input reconstructed from a validated owner-bound receipt.
type ResourceReference struct {
	Owner                  ownership.OwnerKey
	Kind                   ownership.Kind
	LocalID                string
	ReceiptEvidenceSHA256  string
	LauncherEvidenceSHA256 string
	WrapperEvidenceSHA256  string
	ConfigurationSHA256    string
	ProcessEvidence        isolation.ProcessEvidence
	SandboxID              domain.SandboxID
	AttemptID              domain.AttemptID
	Hostname               string
	NetworkMode            provider.SandboxNetworkMode
	Paths                  ArtifactPaths
}

// Validate checks bounded identity and ensures all artifact paths are derived rather than accepted from metadata.
func (reference ResourceReference) Validate(runtimeRoot string) error {
	if err := reference.Owner.Validate(); err != nil {
		return err
	}
	if !reference.Kind.Valid() || reference.LocalID != localIDFor(reference.Kind) {
		return errors.New("launcher resource kind or local ID is not deterministic")
	}
	if !validDigest(reference.ReceiptEvidenceSHA256) || !validDigest(reference.LauncherEvidenceSHA256) {
		return errors.New("launcher resource reference requires receipt and live identity evidence")
	}
	if err := reference.ProcessEvidence.Validate(); err != nil {
		return fmt.Errorf("launcher process evidence: %w", err)
	}
	if err := reference.SandboxID.Validate(); err != nil {
		return fmt.Errorf("launcher Sandbox identity: %w", err)
	}
	switch reference.Kind {
	case ownership.KindKeeperProcess, ownership.KindUTSNamespace, ownership.KindIPCNamespace, ownership.KindNetworkNamespace:
		if reference.Owner.Target.Kind != operation.TargetSandbox || reference.Owner.Target.ID != string(reference.SandboxID) || reference.AttemptID != "" {
			return errors.New("Sandbox launcher resource reference has an invalid owner or Attempt scope")
		}
	case ownership.KindInitProcess, ownership.KindPIDNamespace, ownership.KindMountNamespace, ownership.KindRootfsMount:
		if reference.Owner.Target.Kind != operation.TargetContainer || reference.AttemptID.Validate() != nil {
			return errors.New("Attempt launcher resource reference has an invalid owner or Attempt scope")
		}
	default:
		return errors.New("launcher resource kind is not action-time owned")
	}
	switch reference.Kind {
	case ownership.KindUTSNamespace:
		if !validDigest(reference.ConfigurationSHA256) || reference.NetworkMode != "" {
			return errors.New("UTS namespace reference requires only a hostname configuration digest")
		}
	case ownership.KindNetworkNamespace:
		if !validDigest(reference.ConfigurationSHA256) || reference.Hostname != "" || !reference.NetworkMode.Valid() {
			return errors.New("network namespace reference requires one canonical M3 mode configuration digest")
		}
	case ownership.KindRootfsMount:
		if !validDigest(reference.ConfigurationSHA256) || reference.Hostname != "" || reference.NetworkMode != "" {
			return errors.New("rootfs reference requires only a retained DNS configuration digest")
		}
	default:
		if reference.ConfigurationSHA256 != "" || reference.Hostname != "" || reference.NetworkMode != "" {
			return errors.New("resource reference contains configuration for an unrelated resource kind")
		}
	}
	return reference.Paths.ValidateFor(runtimeRoot, reference.Owner)
}

// KeeperLaunch contains only typed owner, Sandbox, cgroup, and internally derived artifact inputs.
type KeeperLaunch struct {
	Owner     ownership.OwnerKey
	SandboxID domain.SandboxID
	Cgroup    ownership.Receipt
	Paths     ArtifactPaths
}

// InitLaunch contains structured process data and already validated M2 dependencies, never host paths from the API.
type InitLaunch struct {
	Owner             ownership.OwnerKey
	SandboxID         domain.SandboxID
	AttemptID         domain.AttemptID
	Cgroup            ownership.Receipt
	Gate              ownership.Receipt
	Streams           ownership.Receipt
	SandboxNamespaces SandboxNamespaceReferences
	Process           domain.ProcessSpec
	Paths             ArtifactPaths
}

// SandboxNamespaceReferences are action-time launcher references reconstructed from the three adopted Sandbox receipts.
type SandboxNamespaceReferences struct {
	UTS     ResourceReference
	IPC     ResourceReference
	Network ResourceReference
}

// ValidateFor rejects missing, cross-Sandbox, wrong-kind, or caller-path-derived namespace references.
func (references SandboxNamespaceReferences) ValidateFor(runtimeRoot string, sandboxID domain.SandboxID) error {
	if err := sandboxID.Validate(); err != nil {
		return err
	}
	expectations := []struct {
		name      string
		reference ResourceReference
		kind      ownership.Kind
	}{
		{"UTS", references.UTS, ownership.KindUTSNamespace},
		{"IPC", references.IPC, ownership.KindIPCNamespace},
		{"network", references.Network, ownership.KindNetworkNamespace},
	}
	var owner ownership.OwnerKey
	for index, expectation := range expectations {
		reference := expectation.reference
		if err := reference.Validate(runtimeRoot); err != nil {
			return fmt.Errorf("%s Sandbox namespace reference: %w", expectation.name, err)
		}
		if reference.Kind != expectation.kind || reference.Owner.Target.Kind != operation.TargetSandbox ||
			reference.Owner.Target.ID != string(sandboxID) || reference.SandboxID != sandboxID || reference.AttemptID != "" {
			return fmt.Errorf("%s Sandbox namespace reference has the wrong typed scope", expectation.name)
		}
		if index == 0 {
			owner = reference.Owner
		} else if reference.Owner != owner {
			return errors.New("Sandbox namespace references do not share one acquisition owner")
		}
	}
	return nil
}

// NamespaceLaunch binds one namespace capture to an action-time verified wrapper reference.
type NamespaceLaunch struct {
	Process             ResourceReference
	Namespace           isolation.NamespaceType
	Hostname            string
	NetworkMode         provider.SandboxNetworkMode
	ConfigurationSHA256 string
}

// RootfsLaunch binds a catalog-resolved prepared source to verified init, PID, and mount launcher references.
type RootfsLaunch struct {
	Owner               ownership.OwnerKey
	AttemptID           domain.AttemptID
	Process             ResourceReference
	PID                 ResourceReference
	Mount               ResourceReference
	SourceID            provider.OpaqueID
	Source              isolation.RootfsConfig
	DNS                 []string
	ConfigurationSHA256 string
	Paths               ArtifactPaths
}

// Launcher is the explicit host boundary required to implement the slim Linux process topology.
type Launcher interface {
	// Preflight performs read-only capability discovery and must reject incomplete PID1/bootstrap support.
	Preflight(context.Context, provider.IsolationRequirements) (provider.IsolationCapabilities, error)
	// EnsureKeeper creates or rediscovers the strong minimal Sandbox namespace keeper.
	EnsureKeeper(context.Context, KeeperLaunch) (LaunchedProcess, error)
	// EnsureInit creates or rediscovers the closed-gate PID1 shim wrapper without starting its workload child.
	EnsureInit(context.Context, InitLaunch) (LaunchedProcess, error)
	// EnsureNamespace captures one namespace identity from the verified referenced wrapper.
	EnsureNamespace(context.Context, NamespaceLaunch) (string, error)
	// PrepareRootfs performs the one-shot pivot only from a catalog-resolved trusted source;
	// every possible failure effect must remain contained by the checkpointed init owner.
	PrepareRootfs(context.Context, RootfsLaunch) (string, error)
	// Inspect action-time verifies one exact process, namespace, or rootfs resource.
	Inspect(context.Context, ResourceReference) (provider.ResourceObservation, error)
	// Remove action-time verifies and removes one exact process, namespace, or rootfs resource.
	Remove(context.Context, ResourceReference) (provider.CleanupObservation, error)
	// Signal action-time verifies a keeper process before delivering a bounded provider signal.
	Signal(context.Context, ResourceReference, provider.Signal) (provider.SignalObservation, error)
	// ResolveProcess returns a runtime-only PID reference whose method reverifies strong identity on every call.
	ResolveProcess(context.Context, ResourceReference) (cgroupv2.ProcessReference, error)
}

// SourceCatalog resolves an opaque prepared-rootfs ID through trusted daemon configuration.
type SourceCatalog interface {
	// Resolve returns one validated rootfs config without treating the opaque ID as a path.
	Resolve(context.Context, provider.OpaqueID) (isolation.RootfsConfig, error)
}

// ShimClient sends one owner-scoped request to an internally derived private control socket.
type ShimClient interface {
	// Do performs one fresh-connection shim control exchange.
	Do(context.Context, string, shim.ControlRequest) (shim.ControlResponse, error)
}

// RequestIDs supplies non-replayed shim request IDs across one daemon session.
type RequestIDs interface {
	// Next returns a bounded opaque request ID for one action.
	Next(action shim.ControlAction) string
}

// validDigest reports whether value is exactly one lowercase SHA-256 digest.
func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

// localIDFor derives the only opaque identity accepted for one owner-scoped resource kind.
func localIDFor(kind ownership.Kind) string {
	return "slim-" + strings.ReplaceAll(string(kind), "_", "-")
}
