package slim

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"mydocker/internal/cgroupv2"
	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/ownership"
	providerapi "mydocker/internal/provider"
	"mydocker/internal/shim"
)

// Config wires trusted runtime-root, launcher, prepared-source, and shim-control boundaries.
type Config struct {
	RuntimeRoot string
	Launcher    Launcher
	Sources     SourceCatalog
	Shim        ShimClient
	RequestIDs  RequestIDs
}

// IsolationProvider implements the M2 isolation interface and M3 engine Supervisor over one slim topology.
type IsolationProvider struct {
	runtimeRoot string
	launcher    Launcher
	sources     SourceCatalog
	shim        ShimClient
	requestIDs  RequestIDs
	artifacts   *artifactStore
	resolver    *receiptResolver
}

// lockOwner serializes gate consumption and action-time control with launcher
// recovery for one runtime-root/owner pair, including across provider instances.
func (provider *IsolationProvider) lockOwner(token string) func() {
	lock := sharedOwnerOperationLock(provider.runtimeRoot, token)
	lock.Lock()
	return lock.Unlock
}

// New constructs a provider without performing host mutation or privileged preflight.
func New(config Config) (*IsolationProvider, error) {
	if config.Launcher == nil || config.Sources == nil {
		return nil, errors.New("slim provider requires launcher and prepared-source catalog")
	}
	if config.Shim == nil {
		config.Shim = unixShimClient{}
	}
	if config.RequestIDs == nil {
		prefixBytes := make([]byte, 12)
		if _, err := rand.Read(prefixBytes); err != nil {
			return nil, fmt.Errorf("create shim request session identity: %w", err)
		}
		config.RequestIDs = &sessionRequestIDs{prefix: "slim-" + hex.EncodeToString(prefixBytes)}
	}
	artifacts, err := newArtifactStore(config.RuntimeRoot)
	if err != nil {
		return nil, err
	}
	resolver, err := newReceiptResolver(config.RuntimeRoot)
	if err != nil {
		return nil, err
	}
	return &IsolationProvider{
		runtimeRoot: config.RuntimeRoot, launcher: config.Launcher, sources: config.Sources,
		shim: config.Shim, requestIDs: config.RequestIDs, artifacts: artifacts, resolver: resolver,
	}, nil
}

// ResolveProcess implements the cgroup adapter's public ProcessResolver through launcher-owned strong evidence.
func (provider *IsolationProvider) ResolveProcess(ctx context.Context, receipt ownership.Receipt) (cgroupv2.ProcessReference, error) {
	reference, err := provider.resolver.Resolve(providerapi.OwnedReceiptRequest{Owner: receipt.Owner, Receipt: receipt})
	if err != nil {
		return nil, err
	}
	return provider.launcher.ResolveProcess(ctx, reference)
}

// InspectIsolationCapabilities delegates read-only discovery and verifies every requested feature explicitly.
func (provider *IsolationProvider) InspectIsolationCapabilities(ctx context.Context, requirements providerapi.IsolationRequirements) (providerapi.IsolationCapabilities, error) {
	if err := requirements.Validate(); err != nil {
		return providerapi.IsolationCapabilities{}, err
	}
	capabilities, err := provider.launcher.Preflight(ctx, requirements)
	if err != nil {
		return providerapi.IsolationCapabilities{}, err
	}
	if err := capabilities.Validate(); err != nil {
		return providerapi.IsolationCapabilities{}, err
	}
	if err := isolationCapabilitiesSatisfy(capabilities, requirements); err != nil {
		return providerapi.IsolationCapabilities{}, err
	}
	return capabilities, nil
}

// EnsureKeeperProcess launches or rediscovers one ready minimal keeper and returns owner-bound strong evidence.
func (provider *IsolationProvider) EnsureKeeperProcess(ctx context.Context, request providerapi.KeeperProcessRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	paths, err := provider.paths(request.Owner)
	if err != nil {
		return ownership.Receipt{}, err
	}
	if err := provider.artifacts.ensureOwnerDirectory(request.Owner); err != nil {
		return ownership.Receipt{}, err
	}
	process, err := provider.launcher.EnsureKeeper(ctx, KeeperLaunch{
		Owner: request.Owner, SandboxID: request.SandboxID, Cgroup: request.Cgroup, Paths: paths,
	})
	if err != nil {
		return ownership.Receipt{}, err
	}
	if err := process.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	encodedProcess, err := encodeProcessEvidence(process.ProcessEvidence)
	if err != nil {
		return ownership.Receipt{}, err
	}
	return newSlimReceipt(request.Owner, ownership.KindKeeperProcess, map[string]string{
		launcherEvidenceAttribute: process.IdentityEvidenceSHA256,
		wrapperEvidenceAttribute:  process.WrapperEvidenceSHA256,
		sandboxIDAttribute:        string(request.SandboxID),
		processEvidenceAttribute:  encodedProcess,
	})
}

// EnsureInitProcess launches or rediscovers one closed-gate PID1 wrapper without starting the workload child.
func (provider *IsolationProvider) EnsureInitProcess(ctx context.Context, request providerapi.InitProcessRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	if err := validateSlimReceipt(request.Gate); err != nil {
		return ownership.Receipt{}, fmt.Errorf("init gate receipt: %w", err)
	}
	if err := validateSlimReceipt(request.Streams); err != nil {
		return ownership.Receipt{}, fmt.Errorf("init stream receipt: %w", err)
	}
	if request.Gate.Attributes[attemptIDAttribute] != string(request.AttemptID) || request.Streams.Attributes[attemptIDAttribute] != string(request.AttemptID) {
		return ownership.Receipt{}, errors.New("init gate or stream receipt belongs to another Attempt")
	}
	sandboxNamespaces, err := provider.resolveSandboxNamespaces(request.SandboxID, request.SandboxNamespaces)
	if err != nil {
		return ownership.Receipt{}, err
	}
	paths, err := provider.paths(request.Owner)
	if err != nil {
		return ownership.Receipt{}, err
	}
	process, err := provider.launcher.EnsureInit(ctx, InitLaunch{
		Owner: request.Owner, SandboxID: request.SandboxID, AttemptID: request.AttemptID,
		Cgroup: request.Cgroup, Gate: request.Gate, Streams: request.Streams,
		SandboxNamespaces: sandboxNamespaces, Process: request.Process.Clone(), Paths: paths,
	})
	if err != nil {
		return ownership.Receipt{}, err
	}
	if err := process.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	encodedProcess, err := encodeProcessEvidence(process.ProcessEvidence)
	if err != nil {
		return ownership.Receipt{}, err
	}
	return newSlimReceipt(request.Owner, ownership.KindInitProcess, map[string]string{
		launcherEvidenceAttribute: process.IdentityEvidenceSHA256,
		wrapperEvidenceAttribute:  process.WrapperEvidenceSHA256,
		sandboxIDAttribute:        string(request.SandboxID),
		attemptIDAttribute:        string(request.AttemptID),
		processEvidenceAttribute:  encodedProcess,
	})
}

// InspectProcess action-time verifies one exact keeper or init wrapper through the launcher strong identity.
func (provider *IsolationProvider) InspectProcess(ctx context.Context, request providerapi.OwnedReceiptRequest) (providerapi.ResourceObservation, error) {
	reference, err := provider.resolver.Resolve(request)
	if err != nil {
		return providerapi.ResourceObservation{}, err
	}
	return provider.inspectLauncher(ctx, request.Receipt, reference)
}

// RemoveProcess action-time verifies and removes the exact wrapper, never a PID supplied by persistence.
func (provider *IsolationProvider) RemoveProcess(ctx context.Context, request providerapi.OwnedReceiptRequest) (providerapi.CleanupObservation, error) {
	if err := request.ValidateFor(ownership.ProviderLinux, ownership.KindKeeperProcess, ownership.KindInitProcess); err != nil {
		return providerapi.CleanupObservation{}, err
	}
	reference, err := provider.resolver.Resolve(request)
	if err != nil {
		return providerapi.CleanupObservation{}, err
	}
	return provider.removeLauncher(ctx, reference)
}

// EnsureNamespace captures one namespace from the already verified keeper or init wrapper.
func (provider *IsolationProvider) EnsureNamespace(ctx context.Context, request providerapi.NamespaceRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	hostname := request.Hostname
	networkMode := request.NetworkMode
	configurationSHA256 := ""
	if request.Namespace == isolation.NamespaceUTS || request.Namespace == isolation.NamespaceNetwork {
		if request.Namespace == isolation.NamespaceNetwork {
			var err error
			networkMode, err = networkMode.Canonical()
			if err != nil {
				return ownership.Receipt{}, err
			}
		}
		var err error
		configurationSHA256, err = namespaceConfigurationDigest(request.Namespace, hostname, networkMode)
		if err != nil {
			return ownership.Receipt{}, err
		}
	}
	process, err := provider.resolver.Resolve(providerapi.OwnedReceiptRequest{Owner: request.Owner, Receipt: request.Process})
	if err != nil {
		return ownership.Receipt{}, err
	}
	evidence, err := provider.launcher.EnsureNamespace(ctx, NamespaceLaunch{
		Process: process, Namespace: request.Namespace, Hostname: hostname,
		NetworkMode: networkMode, ConfigurationSHA256: configurationSHA256,
	})
	if err != nil {
		return ownership.Receipt{}, err
	}
	if !validDigest(evidence) {
		return ownership.Receipt{}, errors.New("launcher namespace evidence is not a SHA-256 digest")
	}
	kind, err := request.ReceiptKind()
	if err != nil {
		return ownership.Receipt{}, err
	}
	attributes := map[string]string{
		launcherEvidenceAttribute: evidence,
		processEvidenceAttribute:  request.Process.Attributes[processEvidenceAttribute],
	}
	if kind == ownership.KindUTSNamespace || kind == ownership.KindIPCNamespace || kind == ownership.KindNetworkNamespace {
		attributes[sandboxIDAttribute] = string(process.SandboxID)
	} else {
		attributes[sandboxIDAttribute] = string(process.SandboxID)
		attributes[attemptIDAttribute] = string(process.AttemptID)
	}
	if kind == ownership.KindUTSNamespace {
		attributes[configurationEvidenceAttribute] = configurationSHA256
		attributes[hostnameAttribute] = hostname
	}
	if kind == ownership.KindNetworkNamespace {
		attributes[configurationEvidenceAttribute] = configurationSHA256
		attributes[networkModeAttribute] = string(networkMode)
	}
	return newSlimReceipt(request.Owner, kind, attributes)
}

// InspectNamespace action-time verifies one owner-bound namespace receipt through its launcher evidence.
func (provider *IsolationProvider) InspectNamespace(ctx context.Context, request providerapi.OwnedReceiptRequest) (providerapi.ResourceObservation, error) {
	if err := request.ValidateFor(ownership.ProviderLinux,
		ownership.KindUTSNamespace, ownership.KindIPCNamespace, ownership.KindNetworkNamespace,
		ownership.KindPIDNamespace, ownership.KindMountNamespace); err != nil {
		return providerapi.ResourceObservation{}, err
	}
	reference, err := provider.resolver.resolve(request.Receipt)
	if err != nil {
		return providerapi.ResourceObservation{}, err
	}
	return provider.inspectLauncher(ctx, request.Receipt, reference)
}

// RemoveNamespace closes launcher ownership of one exact namespace and proves verified absence.
func (provider *IsolationProvider) RemoveNamespace(ctx context.Context, request providerapi.OwnedReceiptRequest) (providerapi.CleanupObservation, error) {
	if _, err := provider.InspectNamespace(ctx, request); err != nil {
		return providerapi.CleanupObservation{}, err
	}
	reference, err := provider.resolver.resolve(request.Receipt)
	if err != nil {
		return providerapi.CleanupObservation{}, err
	}
	return provider.removeLauncher(ctx, reference)
}

// EnsureRootfs resolves the opaque source catalog entry and delegates the verified PID1 one-shot pivot.
func (provider *IsolationProvider) EnsureRootfs(ctx context.Context, request providerapi.RootfsRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, providerapi.MarkNoEffect(err)
	}
	configurationSHA256, err := dnsConfigurationDigest(request.DNS)
	if err != nil {
		return ownership.Receipt{}, providerapi.MarkNoEffect(err)
	}
	process, err := provider.resolver.Resolve(providerapi.OwnedReceiptRequest{Owner: request.Owner, Receipt: request.Process})
	if err != nil {
		return ownership.Receipt{}, providerapi.MarkNoEffect(err)
	}
	for _, receipt := range []ownership.Receipt{request.PID, request.Mount} {
		if err := validateSlimReceipt(receipt); err != nil {
			return ownership.Receipt{}, providerapi.MarkNoEffect(err)
		}
		if receipt.Attributes[attemptIDAttribute] != string(request.AttemptID) {
			return ownership.Receipt{}, providerapi.MarkNoEffect(errors.New("rootfs namespace receipt belongs to another Attempt"))
		}
	}
	if process.AttemptID != request.AttemptID {
		return ownership.Receipt{}, providerapi.MarkNoEffect(errors.New("rootfs init receipt belongs to another Attempt"))
	}
	pidReference, err := provider.resolver.resolve(request.PID)
	if err != nil {
		return ownership.Receipt{}, providerapi.MarkNoEffect(err)
	}
	mountReference, err := provider.resolver.resolve(request.Mount)
	if err != nil {
		return ownership.Receipt{}, providerapi.MarkNoEffect(err)
	}
	source, err := provider.sources.Resolve(ctx, request.SourceID)
	if err != nil {
		return ownership.Receipt{}, providerapi.MarkNoEffect(err)
	}
	paths, err := provider.paths(request.Owner)
	if err != nil {
		return ownership.Receipt{}, providerapi.MarkNoEffect(err)
	}
	evidence, err := provider.launcher.PrepareRootfs(ctx, RootfsLaunch{
		Owner: request.Owner, AttemptID: request.AttemptID, Process: process,
		PID: pidReference, Mount: mountReference, SourceID: request.SourceID, Source: source,
		DNS: append([]string(nil), request.DNS...), ConfigurationSHA256: configurationSHA256, Paths: paths,
	})
	if err != nil {
		if providerapi.IsNoEffect(err) {
			return ownership.Receipt{}, err
		}
		return ownership.Receipt{}, providerapi.MarkRollbackRequired(err)
	}
	if !validDigest(evidence) {
		return ownership.Receipt{}, providerapi.MarkRollbackRequired(errors.New("launcher rootfs evidence is not a SHA-256 digest"))
	}
	receipt, err := newSlimReceipt(request.Owner, ownership.KindRootfsMount, map[string]string{
		launcherEvidenceAttribute: evidence, sandboxIDAttribute: string(process.SandboxID), attemptIDAttribute: string(request.AttemptID), "source_id": string(request.SourceID),
		configurationEvidenceAttribute: configurationSHA256,
		processEvidenceAttribute:       request.Process.Attributes[processEvidenceAttribute],
	})
	if err != nil {
		return ownership.Receipt{}, providerapi.MarkRollbackRequired(err)
	}
	return receipt, nil
}

// encodeProcessEvidence produces the canonical bounded receipt diagnostic used
// to restore a pidfd after daemon restart; the numeric PID alone never authorizes an action.
func encodeProcessEvidence(evidence isolation.ProcessEvidence) (string, error) {
	if err := evidence.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return "", fmt.Errorf("encode process evidence: %w", err)
	}
	if len(encoded) > 1024 {
		return "", errors.New("process evidence exceeds receipt attribute limit")
	}
	return string(encoded), nil
}

// namespaceConfigurationDigest gives UTS hostname and network mode one canonical
// fingerprint that remains stable across transport response loss and provider retry.
func namespaceConfigurationDigest(namespace isolation.NamespaceType, hostname string, networkMode providerapi.SandboxNetworkMode) (string, error) {
	return ownership.EvidenceDigest(struct {
		Namespace   isolation.NamespaceType        `json:"namespace"`
		Hostname    string                         `json:"hostname,omitempty"`
		NetworkMode providerapi.SandboxNetworkMode `json:"network_mode,omitempty"`
	}{Namespace: namespace, Hostname: hostname, NetworkMode: networkMode})
}

// dnsConfigurationDigest binds the ordered retained Sandbox DNS input to one
// Attempt rootfs preparation without exposing caller-controlled host paths.
func dnsConfigurationDigest(servers []string) (string, error) {
	return ownership.EvidenceDigest(struct {
		DNS []string `json:"dns"`
	}{DNS: append([]string(nil), servers...)})
}

// InspectRootfs action-time verifies one exact rootfs view without accepting a caller path.
func (provider *IsolationProvider) InspectRootfs(ctx context.Context, request providerapi.OwnedReceiptRequest) (providerapi.ResourceObservation, error) {
	if err := request.ValidateFor(ownership.ProviderLinux, ownership.KindRootfsMount); err != nil {
		return providerapi.ResourceObservation{}, err
	}
	reference, err := provider.resolver.resolve(request.Receipt)
	if err != nil {
		return providerapi.ResourceObservation{}, err
	}
	return provider.inspectLauncher(ctx, request.Receipt, reference)
}

// RemoveRootfs removes only the exact launcher-verified Attempt rootfs view.
func (provider *IsolationProvider) RemoveRootfs(ctx context.Context, request providerapi.OwnedReceiptRequest) (providerapi.CleanupObservation, error) {
	if err := request.ValidateFor(ownership.ProviderLinux, ownership.KindRootfsMount); err != nil {
		return providerapi.CleanupObservation{}, err
	}
	reference, err := provider.resolver.resolve(request.Receipt)
	if err != nil {
		return providerapi.CleanupObservation{}, err
	}
	return provider.removeLauncher(ctx, reference)
}

// EnsureStartGate durably creates or rediscovers the closed owner-scoped logical shim gate.
func (provider *IsolationProvider) EnsureStartGate(ctx context.Context, request providerapi.AttemptResourceRequest) (ownership.Receipt, error) {
	if err := validateContext(ctx); err != nil {
		return ownership.Receipt{}, err
	}
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	unlockOwner := provider.lockOwner(request.Owner.Token)
	defer unlockOwner()
	receipt, err := newSlimReceipt(request.Owner, ownership.KindStartGate, map[string]string{attemptIDAttribute: string(request.AttemptID)})
	if err != nil {
		return ownership.Receipt{}, err
	}
	if _, err := provider.artifacts.Ensure(request.Owner, receipt.Kind, receipt.EvidenceSHA256, artifactStateClosed); err != nil {
		return ownership.Receipt{}, err
	}
	return receipt, nil
}

// InspectStartGate verifies one exact durable gate descriptor without opening it.
func (provider *IsolationProvider) InspectStartGate(ctx context.Context, request providerapi.OwnedReceiptRequest) (providerapi.ResourceObservation, error) {
	if err := validateContext(ctx); err != nil {
		return providerapi.ResourceObservation{}, err
	}
	return provider.inspectArtifact(request, ownership.KindStartGate)
}

// RemoveStartGate removes one exact gate descriptor after process cleanup and proves absence.
func (provider *IsolationProvider) RemoveStartGate(ctx context.Context, request providerapi.OwnedReceiptRequest) (providerapi.CleanupObservation, error) {
	if err := validateContext(ctx); err != nil {
		return providerapi.CleanupObservation{}, err
	}
	if err := request.ValidateFor(ownership.ProviderLinux, ownership.KindStartGate); err != nil {
		return providerapi.CleanupObservation{}, err
	}
	unlockOwner := provider.lockOwner(request.Owner.Token)
	defer unlockOwner()
	return provider.removeArtifact(request, ownership.KindStartGate)
}

// EnsureStreams durably creates opaque stdout and stderr references before init launch.
func (provider *IsolationProvider) EnsureStreams(ctx context.Context, request providerapi.AttemptResourceRequest) (ownership.Receipt, error) {
	if err := validateContext(ctx); err != nil {
		return ownership.Receipt{}, err
	}
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	localID := localIDFor(ownership.KindStreams)
	receipt, err := newSlimReceipt(request.Owner, ownership.KindStreams, map[string]string{
		attemptIDAttribute: string(request.AttemptID), "stdout": localID + ":stdout", "stderr": localID + ":stderr",
	})
	if err != nil {
		return ownership.Receipt{}, err
	}
	if _, err := provider.artifacts.Ensure(request.Owner, receipt.Kind, receipt.EvidenceSHA256, artifactStateReady); err != nil {
		return ownership.Receipt{}, err
	}
	return receipt, nil
}

// InspectStreams verifies one exact durable stream descriptor.
func (provider *IsolationProvider) InspectStreams(ctx context.Context, request providerapi.OwnedReceiptRequest) (providerapi.ResourceObservation, error) {
	if err := validateContext(ctx); err != nil {
		return providerapi.ResourceObservation{}, err
	}
	return provider.inspectArtifact(request, ownership.KindStreams)
}

// RemoveStreams removes one exact stream descriptor without interpreting its opaque API references as paths.
func (provider *IsolationProvider) RemoveStreams(ctx context.Context, request providerapi.OwnedReceiptRequest) (providerapi.CleanupObservation, error) {
	if err := validateContext(ctx); err != nil {
		return providerapi.CleanupObservation{}, err
	}
	return provider.removeArtifact(request, ownership.KindStreams)
}

// ReleaseStartGate consumes the shim gate only after exact rootfs and attachment validation.
func (provider *IsolationProvider) ReleaseStartGate(ctx context.Context, request providerapi.ReleaseGateRequest) (providerapi.ResourceObservation, error) {
	if err := request.Validate(); err != nil {
		return providerapi.ResourceObservation{}, err
	}
	unlockOwner := provider.lockOwner(request.Owner.Token)
	defer unlockOwner()
	for _, receipt := range []ownership.Receipt{request.Gate, request.Process, request.Rootfs} {
		if err := validateSlimReceipt(receipt); err != nil {
			return providerapi.ResourceObservation{}, err
		}
	}
	attemptID := request.Process.Attributes[attemptIDAttribute]
	if attemptID == "" || request.Gate.Attributes[attemptIDAttribute] != attemptID || request.Rootfs.Attributes[attemptIDAttribute] != attemptID {
		return providerapi.ResourceObservation{}, errors.New("release receipts do not belong to one Attempt")
	}
	sandboxID := request.Process.Attributes[sandboxIDAttribute]
	if sandboxID == "" || request.Rootfs.Attributes[sandboxIDAttribute] != sandboxID ||
		request.Rootfs.Attributes[processEvidenceAttribute] != request.Process.Attributes[processEvidenceAttribute] {
		return providerapi.ResourceObservation{}, errors.New("release rootfs does not belong to the exact init process")
	}
	cgroupScope, err := validateCgroupReceipt(providerapi.OwnedReceiptRequest{Owner: request.Owner, Receipt: request.Cgroup}, ownership.KindAttemptCgroup)
	if err != nil {
		return providerapi.ResourceObservation{}, fmt.Errorf("release Attempt cgroup: %w", err)
	}
	if string(cgroupScope.sandboxID) != sandboxID || string(cgroupScope.attemptID) != attemptID {
		return providerapi.ResourceObservation{}, errors.New("release cgroup does not belong to the exact Sandbox and Attempt")
	}
	record, found, err := provider.artifacts.Read(request.Owner, ownership.KindStartGate)
	if err != nil {
		return providerapi.ResourceObservation{}, err
	}
	if !found || record.ReceiptEvidence != request.Gate.EvidenceSHA256 {
		return providerapi.ResourceObservation{}, errors.New("owned start gate is absent")
	}
	process, err := provider.resolver.Resolve(providerapi.OwnedReceiptRequest{Owner: request.Owner, Receipt: request.Process})
	if err != nil {
		return providerapi.ResourceObservation{}, err
	}
	if record.State == artifactStateReleased {
		if err := provider.artifacts.ConfirmState(request.Owner, ownership.KindStartGate, request.Gate.EvidenceSHA256, artifactStateReleased); err != nil {
			return providerapi.ResourceObservation{}, err
		}
		observation, err := provider.inspectAttemptShim(ctx, process)
		if err != nil {
			return providerapi.ResourceObservation{}, err
		}
		if observation.State == shim.StatePrepared {
			return providerapi.ResourceObservation{}, errors.New("released start gate conflicts with prepared wrapper state")
		}
		return presentObservation(request.Gate), nil
	}
	releaseRequired := record.State == artifactStateClosed
	if releaseRequired {
		if err := provider.artifacts.Transition(request.Owner, ownership.KindStartGate, request.Gate.EvidenceSHA256, artifactStateConsuming); err != nil {
			return providerapi.ResourceObservation{}, fmt.Errorf("persist start-gate consumption intent: %w", err)
		}
	} else if record.State == artifactStateConsuming {
		if err := provider.artifacts.ConfirmState(request.Owner, ownership.KindStartGate, request.Gate.EvidenceSHA256, artifactStateConsuming); err != nil {
			return providerapi.ResourceObservation{}, err
		}
		observation, err := provider.inspectAttemptShim(ctx, process)
		if err != nil {
			return providerapi.ResourceObservation{}, err
		}
		switch observation.State {
		case shim.StatePrepared:
			releaseRequired = true
		case shim.StateStarting, shim.StateRunning, shim.StateTerminal:
			releaseRequired = false
		default:
			return providerapi.ResourceObservation{}, fmt.Errorf("unsupported shim state %q while recovering start-gate consumption", observation.State)
		}
	} else {
		return providerapi.ResourceObservation{}, fmt.Errorf("unsupported start-gate state %q", record.State)
	}
	if !releaseRequired {
		if err := provider.artifacts.Transition(request.Owner, ownership.KindStartGate, request.Gate.EvidenceSHA256, artifactStateReleased); err != nil {
			return providerapi.ResourceObservation{}, err
		}
		return presentObservation(request.Gate), nil
	}
	response, err := provider.doShim(ctx, process, shim.ActionRelease, "")
	if err != nil {
		return providerapi.ResourceObservation{}, err
	}
	if response.Error != nil {
		if response.Error.Code != shim.CodeAlreadyReleased {
			return providerapi.ResourceObservation{}, response.Error
		}
		observation, err := provider.inspectAttemptShim(ctx, process)
		if err != nil {
			return providerapi.ResourceObservation{}, err
		}
		if observation.State == shim.StatePrepared {
			return providerapi.ResourceObservation{}, errors.New("consumed start gate conflicts with prepared wrapper state")
		}
	} else {
		if response.Observation == nil {
			return providerapi.ResourceObservation{}, errors.New("shim release returned no observation")
		}
		if err := provider.validateAttemptObservation(process, *response.Observation); err != nil {
			return providerapi.ResourceObservation{}, err
		}
		if response.Observation.State != shim.StateStarting && response.Observation.State != shim.StateRunning && response.Observation.State != shim.StateTerminal {
			return providerapi.ResourceObservation{}, errors.New("shim release did not consume the gate")
		}
	}
	if err := provider.artifacts.Transition(request.Owner, ownership.KindStartGate, request.Gate.EvidenceSHA256, artifactStateReleased); err != nil {
		return providerapi.ResourceObservation{}, err
	}
	return presentObservation(request.Gate), nil
}

// SignalVerified routes init workload signals through exact shim mutation replay, retaining its first delivery time, and routes keeper signals through Launcher verification.
func (provider *IsolationProvider) SignalVerified(ctx context.Context, request providerapi.SignalRequest) (providerapi.SignalObservation, error) {
	if err := request.Validate(); err != nil {
		return providerapi.SignalObservation{}, err
	}
	unlockOwner := provider.lockOwner(request.Owner.Token)
	defer unlockOwner()
	reference, err := provider.resolver.Resolve(providerapi.OwnedReceiptRequest{Owner: request.Owner, Receipt: request.Process})
	if err != nil {
		return providerapi.SignalObservation{}, err
	}
	if request.Process.Kind == ownership.KindKeeperProcess {
		observation, err := provider.launcher.Signal(ctx, reference, request.Signal)
		if err != nil {
			return providerapi.SignalObservation{}, err
		}
		if err := observation.Validate(); err != nil {
			return providerapi.SignalObservation{}, err
		}
		if observation.Signal != request.Signal {
			return providerapi.SignalObservation{}, errors.New("launcher signal observation differs from request")
		}
		return observation, nil
	}
	requestID, err := deterministicSignalRequestID(request)
	if err != nil {
		return providerapi.SignalObservation{}, err
	}
	response, err := provider.doShimWithID(ctx, reference, shim.ActionSignal, shim.Signal(request.Signal), requestID)
	if err != nil {
		if shim.IsCode(err, shim.CodeUnavailable) {
			return providerapi.SignalObservation{}, providerapi.MarkObservationUnavailable(err)
		}
		return providerapi.SignalObservation{}, err
	}
	if response.Error != nil {
		switch response.Error.Code {
		case shim.CodeNotRunning:
			return providerapi.SignalObservation{}, fmt.Errorf("%w: %s", providerapi.ErrProcessNotRunning, response.Error)
		case shim.CodeUnavailable:
			return providerapi.SignalObservation{}, providerapi.MarkObservationUnavailable(response.Error)
		}
		return providerapi.SignalObservation{}, response.Error
	}
	if response.Delivery == nil {
		return providerapi.SignalObservation{}, errors.New("shim signal returned no delivery evidence")
	}
	if err := response.Delivery.Validate(); err != nil {
		return providerapi.SignalObservation{}, err
	}
	if response.Delivery.Signal != shim.Signal(request.Signal) {
		return providerapi.SignalObservation{}, errors.New("shim signal delivery differs from request")
	}
	observation := providerapi.SignalObservation{
		Signal: request.Signal, IdentityEvidenceSHA256: response.Delivery.EvidenceSHA256,
		Delivered: true, DeliveredAt: response.Delivery.DeliveredAt,
	}
	return observation, observation.Validate()
}

// inspectLauncher maps exact live launcher evidence to stable receipt-scoped
// observations while preserving typed temporary or permanent launcher errors unchanged.
func (provider *IsolationProvider) inspectLauncher(ctx context.Context, receipt ownership.Receipt, reference ResourceReference) (providerapi.ResourceObservation, error) {
	observation, err := provider.launcher.Inspect(ctx, reference)
	if err != nil {
		return providerapi.ResourceObservation{}, err
	}
	if err := observation.Validate(); err != nil {
		return providerapi.ResourceObservation{}, err
	}
	if observation.Presence == providerapi.PresencePresent {
		if observation.EvidenceSHA256 != reference.LauncherEvidenceSHA256 {
			return providerapi.ResourceObservation{}, errors.New("launcher live evidence differs from receipt")
		}
		return presentObservation(receipt), nil
	}
	return observation, nil
}

// removeLauncher validates the launcher's verified-absence cleanup result.
func (provider *IsolationProvider) removeLauncher(ctx context.Context, reference ResourceReference) (providerapi.CleanupObservation, error) {
	observation, err := provider.launcher.Remove(ctx, reference)
	if err != nil {
		return providerapi.CleanupObservation{}, err
	}
	if err := observation.Validate(); err != nil {
		return providerapi.CleanupObservation{}, err
	}
	return observation, nil
}

// inspectArtifact verifies exact receipt evidence against a checksummed deterministic descriptor.
func (provider *IsolationProvider) inspectArtifact(request providerapi.OwnedReceiptRequest, kind ownership.Kind) (providerapi.ResourceObservation, error) {
	if err := request.ValidateFor(ownership.ProviderLinux, kind); err != nil {
		return providerapi.ResourceObservation{}, err
	}
	if err := validateSlimReceipt(request.Receipt); err != nil {
		return providerapi.ResourceObservation{}, err
	}
	record, found, err := provider.artifacts.Read(request.Owner, kind)
	if err != nil {
		return providerapi.ResourceObservation{}, err
	}
	if !found {
		return absentObservation(), nil
	}
	if record.ReceiptEvidence != request.Receipt.EvidenceSHA256 {
		return providerapi.ResourceObservation{}, ErrReceiptMismatch
	}
	return presentObservation(request.Receipt), nil
}

// removeArtifact deletes one exact descriptor and returns a verified-absence cleanup observation.
func (provider *IsolationProvider) removeArtifact(request providerapi.OwnedReceiptRequest, kind ownership.Kind) (providerapi.CleanupObservation, error) {
	if err := request.ValidateFor(ownership.ProviderLinux, kind); err != nil {
		return providerapi.CleanupObservation{}, err
	}
	if err := validateSlimReceipt(request.Receipt); err != nil {
		return providerapi.CleanupObservation{}, err
	}
	disposition, err := provider.artifacts.Remove(request.Owner, kind, request.Receipt.EvidenceSHA256)
	if err != nil {
		return providerapi.CleanupObservation{}, err
	}
	observation := providerapi.CleanupObservation{Disposition: disposition, After: absentObservation()}
	return observation, observation.Validate()
}

// presentObservation exposes stable receipt-scoped evidence only after a live or artifact readback succeeded.
func presentObservation(receipt ownership.Receipt) providerapi.ResourceObservation {
	return providerapi.ResourceObservation{Presence: providerapi.PresencePresent, Verified: true, EvidenceSHA256: receipt.EvidenceSHA256}
}

// absentObservation is the only verified-absence observation emitted by the slim provider.
func absentObservation() providerapi.ResourceObservation {
	return providerapi.ResourceObservation{Presence: providerapi.PresenceAbsent, Verified: true}
}

// paths returns the only artifact path set accepted by launcher and shim boundaries.
func (provider *IsolationProvider) paths(owner ownership.OwnerKey) (ArtifactPaths, error) {
	return deriveArtifactPaths(provider.runtimeRoot, owner)
}

// resolveSandboxNamespaces converts adopted receipt metadata to exact launcher references without passing receipt text as authority.
func (provider *IsolationProvider) resolveSandboxNamespaces(sandboxID domain.SandboxID, receipts providerapi.SandboxNamespaces) (SandboxNamespaceReferences, error) {
	resolved := make([]ResourceReference, 0, 3)
	for _, receipt := range []ownership.Receipt{receipts.UTS, receipts.IPC, receipts.Network} {
		reference, err := provider.resolver.resolve(receipt)
		if err != nil {
			return SandboxNamespaceReferences{}, err
		}
		resolved = append(resolved, reference)
	}
	result := SandboxNamespaceReferences{UTS: resolved[0], IPC: resolved[1], Network: resolved[2]}
	if err := result.ValidateFor(provider.runtimeRoot, sandboxID); err != nil {
		return SandboxNamespaceReferences{}, err
	}
	return result, nil
}

// isolationCapabilitiesSatisfy checks the requested feature subset without inventing cgroup facts.
func isolationCapabilitiesSatisfy(capabilities providerapi.IsolationCapabilities, requirements providerapi.IsolationRequirements) error {
	checks := []struct {
		name      string
		required  bool
		available bool
	}{
		{"rootful", requirements.Rootful, capabilities.Rootful}, {"pidfd", requirements.PIDFD, capabilities.PIDFD},
		{"pivot_root", requirements.PivotRoot, capabilities.PivotRoot}, {"start_gate", requirements.StartGate, capabilities.StartGate},
		{"streams", requirements.Streams, capabilities.Streams},
	}
	for _, check := range checks {
		if check.required && !check.available {
			return fmt.Errorf("slim launcher is missing %s capability", check.name)
		}
	}
	available := make(map[isolation.NamespaceType]bool, len(capabilities.Namespaces))
	for _, namespace := range capabilities.Namespaces {
		available[namespace] = true
	}
	for _, namespace := range requirements.Namespaces {
		if !available[namespace] {
			return fmt.Errorf("slim launcher is missing %s namespace", namespace)
		}
	}
	return nil
}

// validateContext rejects nil or cancelled calls before any slim artifact or launcher side effect.
func validateContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("slim provider context must not be nil")
	}
	return ctx.Err()
}

// deterministicSignalRequestID binds retries to one durable Kill operation step while leaving content changes detectable by shim replay validation.
func deterministicSignalRequestID(request providerapi.SignalRequest) (string, error) {
	digest, err := ownership.EvidenceDigest(struct {
		ActionOperationID string                 `json:"action_operation_id"`
		Step              providerapi.SignalStep `json:"step"`
		Action            string                 `json:"action"`
	}{string(request.ActionOperationID), request.Step, string(shim.ActionSignal)})
	if err != nil {
		return "", err
	}
	return "signal-" + digest, nil
}
