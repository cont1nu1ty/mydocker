package slim

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"

	"mydocker/internal/cgroupv2"
	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
	"mydocker/internal/shim"
)

// ensureNamespaceAction opens one namespace only through the retained strong
// wrapper identity, applies its one permitted configuration, and returns a
// digest of the exact nsfs evidence for the next receipt checkpoint.
func (launcher *LinuxShimLauncher) ensureNamespaceAction(ctx context.Context, request NamespaceLaunch) (_ string, resultErr error) {
	if err := launcher.validateNamespaceLaunch(ctx, request); err != nil {
		return "", err
	}
	process, present, err := launcher.restoreActionProcess(ctx, request.Process)
	if err != nil {
		return "", err
	}
	if !present {
		return "", errors.New("namespace owner process is not verified present")
	}
	defer func() { resultErr = errors.Join(resultErr, process.Close()) }()
	handle, err := launcher.namespaces.Open(ctx, process, request.Namespace)
	if err != nil {
		return "", err
	}
	defer func() { resultErr = errors.Join(resultErr, handle.Close()) }()
	evidence, err := launcher.verifyActionNamespace(ctx, process, handle, request.Namespace, "")
	if err != nil {
		return "", err
	}
	if request.Namespace == isolation.NamespaceUTS || request.Namespace == isolation.NamespaceNetwork {
		if err := launcher.namespaces.Configure(ctx, handle, request); err != nil {
			return "", err
		}
	}
	if err := process.Verify(ctx); err != nil {
		return "", err
	}
	if err := launcher.verifyActionCgroup(ctx, request.Process, process); err != nil {
		return "", err
	}
	return ownership.EvidenceDigest(evidence)
}

// prepareRootfsAction sends exactly one semantic PID1 rootfs command after
// re-verifying the init wrapper plus checkpointed PID and mount namespaces.
func (launcher *LinuxShimLauncher) prepareRootfsAction(ctx context.Context, request RootfsLaunch) (_ string, resultErr error) {
	command, err := launcher.validateRootfsLaunch(ctx, request)
	if err != nil {
		return "", err
	}
	process, present, err := launcher.restoreActionProcess(ctx, request.Process)
	if err != nil {
		return "", err
	}
	if !present {
		return "", errors.New("rootfs init process is not verified present")
	}
	defer func() { resultErr = errors.Join(resultErr, process.Close()) }()
	pidEvidence, err := launcher.verifyReferenceNamespace(ctx, process, request.PID, isolation.NamespacePID)
	if err != nil {
		return "", err
	}
	mountEvidence, err := launcher.verifyReferenceNamespace(ctx, process, request.Mount, isolation.NamespaceMount)
	if err != nil {
		return "", err
	}
	command.PIDNamespaceInode = pidEvidence.Inode
	command.MountNamespaceInode = mountEvidence.Inode
	if err := command.Validate(); err != nil {
		return "", err
	}
	config, err := launcher.loadActionRuntimeConfig(request.Process)
	if err != nil {
		return "", err
	}
	if err := process.Verify(ctx); err != nil {
		return "", err
	}
	if err := launcher.verifyActionCgroup(ctx, request.Process, process); err != nil {
		return "", err
	}
	commandDigest, err := ownership.EvidenceDigest(command.Clone())
	if err != nil {
		return "", err
	}
	controlRequest := shim.ControlRequest{
		SchemaVersion: shim.SchemaVersion, RequestID: "rootfs-" + commandDigest,
		Owner: request.Owner, Action: shim.ActionPrepareRootfs, Rootfs: &command,
	}
	return launcher.waitRootfsPreparation(ctx, request.Process, config, process, controlRequest, command)
}

// inspectLauncherAction derives one receipt-scoped observation only after the
// backing exact process and every resource-specific identity are verified.
func (launcher *LinuxShimLauncher) inspectLauncherAction(ctx context.Context, reference ResourceReference) (_ provider.ResourceObservation, resultErr error) {
	if err := launcher.validateActionReference(ctx, reference); err != nil {
		return provider.ResourceObservation{}, err
	}
	process, present, err := launcher.restoreActionProcess(ctx, reference)
	if err != nil {
		return provider.ResourceObservation{}, err
	}
	if !present {
		return launcherAbsentObservation(), nil
	}
	defer func() { resultErr = errors.Join(resultErr, process.Close()) }()
	switch reference.Kind {
	case ownership.KindKeeperProcess, ownership.KindInitProcess:
		if _, err := launcher.inspectActionWrapper(ctx, reference, process); err != nil {
			return provider.ResourceObservation{}, err
		}
		if err := launcher.verifyProcessReceiptEvidence(reference); err != nil {
			return provider.ResourceObservation{}, err
		}
	case ownership.KindUTSNamespace, ownership.KindIPCNamespace, ownership.KindNetworkNamespace,
		ownership.KindPIDNamespace, ownership.KindMountNamespace:
		namespace, err := namespaceTypeForResource(reference.Kind)
		if err != nil {
			return provider.ResourceObservation{}, err
		}
		handle, err := launcher.namespaces.Open(ctx, process, namespace)
		if err != nil {
			return provider.ResourceObservation{}, err
		}
		defer func() { resultErr = errors.Join(resultErr, handle.Close()) }()
		if _, err := launcher.verifyActionNamespace(ctx, process, handle, namespace, reference.LauncherEvidenceSHA256); err != nil {
			return provider.ResourceObservation{}, err
		}
		if reference.Kind == ownership.KindUTSNamespace || reference.Kind == ownership.KindNetworkNamespace {
			request := NamespaceLaunch{Process: reference, Namespace: namespace, Hostname: reference.Hostname,
				NetworkMode: reference.NetworkMode, ConfigurationSHA256: reference.ConfigurationSHA256}
			if err := launcher.namespaces.VerifyConfiguration(ctx, handle, request); err != nil {
				return provider.ResourceObservation{}, err
			}
		}
	case ownership.KindRootfsMount:
		observation, err := launcher.inspectActionWrapper(ctx, reference, process)
		if err != nil {
			return provider.ResourceObservation{}, err
		}
		if observation.Rootfs == nil || observation.Rootfs.EvidenceSHA256 != reference.LauncherEvidenceSHA256 {
			return provider.ResourceObservation{}, errors.New("init wrapper rootfs ACK differs from retained receipt")
		}
	default:
		return provider.ResourceObservation{}, errors.New("launcher cannot inspect this resource kind")
	}
	return launcherPresentObservation(reference.LauncherEvidenceSHA256)
}

// removeLauncherAction tears down a resource only by terminating its exact
// retained wrapper identity, which also releases namespaces and rootfs bound to
// that wrapper; raw cgroup member PIDs are never used as cleanup authority.
func (launcher *LinuxShimLauncher) removeLauncherAction(ctx context.Context, reference ResourceReference) (_ provider.CleanupObservation, resultErr error) {
	if err := launcher.validateActionReference(ctx, reference); err != nil {
		return provider.CleanupObservation{}, err
	}
	process, present, err := launcher.restoreActionProcess(ctx, reference)
	if err != nil {
		return provider.CleanupObservation{}, err
	}
	if !present {
		if err := launcher.cleanupActionSocket(reference); err != nil {
			return provider.CleanupObservation{}, err
		}
		return launcherCleanupObservation(provider.CleanupAlreadyAbsent)
	}
	defer func() { resultErr = errors.Join(resultErr, process.Close()) }()
	terminateErr := launcher.terminateProcess(process)
	absentErr := launcher.waitProcessAbsent(ctx, reference.ProcessEvidence)
	if absentErr != nil {
		return provider.CleanupObservation{}, errors.Join(terminateErr, absentErr)
	}
	if err := launcher.cleanupActionSocket(reference); err != nil {
		return provider.CleanupObservation{}, errors.Join(terminateErr, err)
	}
	return launcherCleanupObservation(provider.CleanupRemoved)
}

// signalLauncherAction delivers an explicitly mapped signal only to a live
// verified keeper process and records the exact process evidence at delivery.
func (launcher *LinuxShimLauncher) signalLauncherAction(ctx context.Context, reference ResourceReference, signal provider.Signal) (_ provider.SignalObservation, resultErr error) {
	if err := launcher.validateActionReference(ctx, reference); err != nil {
		return provider.SignalObservation{}, err
	}
	if reference.Kind != ownership.KindKeeperProcess || !signal.Valid() {
		return provider.SignalObservation{}, errors.New("launcher signal accepts only a verified keeper and supported signal")
	}
	process, present, err := launcher.restoreActionProcess(ctx, reference)
	if err != nil {
		return provider.SignalObservation{}, err
	}
	if !present {
		return provider.SignalObservation{}, provider.ErrProcessNotRunning
	}
	defer func() { resultErr = errors.Join(resultErr, process.Close()) }()
	number, err := launcherSignalNumber(signal)
	if err != nil {
		return provider.SignalObservation{}, err
	}
	if err := process.Signal(ctx, number); err != nil {
		return provider.SignalObservation{}, err
	}
	result := provider.SignalObservation{
		Signal: signal, IdentityEvidenceSHA256: reference.LauncherEvidenceSHA256,
		Delivered: true, DeliveredAt: time.Now().Round(0).UTC(),
	}
	return result, result.Validate()
}

// resolveLauncherProcess returns a descriptor-free ProcessReference that
// restores and verifies its pidfd identity afresh on every manager use.
func (launcher *LinuxShimLauncher) resolveLauncherProcess(ctx context.Context, reference ResourceReference) (cgroupv2.ProcessReference, error) {
	if err := launcher.validateActionReference(ctx, reference); err != nil {
		return nil, err
	}
	process, present, err := launcher.restoreActionProcess(ctx, reference)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, provider.ErrProcessNotRunning
	}
	if err := process.Close(); err != nil {
		return nil, err
	}
	return &launcherVerifiedProcess{launcher: launcher, reference: reference}, nil
}

// launcherVerifiedProcess reopens a short-lived exact pidfd for each cgroup
// membership readback, so no stored numeric PID becomes reusable authority.
type launcherVerifiedProcess struct {
	launcher  *LinuxShimLauncher
	reference ResourceReference
}

// VerifiedPID verifies the retained strong process identity and cgroup scope
// before returning a PID to the cgroup manager for a read-only membership check.
func (reference *launcherVerifiedProcess) VerifiedPID(ctx context.Context) (_ int, resultErr error) {
	if reference == nil || reference.launcher == nil {
		return 0, errors.New("launcher verified process is not configured")
	}
	process, present, err := reference.launcher.restoreActionProcess(ctx, reference.reference)
	if err != nil {
		return 0, err
	}
	if !present {
		return 0, provider.ErrProcessNotRunning
	}
	defer func() { resultErr = errors.Join(resultErr, process.Close()) }()
	return process.VerifiedPID(ctx)
}

// validateNamespaceLaunch checks the process role and canonical configuration
// before opening a namespace or issuing a namespace configuration syscall.
func (launcher *LinuxShimLauncher) validateNamespaceLaunch(ctx context.Context, request NamespaceLaunch) error {
	if err := launcher.validateActionReference(ctx, request.Process); err != nil {
		return err
	}
	expectedKind, err := processKindForNamespace(request.Namespace)
	if err != nil {
		return err
	}
	if request.Process.Kind != expectedKind {
		return errors.New("namespace launch process role differs from namespace ownership")
	}
	switch request.Namespace {
	case isolation.NamespaceUTS:
		expected, err := namespaceConfigurationDigest(request.Namespace, request.Hostname, "")
		if err != nil || request.NetworkMode != "" || request.ConfigurationSHA256 != expected {
			return errors.New("UTS namespace launch configuration differs from canonical request")
		}
	case isolation.NamespaceNetwork:
		expected, err := namespaceConfigurationDigest(request.Namespace, "", request.NetworkMode)
		if err != nil || !request.NetworkMode.Valid() || request.Hostname != "" || request.ConfigurationSHA256 != expected {
			return errors.New("network namespace launch configuration differs from canonical request")
		}
	default:
		if request.Hostname != "" || request.NetworkMode != "" || request.ConfigurationSHA256 != "" {
			return errors.New("unconfigured namespace launch contains Sandbox configuration")
		}
	}
	return nil
}

// validateRootfsLaunch rejects cross-owner or cross-wrapper namespace inputs
// before the deferred PID1 command can make a mount or pivot side effect.
func (launcher *LinuxShimLauncher) validateRootfsLaunch(ctx context.Context, request RootfsLaunch) (shim.RootfsRequest, error) {
	if ctx == nil {
		return shim.RootfsRequest{}, errors.New("rootfs launch context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return shim.RootfsRequest{}, err
	}
	if launcher == nil {
		return shim.RootfsRequest{}, errors.New("Linux shim launcher is not configured")
	}
	if err := request.Owner.Validate(); err != nil {
		return shim.RootfsRequest{}, err
	}
	if err := request.AttemptID.Validate(); err != nil {
		return shim.RootfsRequest{}, err
	}
	if err := request.SourceID.Validate(); err != nil {
		return shim.RootfsRequest{}, err
	}
	if err := request.Source.Validate(); err != nil {
		return shim.RootfsRequest{}, err
	}
	if err := request.Paths.ValidateFor(launcher.runtimeRoot, request.Owner); err != nil {
		return shim.RootfsRequest{}, err
	}
	for _, reference := range []ResourceReference{request.Process, request.PID, request.Mount} {
		if err := launcher.validateActionReference(ctx, reference); err != nil {
			return shim.RootfsRequest{}, err
		}
		if reference.Owner != request.Owner || reference.AttemptID != request.AttemptID || reference.ProcessEvidence != request.Process.ProcessEvidence || reference.SandboxID != request.Process.SandboxID {
			return shim.RootfsRequest{}, errors.New("rootfs references do not share one exact init wrapper scope")
		}
	}
	if request.Process.Kind != ownership.KindInitProcess || request.PID.Kind != ownership.KindPIDNamespace || request.Mount.Kind != ownership.KindMountNamespace {
		return shim.RootfsRequest{}, errors.New("rootfs launch requires init, PID namespace, and mount namespace references")
	}
	if request.Process.Paths != request.Paths || request.PID.Paths != request.Paths || request.Mount.Paths != request.Paths {
		return shim.RootfsRequest{}, errors.New("rootfs launch artifact paths differ from its process owner")
	}
	expectedConfig, err := dnsConfigurationDigest(request.DNS)
	if err != nil {
		return shim.RootfsRequest{}, err
	}
	if request.ConfigurationSHA256 != expectedConfig {
		return shim.RootfsRequest{}, errors.New("rootfs DNS configuration digest differs from retained request")
	}
	return shim.RootfsRequest{SourceID: string(request.SourceID), Source: request.Source, DNS: append([]string(nil), request.DNS...), ConfigurationSHA256: expectedConfig}, nil
}

// validateActionReference verifies the reconstructed receipt-derived reference
// and rejects an action after caller cancellation but before host observation.
func (launcher *LinuxShimLauncher) validateActionReference(ctx context.Context, reference ResourceReference) error {
	if ctx == nil {
		return errors.New("launcher action context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if launcher == nil {
		return errors.New("Linux shim launcher is not configured")
	}
	return reference.Validate(launcher.runtimeRoot)
}

// restoreActionProcess proves exact liveness before restoring a short-lived
// pidfd handle and confirming that it remains in its expected cgroup leaf.
func (launcher *LinuxShimLauncher) restoreActionProcess(ctx context.Context, reference ResourceReference) (launcherProcessHandle, bool, error) {
	if err := launcher.validateActionReference(ctx, reference); err != nil {
		return nil, false, err
	}
	present, err := launcher.processes.Present(ctx, reference.ProcessEvidence)
	if err != nil {
		return nil, false, err
	}
	if !present {
		return nil, false, nil
	}
	process, err := launcher.processes.Restore(ctx, reference.ProcessEvidence)
	if err != nil {
		return nil, false, err
	}
	succeeded := false
	defer func() {
		if !succeeded {
			_ = process.Close()
		}
	}()
	evidence, err := process.Evidence()
	if err != nil {
		return nil, false, err
	}
	if evidence != reference.ProcessEvidence {
		return nil, false, errors.New("restored process evidence differs from retained reference")
	}
	if err := process.Verify(ctx); err != nil {
		return nil, false, err
	}
	if err := launcher.verifyActionCgroup(ctx, reference, process); err != nil {
		return nil, false, err
	}
	succeeded = true
	return process, true, nil
}

// verifyActionCgroup checks read-only membership in the exact keeper or
// Attempt leaf selected by the retained resource kind and identity scope.
func (launcher *LinuxShimLauncher) verifyActionCgroup(ctx context.Context, reference ResourceReference, process cgroupv2.ProcessReference) error {
	switch reference.Kind {
	case ownership.KindKeeperProcess, ownership.KindUTSNamespace, ownership.KindIPCNamespace, ownership.KindNetworkNamespace:
		return launcher.cgroups.ConfirmKeeperProcess(ctx, reference.SandboxID, process)
	case ownership.KindInitProcess, ownership.KindPIDNamespace, ownership.KindMountNamespace, ownership.KindRootfsMount:
		return launcher.cgroups.AttachProcess(ctx, reference.SandboxID, reference.AttemptID, process)
	default:
		return errors.New("resource kind has no verified cgroup ownership")
	}
}

// verifyActionNamespace confirms nsfs type, owner, and inode evidence for a
// live wrapper, optionally matching a receipt-issued namespace digest.
func (launcher *LinuxShimLauncher) verifyActionNamespace(ctx context.Context, process launcherProcessHandle, handle launcherNamespaceHandle, namespace isolation.NamespaceType, expectedDigest string) (isolation.NamespaceEvidence, error) {
	evidence, err := handle.Evidence()
	if err != nil {
		return isolation.NamespaceEvidence{}, err
	}
	processEvidence, err := process.Evidence()
	if err != nil {
		return isolation.NamespaceEvidence{}, err
	}
	if err := evidence.Validate(); err != nil {
		return isolation.NamespaceEvidence{}, err
	}
	if evidence.Type != namespace || evidence.Owner != processEvidence {
		return isolation.NamespaceEvidence{}, errors.New("namespace evidence differs from exact wrapper identity")
	}
	digest, err := ownership.EvidenceDigest(evidence)
	if err != nil {
		return isolation.NamespaceEvidence{}, err
	}
	if expectedDigest != "" && digest != expectedDigest {
		return isolation.NamespaceEvidence{}, errors.New("live namespace evidence differs from retained receipt")
	}
	if err := handle.Verify(ctx); err != nil {
		return isolation.NamespaceEvidence{}, err
	}
	if err := process.Verify(ctx); err != nil {
		return isolation.NamespaceEvidence{}, err
	}
	return evidence, nil
}

// verifyReferenceNamespace opens one namespace solely to compare it to an
// already checkpointed PID or mount receipt, then closes the transient handle.
func (launcher *LinuxShimLauncher) verifyReferenceNamespace(ctx context.Context, process launcherProcessHandle, reference ResourceReference, namespace isolation.NamespaceType) (_ isolation.NamespaceEvidence, resultErr error) {
	handle, err := launcher.namespaces.Open(ctx, process, namespace)
	if err != nil {
		return isolation.NamespaceEvidence{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, handle.Close()) }()
	return launcher.verifyActionNamespace(ctx, process, handle, namespace, reference.LauncherEvidenceSHA256)
}

// inspectActionWrapper authenticates a fresh control peer and validates the
// complete live wrapper scope before callers trust process or rootfs state.
func (launcher *LinuxShimLauncher) inspectActionWrapper(ctx context.Context, reference ResourceReference, process launcherProcessHandle) (_ shim.Observation, resultErr error) {
	config, err := launcher.loadActionRuntimeConfig(reference)
	if err != nil {
		return shim.Observation{}, err
	}
	inspectContext, cancel := boundedLauncherContext(ctx, launcher.readinessTimeout)
	defer cancel()
	request := shim.ControlRequest{SchemaVersion: shim.SchemaVersion, RequestID: "inspect-" + config.WrapperEvidence, Owner: reference.Owner, Action: shim.ActionInspect}
	response, peerPID, err := launcher.control.Exchange(inspectContext, reference.Paths.ControlSocket, request)
	if err != nil {
		if shim.IsCode(err, shim.CodeUnavailable) {
			return shim.Observation{}, provider.MarkObservationUnavailable(err)
		}
		return shim.Observation{}, err
	}
	if err := validateControlResponse(request, response); err != nil {
		return shim.Observation{}, err
	}
	if response.Error != nil {
		if shim.IsCode(response.Error, shim.CodeUnavailable) {
			return shim.Observation{}, provider.MarkObservationUnavailable(response.Error)
		}
		return shim.Observation{}, response.Error
	}
	if peerPID != reference.ProcessEvidence.PID || response.Observation == nil {
		return shim.Observation{}, errors.New("control peer or observation differs from strong wrapper identity")
	}
	if err := validateActionWrapperObservation(reference, config, *response.Observation); err != nil {
		return shim.Observation{}, err
	}
	if err := process.Verify(inspectContext); err != nil {
		return shim.Observation{}, err
	}
	if err := launcher.verifyActionCgroup(inspectContext, reference, process); err != nil {
		return shim.Observation{}, err
	}
	return response.Observation.Clone(), nil
}

// loadActionRuntimeConfig reloads the immutable owner config and binds it to
// the exact receipt role before any control request accepts its wrapper digest.
func (launcher *LinuxShimLauncher) loadActionRuntimeConfig(reference ResourceReference) (shim.RuntimeConfig, error) {
	config, err := shim.LoadRuntimeConfig(reference.Paths.Config)
	if err != nil {
		return shim.RuntimeConfig{}, err
	}
	if config.Owner != reference.Owner || config.SandboxID != reference.SandboxID {
		return shim.RuntimeConfig{}, errors.New("runtime config owner scope differs from receipt")
	}
	mode := shim.ModeKeeper
	if resourceBacksInit(reference.Kind) {
		mode = shim.ModeInit
	}
	if config.Mode != mode {
		return shim.RuntimeConfig{}, errors.New("runtime config mode differs from receipt role")
	}
	if mode == shim.ModeInit && (config.ContainerID != domain.ContainerID(reference.Owner.Target.ID) || config.AttemptID != reference.AttemptID) {
		return shim.RuntimeConfig{}, errors.New("init runtime config Attempt scope differs from receipt")
	}
	evidence, err := shim.RuntimeConfigEvidence(config)
	if err != nil {
		return shim.RuntimeConfig{}, err
	}
	if evidence != config.WrapperEvidence || (reference.WrapperEvidenceSHA256 != "" && reference.WrapperEvidenceSHA256 != evidence) {
		return shim.RuntimeConfig{}, errors.New("runtime config evidence differs from retained wrapper evidence")
	}
	return config, nil
}

// validateActionWrapperObservation verifies all stable keeper or init fields
// while allowing legal init prepared, starting, running, or terminal states.
func validateActionWrapperObservation(reference ResourceReference, config shim.RuntimeConfig, observation shim.Observation) error {
	if err := observation.Validate(); err != nil {
		return err
	}
	if observation.Owner != reference.Owner || observation.SandboxID != reference.SandboxID || observation.WrapperEvidence != config.WrapperEvidence {
		return errors.New("wrapper observation owner or evidence differs from retained config")
	}
	if resourceBacksInit(reference.Kind) {
		if observation.Mode != shim.ModeInit || observation.ContainerID != domain.ContainerID(reference.Owner.Target.ID) || observation.AttemptID != reference.AttemptID {
			return errors.New("init wrapper observation differs from Attempt receipt scope")
		}
		return nil
	}
	if observation.Mode != shim.ModeKeeper || observation.ContainerID != "" || observation.AttemptID != "" || observation.State != shim.StatePrepared || observation.Rootfs != nil {
		return errors.New("keeper wrapper observation differs from Sandbox receipt scope")
	}
	return nil
}

// waitRootfsPreparation retries only temporary control unavailability and
// validates the exact ACK before exposing its evidence to a rootfs receipt.
func (launcher *LinuxShimLauncher) waitRootfsPreparation(ctx context.Context, processReference ResourceReference, config shim.RuntimeConfig, process launcherProcessHandle, request shim.ControlRequest, command shim.RootfsRequest) (string, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	waitContext, cancel := boundedLauncherContext(ctx, launcher.readinessTimeout)
	defer cancel()
	for {
		response, peerPID, exchangeErr := launcher.control.Exchange(waitContext, processReference.Paths.ControlSocket, request)
		if exchangeErr == nil {
			if err := validateControlResponse(request, response); err != nil {
				return "", err
			}
			if response.Error != nil {
				if !shim.IsCode(response.Error, shim.CodeUnavailable) {
					return "", response.Error
				}
			} else {
				if peerPID != processReference.ProcessEvidence.PID || response.Rootfs == nil {
					return "", errors.New("rootfs control peer or ACK differs from exact init identity")
				}
				if err := response.Rootfs.ValidateFor(command); err != nil {
					return "", err
				}
				if err := process.Verify(waitContext); err != nil {
					return "", err
				}
				if err := launcher.verifyActionCgroup(waitContext, processReference, process); err != nil {
					return "", err
				}
				if config.Mode != shim.ModeInit || config.WrapperEvidence != processReference.WrapperEvidenceSHA256 {
					return "", errors.New("rootfs configuration differs from exact init receipt")
				}
				return response.Rootfs.EvidenceSHA256, nil
			}
		} else if !shim.IsCode(exchangeErr, shim.CodeUnavailable) {
			return "", exchangeErr
		}
		if err := waitLauncherPoll(waitContext, launcher.pollInterval); err != nil {
			return "", fmt.Errorf("wait for rootfs preparation ACK: %w", err)
		}
	}
}

// verifyProcessReceiptEvidence confirms a process receipt's launcher digest is
// the canonical hash of the exact serialized ProcessEvidence it restored.
func (launcher *LinuxShimLauncher) verifyProcessReceiptEvidence(reference ResourceReference) error {
	digest, err := ownership.EvidenceDigest(reference.ProcessEvidence)
	if err != nil {
		return err
	}
	if digest != reference.LauncherEvidenceSHA256 {
		return errors.New("process receipt live evidence differs from strong process evidence")
	}
	return nil
}

// cleanupActionSocket removes a stale private endpoint only after the exact
// process evidence was proven absent by the caller's cleanup path.
func (launcher *LinuxShimLauncher) cleanupActionSocket(reference ResourceReference) error {
	store, err := newLaunchStore(reference.Paths, reference.Owner)
	if err != nil {
		return err
	}
	return cleanupStaleControlSocket(store)
}

// resourceBacksInit identifies every Attempt-owned resource whose lifetime is
// bound to the same long-lived PID1 wrapper rather than a Sandbox keeper.
func resourceBacksInit(kind ownership.Kind) bool {
	switch kind {
	case ownership.KindInitProcess, ownership.KindPIDNamespace, ownership.KindMountNamespace, ownership.KindRootfsMount:
		return true
	default:
		return false
	}
}

// processKindForNamespace maps the only supported namespace capture targets
// to their retained keeper or init process receipt kinds.
func processKindForNamespace(namespace isolation.NamespaceType) (ownership.Kind, error) {
	switch namespace {
	case isolation.NamespaceUTS, isolation.NamespaceIPC, isolation.NamespaceNetwork:
		return ownership.KindKeeperProcess, nil
	case isolation.NamespacePID, isolation.NamespaceMount:
		return ownership.KindInitProcess, nil
	default:
		return "", errors.New("unsupported namespace capture target")
	}
}

// namespaceTypeForResource maps one persisted namespace receipt kind to the
// exact nsfs type that must be reopened and compared at action time.
func namespaceTypeForResource(kind ownership.Kind) (isolation.NamespaceType, error) {
	switch kind {
	case ownership.KindUTSNamespace:
		return isolation.NamespaceUTS, nil
	case ownership.KindIPCNamespace:
		return isolation.NamespaceIPC, nil
	case ownership.KindNetworkNamespace:
		return isolation.NamespaceNetwork, nil
	case ownership.KindPIDNamespace:
		return isolation.NamespacePID, nil
	case ownership.KindMountNamespace:
		return isolation.NamespaceMount, nil
	default:
		return "", errors.New("resource kind is not a namespace")
	}
}

// launcherSignalNumber maps the public bounded signal vocabulary to the only
// kernel numbers that an exact keeper pidfd cleanup path may receive.
func launcherSignalNumber(signal provider.Signal) (int, error) {
	switch signal {
	case provider.SignalHUP:
		return int(syscall.SIGHUP), nil
	case provider.SignalINT:
		return int(syscall.SIGINT), nil
	case provider.SignalQUIT:
		return int(syscall.SIGQUIT), nil
	case provider.SignalKILL:
		return int(syscall.SIGKILL), nil
	case provider.SignalTERM:
		return int(syscall.SIGTERM), nil
	case provider.SignalUSR1:
		return int(syscall.SIGUSR1), nil
	case provider.SignalUSR2:
		return int(syscall.SIGUSR2), nil
	default:
		return 0, errors.New("unsupported launcher signal")
	}
}

// launcherPresentObservation constructs the only action-time live result that
// can pass through provider receipt-evidence comparison.
func launcherPresentObservation(evidence string) (provider.ResourceObservation, error) {
	result := provider.ResourceObservation{Presence: provider.PresencePresent, Verified: true, EvidenceSHA256: evidence}
	return result, result.Validate()
}

// launcherAbsentObservation constructs a verified absence result without
// leaking a stale process, namespace, or rootfs identity into cleanup logic.
func launcherAbsentObservation() provider.ResourceObservation {
	return provider.ResourceObservation{Presence: provider.PresenceAbsent, Verified: true}
}

// launcherCleanupObservation records either a removal performed through exact
// owner evidence or an idempotent already-absent result after verification.
func launcherCleanupObservation(disposition provider.CleanupDisposition) (provider.CleanupObservation, error) {
	result := provider.CleanupObservation{Disposition: disposition, After: launcherAbsentObservation()}
	return result, result.Validate()
}
