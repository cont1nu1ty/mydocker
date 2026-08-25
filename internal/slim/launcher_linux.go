package slim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"mydocker/internal/cgroupv2"
	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
	"mydocker/internal/shim"
)

const (
	defaultLauncherReadinessTimeout = 10 * time.Second
	defaultLauncherCleanupTimeout   = 10 * time.Second
	defaultLauncherPollInterval     = 25 * time.Millisecond
)

// LinuxShimLauncher composes durable owner artifacts, clone-time cgroup
// placement, pidfds, authenticated shim control, and action-time recovery.
type LinuxShimLauncher struct {
	runtimeRoot      string
	executable       string
	cgroups          launcherCgroupManager
	factory          ProcessFactory
	processes        launcherProcessRuntime
	namespaces       launcherNamespaceRuntime
	control          launcherControlClient
	host             launcherHost
	readinessTimeout time.Duration
	cleanupTimeout   time.Duration
	pollInterval     time.Duration
}

// linuxShimLauncherConfig exposes production seams only to package tests; all
// dependencies preserve the same no-bare-PID and no-process-migration contracts.
type linuxShimLauncherConfig struct {
	runtimeRoot      string
	executable       string
	cgroups          launcherCgroupManager
	factory          ProcessFactory
	processes        launcherProcessRuntime
	namespaces       launcherNamespaceRuntime
	control          launcherControlClient
	host             launcherHost
	readinessTimeout time.Duration
	cleanupTimeout   time.Duration
	pollInterval     time.Duration
}

// NewLinuxShimLauncher constructs production Linux dependencies without
// probing the host, opening cgroups, or creating runtime artifacts.
func NewLinuxShimLauncher(runtimeRoot, executable string, manager *cgroupv2.Manager) (*LinuxShimLauncher, error) {
	if manager == nil {
		return nil, errors.New("Linux shim launcher requires a cgroup v2 manager")
	}
	return newLinuxShimLauncher(linuxShimLauncherConfig{
		runtimeRoot: runtimeRoot, executable: executable, cgroups: manager,
		factory: OSProcessFactory{}, processes: newSystemProcessRuntime(),
		namespaces: newSystemNamespaceRuntime(), control: systemControlClient{}, host: systemLauncherHost{},
	})
}

// newLinuxShimLauncher validates dependency and timing configuration without
// performing host probes or any process, namespace, filesystem, or cgroup mutation.
func newLinuxShimLauncher(config linuxShimLauncherConfig) (*LinuxShimLauncher, error) {
	if !cleanAbsoluteNonRoot(config.runtimeRoot) {
		return nil, errors.New("Linux shim runtime root must be a clean absolute non-root path")
	}
	if !cleanAbsoluteNonRoot(config.executable) {
		return nil, errors.New("Linux shim executable must be a clean absolute non-root path")
	}
	if config.cgroups == nil || config.factory == nil || config.processes == nil || config.namespaces == nil || config.control == nil || config.host == nil {
		return nil, errors.New("Linux shim launcher dependencies must be complete")
	}
	if !cleanAbsoluteNonRoot(config.cgroups.Root()) {
		return nil, errors.New("Linux shim launcher cgroup manager root is invalid")
	}
	if config.readinessTimeout == 0 {
		config.readinessTimeout = defaultLauncherReadinessTimeout
	}
	if config.cleanupTimeout == 0 {
		config.cleanupTimeout = defaultLauncherCleanupTimeout
	}
	if config.pollInterval == 0 {
		config.pollInterval = defaultLauncherPollInterval
	}
	if config.readinessTimeout < 0 || config.cleanupTimeout < 0 || config.pollInterval < 0 || config.pollInterval > config.readinessTimeout {
		return nil, errors.New("Linux shim launcher timeouts must be positive and coherently ordered")
	}
	return &LinuxShimLauncher{
		runtimeRoot: config.runtimeRoot, executable: config.executable,
		cgroups: config.cgroups, factory: config.factory, processes: config.processes,
		namespaces: config.namespaces, control: config.control, host: config.host,
		readinessTimeout: config.readinessTimeout, cleanupTimeout: config.cleanupTimeout, pollInterval: config.pollInterval,
	}, nil
}

// Preflight proves every reported production dependency through read-only
// checks, including the exact configured manager root and feature-specific clone3 probe.
func (launcher *LinuxShimLauncher) Preflight(ctx context.Context, requirements provider.IsolationRequirements) (provider.IsolationCapabilities, error) {
	if err := validateLauncherCall(ctx, launcher, requirements); err != nil {
		return provider.IsolationCapabilities{}, err
	}
	if err := launcher.cgroups.Preflight(ctx); err != nil {
		return provider.IsolationCapabilities{}, fmt.Errorf("preflight exact cgroup manager root: %w", err)
	}
	if err := launcher.factory.Preflight(ctx); err != nil {
		return provider.IsolationCapabilities{}, fmt.Errorf("preflight clone-time cgroup/pidfd process factory: %w", err)
	}
	if err := launcher.host.ValidateExecutable(launcher.executable); err != nil {
		return provider.IsolationCapabilities{}, err
	}
	if err := launcher.host.Preflight(ctx, launcher.cgroups.Root(), requirements); err != nil {
		return provider.IsolationCapabilities{}, err
	}
	if launcher.host.KeeperCloneFlags() == 0 || launcher.host.InitCloneFlags() == 0 {
		return provider.IsolationCapabilities{}, errors.New("Linux shim launcher namespace clone flags are incomplete")
	}
	return provider.IsolationCapabilities{
		Rootful: requirements.Rootful, PIDFD: requirements.PIDFD, PivotRoot: requirements.PivotRoot,
		StartGate: requirements.StartGate, Streams: requirements.Streams,
		Namespaces: append([]isolation.NamespaceType(nil), requirements.Namespaces...),
	}, nil
}

// EnsureKeeper durably records immutable intent before fork, captures and
// checkpoints strong identity before gate release, and safely adopts exact recovery evidence.
func (launcher *LinuxShimLauncher) EnsureKeeper(ctx context.Context, request KeeperLaunch) (LaunchedProcess, error) {
	if err := launcher.validateKeeperLaunch(ctx, request); err != nil {
		return LaunchedProcess{}, err
	}
	lock := launcher.ownerOperationLock(request.Owner.Token)
	lock.Lock()
	defer lock.Unlock()
	store, err := newLaunchStore(request.Paths, request.Owner)
	if err != nil {
		return LaunchedProcess{}, err
	}
	config := shim.RuntimeConfig{
		SchemaVersion: shim.SchemaVersion, Mode: shim.ModeKeeper, Owner: request.Owner,
		SandboxID: request.SandboxID, ControlSocket: request.Paths.ControlSocket,
	}
	prepared, journal, err := store.EnsureIntent(config)
	if err != nil {
		return LaunchedProcess{}, err
	}
	if journal.Phase == launchPhaseAuthorized || journal.Phase == launchPhaseReady {
		return launcher.recoverKeeper(ctx, request, prepared, store, journal)
	}
	if journal.Phase != launchPhaseIntent {
		return LaunchedProcess{}, errors.New("keeper launch journal is not recoverable")
	}
	if err := requireControlSocketAbsent(request.Paths.ControlSocket); err != nil {
		return LaunchedProcess{}, err
	}
	if err := launcher.waitKeeperLeafEmpty(ctx, request.SandboxID); err != nil {
		return LaunchedProcess{}, err
	}
	return launcher.launchKeeper(ctx, request, prepared, store, journal)
}

// validateKeeperLaunch binds the cgroup receipt, owner, Sandbox, and artifact
// paths to the launcher's configured roots before any journal or process mutation.
func (launcher *LinuxShimLauncher) validateKeeperLaunch(ctx context.Context, request KeeperLaunch) error {
	if ctx == nil {
		return errors.New("keeper launch context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if launcher == nil {
		return errors.New("Linux shim launcher is not configured")
	}
	if err := request.Owner.Validate(); err != nil {
		return err
	}
	if err := request.SandboxID.Validate(); err != nil {
		return err
	}
	if request.Owner.Target.Kind != "sandbox" || request.Owner.Target.ID != string(request.SandboxID) {
		return errors.New("keeper launch owner must target the exact Sandbox")
	}
	scope, err := validateCgroupReceipt(provider.OwnedReceiptRequest{Owner: request.Owner, Receipt: request.Cgroup}, ownership.KindKeeperCgroup)
	if err != nil {
		return fmt.Errorf("keeper cgroup receipt: %w", err)
	}
	if scope.sandboxID != request.SandboxID {
		return errors.New("keeper cgroup receipt belongs to another Sandbox")
	}
	return request.Paths.ValidateFor(launcher.runtimeRoot, request.Owner)
}

// ownerOperationLock serializes complete launch/recovery/socket cleanup for one
// owner so a successful reset cannot race a second process into the same socket path.
func (launcher *LinuxShimLauncher) ownerOperationLock(token string) *sync.Mutex {
	return sharedOwnerOperationLock(launcher.runtimeRoot, token)
}

// launchKeeper performs one gated fork and keeps abort authority until strong
// evidence, readiness, and the ready journal are all durable.
func (launcher *LinuxShimLauncher) launchKeeper(ctx context.Context, request KeeperLaunch, config shim.RuntimeConfig, store *launchStore, intent launchJournal) (_ LaunchedProcess, resultErr error) {
	directory, err := launcher.cgroups.OpenKeeper(ctx, request.SandboxID)
	if err != nil {
		return LaunchedProcess{}, err
	}
	directoryOpen := true
	defer func() {
		if directoryOpen {
			resultErr = errors.Join(resultErr, directory.Close())
		}
	}()
	fd, err := checkedDirectoryFD(directory)
	if err != nil {
		return LaunchedProcess{}, err
	}
	spec := ProcessLaunchSpec{
		Executable: launcher.executable,
		Arguments: []string{
			"-config", request.Paths.Config,
			"-config-evidence", config.WrapperEvidence,
			"-release-fd", "3",
		},
		Environment: minimalShimEnvironment(),
		CloneFlags:  launcher.host.KeeperCloneFlags(), CgroupFD: fd, ReleaseFD: 3,
	}
	started, err := launcher.factory.Start(ctx, spec)
	if err != nil {
		return LaunchedProcess{}, err
	}
	abortRequired := true
	defer func() {
		if abortRequired {
			resultErr = errors.Join(resultErr, launcher.abortStartedProcess(started))
		}
	}()
	if err := directory.Close(); err != nil {
		directoryOpen = false
		return LaunchedProcess{}, err
	}
	directoryOpen = false
	pidfd, err := started.TakePIDFD()
	if err != nil {
		return LaunchedProcess{}, err
	}
	process, err := launcher.processes.CaptureFromPIDFD(ctx, started.PID(), pidfd, launcher.executable)
	if err != nil {
		return LaunchedProcess{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, process.Close()) }()
	if err := launcher.cgroups.ConfirmKeeperProcess(ctx, request.SandboxID, process); err != nil {
		return LaunchedProcess{}, err
	}
	evidence, err := process.Evidence()
	if err != nil {
		return LaunchedProcess{}, err
	}
	result, err := launchedProcessFromEvidence(evidence, config.WrapperEvidence)
	if err != nil {
		return LaunchedProcess{}, err
	}
	authorized, err := intent.withProcess(launchPhaseAuthorized, evidence)
	if err != nil {
		return LaunchedProcess{}, err
	}
	if err := store.Write(intent, authorized); err != nil {
		return LaunchedProcess{}, err
	}
	if err := started.Release(); err != nil {
		return LaunchedProcess{}, err
	}
	if err := launcher.waitKeeperReady(ctx, request, config, process); err != nil {
		return LaunchedProcess{}, err
	}
	ready, err := authorized.withProcess(launchPhaseReady, evidence)
	if err != nil {
		return LaunchedProcess{}, err
	}
	if err := store.Write(authorized, ready); err != nil {
		return LaunchedProcess{}, err
	}
	if err := started.Commit(); err != nil {
		abortRequired = false
		return LaunchedProcess{}, errors.Join(err, launcher.terminateProcess(process))
	}
	abortRequired = false
	return result, nil
}

// recoverKeeper adopts only exact journal evidence, cleans exact absent owners
// before reset, and never converts an unjournaled cgroup PID into authority.
func (launcher *LinuxShimLauncher) recoverKeeper(ctx context.Context, request KeeperLaunch, config shim.RuntimeConfig, store *launchStore, journal launchJournal) (_ LaunchedProcess, resultErr error) {
	evidence := *journal.ProcessEvidence
	present, err := launcher.processes.Present(ctx, evidence)
	if err != nil {
		return LaunchedProcess{}, err
	}
	if !present {
		return launcher.relaunchAbsentKeeper(ctx, request, config, store, journal)
	}
	process, err := launcher.processes.Restore(ctx, evidence)
	if err != nil {
		return LaunchedProcess{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, process.Close()) }()
	if err := launcher.cgroups.ConfirmKeeperProcess(ctx, request.SandboxID, process); err != nil {
		return LaunchedProcess{}, err
	}
	result, err := launchedProcessFromEvidence(evidence, config.WrapperEvidence)
	if err != nil {
		return LaunchedProcess{}, err
	}
	if err := launcher.waitKeeperReady(ctx, request, config, process); err != nil {
		if journal.Phase == launchPhaseAuthorized {
			terminateErr := launcher.terminateProcess(process)
			if absentErr := launcher.waitProcessAbsent(ctx, evidence); absentErr != nil {
				return LaunchedProcess{}, errors.Join(err, terminateErr, absentErr)
			}
			result, relaunchErr := launcher.relaunchAbsentKeeper(ctx, request, config, store, journal)
			if relaunchErr != nil {
				return LaunchedProcess{}, errors.Join(err, relaunchErr)
			}
			return result, nil
		}
		return LaunchedProcess{}, err
	}
	if journal.Phase == launchPhaseAuthorized {
		ready, err := journal.withProcess(launchPhaseReady, evidence)
		if err != nil {
			return LaunchedProcess{}, err
		}
		if err := store.Write(journal, ready); err != nil {
			return LaunchedProcess{}, err
		}
	}
	return result, nil
}

// relaunchAbsentKeeper removes only an exact stale socket, CAS-resets the
// absent recorded owner, waits for parent-death cgroup exit, and launches once.
func (launcher *LinuxShimLauncher) relaunchAbsentKeeper(ctx context.Context, request KeeperLaunch, config shim.RuntimeConfig, store *launchStore, journal launchJournal) (LaunchedProcess, error) {
	if err := cleanupStaleControlSocket(store); err != nil {
		return LaunchedProcess{}, err
	}
	reset, err := journal.resetIntentAfterVerifiedAbsence()
	if err != nil {
		return LaunchedProcess{}, err
	}
	if err := store.Write(journal, reset); err != nil {
		return LaunchedProcess{}, err
	}
	if err := launcher.waitKeeperLeafEmpty(ctx, request.SandboxID); err != nil {
		return LaunchedProcess{}, err
	}
	if err := requireControlSocketAbsent(request.Paths.ControlSocket); err != nil {
		return LaunchedProcess{}, err
	}
	return launcher.launchKeeper(ctx, request, config, store, reset)
}

// waitKeeperLeafEmpty bounds restart convergence while an unjournaled gated
// child exits through release-pipe EOF; raw member PIDs never authorize an action.
func (launcher *LinuxShimLauncher) waitKeeperLeafEmpty(ctx context.Context, sandboxID domain.SandboxID) error {
	waitContext, cancel := boundedLauncherContext(ctx, launcher.readinessTimeout)
	defer cancel()
	for {
		members, err := launcher.cgroups.KeeperProcessIDs(waitContext, sandboxID)
		if err != nil {
			return err
		}
		if len(members) == 0 {
			return nil
		}
		if err := waitLauncherPoll(waitContext, launcher.pollInterval); err != nil {
			return fmt.Errorf("wait for unjournaled keeper child to exit without PID authority: %w", err)
		}
	}
}

// waitProcessAbsent repeatedly proves exact ProcessEvidence absence after a
// pidfd kill so reset and relaunch complete within the same Ensure call.
func (launcher *LinuxShimLauncher) waitProcessAbsent(ctx context.Context, evidence isolation.ProcessEvidence) error {
	waitContext, cancel := boundedLauncherContext(ctx, launcher.cleanupTimeout)
	defer cancel()
	for {
		present, err := launcher.processes.Present(waitContext, evidence)
		if err != nil {
			return err
		}
		if !present {
			return nil
		}
		if err := waitLauncherPoll(waitContext, launcher.pollInterval); err != nil {
			return fmt.Errorf("wait for exact keeper process absence: %w", err)
		}
	}
}

// waitKeeperReady polls only transport-unavailable responses under a bounded
// deadline and binds the exact SO_PEERCRED PID, config, owner, and cgroup membership.
func (launcher *LinuxShimLauncher) waitKeeperReady(ctx context.Context, request KeeperLaunch, config shim.RuntimeConfig, process launcherProcessHandle) error {
	readyContext, cancel := boundedLauncherContext(ctx, launcher.readinessTimeout)
	defer cancel()
	evidence, err := process.Evidence()
	if err != nil {
		return err
	}
	controlRequest := shim.ControlRequest{
		SchemaVersion: shim.SchemaVersion, RequestID: "launch-ready-" + config.WrapperEvidence[:32],
		Owner: request.Owner, Action: shim.ActionInspect,
	}
	for {
		response, peerPID, exchangeErr := launcher.control.Exchange(readyContext, request.Paths.ControlSocket, controlRequest)
		if exchangeErr == nil {
			if err := validateControlResponse(controlRequest, response); err != nil {
				return err
			}
			if response.Error != nil {
				if !shim.IsCode(response.Error, shim.CodeUnavailable) {
					return response.Error
				}
			} else {
				if peerPID != evidence.PID {
					return errors.New("keeper control peer PID differs from strong process evidence")
				}
				if response.Observation == nil {
					return errors.New("keeper readiness returned no observation")
				}
				if err := validateKeeperObservation(request, config, *response.Observation); err != nil {
					return err
				}
				if err := process.Verify(readyContext); err != nil {
					return err
				}
				return launcher.cgroups.ConfirmKeeperProcess(readyContext, request.SandboxID, process)
			}
		} else if !shim.IsCode(exchangeErr, shim.CodeUnavailable) {
			return exchangeErr
		}
		if err := waitLauncherPoll(readyContext, launcher.pollInterval); err != nil {
			return fmt.Errorf("wait for keeper readiness: %w", err)
		}
	}
}

// validateControlResponse enforces protocol validation and exact request replay correlation for injected or production clients.
func validateControlResponse(request shim.ControlRequest, response shim.ControlResponse) error {
	if err := response.Validate(); err != nil {
		return err
	}
	if response.RequestID != request.RequestID {
		return errors.New("shim response request ID differs from request")
	}
	return nil
}

// validateKeeperObservation binds a prepared keeper observation to the exact
// immutable owner, Sandbox, and wrapper configuration sent before fork.
func validateKeeperObservation(request KeeperLaunch, config shim.RuntimeConfig, observation shim.Observation) error {
	if err := observation.Validate(); err != nil {
		return err
	}
	if observation.Mode != shim.ModeKeeper || observation.Owner != request.Owner || observation.SandboxID != request.SandboxID ||
		observation.State != shim.StatePrepared || observation.WrapperEvidence != config.WrapperEvidence {
		return errors.New("keeper observation differs from immutable launch scope")
	}
	return nil
}

// launchedProcessFromEvidence validates canonical receipt bounds before the
// release gate can produce a serving process side effect.
func launchedProcessFromEvidence(evidence isolation.ProcessEvidence, wrapperEvidence string) (LaunchedProcess, error) {
	digest, err := ownership.EvidenceDigest(evidence)
	if err != nil {
		return LaunchedProcess{}, err
	}
	result := LaunchedProcess{
		IdentityEvidenceSHA256: digest, WrapperEvidenceSHA256: wrapperEvidence, ProcessEvidence: evidence,
	}
	return result, result.Validate()
}

// abortStartedProcess uses an independent bounded cleanup context so caller
// cancellation cannot strand a child that has not completed durable readiness.
func (launcher *LinuxShimLauncher) abortStartedProcess(started StartedProcess) error {
	cleanupContext, cancel := context.WithTimeout(context.Background(), launcher.cleanupTimeout)
	defer cancel()
	return started.Abort(cleanupContext)
}

// terminateProcess kills and reaps one exact restored pidfd owner after launch
// authority was already committed or an authorized recovery cannot become ready.
func (launcher *LinuxShimLauncher) terminateProcess(process launcherProcessHandle) error {
	cleanupContext, cancel := context.WithTimeout(context.Background(), launcher.cleanupTimeout)
	defer cancel()
	signalErr := process.Signal(cleanupContext, int(syscall.SIGKILL))
	waitErr := process.WaitForExit(cleanupContext, launcher.pollInterval)
	return errors.Join(signalErr, waitErr)
}

// checkedDirectoryFD converts one runtime-only cgroup handle without accepting
// an overflowed descriptor or standard-stream alias.
func checkedDirectoryFD(directory cgroupv2.DirectoryHandle) (int, error) {
	if directory == nil || directory.Fd() > uintptr(^uint(0)>>1) {
		return -1, errors.New("cgroup directory descriptor is invalid")
	}
	fd := int(directory.Fd())
	if fd < 3 {
		return -1, errors.New("cgroup directory descriptor aliases a standard stream")
	}
	return fd, nil
}

// cleanupStaleControlSocket removes only a socket in the already verified
// private owner directory after the exact journaled ProcessEvidence was proven absent.
func cleanupStaleControlSocket(store *launchStore) error {
	info, err := os.Lstat(store.paths.ControlSocket)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return errors.New("stale control artifact is not a Unix socket")
	}
	if err := os.Remove(store.paths.ControlSocket); err != nil {
		return err
	}
	return store.syncDirectory(store.paths.OwnerRoot)
}

// requireControlSocketAbsent rejects any endpoint that lacks a preceding
// exact journal owner and verified absence cleanup authorization.
func requireControlSocketAbsent(path string) error {
	_, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("control socket exists without recoverable process evidence")
}

// boundedLauncherContext preserves an earlier caller deadline and otherwise
// bounds readiness even when reconciliation supplies context.Background.
func boundedLauncherContext(ctx context.Context, maximum time.Duration) (context.Context, context.CancelFunc) {
	if deadline, exists := ctx.Deadline(); exists && time.Until(deadline) <= maximum {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, maximum)
}

// waitLauncherPoll waits one bounded readiness retry interval without sleeping past cancellation.
func waitLauncherPoll(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// cleanAbsoluteNonRoot recognizes the only configured path shape accepted by the launcher constructor.
func cleanAbsoluteNonRoot(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && path != string(filepath.Separator) && !strings.ContainsRune(path, '\x00')
}

// minimalShimEnvironment prevents keeper and init wrappers from inheriting
// daemon credentials or request-scoped environment while retaining deterministic locale and lookup paths.
func minimalShimEnvironment() []string {
	return []string{"PATH=/usr/sbin:/usr/bin:/sbin:/bin", "LANG=C", "LC_ALL=C"}
}

// EnsureInit durably launches or rediscovers the Attempt PID1 bootstrap after
// validating its cgroup, closed gate, streams, and three exact keeper namespaces.
func (launcher *LinuxShimLauncher) EnsureInit(ctx context.Context, request InitLaunch) (LaunchedProcess, error) {
	if launcher == nil {
		return LaunchedProcess{}, errors.New("Linux shim launcher is not configured")
	}
	if err := request.Owner.Validate(); err != nil {
		return LaunchedProcess{}, err
	}
	lock := launcher.ownerOperationLock(request.Owner.Token)
	lock.Lock()
	defer lock.Unlock()
	bootstrapEvidence, err := launcher.validateInitLaunch(ctx, request)
	if err != nil {
		return LaunchedProcess{}, err
	}
	store, err := newLaunchStore(request.Paths, request.Owner)
	if err != nil {
		return LaunchedProcess{}, err
	}
	config := shim.RuntimeConfig{
		SchemaVersion: shim.SchemaVersion, Mode: shim.ModeInit, Owner: request.Owner,
		SandboxID: request.SandboxID, ContainerID: domain.ContainerID(request.Owner.Target.ID), AttemptID: request.AttemptID,
		BootstrapEvidence: bootstrapEvidence, ControlSocket: request.Paths.ControlSocket,
		TerminalPath: request.Paths.Terminal, LogPath: request.Paths.Log, Process: request.Process.Clone(),
	}
	prepared, journal, err := store.EnsureIntent(config)
	if err != nil {
		return LaunchedProcess{}, err
	}
	if journal.Phase == launchPhaseAuthorized || journal.Phase == launchPhaseReady {
		return launcher.recoverInit(ctx, request, prepared, store, journal)
	}
	if journal.Phase != launchPhaseIntent {
		return LaunchedProcess{}, errors.New("init launch journal is not recoverable")
	}
	if err := requireControlSocketAbsent(request.Paths.ControlSocket); err != nil {
		return LaunchedProcess{}, err
	}
	if err := launcher.waitAttemptLeafEmpty(ctx, request.SandboxID, request.AttemptID); err != nil {
		return LaunchedProcess{}, err
	}
	return launcher.launchInit(ctx, request, prepared, store, journal)
}

// validateInitLaunch binds every caller-independent prerequisite and confirms
// the gate remains closed and streams ready before any init journal or fork effect.
func (launcher *LinuxShimLauncher) validateInitLaunch(ctx context.Context, request InitLaunch) (string, error) {
	if ctx == nil {
		return "", errors.New("init launch context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if launcher == nil {
		return "", errors.New("Linux shim launcher is not configured")
	}
	if err := request.Owner.Validate(); err != nil {
		return "", err
	}
	if request.Owner.Target.Kind != "container" {
		return "", errors.New("init launch owner must target a Container")
	}
	if err := domain.ContainerID(request.Owner.Target.ID).Validate(); err != nil {
		return "", err
	}
	if err := request.SandboxID.Validate(); err != nil {
		return "", err
	}
	if err := request.AttemptID.Validate(); err != nil {
		return "", err
	}
	if err := request.Process.Validate(); err != nil {
		return "", err
	}
	if err := request.Paths.ValidateFor(launcher.runtimeRoot, request.Owner); err != nil {
		return "", err
	}
	scope, err := validateCgroupReceipt(provider.OwnedReceiptRequest{Owner: request.Owner, Receipt: request.Cgroup}, ownership.KindAttemptCgroup)
	if err != nil {
		return "", fmt.Errorf("init cgroup receipt: %w", err)
	}
	if scope.sandboxID != request.SandboxID || scope.attemptID != request.AttemptID {
		return "", errors.New("init cgroup receipt belongs to another Sandbox or Attempt")
	}
	if err := request.SandboxNamespaces.ValidateFor(launcher.runtimeRoot, request.SandboxID); err != nil {
		return "", err
	}
	if request.SandboxNamespaces.UTS.ProcessEvidence != request.SandboxNamespaces.IPC.ProcessEvidence ||
		request.SandboxNamespaces.UTS.ProcessEvidence != request.SandboxNamespaces.Network.ProcessEvidence {
		return "", errors.New("init Sandbox namespaces do not share exact keeper ProcessEvidence")
	}
	artifacts, err := newArtifactStore(launcher.runtimeRoot)
	if err != nil {
		return "", err
	}
	if err := validateInitArtifact(artifacts, request.Owner, request.Gate, ownership.KindStartGate, request.AttemptID, artifactStateClosed); err != nil {
		return "", err
	}
	if err := validateInitArtifact(artifacts, request.Owner, request.Streams, ownership.KindStreams, request.AttemptID, artifactStateReady); err != nil {
		return "", err
	}
	return initBootstrapEvidence(request)
}

// validateInitArtifact verifies one canonical dependency receipt against the
// current private artifact state so released gates or replaced streams cannot launch init.
func validateInitArtifact(store *artifactStore, owner ownership.OwnerKey, receipt ownership.Receipt, kind ownership.Kind, attemptID domain.AttemptID, state string) error {
	if err := (provider.OwnedReceiptRequest{Owner: owner, Receipt: receipt}).ValidateFor(ownership.ProviderLinux, kind); err != nil {
		return err
	}
	if err := validateSlimReceipt(receipt); err != nil {
		return err
	}
	if receipt.Attributes[attemptIDAttribute] != string(attemptID) {
		return errors.New("init artifact receipt belongs to another Attempt")
	}
	record, found, err := store.Read(owner, kind)
	if err != nil {
		return err
	}
	if !found || record.ReceiptEvidence != receipt.EvidenceSHA256 || record.State != state {
		return errors.New("init artifact state differs from its exact receipt prerequisite")
	}
	return nil
}

// initBootstrapEvidence hashes exact dependency receipts and namespace
// references into the immutable config used by bootstrap and restart adoption.
func initBootstrapEvidence(request InitLaunch) (string, error) {
	return ownership.EvidenceDigest(struct {
		CgroupEvidence string                     `json:"cgroup_evidence_sha256"`
		GateEvidence   string                     `json:"gate_evidence_sha256"`
		StreamEvidence string                     `json:"stream_evidence_sha256"`
		Namespaces     SandboxNamespaceReferences `json:"sandbox_namespaces"`
	}{request.Cgroup.EvidenceSHA256, request.Gate.EvidenceSHA256, request.Streams.EvidenceSHA256, request.SandboxNamespaces})
}

// initNamespaceInput owns one retained nsfs handle and its child duplicate until ProcessFactory accepts the spec.
type initNamespaceInput struct {
	namespace isolation.NamespaceType
	handle    launcherNamespaceHandle
	fd        int
	evidence  isolation.NamespaceEvidence
}

// openInitNamespaceInputs restores the exact keeper, verifies each namespace
// digest and inode, and returns fixed-order UTS/IPC/network child duplicates.
func (launcher *LinuxShimLauncher) openInitNamespaceInputs(ctx context.Context, request InitLaunch, keeper launcherProcessHandle) (_ []initNamespaceInput, resultErr error) {
	references := []struct {
		namespace isolation.NamespaceType
		reference ResourceReference
	}{
		{isolation.NamespaceUTS, request.SandboxNamespaces.UTS},
		{isolation.NamespaceIPC, request.SandboxNamespaces.IPC},
		{isolation.NamespaceNetwork, request.SandboxNamespaces.Network},
	}
	inputs := make([]initNamespaceInput, 0, len(references))
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, launcher.closeInitNamespaceInputs(inputs, true))
		}
	}()
	keeperEvidence, err := keeper.Evidence()
	if err != nil {
		return nil, err
	}
	for _, expected := range references {
		if expected.reference.ProcessEvidence != keeperEvidence {
			return nil, errors.New("namespace reference ProcessEvidence differs from restored keeper")
		}
		handle, err := launcher.namespaces.Open(ctx, keeper, expected.namespace)
		if err != nil {
			return nil, err
		}
		input := initNamespaceInput{namespace: expected.namespace, handle: handle, fd: -1}
		inputs = append(inputs, input)
		evidence, err := handle.Evidence()
		if err != nil {
			return nil, err
		}
		if err := evidence.Validate(); err != nil {
			return nil, err
		}
		if evidence.Type != expected.namespace || evidence.Owner != keeperEvidence {
			return nil, errors.New("opened namespace evidence differs from exact keeper scope")
		}
		digest, err := ownership.EvidenceDigest(evidence)
		if err != nil {
			return nil, err
		}
		if digest != expected.reference.LauncherEvidenceSHA256 {
			return nil, errors.New("opened namespace inode evidence differs from receipt")
		}
		fd, duplicatedEvidence, err := handle.Duplicate(ctx)
		if err != nil {
			return nil, err
		}
		inputs[len(inputs)-1].fd = fd
		inputs[len(inputs)-1].evidence = evidence
		if duplicatedEvidence != evidence {
			return nil, errors.New("duplicated namespace evidence changed after verification")
		}
	}
	return inputs, nil
}

// closeInitNamespaceInputs closes retained handles and optionally every child
// duplicate not yet transferred to ProcessFactory, preserving all cleanup errors.
func (launcher *LinuxShimLauncher) closeInitNamespaceInputs(inputs []initNamespaceInput, closeDuplicates bool) error {
	var result error
	for index := len(inputs) - 1; index >= 0; index-- {
		if closeDuplicates && inputs[index].fd >= 0 {
			result = errors.Join(result, launcher.namespaces.CloseFD(inputs[index].fd))
		}
		if inputs[index].handle != nil {
			result = errors.Join(result, inputs[index].handle.Close())
		}
	}
	return result
}

// launchInit restores keeper namespace authority, starts one gated PID1
// bootstrap in the Attempt cgroup, and retains abort authority until durable ready.
func (launcher *LinuxShimLauncher) launchInit(ctx context.Context, request InitLaunch, config shim.RuntimeConfig, store *launchStore, intent launchJournal) (_ LaunchedProcess, resultErr error) {
	keeperEvidence := request.SandboxNamespaces.UTS.ProcessEvidence
	keeper, err := launcher.processes.Restore(ctx, keeperEvidence)
	if err != nil {
		return LaunchedProcess{}, err
	}
	keeperOpen := true
	defer func() {
		if keeperOpen {
			resultErr = errors.Join(resultErr, keeper.Close())
		}
	}()
	if err := launcher.cgroups.ConfirmKeeperProcess(ctx, request.SandboxID, keeper); err != nil {
		return LaunchedProcess{}, err
	}
	inputs, err := launcher.openInitNamespaceInputs(ctx, request, keeper)
	if err != nil {
		return LaunchedProcess{}, err
	}
	inputsOpen := true
	duplicatesTransferred := false
	defer func() {
		if inputsOpen {
			resultErr = errors.Join(resultErr, launcher.closeInitNamespaceInputs(inputs, !duplicatesTransferred))
		}
	}()
	directory, err := launcher.cgroups.OpenAttempt(ctx, request.SandboxID, request.AttemptID)
	if err != nil {
		return LaunchedProcess{}, err
	}
	directoryOpen := true
	defer func() {
		if directoryOpen {
			resultErr = errors.Join(resultErr, directory.Close())
		}
	}()
	cgroupFD, err := checkedDirectoryFD(directory)
	if err != nil {
		return LaunchedProcess{}, err
	}
	arguments := []string{
		"bootstrap-init",
		"-config", request.Paths.Config,
		"-config-evidence", config.WrapperEvidence,
		"-uts-fd", "3", "-uts-inode", strconv.FormatUint(inputs[0].evidence.Inode, 10),
		"-ipc-fd", "4", "-ipc-inode", strconv.FormatUint(inputs[1].evidence.Inode, 10),
		"-net-fd", "5", "-net-inode", strconv.FormatUint(inputs[2].evidence.Inode, 10),
		"-release-fd", "6",
	}
	spec := ProcessLaunchSpec{
		Executable: launcher.executable, Arguments: arguments, Environment: minimalShimEnvironment(),
		CloneFlags: launcher.host.InitCloneFlags(), CgroupFD: cgroupFD,
		ExtraFDs: []int{inputs[0].fd, inputs[1].fd, inputs[2].fd}, ReleaseFD: 6,
	}
	duplicatesTransferred = true
	started, err := launcher.factory.Start(ctx, spec)
	if err != nil {
		return LaunchedProcess{}, err
	}
	abortRequired := true
	defer func() {
		if abortRequired {
			resultErr = errors.Join(resultErr, launcher.abortStartedProcess(started))
		}
	}()
	if err := directory.Close(); err != nil {
		directoryOpen = false
		return LaunchedProcess{}, err
	}
	directoryOpen = false
	if err := launcher.closeInitNamespaceInputs(inputs, false); err != nil {
		inputsOpen = false
		return LaunchedProcess{}, err
	}
	inputsOpen = false
	if err := keeper.Close(); err != nil {
		keeperOpen = false
		return LaunchedProcess{}, err
	}
	keeperOpen = false
	pidfd, err := started.TakePIDFD()
	if err != nil {
		return LaunchedProcess{}, err
	}
	process, err := launcher.processes.CaptureFromPIDFD(ctx, started.PID(), pidfd, launcher.executable)
	if err != nil {
		return LaunchedProcess{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, process.Close()) }()
	if err := launcher.cgroups.AttachProcess(ctx, request.SandboxID, request.AttemptID, process); err != nil {
		return LaunchedProcess{}, err
	}
	evidence, err := process.Evidence()
	if err != nil {
		return LaunchedProcess{}, err
	}
	result, err := launchedProcessFromEvidence(evidence, config.WrapperEvidence)
	if err != nil {
		return LaunchedProcess{}, err
	}
	authorized, err := intent.withProcess(launchPhaseAuthorized, evidence)
	if err != nil {
		return LaunchedProcess{}, err
	}
	if err := store.Write(intent, authorized); err != nil {
		return LaunchedProcess{}, err
	}
	if err := started.Release(); err != nil {
		return LaunchedProcess{}, err
	}
	if err := launcher.waitInitReady(ctx, request, config, process); err != nil {
		return LaunchedProcess{}, err
	}
	ready, err := authorized.withProcess(launchPhaseReady, evidence)
	if err != nil {
		return LaunchedProcess{}, err
	}
	if err := store.Write(authorized, ready); err != nil {
		return LaunchedProcess{}, err
	}
	if err := started.Commit(); err != nil {
		abortRequired = false
		return LaunchedProcess{}, errors.Join(err, launcher.terminateProcess(process))
	}
	abortRequired = false
	return result, nil
}

// recoverInit adopts only exact journal evidence and applies the same bounded
// authorized-crash cleanup/reset/single-relaunch contract as keeper recovery.
func (launcher *LinuxShimLauncher) recoverInit(ctx context.Context, request InitLaunch, config shim.RuntimeConfig, store *launchStore, journal launchJournal) (_ LaunchedProcess, resultErr error) {
	evidence := *journal.ProcessEvidence
	present, err := launcher.processes.Present(ctx, evidence)
	if err != nil {
		return LaunchedProcess{}, err
	}
	if !present {
		return launcher.relaunchAbsentInit(ctx, request, config, store, journal)
	}
	process, err := launcher.processes.Restore(ctx, evidence)
	if err != nil {
		return LaunchedProcess{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, process.Close()) }()
	if err := launcher.cgroups.AttachProcess(ctx, request.SandboxID, request.AttemptID, process); err != nil {
		return LaunchedProcess{}, err
	}
	result, err := launchedProcessFromEvidence(evidence, config.WrapperEvidence)
	if err != nil {
		return LaunchedProcess{}, err
	}
	if err := launcher.waitInitReady(ctx, request, config, process); err != nil {
		if journal.Phase == launchPhaseAuthorized {
			terminateErr := launcher.terminateProcess(process)
			if absentErr := launcher.waitProcessAbsent(ctx, evidence); absentErr != nil {
				return LaunchedProcess{}, errors.Join(err, terminateErr, absentErr)
			}
			result, relaunchErr := launcher.relaunchAbsentInit(ctx, request, config, store, journal)
			if relaunchErr != nil {
				return LaunchedProcess{}, errors.Join(err, relaunchErr)
			}
			return result, nil
		}
		return LaunchedProcess{}, err
	}
	if journal.Phase == launchPhaseAuthorized {
		ready, err := journal.withProcess(launchPhaseReady, evidence)
		if err != nil {
			return LaunchedProcess{}, err
		}
		if err := store.Write(journal, ready); err != nil {
			return LaunchedProcess{}, err
		}
	}
	return result, nil
}

// relaunchAbsentInit cleans only the absent recorded owner, CAS-resets its
// journal, waits for the Attempt leaf to empty, and launches exactly once.
func (launcher *LinuxShimLauncher) relaunchAbsentInit(ctx context.Context, request InitLaunch, config shim.RuntimeConfig, store *launchStore, journal launchJournal) (LaunchedProcess, error) {
	if err := cleanupStaleControlSocket(store); err != nil {
		return LaunchedProcess{}, err
	}
	reset, err := journal.resetIntentAfterVerifiedAbsence()
	if err != nil {
		return LaunchedProcess{}, err
	}
	if err := store.Write(journal, reset); err != nil {
		return LaunchedProcess{}, err
	}
	if err := launcher.waitAttemptLeafEmpty(ctx, request.SandboxID, request.AttemptID); err != nil {
		return LaunchedProcess{}, err
	}
	if err := requireControlSocketAbsent(request.Paths.ControlSocket); err != nil {
		return LaunchedProcess{}, err
	}
	return launcher.launchInit(ctx, request, config, store, reset)
}

// waitAttemptLeafEmpty bounds parent-death convergence without converting raw
// unjournaled Attempt member PIDs into signal or adoption authority.
func (launcher *LinuxShimLauncher) waitAttemptLeafEmpty(ctx context.Context, sandboxID domain.SandboxID, attemptID domain.AttemptID) error {
	waitContext, cancel := boundedLauncherContext(ctx, launcher.readinessTimeout)
	defer cancel()
	for {
		members, err := launcher.cgroups.AttemptProcessIDs(waitContext, sandboxID, attemptID)
		if err != nil {
			return err
		}
		if len(members) == 0 {
			return nil
		}
		if err := waitLauncherPoll(waitContext, launcher.pollInterval); err != nil {
			return fmt.Errorf("wait for unjournaled init child to exit without PID authority: %w", err)
		}
	}
}

// waitInitReady binds authenticated peer identity and a prepared rootfs-free
// init observation to the exact config before the ready journal transition.
func (launcher *LinuxShimLauncher) waitInitReady(ctx context.Context, request InitLaunch, config shim.RuntimeConfig, process launcherProcessHandle) error {
	readyContext, cancel := boundedLauncherContext(ctx, launcher.readinessTimeout)
	defer cancel()
	evidence, err := process.Evidence()
	if err != nil {
		return err
	}
	controlRequest := shim.ControlRequest{
		SchemaVersion: shim.SchemaVersion, RequestID: "launch-ready-" + config.WrapperEvidence[:32],
		Owner: request.Owner, Action: shim.ActionInspect,
	}
	for {
		response, peerPID, exchangeErr := launcher.control.Exchange(readyContext, request.Paths.ControlSocket, controlRequest)
		if exchangeErr == nil {
			if err := validateControlResponse(controlRequest, response); err != nil {
				return err
			}
			if response.Error != nil {
				if !shim.IsCode(response.Error, shim.CodeUnavailable) {
					return response.Error
				}
			} else {
				if peerPID != evidence.PID {
					return errors.New("init control peer PID differs from strong process evidence")
				}
				if response.Observation == nil {
					return errors.New("init readiness returned no observation")
				}
				if err := validateInitObservation(request, config, *response.Observation); err != nil {
					return err
				}
				if err := process.Verify(readyContext); err != nil {
					return err
				}
				return launcher.cgroups.AttachProcess(readyContext, request.SandboxID, request.AttemptID, process)
			}
		} else if !shim.IsCode(exchangeErr, shim.CodeUnavailable) {
			return exchangeErr
		}
		if err := waitLauncherPoll(readyContext, launcher.pollInterval); err != nil {
			return fmt.Errorf("wait for init readiness: %w", err)
		}
	}
}

// validateInitObservation rejects any ready response not bound to the exact
// Container, Attempt, Sandbox, config, closed gate, and deferred-rootfs state.
func validateInitObservation(request InitLaunch, config shim.RuntimeConfig, observation shim.Observation) error {
	if err := observation.Validate(); err != nil {
		return err
	}
	if observation.Mode != shim.ModeInit || observation.Owner != request.Owner || observation.SandboxID != request.SandboxID ||
		observation.ContainerID != domain.ContainerID(request.Owner.Target.ID) || observation.AttemptID != request.AttemptID ||
		observation.State != shim.StatePrepared || observation.WrapperEvidence != config.WrapperEvidence || observation.Rootfs != nil {
		return errors.New("init observation differs from immutable launch scope or deferred-rootfs state")
	}
	return nil
}

// EnsureNamespace captures and configures one verified keeper or init namespace.
func (launcher *LinuxShimLauncher) EnsureNamespace(ctx context.Context, request NamespaceLaunch) (string, error) {
	return launcher.ensureNamespaceAction(ctx, request)
}

// PrepareRootfs sends one authenticated semantic rootfs command to verified init PID1.
func (launcher *LinuxShimLauncher) PrepareRootfs(ctx context.Context, request RootfsLaunch) (string, error) {
	return launcher.prepareRootfsAction(ctx, request)
}

// Inspect returns a receipt-scoped observation after exact process and resource verification.
func (launcher *LinuxShimLauncher) Inspect(ctx context.Context, reference ResourceReference) (provider.ResourceObservation, error) {
	return launcher.inspectLauncherAction(ctx, reference)
}

// Remove tears down a resource through its exact retained wrapper identity.
func (launcher *LinuxShimLauncher) Remove(ctx context.Context, reference ResourceReference) (provider.CleanupObservation, error) {
	return launcher.removeLauncherAction(ctx, reference)
}

// Signal delivers one bounded signal only after exact keeper identity recovery.
func (launcher *LinuxShimLauncher) Signal(ctx context.Context, reference ResourceReference, signal provider.Signal) (provider.SignalObservation, error) {
	return launcher.signalLauncherAction(ctx, reference, signal)
}

// ResolveProcess returns a descriptor-free reference that verifies strong identity per use.
func (launcher *LinuxShimLauncher) ResolveProcess(ctx context.Context, reference ResourceReference) (cgroupv2.ProcessReference, error) {
	return launcher.resolveLauncherProcess(ctx, reference)
}

// validateLauncherCall checks complete production preflight inputs without mutating host state.
func validateLauncherCall(ctx context.Context, launcher *LinuxShimLauncher, requirements provider.IsolationRequirements) error {
	if ctx == nil {
		return errors.New("Linux shim launcher context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if launcher == nil || launcher.executable == "" || launcher.runtimeRoot == "" || launcher.cgroups == nil || launcher.factory == nil || launcher.host == nil {
		return errors.New("Linux shim launcher is not configured")
	}
	return requirements.Validate()
}
