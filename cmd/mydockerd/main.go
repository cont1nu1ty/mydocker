// mydockerd is the single-node M3 lifecycle and metadata authority.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	v1 "mydocker/api/runtime/v1"
	"mydocker/internal/cgroupv2"
	"mydocker/internal/daemon"
	"mydocker/internal/engine"
	"mydocker/internal/isolation"
	"mydocker/internal/observability"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
	"mydocker/internal/server"
	"mydocker/internal/slim"
	"mydocker/internal/state"
)

const defaultShutdownTimeout = 15 * time.Second
const terminalWatchInterval = 250 * time.Millisecond

var errAPIStoppedUnexpectedly = errors.New("daemon API stopped unexpectedly")
var errAPIShutdownTimeout = errors.New("daemon API shutdown timed out")
var errAPIWaitUnconfirmed = errors.New("daemon API shutdown returned before Wait confirmed serving stopped")
var errBackgroundShutdownTimeout = errors.New("daemon lifecycle supervisor shutdown timed out")

// daemonConfig contains every host ownership boundary needed by production
// composition; path-bearing values are accepted only from daemon startup flags.
type daemonConfig struct {
	statePath      string
	runtimeRoot    string
	socketPath     string
	cgroupRoot     string
	shimPath       string
	preparedRootFS map[provider.OpaqueID]isolation.RootfsConfig
	shutdown       time.Duration
}

// preparedRootFSFlags collects repeatable opaque-ID-to-path mappings without
// interpreting them until the complete command configuration is validated.
type preparedRootFSFlags []string

// String renders only the number of configured entries so flag errors do not
// copy trusted host paths into incidental diagnostics.
func (values *preparedRootFSFlags) String() string {
	if values == nil {
		return "0 entries"
	}
	return fmt.Sprintf("%d entries", len(*values))
}

// Set appends one raw repeatable mapping; parseDaemonConfig later validates
// duplicates, opaque IDs, and clean absolute prepared-rootfs paths together.
func (values *preparedRootFSFlags) Set(value string) error {
	if values == nil {
		return errors.New("prepared-rootfs flag receiver must not be nil")
	}
	*values = append(*values, value)
	return nil
}

// collectDaemonInfo reads immutable Go build metadata from this daemon binary without consulting a working tree.
func collectDaemonInfo() v1.InfoResponse {
	buildInfo, ok := debug.ReadBuildInfo()
	return daemonInfoFromBuildInfo(buildInfo, ok)
}

// daemonInfoFromBuildInfo converts one Go build snapshot into the bounded v1 daemon identity contract.
func daemonInfoFromBuildInfo(buildInfo *debug.BuildInfo, available bool) v1.InfoResponse {
	identity := v1.DaemonBuildIdentity{
		Source:            v1.DaemonBuildIdentitySource,
		Unavailable:       true,
		UnavailableReason: v1.DaemonBuildUnavailableBuildInfo,
	}
	if !available || buildInfo == nil {
		return v1.InfoResponse{DaemonBuild: identity}
	}
	identity.GoVersion = buildInfo.GoVersion
	identity.MainPath = buildInfo.Main.Path
	identity.MainVersion = buildInfo.Main.Version
	identity.MainSum = buildInfo.Main.Sum
	for _, setting := range buildInfo.Settings {
		switch setting.Key {
		case "vcs":
			identity.VCS = setting.Value
		case "vcs.revision":
			identity.VCSRevision = setting.Value
		case "vcs.time":
			identity.VCSTime = setting.Value
		case "vcs.modified":
			modified, err := strconv.ParseBool(setting.Value)
			if err == nil {
				identity.VCSModified = &modified
			}
		}
	}
	if !v1.UsableVCSRevision(identity.VCSRevision) {
		identity.UnavailableReason = v1.DaemonBuildUnavailableRevision
		return v1.InfoResponse{DaemonBuild: identity}
	}
	if identity.VCSModified == nil {
		identity.UnavailableReason = v1.DaemonBuildUnavailableModified
		return v1.InfoResponse{DaemonBuild: identity}
	}
	identity.Unavailable = false
	identity.UnavailableReason = ""
	return v1.InfoResponse{DaemonBuild: identity}
}

// daemonRuntime is the ordered startup boundary used by runDaemon; tests
// replace it so no host provider, process, cgroup, or namespace action occurs.
type daemonRuntime interface {
	Preflight(context.Context) error
	Reconcile(context.Context) error
	APIService() server.Service
	Close() error
}

// backgroundRuntime is the optional continuously supervised lifecycle loop exposed by the production engine.
type backgroundRuntime interface {
	RunBackground(context.Context) error
}

// managedServer is the lifecycle subset of the UDS server needed by the
// command; a nil Shutdown result must prove serving and handlers are quiescent.
type managedServer interface {
	Start() error
	Wait() error
	Shutdown(context.Context) error
}

// runtimeFactory opens one daemon runtime without binding the public socket.
type runtimeFactory func(context.Context, daemonConfig) (daemonRuntime, error)

// serverFactory builds an unstarted API endpoint after recovery has completed.
type serverFactory func(server.Config, server.Service) (managedServer, error)

// productionRuntime owns durable state, host providers, the reconciled engine,
// and the API adapter for exactly one daemon process.
type productionRuntime struct {
	store     *state.FileStore
	cgroup    cgroupCapabilityInspector
	isolation isolationCapabilityInspector
	engine    *engine.Engine
	service   *daemon.Service
}

// cgroupCapabilityInspector is the read-only portion of the cgroup provider
// used before recovery is allowed to make any host mutation.
type cgroupCapabilityInspector interface {
	InspectCgroupCapabilities(context.Context, provider.CgroupRequirements) (provider.CgroupCapabilities, error)
}

// isolationCapabilityInspector is the read-only portion of the isolation
// provider used to reject unsupported host capabilities before serving.
type isolationCapabilityInspector interface {
	InspectIsolationCapabilities(context.Context, provider.IsolationRequirements) (provider.IsolationCapabilities, error)
}

// main installs only process-level termination notifications and delegates all
// startup ordering, recovery, serving, and graceful shutdown to runDaemon.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := runDaemon(ctx, os.Args[1:], os.Stderr, openProductionRuntime, newProductionServer); err != nil {
		os.Exit(1)
	}
}

// runDaemon parses explicit ownership paths, completes preflight and recovery
// before binding UDS, then quiesces API and watcher work before closing state.
func runDaemon(ctx context.Context, arguments []string, logOutput io.Writer, openRuntime runtimeFactory, openServer serverFactory) (resultErr error) {
	if ctx == nil {
		return errors.New("mydockerd context must not be nil")
	}
	if logOutput == nil || openRuntime == nil || openServer == nil {
		return errors.New("mydockerd requires log, runtime, and server dependencies")
	}
	logger, err := observability.NewJSONLogger(logOutput, time.Now)
	if err != nil {
		return err
	}
	config, err := parseDaemonConfig(arguments)
	if err != nil {
		logDaemonFailure(logger, "daemon configuration rejected", err)
		return err
	}
	runtime, err := openRuntime(ctx, config)
	if err != nil {
		logDaemonFailure(logger, "daemon runtime open failed", err)
		return err
	}
	runtimeMayClose := true
	defer func() {
		if !runtimeMayClose {
			return
		}
		if closeErr := runtime.Close(); closeErr != nil {
			logDaemonFailure(logger, "daemon runtime close failed", closeErr)
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	if err := runtime.Preflight(ctx); err != nil {
		logDaemonFailure(logger, "daemon host preflight failed", err)
		return err
	}
	if err := runtime.Reconcile(ctx); err != nil {
		logDaemonFailure(logger, "daemon recovery failed", err)
		return err
	}
	logDaemonInfo(logger, "daemon recovery completed")
	endpoint, err := openServer(server.Config{SocketPath: config.socketPath, SocketMode: 0o660}, runtime.APIService())
	if err != nil {
		logDaemonFailure(logger, "daemon API construction failed", err)
		return err
	}
	if err := endpoint.Start(); err != nil {
		logDaemonFailure(logger, "daemon API start failed", err)
		return err
	}
	runtimeMayClose = false
	logDaemonInfo(logger, "daemon API serving")
	waitResult := make(chan error, 1)
	go func() {
		waitResult <- endpoint.Wait()
	}()
	backgroundContext, stopBackground := context.WithCancel(context.WithoutCancel(ctx))
	defer stopBackground()
	var backgroundResult <-chan error
	if background, ok := runtime.(backgroundRuntime); ok {
		result := make(chan error, 1)
		backgroundResult = result
		go func() {
			result <- background.RunBackground(backgroundContext)
		}()
	}
	var exitErr error
	select {
	case waitErr := <-waitResult:
		waitResult = nil
		exitErr = endpointExitError(waitErr)
		if exitErr != nil {
			logDaemonFailure(logger, "daemon API stopped unexpectedly", exitErr)
		}
	case <-ctx.Done():
	case backgroundErr := <-backgroundResult:
		backgroundResult = nil
		exitErr = backgroundExitError(backgroundErr)
		if exitErr != nil {
			logDaemonFailure(logger, "daemon lifecycle supervisor stopped", exitErr)
		}
	}
	// Preserve any other result that was already ready when the first select
	// completed; the parent context cannot cancel either independently owned loop.
	if waitResult != nil {
		select {
		case waitErr := <-waitResult:
			waitResult = nil
			if waitErr = endpointExitError(waitErr); waitErr != nil {
				logDaemonFailure(logger, "daemon API stopped unexpectedly", waitErr)
				exitErr = errors.Join(exitErr, waitErr)
			}
		default:
		}
	}
	if backgroundResult != nil {
		select {
		case backgroundErr := <-backgroundResult:
			backgroundResult = nil
			if backgroundErr = backgroundExitError(backgroundErr); backgroundErr != nil {
				logDaemonFailure(logger, "daemon lifecycle supervisor stopped", backgroundErr)
				exitErr = errors.Join(exitErr, backgroundErr)
			}
		default:
		}
	}

	shutdownErr, apiQuiescent := shutdownEndpoint(endpoint, waitResult, config.shutdown)
	waitResult = nil
	if shutdownErr != nil {
		logDaemonFailure(logger, "daemon API shutdown failed", shutdownErr)
	} else {
		logDaemonInfo(logger, "daemon API stopped")
	}
	stopBackground()
	backgroundErr, backgroundQuiescent := joinBackground(backgroundResult, config.shutdown)
	if !expectedBackgroundShutdown(backgroundErr) {
		logDaemonFailure(logger, "daemon lifecycle supervisor shutdown failed", backgroundErr)
		exitErr = errors.Join(exitErr, backgroundErr)
	}
	runtimeMayClose = apiQuiescent && backgroundQuiescent
	return errors.Join(exitErr, shutdownErr)
}

// endpointExitError classifies an endpoint result observed before runDaemon asks it to shut down.
func endpointExitError(waitErr error) error {
	if waitErr == nil {
		return errAPIStoppedUnexpectedly
	}
	return waitErr
}

// shutdownEndpoint gives Shutdown and the already-running Wait call one shared
// bounded drain interval. Quiescence requires both confirmations unless Wait
// was observed before shutdown began.
func shutdownEndpoint(endpoint managedServer, waitResult <-chan error, timeout time.Duration) (error, bool) {
	shutdownContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- endpoint.Shutdown(shutdownContext)
	}()
	select {
	case err := <-result:
		if err != nil {
			return err, false
		}
	case <-shutdownContext.Done():
		return errors.Join(errAPIShutdownTimeout, shutdownContext.Err()), false
	}
	if waitResult == nil {
		return nil, true
	}
	select {
	case err := <-waitResult:
		return err, true
	case <-shutdownContext.Done():
		return errors.Join(errAPIShutdownTimeout, errAPIWaitUnconfirmed, shutdownContext.Err()), false
	}
}

// joinBackground waits at most one configured shutdown interval for the
// watcher to finish after cancellation; timeout never proves state is idle.
func joinBackground(result <-chan error, timeout time.Duration) (error, bool) {
	if result == nil {
		return nil, true
	}
	joinContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	select {
	case err := <-result:
		return err, true
	case <-joinContext.Done():
		return errors.Join(errBackgroundShutdownTimeout, joinContext.Err()), false
	}
}

// backgroundExitError classifies any watcher result observed before runDaemon requests cancellation.
func backgroundExitError(watcherErr error) error {
	if watcherErr == nil {
		return errors.New("daemon lifecycle supervisor stopped unexpectedly")
	}
	return watcherErr
}

// expectedBackgroundShutdown accepts only nil or the exact cancellation sentinel after an explicit stop request.
func expectedBackgroundShutdown(err error) bool {
	return err == nil || err == context.Canceled
}

// parseDaemonConfig requires every host path on the command line and converts
// prepared-rootfs entries into an immutable trusted source catalog input.
func parseDaemonConfig(arguments []string) (daemonConfig, error) {
	flags := flag.NewFlagSet("mydockerd", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var config daemonConfig
	var prepared preparedRootFSFlags
	flags.StringVar(&config.statePath, "state", "", "absolute durable state file path")
	flags.StringVar(&config.runtimeRoot, "runtime-root", "", "absolute private slim artifact directory")
	flags.StringVar(&config.socketPath, "socket", "", "absolute public Unix socket path")
	flags.StringVar(&config.cgroupRoot, "cgroup-root", "", "absolute delegated cgroup v2 directory")
	flags.StringVar(&config.shimPath, "shim", "", "absolute mydocker-shim executable path")
	flags.Var(&prepared, "prepared-rootfs", "repeatable opaque-id=/absolute/prepared/rootfs mapping")
	flags.DurationVar(&config.shutdown, "shutdown-timeout", defaultShutdownTimeout, "per-phase graceful shutdown bound")
	if err := flags.Parse(arguments); err != nil {
		return daemonConfig{}, err
	}
	if flags.NArg() != 0 {
		return daemonConfig{}, errors.New("mydockerd does not accept positional arguments")
	}
	paths := []struct {
		name string
		path string
	}{
		{"state", config.statePath}, {"runtime-root", config.runtimeRoot}, {"socket", config.socketPath},
		{"cgroup-root", config.cgroupRoot}, {"shim", config.shimPath},
	}
	for _, configured := range paths {
		if err := validateConfiguredPath(configured.name, configured.path); err != nil {
			return daemonConfig{}, err
		}
	}
	if config.shutdown <= 0 {
		return daemonConfig{}, errors.New("shutdown-timeout must be greater than zero")
	}
	sources, err := parsePreparedRootFS(prepared)
	if err != nil {
		return daemonConfig{}, err
	}
	config.preparedRootFS = sources
	return config, nil
}

// validateConfiguredPath rejects missing, relative, non-canonical, NUL-bearing,
// or filesystem-root values before any production dependency is opened.
func validateConfiguredPath(name, path string) error {
	if path == "" || strings.ContainsRune(path, '\x00') || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return fmt.Errorf("%s must be a clean absolute non-root path", name)
	}
	return nil
}

// parsePreparedRootFS binds each opaque API source ID to a clean absolute
// rootfs whose immediate parent becomes its explicit trusted ownership root.
func parsePreparedRootFS(values []string) (map[provider.OpaqueID]isolation.RootfsConfig, error) {
	if len(values) == 0 {
		return nil, errors.New("at least one --prepared-rootfs mapping is required")
	}
	sources := make(map[provider.OpaqueID]isolation.RootfsConfig, len(values))
	for _, value := range values {
		idText, path, found := strings.Cut(value, "=")
		id := provider.OpaqueID(idText)
		if !found || idText == "" || path == "" {
			return nil, errors.New("prepared-rootfs must use opaque-id=/absolute/prepared/rootfs")
		}
		if err := id.Validate(); err != nil {
			return nil, fmt.Errorf("prepared-rootfs ID %q: %w", id, err)
		}
		if _, duplicate := sources[id]; duplicate {
			return nil, fmt.Errorf("prepared-rootfs ID %q is duplicated", id)
		}
		if err := validateConfiguredPath("prepared-rootfs", path); err != nil {
			return nil, err
		}
		source := isolation.RootfsConfig{AllowedRoot: filepath.Dir(path), Rootfs: path}
		if err := source.Validate(); err != nil {
			return nil, fmt.Errorf("prepared-rootfs ID %q: %w", id, err)
		}
		sources[id] = source
	}
	return sources, nil
}

// openProductionRuntime constructs only production implementations, retaining
// the FileStore lock until Close and cleaning it up on any partial wiring error.
func openProductionRuntime(ctx context.Context, config daemonConfig) (_ daemonRuntime, resultErr error) {
	if ctx == nil {
		return nil, errors.New("production runtime context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store, err := state.NewFileStore(config.statePath)
	if err != nil {
		return nil, fmt.Errorf("open durable state: %w", err)
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, store.Close())
		}
	}()
	cgroupManager, err := cgroupv2.NewManager(cgroupv2.Config{Root: config.cgroupRoot}, cgroupv2.OSFileSystem{}, cgroupv2.LinuxHostProbe{})
	if err != nil {
		return nil, err
	}
	launcher, err := slim.NewLinuxShimLauncher(config.runtimeRoot, config.shimPath, cgroupManager)
	if err != nil {
		return nil, err
	}
	catalog, err := slim.NewStaticSourceCatalog(config.preparedRootFS)
	if err != nil {
		return nil, err
	}
	isolationProvider, err := slim.New(slim.Config{RuntimeRoot: config.runtimeRoot, Launcher: launcher, Sources: catalog})
	if err != nil {
		return nil, err
	}
	logRegistry, err := daemon.NewRuntimeLogRegistry(config.runtimeRoot)
	if err != nil {
		return nil, err
	}
	cgroupProvider, err := slim.NewCgroupProvider(cgroupManager, isolationProvider)
	if err != nil {
		return nil, err
	}
	rollbackRegistry, err := newProductionRollbackRegistry(cgroupProvider, isolationProvider)
	if err != nil {
		return nil, err
	}
	runtimeEngine, err := engine.New(store, engine.Providers{
		Cgroup: cgroupProvider, Isolation: isolationProvider, Supervisor: isolationProvider, Rollback: rollbackRegistry,
	})
	if err != nil {
		return nil, err
	}
	apiService, err := daemon.NewServiceWithInfo(runtimeEngine, logRegistry, collectDaemonInfo())
	if err != nil {
		return nil, err
	}
	return &productionRuntime{
		store: store, cgroup: cgroupProvider, isolation: isolationProvider, engine: runtimeEngine, service: apiService,
	}, nil
}

// newProductionRollbackRegistry binds every persisted inverse route to the
// exact receipt-verifying removal method owned by its production provider.
func newProductionRollbackRegistry(cgroups provider.CgroupProvider, isolationProvider provider.IsolationProvider) (*provider.RollbackRegistry, error) {
	if cgroups == nil || isolationProvider == nil {
		return nil, errors.New("production rollback providers must not be nil")
	}
	return provider.NewRollbackRegistry(
		provider.RollbackRegistration{Provider: ownership.ProviderCgroupV2, Action: ownership.ActionRemoveCgroup, Handler: func(ctx context.Context, owner ownership.OwnerKey, receipt ownership.Receipt) (provider.CleanupObservation, error) {
			return removeCgroupReceipt(ctx, cgroups, owner, receipt)
		}},
		provider.RollbackRegistration{Provider: ownership.ProviderLinux, Action: ownership.ActionStopProcess, Handler: func(ctx context.Context, owner ownership.OwnerKey, receipt ownership.Receipt) (provider.CleanupObservation, error) {
			return isolationProvider.RemoveProcess(ctx, provider.OwnedReceiptRequest{Owner: owner, Receipt: receipt})
		}},
		provider.RollbackRegistration{Provider: ownership.ProviderLinux, Action: ownership.ActionUnmountRoot, Handler: func(ctx context.Context, owner ownership.OwnerKey, receipt ownership.Receipt) (provider.CleanupObservation, error) {
			return isolationProvider.RemoveRootfs(ctx, provider.OwnedReceiptRequest{Owner: owner, Receipt: receipt})
		}},
		provider.RollbackRegistration{Provider: ownership.ProviderLinux, Action: ownership.ActionCloseGate, Handler: func(ctx context.Context, owner ownership.OwnerKey, receipt ownership.Receipt) (provider.CleanupObservation, error) {
			return isolationProvider.RemoveStartGate(ctx, provider.OwnedReceiptRequest{Owner: owner, Receipt: receipt})
		}},
		provider.RollbackRegistration{Provider: ownership.ProviderLinux, Action: ownership.ActionCloseStreams, Handler: func(ctx context.Context, owner ownership.OwnerKey, receipt ownership.Receipt) (provider.CleanupObservation, error) {
			return isolationProvider.RemoveStreams(ctx, provider.OwnedReceiptRequest{Owner: owner, Receipt: receipt})
		}},
	)
}

// removeCgroupReceipt dispatches a rollback only by the validated receipt kind;
// no receipt local ID or attribute is interpreted as a host path.
func removeCgroupReceipt(ctx context.Context, cgroups provider.CgroupProvider, owner ownership.OwnerKey, receipt ownership.Receipt) (provider.CleanupObservation, error) {
	request := provider.OwnedReceiptRequest{Owner: owner, Receipt: receipt}
	switch receipt.Kind {
	case ownership.KindSandboxCgroup:
		return cgroups.RemoveSandboxCgroup(ctx, request)
	case ownership.KindKeeperCgroup:
		return cgroups.RemoveKeeperCgroup(ctx, request)
	case ownership.KindAttemptCgroup:
		return cgroups.RemoveAttemptCgroup(ctx, request)
	default:
		return provider.CleanupObservation{}, fmt.Errorf("unsupported cgroup rollback receipt kind %q", receipt.Kind)
	}
}

// Preflight performs both provider checks read-only before recovery may mutate host state.
func (runtime *productionRuntime) Preflight(ctx context.Context) error {
	if runtime == nil {
		return errors.New("production runtime must not be nil")
	}
	return preflightHost(ctx, runtime.cgroup, runtime.isolation)
}

// preflightHost verifies the complete M2 capability set without mutating persistence or host resources.
func preflightHost(ctx context.Context, cgroups cgroupCapabilityInspector, isolationProvider isolationCapabilityInspector) error {
	if ctx == nil {
		return errors.New("host preflight context must not be nil")
	}
	if cgroups == nil || isolationProvider == nil {
		return errors.New("host preflight providers must not be nil")
	}
	requirements := provider.M2Requirements()
	isolationCapabilities, err := isolationProvider.InspectIsolationCapabilities(ctx, requirements.Isolation)
	if err != nil {
		return fmt.Errorf("inspect isolation capabilities: %w", err)
	}
	cgroupCapabilities, err := cgroups.InspectCgroupCapabilities(ctx, requirements.Cgroup)
	if err != nil {
		return fmt.Errorf("inspect cgroup capabilities: %w", err)
	}
	capabilities := provider.Capabilities{
		SchemaVersion: provider.SchemaVersion, Cgroup: cgroupCapabilities, Isolation: isolationCapabilities,
	}
	return capabilities.Satisfies(requirements)
}

// Reconcile completes Engine's global discovery barrier and all required
// recovery actions before API construction or socket binding is permitted.
func (runtime *productionRuntime) Reconcile(ctx context.Context) error {
	if runtime == nil || runtime.engine == nil {
		return errors.New("production runtime engine must not be nil")
	}
	_, err := runtime.engine.Reconcile(ctx)
	return err
}

// RunBackground continuously projects naturally exited workload children into durable stopped lifecycle facts.
func (runtime *productionRuntime) RunBackground(ctx context.Context) error {
	if runtime == nil || runtime.engine == nil {
		return errors.New("production runtime engine must not be nil")
	}
	return runtime.engine.WatchRuntime(ctx, terminalWatchInterval)
}

// APIService returns the already wired transport adapter after successful
// preflight and recovery; runDaemon controls when the endpoint may expose it.
func (runtime *productionRuntime) APIService() server.Service {
	if runtime == nil {
		return nil
	}
	return runtime.service
}

// Close releases the durable state lock; host resources remain governed by
// their persisted lifecycle and are never implicitly destroyed on shutdown.
func (runtime *productionRuntime) Close() error {
	if runtime == nil || runtime.store == nil {
		return nil
	}
	return runtime.store.Close()
}

// newProductionServer constructs an unstarted strict v1 HTTP-over-UDS server;
// filesystem binding remains exclusively inside its later Start call.
func newProductionServer(config server.Config, service server.Service) (managedServer, error) {
	return server.New(config, service)
}

// logDaemonInfo emits one fixed low-detail process-lifecycle fact. Per-request
// diagnostic correlation is not yet wired; durable stage events remain the
// current operation-correlated source and concrete IDs never become metrics.
func logDaemonInfo(logger *observability.JSONLogger, message string) {
	if logger == nil {
		return
	}
	_ = logger.Write(observability.LogRecord{Level: observability.LevelInfo, Message: message})
}

// logDaemonFailure emits one bounded single-line diagnostic while returning
// the original error separately to preserve errors.Is behavior for callers.
func logDaemonFailure(logger *observability.JSONLogger, message string, err error) {
	if logger == nil {
		return
	}
	_ = logger.Write(observability.LogRecord{Level: observability.LevelError, Message: message, Error: boundedError(err)})
}

// boundedError replaces control characters and caps diagnostic text so logger
// validation cannot turn an original startup failure into a missing record.
func boundedError(err error) string {
	if err == nil {
		return "unknown error"
	}
	text := strings.Map(func(character rune) rune {
		if unicode.IsControl(character) {
			return ' '
		}
		return character
	}, err.Error())
	text = strings.TrimSpace(text)
	if len(text) > 8192 {
		text = text[:8192]
	}
	if text == "" {
		return "unknown error"
	}
	return text
}
