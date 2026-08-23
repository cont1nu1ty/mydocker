package slim

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"mydocker/internal/cgroupv2"
	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
	"mydocker/internal/shim"
)

// TestLinuxShimLauncherPreflightUsesOnlyInjectedReadOnlyProofs verifies full
// capabilities are reported only after the manager, factory, executable, and host seams succeed.
func TestLinuxShimLauncherPreflightUsesOnlyInjectedReadOnlyProofs(t *testing.T) {
	fixture := newKeeperLauncherFixture(t)
	requirements := provider.M2Requirements().Isolation
	capabilities, err := fixture.launcher.Preflight(context.Background(), requirements)
	if err != nil {
		t.Fatal(err)
	}
	if err := capabilities.Validate(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(capabilities.Namespaces, requirements.Namespaces) || !capabilities.Rootful || !capabilities.PIDFD || !capabilities.PivotRoot || !capabilities.StartGate || !capabilities.Streams {
		t.Fatalf("capabilities=%+v, want exact requirements", capabilities)
	}
	if fixture.manager.preflightCalls != 1 || fixture.factory.preflightCalls != 1 || fixture.host.preflightCalls != 1 || fixture.host.validatedExecutable != fixture.executable {
		t.Fatalf("preflight calls manager=%d factory=%d host=%d executable=%q", fixture.manager.preflightCalls, fixture.factory.preflightCalls, fixture.host.preflightCalls, fixture.host.validatedExecutable)
	}
	fixture.factory.preflightErr = errors.New("clone3 feature probe rejected")
	if _, err := fixture.launcher.Preflight(context.Background(), requirements); !errors.Is(err, fixture.factory.preflightErr) {
		t.Fatalf("factory failure error=%v", err)
	}
}

// TestLinuxShimLauncherEnsureKeeperPersistsBeforeRelease verifies intent exists
// before Start, exact ProcessEvidence is durable before Release, and ready precedes Commit.
func TestLinuxShimLauncherEnsureKeeperPersistsBeforeRelease(t *testing.T) {
	fixture := newKeeperLauncherFixture(t)
	fixture.factory.onStart = func(spec ProcessLaunchSpec) error {
		journal := readKeeperLaunchJournal(t, fixture)
		if journal.Phase != launchPhaseIntent {
			return errors.New("process started before durable intent")
		}
		fixture.manager.setMembers(fixture.evidence.PID)
		return nil
	}
	fixture.started.onRelease = func() error {
		journal := readKeeperLaunchJournal(t, fixture)
		if journal.Phase != launchPhaseAuthorized || journal.ProcessEvidence == nil || *journal.ProcessEvidence != fixture.evidence {
			return errors.New("release preceded durable exact process evidence")
		}
		return nil
	}
	fixture.started.onCommit = func() error {
		if readKeeperLaunchJournal(t, fixture).Phase != launchPhaseReady {
			return errors.New("commit preceded durable readiness")
		}
		return nil
	}
	result, err := fixture.launcher.EnsureKeeper(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Validate(); err != nil || result.ProcessEvidence != fixture.evidence {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if fixture.factory.starts != 1 || fixture.started.releases != 1 || fixture.started.commits != 1 || fixture.started.aborts != 0 {
		t.Fatalf("starts=%d releases=%d commits=%d aborts=%d", fixture.factory.starts, fixture.started.releases, fixture.started.commits, fixture.started.aborts)
	}
	wantArguments := []string{
		"-config", fixture.request.Paths.Config,
		"-config-evidence", result.WrapperEvidenceSHA256,
		"-release-fd", "3",
	}
	if fixture.factory.spec.Executable != fixture.executable || fixture.factory.spec.CgroupFD != 42 || fixture.factory.spec.CloneFlags != fixture.host.keeperFlags || fixture.factory.spec.ReleaseFD != 3 || !reflect.DeepEqual(fixture.factory.spec.Arguments, wantArguments) {
		t.Fatalf("launch spec=%+v", fixture.factory.spec)
	}
	if !reflect.DeepEqual(fixture.factory.spec.Environment, minimalShimEnvironment()) {
		t.Fatalf("shim environment=%v, want explicit minimum", fixture.factory.spec.Environment)
	}
	if !fixture.control.sawDeadline {
		t.Fatal("background readiness exchange lacked a bounded deadline")
	}
}

// TestLinuxShimLauncherEnsureKeeperRecoversAuthorizedProcess verifies response
// loss or daemon restart adopts exact evidence, authenticates its socket, and never forks again.
func TestLinuxShimLauncherEnsureKeeperRecoversAuthorizedProcess(t *testing.T) {
	fixture := newKeeperLauncherFixture(t)
	config, intent := ensureKeeperIntent(t, fixture)
	authorized, err := intent.withProcess(launchPhaseAuthorized, fixture.evidence)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newLaunchStore(fixture.request.Paths, fixture.request.Owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(intent, authorized); err != nil {
		t.Fatal(err)
	}
	fixture.runtime.present = true
	fixture.manager.setMembers(fixture.evidence.PID)
	fixture.control.unavailable = 1
	result, err := fixture.launcher.EnsureKeeper(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if result.ProcessEvidence != fixture.evidence || result.WrapperEvidenceSHA256 != config.WrapperEvidence {
		t.Fatalf("recovered result=%+v", result)
	}
	if fixture.factory.starts != 0 || fixture.runtime.restoreCalls != 1 || fixture.control.calls != 2 {
		t.Fatalf("starts=%d restores=%d controls=%d", fixture.factory.starts, fixture.runtime.restoreCalls, fixture.control.calls)
	}
	if readKeeperLaunchJournal(t, fixture).Phase != launchPhaseReady {
		t.Fatal("authorized recovery did not durably advance to ready")
	}
}

// TestLinuxShimLauncherEnsureKeeperWaitsForIntentCrashChild verifies a daemon
// restart waits for the gate-EOF child to leave its leaf and relaunches in one call.
func TestLinuxShimLauncherEnsureKeeperWaitsForIntentCrashChild(t *testing.T) {
	fixture := newKeeperLauncherFixture(t)
	_, _ = ensureKeeperIntent(t, fixture)
	fixture.manager.memberSnapshots = [][]int{{fixture.evidence.PID}, {}}
	fixture.factory.onStart = func(ProcessLaunchSpec) error {
		fixture.manager.setMembers(fixture.evidence.PID)
		return nil
	}
	result, err := fixture.launcher.EnsureKeeper(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if result.ProcessEvidence != fixture.evidence || fixture.factory.starts != 1 || fixture.manager.membershipReads < 2 {
		t.Fatalf("result=%+v starts=%d membership reads=%d", result, fixture.factory.starts, fixture.manager.membershipReads)
	}
}

// TestLinuxShimLauncherEnsureKeeperKillsStuckAuthorizedChildAndRelaunches verifies
// a pre-release crash is killed by exact evidence, reset, and replaced once in the same call.
func TestLinuxShimLauncherEnsureKeeperKillsStuckAuthorizedChildAndRelaunches(t *testing.T) {
	fixture := newKeeperLauncherFixture(t)
	fixture.launcher.readinessTimeout = 5 * time.Millisecond
	config, intent := ensureKeeperIntent(t, fixture)
	authorized, err := intent.withProcess(launchPhaseAuthorized, fixture.evidence)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newLaunchStore(fixture.request.Paths, fixture.request.Owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(intent, authorized); err != nil {
		t.Fatal(err)
	}
	fixture.runtime.present = true
	fixture.manager.setMembers(fixture.evidence.PID)
	fixture.control.unavailable = 1 << 20
	fixture.runtime.onSignal = func() {
		fixture.runtime.present = false
		fixture.manager.setMembers()
		fixture.control.unavailable = 0
	}
	fixture.factory.onStart = func(ProcessLaunchSpec) error {
		fixture.manager.setMembers(fixture.evidence.PID)
		return nil
	}
	result, err := fixture.launcher.EnsureKeeper(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if result.ProcessEvidence != fixture.evidence || result.WrapperEvidenceSHA256 != config.WrapperEvidence {
		t.Fatalf("result=%+v", result)
	}
	if fixture.runtime.restoreCalls != 1 || fixture.runtime.signalCalls != 1 || fixture.factory.starts != 1 || fixture.started.aborts != 0 {
		t.Fatalf("restores=%d signals=%d starts=%d aborts=%d", fixture.runtime.restoreCalls, fixture.runtime.signalCalls, fixture.factory.starts, fixture.started.aborts)
	}
	if readKeeperLaunchJournal(t, fixture).Phase != launchPhaseReady {
		t.Fatal("same-call authorized recovery did not end ready")
	}
}

// TestLinuxShimLauncherEnsureKeeperAcceptsExactAbsenceAfterSignalRace verifies
// an already-exited signal error cannot defeat exact absence proof and same-call relaunch.
func TestLinuxShimLauncherEnsureKeeperAcceptsExactAbsenceAfterSignalRace(t *testing.T) {
	fixture := newKeeperLauncherFixture(t)
	fixture.launcher.readinessTimeout = 5 * time.Millisecond
	_, intent := ensureKeeperIntent(t, fixture)
	authorized, err := intent.withProcess(launchPhaseAuthorized, fixture.evidence)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newLaunchStore(fixture.request.Paths, fixture.request.Owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(intent, authorized); err != nil {
		t.Fatal(err)
	}
	fixture.runtime.present = true
	fixture.manager.setMembers(fixture.evidence.PID)
	fixture.control.unavailable = 1 << 20
	fixture.runtime.signalErr = errors.New("fake pidfd reports already exited")
	fixture.runtime.onSignal = func() {
		fixture.runtime.present = false
		fixture.manager.setMembers()
		fixture.control.unavailable = 0
	}
	fixture.factory.onStart = func(ProcessLaunchSpec) error {
		fixture.manager.setMembers(fixture.evidence.PID)
		return nil
	}
	result, err := fixture.launcher.EnsureKeeper(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if result.ProcessEvidence != fixture.evidence || fixture.factory.starts != 1 || fixture.runtime.signalCalls != 1 {
		t.Fatalf("result=%+v starts=%d signals=%d", result, fixture.factory.starts, fixture.runtime.signalCalls)
	}
}

// TestLinuxShimLauncherEnsureKeeperRejectsSignalErrorWithoutAbsence verifies a
// failed exact signal cannot reset or fork while the journaled process remains present.
func TestLinuxShimLauncherEnsureKeeperRejectsSignalErrorWithoutAbsence(t *testing.T) {
	fixture := newKeeperLauncherFixture(t)
	fixture.launcher.readinessTimeout = 4 * time.Millisecond
	fixture.launcher.cleanupTimeout = 4 * time.Millisecond
	_, intent := ensureKeeperIntent(t, fixture)
	authorized, err := intent.withProcess(launchPhaseAuthorized, fixture.evidence)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newLaunchStore(fixture.request.Paths, fixture.request.Owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(intent, authorized); err != nil {
		t.Fatal(err)
	}
	fixture.runtime.present = true
	fixture.manager.setMembers(fixture.evidence.PID)
	fixture.control.unavailable = 1 << 20
	fixture.runtime.signalErr = errors.New("injected signal failure with live process")
	if _, err := fixture.launcher.EnsureKeeper(context.Background(), fixture.request); err == nil {
		t.Fatal("live process after signal failure was treated as absent")
	}
	if fixture.factory.starts != 0 || readKeeperLaunchJournal(t, fixture).Phase != launchPhaseAuthorized {
		t.Fatalf("starts=%d journal=%s, want zero fork and authorized evidence", fixture.factory.starts, readKeeperLaunchJournal(t, fixture).Phase)
	}
}

// TestLinuxShimLauncherEnsureKeeperDoesNotForkWhileOldIntentMemberPersists verifies
// bounded non-convergence returns an error without trusting, signaling, or duplicating a raw PID.
func TestLinuxShimLauncherEnsureKeeperDoesNotForkWhileOldIntentMemberPersists(t *testing.T) {
	fixture := newKeeperLauncherFixture(t)
	fixture.launcher.readinessTimeout = 4 * time.Millisecond
	_, _ = ensureKeeperIntent(t, fixture)
	fixture.manager.setMembers(fixture.evidence.PID)
	if _, err := fixture.launcher.EnsureKeeper(context.Background(), fixture.request); err == nil {
		t.Fatal("persistent unjournaled member reported success")
	}
	if fixture.factory.starts != 0 || fixture.runtime.signalCalls != 0 {
		t.Fatalf("starts=%d signals=%d, want zero unsafe effects", fixture.factory.starts, fixture.runtime.signalCalls)
	}
}

// TestLinuxShimLauncherEnsureKeeperRejectsWrongControlPeer verifies a forged
// socket cannot authorize readiness and the exact unreleased child is aborted.
func TestLinuxShimLauncherEnsureKeeperRejectsWrongControlPeer(t *testing.T) {
	fixture := newKeeperLauncherFixture(t)
	fixture.factory.onStart = func(ProcessLaunchSpec) error {
		fixture.manager.setMembers(fixture.evidence.PID)
		return nil
	}
	fixture.started.onAbort = func() error {
		fixture.manager.setMembers()
		fixture.runtime.present = false
		return nil
	}
	fixture.control.peerPID = fixture.evidence.PID + 1
	if _, err := fixture.launcher.EnsureKeeper(context.Background(), fixture.request); err == nil {
		t.Fatal("wrong SO_PEERCRED PID was accepted")
	}
	if fixture.started.aborts != 1 || fixture.started.commits != 0 {
		t.Fatalf("aborts=%d commits=%d", fixture.started.aborts, fixture.started.commits)
	}
}

// keeperLauncherFixture groups syscall-free production launcher seams and durable owner artifacts.
type keeperLauncherFixture struct {
	launcher   *LinuxShimLauncher
	manager    *fakeKeeperCgroupManager
	factory    *fakeKeeperProcessFactory
	started    *fakeKeeperStartedProcess
	runtime    *fakeKeeperProcessRuntime
	control    *fakeKeeperControlClient
	host       *fakeKeeperHost
	request    KeeperLaunch
	evidence   isolation.ProcessEvidence
	executable string
}

// newKeeperLauncherFixture constructs private files and fakes without executing any real process, signal, namespace, or cgroup operation.
func newKeeperLauncherFixture(t *testing.T) keeperLauncherFixture {
	t.Helper()
	runtimeRoot := privateSlimRoot(t)
	owner := testOwner(t, "op-production-keeper", operation.TargetSandbox, "sandbox-production-keeper")
	artifacts, err := newArtifactStore(runtimeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := artifacts.ensureOwnerDirectory(owner); err != nil {
		t.Fatal(err)
	}
	paths, err := deriveArtifactPaths(runtimeRoot, owner)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := newCgroupReceipt(owner, ownership.KindKeeperCgroup, "sandbox-production-keeper", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	executable := "/usr/libexec/mydocker-shim"
	evidence := isolation.ProcessEvidence{
		PID: 4242, BootID: "fake-production-boot", StartTime: 98765,
		CgroupPath: "/mydocker/keeper", Executable: executable,
	}
	manager := &fakeKeeperCgroupManager{root: "/sys/fs/cgroup/mydocker", directory: &fakeKeeperDirectory{fd: 42}}
	started := &fakeKeeperStartedProcess{pid: evidence.PID, pidfd: 70}
	factory := &fakeKeeperProcessFactory{started: started}
	runtime := &fakeKeeperProcessRuntime{evidence: evidence}
	control := &fakeKeeperControlClient{peerPID: evidence.PID}
	host := &fakeKeeperHost{keeperFlags: 0x70000000, initFlags: 0x30000000}
	launcher, err := newLinuxShimLauncher(linuxShimLauncherConfig{
		runtimeRoot: runtimeRoot, executable: executable, cgroups: manager,
		factory: factory, processes: runtime, namespaces: fakeKeeperNamespaceRuntime{}, control: control, host: host,
		readinessTimeout: time.Second, cleanupTimeout: time.Second, pollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture := keeperLauncherFixture{
		launcher: launcher, manager: manager, factory: factory, started: started,
		runtime: runtime, control: control, host: host, evidence: evidence, executable: executable,
		request: KeeperLaunch{Owner: owner, SandboxID: "sandbox-production-keeper", Cgroup: receipt, Paths: paths},
	}
	control.fixture = &fixture
	return fixture
}

// ensureKeeperIntent persists and returns the immutable config plus initial journal without launching a process.
func ensureKeeperIntent(t *testing.T, fixture keeperLauncherFixture) (shim.RuntimeConfig, launchJournal) {
	t.Helper()
	store, err := newLaunchStore(fixture.request.Paths, fixture.request.Owner)
	if err != nil {
		t.Fatal(err)
	}
	config, journal, err := store.EnsureIntent(shim.RuntimeConfig{
		SchemaVersion: shim.SchemaVersion, Mode: shim.ModeKeeper, Owner: fixture.request.Owner,
		SandboxID: fixture.request.SandboxID, ControlSocket: fixture.request.Paths.ControlSocket,
	})
	if err != nil {
		t.Fatal(err)
	}
	return config, journal
}

// readKeeperLaunchJournal reads one validated fixture journal for ordering assertions inside fake side effects.
func readKeeperLaunchJournal(t *testing.T, fixture keeperLauncherFixture) launchJournal {
	t.Helper()
	store, err := newLaunchStore(fixture.request.Paths, fixture.request.Owner)
	if err != nil {
		t.Fatal(err)
	}
	journal, found, err := store.Read()
	if err != nil || !found {
		t.Fatalf("read launch journal found=%t error=%v", found, err)
	}
	return journal
}

// fakeKeeperDirectory is a non-OS cgroup directory identity used only for clone spec assertions.
type fakeKeeperDirectory struct {
	fd     uintptr
	closed bool
}

// Fd returns the inert descriptor number expected in the fake ProcessLaunchSpec.
func (directory *fakeKeeperDirectory) Fd() uintptr { return directory.fd }

// Close records ownership release without closing a real descriptor.
func (directory *fakeKeeperDirectory) Close() error {
	directory.closed = true
	return nil
}

// fakeKeeperCgroupManager models exact membership and never writes a cgroup filesystem.
type fakeKeeperCgroupManager struct {
	mu              sync.Mutex
	root            string
	directory       *fakeKeeperDirectory
	members         []int
	memberSnapshots [][]int
	preflightCalls  int
	confirmCalls    int
	membershipReads int
}

// Root returns the configured fake delegated root used by host preflight assertions.
func (manager *fakeKeeperCgroupManager) Root() string { return manager.root }

// Preflight records a read-only fake manager probe.
func (manager *fakeKeeperCgroupManager) Preflight(ctx context.Context) error {
	manager.preflightCalls++
	return ctx.Err()
}

// OpenKeeper returns a fresh inert descriptor handle for clone-time placement.
func (manager *fakeKeeperCgroupManager) OpenKeeper(ctx context.Context, _ domain.SandboxID) (cgroupv2.DirectoryHandle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	manager.directory = &fakeKeeperDirectory{fd: 42}
	return manager.directory, nil
}

// OpenAttempt is unused by keeper tests and fails if the launcher crosses the role boundary.
func (manager *fakeKeeperCgroupManager) OpenAttempt(context.Context, domain.SandboxID, domain.AttemptID) (cgroupv2.DirectoryHandle, error) {
	return nil, errors.New("unexpected Attempt cgroup open")
}

// KeeperProcessIDs returns a defensive snapshot of fake exact leaf membership.
func (manager *fakeKeeperCgroupManager) KeeperProcessIDs(ctx context.Context, _ domain.SandboxID) ([]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.membershipReads++
	if len(manager.memberSnapshots) > 0 {
		next := append([]int(nil), manager.memberSnapshots[0]...)
		manager.memberSnapshots = manager.memberSnapshots[1:]
		manager.members = append([]int(nil), next...)
		return next, nil
	}
	return append([]int(nil), manager.members...), nil
}

// AttemptProcessIDs is unused by keeper tests and fails if init recovery is invoked.
func (manager *fakeKeeperCgroupManager) AttemptProcessIDs(context.Context, domain.SandboxID, domain.AttemptID) ([]int, error) {
	return nil, errors.New("unexpected Attempt membership read")
}

// ConfirmKeeperProcess checks the strongly verified fake PID is currently an exact leaf member.
func (manager *fakeKeeperCgroupManager) ConfirmKeeperProcess(ctx context.Context, _ domain.SandboxID, process cgroupv2.ProcessReference) error {
	pid, err := process.VerifiedPID(ctx)
	if err != nil {
		return err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.confirmCalls++
	for _, member := range manager.members {
		if member == pid {
			return nil
		}
	}
	return errors.New("fake verified PID is absent from keeper leaf")
}

// AttachProcess is unused by keeper tests and fails if init attachment is invoked.
func (manager *fakeKeeperCgroupManager) AttachProcess(context.Context, domain.SandboxID, domain.AttemptID, cgroupv2.ProcessReference) error {
	return errors.New("unexpected Attempt process attachment")
}

// setMembers atomically replaces the fake cgroup member snapshot for launch and cleanup callbacks.
func (manager *fakeKeeperCgroupManager) setMembers(members ...int) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.members = append([]int(nil), members...)
}

// fakeKeeperProcessFactory captures launch specs and returns an inert gated process state machine.
type fakeKeeperProcessFactory struct {
	started        *fakeKeeperStartedProcess
	spec           ProcessLaunchSpec
	starts         int
	preflightCalls int
	preflightErr   error
	onStart        func(ProcessLaunchSpec) error
}

// Preflight records the injected clone3 feature result without a syscall.
func (factory *fakeKeeperProcessFactory) Preflight(ctx context.Context) error {
	factory.preflightCalls++
	return errors.Join(ctx.Err(), factory.preflightErr)
}

// Start records one already validated spec and invokes a pure ordering callback.
func (factory *fakeKeeperProcessFactory) Start(ctx context.Context, spec ProcessLaunchSpec) (StartedProcess, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	factory.starts++
	factory.spec = spec
	if factory.onStart != nil {
		if err := factory.onStart(spec); err != nil {
			return nil, err
		}
	}
	return factory.started, nil
}

// fakeKeeperStartedProcess models release/abort/commit without exec, pidfd signaling, or wait syscalls.
type fakeKeeperStartedProcess struct {
	pid       int
	pidfd     int
	releases  int
	aborts    int
	commits   int
	onRelease func() error
	onAbort   func() error
	onCommit  func() error
}

// PID returns the fake child number paired with the inert pidfd token.
func (process *fakeKeeperStartedProcess) PID() int { return process.pid }

// TakePIDFD transfers the inert pidfd token once.
func (process *fakeKeeperStartedProcess) TakePIDFD() (int, error) {
	if process.pidfd < 0 {
		return -1, errors.New("fake pidfd already transferred")
	}
	fd := process.pidfd
	process.pidfd = -1
	return fd, nil
}

// Release records gate authorization after invoking the durable-journal assertion.
func (process *fakeKeeperStartedProcess) Release() error {
	process.releases++
	if process.onRelease != nil {
		return process.onRelease()
	}
	return nil
}

// Abort records exact cleanup and invokes the fake membership removal callback.
func (process *fakeKeeperStartedProcess) Abort(ctx context.Context) error {
	process.aborts++
	if err := ctx.Err(); err != nil {
		return err
	}
	if process.onAbort != nil {
		return process.onAbort()
	}
	return nil
}

// Commit records launch-handle release after invoking the ready-journal assertion.
func (process *fakeKeeperStartedProcess) Commit() error {
	process.commits++
	if process.onCommit != nil {
		return process.onCommit()
	}
	return nil
}

// fakeKeeperProcessRuntime returns fresh inert strong handles from configured evidence.
type fakeKeeperProcessRuntime struct {
	evidence     isolation.ProcessEvidence
	present      bool
	captureCalls int
	restoreCalls int
	signalCalls  int
	signalErr    error
	onSignal     func()
}

// CaptureFromPIDFD binds the supplied fake PID to the configured evidence without opening procfs.
func (runtime *fakeKeeperProcessRuntime) CaptureFromPIDFD(ctx context.Context, pid, _ int, executable string) (launcherProcessHandle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pid != runtime.evidence.PID || executable != runtime.evidence.Executable {
		return nil, errors.New("fake clone-time identity mismatch")
	}
	runtime.captureCalls++
	runtime.present = true
	return &fakeKeeperProcessHandle{evidence: runtime.evidence, runtime: runtime}, nil
}

// CapturePeer is unused because the launcher binds SO_PEERCRED to its already restored exact handle.
func (runtime *fakeKeeperProcessRuntime) CapturePeer(context.Context, int, string) (launcherProcessHandle, error) {
	return nil, errors.New("unexpected peer capture")
}

// Restore reopens a fresh inert handle only for byte-exact persisted evidence.
func (runtime *fakeKeeperProcessRuntime) Restore(ctx context.Context, evidence isolation.ProcessEvidence) (launcherProcessHandle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if evidence != runtime.evidence {
		return nil, errors.New("fake restore evidence mismatch")
	}
	runtime.restoreCalls++
	return &fakeKeeperProcessHandle{evidence: evidence, runtime: runtime}, nil
}

// Present returns the configured exact-evidence liveness result.
func (runtime *fakeKeeperProcessRuntime) Present(ctx context.Context, evidence isolation.ProcessEvidence) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if evidence != runtime.evidence {
		return false, errors.New("fake presence evidence mismatch")
	}
	return runtime.present, nil
}

// fakeKeeperProcessHandle is an inert action-time-verifying process reference.
type fakeKeeperProcessHandle struct {
	evidence isolation.ProcessEvidence
	runtime  *fakeKeeperProcessRuntime
	closed   bool
}

// VerifiedPID returns the PID only while the fake handle remains open.
func (handle *fakeKeeperProcessHandle) VerifiedPID(ctx context.Context) (int, error) {
	if err := handle.Verify(ctx); err != nil {
		return 0, err
	}
	return handle.evidence.PID, nil
}

// Evidence returns the immutable serializable fake identity.
func (handle *fakeKeeperProcessHandle) Evidence() (isolation.ProcessEvidence, error) {
	if handle.closed {
		return isolation.ProcessEvidence{}, errors.New("fake process handle is closed")
	}
	return handle.evidence, nil
}

// Verify checks cancellation and fake handle lifetime without a syscall.
func (handle *fakeKeeperProcessHandle) Verify(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if handle.closed {
		return errors.New("fake process handle is closed")
	}
	return nil
}

// Signal records no real signal, invokes deterministic fake exit behavior, and verifies handle lifetime.
func (handle *fakeKeeperProcessHandle) Signal(ctx context.Context, _ int) error {
	if err := handle.Verify(ctx); err != nil {
		return err
	}
	if handle.runtime != nil {
		handle.runtime.signalCalls++
		if handle.runtime.onSignal != nil {
			handle.runtime.onSignal()
		}
		return handle.runtime.signalErr
	}
	return nil
}

// WaitForExit records no real wait and only verifies caller cancellation.
func (handle *fakeKeeperProcessHandle) WaitForExit(ctx context.Context, _ time.Duration) error {
	return ctx.Err()
}

// Close releases only the inert handle state.
func (handle *fakeKeeperProcessHandle) Close() error {
	handle.closed = true
	return nil
}

// fakeKeeperNamespaceRuntime rejects namespace work during the keeper-only batch.
type fakeKeeperNamespaceRuntime struct{}

// Open fails if keeper launch unexpectedly tries to open a namespace handle.
func (fakeKeeperNamespaceRuntime) Open(context.Context, launcherProcessHandle, isolation.NamespaceType) (launcherNamespaceHandle, error) {
	return nil, errors.New("unexpected namespace open")
}

// Configure fails if keeper launch unexpectedly tries to join or mutate a namespace.
func (fakeKeeperNamespaceRuntime) Configure(context.Context, launcherNamespaceHandle, NamespaceLaunch) error {
	return errors.New("unexpected namespace configuration")
}

// VerifyConfiguration fails if the keeper-only batch unexpectedly inspects a
// namespace configuration before an init or action-time request owns it.
func (fakeKeeperNamespaceRuntime) VerifyConfiguration(context.Context, launcherNamespaceHandle, NamespaceLaunch) error {
	return errors.New("unexpected namespace configuration verification")
}

// CloseFD fails if keeper launch unexpectedly creates a namespace descriptor duplicate.
func (fakeKeeperNamespaceRuntime) CloseFD(int) error {
	return errors.New("unexpected namespace descriptor close")
}

// fakeKeeperControlClient builds a real in-memory keeper observation from the durable config while performing no socket I/O.
type fakeKeeperControlClient struct {
	fixture     *keeperLauncherFixture
	peerPID     int
	unavailable int
	calls       int
	sawDeadline bool
}

// Exchange validates bounded caller context and returns a checksummed in-memory shim response.
func (client *fakeKeeperControlClient) Exchange(ctx context.Context, path string, request shim.ControlRequest) (shim.ControlResponse, int, error) {
	client.calls++
	_, client.sawDeadline = ctx.Deadline()
	if client.unavailable > 0 {
		client.unavailable--
		return shim.ControlResponse{}, 0, &shim.Error{Code: shim.CodeUnavailable, Message: "injected response loss"}
	}
	if filepath.Clean(path) != client.fixture.request.Paths.ControlSocket {
		return shim.ControlResponse{}, 0, errors.New("fake control path mismatch")
	}
	config, err := shim.LoadRuntimeConfig(client.fixture.request.Paths.Config)
	if err != nil {
		return shim.ControlResponse{}, 0, err
	}
	wrapper, err := shim.NewKeeper(config.KeeperSpec())
	if err != nil {
		return shim.ControlResponse{}, 0, err
	}
	return wrapper.HandleControl(request), client.peerPID, nil
}

// fakeKeeperHost records read-only checks and supplies inert nonzero clone flags.
type fakeKeeperHost struct {
	keeperFlags         uintptr
	initFlags           uintptr
	preflightCalls      int
	validatedExecutable string
}

// Preflight verifies the launcher passed the fake manager's exact root and records no side effect.
func (host *fakeKeeperHost) Preflight(ctx context.Context, root string, _ provider.IsolationRequirements) error {
	host.preflightCalls++
	if err := ctx.Err(); err != nil {
		return err
	}
	if root != "/sys/fs/cgroup/mydocker" {
		return errors.New("host preflight received a different cgroup root")
	}
	return nil
}

// ValidateExecutable records the absolute executable trust-check input without touching the filesystem.
func (host *fakeKeeperHost) ValidateExecutable(path string) error {
	host.validatedExecutable = path
	return nil
}

// KeeperCloneFlags returns the inert keeper namespace mask asserted by the fake factory.
func (host *fakeKeeperHost) KeeperCloneFlags() uintptr { return host.keeperFlags }

// InitCloneFlags returns the inert init namespace mask needed for complete preflight.
func (host *fakeKeeperHost) InitCloneFlags() uintptr { return host.initFlags }
