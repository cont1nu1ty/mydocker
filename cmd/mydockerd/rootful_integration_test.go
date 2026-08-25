//go:build linux && mydocker_rootful

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "mydocker/api/runtime/v1"
	"mydocker/internal/cgroupv2"
	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/provider"
	"mydocker/internal/slim"
	clientapi "mydocker/pkg/client"
)

const rootfulSourceID = provider.OpaqueID("rootful-prepared")

// TestRootfulM3ProductionLifecycle exercises the real production composition,
// HTTP/JSON UDS, public client, Linux launcher, shim, namespaces, prepared
// rootfs, and cgroup v2 only after the disposable-host gate is fully proven.
func TestRootfulM3ProductionLifecycle(t *testing.T) {
	environment, enabled, err := loadRootfulTestEnvironment(os.LookupEnv)
	if err != nil {
		t.Fatalf("rootful safety gate rejected invocation before mutation: %v", err)
	}
	if !enabled {
		t.Skipf("set %s=1 only inside the documented disposable rootful VM workflow", rootfulEnableEnvironment)
	}
	preflightConfig := isolation.DefaultPreflightConfig()
	preflightConfig.CgroupRoot = environment.CgroupRoot
	preflightConfig.ForPrivilegedTest = true
	preflightConfig.AllowPrivilegedTest = true
	preflightConfig.DisposableEnvironment = true
	if err := isolation.ValidatePrivilegedTest(preflightConfig); err != nil {
		t.Fatalf("validate privileged-test policy before mutation: %v", err)
	}
	report, err := isolation.PreflightSystem(context.Background(), preflightConfig)
	if err != nil {
		t.Fatalf("read-only privileged host preflight failed before mutation: %v", err)
	}
	if !report.PrivilegedTestAllowed || !report.Rootful || !report.CgroupV2 || !report.Pidfd {
		t.Fatalf("read-only privileged host preflight report is incomplete: %+v", report)
	}
	if err := validateRootfulTestPaths(environment); err != nil {
		t.Fatalf("validate dedicated rootful paths before mutation: %v", err)
	}
	manager, err := cgroupv2.NewManager(cgroupv2.Config{Root: environment.CgroupRoot}, cgroupv2.OSFileSystem{}, cgroupv2.LinuxHostProbe{})
	if err != nil {
		t.Fatalf("construct read-only cgroup verifier before mutation: %v", err)
	}
	if err := manager.Preflight(context.Background()); err != nil {
		t.Fatalf("verify delegated cgroup controllers before mutation: %v", err)
	}
	if err := (slim.OSProcessFactory{}).Preflight(context.Background()); err != nil {
		t.Fatalf("verify clone3 cgroup/pidfd launch support before mutation: %v", err)
	}
	if err := requireEmptyRootfulCgroupRoot(environment.CgroupRoot); err != nil {
		t.Fatalf("dedicated cgroup root is not initially empty: %v", err)
	}
	t.Cleanup(func() {
		if err := requireEmptyRootfulCgroupRoot(environment.CgroupRoot); err != nil {
			t.Errorf("rootful suite left cgroup residue: %v", err)
		}
	})

	runRoot, err := os.MkdirTemp(environment.WorkRoot, "m3-run-")
	if err != nil {
		t.Fatalf("create isolated test run after successful preflight: %v", err)
	}
	if err := os.Chmod(runRoot, 0o700); err != nil {
		t.Fatalf("restrict isolated test run: %v", err)
	}
	t.Logf("rootful evidence root (retained for inspection): %s", runRoot)
	if !t.Run("none_exit_pid1", func(t *testing.T) {
		runRootfulExitScenario(t, environment, runRoot, manager)
	}) {
		return
	}
	if !t.Run("loopback_daemon_reopen_signal", func(t *testing.T) {
		runRootfulRecoverySignalScenario(t, environment, runRoot, manager)
	}) {
		return
	}
	if !t.Run("memory_oom_attribution", func(t *testing.T) {
		oomExecutable := prepareRootfulOOMHelper(t, environment, runRoot)
		runRootfulOOMScenario(t, environment, runRoot, manager, oomExecutable)
	}) {
		return
	}
}

// prepareRootfulOOMHelper builds one fixed, static, test-only workload after
// every host safety gate has passed. The binary touches retained pages until
// its Attempt cgroup records an OOM kill; it is never used outside the
// disposable rootfs and is removed only if its exact inode is still present.
func prepareRootfulOOMHelper(t *testing.T, environment rootfulTestEnvironment, runRoot string) string {
	t.Helper()
	const containerPath = "/bin/mydocker-oom-stressor"
	hostPath := filepath.Join(environment.Rootfs, strings.TrimPrefix(containerPath, "/"))
	if _, err := os.Lstat(hostPath); err == nil {
		t.Fatalf("refuse to replace pre-existing OOM helper path %s", hostPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspect OOM helper destination: %v", err)
	}
	sourcePath := filepath.Join(runRoot, "oom-stressor.go")
	source := strings.Join([]string{
		"package main",
		"",
		"import (",
		"  \"fmt\"",
		"  \"runtime\"",
		"  \"runtime/debug\"",
		")",
		"",
		"func main() {",
		"  debug.SetGCPercent(-1)",
		"  fmt.Println(\"MYDOCKER_OOM_READY\")",
		"  retained := make([][]byte, 0, 64)",
		"  for {",
		"    block := make([]byte, 4<<20)",
		"    for offset := 0; offset < len(block); offset += 4096 { block[offset] = 1 }",
		"    retained = append(retained, block)",
		"    runtime.KeepAlive(retained)",
		"  }",
		"}",
		"",
	}, "\n")
	if err := os.WriteFile(sourcePath, []byte(source), 0o600); err != nil {
		t.Fatalf("write fixed OOM helper source: %v", err)
	}
	compiledPath := filepath.Join(runRoot, "oom-stressor")
	buildContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(buildContext, "go", "build", "-trimpath", "-o", compiledPath, sourcePath)
	command.Env = make([]string, 0, len(os.Environ())+3)
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "CGO_ENABLED=") || strings.HasPrefix(entry, "GOTOOLCHAIN=") || strings.HasPrefix(entry, "GOCACHE=") {
			continue
		}
		command.Env = append(command.Env, entry)
	}
	command.Env = append(command.Env, "CGO_ENABLED=0", "GOTOOLCHAIN=local", "GOCACHE="+filepath.Join(runRoot, "go-cache"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build fixed OOM helper: %v: %s", err, output)
	}
	if err := os.Chmod(compiledPath, 0o755); err != nil {
		t.Fatalf("set compiled OOM helper mode: %v", err)
	}
	if err := requireRootOwnedRegular(compiledPath, true); err != nil {
		t.Fatalf("validate compiled OOM helper: %v", err)
	}
	sourceBinary, err := os.Open(compiledPath)
	if err != nil {
		t.Fatalf("open compiled OOM helper: %v", err)
	}
	defer sourceBinary.Close()
	destination, err := os.OpenFile(hostPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o755)
	if err != nil {
		t.Fatalf("publish OOM helper to exclusive rootfs path: %v", err)
	}
	destinationInfo, err := destination.Stat()
	if err != nil {
		_ = destination.Close()
		_ = os.Remove(hostPath)
		t.Fatalf("inspect exclusive OOM helper destination: %v", err)
	}
	published := false
	defer func() {
		if published {
			return
		}
		current, inspectErr := os.Lstat(hostPath)
		if inspectErr == nil && os.SameFile(destinationInfo, current) {
			_ = os.Remove(hostPath)
		}
	}()
	_, copyErr := io.Copy(destination, sourceBinary)
	publishErr := errors.Join(copyErr, destination.Chmod(0o755), destination.Sync(), destination.Close())
	if publishErr != nil {
		t.Fatalf("publish complete OOM helper: %v", publishErr)
	}
	builtInfo, err := os.Lstat(hostPath)
	if err != nil {
		t.Fatalf("inspect built OOM helper: %v", err)
	}
	if err := requireRootOwnedRegular(hostPath, true); err != nil {
		t.Fatalf("validate built OOM helper: %v", err)
	}
	published = true
	t.Cleanup(func() {
		current, err := os.Lstat(hostPath)
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		if err != nil || !os.SameFile(builtInfo, current) {
			t.Errorf("OOM helper identity changed; refusing cleanup: %v", err)
			return
		}
		if err := os.Remove(hostPath); err != nil {
			t.Errorf("remove exact OOM helper: %v", err)
		}
	})
	return containerPath
}

// runRootfulExitScenario verifies network=none, the fresh PID namespace's PID
// 1 view, captured non-zero exit, durable logs/events, and leaf-to-parent cleanup.
func runRootfulExitScenario(t *testing.T, environment rootfulTestEnvironment, runRoot string, manager *cgroupv2.Manager) {
	t.Helper()
	harness := newRootfulDaemonHarness(t, environment, runRoot, "none")
	cleaned := false
	t.Cleanup(func() {
		cleanupRootfulScenario(harness, "sandbox-rootful-none", "container-rootful-none", rootfulScenarioOperations("none"), &cleaned)
	})
	startContext, cancelStart := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStart()
	if err := harness.Start(startContext); err != nil {
		t.Fatalf("start production daemon: %v", err)
	}
	client := harness.Client()
	operations := rootfulScenarioOperations("none")
	if _, err := client.CreateSandbox(context.Background(), operations.createSandbox, v1.CreateSandboxRequest{
		SandboxID: "sandbox-rootful-none",
		Spec: v1.SandboxSpec{
			Hostname: "rootful-none", Network: v1.NetworkIntent{Mode: "none"},
			Resources: v1.Resources{Requests: v1.ResourceRequests{}, Limits: v1.ResourceLimits{}},
		},
	}); err != nil {
		t.Fatalf("CreateSandbox(none): %v", err)
	}
	if _, err := client.CreateContainer(context.Background(), operations.createContainer, "sandbox-rootful-none", v1.CreateContainerRequest{
		ContainerID: "container-rootful-none", AttemptID: "attempt-rootful-none", RootFS: string(rootfulSourceID),
		Process: v1.ProcessSpec{
			Argv:             []string{"/bin/sh", "-c", `IFS=' ' read -r pid_one rest < /proc/1/stat; printf 'MYDOCKER_PID1=%s SELF=%s\n' "$pid_one" "$$"; printf 'MYDOCKER_EXIT_MARKER\n'; exit 23`},
			WorkingDirectory: "/",
		},
	}); err != nil {
		t.Fatalf("CreateContainer(none): %v", err)
	}
	if _, err := client.StartContainer(context.Background(), operations.startContainer, "container-rootful-none"); err != nil {
		t.Fatalf("StartContainer(none): %v", err)
	}
	terminal := waitRootfulContainerPhase(t, client, "container-rootful-none", "stopped", 30*time.Second)
	if terminal.Status.Outcome.Presence != "captured" || terminal.Status.Outcome.ExitCode == nil || *terminal.Status.Outcome.ExitCode != 23 || terminal.Status.Outcome.Signal != "" {
		t.Fatalf("natural-exit outcome = %+v, want captured exit 23", terminal.Status.Outcome)
	}
	logs := collectRootfulLogs(t, client, "container-rootful-none", "attempt-rootful-none")
	if !bytes.Contains(logs, []byte("MYDOCKER_PID1=1")) || !bytes.Contains(logs, []byte("MYDOCKER_EXIT_MARKER")) {
		t.Fatalf("workload logs do not prove PID 1/proc and exit markers: %q", logs)
	}
	if _, err := client.DeleteContainer(context.Background(), operations.deleteContainer, "container-rootful-none"); err != nil {
		t.Fatalf("DeleteContainer(none): %v", err)
	}
	if _, err := client.StopSandbox(context.Background(), operations.stopSandbox, "sandbox-rootful-none"); err != nil {
		t.Fatalf("StopSandbox(none): %v", err)
	}
	if _, err := client.DeleteSandbox(context.Background(), operations.deleteSandbox, "sandbox-rootful-none"); err != nil {
		t.Fatalf("DeleteSandbox(none): %v", err)
	}
	assertRootfulOperationEvents(t, client, operations.required(false))
	assertRootfulCgroupsAbsent(t, manager, "sandbox-rootful-none", "attempt-rootful-none")
	cleaned = true
	stopContext, cancelStop := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelStop()
	if err := harness.Stop(stopContext); err != nil {
		t.Fatalf("stop production daemon: %v", err)
	}
}

// runRootfulRecoverySignalScenario verifies loopback configuration, live
// Attempt cgroup membership, daemon close/reopen adoption, verified SIGTERM
// delivery, terminal attribution, and exact cgroup cleanup.
func runRootfulRecoverySignalScenario(t *testing.T, environment rootfulTestEnvironment, runRoot string, manager *cgroupv2.Manager) {
	t.Helper()
	harness := newRootfulDaemonHarness(t, environment, runRoot, "loopback")
	cleaned := false
	operations := rootfulScenarioOperations("loopback")
	t.Cleanup(func() {
		cleanupRootfulScenario(harness, "sandbox-rootful-loopback", "container-rootful-loopback", operations, &cleaned)
	})
	startContext, cancelStart := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStart()
	if err := harness.Start(startContext); err != nil {
		t.Fatalf("start production daemon: %v", err)
	}
	client := harness.Client()
	cpuLimitMilli := int64(500)
	memoryLimitBytes := int64(64 << 20)
	pidsLimit := int64(64)
	if _, err := client.CreateSandbox(context.Background(), operations.createSandbox, v1.CreateSandboxRequest{
		SandboxID: "sandbox-rootful-loopback",
		Spec: v1.SandboxSpec{
			Hostname: "rootful-loopback", DNS: []string{"203.0.113.53"}, Network: v1.NetworkIntent{Mode: "loopback"},
			Resources: v1.Resources{Requests: v1.ResourceRequests{}, Limits: v1.ResourceLimits{
				CPULimitMilli: &cpuLimitMilli, MemoryLimitBytes: &memoryLimitBytes, PidsLimit: &pidsLimit,
			}},
		},
	}); err != nil {
		t.Fatalf("CreateSandbox(loopback): %v", err)
	}
	policy := v1.TerminationPolicy{Signal: "SIGTERM", GracePeriodNanoseconds: int64(time.Second), EscalationSignal: "SIGKILL"}
	if _, err := client.CreateContainer(context.Background(), operations.createContainer, "sandbox-rootful-loopback", v1.CreateContainerRequest{
		ContainerID: "container-rootful-loopback", AttemptID: "attempt-rootful-loopback", RootFS: string(rootfulSourceID),
		Process: v1.ProcessSpec{
			Argv:             []string{"/bin/sh", "-c", `IFS= read -r observed_hostname < /proc/sys/kernel/hostname; { IFS= read -r generated; IFS= read -r nameserver; } < /etc/resolv.conf; printf 'MYDOCKER_HOSTNAME=%s\n' "$observed_hostname"; printf 'MYDOCKER_RESOLV=%s\n' "$nameserver"; printf 'MYDOCKER_SIGNAL_READY\n'; exec /bin/sleep 300`},
			WorkingDirectory: "/", Termination: policy,
		},
	}); err != nil {
		t.Fatalf("CreateContainer(loopback): %v", err)
	}
	started, err := client.StartContainer(context.Background(), operations.startContainer, "container-rootful-loopback")
	if err != nil {
		t.Fatalf("StartContainer(loopback): %v", err)
	}
	if started.Container.Status.Phase != "running" {
		t.Fatalf("long-running signal workload phase = %q, want running", started.Container.Status.Phase)
	}
	waitRootfulLogMarker(t, client, "container-rootful-loopback", "attempt-rootful-loopback", "MYDOCKER_SIGNAL_READY", 20*time.Second)
	startupLogs := collectRootfulLogs(t, client, "container-rootful-loopback", "attempt-rootful-loopback")
	if !bytes.Contains(startupLogs, []byte("MYDOCKER_HOSTNAME=rootful-loopback")) ||
		!bytes.Contains(startupLogs, []byte("MYDOCKER_RESOLV=nameserver 203.0.113.53")) {
		t.Fatalf("loopback workload did not observe configured hostname/DNS: %q", startupLogs)
	}
	assertRootfulCgroupPopulated(t, manager, "sandbox-rootful-loopback", "attempt-rootful-loopback")
	assertRootfulCgroupLimits(t, manager, "sandbox-rootful-loopback", "attempt-rootful-loopback", "50000 100000", "67108864", "64")

	stopContext, cancelStop := context.WithTimeout(context.Background(), 20*time.Second)
	if err := harness.Stop(stopContext); err != nil {
		cancelStop()
		t.Fatalf("stop first daemon session: %v", err)
	}
	cancelStop()
	if _, err := os.Lstat(harness.config.socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("public UDS survived daemon shutdown: %v", err)
	}
	reopenContext, cancelReopen := context.WithTimeout(context.Background(), 30*time.Second)
	if err := harness.Start(reopenContext); err != nil {
		cancelReopen()
		t.Fatalf("reopen production daemon over durable state: %v", err)
	}
	cancelReopen()
	client = harness.Client()
	recovered, err := client.GetContainer(context.Background(), "container-rootful-loopback")
	if err != nil {
		t.Fatalf("GetContainer after daemon reopen: %v", err)
	}
	if recovered.Container.Status.Phase != "running" {
		t.Fatalf("recovered Container phase = %q, want running", recovered.Container.Status.Phase)
	}
	operation, err := client.GetOperation(context.Background(), operations.startContainer)
	if err != nil || operation.Operation.State != "succeeded" {
		t.Fatalf("recovered start operation = %+v, error = %v", operation.Operation, err)
	}
	terminal, err := client.KillContainer(context.Background(), operations.killContainer, "container-rootful-loopback", policy)
	if err != nil {
		t.Fatalf("KillContainer after daemon reopen: %v", err)
	}
	if terminal.Container.Status.Outcome.Presence != "captured" || terminal.Container.Status.Outcome.Signal != "SIGTERM" || terminal.Container.Status.Outcome.ExitCode != nil {
		t.Fatalf("signal outcome = %+v, want captured SIGTERM", terminal.Container.Status.Outcome)
	}
	if _, err := client.DeleteContainer(context.Background(), operations.deleteContainer, "container-rootful-loopback"); err != nil {
		t.Fatalf("DeleteContainer(loopback): %v", err)
	}
	if _, err := client.StopSandbox(context.Background(), operations.stopSandbox, "sandbox-rootful-loopback"); err != nil {
		t.Fatalf("StopSandbox(loopback): %v", err)
	}
	if _, err := client.DeleteSandbox(context.Background(), operations.deleteSandbox, "sandbox-rootful-loopback"); err != nil {
		t.Fatalf("DeleteSandbox(loopback): %v", err)
	}
	assertRootfulOperationEvents(t, client, operations.required(true))
	assertRootfulCgroupsAbsent(t, manager, "sandbox-rootful-loopback", "attempt-rootful-loopback")
	cleaned = true
	finalStopContext, cancelFinalStop := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelFinalStop()
	if err := harness.Stop(finalStopContext); err != nil {
		t.Fatalf("stop reopened production daemon: %v", err)
	}
}

// runRootfulOOMScenario verifies a fixed memory-pressure workload is killed by
// the Attempt memory cgroup and that both kernel counters and the public
// terminal projection independently identify the OOM event.
func runRootfulOOMScenario(t *testing.T, environment rootfulTestEnvironment, runRoot string, manager *cgroupv2.Manager, workloadPath string) {
	t.Helper()
	if workloadPath != "/bin/mydocker-oom-stressor" {
		t.Fatalf("unexpected OOM workload path %q", workloadPath)
	}
	harness := newRootfulDaemonHarness(t, environment, runRoot, "oom")
	cleaned := false
	operations := rootfulScenarioOperations("oom")
	t.Cleanup(func() {
		cleanupRootfulScenario(harness, "sandbox-rootful-oom", "container-rootful-oom", operations, &cleaned)
	})
	startContext, cancelStart := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStart()
	if err := harness.Start(startContext); err != nil {
		t.Fatalf("start production daemon: %v", err)
	}
	client := harness.Client()
	memoryLimitBytes := int64(128 << 20)
	pidsLimit := int64(32)
	if _, err := client.CreateSandbox(context.Background(), operations.createSandbox, v1.CreateSandboxRequest{
		SandboxID: "sandbox-rootful-oom",
		Spec: v1.SandboxSpec{
			Hostname: "rootful-oom", Network: v1.NetworkIntent{Mode: "none"},
			Resources: v1.Resources{Requests: v1.ResourceRequests{}, Limits: v1.ResourceLimits{
				MemoryLimitBytes: &memoryLimitBytes, PidsLimit: &pidsLimit,
			}},
		},
	}); err != nil {
		t.Fatalf("CreateSandbox(OOM): %v", err)
	}
	if _, err := client.CreateContainer(context.Background(), operations.createContainer, "sandbox-rootful-oom", v1.CreateContainerRequest{
		ContainerID: "container-rootful-oom", AttemptID: "attempt-rootful-oom", RootFS: string(rootfulSourceID),
		Process: v1.ProcessSpec{Argv: []string{workloadPath}, WorkingDirectory: "/"},
	}); err != nil {
		t.Fatalf("CreateContainer(OOM): %v", err)
	}
	oomBaseline := readRootfulOOMSnapshot(t, manager, "sandbox-rootful-oom", "attempt-rootful-oom")
	if _, err := client.StartContainer(context.Background(), operations.startContainer, "container-rootful-oom"); err != nil {
		t.Fatalf("StartContainer(OOM): %v", err)
	}
	terminal := waitRootfulContainerPhase(t, client, "container-rootful-oom", "stopped", 60*time.Second)
	if terminal.Status.Outcome.Presence != "captured" || terminal.Status.Outcome.OOM != "true" ||
		terminal.Status.Outcome.Signal != "SIGKILL" || terminal.Status.Outcome.ExitCode != nil {
		t.Fatalf("OOM outcome = %+v, want captured SIGKILL with OOM=true", terminal.Status.Outcome)
	}
	logs := collectRootfulLogs(t, client, "container-rootful-oom", "attempt-rootful-oom")
	if !bytes.Contains(logs, []byte("MYDOCKER_OOM_READY")) {
		t.Fatalf("OOM workload startup marker is absent: %q", logs)
	}
	assertRootfulOOMDelta(t, manager, "sandbox-rootful-oom", "attempt-rootful-oom", oomBaseline)
	assertRootfulCgroupLimits(t, manager, "sandbox-rootful-oom", "attempt-rootful-oom", "max 100000", "134217728", "32")
	if _, err := client.DeleteContainer(context.Background(), operations.deleteContainer, "container-rootful-oom"); err != nil {
		t.Fatalf("DeleteContainer(OOM): %v", err)
	}
	if _, err := client.StopSandbox(context.Background(), operations.stopSandbox, "sandbox-rootful-oom"); err != nil {
		t.Fatalf("StopSandbox(OOM): %v", err)
	}
	if _, err := client.DeleteSandbox(context.Background(), operations.deleteSandbox, "sandbox-rootful-oom"); err != nil {
		t.Fatalf("DeleteSandbox(OOM): %v", err)
	}
	assertRootfulOperationEvents(t, client, operations.required(false))
	assertRootfulCgroupsAbsent(t, manager, "sandbox-rootful-oom", "attempt-rootful-oom")
	cleaned = true
	stopContext, cancelStop := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelStop()
	if err := harness.Stop(stopContext); err != nil {
		t.Fatalf("stop production daemon: %v", err)
	}
}

// rootfulOperationSet assigns stable unique public operation IDs and supplies
// the expected complete-event inventory for one lifecycle scenario.
type rootfulOperationSet struct {
	createSandbox   string
	createContainer string
	startContainer  string
	killContainer   string
	deleteContainer string
	stopSandbox     string
	deleteSandbox   string
}

// rootfulScenarioOperations returns deterministic IDs that can be replayed by
// failure cleanup without issuing a semantically different mutation.
func rootfulScenarioOperations(scope string) rootfulOperationSet {
	prefix := "op-rootful-" + scope + "-"
	return rootfulOperationSet{
		createSandbox: prefix + "create-sandbox", createContainer: prefix + "create-container",
		startContainer: prefix + "start-container", killContainer: prefix + "kill-container",
		deleteContainer: prefix + "delete-container", stopSandbox: prefix + "stop-sandbox",
		deleteSandbox: prefix + "delete-sandbox",
	}
}

// required lists every formal mutation whose successful complete event must
// be present; natural-exit scenarios do not include an explicit Kill.
func (operations rootfulOperationSet) required(includeKill bool) []string {
	result := []string{operations.createSandbox, operations.createContainer, operations.startContainer}
	if includeKill {
		result = append(result, operations.killContainer)
	}
	return append(result, operations.deleteContainer, operations.stopSandbox, operations.deleteSandbox)
}

// rootfulDaemonHarness starts and reopens runDaemon with production factories
// while exposing only the public client to lifecycle scenario code.
type rootfulDaemonHarness struct {
	config daemonConfig
	args   []string
	cancel context.CancelFunc
	done   chan error
	client *clientapi.Client
}

// newRootfulDaemonHarness creates private state/runtime/socket paths only after
// the top-level privileged gate and read-only host preflight have succeeded.
func newRootfulDaemonHarness(t *testing.T, environment rootfulTestEnvironment, runRoot, name string) *rootfulDaemonHarness {
	t.Helper()
	scenarioRoot := filepath.Join(runRoot, name)
	if err := os.Mkdir(scenarioRoot, 0o700); err != nil {
		t.Fatalf("create scenario root: %v", err)
	}
	stateRoot := filepath.Join(scenarioRoot, "state")
	socketRoot := filepath.Join(scenarioRoot, "socket")
	for description, path := range map[string]string{"state": stateRoot, "socket": socketRoot} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatalf("create private %s root: %v", description, err)
		}
	}
	runtimeComponent := map[string]string{"none": "rn", "loopback": "rl", "oom": "ro"}[name]
	if runtimeComponent == "" {
		t.Fatalf("unsupported rootful scenario name %q", name)
	}
	runtimeRoot := filepath.Join(environment.WorkRoot, runtimeComponent)
	if err := os.Mkdir(runtimeRoot, 0o700); err != nil {
		t.Fatalf("create scenario runtime root: %v", err)
	}
	config := daemonConfig{
		statePath: filepath.Join(stateRoot, "state.json"), runtimeRoot: runtimeRoot,
		socketPath: filepath.Join(socketRoot, "api.sock"), cgroupRoot: environment.CgroupRoot,
		shimPath: environment.Shim,
		preparedRootFS: map[provider.OpaqueID]isolation.RootfsConfig{
			rootfulSourceID: {AllowedRoot: filepath.Dir(environment.Rootfs), Rootfs: environment.Rootfs},
		},
		shutdown: 10 * time.Second,
	}
	args := []string{
		"--state", config.statePath, "--runtime-root", config.runtimeRoot, "--socket", config.socketPath,
		"--cgroup-root", config.cgroupRoot, "--shim", config.shimPath,
		"--prepared-rootfs", string(rootfulSourceID) + "=" + environment.Rootfs,
		"--shutdown-timeout", config.shutdown.String(),
	}
	return &rootfulDaemonHarness{config: config, args: args}
}

// Start launches one production daemon session, then proves the UDS is serving
// by completing a public read-only client request before returning.
func (harness *rootfulDaemonHarness) Start(ctx context.Context) error {
	if harness == nil || harness.cancel != nil || harness.done != nil || harness.client != nil {
		return errors.New("rootful daemon harness is already running or invalid")
	}
	if ctx == nil {
		return errors.New("rootful daemon start context is nil")
	}
	daemonContext, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	harness.cancel = cancel
	harness.done = done
	go func() {
		done <- runDaemon(daemonContext, append([]string(nil), harness.args...), os.Stderr, openProductionRuntime, newProductionServer)
	}()
	client, err := clientapi.New(clientapi.Config{
		SocketPath: harness.config.socketPath, Timeout: 2 * time.Second, DialTimeout: 250 * time.Millisecond,
		TransportRetries: 0, MaxResponseBytes: 32 << 20,
	})
	if err != nil {
		cancel()
		return err
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		probeContext, cancelProbe := context.WithTimeout(ctx, 500*time.Millisecond)
		_, probeErr := client.ListSandboxes(probeContext)
		cancelProbe()
		if probeErr == nil {
			harness.client = client
			return nil
		}
		select {
		case daemonErr := <-done:
			client.CloseIdleConnections()
			harness.cancel = nil
			harness.done = nil
			return fmt.Errorf("production daemon exited before UDS readiness: %w", daemonErr)
		case <-ctx.Done():
			cancel()
			client.CloseIdleConnections()
			return fmt.Errorf("wait for production UDS readiness: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// Client returns the currently connected public client and never exposes the
// production engine or provider to scenario assertions.
func (harness *rootfulDaemonHarness) Client() *clientapi.Client {
	if harness == nil {
		return nil
	}
	return harness.client
}

// Stop gracefully quiesces the public API and background recovery watchers,
// closes the FileStore, and leaves detached shim-owned workloads untouched.
func (harness *rootfulDaemonHarness) Stop(ctx context.Context) error {
	if harness == nil {
		return errors.New("rootful daemon harness is nil")
	}
	if harness.cancel == nil || harness.done == nil {
		return nil
	}
	if harness.client != nil {
		harness.client.CloseIdleConnections()
	}
	harness.cancel()
	select {
	case err := <-harness.done:
		harness.cancel = nil
		harness.done = nil
		harness.client = nil
		return err
	case <-ctx.Done():
		return fmt.Errorf("wait for production daemon shutdown: %w", ctx.Err())
	}
}

// cleanupRootfulScenario uses only the public API for best-effort failure
// cleanup; it never recursively removes cgroups, mounts, or runtime artifacts.
func cleanupRootfulScenario(harness *rootfulDaemonHarness, sandboxID, containerID string, operations rootfulOperationSet, cleaned *bool) {
	if harness == nil {
		return
	}
	cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if cleaned == nil || !*cleaned {
		if harness.Client() == nil {
			_ = harness.Start(cleanupContext)
		}
		if client := harness.Client(); client != nil {
			if container, err := client.GetContainer(cleanupContext, containerID); err == nil {
				if container.Container.Status.Phase == "running" {
					_, _ = client.KillContainer(cleanupContext, operations.killContainer, containerID, v1.TerminationPolicy{
						Signal: "SIGTERM", GracePeriodNanoseconds: int64(time.Second), EscalationSignal: "SIGKILL",
					})
				}
				_, _ = client.DeleteContainer(cleanupContext, operations.deleteContainer, containerID)
			}
			if _, err := client.GetSandbox(cleanupContext, sandboxID); err == nil {
				_, _ = client.StopSandbox(cleanupContext, operations.stopSandbox, sandboxID)
				_, _ = client.DeleteSandbox(cleanupContext, operations.deleteSandbox, sandboxID)
			}
		}
	}
	_ = harness.Stop(cleanupContext)
}

// waitRootfulContainerPhase polls the authoritative public projection until
// the requested phase appears or a bounded integration deadline expires.
func waitRootfulContainerPhase(t *testing.T, client *clientapi.Client, containerID, phase string, maximum time.Duration) v1.Container {
	t.Helper()
	deadline := time.Now().Add(maximum)
	for {
		response, err := client.GetContainer(context.Background(), containerID)
		if err == nil && response.Container.Status.Phase == phase {
			return response.Container
		}
		if time.Now().After(deadline) {
			t.Fatalf("Container %s did not reach %s; last error=%v response=%+v", containerID, phase, err, response.Container.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// collectRootfulLogs reads every currently retained log page through the
// identity-bound public cursor and returns stream payloads in global order.
func collectRootfulLogs(t *testing.T, client *clientapi.Client, containerID, attemptID string) []byte {
	t.Helper()
	var cursor v1.LogCursor
	var payload bytes.Buffer
	for {
		page, err := client.Logs(context.Background(), containerID, attemptID, cursor, 100)
		if err != nil {
			t.Fatalf("Logs(%s/%s): %v", containerID, attemptID, err)
		}
		for _, frame := range page.Frames {
			payload.Write(frame.Payload)
		}
		cursor = page.NextCursor
		if !page.HasMore {
			return payload.Bytes()
		}
	}
}

// waitRootfulLogMarker waits for a startup marker before daemon restart or
// signal delivery, proving the direct workload child actually executed.
func waitRootfulLogMarker(t *testing.T, client *clientapi.Client, containerID, attemptID, marker string, maximum time.Duration) {
	t.Helper()
	deadline := time.Now().Add(maximum)
	for {
		page, err := client.Logs(context.Background(), containerID, attemptID, "", 100)
		if err == nil {
			var payload bytes.Buffer
			for _, frame := range page.Frames {
				payload.Write(frame.Payload)
			}
			if bytes.Contains(payload.Bytes(), []byte(marker)) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("workload log marker %q did not appear: %v", marker, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// assertRootfulOperationEvents requires one successful complete event for
// every formal public mutation in a scenario.
func assertRootfulOperationEvents(t *testing.T, client *clientapi.Client, operationIDs []string) {
	t.Helper()
	wanted := make(map[string]bool, len(operationIDs))
	for _, operationID := range operationIDs {
		wanted[operationID] = false
	}
	var after v1.ResumeToken
	for {
		page, err := client.Events(context.Background(), after, 500)
		if err != nil {
			t.Fatalf("Events(): %v", err)
		}
		for _, event := range page.Events {
			if _, exists := wanted[event.OperationID]; exists && event.Stage == "complete" && (event.Result == "succeeded" || event.Result == "noop") {
				wanted[event.OperationID] = true
			}
		}
		after = page.NextResumeToken
		if !page.HasMore {
			break
		}
	}
	for operationID, complete := range wanted {
		if !complete {
			t.Errorf("operation %s lacks a successful complete event", operationID)
		}
	}
}

// assertRootfulCgroupPopulated verifies production clone-time placement put
// both keeper and Attempt processes into their dedicated sibling leaves.
func assertRootfulCgroupPopulated(t *testing.T, manager *cgroupv2.Manager, sandboxID, attemptID string) {
	t.Helper()
	keeper, err := manager.KeeperPath(providerSandboxID(sandboxID))
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := manager.AttemptPath(providerSandboxID(sandboxID), providerAttemptID(attemptID))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{keeper, attempt} {
		members, err := os.ReadFile(filepath.Join(path, "cgroup.procs"))
		if err != nil || strings.TrimSpace(string(members)) == "" {
			t.Fatalf("cgroup %s has no verified member: members=%q error=%v", path, members, err)
		}
	}
}

// assertRootfulCgroupLimits reads the real Attempt controller files and
// requires exact canonical values after production write-and-readback setup.
func assertRootfulCgroupLimits(t *testing.T, manager *cgroupv2.Manager, sandboxID, attemptID, cpuMax, memoryMax, pidsMax string) {
	t.Helper()
	attempt, err := manager.AttemptPath(providerSandboxID(sandboxID), providerAttemptID(attemptID))
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{"cpu.max": cpuMax, "memory.max": memoryMax, "pids.max": pidsMax}
	for name, wanted := range expected {
		payload, err := os.ReadFile(filepath.Join(attempt, name))
		if err != nil {
			t.Fatalf("read Attempt %s: %v", name, err)
		}
		observed := strings.Join(strings.Fields(string(payload)), " ")
		if observed != wanted {
			t.Fatalf("Attempt %s = %q, want %q", name, observed, wanted)
		}
	}
}

// readRootfulOOMSnapshot reads memory.events.local through the production
// cgroup manager at one explicit evidence boundary.
func readRootfulOOMSnapshot(t *testing.T, manager *cgroupv2.Manager, sandboxID, attemptID string) cgroupv2.OOMSnapshot {
	t.Helper()
	snapshot, err := manager.SnapshotOOM(context.Background(), providerSandboxID(sandboxID), providerAttemptID(attemptID))
	if err != nil {
		t.Fatalf("read Attempt OOM counters: %v", err)
	}
	return snapshot
}

// assertRootfulOOMDelta requires a monotonic kernel OOM-kill fact in the exact
// Attempt leaf before that cgroup is removed.
func assertRootfulOOMDelta(t *testing.T, manager *cgroupv2.Manager, sandboxID, attemptID string, baseline cgroupv2.OOMSnapshot) {
	t.Helper()
	current := readRootfulOOMSnapshot(t, manager, sandboxID, attemptID)
	delta, err := current.Delta(baseline)
	if err != nil {
		t.Fatalf("compare Attempt OOM counters: %v", err)
	}
	if delta.OOM == 0 || !delta.Killed() {
		t.Fatalf("memory.events.local OOM delta = %+v, want oom and an OOM-kill counter", delta)
	}
}

// assertRootfulCgroupsAbsent verifies the exact Attempt, keeper, and Sandbox
// paths are absent after public delete operations complete.
func assertRootfulCgroupsAbsent(t *testing.T, manager *cgroupv2.Manager, sandboxID, attemptID string) {
	t.Helper()
	sandbox, err := manager.SandboxPath(providerSandboxID(sandboxID))
	if err != nil {
		t.Fatal(err)
	}
	keeper, err := manager.KeeperPath(providerSandboxID(sandboxID))
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := manager.AttemptPath(providerSandboxID(sandboxID), providerAttemptID(attemptID))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{attempt, keeper, sandbox} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("owned cgroup still exists after cleanup: %s (error=%v)", path, err)
		}
	}
}

// providerSandboxID keeps domain identity conversion localized in the tagged
// test without accepting a path or numeric process identity.
func providerSandboxID(value string) domain.SandboxID { return domain.SandboxID(value) }

// providerAttemptID keeps domain identity conversion localized in the tagged
// test without accepting a path or numeric process identity.
func providerAttemptID(value string) domain.AttemptID { return domain.AttemptID(value) }
