package slim

import (
	"context"
	"errors"
	"time"

	"mydocker/internal/cgroupv2"
	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/provider"
	"mydocker/internal/shim"
)

// launcherCgroupManager is the exact read/open/membership subset required for
// clone-time placement and action-time recovery; implementations never migrate processes.
type launcherCgroupManager interface {
	Root() string
	Preflight(context.Context) error
	OpenKeeper(context.Context, domain.SandboxID) (cgroupv2.DirectoryHandle, error)
	OpenAttempt(context.Context, domain.SandboxID, domain.AttemptID) (cgroupv2.DirectoryHandle, error)
	KeeperProcessIDs(context.Context, domain.SandboxID) ([]int, error)
	AttemptProcessIDs(context.Context, domain.SandboxID, domain.AttemptID) ([]int, error)
	ConfirmKeeperProcess(context.Context, domain.SandboxID, cgroupv2.ProcessReference) error
	AttachProcess(context.Context, domain.SandboxID, domain.AttemptID, cgroupv2.ProcessReference) error
}

// launcherProcessHandle is a runtime-only pidfd with serializable evidence and exact cleanup operations.
type launcherProcessHandle interface {
	cgroupv2.ProcessReference
	Evidence() (isolation.ProcessEvidence, error)
	Verify(context.Context) error
	Signal(context.Context, int) error
	WaitForExit(context.Context, time.Duration) error
	Close() error
}

// launcherProcessRuntime captures, restores, and observes strong pidfd identities without exposing raw PID authority.
type launcherProcessRuntime interface {
	CaptureFromPIDFD(context.Context, int, int, string) (launcherProcessHandle, error)
	CapturePeer(context.Context, int, string) (launcherProcessHandle, error)
	Restore(context.Context, isolation.ProcessEvidence) (launcherProcessHandle, error)
	Present(context.Context, isolation.ProcessEvidence) (bool, error)
}

// launcherNamespaceHandle retains one verified nsfs descriptor tied to a strong process owner.
type launcherNamespaceHandle interface {
	Evidence() (isolation.NamespaceEvidence, error)
	Verify(context.Context) error
	Duplicate(context.Context) (int, isolation.NamespaceEvidence, error)
	Close() error
}

// launcherNamespaceRuntime opens, configures, and read-only verifies namespaces
// only through verified process handles.
type launcherNamespaceRuntime interface {
	Open(context.Context, launcherProcessHandle, isolation.NamespaceType) (launcherNamespaceHandle, error)
	Configure(context.Context, launcherNamespaceHandle, NamespaceLaunch) error
	VerifyConfiguration(context.Context, launcherNamespaceHandle, NamespaceLaunch) error
	CloseFD(int) error
}

// launcherControlClient returns both a validated shim response and kernel-authenticated Unix peer PID.
type launcherControlClient interface {
	Exchange(context.Context, string, shim.ControlRequest) (shim.ControlResponse, int, error)
}

// launcherHost performs read-only host capability and executable trust checks.
type launcherHost interface {
	Preflight(context.Context, string, provider.IsolationRequirements) error
	ValidateExecutable(string) error
	KeeperCloneFlags() uintptr
	InitCloneFlags() uintptr
}

// systemControlClient delegates fresh bounded exchanges to the production SO_PEERCRED client.
type systemControlClient struct{}

// Exchange performs one bounded authenticated control request without retaining a session.
func (systemControlClient) Exchange(ctx context.Context, path string, request shim.ControlRequest) (shim.ControlResponse, int, error) {
	return shim.DoControlWithPeer(ctx, path, request)
}

// systemProcessRuntime adapts public isolation pidfd primitives to launcher interfaces.
type systemProcessRuntime struct {
	ops isolation.Ops
}

// newSystemProcessRuntime constructs a syscall adapter without opening any process or descriptor.
func newSystemProcessRuntime() systemProcessRuntime {
	return systemProcessRuntime{ops: isolation.NewSystemOps()}
}

// CaptureFromPIDFD adopts the clone-time pidfd and verifies executable evidence before manager membership confirmation.
func (runtime systemProcessRuntime) CaptureFromPIDFD(ctx context.Context, pid, pidfd int, executable string) (launcherProcessHandle, error) {
	return isolation.CaptureProcessHandleFromPIDFDExecutable(ctx, runtime.ops, pid, pidfd, executable)
}

// CapturePeer opens a strong handle around an authenticated socket peer before manager membership confirmation.
func (runtime systemProcessRuntime) CapturePeer(ctx context.Context, pid int, executable string) (launcherProcessHandle, error) {
	return isolation.CaptureProcessHandleExecutable(ctx, runtime.ops, pid, executable)
}

// Restore reopens a pidfd only when every persisted identity component still matches.
func (runtime systemProcessRuntime) Restore(ctx context.Context, evidence isolation.ProcessEvidence) (launcherProcessHandle, error) {
	return isolation.RestoreProcessHandle(ctx, runtime.ops, evidence)
}

// Present distinguishes exact liveness from verified exit or PID reuse.
func (runtime systemProcessRuntime) Present(ctx context.Context, evidence isolation.ProcessEvidence) (bool, error) {
	return isolation.ProcessEvidencePresent(ctx, runtime.ops, evidence)
}

// systemNamespaceRuntime adapts nsfs handles and locked-thread configuration without mutating during construction.
type systemNamespaceRuntime struct {
	ops isolation.Ops
}

// newSystemNamespaceRuntime constructs the production namespace adapter without joining a namespace.
func newSystemNamespaceRuntime() systemNamespaceRuntime {
	return systemNamespaceRuntime{ops: isolation.NewSystemOps()}
}

// Open requires the production strong process handle before opening one owner-bound namespace descriptor.
func (runtime systemNamespaceRuntime) Open(ctx context.Context, process launcherProcessHandle, namespace isolation.NamespaceType) (launcherNamespaceHandle, error) {
	concrete, ok := process.(*isolation.ProcessHandle)
	if !ok {
		return nil, errors.New("system namespace runtime requires an isolation ProcessHandle")
	}
	return isolation.OpenNamespaceHandle(ctx, concrete, namespace)
}

// Configure joins one verified namespace on a disposable locked thread and applies only its canonical M3 setting.
func (runtime systemNamespaceRuntime) Configure(ctx context.Context, handle launcherNamespaceHandle, request NamespaceLaunch) error {
	concrete, ok := handle.(*isolation.NamespaceHandle)
	if !ok {
		return errors.New("system namespace runtime requires an isolation NamespaceHandle")
	}
	return isolation.RunNamespaceSession(ctx, runtime.ops, []*isolation.NamespaceHandle{concrete}, func(actionContext context.Context, helper *isolation.LockedHelper) error {
		switch request.Namespace {
		case isolation.NamespaceUTS:
			return helper.ConfigureHostname(actionContext, request.Hostname)
		case isolation.NamespaceNetwork:
			return helper.ConfigureLoopback(actionContext, request.NetworkMode == provider.SandboxNetworkLoopback)
		default:
			return nil
		}
	})
}

// VerifyConfiguration joins one verified namespace on a disposable locked
// thread and reads back only the configuration relevant to its retained kind.
func (runtime systemNamespaceRuntime) VerifyConfiguration(ctx context.Context, handle launcherNamespaceHandle, request NamespaceLaunch) error {
	concrete, ok := handle.(*isolation.NamespaceHandle)
	if !ok {
		return errors.New("system namespace runtime requires an isolation NamespaceHandle")
	}
	return isolation.RunNamespaceSession(ctx, runtime.ops, []*isolation.NamespaceHandle{concrete}, func(actionContext context.Context, helper *isolation.LockedHelper) error {
		switch request.Namespace {
		case isolation.NamespaceUTS:
			return helper.VerifyHostname(actionContext, request.Hostname)
		case isolation.NamespaceNetwork:
			return helper.VerifyLoopback(actionContext, request.NetworkMode == provider.SandboxNetworkLoopback)
		default:
			return nil
		}
	})
}

// CloseFD closes one duplicate that could not be transferred to ProcessFactory before an init launch failure.
func (runtime systemNamespaceRuntime) CloseFD(fd int) error {
	return runtime.ops.Close(fd)
}
