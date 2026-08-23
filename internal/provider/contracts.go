package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"unicode"
	"unicode/utf8"

	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
)

const (
	maxSandboxHostnameBytes = 64
	maxSandboxDNSServers    = 3
)

// OpaqueID is provider discovery input whose interpretation is fixed by provider configuration, never a host path.
type OpaqueID string

// Validate rejects empty, unbounded, path-shaped, whitespace, or control-character provider identifiers.
func (id OpaqueID) Validate() error {
	value := string(id)
	if value == "" || len(value) > 256 || strings.ContainsAny(value, `/\\`) {
		return errors.New("provider identifier must be non-empty, bounded, and contain no path separators")
	}
	for _, character := range value {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return errors.New("provider identifier must contain no whitespace or control characters")
		}
	}
	return nil
}

// Signal is the bounded signal vocabulary accepted by the verified process provider.
type Signal string

const (
	// SignalHUP requests the conventional hangup behavior from a verified process.
	SignalHUP Signal = "SIGHUP"
	// SignalINT requests interactive interruption from a verified process.
	SignalINT Signal = "SIGINT"
	// SignalQUIT requests the conventional quit behavior from a verified process.
	SignalQUIT Signal = "SIGQUIT"
	// SignalKILL requests immediate kernel termination of a verified process.
	SignalKILL Signal = "SIGKILL"
	// SignalTERM requests conventional graceful termination from a verified process.
	SignalTERM Signal = "SIGTERM"
	// SignalUSR1 requests the first application-defined signal from a verified process.
	SignalUSR1 Signal = "SIGUSR1"
	// SignalUSR2 requests the second application-defined signal from a verified process.
	SignalUSR2 Signal = "SIGUSR2"
)

// Valid reports whether a signal belongs to the explicit provider vocabulary.
func (s Signal) Valid() bool {
	switch s {
	case SignalHUP, SignalINT, SignalQUIT, SignalKILL, SignalTERM, SignalUSR1, SignalUSR2:
		return true
	default:
		return false
	}
}

// SandboxNetworkMode is the bounded M3 network-namespace configuration that a
// launcher must apply and later verify rather than treating as diagnostic text.
type SandboxNetworkMode string

const (
	// SandboxNetworkNone requests an isolated network namespace with no configured loopback interface.
	SandboxNetworkNone SandboxNetworkMode = "none"
	// SandboxNetworkLoopback requests an isolated network namespace whose loopback interface is configured up.
	SandboxNetworkLoopback SandboxNetworkMode = "loopback"
)

// Valid reports whether the mode belongs to the explicitly implemented M3 provider vocabulary.
func (mode SandboxNetworkMode) Valid() bool {
	return mode == SandboxNetworkNone || mode == SandboxNetworkLoopback
}

// Canonical maps the historical empty internal value to the M3 none default and
// preserves either explicit mode so provider evidence has one stable identity.
func (mode SandboxNetworkMode) Canonical() (SandboxNetworkMode, error) {
	if mode == "" {
		return SandboxNetworkNone, nil
	}
	if !mode.Valid() {
		return "", fmt.Errorf("unsupported Sandbox network mode %q", mode)
	}
	return mode, nil
}

// SignalStep distinguishes the graceful delivery from its optional escalation within one durable Kill operation.
type SignalStep string

const (
	// SignalStepInitial identifies the first signal in an explicit termination policy.
	SignalStepInitial SignalStep = "initial"
	// SignalStepEscalation identifies the signal delivered only after the explicit grace period expires.
	SignalStepEscalation SignalStep = "escalation"
)

// Valid reports whether the step can participate in a deterministic signal idempotency key.
func (step SignalStep) Valid() bool {
	return step == SignalStepInitial || step == SignalStepEscalation
}

// CgroupProvider exposes one host effect or readback per method so the engine can checkpoint every acquisition.
// Ensure failures are ambiguity-preserving by default; an implementation may return MarkNoEffect only after proving it acquired or changed nothing.
type CgroupProvider interface {
	// InspectCgroupCapabilities performs read-only cgroup preflight for typed requirements.
	InspectCgroupCapabilities(context.Context, CgroupRequirements) (CgroupCapabilities, error)
	// EnsureSandboxCgroup creates or rediscovers only the deterministic Sandbox parent cgroup.
	EnsureSandboxCgroup(context.Context, SandboxCgroupRequest) (ownership.Receipt, error)
	// InspectSandboxCgroup verifies presence or absence of one receipt-bound Sandbox cgroup.
	InspectSandboxCgroup(context.Context, OwnedReceiptRequest) (ResourceObservation, error)
	// RemoveSandboxCgroup idempotently removes one exact empty receipt-bound Sandbox cgroup.
	RemoveSandboxCgroup(context.Context, OwnedReceiptRequest) (CleanupObservation, error)
	// EnsureKeeperCgroup creates or rediscovers the process-bearing leaf below the process-free Sandbox parent.
	EnsureKeeperCgroup(context.Context, KeeperCgroupRequest) (ownership.Receipt, error)
	// InspectKeeperCgroup verifies one receipt-bound keeper leaf and its membership.
	InspectKeeperCgroup(context.Context, OwnedReceiptRequest) (ResourceObservation, error)
	// RemoveKeeperCgroup removes the exact empty keeper leaf before the Sandbox parent.
	RemoveKeeperCgroup(context.Context, OwnedReceiptRequest) (CleanupObservation, error)
	// EnsureAttemptCgroup creates or rediscovers one child cgroup and applies only resource limits.
	EnsureAttemptCgroup(context.Context, AttemptCgroupRequest) (ownership.Receipt, error)
	// InspectAttemptCgroup verifies presence or absence of one receipt-bound Attempt cgroup.
	InspectAttemptCgroup(context.Context, OwnedReceiptRequest) (ResourceObservation, error)
	// SnapshotAttemptOOM reads owner-verified local memory event counters for later Attempt-scoped delta classification.
	SnapshotAttemptOOM(context.Context, OwnedReceiptRequest) (OOMSnapshot, error)
	// RemoveAttemptCgroup idempotently removes one exact empty receipt-bound Attempt cgroup.
	RemoveAttemptCgroup(context.Context, OwnedReceiptRequest) (CleanupObservation, error)
	// AttachAttemptProcess action-time verifies that an init captured inside the target cgroup remains a member.
	// It must never migrate an already captured strong identity because cgroup membership is part of that evidence.
	AttachAttemptProcess(context.Context, AttachProcessRequest) (AttachmentObservation, error)
}

// IsolationProvider exposes individually checkpointable process, namespace, rootfs, gate, and stream effects.
// Ensure failures are ambiguity-preserving by default; an implementation may
// return MarkNoEffect only after proving it changed nothing, or
// MarkRollbackRequired only after proving prior checkpointed owners contain
// every possible effect.
type IsolationProvider interface {
	// InspectIsolationCapabilities performs read-only isolation preflight for typed requirements.
	InspectIsolationCapabilities(context.Context, IsolationRequirements) (IsolationCapabilities, error)
	// EnsureKeeperProcess creates or rediscovers only the gated Sandbox keeper process.
	EnsureKeeperProcess(context.Context, KeeperProcessRequest) (ownership.Receipt, error)
	// EnsureInitProcess creates or rediscovers only the gated Attempt init process.
	EnsureInitProcess(context.Context, InitProcessRequest) (ownership.Receipt, error)
	// InspectProcess action-time verifies presence or absence of one keeper or init receipt.
	InspectProcess(context.Context, OwnedReceiptRequest) (ResourceObservation, error)
	// RemoveProcess action-time verifies identity before termination and treats verified absence as success.
	RemoveProcess(context.Context, OwnedReceiptRequest) (CleanupObservation, error)
	// EnsureNamespace captures one namespace receipt from an already verified keeper or init process.
	EnsureNamespace(context.Context, NamespaceRequest) (ownership.Receipt, error)
	// InspectNamespace verifies one namespace inode and its receipt-bound process owner.
	InspectNamespace(context.Context, OwnedReceiptRequest) (ResourceObservation, error)
	// RemoveNamespace closes provider handles and proves the exact namespace is no longer owned.
	RemoveNamespace(context.Context, OwnedReceiptRequest) (CleanupObservation, error)
	// EnsureRootfs prepares or rediscovers only one Attempt mount-namespace rootfs view.
	EnsureRootfs(context.Context, RootfsRequest) (ownership.Receipt, error)
	// InspectRootfs verifies one receipt-bound rootfs view without accepting a caller path.
	InspectRootfs(context.Context, OwnedReceiptRequest) (ResourceObservation, error)
	// RemoveRootfs idempotently detaches one exact receipt-bound rootfs view.
	RemoveRootfs(context.Context, OwnedReceiptRequest) (CleanupObservation, error)
	// EnsureStartGate creates or rediscovers only one closed one-shot Attempt gate.
	EnsureStartGate(context.Context, AttemptResourceRequest) (ownership.Receipt, error)
	// InspectStartGate verifies one receipt-bound gate without releasing it.
	InspectStartGate(context.Context, OwnedReceiptRequest) (ResourceObservation, error)
	// RemoveStartGate idempotently closes one exact unconsumed gate.
	RemoveStartGate(context.Context, OwnedReceiptRequest) (CleanupObservation, error)
	// EnsureStreams creates or rediscovers only the owned stdout and stderr stream endpoints.
	EnsureStreams(context.Context, AttemptResourceRequest) (ownership.Receipt, error)
	// InspectStreams verifies one receipt-bound stream set.
	InspectStreams(context.Context, OwnedReceiptRequest) (ResourceObservation, error)
	// RemoveStreams idempotently closes and publishes one exact stream set.
	RemoveStreams(context.Context, OwnedReceiptRequest) (CleanupObservation, error)
	// ReleaseStartGate opens a gate only after verified init-to-cgroup attachment evidence is supplied.
	ReleaseStartGate(context.Context, ReleaseGateRequest) (ResourceObservation, error)
	// SignalVerified reverifies strong process identity inside the provider immediately before pidfd delivery.
	SignalVerified(context.Context, SignalRequest) (SignalObservation, error)
}

// SandboxCgroupRequest names one Sandbox cgroup through typed identity and deterministic owner input.
type SandboxCgroupRequest struct {
	Owner     ownership.OwnerKey `json:"owner"`
	SandboxID domain.SandboxID   `json:"sandbox_id"`
}

// KeeperCgroupRequest names the process-bearing keeper leaf through its already acquired Sandbox parent receipt.
type KeeperCgroupRequest struct {
	Owner     ownership.OwnerKey `json:"owner"`
	SandboxID domain.SandboxID   `json:"sandbox_id"`
	Parent    ownership.Receipt  `json:"parent"`
}

// Validate rejects a keeper leaf request unless its process-free parent belongs to the same owner.
func (r KeeperCgroupRequest) Validate() error {
	if err := r.SandboxID.Validate(); err != nil {
		return err
	}
	return validateReceipt(r.Owner, r.Parent, ownership.ProviderCgroupV2, ownership.KindSandboxCgroup)
}

// Validate rejects an invalid owner or Sandbox identity before cgroup acquisition.
func (r SandboxCgroupRequest) Validate() error {
	if err := r.Owner.Validate(); err != nil {
		return fmt.Errorf("sandbox cgroup owner: %w", err)
	}
	return r.SandboxID.Validate()
}

// AttemptCgroupRequest names one Attempt cgroup and preserves requests separately from enforced limits.
type AttemptCgroupRequest struct {
	Owner     ownership.OwnerKey            `json:"owner"`
	SandboxID domain.SandboxID              `json:"sandbox_id"`
	AttemptID domain.AttemptID              `json:"attempt_id"`
	Limits    domain.ResolvedResourceLimits `json:"limits"`
}

// Validate rejects invalid identity or resource policy before any cgroup acquisition.
func (r AttemptCgroupRequest) Validate() error {
	if err := r.Owner.Validate(); err != nil {
		return fmt.Errorf("attempt cgroup owner: %w", err)
	}
	if err := r.SandboxID.Validate(); err != nil {
		return err
	}
	if err := r.AttemptID.Validate(); err != nil {
		return err
	}
	return r.Limits.Validate()
}

// OwnedReceiptRequest authorizes discovery only through a deterministic owner and validated receipt.
type OwnedReceiptRequest struct {
	Owner   ownership.OwnerKey `json:"owner"`
	Receipt ownership.Receipt  `json:"receipt"`
}

// ValidateFor rejects owner drift or a receipt outside the expected provider and kind set.
func (r OwnedReceiptRequest) ValidateFor(expectedProvider ownership.Provider, expectedKinds ...ownership.Kind) error {
	if err := r.Owner.Validate(); err != nil {
		return fmt.Errorf("request owner: %w", err)
	}
	if err := r.Receipt.Validate(); err != nil {
		return fmt.Errorf("request receipt: %w", err)
	}
	if r.Receipt.Owner != r.Owner {
		return errors.New("receipt owner does not match request owner")
	}
	if r.Receipt.Provider != expectedProvider {
		return fmt.Errorf("receipt provider %q does not match expected provider %q", r.Receipt.Provider, expectedProvider)
	}
	for _, kind := range expectedKinds {
		if r.Receipt.Kind == kind {
			return nil
		}
	}
	return fmt.Errorf("receipt kind %q is not accepted by this provider method", r.Receipt.Kind)
}

// AttachProcessRequest binds one strong init-process receipt to one owned Attempt cgroup receipt.
type AttachProcessRequest struct {
	Owner   ownership.OwnerKey `json:"owner"`
	Cgroup  ownership.Receipt  `json:"cgroup"`
	Process ownership.Receipt  `json:"process"`
}

// Validate rejects cross-owner attachment or anything other than Attempt-cgroup and init-process receipts.
func (r AttachProcessRequest) Validate() error {
	if err := validateReceipt(r.Owner, r.Cgroup, ownership.ProviderCgroupV2, ownership.KindAttemptCgroup); err != nil {
		return fmt.Errorf("attach cgroup: %w", err)
	}
	if err := validateReceipt(r.Owner, r.Process, ownership.ProviderLinux, ownership.KindInitProcess); err != nil {
		return fmt.Errorf("attach process: %w", err)
	}
	return nil
}

// KeeperProcessRequest describes only the keeper process effect and its already acquired parent cgroup.
type KeeperProcessRequest struct {
	Owner     ownership.OwnerKey `json:"owner"`
	SandboxID domain.SandboxID   `json:"sandbox_id"`
	Cgroup    ownership.Receipt  `json:"cgroup"`
}

// Validate rejects a keeper request not bound to the same owner and Sandbox cgroup.
func (r KeeperProcessRequest) Validate() error {
	if err := r.SandboxID.Validate(); err != nil {
		return err
	}
	return validateReceipt(r.Owner, r.Cgroup, ownership.ProviderCgroupV2, ownership.KindKeeperCgroup)
}

// AttemptResourceRequest names one provider-owned Attempt resource without accepting a host path.
type AttemptResourceRequest struct {
	Owner     ownership.OwnerKey `json:"owner"`
	AttemptID domain.AttemptID   `json:"attempt_id"`
}

// Validate rejects invalid owner or Attempt identity before gate or stream acquisition.
func (r AttemptResourceRequest) Validate() error {
	if err := r.Owner.Validate(); err != nil {
		return fmt.Errorf("attempt resource owner: %w", err)
	}
	return r.AttemptID.Validate()
}

// InitProcessRequest describes only init creation and references separately acquired cgroup, gate, and streams.
type InitProcessRequest struct {
	Owner             ownership.OwnerKey `json:"owner"`
	SandboxID         domain.SandboxID   `json:"sandbox_id"`
	AttemptID         domain.AttemptID   `json:"attempt_id"`
	Cgroup            ownership.Receipt  `json:"cgroup"`
	Gate              ownership.Receipt  `json:"gate"`
	Streams           ownership.Receipt  `json:"streams"`
	SandboxNamespaces SandboxNamespaces  `json:"sandbox_namespaces"`
	Process           domain.ProcessSpec `json:"process"`
}

// Validate rejects an init request unless every dependency shares the same owner and exact resource kind.
func (r InitProcessRequest) Validate() error {
	if err := r.SandboxID.Validate(); err != nil {
		return err
	}
	if err := r.AttemptID.Validate(); err != nil {
		return err
	}
	dependencies := []struct {
		name     string
		receipt  ownership.Receipt
		provider ownership.Provider
		kind     ownership.Kind
	}{
		{name: "cgroup", receipt: r.Cgroup, provider: ownership.ProviderCgroupV2, kind: ownership.KindAttemptCgroup},
		{name: "gate", receipt: r.Gate, provider: ownership.ProviderLinux, kind: ownership.KindStartGate},
		{name: "streams", receipt: r.Streams, provider: ownership.ProviderLinux, kind: ownership.KindStreams},
	}
	for _, dependency := range dependencies {
		if err := validateReceipt(r.Owner, dependency.receipt, dependency.provider, dependency.kind); err != nil {
			return fmt.Errorf("init %s: %w", dependency.name, err)
		}
	}
	if err := r.SandboxNamespaces.ValidateFor(r.SandboxID); err != nil {
		return fmt.Errorf("init Sandbox namespaces: %w", err)
	}
	return r.Process.Validate()
}

// SandboxNamespaces is the exact adopted stable namespace authority an Attempt launcher may join.
// The receipts retain their original Sandbox-create owner and are never rebound to the Container operation.
type SandboxNamespaces struct {
	UTS     ownership.Receipt `json:"uts"`
	IPC     ownership.Receipt `json:"ipc"`
	Network ownership.Receipt `json:"network"`
}

// ValidateFor rejects non-adopted, cross-Sandbox, cross-owner, or wrong-kind namespace authority.
func (namespaces SandboxNamespaces) ValidateFor(sandboxID domain.SandboxID) error {
	if err := sandboxID.Validate(); err != nil {
		return err
	}
	expectations := []struct {
		name    string
		receipt ownership.Receipt
		kind    ownership.Kind
	}{
		{name: "UTS", receipt: namespaces.UTS, kind: ownership.KindUTSNamespace},
		{name: "IPC", receipt: namespaces.IPC, kind: ownership.KindIPCNamespace},
		{name: "network", receipt: namespaces.Network, kind: ownership.KindNetworkNamespace},
	}
	var owner ownership.OwnerKey
	for index, expectation := range expectations {
		receipt := expectation.receipt
		if err := receipt.Validate(); err != nil {
			return fmt.Errorf("%s receipt: %w", expectation.name, err)
		}
		if receipt.Provider != ownership.ProviderLinux || receipt.Kind != expectation.kind || !receipt.Adopted {
			return fmt.Errorf("%s receipt must be an adopted Linux %s resource", expectation.name, expectation.kind)
		}
		if receipt.Owner.Target.Kind != operation.TargetSandbox || receipt.Owner.Target.ID != string(sandboxID) {
			return fmt.Errorf("%s receipt belongs to another Sandbox", expectation.name)
		}
		if index == 0 {
			owner = receipt.Owner
		} else if receipt.Owner != owner {
			return errors.New("Sandbox namespace receipts do not share one acquisition owner")
		}
	}
	return nil
}

// NamespaceRequest captures exactly one namespace from an already strong process receipt.
type NamespaceRequest struct {
	Owner       ownership.OwnerKey      `json:"owner"`
	Process     ownership.Receipt       `json:"process"`
	Namespace   isolation.NamespaceType `json:"namespace"`
	Hostname    string                  `json:"hostname,omitempty"`
	NetworkMode SandboxNetworkMode      `json:"network_mode,omitempty"`
}

// Validate enforces keeper ownership for Sandbox namespaces, init ownership for
// Attempt namespaces, and an exact configuration only on the namespace it affects.
func (r NamespaceRequest) Validate() error {
	if !r.Namespace.Valid() {
		return fmt.Errorf("unsupported namespace %q", r.Namespace)
	}
	expectedProcess := ownership.KindKeeperProcess
	if r.Namespace == isolation.NamespacePID || r.Namespace == isolation.NamespaceMount {
		expectedProcess = ownership.KindInitProcess
	}
	if err := validateReceipt(r.Owner, r.Process, ownership.ProviderLinux, expectedProcess); err != nil {
		return err
	}
	switch r.Namespace {
	case isolation.NamespaceUTS:
		if err := validateHostnameInput(r.Hostname); err != nil {
			return err
		}
		if r.NetworkMode != "" {
			return errors.New("UTS namespace request must not contain network configuration")
		}
	case isolation.NamespaceNetwork:
		if r.Hostname != "" {
			return errors.New("network namespace request must not contain a hostname")
		}
		if _, err := r.NetworkMode.Canonical(); err != nil {
			return err
		}
	default:
		if r.Hostname != "" || r.NetworkMode != "" {
			return errors.New("IPC, PID, and mount namespace requests must not contain Sandbox UTS or network configuration")
		}
	}
	return nil
}

// validateHostnameInput rejects values that cannot fit a Linux UTS nodename or
// a bounded receipt attribute before an isolation launcher can be called.
func validateHostnameInput(hostname string) error {
	if hostname == "" {
		return nil
	}
	if len(hostname) > maxSandboxHostnameBytes || !utf8.ValidString(hostname) || strings.ContainsRune(hostname, '\x00') {
		return errors.New("UTS hostname must be valid UTF-8 without NUL and no longer than 64 bytes")
	}
	return nil
}

// ReceiptKind returns the exact ownership kind produced for this namespace request.
func (r NamespaceRequest) ReceiptKind() (ownership.Kind, error) {
	switch r.Namespace {
	case isolation.NamespaceUTS:
		return ownership.KindUTSNamespace, nil
	case isolation.NamespaceIPC:
		return ownership.KindIPCNamespace, nil
	case isolation.NamespaceNetwork:
		return ownership.KindNetworkNamespace, nil
	case isolation.NamespacePID:
		return ownership.KindPIDNamespace, nil
	case isolation.NamespaceMount:
		return ownership.KindMountNamespace, nil
	default:
		return "", fmt.Errorf("unsupported namespace %q", r.Namespace)
	}
}

// RootfsRequest identifies a prepared source only after the owner-verified init, PID namespace, and mount namespace were checkpointed.
type RootfsRequest struct {
	Owner     ownership.OwnerKey `json:"owner"`
	AttemptID domain.AttemptID   `json:"attempt_id"`
	Process   ownership.Receipt  `json:"process"`
	PID       ownership.Receipt  `json:"pid_namespace"`
	Mount     ownership.Receipt  `json:"mount_namespace"`
	SourceID  OpaqueID           `json:"source_id"`
	DNS       []string           `json:"dns,omitempty"`
}

// Validate rejects path-shaped source authority, malformed retained Sandbox DNS,
// or rootfs work attempted before all namespace evidence belongs to the same init owner.
func (r RootfsRequest) Validate() error {
	if err := r.AttemptID.Validate(); err != nil {
		return err
	}
	if err := r.SourceID.Validate(); err != nil {
		return err
	}
	if err := validateDNSInput(r.DNS); err != nil {
		return err
	}
	dependencies := []struct {
		name    string
		receipt ownership.Receipt
		kind    ownership.Kind
	}{
		{name: "process", receipt: r.Process, kind: ownership.KindInitProcess},
		{name: "pid namespace", receipt: r.PID, kind: ownership.KindPIDNamespace},
		{name: "mount namespace", receipt: r.Mount, kind: ownership.KindMountNamespace},
	}
	for _, dependency := range dependencies {
		if err := validateReceipt(r.Owner, dependency.receipt, ownership.ProviderLinux, dependency.kind); err != nil {
			return fmt.Errorf("rootfs %s: %w", dependency.name, err)
		}
	}
	return nil
}

// validateDNSInput keeps the rootfs provider contract aligned with the immutable
// SandboxSpec bound of at most three resolv.conf IP address literals.
func validateDNSInput(servers []string) error {
	if len(servers) > maxSandboxDNSServers {
		return errors.New("rootfs DNS must contain no more than 3 servers")
	}
	for index, server := range servers {
		if net.ParseIP(server) == nil {
			return fmt.Errorf("DNS server %d must be an IP address literal", index)
		}
	}
	return nil
}

// ReleaseGateRequest binds gate release to the same init owner, prepared rootfs, and verified cgroup attachment observation.
type ReleaseGateRequest struct {
	Owner      ownership.OwnerKey    `json:"owner"`
	Gate       ownership.Receipt     `json:"gate"`
	Process    ownership.Receipt     `json:"process"`
	Cgroup     ownership.Receipt     `json:"cgroup"`
	Rootfs     ownership.Receipt     `json:"rootfs"`
	Attachment AttachmentObservation `json:"attachment"`
}

// Validate prevents releasing a gate for another owner or before rootfs preparation and cgroup membership were verified.
func (r ReleaseGateRequest) Validate() error {
	if err := validateReceipt(r.Owner, r.Gate, ownership.ProviderLinux, ownership.KindStartGate); err != nil {
		return fmt.Errorf("release gate: %w", err)
	}
	if err := validateReceipt(r.Owner, r.Process, ownership.ProviderLinux, ownership.KindInitProcess); err != nil {
		return fmt.Errorf("release process: %w", err)
	}
	if err := validateReceipt(r.Owner, r.Cgroup, ownership.ProviderCgroupV2, ownership.KindAttemptCgroup); err != nil {
		return fmt.Errorf("release cgroup: %w", err)
	}
	if err := validateReceipt(r.Owner, r.Rootfs, ownership.ProviderLinux, ownership.KindRootfsMount); err != nil {
		return fmt.Errorf("release rootfs: %w", err)
	}
	return r.Attachment.ValidateFor(AttachProcessRequest{Owner: r.Owner, Cgroup: r.Cgroup, Process: r.Process})
}

// SignalRequest authorizes one idempotent Kill step through a strong process receipt and has deliberately no PID or path field.
type SignalRequest struct {
	Owner             ownership.OwnerKey    `json:"owner"`
	Process           ownership.Receipt     `json:"process"`
	ActionOperationID operation.OperationID `json:"action_operation_id"`
	Step              SignalStep            `json:"step"`
	Signal            Signal                `json:"signal"`
}

// Validate rejects an unkeyed, raw, or unsupported signal intent and any cross-owner process receipt.
func (r SignalRequest) Validate() error {
	if err := r.ActionOperationID.Validate(); err != nil {
		return fmt.Errorf("signal action operation: %w", err)
	}
	if !r.Step.Valid() {
		return fmt.Errorf("unsupported signal step %q", r.Step)
	}
	if !r.Signal.Valid() {
		return fmt.Errorf("unsupported signal %q", r.Signal)
	}
	return validateReceipt(r.Owner, r.Process, ownership.ProviderLinux, ownership.KindKeeperProcess, ownership.KindInitProcess)
}

// validateReceipt checks a strong owner binding and one of the exact provider resource kinds accepted by a method.
func validateReceipt(owner ownership.OwnerKey, receipt ownership.Receipt, expectedProvider ownership.Provider, expectedKinds ...ownership.Kind) error {
	return (OwnedReceiptRequest{Owner: owner, Receipt: receipt}).ValidateFor(expectedProvider, expectedKinds...)
}
