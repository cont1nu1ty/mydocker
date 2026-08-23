package provider

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/rollback"
)

// testOwner constructs a validated deterministic owner for provider contract tests.
func testOwner(t *testing.T, operationID operation.OperationID) ownership.OwnerKey {
	t.Helper()
	owner, err := ownership.NewOwnerKey(
		operationID,
		operation.Target{Kind: operation.TargetAttempt, ID: "attempt-contract"},
		domain.InitialGeneration,
	)
	if err != nil {
		t.Fatalf("NewOwnerKey() error = %v", err)
	}
	return owner
}

// testSandboxOwner constructs the original owner retained by adopted stable namespace receipts.
func testSandboxOwner(t *testing.T, operationID operation.OperationID) ownership.OwnerKey {
	t.Helper()
	owner, err := ownership.NewOwnerKey(
		operationID,
		operation.Target{Kind: operation.TargetSandbox, ID: "sandbox-contract"},
		domain.InitialGeneration,
	)
	if err != nil {
		t.Fatalf("NewOwnerKey() Sandbox error = %v", err)
	}
	return owner
}

// testReceipt constructs one valid receipt with deterministic evidence and caller-selected ownership kind.
func testReceipt(t *testing.T, owner ownership.OwnerKey, kind ownership.Kind, localID string, adopted bool) ownership.Receipt {
	t.Helper()
	providerName := ownership.ProviderLinux
	if kind == ownership.KindSandboxCgroup || kind == ownership.KindAttemptCgroup || kind == ownership.KindKeeperCgroup {
		providerName = ownership.ProviderCgroupV2
	}
	evidence, err := ownership.EvidenceDigest(struct {
		Kind    ownership.Kind `json:"kind"`
		LocalID string         `json:"local_id"`
	}{Kind: kind, LocalID: localID})
	if err != nil {
		t.Fatalf("EvidenceDigest() error = %v", err)
	}
	return ownership.Receipt{
		SchemaVersion:  ownership.SchemaVersion,
		Provider:       providerName,
		Kind:           kind,
		LocalID:        localID,
		Owner:          owner,
		EvidenceSHA256: evidence,
		Attributes:     map[string]string{"fact": localID},
		Adopted:        adopted,
	}
}

// fullCapabilities returns a complete M2 preflight observation for satisfaction tests.
func fullCapabilities() Capabilities {
	requirements := M2Requirements()
	return Capabilities{
		SchemaVersion: SchemaVersion,
		Cgroup: CgroupCapabilities{
			UnifiedV2:   true,
			Delegated:   true,
			Controllers: append([]CgroupController(nil), requirements.Cgroup.Controllers...),
		},
		Isolation: IsolationCapabilities{
			Rootful:    true,
			PIDFD:      true,
			PivotRoot:  true,
			StartGate:  true,
			Streams:    true,
			Namespaces: append([]isolation.NamespaceType(nil), requirements.Isolation.Namespaces...),
		},
	}
}

// TestCapabilitiesSatisfyTypedM2Requirements verifies exact typed features pass and any missing fact fails closed.
func TestCapabilitiesSatisfyTypedM2Requirements(t *testing.T) {
	requirements := M2Requirements()
	if err := requirements.Validate(); err != nil {
		t.Fatalf("M2Requirements().Validate() error = %v", err)
	}
	capabilities := fullCapabilities()
	if err := capabilities.Satisfies(requirements); err != nil {
		t.Fatalf("Capabilities.Satisfies() error = %v", err)
	}

	missingController := fullCapabilities()
	missingController.Cgroup.Controllers = missingController.Cgroup.Controllers[:2]
	if err := missingController.Satisfies(requirements); err == nil || !strings.Contains(err.Error(), "pids") {
		t.Fatalf("missing controller error = %v, want pids failure", err)
	}
	missingNamespace := fullCapabilities()
	missingNamespace.Isolation.Namespaces = missingNamespace.Isolation.Namespaces[:4]
	if err := missingNamespace.Satisfies(requirements); err == nil || !strings.Contains(err.Error(), "mnt") {
		t.Fatalf("missing namespace error = %v, want mount failure", err)
	}
	duplicate := requirements
	duplicate.Cgroup.Controllers = append(duplicate.Cgroup.Controllers, ControllerCPU)
	if err := duplicate.Validate(); err == nil {
		t.Fatal("Requirements.Validate() accepted a duplicate controller")
	}
}

// TestTypedRequestsRejectCrossOwnerAndPathAuthority verifies provider calls are owner-bound and never authorized by arbitrary paths.
func TestTypedRequestsRejectCrossOwnerAndPathAuthority(t *testing.T) {
	owner := testOwner(t, "op-provider")
	otherOwner := testOwner(t, "op-other")
	cgroupReceipt := testReceipt(t, owner, ownership.KindAttemptCgroup, "attempt-cgroup", false)
	gateReceipt := testReceipt(t, owner, ownership.KindStartGate, "start-gate", false)
	streamsReceipt := testReceipt(t, owner, ownership.KindStreams, "streams", false)
	initReceipt := testReceipt(t, owner, ownership.KindInitProcess, "init-process", false)
	sandboxOwner := testSandboxOwner(t, "op-sandbox-provider")
	sandboxNamespaces := SandboxNamespaces{
		UTS:     testReceipt(t, sandboxOwner, ownership.KindUTSNamespace, "sandbox-uts", true),
		IPC:     testReceipt(t, sandboxOwner, ownership.KindIPCNamespace, "sandbox-ipc", true),
		Network: testReceipt(t, sandboxOwner, ownership.KindNetworkNamespace, "sandbox-net", true),
	}

	initRequest := InitProcessRequest{
		Owner: owner, SandboxID: "sandbox-contract", AttemptID: "attempt-contract",
		Cgroup: cgroupReceipt, Gate: gateReceipt, Streams: streamsReceipt, SandboxNamespaces: sandboxNamespaces,
		Process: domain.ProcessSpec{Argv: []string{"/bin/workload"}},
	}
	if err := initRequest.Validate(); err != nil {
		t.Fatalf("InitProcessRequest.Validate() error = %v", err)
	}
	initRequest.Owner = otherOwner
	if err := initRequest.Validate(); err == nil {
		t.Fatal("InitProcessRequest.Validate() accepted cross-owner dependencies")
	}

	pidReceipt := testReceipt(t, owner, ownership.KindPIDNamespace, "pid-ns", false)
	mountReceipt := testReceipt(t, owner, ownership.KindMountNamespace, "mount-ns", false)
	rootfsRequest := RootfsRequest{
		Owner: owner, AttemptID: "attempt-contract", Process: initReceipt,
		PID: pidReceipt, Mount: mountReceipt, SourceID: "prepared-rootfs-01", DNS: []string{"1.1.1.1"},
	}
	if err := rootfsRequest.Validate(); err != nil {
		t.Fatalf("RootfsRequest.Validate() error = %v", err)
	}
	rootfsRequest.SourceID = "/var/lib/mydocker/rootfs"
	if err := rootfsRequest.Validate(); err == nil {
		t.Fatal("RootfsRequest.Validate() accepted an absolute path as provider authority")
	}
	rootfsRequest.SourceID = "prepared-rootfs-01"
	rootfsRequest.Mount = ownership.Receipt{}
	if err := rootfsRequest.Validate(); err == nil {
		t.Fatal("RootfsRequest.Validate() accepted missing mount-namespace checkpoint evidence")
	}
	rootfsRequest.Mount = mountReceipt
	rootfsRequest.DNS = []string{""}
	if err := rootfsRequest.Validate(); err == nil {
		t.Fatal("RootfsRequest.Validate() accepted empty retained Sandbox DNS")
	}
	rootfsRequest.DNS = []string{"resolver.example"}
	if err := rootfsRequest.Validate(); err == nil {
		t.Fatal("RootfsRequest.Validate() accepted a non-address DNS server")
	}
	rootfsRequest.DNS = []string{"1.1.1.1", "8.8.8.8", "9.9.9.9", "2001:4860:4860::8888"}
	if err := rootfsRequest.Validate(); err == nil {
		t.Fatal("RootfsRequest.Validate() accepted more than three DNS servers")
	}

	signalRequest := SignalRequest{
		Owner: owner, Process: initReceipt, ActionOperationID: "op-signal-action",
		Step: SignalStepInitial, Signal: SignalTERM,
	}
	if err := signalRequest.Validate(); err != nil {
		t.Fatalf("SignalRequest.Validate() error = %v", err)
	}
	signalRequest.Signal = "9"
	if err := signalRequest.Validate(); err == nil {
		t.Fatal("SignalRequest.Validate() accepted a raw signal number")
	}
	signalRequest.Signal = SignalTERM
	signalRequest.Step = ""
	if err := signalRequest.Validate(); err == nil {
		t.Fatal("SignalRequest.Validate() accepted a signal without a durable step key")
	}

	rootfsReceipt := testReceipt(t, owner, ownership.KindRootfsMount, "rootfs", false)
	attachmentRequest := AttachProcessRequest{Owner: owner, Cgroup: cgroupReceipt, Process: initReceipt}
	attachment, err := NewAttachmentObservation(attachmentRequest)
	if err != nil {
		t.Fatalf("NewAttachmentObservation() error = %v", err)
	}
	release := ReleaseGateRequest{
		Owner: owner, Gate: gateReceipt, Process: initReceipt, Cgroup: cgroupReceipt,
		Rootfs: rootfsReceipt, Attachment: attachment,
	}
	if err := release.Validate(); err != nil {
		t.Fatalf("ReleaseGateRequest.Validate() error = %v", err)
	}
	release.Attachment.Verified = false
	if err := release.Validate(); err == nil {
		t.Fatal("ReleaseGateRequest.Validate() accepted an unverified cgroup attachment")
	}
	release.Attachment = attachment
	release.Rootfs = ownership.Receipt{}
	if err := release.Validate(); err == nil {
		t.Fatal("ReleaseGateRequest.Validate() accepted a missing prepared-rootfs receipt")
	}
	release.Rootfs = rootfsReceipt
	release.Cgroup = testReceipt(t, owner, ownership.KindAttemptCgroup, "different-attempt-cgroup", false)
	if err := release.Validate(); err == nil {
		t.Fatal("ReleaseGateRequest.Validate() accepted attachment evidence from another Attempt cgroup")
	}
	tampered := attachment
	tampered.ProcessReceiptSHA256 = strings.Repeat("f", 64)
	if err := tampered.Validate(); err == nil {
		t.Fatal("AttachmentObservation.Validate() accepted tampered process scope")
	}
	otherCgroup := testReceipt(t, otherOwner, ownership.KindAttemptCgroup, "other-attempt-cgroup", false)
	otherInit := testReceipt(t, otherOwner, ownership.KindInitProcess, "other-init", false)
	if err := attachment.ValidateFor(AttachProcessRequest{Owner: otherOwner, Cgroup: otherCgroup, Process: otherInit}); err == nil {
		t.Fatal("AttachmentObservation.ValidateFor() accepted another owner")
	}
}

// TestNamespaceRequestsBindKindsToProcessOwners verifies stable and per-Attempt namespaces cannot be crossed.
func TestNamespaceRequestsBindKindsToProcessOwners(t *testing.T) {
	owner := testOwner(t, "op-namespace")
	keeper := testReceipt(t, owner, ownership.KindKeeperProcess, "keeper", false)
	initProcess := testReceipt(t, owner, ownership.KindInitProcess, "init", false)

	uts := NamespaceRequest{Owner: owner, Process: keeper, Namespace: isolation.NamespaceUTS, Hostname: "sandbox-contract"}
	if err := uts.Validate(); err != nil {
		t.Fatalf("UTS NamespaceRequest.Validate() error = %v", err)
	}
	if kind, err := uts.ReceiptKind(); err != nil || kind != ownership.KindUTSNamespace {
		t.Fatalf("UTS ReceiptKind() = (%q, %v)", kind, err)
	}
	pid := NamespaceRequest{Owner: owner, Process: initProcess, Namespace: isolation.NamespacePID}
	if err := pid.Validate(); err != nil {
		t.Fatalf("PID NamespaceRequest.Validate() error = %v", err)
	}
	pid.Process = keeper
	if err := pid.Validate(); err == nil {
		t.Fatal("PID NamespaceRequest.Validate() accepted a Sandbox keeper owner")
	}
	uts.Process = initProcess
	if err := uts.Validate(); err == nil {
		t.Fatal("UTS NamespaceRequest.Validate() accepted an Attempt init owner")
	}
	overlongUTS := NamespaceRequest{
		Owner: owner, Process: keeper, Namespace: isolation.NamespaceUTS, Hostname: strings.Repeat("h", 65),
	}
	if err := overlongUTS.Validate(); err == nil {
		t.Fatal("UTS NamespaceRequest.Validate() accepted a hostname beyond the Linux nodename bound")
	}
	overlongUTS.Hostname = string([]byte{0xff})
	if err := overlongUTS.Validate(); err == nil {
		t.Fatal("UTS NamespaceRequest.Validate() accepted invalid UTF-8")
	}

	network := NamespaceRequest{
		Owner: owner, Process: keeper, Namespace: isolation.NamespaceNetwork, NetworkMode: SandboxNetworkLoopback,
	}
	if err := network.Validate(); err != nil {
		t.Fatalf("network NamespaceRequest.Validate() error = %v", err)
	}
	network.NetworkMode = "bridge"
	if err := network.Validate(); err == nil {
		t.Fatal("network NamespaceRequest.Validate() accepted a mode outside the M3 vocabulary")
	}
	network.NetworkMode = SandboxNetworkNone
	network.Hostname = "crossed-field"
	if err := network.Validate(); err == nil {
		t.Fatal("network NamespaceRequest.Validate() accepted UTS configuration")
	}

	configuredPID := pid
	configuredPID.Process = initProcess
	configuredPID.Hostname = "crossed-field"
	if err := configuredPID.Validate(); err == nil {
		t.Fatal("PID NamespaceRequest.Validate() accepted Sandbox configuration")
	}
}

// TestReceiptSetsRequireExactKindsAndOneOwner verifies complete inventories cannot mix provider kinds, owners, or adoption state.
func TestReceiptSetsRequireExactKindsAndOneOwner(t *testing.T) {
	owner := testOwner(t, "op-receipts")
	sandbox := SandboxReceipts{
		Owner:        owner,
		Cgroup:       testReceipt(t, owner, ownership.KindSandboxCgroup, "sandbox-cgroup", false),
		KeeperCgroup: testReceipt(t, owner, ownership.KindKeeperCgroup, "keeper-cgroup", false),
		Keeper:       testReceipt(t, owner, ownership.KindKeeperProcess, "keeper", false),
		UTS:          testReceipt(t, owner, ownership.KindUTSNamespace, "uts", false),
		IPC:          testReceipt(t, owner, ownership.KindIPCNamespace, "ipc", false),
		Network:      testReceipt(t, owner, ownership.KindNetworkNamespace, "network", false),
	}
	if err := sandbox.Validate(); err != nil {
		t.Fatalf("SandboxReceipts.Validate() error = %v", err)
	}
	wrongKind := sandbox
	wrongKind.Network = testReceipt(t, owner, ownership.KindMountNamespace, "network", false)
	if err := wrongKind.Validate(); err == nil {
		t.Fatal("SandboxReceipts.Validate() accepted a mount receipt as network namespace")
	}
	mixedAdoption := sandbox
	mixedAdoption.UTS, _ = mixedAdoption.UTS.Adopt()
	if err := mixedAdoption.Validate(); err == nil {
		t.Fatal("SandboxReceipts.Validate() accepted mixed adoption state")
	}

	attempt := AttemptReceipts{
		Owner:   owner,
		Cgroup:  testReceipt(t, owner, ownership.KindAttemptCgroup, "attempt-cgroup", false),
		Init:    testReceipt(t, owner, ownership.KindInitProcess, "init", false),
		PID:     testReceipt(t, owner, ownership.KindPIDNamespace, "pid", false),
		Mount:   testReceipt(t, owner, ownership.KindMountNamespace, "mount", false),
		Rootfs:  testReceipt(t, owner, ownership.KindRootfsMount, "rootfs", false),
		Gate:    testReceipt(t, owner, ownership.KindStartGate, "gate", false),
		Streams: testReceipt(t, owner, ownership.KindStreams, "streams", false),
	}
	if err := attempt.Validate(); err != nil {
		t.Fatalf("AttemptReceipts.Validate() error = %v", err)
	}
	otherOwner := testOwner(t, "op-receipts-other")
	crossOwner := attempt
	crossOwner.Rootfs = testReceipt(t, otherOwner, ownership.KindRootfsMount, "rootfs", false)
	if err := crossOwner.Validate(); err == nil {
		t.Fatal("AttemptReceipts.Validate() accepted a receipt from another owner")
	}

	clones := attempt.Receipts()
	clones[0].Attributes["fact"] = "changed"
	if reflect.DeepEqual(clones[0].Attributes, attempt.Cgroup.Attributes) {
		t.Fatal("AttemptReceipts.Receipts() retained a mutable attribute alias")
	}
}

// TestTypedObservationsFailClosed verifies inspect, cleanup, attachment, and signal success require exact evidence states, including a non-zero delivery time.
func TestTypedObservationsFailClosed(t *testing.T) {
	digest := strings.Repeat("a", 64)
	observations := []ResourceObservation{
		{Presence: PresencePresent, Verified: true, EvidenceSHA256: digest},
		{Presence: PresenceAbsent, Verified: true},
		{Presence: PresenceUnknown, Reason: "readback unavailable"},
	}
	for _, observation := range observations {
		if err := observation.Validate(); err != nil {
			t.Fatalf("ResourceObservation.Validate(%#v) error = %v", observation, err)
		}
	}
	unsafeUnknown := ResourceObservation{Presence: PresenceUnknown, Verified: true, Reason: "trusted anyway"}
	if err := unsafeUnknown.Validate(); err == nil {
		t.Fatal("ResourceObservation.Validate() accepted an authorizing unknown state")
	}
	cleanup := CleanupObservation{
		Disposition: CleanupAlreadyAbsent,
		After:       ResourceObservation{Presence: PresenceAbsent, Verified: true},
	}
	if err := cleanup.Validate(); err != nil {
		t.Fatalf("CleanupObservation.Validate() error = %v", err)
	}
	owner := testOwner(t, "op-observation-owner")
	resource, err := testReceipt(t, owner, ownership.KindAttemptCgroup, "attempt-cleanup", false).Adopt()
	if err != nil {
		t.Fatalf("Receipt.Adopt() setup error = %v", err)
	}
	if release, err := cleanup.Release("op-observation-delete", resource); err != nil || release.CleanupOperationID != "op-observation-delete" {
		t.Fatalf("CleanupObservation.Release() = (%#v, %v)", release, err)
	}
	cleanup.After = ResourceObservation{Presence: PresencePresent, Verified: true, EvidenceSHA256: digest}
	if err := cleanup.Validate(); err == nil {
		t.Fatal("CleanupObservation.Validate() accepted a present resource after cleanup")
	}
	if err := (SignalObservation{
		Signal: SignalKILL, Delivered: true, IdentityEvidenceSHA256: digest,
		DeliveredAt: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC),
	}).Validate(); err != nil {
		t.Fatalf("SignalObservation.Validate() error = %v", err)
	}
}

// TestOOMSnapshotsRequireOneAttemptScope verifies valid counters classify deltas but cross-Attempt evidence never does.
func TestOOMSnapshotsRequireOneAttemptScope(t *testing.T) {
	owner := testOwner(t, "op-oom-owner")
	receipt := testReceipt(t, owner, ownership.KindAttemptCgroup, "attempt-oom", false)
	scope := OwnedReceiptRequest{Owner: owner, Receipt: receipt}
	before, err := NewOOMSnapshot(scope, 1, 0, 0)
	if err != nil {
		t.Fatalf("NewOOMSnapshot(before) error = %v", err)
	}
	after, err := NewOOMSnapshot(scope, 2, 1, 0)
	if err != nil {
		t.Fatalf("NewOOMSnapshot(after) error = %v", err)
	}
	if evidence, err := after.Delta(before); err != nil || evidence != domain.EvidenceTrue {
		t.Fatalf("OOMSnapshot.Delta() = (%q, %v), want true", evidence, err)
	}
	otherOwner := testOwner(t, "op-oom-other")
	otherScope := OwnedReceiptRequest{Owner: otherOwner, Receipt: testReceipt(t, otherOwner, ownership.KindAttemptCgroup, "attempt-oom-other", false)}
	other, err := NewOOMSnapshot(otherScope, 2, 1, 0)
	if err != nil {
		t.Fatalf("NewOOMSnapshot(other) error = %v", err)
	}
	if evidence, err := other.Delta(before); err == nil || evidence != domain.EvidenceUnknown {
		t.Fatalf("OOMSnapshot.Delta(cross scope) = (%q, %v), want unknown error", evidence, err)
	}
	tampered := after
	tampered.OOMKill++
	if err := tampered.Validate(); err == nil {
		t.Fatal("OOMSnapshot.Validate() accepted counters that no longer match evidence")
	}
}

// TestRollbackRegistryDispatchesValidatedReceipt verifies descriptors route by provider/action only after owner validation.
func TestRollbackRegistryDispatchesValidatedReceipt(t *testing.T) {
	owner := testOwner(t, "op-rollback-provider")
	receipt := testReceipt(t, owner, ownership.KindInitProcess, "init-rollback", false)
	descriptor, err := ownership.InverseDescriptor(receipt, ownership.ActionStopProcess)
	if err != nil {
		t.Fatalf("InverseDescriptor() error = %v", err)
	}
	called := 0
	registry, err := NewRollbackRegistry(RollbackRegistration{
		Provider: ownership.ProviderLinux,
		Action:   ownership.ActionStopProcess,
		Handler: func(_ context.Context, gotOwner ownership.OwnerKey, gotReceipt ownership.Receipt) (CleanupObservation, error) {
			called++
			if gotOwner != owner || !reflect.DeepEqual(gotReceipt, receipt) {
				t.Fatalf("rollback handler input = (%#v, %#v)", gotOwner, gotReceipt)
			}
			return CleanupObservation{
				Disposition: CleanupAlreadyAbsent,
				After:       ResourceObservation{Presence: PresenceAbsent, Verified: true},
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewRollbackRegistry() error = %v", err)
	}
	resolver, err := registry.Resolver(owner)
	if err != nil {
		t.Fatalf("RollbackRegistry.Resolver() error = %v", err)
	}
	inverse, err := resolver(descriptor)
	if err != nil {
		t.Fatalf("resolver(descriptor) error = %v", err)
	}
	if called != 0 {
		t.Fatal("rollback handler ran during descriptor resolution")
	}
	if err := inverse(context.Background()); err != nil {
		t.Fatalf("resolved inverse error = %v", err)
	}
	if called != 1 {
		t.Fatalf("rollback handler calls = %d, want 1", called)
	}
}

// TestRollbackRegistryRejectsUnknownOwnerAndUnsafeSuccess verifies every recovery ambiguity fails closed.
func TestRollbackRegistryRejectsUnknownOwnerAndUnsafeSuccess(t *testing.T) {
	owner := testOwner(t, "op-rollback-closed")
	receipt := testReceipt(t, owner, ownership.KindInitProcess, "init-closed", false)
	descriptor, err := ownership.InverseDescriptor(receipt, ownership.ActionStopProcess)
	if err != nil {
		t.Fatalf("InverseDescriptor() error = %v", err)
	}

	emptyRegistry, err := NewRollbackRegistry()
	if err != nil {
		t.Fatalf("NewRollbackRegistry() error = %v", err)
	}
	resolver, err := emptyRegistry.Resolver(owner)
	if err != nil {
		t.Fatalf("RollbackRegistry.Resolver() error = %v", err)
	}
	if _, err := resolver(descriptor); !errors.Is(err, ErrUnknownRollbackRoute) {
		t.Fatalf("unknown route error = %v", err)
	}

	otherOwner := testOwner(t, "op-rollback-other")
	registry, err := NewRollbackRegistry(RollbackRegistration{
		Provider: ownership.ProviderLinux,
		Action:   ownership.ActionStopProcess,
		Handler: func(context.Context, ownership.OwnerKey, ownership.Receipt) (CleanupObservation, error) {
			return CleanupObservation{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewRollbackRegistry() error = %v", err)
	}
	otherResolver, err := registry.Resolver(otherOwner)
	if err != nil {
		t.Fatalf("RollbackRegistry.Resolver() error = %v", err)
	}
	if _, err := otherResolver(descriptor); !errors.Is(err, ErrRollbackOwnerMismatch) {
		t.Fatalf("owner mismatch error = %v", err)
	}

	ownerResolver, err := registry.Resolver(owner)
	if err != nil {
		t.Fatalf("RollbackRegistry.Resolver() error = %v", err)
	}
	inverse, err := ownerResolver(descriptor)
	if err != nil {
		t.Fatalf("resolver(descriptor) error = %v", err)
	}
	if err := inverse(context.Background()); err == nil || !strings.Contains(err.Error(), "unsafe success") {
		t.Fatalf("unsafe cleanup result error = %v", err)
	}

	if _, err := ownerResolver(rollback.Descriptor{}); err == nil {
		t.Fatal("rollback resolver accepted a descriptor outside ownership.ReceiptFromDescriptor")
	}
	if _, err := NewRollbackRegistry(RollbackRegistration{
		Provider: ownership.ProviderLinux,
		Action:   ownership.ActionRemoveCgroup,
		Handler: func(context.Context, ownership.OwnerKey, ownership.Receipt) (CleanupObservation, error) {
			return CleanupObservation{}, nil
		},
	}); err == nil {
		t.Fatal("NewRollbackRegistry() accepted an impossible provider/action route")
	}
}
