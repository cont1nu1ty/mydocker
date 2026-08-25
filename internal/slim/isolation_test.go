package slim

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mydocker/internal/cgroupv2"
	"mydocker/internal/domain"
	"mydocker/internal/engine"
	"mydocker/internal/isolation"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	providerapi "mydocker/internal/provider"
	"mydocker/internal/shim"
)

// fakeRuntime retains fake strong resources, wrappers, and children without executing host operations.
type fakeRuntime struct {
	mu        sync.Mutex
	resources map[string]string
	configs   map[string]string
	wrappers  map[string]*shim.Wrapper
	children  map[string]*fakeSlimChild
}

// newFakeRuntime initializes the shared fake launcher and shim-client state.
func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{
		resources: make(map[string]string), configs: make(map[string]string), wrappers: make(map[string]*shim.Wrapper),
		children: make(map[string]*fakeSlimChild),
	}
}

// fakeLauncher implements the complete host contract through in-memory strong evidence only.
type fakeLauncher struct {
	runtime               *fakeRuntime
	namespaceMu           sync.Mutex
	lastNamespaces        map[isolation.NamespaceType]NamespaceLaunch
	rootfsMu              sync.Mutex
	lastRootfs            RootfsLaunch
	dropNamespaceResponse bool
	droppedNamespace      atomic.Bool
	dropRootfsResponse    bool
	droppedRootfs         atomic.Bool
	namespaceCalls        atomic.Int32
	rootfsCalls           atomic.Int32
	inspectErr            error
}

// Preflight reports exactly the requested fake capabilities for non-privileged contract tests.
func (launcher *fakeLauncher) Preflight(ctx context.Context, requirements providerapi.IsolationRequirements) (providerapi.IsolationCapabilities, error) {
	if err := validateContext(ctx); err != nil {
		return providerapi.IsolationCapabilities{}, err
	}
	return providerapi.IsolationCapabilities{
		Rootful: requirements.Rootful, PIDFD: requirements.PIDFD, PivotRoot: requirements.PivotRoot,
		StartGate: requirements.StartGate, Streams: requirements.Streams,
		Namespaces: append([]isolation.NamespaceType(nil), requirements.Namespaces...),
	}, nil
}

// EnsureKeeper registers one strong fake keeper and ready shim endpoint idempotently.
func (launcher *fakeLauncher) EnsureKeeper(ctx context.Context, request KeeperLaunch) (LaunchedProcess, error) {
	if err := validateContext(ctx); err != nil {
		return LaunchedProcess{}, err
	}
	processEvidence := fakeProcessEvidence(request.Owner)
	identity := mustDigest(processEvidence)
	wrapperEvidence := fakeEvidence(request.Owner, ownership.KindKeeperProcess, "wrapper")
	key := fakeResourceKey(request.Owner, ownership.KindKeeperProcess)
	launcher.runtime.mu.Lock()
	defer launcher.runtime.mu.Unlock()
	if existing, found := launcher.runtime.resources[key]; found {
		return LaunchedProcess{IdentityEvidenceSHA256: existing, WrapperEvidenceSHA256: wrapperEvidence, ProcessEvidence: processEvidence}, nil
	}
	wrapper, err := shim.NewKeeper(shim.KeeperSpec{
		Owner: request.Owner, SandboxID: request.SandboxID, WrapperEvidence: wrapperEvidence,
	})
	if err != nil {
		return LaunchedProcess{}, err
	}
	launcher.runtime.resources[key] = identity
	launcher.runtime.wrappers[request.Paths.ControlSocket] = wrapper
	return LaunchedProcess{IdentityEvidenceSHA256: identity, WrapperEvidenceSHA256: wrapperEvidence, ProcessEvidence: processEvidence}, nil
}

// EnsureInit registers one closed-gate fake init wrapper and injectable child idempotently.
func (launcher *fakeLauncher) EnsureInit(ctx context.Context, request InitLaunch) (LaunchedProcess, error) {
	if err := validateContext(ctx); err != nil {
		return LaunchedProcess{}, err
	}
	processEvidence := fakeProcessEvidence(request.Owner)
	identity := mustDigest(processEvidence)
	wrapperEvidence := fakeEvidence(request.Owner, ownership.KindInitProcess, "wrapper")
	key := fakeResourceKey(request.Owner, ownership.KindInitProcess)
	launcher.runtime.mu.Lock()
	defer launcher.runtime.mu.Unlock()
	if existing, found := launcher.runtime.resources[key]; found {
		return LaunchedProcess{IdentityEvidenceSHA256: existing, WrapperEvidenceSHA256: wrapperEvidence, ProcessEvidence: processEvidence}, nil
	}
	child := newFakeSlimChild(request.Owner)
	wrapper, err := shim.NewInit(shim.InitSpec{
		Owner: request.Owner, SandboxID: request.SandboxID, ContainerID: domain.ContainerID(request.Owner.Target.ID),
		AttemptID: request.AttemptID, WrapperEvidence: wrapperEvidence, Process: request.Process.Clone(),
	}, shim.InitDependencies{
		Runner: &fakeSlimRunner{child: child}, Stdout: io.Discard, Stderr: io.Discard,
		Terminal: &fakeSlimTerminal{}, Clock: fakeSlimClock{},
	})
	if err != nil {
		return LaunchedProcess{}, err
	}
	launcher.runtime.resources[key] = identity
	launcher.runtime.wrappers[request.Paths.ControlSocket] = wrapper
	launcher.runtime.children[request.Owner.Token] = child
	return LaunchedProcess{IdentityEvidenceSHA256: identity, WrapperEvidenceSHA256: wrapperEvidence, ProcessEvidence: processEvidence}, nil
}

// fakeProcessEvidence returns complete serializable evidence so receipt replay
// tests exercise daemon-restart pidfd restoration inputs rather than a bare PID.
func fakeProcessEvidence(owner ownership.OwnerKey) isolation.ProcessEvidence {
	return isolation.ProcessEvidence{
		PID: 4242, BootID: "fake-boot-id", StartTime: 123456,
		CgroupPath: "/mydocker/" + owner.Token, Executable: "/usr/libexec/mydocker-shim",
	}
}

// EnsureNamespace records one owner/kind-specific fake namespace identity.
func (launcher *fakeLauncher) EnsureNamespace(ctx context.Context, request NamespaceLaunch) (string, error) {
	if err := validateContext(ctx); err != nil {
		return "", err
	}
	launcher.namespaceCalls.Add(1)
	kind, err := namespaceKind(request.Namespace)
	if err != nil {
		return "", err
	}
	if request.Namespace == isolation.NamespaceUTS || request.Namespace == isolation.NamespaceNetwork {
		expected, digestErr := namespaceConfigurationDigest(request.Namespace, request.Hostname, request.NetworkMode)
		if digestErr != nil {
			return "", digestErr
		}
		if request.ConfigurationSHA256 != expected {
			return "", errors.New("fake namespace configuration fingerprint differs")
		}
	} else if request.Hostname != "" || request.NetworkMode != "" || request.ConfigurationSHA256 != "" {
		return "", errors.New("fake unrelated namespace received Sandbox configuration")
	}
	evidence, err := ownership.EvidenceDigest(struct {
		Owner               ownership.OwnerKey             `json:"owner"`
		Kind                ownership.Kind                 `json:"kind"`
		ProcessEvidence     string                         `json:"process_evidence_sha256"`
		Hostname            string                         `json:"hostname,omitempty"`
		NetworkMode         providerapi.SandboxNetworkMode `json:"network_mode,omitempty"`
		ConfigurationSHA256 string                         `json:"configuration_sha256,omitempty"`
	}{
		Owner: request.Process.Owner, Kind: kind, ProcessEvidence: request.Process.LauncherEvidenceSHA256,
		Hostname: request.Hostname, NetworkMode: request.NetworkMode, ConfigurationSHA256: request.ConfigurationSHA256,
	})
	if err != nil {
		return "", err
	}
	launcher.runtime.mu.Lock()
	key := fakeResourceKey(request.Process.Owner, kind)
	if existing, found := launcher.runtime.resources[key]; found && existing != evidence {
		launcher.runtime.mu.Unlock()
		return "", errors.New("fake namespace already exists with another configuration")
	}
	launcher.runtime.resources[key] = evidence
	launcher.runtime.configs[key] = request.ConfigurationSHA256
	launcher.runtime.mu.Unlock()
	launcher.namespaceMu.Lock()
	if launcher.lastNamespaces == nil {
		launcher.lastNamespaces = make(map[isolation.NamespaceType]NamespaceLaunch)
	}
	launcher.lastNamespaces[request.Namespace] = request
	launcher.namespaceMu.Unlock()
	if launcher.dropNamespaceResponse && launcher.droppedNamespace.CompareAndSwap(false, true) {
		return "", errors.New("injected namespace response loss after acquisition")
	}
	return evidence, nil
}

// PrepareRootfs records only the catalog-resolved rootfs input and fake evidence.
func (launcher *fakeLauncher) PrepareRootfs(ctx context.Context, request RootfsLaunch) (string, error) {
	if err := validateContext(ctx); err != nil {
		return "", err
	}
	launcher.rootfsCalls.Add(1)
	expected, err := dnsConfigurationDigest(request.DNS)
	if err != nil {
		return "", err
	}
	if request.ConfigurationSHA256 != expected {
		return "", errors.New("fake rootfs DNS configuration fingerprint differs")
	}
	evidence, err := ownership.EvidenceDigest(struct {
		Owner                ownership.OwnerKey   `json:"owner"`
		AttemptID            domain.AttemptID     `json:"attempt_id"`
		SourceID             providerapi.OpaqueID `json:"source_id"`
		SourceRootfs         string               `json:"source_rootfs"`
		PIDReceiptEvidence   string               `json:"pid_receipt_evidence_sha256"`
		MountReceiptEvidence string               `json:"mount_receipt_evidence_sha256"`
		ConfigurationSHA256  string               `json:"configuration_sha256"`
	}{
		Owner: request.Owner, AttemptID: request.AttemptID, SourceID: request.SourceID,
		SourceRootfs: request.Source.Rootfs, PIDReceiptEvidence: request.PID.ReceiptEvidenceSHA256,
		MountReceiptEvidence: request.Mount.ReceiptEvidenceSHA256, ConfigurationSHA256: request.ConfigurationSHA256,
	})
	if err != nil {
		return "", err
	}
	launcher.runtime.mu.Lock()
	key := fakeResourceKey(request.Owner, ownership.KindRootfsMount)
	if existing, found := launcher.runtime.resources[key]; found && existing != evidence {
		launcher.runtime.mu.Unlock()
		return "", errors.New("fake rootfs already exists with another DNS configuration")
	}
	launcher.runtime.resources[key] = evidence
	launcher.runtime.configs[key] = request.ConfigurationSHA256
	launcher.runtime.mu.Unlock()
	launcher.rootfsMu.Lock()
	launcher.lastRootfs = request
	launcher.rootfsMu.Unlock()
	if launcher.dropRootfsResponse && launcher.droppedRootfs.CompareAndSwap(false, true) {
		return "", errors.New("injected rootfs response loss after preparation")
	}
	return evidence, nil
}

// TestIsolationProviderRejectsEnvironmentBeforeLauncherEffects verifies
// overlong UTS and malformed DNS inputs never reach either host-effect method.
func TestIsolationProviderRejectsEnvironmentBeforeLauncherEffects(t *testing.T) {
	launcher := &fakeLauncher{runtime: newFakeRuntime()}
	fixture := newSlimFixture(t, launcher)
	ctx := context.Background()
	sandboxOwner := testOwner(t, "sandbox-invalid-environment", operation.TargetSandbox, "sandbox-invalid-environment")
	keeper, err := fixture.provider.EnsureKeeperProcess(ctx, providerapi.KeeperProcessRequest{
		Owner: sandboxOwner, SandboxID: "sandbox-invalid-environment",
		Cgroup: fakeCgroupReceipt(t, sandboxOwner, ownership.KindKeeperCgroup, "sandbox-invalid-environment", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.provider.EnsureNamespace(ctx, providerapi.NamespaceRequest{
		Owner: sandboxOwner, Process: keeper, Namespace: isolation.NamespaceUTS, Hostname: strings.Repeat("h", 65),
	}); err == nil {
		t.Fatal("overlong hostname reached provider success")
	}
	if calls := launcher.namespaceCalls.Load(); calls != 0 {
		t.Fatalf("namespace launcher calls after invalid hostname = %d, want 0", calls)
	}

	containerOwner := testOwner(t, "container-invalid-environment", operation.TargetContainer, "container-invalid-environment")
	if _, err := fixture.provider.EnsureRootfs(ctx, providerapi.RootfsRequest{
		Owner: containerOwner, AttemptID: "attempt-invalid-environment", SourceID: "prepared-one",
		DNS: []string{"resolver.example"},
	}); err == nil {
		t.Fatal("malformed DNS reached provider success")
	}
	if calls := launcher.rootfsCalls.Load(); calls != 0 {
		t.Fatalf("rootfs launcher calls after invalid DNS = %d, want 0", calls)
	}
}

// TestInspectLauncherPreservesObservationClassification verifies a temporary
// read outage passes through exactly while an unrelated permanent launcher
// failure remains distinct rather than being generalized.
func TestInspectLauncherPreservesObservationClassification(t *testing.T) {
	launcher := &fakeLauncher{runtime: newFakeRuntime()}
	fixture := newSlimFixture(t, launcher)
	owner := testOwner(t, "sandbox-inspect-classification", operation.TargetSandbox, "sandbox-inspect-classification")
	keeper, err := fixture.provider.EnsureKeeperProcess(context.Background(), providerapi.KeeperProcessRequest{
		Owner: owner, SandboxID: "sandbox-inspect-classification",
		Cgroup: fakeCgroupReceipt(t, owner, ownership.KindKeeperCgroup, "sandbox-inspect-classification", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	classified := providerapi.MarkObservationUnavailable(errors.New("temporary fake read outage"))
	launcher.inspectErr = classified
	_, err = fixture.provider.InspectProcess(context.Background(), providerapi.OwnedReceiptRequest{Owner: owner, Receipt: keeper})
	if err != classified {
		t.Fatalf("InspectProcess() error = %v, want exact classified outage", err)
	}
	permanentFailure := errors.New("injected permanent launcher failure")
	launcher.inspectErr = permanentFailure
	_, err = fixture.provider.InspectProcess(context.Background(), providerapi.OwnedReceiptRequest{Owner: owner, Receipt: keeper})
	if !errors.Is(err, permanentFailure) || providerapi.IsObservationUnavailable(err) {
		t.Fatalf("InspectProcess(permanent) error = %v, want distinct launcher failure", err)
	}
}

// Inspect verifies the exact fake launcher evidence or reports verified absence.
func (launcher *fakeLauncher) Inspect(ctx context.Context, reference ResourceReference) (providerapi.ResourceObservation, error) {
	if err := validateContext(ctx); err != nil {
		return providerapi.ResourceObservation{}, err
	}
	if launcher.inspectErr != nil {
		return providerapi.ResourceObservation{}, launcher.inspectErr
	}
	launcher.runtime.mu.Lock()
	key := fakeResourceKey(reference.Owner, reference.Kind)
	evidence, found := launcher.runtime.resources[key]
	configuration := launcher.runtime.configs[key]
	launcher.runtime.mu.Unlock()
	if !found {
		return absentObservation(), nil
	}
	if evidence != reference.LauncherEvidenceSHA256 {
		return providerapi.ResourceObservation{}, errors.New("fake strong resource evidence changed")
	}
	if configuration != reference.ConfigurationSHA256 {
		return providerapi.ResourceObservation{}, errors.New("fake live resource configuration changed")
	}
	return providerapi.ResourceObservation{Presence: providerapi.PresencePresent, Verified: true, EvidenceSHA256: evidence}, nil
}

// Remove verifies fake evidence, removes the exact resource, and reports idempotent absence.
func (launcher *fakeLauncher) Remove(ctx context.Context, reference ResourceReference) (providerapi.CleanupObservation, error) {
	observation, err := launcher.Inspect(ctx, reference)
	if err != nil {
		return providerapi.CleanupObservation{}, err
	}
	disposition := providerapi.CleanupAlreadyAbsent
	if observation.Presence == providerapi.PresencePresent {
		launcher.runtime.mu.Lock()
		key := fakeResourceKey(reference.Owner, reference.Kind)
		delete(launcher.runtime.resources, key)
		delete(launcher.runtime.configs, key)
		if reference.Kind == ownership.KindKeeperProcess || reference.Kind == ownership.KindInitProcess {
			delete(launcher.runtime.wrappers, reference.Paths.ControlSocket)
		}
		launcher.runtime.mu.Unlock()
		disposition = providerapi.CleanupRemoved
	}
	result := providerapi.CleanupObservation{Disposition: disposition, After: absentObservation()}
	return result, result.Validate()
}

// Signal action-time verifies a fake keeper before returning bounded delivery evidence.
func (launcher *fakeLauncher) Signal(ctx context.Context, reference ResourceReference, signal providerapi.Signal) (providerapi.SignalObservation, error) {
	observation, err := launcher.Inspect(ctx, reference)
	if err != nil || observation.Presence != providerapi.PresencePresent {
		return providerapi.SignalObservation{}, errors.Join(err, errors.New("fake keeper is absent"))
	}
	result := providerapi.SignalObservation{
		Signal: signal, IdentityEvidenceSHA256: reference.LauncherEvidenceSHA256,
		Delivered: true, DeliveredAt: fakeSlimClock{}.Now(),
	}
	return result, result.Validate()
}

// ResolveProcess returns a fake reference whose PID is available only after repeated evidence verification.
func (launcher *fakeLauncher) ResolveProcess(ctx context.Context, reference ResourceReference) (cgroupv2.ProcessReference, error) {
	observation, err := launcher.Inspect(ctx, reference)
	if err != nil || observation.Presence != providerapi.PresencePresent {
		return nil, errors.Join(err, errors.New("fake process is absent"))
	}
	return &fakePIDReference{runtime: launcher.runtime, reference: reference}, nil
}

// fakePIDReference supplies a diagnostic PID only after checking the current fake strong evidence.
type fakePIDReference struct {
	runtime   *fakeRuntime
	reference ResourceReference
}

// VerifiedPID action-time checks the exact resource evidence before returning a test-only PID.
func (reference *fakePIDReference) VerifiedPID(ctx context.Context) (int, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	reference.runtime.mu.Lock()
	evidence := reference.runtime.resources[fakeResourceKey(reference.reference.Owner, reference.reference.Kind)]
	reference.runtime.mu.Unlock()
	if evidence != reference.reference.LauncherEvidenceSHA256 {
		return 0, errors.New("fake PID reference strong evidence changed")
	}
	return 4242, nil
}

// fakeShimClient routes derived socket paths to in-memory wrappers and simulates fresh daemon connections.
type fakeShimClient struct {
	runtime *fakeRuntime
}

// Do forwards one request to the registered wrapper without using a Unix socket.
func (client fakeShimClient) Do(ctx context.Context, path string, expectedPeerPID int, request shim.ControlRequest) (shim.ControlResponse, int, error) {
	if err := validateContext(ctx); err != nil {
		return shim.ControlResponse{}, 0, err
	}
	if expectedPeerPID != 4242 {
		return shim.ControlResponse{}, 0, errors.New("fake shim peer differs from expected process evidence")
	}
	client.runtime.mu.Lock()
	wrapper := client.runtime.wrappers[path]
	client.runtime.mu.Unlock()
	if wrapper == nil {
		return shim.ControlResponse{}, 0, errors.New("fake shim endpoint is absent")
	}
	return wrapper.HandleControl(request), 4242, nil
}

// lossyShimClient drops the first completed signal response so provider retry must use shim's cached result.
type lossyShimClient struct {
	delegate ShimClient
	lost     atomic.Bool
}

// countingShimClient counts physical release requests while delegating every
// control exchange to the same in-memory wrapper.
type countingShimClient struct {
	delegate ShimClient
	releases atomic.Int32
}

// Do records only ActionRelease calls so concurrent provider retries can prove
// the durable gate intent suppresses duplicate physical release requests.
func (client *countingShimClient) Do(ctx context.Context, path string, expectedPeerPID int, request shim.ControlRequest) (shim.ControlResponse, int, error) {
	if request.Action == shim.ActionRelease {
		client.releases.Add(1)
	}
	return client.delegate.Do(ctx, path, expectedPeerPID, request)
}

// lossyReleaseShimClient drops the first completed release response after the
// wrapper has already consumed its one-shot gate.
type lossyReleaseShimClient struct {
	delegate     ShimClient
	afterRelease func(string)
	lost         atomic.Bool
	releases     atomic.Int32
}

// Do injects the exact response-loss window between workload start and the
// provider's final released-state publication.
func (client *lossyReleaseShimClient) Do(ctx context.Context, path string, expectedPeerPID int, request shim.ControlRequest) (shim.ControlResponse, int, error) {
	response, peerPID, err := client.delegate.Do(ctx, path, expectedPeerPID, request)
	if request.Action == shim.ActionRelease {
		client.releases.Add(1)
		if err == nil && client.lost.CompareAndSwap(false, true) {
			if client.afterRelease != nil {
				client.afterRelease(path)
			}
			return shim.ControlResponse{}, 0, errors.New("injected release response loss after gate consumption")
		}
	}
	return response, peerPID, err
}

// preparedFakeAttempt contains the exact receipts needed to exercise one
// owner-scoped gate release without host namespace or cgroup side effects.
type preparedFakeAttempt struct {
	fixture    slimFixture
	owner      ownership.OwnerKey
	gate       ownership.Receipt
	process    ownership.Receipt
	cgroup     ownership.Receipt
	rootfs     ownership.Receipt
	attachment providerapi.AttachmentObservation
}

// newPreparedFakeAttempt builds a ready Sandbox and one prepared Attempt using
// the complete fake launcher/provider contract.
func newPreparedFakeAttempt(t *testing.T, suffix string) preparedFakeAttempt {
	t.Helper()
	launcher := &fakeLauncher{runtime: newFakeRuntime()}
	fixture := newSlimFixture(t, launcher)
	ctx := context.Background()
	sandboxID := domain.SandboxID("sandbox-" + suffix)
	sandboxOwner := testOwner(t, "sandbox-operation-"+suffix, operation.TargetSandbox, string(sandboxID))
	keeper, err := fixture.provider.EnsureKeeperProcess(ctx, providerapi.KeeperProcessRequest{
		Owner: sandboxOwner, SandboxID: sandboxID, Cgroup: fakeCgroupReceipt(t, sandboxOwner, ownership.KindKeeperCgroup, sandboxID, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	namespaces := make(map[isolation.NamespaceType]ownership.Receipt, 3)
	for _, namespace := range []isolation.NamespaceType{isolation.NamespaceUTS, isolation.NamespaceIPC, isolation.NamespaceNetwork} {
		request := providerapi.NamespaceRequest{Owner: sandboxOwner, Process: keeper, Namespace: namespace}
		if namespace == isolation.NamespaceUTS {
			request.Hostname = string(sandboxID)
		}
		if namespace == isolation.NamespaceNetwork {
			request.NetworkMode = providerapi.SandboxNetworkLoopback
		}
		receipt, err := fixture.provider.EnsureNamespace(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err = receipt.Adopt()
		if err != nil {
			t.Fatal(err)
		}
		namespaces[namespace] = receipt
	}
	containerID := domain.ContainerID("container-" + suffix)
	attemptID := domain.AttemptID("attempt-" + suffix)
	owner := testOwner(t, "container-operation-"+suffix, operation.TargetContainer, string(containerID))
	gate, err := fixture.provider.EnsureStartGate(ctx, providerapi.AttemptResourceRequest{Owner: owner, AttemptID: attemptID})
	if err != nil {
		t.Fatal(err)
	}
	streams, err := fixture.provider.EnsureStreams(ctx, providerapi.AttemptResourceRequest{Owner: owner, AttemptID: attemptID})
	if err != nil {
		t.Fatal(err)
	}
	cgroup := fakeCgroupReceipt(t, owner, ownership.KindAttemptCgroup, sandboxID, attemptID)
	process, err := fixture.provider.EnsureInitProcess(ctx, providerapi.InitProcessRequest{
		Owner: owner, SandboxID: sandboxID, AttemptID: attemptID, Cgroup: cgroup, Gate: gate, Streams: streams,
		SandboxNamespaces: providerapi.SandboxNamespaces{
			UTS: namespaces[isolation.NamespaceUTS], IPC: namespaces[isolation.NamespaceIPC], Network: namespaces[isolation.NamespaceNetwork],
		},
		Process: domain.ProcessSpec{Argv: []string{"/fake/workload"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	pid, err := fixture.provider.EnsureNamespace(ctx, providerapi.NamespaceRequest{Owner: owner, Process: process, Namespace: isolation.NamespacePID})
	if err != nil {
		t.Fatal(err)
	}
	mount, err := fixture.provider.EnsureNamespace(ctx, providerapi.NamespaceRequest{Owner: owner, Process: process, Namespace: isolation.NamespaceMount})
	if err != nil {
		t.Fatal(err)
	}
	rootfs, err := fixture.provider.EnsureRootfs(ctx, providerapi.RootfsRequest{
		Owner: owner, AttemptID: attemptID, Process: process, PID: pid, Mount: mount, SourceID: "prepared-one",
	})
	if err != nil {
		t.Fatal(err)
	}
	attachment, err := providerapi.NewAttachmentObservation(providerapi.AttachProcessRequest{Owner: owner, Cgroup: cgroup, Process: process})
	if err != nil {
		t.Fatal(err)
	}
	return preparedFakeAttempt{
		fixture: fixture, owner: owner, gate: gate, process: process, cgroup: cgroup, rootfs: rootfs, attachment: attachment,
	}
}

// Do forwards the side effect exactly once, then injects one post-commit response loss before allowing replay.
func (client *lossyShimClient) Do(ctx context.Context, path string, expectedPeerPID int, request shim.ControlRequest) (shim.ControlResponse, int, error) {
	response, peerPID, err := client.delegate.Do(ctx, path, expectedPeerPID, request)
	if err != nil {
		return shim.ControlResponse{}, 0, err
	}
	if request.Action == shim.ActionSignal && client.lost.CompareAndSwap(false, true) {
		return shim.ControlResponse{}, 0, errors.New("injected signal response loss after shim commit")
	}
	return response, peerPID, nil
}

// unavailableShimClient returns CodeUnavailable either as a transport failure
// or a valid correlated shim response without forwarding a signal side effect.
type unavailableShimClient struct {
	response bool
}

// Do injects the selected unavailable path while retaining the request ID when
// the failure is represented by a validated control response.
func (client unavailableShimClient) Do(_ context.Context, _ string, _ int, request shim.ControlRequest) (shim.ControlResponse, int, error) {
	unavailable := &shim.Error{Code: shim.CodeUnavailable, Message: "injected shim outage"}
	if !client.response {
		return shim.ControlResponse{}, 0, unavailable
	}
	return shim.ControlResponse{
		SchemaVersion: shim.SchemaVersion, RequestID: request.RequestID, Error: unavailable,
	}, 4242, nil
}

// fakeRequestIDs returns predictable unique request IDs for non-signal test actions.
type fakeRequestIDs struct {
	next atomic.Uint64
}

// Next returns one bounded action-prefixed request identity.
func (ids *fakeRequestIDs) Next(action shim.ControlAction) string {
	return string(action) + "-request-" + fmt.Sprint(ids.next.Add(1))
}

// fakeSlimRunner returns one injected child and counts no OS side effects.
type fakeSlimRunner struct {
	child *fakeSlimChild
}

// Start returns the fake child without fork or exec.
func (runner *fakeSlimRunner) Start(domain.ProcessSpec, io.Writer, io.Writer) (shim.Child, error) {
	return runner.child, nil
}

// fakeSlimChild blocks wait on a test channel and counts verified signal calls.
type fakeSlimChild struct {
	identity shim.ChildIdentity
	exit     chan shim.ChildExitEvidence
	signals  atomic.Int32
}

// newFakeSlimChild constructs owner-distinct valid child evidence.
func newFakeSlimChild(owner ownership.OwnerKey) *fakeSlimChild {
	return &fakeSlimChild{
		identity: shim.ChildIdentity{Handle: "fake-child", EvidenceSHA256: fakeEvidence(owner, ownership.KindInitProcess, "child")},
		exit:     make(chan shim.ChildExitEvidence, 1),
	}
}

// Identity returns immutable fake child evidence.
func (child *fakeSlimChild) Identity() shim.ChildIdentity {
	return child.identity
}

// Wait blocks until the test publishes one exit result.
func (child *fakeSlimChild) Wait() (shim.ChildExitEvidence, error) {
	return <-child.exit, nil
}

// SignalVerified counts action-time delivery and returns child-bound evidence.
func (child *fakeSlimChild) SignalVerified(signal shim.Signal) (shim.SignalDelivery, error) {
	child.signals.Add(1)
	return shim.SignalDelivery{
		Identity: child.identity, Signal: signal, Delivered: true,
		EvidenceSHA256: mustDigest(struct {
			Identity shim.ChildIdentity
			Signal   shim.Signal
		}{child.identity, signal}),
	}, nil
}

// fakeSlimTerminal retains one immutable terminal record for fake wrapper tests.
type fakeSlimTerminal struct {
	mu     sync.Mutex
	record *shim.TerminalRecord
}

// Load returns an independent fake terminal record.
func (store *fakeSlimTerminal) Load() (shim.TerminalRecord, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.record == nil {
		return shim.TerminalRecord{}, false, nil
	}
	return store.record.Clone(), true, nil
}

// Commit stores the first terminal record and rejects replacement.
func (store *fakeSlimTerminal) Commit(record shim.TerminalRecord) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.record != nil {
		return shim.ErrTerminalExists
	}
	clone := record.Clone()
	store.record = &clone
	return nil
}

// fakeSlimClock supplies stable persistence timestamps.
type fakeSlimClock struct{}

// Now returns one JSON-stable fake wall time.
func (fakeSlimClock) Now() time.Time {
	return time.Date(2026, 8, 21, 14, 0, 0, 0, time.UTC)
}

// TestIsolationProviderFullFakeContract verifies all resource, shim, signal-replay, supervisor, and cleanup boundaries.
func TestIsolationProviderFullFakeContract(t *testing.T) {
	fixture := newSlimFixture(t, &fakeLauncher{runtime: newFakeRuntime(), dropNamespaceResponse: true, dropRootfsResponse: true})
	ctx := context.Background()
	requirements := providerapi.M2Requirements().Isolation
	if _, err := fixture.provider.InspectIsolationCapabilities(ctx, requirements); err != nil {
		t.Fatal(err)
	}
	sandboxOwner := testOwner(t, "sandbox-operation", operation.TargetSandbox, "sandbox-one")
	keeper, err := fixture.provider.EnsureKeeperProcess(ctx, providerapi.KeeperProcessRequest{
		Owner: sandboxOwner, SandboxID: "sandbox-one",
		Cgroup: fakeCgroupReceipt(t, sandboxOwner, ownership.KindKeeperCgroup, "sandbox-one", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	namespaces := make(map[isolation.NamespaceType]ownership.Receipt)
	for _, namespace := range []isolation.NamespaceType{isolation.NamespaceUTS, isolation.NamespaceIPC, isolation.NamespaceNetwork} {
		request := providerapi.NamespaceRequest{Owner: sandboxOwner, Process: keeper, Namespace: namespace}
		if namespace == isolation.NamespaceUTS {
			request.Hostname = "sandbox-one-host"
		}
		if namespace == isolation.NamespaceNetwork {
			request.NetworkMode = providerapi.SandboxNetworkLoopback
		}
		receipt, err := fixture.provider.EnsureNamespace(ctx, request)
		if namespace == isolation.NamespaceUTS {
			if err == nil {
				t.Fatal("UTS response-loss injection did not surface after fake acquisition")
			}
			receipt, err = fixture.provider.EnsureNamespace(ctx, request)
		}
		if err != nil {
			t.Fatal(err)
		}
		replayed, err := fixture.provider.EnsureNamespace(ctx, request)
		if err != nil || replayed.EvidenceSHA256 != receipt.EvidenceSHA256 {
			t.Fatalf("namespace %s replay receipt=%+v error=%v", namespace, replayed, err)
		}
		receipt, err = receipt.Adopt()
		if err != nil {
			t.Fatal(err)
		}
		namespaces[namespace] = receipt
	}
	fixture.launcher.namespaceMu.Lock()
	utsLaunch := fixture.launcher.lastNamespaces[isolation.NamespaceUTS]
	networkLaunch := fixture.launcher.lastNamespaces[isolation.NamespaceNetwork]
	fixture.launcher.namespaceMu.Unlock()
	if utsLaunch.Hostname != "sandbox-one-host" || !validDigest(utsLaunch.ConfigurationSHA256) {
		t.Fatalf("UTS launch omitted hostname configuration: %+v", utsLaunch)
	}
	if networkLaunch.NetworkMode != providerapi.SandboxNetworkLoopback || !validDigest(networkLaunch.ConfigurationSHA256) {
		t.Fatalf("network launch omitted loopback configuration: %+v", networkLaunch)
	}
	if _, err := fixture.provider.EnsureNamespace(ctx, providerapi.NamespaceRequest{
		Owner: sandboxOwner, Process: keeper, Namespace: isolation.NamespaceNetwork, NetworkMode: providerapi.SandboxNetworkNone,
	}); err == nil {
		t.Fatal("network namespace silently reused a loopback resource for mode none")
	}
	tamperedNetwork := namespaces[isolation.NamespaceNetwork].Clone()
	tamperedNetwork.Attributes[networkModeAttribute] = string(providerapi.SandboxNetworkNone)
	tamperedNetwork.Attributes[configurationEvidenceAttribute], err = namespaceConfigurationDigest(
		isolation.NamespaceNetwork, "", providerapi.SandboxNetworkNone,
	)
	if err != nil {
		t.Fatal(err)
	}
	tamperedNetwork.EvidenceSHA256, err = slimReceiptDigest(
		tamperedNetwork.Owner, tamperedNetwork.Kind, tamperedNetwork.LocalID, tamperedNetwork.Attributes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.provider.InspectNamespace(ctx, providerapi.OwnedReceiptRequest{
		Owner: sandboxOwner, Receipt: tamperedNetwork,
	}); err == nil {
		t.Fatal("network inspection accepted receipt configuration that differs from live fake evidence")
	}
	containerOwner := testOwner(t, "container-operation", operation.TargetContainer, "container-one")
	gate, err := fixture.provider.EnsureStartGate(ctx, providerapi.AttemptResourceRequest{Owner: containerOwner, AttemptID: "attempt-one"})
	if err != nil {
		t.Fatal(err)
	}
	streams, err := fixture.provider.EnsureStreams(ctx, providerapi.AttemptResourceRequest{Owner: containerOwner, AttemptID: "attempt-one"})
	if err != nil {
		t.Fatal(err)
	}
	attemptCgroup := fakeCgroupReceipt(t, containerOwner, ownership.KindAttemptCgroup, "sandbox-one", "attempt-one")
	initReceipt, err := fixture.provider.EnsureInitProcess(ctx, providerapi.InitProcessRequest{
		Owner: containerOwner, SandboxID: "sandbox-one", AttemptID: "attempt-one",
		Cgroup: attemptCgroup, Gate: gate, Streams: streams,
		SandboxNamespaces: providerapi.SandboxNamespaces{
			UTS: namespaces[isolation.NamespaceUTS], IPC: namespaces[isolation.NamespaceIPC], Network: namespaces[isolation.NamespaceNetwork],
		},
		Process: domain.ProcessSpec{Argv: []string{"/fake/workload", "argument with space"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	pidReceipt, err := fixture.provider.EnsureNamespace(ctx, providerapi.NamespaceRequest{Owner: containerOwner, Process: initReceipt, Namespace: isolation.NamespacePID})
	if err != nil {
		t.Fatal(err)
	}
	mountReceipt, err := fixture.provider.EnsureNamespace(ctx, providerapi.NamespaceRequest{Owner: containerOwner, Process: initReceipt, Namespace: isolation.NamespaceMount})
	if err != nil {
		t.Fatal(err)
	}
	rootfsRequest := providerapi.RootfsRequest{
		Owner: containerOwner, AttemptID: "attempt-one", Process: initReceipt,
		PID: pidReceipt, Mount: mountReceipt, SourceID: "prepared-one", DNS: []string{"1.1.1.1", "8.8.8.8"},
	}
	rootfs, err := fixture.provider.EnsureRootfs(ctx, rootfsRequest)
	if err == nil || !providerapi.IsRollbackRequired(err) || providerapi.IsNoEffect(err) {
		t.Fatalf("rootfs response-loss disposition = %v, want rollback-required after fake preparation", err)
	}
	rootfs, err = fixture.provider.EnsureRootfs(ctx, rootfsRequest)
	if err != nil {
		t.Fatal(err)
	}
	fixture.launcher.rootfsMu.Lock()
	rootfsLaunch := fixture.launcher.lastRootfs
	fixture.launcher.rootfsMu.Unlock()
	if rootfsLaunch.SourceID != "prepared-one" || rootfsLaunch.Source.Rootfs != "/trusted/prepared/rootfs" ||
		rootfsLaunch.PID.Kind != ownership.KindPIDNamespace || rootfsLaunch.Mount.Kind != ownership.KindMountNamespace ||
		!reflect.DeepEqual(rootfsLaunch.DNS, []string{"1.1.1.1", "8.8.8.8"}) || !validDigest(rootfsLaunch.ConfigurationSHA256) {
		t.Fatalf("catalog-resolved rootfs launch=%+v", rootfsLaunch)
	}
	replayedRootfs, err := fixture.provider.EnsureRootfs(ctx, rootfsRequest)
	if err != nil || replayedRootfs.EvidenceSHA256 != rootfs.EvidenceSHA256 {
		t.Fatalf("rootfs retry receipt=%+v error=%v", replayedRootfs, err)
	}
	if _, err := fixture.provider.EnsureRootfs(ctx, providerapi.RootfsRequest{
		Owner: containerOwner, AttemptID: "attempt-one", Process: initReceipt,
		PID: pidReceipt, Mount: mountReceipt, SourceID: "prepared-one", DNS: []string{"9.9.9.9"},
	}); err == nil {
		t.Fatal("rootfs silently reused preparation evidence for different retained DNS")
	}
	tamperedRootfs := rootfs.Clone()
	tamperedRootfs.Attributes[configurationEvidenceAttribute], err = dnsConfigurationDigest([]string{"9.9.9.9"})
	if err != nil {
		t.Fatal(err)
	}
	tamperedRootfs.EvidenceSHA256, err = slimReceiptDigest(
		tamperedRootfs.Owner, tamperedRootfs.Kind, tamperedRootfs.LocalID, tamperedRootfs.Attributes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.provider.InspectRootfs(ctx, providerapi.OwnedReceiptRequest{
		Owner: containerOwner, Receipt: tamperedRootfs,
	}); err == nil {
		t.Fatal("rootfs inspection accepted DNS configuration that differs from live fake evidence")
	}
	prepared, err := fixture.provider.ObserveAttempt(ctx, providerapi.OwnedReceiptRequest{Owner: containerOwner, Receipt: initReceipt})
	if err != nil || !prepared.Prepared {
		t.Fatalf("prepared=%+v error=%v", prepared, err)
	}
	attachment, err := providerapi.NewAttachmentObservation(providerapi.AttachProcessRequest{Owner: containerOwner, Cgroup: attemptCgroup, Process: initReceipt})
	if err != nil {
		t.Fatal(err)
	}
	releaseRequest := providerapi.ReleaseGateRequest{
		Owner: containerOwner, Gate: gate, Process: initReceipt, Cgroup: attemptCgroup, Rootfs: rootfs, Attachment: attachment,
	}
	lossyRelease := &lossyReleaseShimClient{delegate: fixture.provider.shim}
	fixture.provider.shim = lossyRelease
	if _, err := fixture.provider.ReleaseStartGate(ctx, releaseRequest); err == nil {
		t.Fatal("post-consumption release response loss was not surfaced")
	}
	gateRecord, found, err := fixture.provider.artifacts.Read(containerOwner, ownership.KindStartGate)
	if err != nil || !found || gateRecord.State != artifactStateConsuming {
		t.Fatalf("gate after response loss = (%+v, %t, %v), want consuming", gateRecord, found, err)
	}
	countingRelease := &countingShimClient{delegate: fakeShimClient{runtime: fixture.runtime}}
	fixture.provider.shim = countingRelease
	const concurrentReleases = 16
	releaseErrors := make(chan error, concurrentReleases)
	var releaseWait sync.WaitGroup
	releaseWait.Add(concurrentReleases)
	for index := 0; index < concurrentReleases; index++ {
		go func() {
			defer releaseWait.Done()
			_, releaseErr := fixture.provider.ReleaseStartGate(ctx, releaseRequest)
			releaseErrors <- releaseErr
		}()
	}
	releaseWait.Wait()
	close(releaseErrors)
	for releaseErr := range releaseErrors {
		if releaseErr != nil {
			t.Fatal(releaseErr)
		}
	}
	if initial, retries := lossyRelease.releases.Load(), countingRelease.releases.Load(); initial != 1 || retries != 0 {
		t.Fatalf("physical shim release calls = initial %d retries %d, want 1 and 0", initial, retries)
	}
	running, err := fixture.provider.ObserveAttempt(ctx, providerapi.OwnedReceiptRequest{Owner: containerOwner, Receipt: initReceipt})
	if err != nil || !running.Running {
		t.Fatalf("running=%+v error=%v", running, err)
	}
	signalRequest := providerapi.SignalRequest{
		Owner: containerOwner, Process: initReceipt, ActionOperationID: "kill-operation",
		Step: providerapi.SignalStepInitial, Signal: providerapi.SignalTERM,
	}
	for _, response := range []bool{false, true} {
		fixture.provider.shim = unavailableShimClient{response: response}
		_, unavailableErr := fixture.provider.SignalVerified(ctx, signalRequest)
		if !providerapi.IsObservationUnavailable(unavailableErr) || !shim.IsCode(unavailableErr, shim.CodeUnavailable) {
			t.Fatalf("signal unavailable response=%t error=%v, want retryable typed observation outage", response, unavailableErr)
		}
	}
	fixture.provider.shim = &lossyShimClient{delegate: fakeShimClient{runtime: fixture.runtime}}
	if _, err := fixture.provider.SignalVerified(ctx, signalRequest); err == nil {
		t.Fatal("post-commit signal response loss was not surfaced")
	}
	child := fixture.child(containerOwner)
	if child.signals.Load() != 1 {
		t.Fatalf("verified signal calls after response loss=%d, want 1", child.signals.Load())
	}
	var replayedAt time.Time
	for retry := 0; retry < 2; retry++ {
		observation, err := fixture.provider.SignalVerified(ctx, signalRequest)
		if err != nil {
			t.Fatalf("signal retry %d: %v", retry, err)
		}
		if retry == 0 {
			replayedAt = observation.DeliveredAt
		} else if !observation.DeliveredAt.Equal(replayedAt) {
			t.Fatalf("signal retry delivery time = %s, want stable %s", observation.DeliveredAt, replayedAt)
		}
	}
	if child.signals.Load() != 1 {
		t.Fatalf("verified signal calls=%d, want 1", child.signals.Load())
	}
	tampered := signalRequest
	tampered.Signal = providerapi.SignalKILL
	if _, err := fixture.provider.SignalVerified(ctx, tampered); err == nil || child.signals.Load() != 1 {
		t.Fatalf("tampered signal error=%v calls=%d", err, child.signals.Load())
	}
	child.exit <- fakeSuccessfulExit(child.identity)
	terminal := waitForTerminalObservation(t, fixture.provider, containerOwner, initReceipt)
	if terminal.StartFailed || terminal.Outcome.ExitCode == nil || *terminal.Outcome.ExitCode != 0 {
		t.Fatalf("terminal outcome=%+v", terminal.Outcome)
	}
	processReference, err := fixture.provider.ResolveProcess(ctx, initReceipt)
	if err != nil {
		t.Fatal(err)
	}
	if pid, err := processReference.VerifiedPID(ctx); err != nil || pid != 4242 {
		t.Fatalf("verified fake PID=%d error=%v", pid, err)
	}
	for _, cleanup := range []func() (providerapi.CleanupObservation, error){
		func() (providerapi.CleanupObservation, error) {
			return fixture.provider.RemoveRootfs(ctx, providerapi.OwnedReceiptRequest{Owner: containerOwner, Receipt: rootfs})
		},
		func() (providerapi.CleanupObservation, error) {
			return fixture.provider.RemoveNamespace(ctx, providerapi.OwnedReceiptRequest{Owner: containerOwner, Receipt: mountReceipt})
		},
		func() (providerapi.CleanupObservation, error) {
			return fixture.provider.RemoveNamespace(ctx, providerapi.OwnedReceiptRequest{Owner: containerOwner, Receipt: pidReceipt})
		},
		func() (providerapi.CleanupObservation, error) {
			return fixture.provider.RemoveProcess(ctx, providerapi.OwnedReceiptRequest{Owner: containerOwner, Receipt: initReceipt})
		},
		func() (providerapi.CleanupObservation, error) {
			return fixture.provider.RemoveStreams(ctx, providerapi.OwnedReceiptRequest{Owner: containerOwner, Receipt: streams})
		},
		func() (providerapi.CleanupObservation, error) {
			return fixture.provider.RemoveStartGate(ctx, providerapi.OwnedReceiptRequest{Owner: containerOwner, Receipt: gate})
		},
	} {
		result, err := cleanup()
		if err != nil || result.After.Presence != providerapi.PresenceAbsent {
			t.Fatalf("cleanup=%+v error=%v", result, err)
		}
	}
}

// TestObserveAttemptProjectsLaunchAbortedAsStartFailure verifies an executed
// but unpublishable workload retains its unknown outcome while explicitly
// failing the active Start operation rather than masquerading as success.
func TestObserveAttemptProjectsLaunchAbortedAsStartFailure(t *testing.T) {
	attempt := newPreparedFakeAttempt(t, "launch-aborted")
	spec := shim.InitSpec{
		Owner: attempt.owner, SandboxID: domain.SandboxID(attempt.process.Attributes[sandboxIDAttribute]),
		ContainerID: domain.ContainerID(attempt.owner.Target.ID), AttemptID: domain.AttemptID(attempt.process.Attributes[attemptIDAttribute]),
		WrapperEvidence: attempt.process.Attributes[wrapperEvidenceAttribute], Process: domain.ProcessSpec{Argv: []string{"/fake/workload"}},
	}
	record, err := shim.NewTerminalRecord(
		spec, shim.TerminalLaunchAborted, domain.UnknownOutcome(domain.EvidenceUnknown), nil,
		"strong child handle publication failed after exec", fakeSlimClock{}.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	wrapper, err := shim.NewInit(spec, shim.InitDependencies{
		Runner: &fakeSlimRunner{child: newFakeSlimChild(attempt.owner)}, Stdout: io.Discard, Stderr: io.Discard,
		Terminal: &fakeSlimTerminal{record: &record}, Clock: fakeSlimClock{},
	})
	if err != nil {
		t.Fatal(err)
	}
	paths, err := deriveArtifactPaths(attempt.fixture.root, attempt.owner)
	if err != nil {
		t.Fatal(err)
	}
	attempt.fixture.runtime.mu.Lock()
	attempt.fixture.runtime.wrappers[paths.ControlSocket] = wrapper
	attempt.fixture.runtime.mu.Unlock()
	observation, err := attempt.fixture.provider.ObserveAttempt(context.Background(), providerapi.OwnedReceiptRequest{
		Owner: attempt.owner, Receipt: attempt.process,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !observation.Terminal || !observation.StartFailed || observation.Outcome.Presence != domain.OutcomeUnknown {
		t.Fatalf("launch-aborted projection = %+v", observation)
	}
}

// TestTerminalReasonFailsStartClassifiesOnlyPublicationFailures verifies wait
// evidence loss after a published child does not retroactively redefine a
// successful workload start as a start failure.
func TestTerminalReasonFailsStartClassifiesOnlyPublicationFailures(t *testing.T) {
	tests := []struct {
		reason shim.TerminalReason
		want   bool
	}{
		{reason: shim.TerminalStartFailed, want: true},
		{reason: shim.TerminalLaunchAborted, want: true},
		{reason: shim.TerminalChildExit, want: false},
		{reason: shim.TerminalWaitFailed, want: false},
	}
	for _, test := range tests {
		if got := terminalReasonFailsStart(test.reason); got != test.want {
			t.Fatalf("terminalReasonFailsStart(%q) = %t, want %t", test.reason, got, test.want)
		}
	}
}

// TestReleaseResponseLossWithAbsentWrapperStaysFailClosed verifies an exec'd
// workload cannot be launched a second time when its init wrapper disappears
// before the release response and final gate publication reach the daemon.
func TestReleaseResponseLossWithAbsentWrapperStaysFailClosed(t *testing.T) {
	attempt := newPreparedFakeAttempt(t, "release-crash")
	client := &lossyReleaseShimClient{
		delegate: fakeShimClient{runtime: attempt.fixture.runtime},
		afterRelease: func(path string) {
			attempt.fixture.runtime.mu.Lock()
			delete(attempt.fixture.runtime.wrappers, path)
			attempt.fixture.runtime.mu.Unlock()
		},
	}
	attempt.fixture.provider.shim = client
	request := providerapi.ReleaseGateRequest{
		Owner: attempt.owner, Gate: attempt.gate, Process: attempt.process,
		Cgroup: attempt.cgroup, Rootfs: attempt.rootfs, Attachment: attempt.attachment,
	}
	if _, err := attempt.fixture.provider.ReleaseStartGate(context.Background(), request); err == nil {
		t.Fatal("release response loss after wrapper disappearance was not surfaced")
	}
	if _, err := attempt.fixture.provider.ReleaseStartGate(context.Background(), request); err == nil {
		t.Fatal("retry unexpectedly released an absent wrapper")
	}
	if calls := client.releases.Load(); calls != 1 {
		t.Fatalf("physical shim release calls = %d, want one", calls)
	}
	record, found, err := attempt.fixture.provider.artifacts.Read(attempt.owner, ownership.KindStartGate)
	if err != nil || !found || record.State != artifactStateConsuming {
		t.Fatalf("gate after absent-wrapper retry = (%+v, %t, %v), want consuming", record, found, err)
	}
	if err := validateInitArtifact(
		attempt.fixture.provider.artifacts, attempt.owner, attempt.gate, ownership.KindStartGate,
		domain.AttemptID(attempt.gate.Attributes[attemptIDAttribute]), artifactStateClosed,
	); err == nil {
		t.Fatal("launcher prerequisite accepted a consuming gate for init relaunch")
	}
	recovered, err := attempt.fixture.provider.EnsureStartGate(context.Background(), providerapi.AttemptResourceRequest{
		Owner: attempt.owner, AttemptID: domain.AttemptID(attempt.gate.Attributes[attemptIDAttribute]),
	})
	if err != nil || recovered.EvidenceSHA256 != attempt.gate.EvidenceSHA256 {
		t.Fatalf("EnsureStartGate(consuming) = (%+v, %v)", recovered, err)
	}
	record, found, err = attempt.fixture.provider.artifacts.Read(attempt.owner, ownership.KindStartGate)
	if err != nil || !found || record.State != artifactStateConsuming {
		t.Fatalf("gate after Ensure replay = (%+v, %t, %v), want consuming", record, found, err)
	}
	if _, err := attempt.fixture.provider.RemoveProcess(context.Background(), providerapi.OwnedReceiptRequest{
		Owner: attempt.owner, Receipt: attempt.process,
	}); err != nil {
		t.Fatal(err)
	}
	removed, err := attempt.fixture.provider.RemoveStartGate(context.Background(), providerapi.OwnedReceiptRequest{
		Owner: attempt.owner, Receipt: attempt.gate,
	})
	if err != nil || removed.After.Presence != providerapi.PresenceAbsent {
		t.Fatalf("RemoveStartGate(consuming) = (%+v, %v)", removed, err)
	}
}

// TestReleaseWaitsForDurableConsumptionIntent verifies a directory-sync
// uncertainty after publishing consuming prevents the shim side effect until
// a later retry rediscovers the durable intent.
func TestReleaseWaitsForDurableConsumptionIntent(t *testing.T) {
	attempt := newPreparedFakeAttempt(t, "release-sync")
	counting := &countingShimClient{delegate: fakeShimClient{runtime: attempt.fixture.runtime}}
	attempt.fixture.provider.shim = counting
	paths, err := deriveArtifactPaths(attempt.fixture.root, attempt.owner)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected consuming directory sync uncertainty")
	failed := false
	attempt.fixture.provider.artifacts.syncDirectory = func(path string) error {
		if filepath.Clean(path) == filepath.Clean(paths.OwnerRoot) && !failed {
			failed = true
			return injected
		}
		return syncArtifactDirectory(path)
	}
	request := providerapi.ReleaseGateRequest{
		Owner: attempt.owner, Gate: attempt.gate, Process: attempt.process,
		Cgroup: attempt.cgroup, Rootfs: attempt.rootfs, Attachment: attempt.attachment,
	}
	if _, err := attempt.fixture.provider.ReleaseStartGate(context.Background(), request); !errors.Is(err, injected) {
		t.Fatalf("ReleaseStartGate(sync uncertainty) error = %v, want injected error", err)
	}
	if calls := counting.releases.Load(); calls != 0 {
		t.Fatalf("physical shim release calls after uncertain intent = %d, want zero", calls)
	}
	attempt.fixture.provider.artifacts.syncDirectory = func(path string) error {
		if filepath.Clean(path) == filepath.Clean(paths.OwnerRoot) {
			return injected
		}
		return syncArtifactDirectory(path)
	}
	if _, err := attempt.fixture.provider.ReleaseStartGate(context.Background(), request); !errors.Is(err, injected) {
		t.Fatalf("ReleaseStartGate(confirmation uncertainty) error = %v, want injected error", err)
	}
	if calls := counting.releases.Load(); calls != 0 {
		t.Fatalf("physical shim release calls before confirmed intent = %d, want zero", calls)
	}
	attempt.fixture.provider.artifacts.syncDirectory = syncArtifactDirectory
	if _, err := attempt.fixture.provider.ReleaseStartGate(context.Background(), request); err != nil {
		t.Fatalf("ReleaseStartGate(retry) error = %v", err)
	}
	if calls := counting.releases.Load(); calls != 1 {
		t.Fatalf("physical shim release calls after durable retry = %d, want one", calls)
	}
}

// TestOpaqueSourceAndReceiptPathsFailClosed verifies API and receipt text never becomes launcher artifact authority.
func TestOpaqueSourceAndReceiptPathsFailClosed(t *testing.T) {
	launcher := &fakeLauncher{runtime: newFakeRuntime()}
	fixture := newSlimFixture(t, launcher)
	owner := testOwner(t, "path-operation", operation.TargetContainer, "container-path")
	gate, err := fixture.provider.EnsureStartGate(context.Background(), providerapi.AttemptResourceRequest{Owner: owner, AttemptID: "attempt-path"})
	if err != nil {
		t.Fatal(err)
	}
	tampered := gate.Clone()
	tampered.Attributes["path"] = "/tmp/attacker"
	if _, err := fixture.provider.InspectStartGate(context.Background(), providerapi.OwnedReceiptRequest{Owner: owner, Receipt: tampered}); err == nil {
		t.Fatal("path-bearing receipt attribute was accepted")
	}
	if _, err := fixture.sources.Resolve(context.Background(), providerapi.OpaqueID("/tmp/rootfs")); err == nil {
		t.Fatal("path-shaped source ID was accepted")
	}
	paths, err := deriveArtifactPaths(fixture.root, owner)
	if err != nil {
		t.Fatal(err)
	}
	wantOwnerRoot := filepath.Join(fixture.root, "owners", owner.Token)
	if paths.OwnerRoot != wantOwnerRoot || strings.Contains(paths.OwnerRoot, string(gate.LocalID)) {
		t.Fatalf("derived paths=%+v want owner root %s", paths, wantOwnerRoot)
	}
}

// TestLinuxShimLauncherConstructorFailsBeforeMutation verifies incomplete
// production dependency injection is rejected without touching runtime state.
func TestLinuxShimLauncherConstructorFailsBeforeMutation(t *testing.T) {
	runtimeRoot := privateSlimRoot(t)
	if _, err := NewLinuxShimLauncher(runtimeRoot, "/usr/libexec/mydocker-shim", nil); err == nil {
		t.Fatal("production launcher accepted a nil cgroup manager")
	}
	if _, err := os.Lstat(filepath.Join(runtimeRoot, "owners")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fail-closed preflight mutated runtime artifacts: %v", err)
	}
}

// slimFixture groups fake provider dependencies and trusted prepared-rootfs configuration.
type slimFixture struct {
	provider *IsolationProvider
	launcher *fakeLauncher
	runtime  *fakeRuntime
	sources  *StaticSourceCatalog
	root     string
}

// newSlimFixture creates one private non-privileged fake provider environment.
func newSlimFixture(t *testing.T, launcher *fakeLauncher) slimFixture {
	t.Helper()
	root := privateSlimRoot(t)
	sources, err := NewStaticSourceCatalog(map[providerapi.OpaqueID]isolation.RootfsConfig{
		"prepared-one": {AllowedRoot: "/trusted/prepared", Rootfs: "/trusted/prepared/rootfs"},
	})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := New(Config{
		RuntimeRoot: root, Launcher: launcher, Sources: sources,
		Shim: fakeShimClient{runtime: launcher.runtime}, RequestIDs: &fakeRequestIDs{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return slimFixture{provider: provider, launcher: launcher, runtime: launcher.runtime, sources: sources, root: root}
}

// child returns the fake child registered for one init owner.
func (fixture slimFixture) child(owner ownership.OwnerKey) *fakeSlimChild {
	fixture.runtime.mu.Lock()
	defer fixture.runtime.mu.Unlock()
	return fixture.runtime.children[owner.Token]
}

// testOwner constructs one deterministic valid owner binding for a fake lifecycle intent.
func testOwner(t *testing.T, operationID string, kind operation.TargetKind, id string) ownership.OwnerKey {
	t.Helper()
	owner, err := ownership.NewOwnerKey(operation.OperationID(operationID), operation.Target{Kind: kind, ID: id}, domain.InitialGeneration)
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

// fakeCgroupReceipt constructs the minimum valid external cgroup dependency used by isolation contracts.
func fakeCgroupReceipt(t *testing.T, owner ownership.OwnerKey, kind ownership.Kind, sandboxID domain.SandboxID, attemptID domain.AttemptID) ownership.Receipt {
	t.Helper()
	var effective *cgroupv2.EffectiveLimits
	if kind == ownership.KindAttemptCgroup {
		effective = &cgroupv2.EffectiveLimits{}
	}
	receipt, err := newCgroupReceipt(owner, kind, sandboxID, attemptID, effective)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

// fakeEvidence creates deterministic canonical evidence for one owner/kind/fact tuple.
func fakeEvidence(owner ownership.OwnerKey, kind ownership.Kind, fact string) string {
	return mustDigest(struct {
		Owner ownership.OwnerKey
		Kind  ownership.Kind
		Fact  string
	}{owner, kind, fact})
}

// mustDigest returns canonical evidence for JSON-safe fake test values.
func mustDigest(value any) string {
	digest, err := ownership.EvidenceDigest(value)
	if err != nil {
		panic(err)
	}
	return digest
}

// fakeResourceKey scopes fake launcher objects by complete owner token and resource kind.
func fakeResourceKey(owner ownership.OwnerKey, kind ownership.Kind) string {
	return owner.Token + "\x00" + string(kind)
}

// namespaceKind maps one isolation namespace to its exact ownership receipt kind.
func namespaceKind(namespace isolation.NamespaceType) (ownership.Kind, error) {
	switch namespace {
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
		return "", errors.New("unsupported fake namespace")
	}
}

// fakeSuccessfulExit returns captured non-OOM child completion for supervisor mapping.
func fakeSuccessfulExit(identity shim.ChildIdentity) shim.ChildExitEvidence {
	code := int32(0)
	startedAt := time.Date(2026, 8, 21, 14, 0, 0, 0, time.UTC)
	return shim.ChildExitEvidence{
		Identity: identity, ExitCode: &code, OOM: domain.EvidenceFalse,
		StartedAt: startedAt, FinishedAt: startedAt.Add(time.Second), RunningDuration: time.Second,
	}
}

// waitForTerminalObservation waits only for the asynchronous fake reaper contract, not a performance target.
func waitForTerminalObservation(t *testing.T, provider *IsolationProvider, owner ownership.OwnerKey, receipt ownership.Receipt) engine.AttemptObservation {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		observation, err := provider.ObserveAttempt(context.Background(), providerapi.OwnedReceiptRequest{Owner: owner, Receipt: receipt})
		if err == nil && observation.Terminal {
			return observation
		}
		if time.Now().After(deadline) {
			t.Fatalf("Attempt never became terminal; observation=%+v error=%v", observation, err)
		}
		time.Sleep(time.Millisecond)
	}
}

// privateSlimRoot creates an explicitly mode-0700 runtime root for artifact tests.
func privateSlimRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

var (
	_ providerapi.IsolationProvider = (*IsolationProvider)(nil)
	_ engine.Supervisor             = (*IsolationProvider)(nil)
	_ ProcessResolver               = (*IsolationProvider)(nil)
)
