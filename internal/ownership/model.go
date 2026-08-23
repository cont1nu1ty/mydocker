package ownership

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/rollback"
)

// SchemaVersion is the only ownership receipt schema understood by M2.
const SchemaVersion uint32 = 1

// Provider identifies the bounded implementation that can rediscover and verify a receipt.
type Provider string

const (
	// ProviderLinux delegates namespace, process, gate, mount, and stream evidence to the Linux isolation provider.
	ProviderLinux Provider = "linux"
	// ProviderCgroupV2 delegates cgroup evidence to the cgroup v2 provider.
	ProviderCgroupV2 Provider = "cgroupv2"
)

// Valid reports whether the provider belongs to the M2 recovery registry.
func (p Provider) Valid() bool {
	return p == ProviderLinux || p == ProviderCgroupV2
}

// Kind identifies one host resource without embedding a host path or numeric PID as authority.
type Kind string

const (
	// KindSandboxCgroup identifies the Sandbox parent cgroup.
	KindSandboxCgroup Kind = "sandbox_cgroup"
	// KindAttemptCgroup identifies one Attempt child cgroup.
	KindAttemptCgroup Kind = "attempt_cgroup"
	// KindKeeperCgroup identifies the process-bearing keeper leaf below a process-free Sandbox parent.
	KindKeeperCgroup Kind = "keeper_cgroup"
	// KindKeeperProcess identifies the process that keeps stable Sandbox namespaces alive.
	KindKeeperProcess Kind = "keeper_process"
	// KindUTSNamespace identifies a Sandbox-owned UTS namespace.
	KindUTSNamespace Kind = "uts_namespace"
	// KindIPCNamespace identifies a Sandbox-owned IPC namespace.
	KindIPCNamespace Kind = "ipc_namespace"
	// KindNetworkNamespace identifies a Sandbox-owned network namespace.
	KindNetworkNamespace Kind = "network_namespace"
	// KindInitProcess identifies one gated Attempt init process.
	KindInitProcess Kind = "init_process"
	// KindPIDNamespace identifies an Attempt-owned PID namespace.
	KindPIDNamespace Kind = "pid_namespace"
	// KindMountNamespace identifies an Attempt-owned mount namespace.
	KindMountNamespace Kind = "mount_namespace"
	// KindRootfsMount identifies the rootfs mount visible only inside an Attempt mount namespace.
	KindRootfsMount Kind = "rootfs_mount"
	// KindStartGate identifies the one-shot start gate owned by an Attempt.
	KindStartGate Kind = "start_gate"
	// KindStreams identifies persisted stdout and stderr stream endpoints for an Attempt.
	KindStreams Kind = "streams"
)

// Valid reports whether kind belongs to the bounded M2 host inventory.
func (k Kind) Valid() bool {
	switch k {
	case KindSandboxCgroup, KindAttemptCgroup, KindKeeperCgroup, KindKeeperProcess,
		KindUTSNamespace, KindIPCNamespace, KindNetworkNamespace,
		KindInitProcess, KindPIDNamespace, KindMountNamespace,
		KindRootfsMount, KindStartGate, KindStreams:
		return true
	default:
		return false
	}
}

// Action identifies a bounded idempotent inverse that a provider registry may reconstruct.
type Action string

const (
	// ActionRemoveCgroup removes one verified, empty owned cgroup.
	ActionRemoveCgroup Action = "remove_cgroup"
	// ActionStopProcess terminates and reaps one action-time verified owned process.
	ActionStopProcess Action = "stop_process"
	// ActionUnmountRoot removes one verified Attempt mount namespace/rootfs view.
	ActionUnmountRoot Action = "unmount_root"
	// ActionCloseGate closes one unconsumed start gate without releasing the workload.
	ActionCloseGate Action = "close_gate"
	// ActionCloseStreams closes and publishes one Attempt stream set.
	ActionCloseStreams Action = "close_streams"
)

// Valid reports whether action belongs to the bounded M2 inverse registry.
func (a Action) Valid() bool {
	switch a {
	case ActionRemoveCgroup, ActionStopProcess, ActionUnmountRoot, ActionCloseGate, ActionCloseStreams:
		return true
	default:
		return false
	}
}

// ValidFor reports whether a bounded inverse can act on the receipt kind through its owning provider.
func (a Action) ValidFor(provider Provider, kind Kind) bool {
	switch a {
	case ActionRemoveCgroup:
		return provider == ProviderCgroupV2 && (kind == KindSandboxCgroup || kind == KindAttemptCgroup || kind == KindKeeperCgroup)
	case ActionStopProcess:
		return provider == ProviderLinux && (kind == KindKeeperProcess || kind == KindInitProcess)
	case ActionUnmountRoot:
		return provider == ProviderLinux && kind == KindRootfsMount
	case ActionCloseGate:
		return provider == ProviderLinux && kind == KindStartGate
	case ActionCloseStreams:
		return provider == ProviderLinux && kind == KindStreams
	default:
		return false
	}
}

// OwnerKey binds deterministic provider naming to a previously persisted lifecycle intent.
type OwnerKey struct {
	OperationID operation.OperationID `json:"operation_id"`
	Target      operation.Target      `json:"target"`
	Generation  domain.Generation     `json:"generation"`
	Token       string                `json:"token"`
}

// NewOwnerKey derives a stable non-secret token for provider Ensure and Inspect calls.
func NewOwnerKey(operationID operation.OperationID, target operation.Target, generation domain.Generation) (OwnerKey, error) {
	key := OwnerKey{OperationID: operationID, Target: target, Generation: generation}
	if err := operationID.Validate(); err != nil {
		return OwnerKey{}, err
	}
	if err := target.Validate(); err != nil {
		return OwnerKey{}, err
	}
	if generation == 0 {
		return OwnerKey{}, errors.New("owner generation must be greater than zero")
	}
	encoded, err := json.Marshal(struct {
		OperationID operation.OperationID `json:"operation_id"`
		Target      operation.Target      `json:"target"`
		Generation  domain.Generation     `json:"generation"`
	}{operationID, target, generation})
	if err != nil {
		return OwnerKey{}, fmt.Errorf("encode owner key: %w", err)
	}
	digest := sha256.Sum256(encoded)
	key.Token = hex.EncodeToString(digest[:])
	return key, nil
}

// Validate rejects an incomplete owner binding or a token that does not match its immutable fields.
func (k OwnerKey) Validate() error {
	expected, err := NewOwnerKey(k.OperationID, k.Target, k.Generation)
	if err != nil {
		return err
	}
	if k.Token != expected.Token {
		return errors.New("owner token does not match operation, target, and generation")
	}
	return nil
}

// Receipt records deterministic discovery input and evidence for one acquired host resource.
// Attributes may contain diagnostics such as a PID, start time, namespace inode, or relative cgroup name,
// but no attribute authorizes an action until the provider verifies the live object again.
type Receipt struct {
	SchemaVersion  uint32            `json:"schema_version"`
	Provider       Provider          `json:"provider"`
	Kind           Kind              `json:"kind"`
	LocalID        string            `json:"local_id"`
	Owner          OwnerKey          `json:"owner"`
	EvidenceSHA256 string            `json:"evidence_sha256"`
	Attributes     map[string]string `json:"attributes,omitempty"`
	Adopted        bool              `json:"adopted"`
}

// Release records provider-verified absence of one previously adopted host resource.
// CleanupOperationID identifies the delete operation that obtained the evidence;
// Resource remains the immutable acquisition receipt so recovery never authorizes
// cleanup from a caller-provided path or PID.
type Release struct {
	SchemaVersion      uint32                `json:"schema_version"`
	CleanupOperationID operation.OperationID `json:"cleanup_operation_id"`
	Resource           Receipt               `json:"resource"`
	EvidenceSHA256     string                `json:"evidence_sha256"`
}

// Clone returns a release whose nested resource attributes cannot alias persistent state.
func (r Release) Clone() Release {
	clone := r
	clone.Resource = r.Resource.Clone()
	return clone
}

// Validate checks that an adopted resource and its absence evidence are bound to a delete operation.
func (r Release) Validate() error {
	if r.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported ownership release schema version %d", r.SchemaVersion)
	}
	if err := r.CleanupOperationID.Validate(); err != nil {
		return fmt.Errorf("cleanup operation ID: %w", err)
	}
	if err := r.Resource.Validate(); err != nil {
		return fmt.Errorf("released resource: %w", err)
	}
	if !r.Resource.Adopted {
		return errors.New("released resource must be an adopted inventory receipt")
	}
	if !validDigest(r.EvidenceSHA256) {
		return errors.New("release evidence must be a lowercase SHA-256 digest")
	}
	return nil
}

// NewRelease binds provider absence evidence to one delete operation and adopted resource.
func NewRelease(cleanupOperationID operation.OperationID, resource Receipt, evidence any) (Release, error) {
	digest, err := EvidenceDigest(evidence)
	if err != nil {
		return Release{}, err
	}
	release := Release{
		SchemaVersion:      SchemaVersion,
		CleanupOperationID: cleanupOperationID,
		Resource:           resource.Clone(),
		EvidenceSHA256:     digest,
	}
	if err := release.Validate(); err != nil {
		return Release{}, err
	}
	return release, nil
}

// Clone returns a receipt whose provider attributes cannot alias persistent state.
func (r Receipt) Clone() Receipt {
	clone := r
	if len(r.Attributes) > 0 {
		clone.Attributes = make(map[string]string, len(r.Attributes))
		for key, value := range r.Attributes {
			clone.Attributes[key] = value
		}
	} else {
		// Normalize nil and an empty diagnostics map to one persistence identity;
		// omitempty JSON round-trips an empty map as nil inside rollback metadata.
		clone.Attributes = nil
	}
	return clone
}

// Validate checks the bounded provider identity, owner binding, opaque local ID, evidence digest, and diagnostics.
func (r Receipt) Validate() error {
	if r.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported ownership schema version %d", r.SchemaVersion)
	}
	if !r.Provider.Valid() || !r.Kind.Valid() {
		return errors.New("ownership provider and kind must be supported")
	}
	if (r.Provider == ProviderCgroupV2) != (r.Kind == KindSandboxCgroup || r.Kind == KindAttemptCgroup || r.Kind == KindKeeperCgroup) {
		return errors.New("ownership provider does not implement the receipt kind")
	}
	if err := validateOpaque("local ID", r.LocalID, 256); err != nil {
		return err
	}
	if err := r.Owner.Validate(); err != nil {
		return fmt.Errorf("receipt owner: %w", err)
	}
	if !validDigest(r.EvidenceSHA256) {
		return errors.New("receipt evidence must be a lowercase SHA-256 digest")
	}
	for key, value := range r.Attributes {
		if err := validateOpaque("attribute key", key, 128); err != nil {
			return err
		}
		if strings.ContainsRune(value, '\x00') || len(value) > 1024 {
			return fmt.Errorf("receipt attribute %q is not persistence safe", key)
		}
	}
	return nil
}

// Adopt returns an immutable ownership receipt for transfer from operation rollback state to a resource inventory.
func (r Receipt) Adopt() (Receipt, error) {
	if err := r.Validate(); err != nil {
		return Receipt{}, err
	}
	clone := r.Clone()
	clone.Adopted = true
	return clone, nil
}

// EvidenceDigest hashes canonical evidence bytes supplied by a provider after live readback.
func EvidenceDigest(evidence any) (string, error) {
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return "", fmt.Errorf("encode ownership evidence: %w", err)
	}
	var canonical any
	if err := json.Unmarshal(encoded, &canonical); err != nil {
		return "", fmt.Errorf("canonicalize ownership evidence: %w", err)
	}
	encoded, err = json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("encode canonical ownership evidence: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// InverseDescriptor builds a bounded rollback descriptor whose metadata is a validated receipt, never an arbitrary path command.
func InverseDescriptor(receipt Receipt, action Action) (rollback.Descriptor, error) {
	if err := receipt.Validate(); err != nil {
		return rollback.Descriptor{}, err
	}
	if receipt.Adopted {
		return rollback.Descriptor{}, errors.New("adopted resource cannot remain armed for acquisition rollback")
	}
	if !action.ValidFor(receipt.Provider, receipt.Kind) {
		return rollback.Descriptor{}, fmt.Errorf("ownership inverse action %q is incompatible with %s/%s", action, receipt.Provider, receipt.Kind)
	}
	metadata, err := json.Marshal(receipt)
	if err != nil {
		return rollback.Descriptor{}, fmt.Errorf("encode ownership receipt: %w", err)
	}
	return rollback.Descriptor{
		SchemaVersion: rollback.SchemaVersion,
		Name:          string(receipt.Kind) + ":" + receipt.LocalID,
		Action:        string(action),
		Target:        receipt.Owner.Token,
		Metadata:      metadata,
	}, nil
}

// ReceiptFromDescriptor validates a bounded action and restores the receipt for action-time provider verification.
func ReceiptFromDescriptor(descriptor rollback.Descriptor) (Receipt, Action, error) {
	if err := descriptor.Validate(); err != nil {
		return Receipt{}, "", err
	}
	action := Action(descriptor.Action)
	if !action.Valid() {
		return Receipt{}, "", fmt.Errorf("unsupported ownership inverse action %q", action)
	}
	var receipt Receipt
	if err := json.Unmarshal(descriptor.Metadata, &receipt); err != nil {
		return Receipt{}, "", fmt.Errorf("decode ownership receipt: %w", err)
	}
	if err := receipt.Validate(); err != nil {
		return Receipt{}, "", err
	}
	expected, err := InverseDescriptor(receipt, action)
	if err != nil {
		return Receipt{}, "", err
	}
	if descriptor.Name != expected.Name || descriptor.Target != expected.Target {
		return Receipt{}, "", errors.New("rollback descriptor does not match its ownership receipt")
	}
	return receipt.Clone(), action, nil
}

// SortReceipts returns a stable provider/kind/local-ID ordering for persistence and fingerprint tests.
func SortReceipts(receipts []Receipt) []Receipt {
	clones := make([]Receipt, len(receipts))
	for index, receipt := range receipts {
		clones[index] = receipt.Clone()
	}
	sort.Slice(clones, func(left, right int) bool {
		if clones[left].Provider != clones[right].Provider {
			return clones[left].Provider < clones[right].Provider
		}
		if clones[left].Kind != clones[right].Kind {
			return clones[left].Kind < clones[right].Kind
		}
		return clones[left].LocalID < clones[right].LocalID
	})
	return clones
}

// ValidateReceiptJournalProfile requires the complete ordered M2 resource profile for one lifecycle target.
// Acquisition order is part of the crash-recovery contract because rollback executes actionable entries in reverse.
func ValidateReceiptJournalProfile(targetKind operation.TargetKind, receipts []Receipt) error {
	expected, err := expectedProfile(targetKind)
	if err != nil {
		return err
	}
	if len(receipts) != len(expected) {
		return fmt.Errorf("%s receipt profile has %d entries, want %d", targetKind, len(receipts), len(expected))
	}
	return validateReceiptJournalExpectations(receipts, expected)
}

// ValidateReceiptJournalPrefix requires every active acquisition to be the canonical prefix of its complete M2 profile.
func ValidateReceiptJournalPrefix(targetKind operation.TargetKind, receipts []Receipt) error {
	expected, err := expectedProfile(targetKind)
	if err != nil {
		return err
	}
	if len(receipts) > len(expected) {
		return fmt.Errorf("%s receipt prefix has %d entries, maximum %d", targetKind, len(receipts), len(expected))
	}
	return validateReceiptJournalExpectations(receipts, expected[:len(receipts)])
}

// validateReceiptJournalExpectations compares one complete or partial journal with its dependency-ordered profile.
func validateReceiptJournalExpectations(receipts []Receipt, expected []profileExpectation) error {
	var owner OwnerKey
	for index, expectation := range expected {
		receipt := receipts[index]
		if err := receipt.Validate(); err != nil {
			return fmt.Errorf("receipt profile entry %d: %w", index, err)
		}
		if receipt.Provider != expectation.provider || receipt.Kind != expectation.kind {
			return fmt.Errorf("receipt profile entry %d is %s/%s, want %s/%s",
				index, receipt.Provider, receipt.Kind, expectation.provider, expectation.kind)
		}
		if index == 0 {
			owner = receipt.Owner
		} else if receipt.Owner != owner {
			return fmt.Errorf("receipt profile entry %d has a different owner", index)
		}
	}
	return nil
}

// profileExpectation names one provider-owned resource position in the durable acquisition journal.
type profileExpectation struct {
	provider Provider
	kind     Kind
}

// expectedProfile returns the only complete acquisition order supported by the initial M2 provider contract.
func expectedProfile(targetKind operation.TargetKind) ([]profileExpectation, error) {
	switch targetKind {
	case operation.TargetSandbox:
		return []profileExpectation{
			{ProviderCgroupV2, KindSandboxCgroup},
			{ProviderCgroupV2, KindKeeperCgroup},
			{ProviderLinux, KindKeeperProcess},
			{ProviderLinux, KindUTSNamespace},
			{ProviderLinux, KindIPCNamespace},
			{ProviderLinux, KindNetworkNamespace},
		}, nil
	case operation.TargetContainer, operation.TargetAttempt:
		return []profileExpectation{
			{ProviderCgroupV2, KindAttemptCgroup},
			{ProviderLinux, KindStartGate},
			{ProviderLinux, KindStreams},
			{ProviderLinux, KindInitProcess},
			{ProviderLinux, KindPIDNamespace},
			{ProviderLinux, KindMountNamespace},
			{ProviderLinux, KindRootfsMount},
		}, nil
	default:
		return nil, fmt.Errorf("target kind %q has no M2 receipt profile", targetKind)
	}
}

// validateOpaque rejects path separators, whitespace/control characters, and unbounded provider identifiers.
func validateOpaque(field, value string, maximum int) error {
	if value == "" || len(value) > maximum || strings.ContainsAny(value, `/\\`) {
		return fmt.Errorf("%s must be a non-empty bounded opaque identifier without path separators", field)
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return fmt.Errorf("%s contains whitespace or control characters", field)
		}
	}
	return nil
}

// validDigest reports whether value is exactly one lowercase SHA-256 hexadecimal digest.
func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}
