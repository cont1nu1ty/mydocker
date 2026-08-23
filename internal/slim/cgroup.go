package slim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"mydocker/internal/cgroupv2"
	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
)

const (
	attributeSandboxID = "sandbox_id"
	attributeAttemptID = "attempt_id"
	attributeEffective = "effective_limits"
)

// ProcessResolver restores a runtime-only strong process handle from one exact persisted process receipt.
type ProcessResolver interface {
	ResolveProcess(context.Context, ownership.Receipt) (cgroupv2.ProcessReference, error)
}

// CgroupProvider is the production cgroup-v2 adapter used by the slim M3 host provider.
type CgroupProvider struct {
	manager   *cgroupv2.Manager
	processes ProcessResolver
}

// NewCgroupProvider validates concrete dependencies without touching the delegated hierarchy.
func NewCgroupProvider(manager *cgroupv2.Manager, processes ProcessResolver) (*CgroupProvider, error) {
	if manager == nil {
		return nil, errors.New("slim cgroup provider requires a cgroup v2 manager")
	}
	if processes == nil {
		return nil, errors.New("slim cgroup provider requires a strong process resolver")
	}
	return &CgroupProvider{manager: manager, processes: processes}, nil
}

// InspectCgroupCapabilities performs only the manager's read-only exact-root preflight.
func (adapter *CgroupProvider) InspectCgroupCapabilities(ctx context.Context, requirements provider.CgroupRequirements) (provider.CgroupCapabilities, error) {
	if err := requirements.Validate(); err != nil {
		return provider.CgroupCapabilities{}, err
	}
	if err := adapter.manager.Preflight(ctx); err != nil {
		return provider.CgroupCapabilities{}, err
	}
	return provider.CgroupCapabilities{
		UnifiedV2: true, Delegated: true,
		Controllers: append([]provider.CgroupController(nil), requirements.Controllers...),
	}, nil
}

// EnsureSandboxCgroup creates or rediscovers one process-free Sandbox parent and returns path-free evidence.
func (adapter *CgroupProvider) EnsureSandboxCgroup(ctx context.Context, request provider.SandboxCgroupRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	if _, err := adapter.manager.CreateSandbox(ctx, request.SandboxID); err != nil {
		return ownership.Receipt{}, err
	}
	return newCgroupReceipt(request.Owner, ownership.KindSandboxCgroup, request.SandboxID, "", nil)
}

// InspectSandboxCgroup verifies the durable receipt before checking the deterministic manager path read-only.
func (adapter *CgroupProvider) InspectSandboxCgroup(ctx context.Context, request provider.OwnedReceiptRequest) (provider.ResourceObservation, error) {
	scope, err := validateCgroupReceipt(request, ownership.KindSandboxCgroup)
	if err != nil {
		return provider.ResourceObservation{}, err
	}
	present, err := adapter.manager.InspectSandboxPresence(ctx, scope.sandboxID)
	return cgroupPresenceObservation(present, request.Receipt.EvidenceSHA256, err)
}

// RemoveSandboxCgroup removes only the exact verified empty Sandbox parent and proves absence afterward.
func (adapter *CgroupProvider) RemoveSandboxCgroup(ctx context.Context, request provider.OwnedReceiptRequest) (provider.CleanupObservation, error) {
	scope, err := validateCgroupReceipt(request, ownership.KindSandboxCgroup)
	if err != nil {
		return provider.CleanupObservation{}, err
	}
	before, err := adapter.manager.InspectSandboxPresence(ctx, scope.sandboxID)
	if err != nil {
		return provider.CleanupObservation{}, err
	}
	if err := adapter.manager.RemoveSandbox(ctx, scope.sandboxID); err != nil {
		return provider.CleanupObservation{}, err
	}
	return verifyCgroupRemoval(ctx, before, func() (bool, error) {
		return adapter.manager.InspectSandboxPresence(ctx, scope.sandboxID)
	})
}

// EnsureKeeperCgroup creates the fixed process-bearing leaf under its already verified Sandbox parent.
func (adapter *CgroupProvider) EnsureKeeperCgroup(ctx context.Context, request provider.KeeperCgroupRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	if _, err := adapter.manager.CreateKeeper(ctx, request.SandboxID); err != nil {
		return ownership.Receipt{}, err
	}
	return newCgroupReceipt(request.Owner, ownership.KindKeeperCgroup, request.SandboxID, "", nil)
}

// InspectKeeperCgroup verifies the fixed keeper leaf without trusting a receipt-supplied path.
func (adapter *CgroupProvider) InspectKeeperCgroup(ctx context.Context, request provider.OwnedReceiptRequest) (provider.ResourceObservation, error) {
	scope, err := validateCgroupReceipt(request, ownership.KindKeeperCgroup)
	if err != nil {
		return provider.ResourceObservation{}, err
	}
	present, err := adapter.manager.InspectKeeperPresence(ctx, scope.sandboxID)
	return cgroupPresenceObservation(present, request.Receipt.EvidenceSHA256, err)
}

// RemoveKeeperCgroup removes one exact empty keeper leaf and verifies its deterministic path is absent.
func (adapter *CgroupProvider) RemoveKeeperCgroup(ctx context.Context, request provider.OwnedReceiptRequest) (provider.CleanupObservation, error) {
	scope, err := validateCgroupReceipt(request, ownership.KindKeeperCgroup)
	if err != nil {
		return provider.CleanupObservation{}, err
	}
	before, err := adapter.manager.InspectKeeperPresence(ctx, scope.sandboxID)
	if err != nil {
		return provider.CleanupObservation{}, err
	}
	if err := adapter.manager.RemoveKeeper(ctx, scope.sandboxID); err != nil {
		return provider.CleanupObservation{}, err
	}
	return verifyCgroupRemoval(ctx, before, func() (bool, error) {
		return adapter.manager.InspectKeeperPresence(ctx, scope.sandboxID)
	})
}

// EnsureAttemptCgroup applies immutable resolved limits and persists their host-canonical readback in the receipt.
func (adapter *CgroupProvider) EnsureAttemptCgroup(ctx context.Context, request provider.AttemptCgroupRequest) (ownership.Receipt, error) {
	if err := request.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	_, effective, err := adapter.manager.CreateAttempt(ctx, request.SandboxID, request.AttemptID, request.Limits)
	if err != nil {
		return ownership.Receipt{}, err
	}
	return newCgroupReceipt(request.Owner, ownership.KindAttemptCgroup, request.SandboxID, request.AttemptID, &effective)
}

// InspectAttemptCgroup checks presence and exact controller readback against the immutable receipt evidence.
func (adapter *CgroupProvider) InspectAttemptCgroup(ctx context.Context, request provider.OwnedReceiptRequest) (provider.ResourceObservation, error) {
	scope, err := validateCgroupReceipt(request, ownership.KindAttemptCgroup)
	if err != nil {
		return provider.ResourceObservation{}, err
	}
	present, err := adapter.manager.InspectAttemptPresence(ctx, scope.sandboxID, scope.attemptID)
	if err != nil || !present {
		return cgroupPresenceObservation(present, request.Receipt.EvidenceSHA256, err)
	}
	effective, err := adapter.manager.ReadEffectiveLimits(ctx, scope.sandboxID, scope.attemptID)
	if err != nil {
		return provider.ResourceObservation{}, cgroupObservationError(err)
	}
	if !effective.Equal(*scope.effective) {
		return provider.ResourceObservation{}, fmt.Errorf("Attempt cgroup effective limits changed: %w", cgroupv2.ErrEffectiveMismatch)
	}
	return cgroupPresenceObservation(true, request.Receipt.EvidenceSHA256, nil)
}

// SnapshotAttemptOOM returns owner-scoped local memory event counters for the exact Attempt receipt.
func (adapter *CgroupProvider) SnapshotAttemptOOM(ctx context.Context, request provider.OwnedReceiptRequest) (provider.OOMSnapshot, error) {
	scope, err := validateCgroupReceipt(request, ownership.KindAttemptCgroup)
	if err != nil {
		return provider.OOMSnapshot{}, err
	}
	snapshot, err := adapter.manager.SnapshotOOM(ctx, scope.sandboxID, scope.attemptID)
	if err != nil {
		return provider.OOMSnapshot{}, err
	}
	return provider.NewOOMSnapshot(request, snapshot.OOM, snapshot.OOMKill, snapshot.OOMGroupKill)
}

// RemoveAttemptCgroup removes one empty deterministic Attempt leaf and verifies absence afterward.
func (adapter *CgroupProvider) RemoveAttemptCgroup(ctx context.Context, request provider.OwnedReceiptRequest) (provider.CleanupObservation, error) {
	scope, err := validateCgroupReceipt(request, ownership.KindAttemptCgroup)
	if err != nil {
		return provider.CleanupObservation{}, err
	}
	before, err := adapter.manager.InspectAttemptPresence(ctx, scope.sandboxID, scope.attemptID)
	if err != nil {
		return provider.CleanupObservation{}, err
	}
	if err := adapter.manager.RemoveAttempt(ctx, scope.sandboxID, scope.attemptID); err != nil {
		return provider.CleanupObservation{}, err
	}
	return verifyCgroupRemoval(ctx, before, func() (bool, error) {
		return adapter.manager.InspectAttemptPresence(ctx, scope.sandboxID, scope.attemptID)
	})
}

// AttachAttemptProcess restores a strong process handle and brackets cgroup membership readback with action-time verification.
func (adapter *CgroupProvider) AttachAttemptProcess(ctx context.Context, request provider.AttachProcessRequest) (provider.AttachmentObservation, error) {
	if err := request.Validate(); err != nil {
		return provider.AttachmentObservation{}, err
	}
	scope, err := validateCgroupReceipt(provider.OwnedReceiptRequest{Owner: request.Owner, Receipt: request.Cgroup}, ownership.KindAttemptCgroup)
	if err != nil {
		return provider.AttachmentObservation{}, err
	}
	process, err := adapter.processes.ResolveProcess(ctx, request.Process)
	if err != nil {
		return provider.AttachmentObservation{}, err
	}
	if err := adapter.manager.AttachProcess(ctx, scope.sandboxID, scope.attemptID, process); err != nil {
		return provider.AttachmentObservation{}, err
	}
	return provider.NewAttachmentObservation(request)
}

// cgroupScope is the provider-private identity reconstructed only from validated bounded receipt attributes.
type cgroupScope struct {
	sandboxID domain.SandboxID
	attemptID domain.AttemptID
	effective *cgroupv2.EffectiveLimits
}

// newCgroupReceipt constructs path-free evidence from typed identities and host-canonical effective limits.
func newCgroupReceipt(owner ownership.OwnerKey, kind ownership.Kind, sandboxID domain.SandboxID, attemptID domain.AttemptID, effective *cgroupv2.EffectiveLimits) (ownership.Receipt, error) {
	attributes := map[string]string{attributeSandboxID: string(sandboxID)}
	if attemptID != "" {
		attributes[attributeAttemptID] = string(attemptID)
	}
	if effective != nil {
		encoded, err := json.Marshal(effective)
		if err != nil {
			return ownership.Receipt{}, err
		}
		attributes[attributeEffective] = string(encoded)
	}
	evidence, err := cgroupReceiptEvidence(owner, kind, sandboxID, attemptID, effective)
	if err != nil {
		return ownership.Receipt{}, err
	}
	receipt := ownership.Receipt{
		SchemaVersion: ownership.SchemaVersion, Provider: ownership.ProviderCgroupV2, Kind: kind,
		LocalID: string(kind) + "-" + evidence[:24], Owner: owner, EvidenceSHA256: evidence, Attributes: attributes,
	}
	if err := receipt.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	return receipt, nil
}

// cgroupReceiptEvidence binds a receipt to typed resource identity rather than a caller-controlled host path.
func cgroupReceiptEvidence(owner ownership.OwnerKey, kind ownership.Kind, sandboxID domain.SandboxID, attemptID domain.AttemptID, effective *cgroupv2.EffectiveLimits) (string, error) {
	return ownership.EvidenceDigest(struct {
		Owner     ownership.OwnerKey        `json:"owner"`
		Kind      ownership.Kind            `json:"kind"`
		SandboxID domain.SandboxID          `json:"sandbox_id"`
		AttemptID domain.AttemptID          `json:"attempt_id,omitempty"`
		Effective *cgroupv2.EffectiveLimits `json:"effective,omitempty"`
	}{owner, kind, sandboxID, attemptID, effective})
}

// validateCgroupReceipt proves every persisted field is the canonical encoding for its bounded typed scope.
func validateCgroupReceipt(request provider.OwnedReceiptRequest, kind ownership.Kind) (cgroupScope, error) {
	if err := request.ValidateFor(ownership.ProviderCgroupV2, kind); err != nil {
		return cgroupScope{}, err
	}
	sandboxID := domain.SandboxID(request.Receipt.Attributes[attributeSandboxID])
	if err := sandboxID.Validate(); err != nil {
		return cgroupScope{}, err
	}
	scope := cgroupScope{sandboxID: sandboxID}
	if kind == ownership.KindAttemptCgroup {
		scope.attemptID = domain.AttemptID(request.Receipt.Attributes[attributeAttemptID])
		if err := scope.attemptID.Validate(); err != nil {
			return cgroupScope{}, err
		}
		var effective cgroupv2.EffectiveLimits
		if err := json.Unmarshal([]byte(request.Receipt.Attributes[attributeEffective]), &effective); err != nil {
			return cgroupScope{}, fmt.Errorf("decode Attempt effective limits: %w", err)
		}
		scope.effective = &effective
	}
	expected, err := newCgroupReceipt(request.Owner, kind, scope.sandboxID, scope.attemptID, scope.effective)
	if err != nil {
		return cgroupScope{}, err
	}
	expected.Adopted = request.Receipt.Adopted
	if !reflect.DeepEqual(expected, request.Receipt.Clone()) {
		return cgroupScope{}, errors.New("cgroup receipt does not match its canonical typed identity")
	}
	if kind != ownership.KindAttemptCgroup && request.Owner.Target.Kind != operation.TargetSandbox {
		return cgroupScope{}, errors.New("Sandbox cgroup receipt owner must target a Sandbox")
	}
	if kind == ownership.KindAttemptCgroup && request.Owner.Target.Kind != operation.TargetContainer {
		return cgroupScope{}, errors.New("Attempt cgroup receipt owner must target a Container")
	}
	return scope, nil
}

// cgroupPresenceObservation converts one exact read-only manager result to the provider's fail-closed vocabulary.
func cgroupPresenceObservation(present bool, evidence string, err error) (provider.ResourceObservation, error) {
	if err != nil {
		return provider.ResourceObservation{}, cgroupObservationError(err)
	}
	if !present {
		return provider.ResourceObservation{Presence: provider.PresenceAbsent, Verified: true}, nil
	}
	return provider.ResourceObservation{Presence: provider.PresencePresent, Verified: true, EvidenceSHA256: evidence}, nil
}

// cgroupObservationError translates incomplete read-only kernel evidence into
// a retryable unknown observation without weakening any mutating cgroup path.
func cgroupObservationError(err error) error {
	if errors.Is(err, cgroupv2.ErrUnknownState) {
		return provider.MarkObservationUnavailable(err)
	}
	return err
}

// verifyCgroupRemoval converts a post-remove deterministic absence check into an idempotent cleanup observation.
func verifyCgroupRemoval(ctx context.Context, existed bool, inspect func() (bool, error)) (provider.CleanupObservation, error) {
	if err := ctx.Err(); err != nil {
		return provider.CleanupObservation{}, err
	}
	present, err := inspect()
	if err != nil {
		return provider.CleanupObservation{}, err
	}
	if present {
		return provider.CleanupObservation{}, errors.New("cgroup remained present after exact removal")
	}
	disposition := provider.CleanupAlreadyAbsent
	if existed {
		disposition = provider.CleanupRemoved
	}
	return provider.CleanupObservation{
		Disposition: disposition, After: provider.ResourceObservation{Presence: provider.PresenceAbsent, Verified: true},
	}, nil
}
