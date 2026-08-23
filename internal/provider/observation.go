package provider

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
)

// Presence is the three-state result of inspecting one exact owned host resource.
type Presence string

const (
	// PresencePresent means the provider verified the live resource and its owner evidence.
	PresencePresent Presence = "present"
	// PresenceAbsent means the provider verified that the exact deterministic resource is absent.
	PresenceAbsent Presence = "absent"
	// PresenceUnknown means the provider could not prove either presence or absence and no action is authorized.
	PresenceUnknown Presence = "unknown"
)

// Valid reports whether the presence belongs to the fail-closed observation vocabulary.
func (p Presence) Valid() bool {
	return p == PresencePresent || p == PresenceAbsent || p == PresenceUnknown
}

// ResourceObservation records verified presence or absence without using a path or PID as authority.
type ResourceObservation struct {
	Presence       Presence `json:"presence"`
	Verified       bool     `json:"verified"`
	EvidenceSHA256 string   `json:"evidence_sha256,omitempty"`
	Reason         string   `json:"reason,omitempty"`
}

// Validate enforces evidence for present resources, verified absence, and non-authorizing unknown state.
func (o ResourceObservation) Validate() error {
	if !o.Presence.Valid() {
		return fmt.Errorf("unsupported resource presence %q", o.Presence)
	}
	if strings.ContainsRune(o.Reason, '\x00') {
		return errors.New("resource observation reason must not contain NUL")
	}
	switch o.Presence {
	case PresencePresent:
		if !o.Verified || !validDigest(o.EvidenceSHA256) || o.Reason != "" {
			return errors.New("present resource observation requires verified SHA-256 evidence and no failure reason")
		}
	case PresenceAbsent:
		if !o.Verified || o.EvidenceSHA256 != "" || o.Reason != "" {
			return errors.New("absent resource observation requires verified absence without object evidence or reason")
		}
	case PresenceUnknown:
		if o.Verified || o.EvidenceSHA256 != "" || strings.TrimSpace(o.Reason) == "" {
			return errors.New("unknown resource observation must be unverified and include a reason")
		}
	}
	return nil
}

// CleanupDisposition distinguishes a removal performed now from an idempotent already-absent result.
type CleanupDisposition string

const (
	// CleanupRemoved means this call removed the exact verified owned resource.
	CleanupRemoved CleanupDisposition = "removed"
	// CleanupAlreadyAbsent means the exact deterministic resource was absent before this call.
	CleanupAlreadyAbsent CleanupDisposition = "already_absent"
)

// Valid reports whether a cleanup disposition is a successful idempotent terminal result.
func (d CleanupDisposition) Valid() bool {
	return d == CleanupRemoved || d == CleanupAlreadyAbsent
}

// CleanupObservation proves successful cleanup ended in verified absence.
type CleanupObservation struct {
	Disposition CleanupDisposition  `json:"disposition"`
	After       ResourceObservation `json:"after"`
}

// Validate rejects cleanup success unless the provider proved the exact resource absent afterward.
func (o CleanupObservation) Validate() error {
	if !o.Disposition.Valid() {
		return fmt.Errorf("unsupported cleanup disposition %q", o.Disposition)
	}
	if err := o.After.Validate(); err != nil {
		return fmt.Errorf("validate cleanup observation: %w", err)
	}
	if o.After.Presence != PresenceAbsent {
		return errors.New("successful cleanup must end in verified absence")
	}
	return nil
}

// Release converts a typed verified-absence observation into durable cleanup proof for one delete operation.
func (o CleanupObservation) Release(cleanupOperationID operation.OperationID, resource ownership.Receipt) (ownership.Release, error) {
	if err := o.Validate(); err != nil {
		return ownership.Release{}, err
	}
	return ownership.NewRelease(cleanupOperationID, resource, o)
}

// OOMSnapshot is one owner-verified read of memory.events.local counters for an Attempt cgroup.
type OOMSnapshot struct {
	Owner                ownership.OwnerKey `json:"owner"`
	CgroupEvidenceSHA256 string             `json:"cgroup_evidence_sha256"`
	OOM                  uint64             `json:"oom"`
	OOMKill              uint64             `json:"oom_kill"`
	OOMGroupKill         uint64             `json:"oom_group_kill"`
	EvidenceSHA256       string             `json:"evidence_sha256"`
}

// NewOOMSnapshot binds local memory event counters to canonical evidence for persistence and comparison.
func NewOOMSnapshot(scope OwnedReceiptRequest, oom, oomKill, oomGroupKill uint64) (OOMSnapshot, error) {
	if err := scope.ValidateFor(ownership.ProviderCgroupV2, ownership.KindAttemptCgroup); err != nil {
		return OOMSnapshot{}, fmt.Errorf("OOM snapshot scope: %w", err)
	}
	snapshot := OOMSnapshot{
		Owner: scope.Owner, CgroupEvidenceSHA256: scope.Receipt.EvidenceSHA256,
		OOM: oom, OOMKill: oomKill, OOMGroupKill: oomGroupKill,
	}
	digest, err := ownership.EvidenceDigest(struct {
		Owner                ownership.OwnerKey `json:"owner"`
		CgroupEvidenceSHA256 string             `json:"cgroup_evidence_sha256"`
		OOM                  uint64             `json:"oom"`
		OOMKill              uint64             `json:"oom_kill"`
		OOMGroupKill         uint64             `json:"oom_group_kill"`
	}{scope.Owner, scope.Receipt.EvidenceSHA256, oom, oomKill, oomGroupKill})
	if err != nil {
		return OOMSnapshot{}, err
	}
	snapshot.EvidenceSHA256 = digest
	return snapshot, nil
}

// Validate rejects an OOM snapshot whose counters no longer match its canonical evidence digest.
func (s OOMSnapshot) Validate() error {
	scopeReceipt := ownership.Receipt{
		SchemaVersion: ownership.SchemaVersion, Provider: ownership.ProviderCgroupV2,
		Kind: ownership.KindAttemptCgroup, LocalID: "oom-scope", Owner: s.Owner,
		EvidenceSHA256: s.CgroupEvidenceSHA256,
	}
	expected, err := NewOOMSnapshot(OwnedReceiptRequest{Owner: s.Owner, Receipt: scopeReceipt}, s.OOM, s.OOMKill, s.OOMGroupKill)
	if err != nil {
		return err
	}
	if s.EvidenceSHA256 != expected.EvidenceSHA256 {
		return errors.New("OOM snapshot evidence does not match its counters")
	}
	return nil
}

// Delta classifies OOM evidence from two snapshots of the same Attempt and fails closed on counter regression.
func (s OOMSnapshot) Delta(earlier OOMSnapshot) (domain.EvidenceState, error) {
	if err := earlier.Validate(); err != nil {
		return domain.EvidenceUnknown, fmt.Errorf("earlier OOM snapshot: %w", err)
	}
	if err := s.Validate(); err != nil {
		return domain.EvidenceUnknown, fmt.Errorf("later OOM snapshot: %w", err)
	}
	if s.Owner != earlier.Owner || s.CgroupEvidenceSHA256 != earlier.CgroupEvidenceSHA256 {
		return domain.EvidenceUnknown, errors.New("OOM snapshots belong to different Attempt cgroups")
	}
	if s.OOM < earlier.OOM || s.OOMKill < earlier.OOMKill || s.OOMGroupKill < earlier.OOMGroupKill {
		return domain.EvidenceUnknown, errors.New("OOM counters regressed")
	}
	if s.OOMKill > earlier.OOMKill || s.OOMGroupKill > earlier.OOMGroupKill {
		return domain.EvidenceTrue, nil
	}
	return domain.EvidenceFalse, nil
}

// AttachmentObservation proves that one exact init-process receipt was read back in one exact owned Attempt-cgroup receipt.
type AttachmentObservation struct {
	Owner                ownership.OwnerKey `json:"owner"`
	CgroupReceiptSHA256  string             `json:"cgroup_receipt_sha256"`
	ProcessReceiptSHA256 string             `json:"process_receipt_sha256"`
	Verified             bool               `json:"verified"`
	EvidenceSHA256       string             `json:"evidence_sha256"`
}

// NewAttachmentObservation binds a successful membership readback to the exact validated cgroup and init receipts used for the action.
func NewAttachmentObservation(request AttachProcessRequest) (AttachmentObservation, error) {
	if err := request.Validate(); err != nil {
		return AttachmentObservation{}, fmt.Errorf("attachment request: %w", err)
	}
	cgroupDigest, err := ownership.EvidenceDigest(request.Cgroup)
	if err != nil {
		return AttachmentObservation{}, err
	}
	processDigest, err := ownership.EvidenceDigest(request.Process)
	if err != nil {
		return AttachmentObservation{}, err
	}
	observation := AttachmentObservation{
		Owner: request.Owner, CgroupReceiptSHA256: cgroupDigest, ProcessReceiptSHA256: processDigest, Verified: true,
	}
	observation.EvidenceSHA256, err = attachmentObservationDigest(observation)
	if err != nil {
		return AttachmentObservation{}, err
	}
	return observation, nil
}

// Validate rejects attachment evidence that is unverified, ownerless, malformed, or no longer matches its canonical scope digest.
func (o AttachmentObservation) Validate() error {
	if err := o.Owner.Validate(); err != nil {
		return fmt.Errorf("attachment owner: %w", err)
	}
	if !o.Verified || !validDigest(o.CgroupReceiptSHA256) || !validDigest(o.ProcessReceiptSHA256) || !validDigest(o.EvidenceSHA256) {
		return errors.New("cgroup attachment requires verified, receipt-scoped SHA-256 evidence")
	}
	expected, err := attachmentObservationDigest(o)
	if err != nil {
		return err
	}
	if o.EvidenceSHA256 != expected {
		return errors.New("cgroup attachment evidence does not match its owner and receipt scope")
	}
	return nil
}

// ValidateFor proves the observation came from the exact attachment request now authorizing gate release.
func (o AttachmentObservation) ValidateFor(request AttachProcessRequest) error {
	if err := o.Validate(); err != nil {
		return err
	}
	expected, err := NewAttachmentObservation(request)
	if err != nil {
		return err
	}
	if o != expected {
		return errors.New("cgroup attachment observation belongs to a different owner, cgroup, or init process")
	}
	return nil
}

// attachmentObservationDigest computes the canonical membership fact without recursively including its digest field.
func attachmentObservationDigest(o AttachmentObservation) (string, error) {
	return ownership.EvidenceDigest(struct {
		Owner                ownership.OwnerKey `json:"owner"`
		CgroupReceiptSHA256  string             `json:"cgroup_receipt_sha256"`
		ProcessReceiptSHA256 string             `json:"process_receipt_sha256"`
		Verified             bool               `json:"verified"`
	}{o.Owner, o.CgroupReceiptSHA256, o.ProcessReceiptSHA256, o.Verified})
}

// SignalObservation proves a signal was sent only after action-time process identity verification.
type SignalObservation struct {
	Signal                 Signal    `json:"signal"`
	IdentityEvidenceSHA256 string    `json:"identity_evidence_sha256"`
	Delivered              bool      `json:"delivered"`
	DeliveredAt            time.Time `json:"delivered_at"`
}

// Validate rejects a successful signal result without strong identity evidence, delivery confirmation, or the action-completion time used by durable grace accounting.
func (o SignalObservation) Validate() error {
	if !o.Signal.Valid() || !o.Delivered || !validDigest(o.IdentityEvidenceSHA256) || o.DeliveredAt.IsZero() {
		return errors.New("signal observation requires a supported signal, verified identity evidence, delivery, and delivery time")
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
