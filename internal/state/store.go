package state

import (
	"context"
	"fmt"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
	"mydocker/internal/rollback"
)

// SandboxRecord wraps one Sandbox in the persistence schema and a store-local
// CAS revision. Revision is deliberately separate from Sandbox.Generation.
type SandboxRecord struct {
	SchemaVersion SchemaVersion       `json:"schema_version"`
	Revision      Revision            `json:"revision"`
	Sandbox       domain.Sandbox      `json:"sandbox"`
	HostResources []ownership.Receipt `json:"host_resources,omitempty"`
}

// NewSandboxRecord prepares a new v1 Sandbox envelope for a create operation;
// the store assigns its first non-zero revision when it commits the record.
func NewSandboxRecord(sandbox domain.Sandbox) SandboxRecord {
	return SandboxRecord{SchemaVersion: SchemaVersionV1, Sandbox: sandbox.Clone()}
}

// Clone returns a deeply independent Sandbox envelope for transaction and
// caller boundaries, preventing mutation outside a Store callback.
func (r SandboxRecord) Clone() SandboxRecord {
	clone := r
	clone.Sandbox = r.Sandbox.Clone()
	clone.HostResources = cloneReceipts(r.HostResources)
	return clone
}

// ContainerAttemptRecord persists the M1 one-to-one Container/Attempt pair as
// one CAS unit so the two identities and projected status cannot tear apart.
type ContainerAttemptRecord struct {
	SchemaVersion    SchemaVersion           `json:"schema_version"`
	Revision         Revision                `json:"revision"`
	ContainerAttempt domain.ContainerAttempt `json:"container_attempt"`
	HostResources    []ownership.Receipt     `json:"host_resources,omitempty"`
}

// NewContainerAttemptRecord prepares a new v1 aggregate envelope whose first
// revision is assigned only when its surrounding transaction commits.
func NewContainerAttemptRecord(pair domain.ContainerAttempt) ContainerAttemptRecord {
	return ContainerAttemptRecord{
		SchemaVersion:    SchemaVersionV1,
		ContainerAttempt: pair.Clone(),
	}
}

// Clone returns a deeply independent aggregate envelope so nested argv,
// environment, resources, outcomes, and conditions cannot alias store memory.
func (r ContainerAttemptRecord) Clone() ContainerAttemptRecord {
	clone := r
	clone.ContainerAttempt = r.ContainerAttempt.Clone()
	clone.HostResources = cloneReceipts(r.HostResources)
	return clone
}

// OperationRecord wraps a durable operation model in the state schema and a
// CAS revision used by reconcilers to prevent concurrent stage advancement.
type OperationRecord struct {
	SchemaVersion          SchemaVersion         `json:"schema_version"`
	Revision               Revision              `json:"revision"`
	HostProfile            HostProfile           `json:"host_profile"`
	Operation              operation.Operation   `json:"operation"`
	RollbackCause          *rollback.Cause       `json:"rollback_cause,omitempty"`
	OOMBaseline            *provider.OOMSnapshot `json:"oom_baseline,omitempty"`
	KillEscalationDeadline *time.Time            `json:"kill_escalation_deadline,omitempty"`
	Rollback               []rollback.Record     `json:"rollback,omitempty"`
	Receipts               []ownership.Receipt   `json:"receipts,omitempty"`
	Releases               []ownership.Release   `json:"releases,omitempty"`
}

// NewOperationRecord prepares an operation envelope for its first atomic
// persist-intent transaction without borrowing the response byte slice.
func NewOperationRecord(value operation.Operation) OperationRecord {
	return OperationRecord{
		SchemaVersion: SchemaVersionV1,
		HostProfile:   HostProfileAbstractM1,
		Operation:     value.Clone(),
	}
}

// NewOperationRecordForProfile prepares an operation envelope with an explicit host execution contract.
func NewOperationRecordForProfile(value operation.Operation, profile HostProfile) (OperationRecord, error) {
	if !profile.Valid() {
		return OperationRecord{}, fmt.Errorf("invalid host profile %q", profile)
	}
	record := NewOperationRecord(value)
	record.HostProfile = profile
	return record, nil
}

// Clone returns a deeply independent operation envelope, including replay response and optional deadline pointers, for safe use outside the store lock.
func (r OperationRecord) Clone() OperationRecord {
	clone := r
	clone.Operation = r.Operation.Clone()
	if r.RollbackCause != nil {
		cause := r.RollbackCause.Clone()
		clone.RollbackCause = &cause
	}
	if r.OOMBaseline != nil {
		baseline := *r.OOMBaseline
		clone.OOMBaseline = &baseline
	}
	if r.KillEscalationDeadline != nil {
		deadline := *r.KillEscalationDeadline
		clone.KillEscalationDeadline = &deadline
	}
	clone.Rollback = make([]rollback.Record, len(r.Rollback))
	for index, record := range r.Rollback {
		clone.Rollback[index] = record.Clone()
	}
	clone.Receipts = cloneReceipts(r.Receipts)
	clone.Releases = cloneReleases(r.Releases)
	return clone
}

// cloneReceipts copies receipt attributes so transaction callers cannot mutate
// persisted host ownership evidence through a returned record.
func cloneReceipts(receipts []ownership.Receipt) []ownership.Receipt {
	if receipts == nil {
		return nil
	}
	clones := make([]ownership.Receipt, len(receipts))
	for index, receipt := range receipts {
		clones[index] = receipt.Clone()
	}
	return clones
}

// cloneReleases copies cleanup proof and nested receipt attributes for safe transaction boundaries.
func cloneReleases(releases []ownership.Release) []ownership.Release {
	if releases == nil {
		return nil
	}
	clones := make([]ownership.Release, len(releases))
	for index, release := range releases {
		clones[index] = release.Clone()
	}
	return clones
}

// Reader exposes immutable snapshots within one callback-scoped store view.
// Every returned value is a deep copy and remains safe after the callback ends.
type Reader interface {
	// GetSandbox returns one Sandbox by stable identity or ErrNotFound.
	GetSandbox(id domain.SandboxID) (SandboxRecord, error)
	// ListSandboxes returns every Sandbox in deterministic identity order.
	ListSandboxes() ([]SandboxRecord, error)
	// GetContainerAttempt returns one atomic pair by its Container identity.
	GetContainerAttempt(containerID domain.ContainerID) (ContainerAttemptRecord, error)
	// ListContainerAttempts returns one Sandbox's pairs in Container identity order.
	ListContainerAttempts(sandboxID domain.SandboxID) ([]ContainerAttemptRecord, error)
	// GetOperation returns one durable idempotency record by client identity.
	GetOperation(id operation.OperationID) (OperationRecord, error)
	// ListOperations returns every retained operation in deterministic identity order.
	ListOperations() ([]OperationRecord, error)
	// ActiveOperation finds the unfinished operation currently owning a target.
	ActiveOperation(target operation.Target) (OperationRecord, error)
	// EventsAfter returns events strictly after a resume token with optional paging.
	EventsAfter(after EventSequence, limit int) ([]operation.Event, error)
}

// Tx extends Reader with atomic mutations. Put methods use expectedRevision ==
// zero for create and an exact non-zero revision for update.
type Tx interface {
	Reader
	// PutSandbox creates at revision zero or updates the exact expected revision.
	PutSandbox(record SandboxRecord, expectedRevision Revision) (SandboxRecord, error)
	// DeleteSandbox removes only the exact revision observed by the caller.
	DeleteSandbox(id domain.SandboxID, expectedRevision Revision) error
	// PutContainerAttempt atomically creates or CAS-updates a one-to-one pair.
	PutContainerAttempt(record ContainerAttemptRecord, expectedRevision Revision) (ContainerAttemptRecord, error)
	// DeleteContainerAttempt removes Container and Attempt metadata as one CAS unit.
	DeleteContainerAttempt(containerID domain.ContainerID, expectedRevision Revision) error
	// PutOperation creates or advances an operation without changing its binding.
	PutOperation(record OperationRecord, expectedRevision Revision) (OperationRecord, error)
	// AppendEvent assigns the next transaction-local candidate global sequence.
	AppendEvent(event operation.Event) (operation.Event, error)
}

// Store provides callback-scoped read and update transactions. Update commits
// every resource, operation, and event change together only when fn succeeds.
type Store interface {
	// View supplies a callback-scoped consistent reader without permitting writes.
	View(ctx context.Context, fn func(Reader) error) error
	// Update atomically commits all callback changes only when the callback succeeds.
	Update(ctx context.Context, fn func(Tx) error) error
}
