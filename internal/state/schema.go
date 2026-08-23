// Package state defines the versioned persistence boundary used by the M1
// lifecycle model and provides an in-memory transactional implementation for
// deterministic unit tests.
package state

import "mydocker/internal/operation"

// SchemaVersion identifies the persisted envelope format independently from a
// resource's spec generation or the store's compare-and-swap revision.
type SchemaVersion uint32

const (
	// SchemaVersionV1 is the only record schema accepted by the M1 state store.
	SchemaVersionV1 SchemaVersion = 1
)

// Revision is a store-local compare-and-swap token. It prevents lost updates
// and must never be exposed as a Sandbox or Container spec generation.
type Revision uint64

// EventSequence is the store-wide use of the operation event ordering token.
// Values start at one and aborted transactions do not consume a value.
type EventSequence = operation.Sequence

// HostProfile identifies whether an operation is a pure M1 contract exercise or must own the complete Linux M2 resource set.
type HostProfile string

const (
	// HostProfileAbstractM1 permits pure domain/lifecycle tests and never accepts host receipts.
	HostProfileAbstractM1 HostProfile = "abstract_m1"
	// HostProfileLinuxM2 requires the complete ordered cgroup/process/namespace/rootfs receipt profile for successful create.
	HostProfileLinuxM2 HostProfile = "linux_m2"
)

// Valid reports whether a host profile belongs to the version-one persistence vocabulary.
func (p HostProfile) Valid() bool {
	return p == HostProfileAbstractM1 || p == HostProfileLinuxM2
}
