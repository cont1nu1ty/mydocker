package state

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"mydocker/internal/operation"
)

const (
	// DefaultTerminalOperationLimit is the count-based exact-response replay
	// window retained as complete operation records by production stores.
	DefaultTerminalOperationLimit = 1024
	// DefaultOperationIdentityLimit bounds complete operations plus retired ID
	// tombstones; reaching it rejects new intent before any host side effect.
	DefaultOperationIdentityLimit = 65536
	// DefaultEventLimit is the contiguous event suffix retained for resume and diagnostics.
	DefaultEventLimit = 8192
)

// RetentionPolicy bounds the unreferenced terminal replay window, all remembered
// operation identities, and the contiguous event suffix in one store commit.
// Terminal create operations that still own a live HostResources inventory are
// additionally pinned, but continue to consume the hard identity budget.
type RetentionPolicy struct {
	TerminalOperationLimit int
	OperationIdentityLimit int
	EventLimit             int
}

// Validate rejects policies that could discard active intent, forget a retired
// operation identity, or leave no event from which a new reader can start.
func (policy RetentionPolicy) Validate() error {
	if policy.TerminalOperationLimit <= 0 {
		return errorsRetentionPolicy("terminal operation limit must be positive")
	}
	if policy.OperationIdentityLimit < policy.TerminalOperationLimit {
		return errorsRetentionPolicy("operation identity limit must cover the terminal replay window")
	}
	if policy.EventLimit <= 0 {
		return errorsRetentionPolicy("event limit must be positive")
	}
	return nil
}

// DefaultRetentionPolicy returns the production count-based limits used by
// MemoryStore and FileStore unless a focused test supplies smaller values.
func DefaultRetentionPolicy() RetentionPolicy {
	return RetentionPolicy{
		TerminalOperationLimit: DefaultTerminalOperationLimit,
		OperationIdentityLimit: DefaultOperationIdentityLimit,
		EventLimit:             DefaultEventLimit,
	}
}

// retiredOperation is the bounded fail-closed proof that an operation ID once
// existed after its full response left the exact-replay window.
type retiredOperation struct {
	OperationIDSHA256     string                       `json:"operation_id_sha256"`
	Type                  operation.Type               `json:"type"`
	Target                operation.Target             `json:"target"`
	Fingerprint           operation.RequestFingerprint `json:"fingerprint"`
	Reason                operation.ReasonClass        `json:"reason"`
	TerminalSequence      uint64                       `json:"terminal_sequence"`
	TerminalEventSequence EventSequence                `json:"terminal_event_sequence,omitempty"`
}

// validate rejects a tombstone that cannot safely block ID reuse or validate
// retained events without restoring the discarded response payload.
func (retired retiredOperation) validate() error {
	if len(retired.OperationIDSHA256) != sha256.Size*2 {
		return fmt.Errorf("retired operation digest has invalid length: %w", ErrInvalidRecord)
	}
	if _, err := hex.DecodeString(retired.OperationIDSHA256); err != nil {
		return fmt.Errorf("retired operation digest is not lowercase hexadecimal: %w", ErrInvalidRecord)
	}
	if retired.OperationIDSHA256 != stringLower(retired.OperationIDSHA256) {
		return fmt.Errorf("retired operation digest is not canonical: %w", ErrInvalidRecord)
	}
	if !retired.Type.Valid() {
		return fmt.Errorf("retired operation type %q is invalid: %w", retired.Type, ErrInvalidRecord)
	}
	if err := retired.Target.Validate(); err != nil {
		return fmt.Errorf("retired operation target: %w: %v", ErrInvalidRecord, err)
	}
	if err := retired.Fingerprint.Validate(); err != nil {
		return fmt.Errorf("retired operation fingerprint: %w: %v", ErrInvalidRecord, err)
	}
	if !retired.Reason.Valid() || retired.TerminalSequence == 0 {
		return fmt.Errorf("retired operation terminal metadata is invalid: %w", ErrInvalidRecord)
	}
	return nil
}

// operationIDDigest returns the canonical fixed-size key retained after an
// operation's response and request fingerprint have left the replay window.
func operationIDDigest(id operation.OperationID) string {
	digest := sha256.Sum256([]byte(id))
	return hex.EncodeToString(digest[:])
}

// errorsRetentionPolicy wraps configuration failures in the same invalid-record
// class used before a store accepts any callback or filesystem mutation.
func errorsRetentionPolicy(message string) error {
	return fmt.Errorf("%s: %w", message, ErrInvalidRecord)
}

// stringLower avoids locale-sensitive canonical digest comparisons.
func stringLower(value string) string {
	for index := 0; index < len(value); index++ {
		if value[index] >= 'A' && value[index] <= 'F' {
			copyValue := []byte(value)
			for position, character := range copyValue {
				if character >= 'A' && character <= 'F' {
					copyValue[position] = character + ('a' - 'A')
				}
			}
			return string(copyValue)
		}
	}
	return value
}

// applyRetention assigns deterministic terminal order, converts old unreferenced
// terminal records to fail-closed tombstones, and keeps one contiguous event
// suffix. Live resource inventories pin their exact create-owner records until
// verified deletion removes those receipts; pinned records still count toward
// OperationIdentityLimit. Callers persist the returned candidate atomically or
// discard every change.
func (data *memoryData) applyRetention(policy RetentionPolicy) (bool, error) {
	if data == nil {
		return false, fmt.Errorf("retention data is nil: %w", ErrInvalidRecord)
	}
	if err := policy.Validate(); err != nil {
		return false, err
	}
	data.ensureRetentionMaps()
	changed, err := data.assignTerminalSequences()
	if err != nil {
		return false, err
	}
	if len(data.operations)+len(data.retiredOperations) > policy.OperationIdentityLimit {
		return false, fmt.Errorf("operation identity count exceeds %d: %w", policy.OperationIdentityLimit, ErrRetentionCapacity)
	}
	type terminalCandidate struct {
		id       operation.OperationID
		sequence uint64
	}
	pinned := data.liveInventoryOwnerOperations()
	terminal := make([]terminalCandidate, 0, len(data.terminalOperationSequences))
	for id, sequence := range data.terminalOperationSequences {
		record, exists := data.operations[id]
		if !exists || !record.Operation.State.Terminal() || sequence == 0 {
			return false, fmt.Errorf("terminal retention metadata for %q is inconsistent: %w", id, ErrInvariantViolation)
		}
		if _, retained := pinned[id]; retained {
			continue
		}
		terminal = append(terminal, terminalCandidate{id: id, sequence: sequence})
	}
	sort.Slice(terminal, func(left, right int) bool {
		if terminal[left].sequence == terminal[right].sequence {
			return terminal[left].id < terminal[right].id
		}
		return terminal[left].sequence < terminal[right].sequence
	})
	retireCount := len(terminal) - policy.TerminalOperationLimit
	for index := 0; index < retireCount; index++ {
		candidate := terminal[index]
		record := data.operations[candidate.id]
		digest := operationIDDigest(candidate.id)
		if _, exists := data.retiredOperations[digest]; exists {
			return false, fmt.Errorf("retired operation digest collision for %q: %w", candidate.id, ErrInvariantViolation)
		}
		data.retiredOperations[digest] = retiredOperation{
			OperationIDSHA256: digest,
			Type:              record.Operation.Type, Target: record.Operation.Target, Fingerprint: record.Operation.Fingerprint,
			Reason: record.Operation.Reason, TerminalSequence: candidate.sequence,
			TerminalEventSequence: data.lastEventForOperation(candidate.id),
		}
		delete(data.operations, candidate.id)
		delete(data.terminalOperationSequences, candidate.id)
		changed = true
	}
	if len(data.events) > policy.EventLimit {
		retained := make([]operation.Event, policy.EventLimit)
		start := len(data.events) - policy.EventLimit
		for index := range retained {
			retained[index] = data.events[start+index].Clone()
		}
		data.events = retained
		changed = true
	}
	first := data.nextAvailableEventSequence()
	if data.firstEventSequence != first {
		data.firstEventSequence = first
		changed = true
	}
	return changed, nil
}

// liveInventoryOwnerOperations returns the exact durable operation identities
// referenced by current Sandbox and Container HostResources inventories. The
// resulting pin set is bounded by the store-wide operation identity limit and
// disappears automatically when verified deletion clears the inventory.
func (data memoryData) liveInventoryOwnerOperations() map[operation.OperationID]struct{} {
	pinned := make(map[operation.OperationID]struct{})
	for _, record := range data.sandboxes {
		for _, receipt := range record.HostResources {
			pinned[receipt.Owner.OperationID] = struct{}{}
		}
	}
	for _, record := range data.containerAttempts {
		for _, receipt := range record.HostResources {
			pinned[receipt.Owner.OperationID] = struct{}{}
		}
	}
	return pinned
}

// ensureRetentionMaps initializes additive v2 metadata when opening a v1
// payload or constructing a focused in-memory candidate directly in tests.
func (data *memoryData) ensureRetentionMaps() {
	if data.terminalOperationSequences == nil {
		data.terminalOperationSequences = make(map[operation.OperationID]uint64)
	}
	if data.retiredOperations == nil {
		data.retiredOperations = make(map[string]retiredOperation)
	}
	if data.firstEventSequence == 0 {
		data.firstEventSequence = 1
	}
}

// assignTerminalSequences gives every full terminal record one immutable
// store-local order after the transaction has finalized its replay response.
func (data *memoryData) assignTerminalSequences() (bool, error) {
	missing := make([]operation.OperationID, 0)
	for id, record := range data.operations {
		if record.Operation.State.Terminal() {
			if data.terminalOperationSequences[id] == 0 {
				missing = append(missing, id)
			}
			continue
		}
		if _, exists := data.terminalOperationSequences[id]; exists {
			return false, fmt.Errorf("active operation %q has terminal retention order: %w", id, ErrInvariantViolation)
		}
	}
	if len(missing) == 0 {
		return false, nil
	}
	sort.Slice(missing, func(left, right int) bool {
		leftEvent := data.lastEventForOperation(missing[left])
		rightEvent := data.lastEventForOperation(missing[right])
		if leftEvent != rightEvent {
			if leftEvent == 0 {
				return true
			}
			if rightEvent == 0 {
				return false
			}
			return leftEvent < rightEvent
		}
		return missing[left] < missing[right]
	})
	for _, id := range missing {
		if data.lastTerminalOperationSequence == ^uint64(0) {
			return false, fmt.Errorf("terminal operation sequence overflow: %w", ErrRetentionCapacity)
		}
		data.lastTerminalOperationSequence++
		data.terminalOperationSequences[id] = data.lastTerminalOperationSequence
	}
	return true, nil
}

// lastEventForOperation finds the latest currently retained event for ordering
// migration and tombstone diagnostics; zero means no event remains available.
func (data memoryData) lastEventForOperation(id operation.OperationID) EventSequence {
	for index := len(data.events) - 1; index >= 0; index-- {
		if data.events[index].OperationID == id {
			return data.events[index].Sequence
		}
	}
	return 0
}

// nextAvailableEventSequence returns the first retained event or, for an empty
// stream, the next global sequence without resetting the monotonic counter.
func (data memoryData) nextAvailableEventSequence() EventSequence {
	if len(data.events) > 0 {
		return data.events[0].Sequence
	}
	next, err := data.lastEventSequence.Next()
	if err != nil {
		return data.lastEventSequence
	}
	return next
}

// retiredOperationFor returns the fixed-size tombstone matching an incoming
// ID; a SHA-256 collision fails closed by classifying the new ID as expired.
func (data memoryData) retiredOperationFor(id operation.OperationID) (retiredOperation, bool) {
	retired, exists := data.retiredOperations[operationIDDigest(id)]
	return retired, exists
}
