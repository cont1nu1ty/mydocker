package state

import (
	"errors"
	"fmt"
)

var (
	// ErrNotFound reports that the requested durable record is absent.
	ErrNotFound = errors.New("state record not found")
	// ErrRevisionConflict reports a stale or otherwise invalid CAS revision.
	ErrRevisionConflict = errors.New("state revision conflict")
	// ErrUnsupportedSchema reports a record whose persisted schema has no explicit reader or migration path.
	ErrUnsupportedSchema = errors.New("unsupported state schema")
	// ErrInvalidRecord reports a malformed record that cannot safely be stored.
	ErrInvalidRecord = errors.New("invalid state record")
	// ErrActiveOperation reports that another unfinished operation owns a target.
	ErrActiveOperation = errors.New("active operation already exists for target")
	// ErrInvalidEventLimit reports a negative event page limit.
	ErrInvalidEventLimit = errors.New("event limit must not be negative")
	// ErrInvariantViolation reports persisted state that violates a store invariant.
	ErrInvariantViolation = errors.New("state invariant violation")
	// ErrNilCallback reports a Store call that cannot execute transaction work.
	ErrNilCallback = errors.New("state callback must not be nil")
	// ErrTransactionClosed reports use of a callback-scoped view after return.
	ErrTransactionClosed = errors.New("state transaction is closed")
	// ErrFileStoreLocked reports that another daemon instance owns the durable state path.
	ErrFileStoreLocked = errors.New("state file store is already locked")
	// ErrFileStoreClosed reports use of a FileStore after its process lock was released.
	ErrFileStoreClosed = errors.New("state file store is closed")
	// ErrDurabilityUncertain reports that rename completed but directory durability was not confirmed.
	ErrDurabilityUncertain = errors.New("state file durability is uncertain")
	// ErrOperationExpired reports an operation ID whose exact response left the replay window but whose tombstone still forbids reuse.
	ErrOperationExpired = errors.New("operation replay window expired")
	// ErrEventResumeGap reports a non-empty resume position outside the retained committed event window.
	ErrEventResumeGap = errors.New("event resume position is outside the retained stream")
	// ErrRetentionCapacity reports that accepting another identity would exceed the fail-closed bounded history.
	ErrRetentionCapacity = errors.New("state retention capacity exhausted")
)

// EventResumeGapError carries the first retained sequence for internal
// diagnostics while API mapping exposes only the stable resume-gap category.
type EventResumeGapError struct {
	Requested      EventSequence
	FirstAvailable EventSequence
	LastAvailable  EventSequence
}

// Error describes the compacted prefix without changing the stable sentinel classification.
func (err *EventResumeGapError) Error() string {
	if err == nil {
		return ErrEventResumeGap.Error()
	}
	if err.Requested > err.LastAvailable {
		return fmt.Sprintf("event resume sequence %d exceeds last committed sequence %d", err.Requested, err.LastAvailable)
	}
	return fmt.Sprintf("event resume sequence %d precedes first available sequence %d", err.Requested, err.FirstAvailable)
}

// Is lets errors.Is classify every event-prefix miss as ErrEventResumeGap.
func (err *EventResumeGapError) Is(target error) bool {
	return target == ErrEventResumeGap
}
