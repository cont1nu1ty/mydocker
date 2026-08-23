package operation

import (
	"errors"
	"fmt"
)

// Binding is the immutable identity tuple permanently associated with one
// operation ID during the operation retention window.
type Binding struct {
	ID          OperationID
	Type        Type
	Target      Target
	Fingerprint RequestFingerprint
}

// Validate rejects an incomplete operation binding before resolution.
func (b Binding) Validate() error {
	if err := b.ID.Validate(); err != nil {
		return err
	}
	if !b.Type.Valid() {
		return fmt.Errorf("invalid operation type %q", b.Type)
	}
	if err := b.Target.Validate(); err != nil {
		return err
	}
	if err := validateTypeTarget(b.Type, b.Target.Kind); err != nil {
		return err
	}
	return b.Fingerprint.Validate()
}

// Binding returns the immutable identity tuple stored by this operation.
func (o Operation) Binding() Binding {
	return Binding{
		ID:          o.ID,
		Type:        o.Type,
		Target:      o.Target,
		Fingerprint: o.Fingerprint,
	}
}

// Resolution is the deterministic action selected for an incoming request.
type Resolution string

const (
	// ResolutionNew persists and starts an operation that has no existing record.
	ResolutionNew Resolution = "new"
	// ResolutionResume continues reconciliation of the existing active record.
	ResolutionResume Resolution = "resume"
	// ResolutionReplay returns the terminal result without repeating side effects.
	ResolutionReplay Resolution = "replay"
)

// Valid reports whether a resolution belongs to the bounded retry vocabulary.
func (r Resolution) Valid() bool {
	switch r {
	case ResolutionNew, ResolutionResume, ResolutionReplay:
		return true
	default:
		return false
	}
}

// BindingMismatchError reports reuse of an operation ID with different
// immutable request content.
type BindingMismatchError struct {
	ID        OperationID
	Field     string
	Existing  string
	Requested string
}

// Error describes the mismatched binding field without exposing request bodies.
func (e *BindingMismatchError) Error() string {
	return fmt.Sprintf("operation ID %q is already bound to a different %s (existing %q, requested %q)",
		e.ID, e.Field, e.Existing, e.Requested)
}

// Is lets errors.Is classify all binding mismatches by a sentinel target.
func (e *BindingMismatchError) Is(target error) bool {
	return target == ErrBindingMismatch
}

// ErrBindingMismatch classifies operation ID reuse with a different binding.
var ErrBindingMismatch = errors.New("operation ID binding mismatch")

// ActiveConflictError reports a different active operation on the same target.
type ActiveConflictError struct {
	Target      Target
	ActiveID    OperationID
	RequestedID OperationID
}

// Error describes the conflicting identities and resource target.
func (e *ActiveConflictError) Error() string {
	return fmt.Sprintf("target %s/%s has active operation %q; requested operation %q conflicts",
		e.Target.Kind, e.Target.ID, e.ActiveID, e.RequestedID)
}

// Is lets errors.Is classify all active target conflicts by a sentinel target.
func (e *ActiveConflictError) Is(target error) bool {
	return target == ErrActiveConflict
}

// ErrActiveConflict classifies a different active operation on the same target.
var ErrActiveConflict = errors.New("active operation conflict")

// Resolve selects New, Resume, or Replay by comparing an optional durable
// record with the incoming immutable binding.
func Resolve(existing *Operation, requested Binding) (Resolution, error) {
	if err := requested.Validate(); err != nil {
		return "", fmt.Errorf("validate requested operation binding: %w", err)
	}
	if existing == nil {
		return ResolutionNew, nil
	}
	if err := existing.Validate(); err != nil {
		return "", fmt.Errorf("validate existing operation: %w", err)
	}
	if err := compareBinding(existing.Binding(), requested); err != nil {
		return "", err
	}
	if existing.State.Active() {
		return ResolutionResume, nil
	}
	return ResolutionReplay, nil
}

// CheckActiveConflict rejects a different active operation on the requested
// target while allowing retries of the same operation ID.
func CheckActiveConflict(active *Operation, requested Binding) error {
	if err := requested.Validate(); err != nil {
		return fmt.Errorf("validate requested operation binding: %w", err)
	}
	if active == nil {
		return nil
	}
	if err := active.Validate(); err != nil {
		return fmt.Errorf("validate active operation: %w", err)
	}
	if !active.State.Active() || !active.Target.Equal(requested.Target) || active.ID == requested.ID {
		return nil
	}
	return &ActiveConflictError{
		Target:      requested.Target,
		ActiveID:    active.ID,
		RequestedID: requested.ID,
	}
}

// compareBinding pinpoints the first immutable binding field changed by a retry.
func compareBinding(existing, requested Binding) error {
	if existing.ID != requested.ID {
		return &BindingMismatchError{
			ID: requested.ID, Field: "operation_id",
			Existing: string(existing.ID), Requested: string(requested.ID),
		}
	}
	if existing.Type != requested.Type {
		return &BindingMismatchError{
			ID: requested.ID, Field: "operation_type",
			Existing: string(existing.Type), Requested: string(requested.Type),
		}
	}
	if !existing.Target.Equal(requested.Target) {
		return &BindingMismatchError{
			ID: requested.ID, Field: "target",
			Existing:  existing.Target.Kind.String() + "/" + existing.Target.ID,
			Requested: requested.Target.Kind.String() + "/" + requested.Target.ID,
		}
	}
	if !existing.Fingerprint.Equal(requested.Fingerprint) {
		return &BindingMismatchError{
			ID: requested.ID, Field: "request_fingerprint",
			Existing: existing.Fingerprint.SHA256, Requested: requested.Fingerprint.SHA256,
		}
	}
	return nil
}

// String returns the stable persistence spelling of a target kind.
func (k TargetKind) String() string {
	return string(k)
}
