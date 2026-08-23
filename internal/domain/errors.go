package domain

import (
	"errors"
	"fmt"
)

// ErrorCode is a bounded machine-readable lifecycle failure category.
type ErrorCode string

const (
	// CodeInvalidArgument reports a malformed ID, spec, policy, or outcome.
	CodeInvalidArgument ErrorCode = "invalid_argument"
	// CodeInvalidTransition reports a lifecycle edge not present in the state machine.
	CodeInvalidTransition ErrorCode = "invalid_transition"
	// CodeNotFound reports an absent resource whose prior ownership is not proven.
	CodeNotFound ErrorCode = "not_found"
	// CodeAlreadyExists reports an immutable identity collision.
	CodeAlreadyExists ErrorCode = "already_exists"
	// CodeFailedPrecondition reports a valid request blocked by current lifecycle state.
	CodeFailedPrecondition ErrorCode = "failed_precondition"
	// CodeConflict reports an incompatible operation already active on a resource.
	CodeConflict ErrorCode = "conflict"
	// CodeUnsafeIdentity reports missing strong process-identity evidence.
	CodeUnsafeIdentity ErrorCode = "unsafe_identity"
	// CodeOutcomeUnknown reports that a terminal result has not been verified.
	CodeOutcomeUnknown ErrorCode = "outcome_unknown"
)

// Error carries a stable code while retaining an optional underlying cause.
type Error struct {
	Code    ErrorCode
	Field   string
	Message string
	Cause   error
}

// Error returns a diagnostic message without requiring callers to parse it.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	prefix := string(e.Code)
	if e.Field != "" {
		prefix += " " + e.Field
	}
	if e.Message == "" {
		return prefix
	}
	return prefix + ": " + e.Message
}

// Unwrap exposes the underlying implementation error for errors.Is and errors.As.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// NewError constructs a coded lifecycle error for validation and guards.
func NewError(code ErrorCode, field, message string) error {
	return &Error{Code: code, Field: field, Message: message}
}

// WrapError adds a stable lifecycle code while preserving the original cause.
func WrapError(code ErrorCode, field, message string, cause error) error {
	return &Error{Code: code, Field: field, Message: message, Cause: cause}
}

// IsCode reports whether an error chain contains the requested lifecycle code.
func IsCode(err error, code ErrorCode) bool {
	var coded *Error
	return errors.As(err, &coded) && coded.Code == code
}

// transitionError constructs the uniform diagnostic used by both state machines.
func transitionError(kind string, from, to fmt.Stringer) error {
	return NewError(CodeInvalidTransition, "phase",
		fmt.Sprintf("%s cannot transition from %s to %s", kind, from, to))
}
