package client

import (
	"context"
	"errors"
	"fmt"

	v1 "mydocker/api/runtime/v1"
)

// RemoteError is a validated non-success response returned by mydockerd.
type RemoteError struct {
	StatusCode int
	Envelope   v1.ErrorEnvelope
}

// Error returns a correlation-friendly diagnostic while behavior keys off Code.
func (e *RemoteError) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := fmt.Sprintf("mydocker API %s: %s", e.Envelope.Error.Code, e.Envelope.Error.Message)
	if e.Envelope.Error.Field != "" {
		message += " (field " + e.Envelope.Error.Field + ")"
	}
	return message
}

// Code returns the stable server classification suitable for programmatic handling.
func (e *RemoteError) Code() v1.ErrorCode {
	if e == nil {
		return v1.CodeInternal
	}
	return e.Envelope.Error.Code
}

// ExitStatus returns the stable CLI mapping for this remote failure.
func (e *RemoteError) ExitStatus() int {
	return v1.ExitStatus(e.Code())
}

// TransportError reports that no valid API response was received after bounded retries.
type TransportError struct {
	Cause error
}

// Error returns a diagnostic without changing the stable unavailable classification.
func (e *TransportError) Error() string {
	if e == nil || e.Cause == nil {
		return "mydocker API transport unavailable"
	}
	return "mydocker API transport unavailable: " + e.Cause.Error()
}

// Unwrap preserves the dial or request error for errors.Is and errors.As.
func (e *TransportError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// CodeOf maps remote, transport, deadline, and cancellation failures to the v1 vocabulary.
func CodeOf(err error) v1.ErrorCode {
	if err == nil {
		return ""
	}
	var remote *RemoteError
	if errors.As(err, &remote) {
		return remote.Code()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return v1.CodeDeadlineExceeded
	}
	if errors.Is(err, context.Canceled) {
		return v1.CodeCanceled
	}
	var transport *TransportError
	if errors.As(err, &transport) {
		return v1.CodeUnavailable
	}
	var local *v1.Error
	if errors.As(err, &local) && local.Code.Valid() {
		return local.Code
	}
	return v1.CodeInternal
}

// IsOperationExpired reports that the daemon remembers an operation ID but can
// no longer replay its terminal response; callers must not resubmit that ID.
func IsOperationExpired(err error) bool {
	return CodeOf(err) == v1.CodeOperationExpired
}

// IsResumeGap reports that an event or workload-log cursor is outside committed
// history so a caller can deliberately restart observation from an empty token.
func IsResumeGap(err error) bool {
	return CodeOf(err) == v1.CodeResumeGap
}

// IsResourceExhausted reports that bounded daemon state needs operator rotation
// rather than automatic retry with another operation identity.
func IsResourceExhausted(err error) bool {
	return CodeOf(err) == v1.CodeResourceExhausted
}

// ExitStatus maps any client failure to the stable v1 command status contract.
func ExitStatus(err error) int {
	return v1.ExitStatus(CodeOf(err))
}
