package v1

import (
	"errors"
	"fmt"
	"net/http"
)

// ErrorCode is the bounded machine-readable v1 failure vocabulary.
type ErrorCode string

const (
	// CodeInvalidArgument reports malformed input or a non-canonical query.
	CodeInvalidArgument ErrorCode = "invalid_argument"
	// CodeUnsupportedVersion reports a request outside the /v1 API boundary.
	CodeUnsupportedVersion ErrorCode = "unsupported_version"
	// CodeMethodNotAllowed reports a known path used with an unsupported method.
	CodeMethodNotAllowed ErrorCode = "method_not_allowed"
	// CodeRequestTooLarge reports a body larger than the configured server limit.
	CodeRequestTooLarge ErrorCode = "request_too_large"
	// CodeNotFound reports an absent resource or an operation identity the daemon never retained.
	CodeNotFound ErrorCode = "not_found"
	// CodeAlreadyExists reports an immutable identity collision.
	CodeAlreadyExists ErrorCode = "already_exists"
	// CodeFailedPrecondition reports lifecycle state that blocks a valid request.
	CodeFailedPrecondition ErrorCode = "failed_precondition"
	// CodeConflict reports an incompatible active operation or operation-ID reuse.
	CodeConflict ErrorCode = "conflict"
	// CodeOperationExpired reports a remembered operation whose exact response left the replay window.
	CodeOperationExpired ErrorCode = "operation_expired"
	// CodeResumeGap reports an event or log cursor outside the committed resumable boundary.
	CodeResumeGap ErrorCode = "resume_gap"
	// CodeResourceExhausted reports a hard bounded-state capacity that requires operator action.
	CodeResourceExhausted ErrorCode = "resource_exhausted"
	// CodeUnsafeIdentity reports missing strong process ownership evidence.
	CodeUnsafeIdentity ErrorCode = "unsafe_identity"
	// CodeOutcomeUnknown reports unavailable trustworthy terminal process facts.
	CodeOutcomeUnknown ErrorCode = "outcome_unknown"
	// CodeCanceled reports caller cancellation before a result could be returned.
	CodeCanceled ErrorCode = "canceled"
	// CodeDeadlineExceeded reports the configured request deadline expiring.
	CodeDeadlineExceeded ErrorCode = "deadline_exceeded"
	// CodeUnavailable reports a temporary daemon or dependency outage.
	CodeUnavailable ErrorCode = "unavailable"
	// CodeInternal reports a server failure whose implementation detail is not exposed.
	CodeInternal ErrorCode = "internal"
)

// Error is the in-process typed failure returned by a Service implementation.
type Error struct {
	Code      ErrorCode
	Field     string
	Message   string
	Retryable bool
	Cause     error
}

// Error returns a diagnostic string while callers rely on Code for behavior.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := string(e.Code)
	if e.Field != "" {
		message += " " + e.Field
	}
	if e.Message != "" {
		message += ": " + e.Message
	}
	return message
}

// Unwrap preserves an internal cause for errors.Is and errors.As without putting it on the wire.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// NewError constructs a safe typed service or validation failure.
func NewError(code ErrorCode, field, message string) error {
	return &Error{Code: code, Field: field, Message: message}
}

// WrapError retains a private cause while supplying a stable public classification.
func WrapError(code ErrorCode, field, message string, retryable bool, cause error) error {
	return &Error{Code: code, Field: field, Message: message, Retryable: retryable, Cause: cause}
}

// ErrorDetail is the stable JSON projection of a typed API failure.
type ErrorDetail struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	Field     string    `json:"field,omitempty"`
	Retryable bool      `json:"retryable"`
}

// ErrorEnvelope correlates a wire failure with its transport and durable operation identities.
type ErrorEnvelope struct {
	Error       ErrorDetail `json:"error"`
	RequestID   string      `json:"request_id,omitempty"`
	OperationID string      `json:"operation_id,omitempty"`
}

// Validate rejects an unknown or internally inconsistent error envelope received from a daemon.
func (e ErrorEnvelope) Validate() error {
	if !e.Error.Code.Valid() {
		return fmt.Errorf("unknown v1 error code %q", e.Error.Code)
	}
	if e.Error.Message == "" {
		return errors.New("v1 error message must not be empty")
	}
	return nil
}

// Valid reports whether a code belongs to the stable v1 error vocabulary.
func (c ErrorCode) Valid() bool {
	switch c {
	case CodeInvalidArgument, CodeUnsupportedVersion, CodeMethodNotAllowed,
		CodeRequestTooLarge, CodeNotFound, CodeAlreadyExists, CodeFailedPrecondition,
		CodeConflict, CodeOperationExpired, CodeResumeGap, CodeResourceExhausted,
		CodeUnsafeIdentity, CodeOutcomeUnknown, CodeCanceled,
		CodeDeadlineExceeded, CodeUnavailable, CodeInternal:
		return true
	default:
		return false
	}
}

// HTTPStatus maps a stable error code to its v1 HTTP transport status.
func HTTPStatus(code ErrorCode) int {
	switch code {
	case CodeInvalidArgument:
		return http.StatusBadRequest
	case CodeUnsupportedVersion, CodeNotFound:
		return http.StatusNotFound
	case CodeMethodNotAllowed:
		return http.StatusMethodNotAllowed
	case CodeRequestTooLarge:
		return http.StatusRequestEntityTooLarge
	case CodeAlreadyExists, CodeConflict:
		return http.StatusConflict
	case CodeOperationExpired, CodeResumeGap:
		return http.StatusGone
	case CodeResourceExhausted:
		return http.StatusInsufficientStorage
	case CodeFailedPrecondition, CodeUnsafeIdentity, CodeOutcomeUnknown:
		return http.StatusPreconditionFailed
	case CodeCanceled:
		return 499
	case CodeDeadlineExceeded:
		return http.StatusGatewayTimeout
	case CodeUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// ExitStatus maps API outcomes to stable CLI process statuses without importing command code.
func ExitStatus(code ErrorCode) int {
	switch code {
	case "":
		return 0
	case CodeInvalidArgument, CodeUnsupportedVersion, CodeMethodNotAllowed, CodeRequestTooLarge:
		return 2
	case CodeNotFound:
		return 3
	case CodeAlreadyExists, CodeFailedPrecondition, CodeConflict, CodeOperationExpired, CodeResumeGap,
		CodeUnsafeIdentity, CodeOutcomeUnknown:
		return 4
	case CodeCanceled, CodeDeadlineExceeded, CodeUnavailable, CodeResourceExhausted:
		return 5
	default:
		return 1
	}
}

// ErrorDetailFrom converts an error chain to a safe wire detail and hides unknown causes.
func ErrorDetailFrom(err error) ErrorDetail {
	var typed *Error
	if errors.As(err, &typed) && typed.Code.Valid() {
		message := typed.Message
		if message == "" {
			message = string(typed.Code)
		}
		return ErrorDetail{Code: typed.Code, Field: typed.Field, Message: message, Retryable: typed.Retryable}
	}
	return ErrorDetail{Code: CodeInternal, Message: "internal server error"}
}
