package daemon

import (
	"context"
	"errors"

	v1 "mydocker/api/runtime/v1"
	"mydocker/internal/cgroupv2"
	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/lifecycle"
	"mydocker/internal/logstore"
	"mydocker/internal/operation"
	"mydocker/internal/provider"
	"mydocker/internal/rollback"
	"mydocker/internal/shim"
	"mydocker/internal/slim"
	"mydocker/internal/state"
)

// MapError classifies internal error chains into the bounded v1 vocabulary and
// keeps provider, filesystem, and persistence diagnostics out of wire messages.
func MapError(err error) error {
	if err == nil {
		return nil
	}
	var existing *v1.Error
	if errors.As(err, &existing) && existing.Code.Valid() {
		return existing
	}
	if errors.Is(err, context.Canceled) {
		return v1.WrapError(v1.CodeCanceled, "", "request was canceled", false, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return v1.WrapError(v1.CodeDeadlineExceeded, "", "request deadline was exceeded; query or retry with the same operation ID", true, err)
	}
	var domainError *domain.Error
	if errors.As(err, &domainError) {
		return mapDomainError(domainError)
	}
	var operationFailure *lifecycle.OperationFailureError
	if errors.As(err, &operationFailure) {
		return mapOperationFailure(operationFailure)
	}
	if errors.Is(err, operation.ErrBindingMismatch) {
		return v1.WrapError(v1.CodeConflict, "operation_id", "is already bound to different immutable request content", false, err)
	}
	if errors.Is(err, operation.ErrActiveConflict) || errors.Is(err, state.ErrActiveOperation) {
		return v1.WrapError(v1.CodeConflict, "operation_id", "another active operation owns the requested resource", true, err)
	}
	if errors.Is(err, state.ErrOperationExpired) {
		return v1.WrapError(v1.CodeOperationExpired, "operation_id", "the exact terminal response is no longer retained; do not reuse this operation ID", false, err)
	}
	if errors.Is(err, state.ErrEventResumeGap) {
		return v1.WrapError(v1.CodeResumeGap, "resume_token", "the requested position is outside the retained committed event stream; restart with an empty resume token", false, err)
	}
	if errors.Is(err, state.ErrRetentionCapacity) {
		return v1.WrapError(v1.CodeResourceExhausted, "operation_id", "the bounded operation identity capacity is exhausted and requires operator state rotation", false, err)
	}
	if errors.Is(err, state.ErrNotFound) || errors.Is(err, ErrLogNotFound) || errors.Is(err, logstore.ErrNotFound) {
		return v1.WrapError(v1.CodeNotFound, "", "requested resource was not found", false, err)
	}
	if errors.Is(err, state.ErrRevisionConflict) {
		return v1.WrapError(v1.CodeConflict, "", "authoritative state changed concurrently", true, err)
	}
	if errors.Is(err, slim.ErrLauncherIncomplete) {
		return v1.WrapError(v1.CodeUnavailable, "", "Linux workload launcher is not available in this build", false, err)
	}
	var shimError *shim.Error
	if errors.As(err, &shimError) {
		switch shimError.Code {
		case shim.CodeUnavailable, shim.CodePersistenceFailed:
			return v1.WrapError(v1.CodeUnavailable, "", "workload supervisor is temporarily unavailable", true, err)
		case shim.CodeOwnerMismatch:
			return v1.WrapError(v1.CodeUnsafeIdentity, "", "workload supervisor ownership could not be verified", false, err)
		case shim.CodeInvalidArgument, shim.CodeUnsupportedRequest:
			return v1.WrapError(v1.CodeInvalidArgument, "", "workload supervisor request is invalid", false, err)
		case shim.CodeDuplicateRequest:
			return v1.WrapError(v1.CodeConflict, "operation_id", "workload action identity was reused with different content", false, err)
		case shim.CodeNotRunning, shim.CodeAlreadyReleased, shim.CodeWrongMode, shim.CodeStartFailed:
			return v1.WrapError(v1.CodeFailedPrecondition, "", "workload supervisor state does not permit the action", false, err)
		}
	}
	if errors.Is(err, state.ErrInvalidEventLimit) || errors.Is(err, logstore.ErrInvalidLimit) {
		return v1.WrapError(v1.CodeInvalidArgument, "limit", "must not be negative", false, err)
	}
	if errors.Is(err, state.ErrFileStoreLocked) || errors.Is(err, state.ErrFileStoreClosed) {
		return v1.WrapError(v1.CodeUnavailable, "", "daemon state storage is unavailable", true, err)
	}
	if errors.Is(err, state.ErrDurabilityUncertain) {
		return v1.WrapError(v1.CodeUnavailable, "", "state durability is uncertain; the daemon must reopen and reconcile before retry", true, err)
	}
	if errors.Is(err, state.ErrUnsupportedSchema) || errors.Is(err, state.ErrInvalidRecord) || errors.Is(err, state.ErrInvariantViolation) {
		return v1.WrapError(v1.CodeInternal, "", "durable state cannot be interpreted safely", false, err)
	}
	if errors.Is(err, lifecycle.ErrProcessVerificationUnavailable) || errors.Is(err, isolation.ErrUnsafeIdentity) ||
		errors.Is(err, isolation.ErrUnsafePath) || errors.Is(err, cgroupv2.ErrOutsideRoot) ||
		errors.Is(err, provider.ErrRollbackOwnerMismatch) {
		return v1.WrapError(v1.CodeUnsafeIdentity, "", "strong host resource ownership could not be verified", false, err)
	}
	if errors.Is(err, lifecycle.ErrVerificationRequired) || errors.Is(err, lifecycle.ErrOperationType) ||
		errors.Is(err, isolation.ErrPreflight) || errors.Is(err, isolation.ErrUnsupportedPlatform) ||
		errors.Is(err, cgroupv2.ErrUnsupported) || errors.Is(err, cgroupv2.ErrPopulated) ||
		errors.Is(err, cgroupv2.ErrBusy) || errors.Is(err, cgroupv2.ErrEffectiveMismatch) {
		return v1.WrapError(v1.CodeFailedPrecondition, "", "runtime preconditions or verified lifecycle evidence are not satisfied", false, err)
	}
	if errors.Is(err, cgroupv2.ErrUnknownState) || errors.Is(err, isolation.ErrClosed) ||
		provider.IsObservationUnavailable(err) ||
		errors.Is(err, logstore.ErrClosed) || errors.Is(err, logstore.ErrAppendUnavailable) ||
		errors.Is(err, logstore.ErrReadUnavailable) || errors.Is(err, logstore.ErrInUse) || errors.Is(err, ErrLogRegistryUnavailable) {
		return v1.WrapError(v1.CodeUnavailable, "", "runtime evidence or output storage is temporarily unavailable", true, err)
	}
	if errors.Is(err, logstore.ErrIdentityMismatch) || errors.Is(err, logstore.ErrCorrupt) ||
		errors.Is(err, logstore.ErrUnsupportedSchema) || errors.Is(err, logstore.ErrUnsafePath) ||
		errors.Is(err, ErrLogAlreadyRegistered) || errors.Is(err, ErrLogRegistrationUnsafe) {
		return v1.WrapError(v1.CodeInternal, "logs", "workload output cannot be interpreted safely", false, err)
	}
	if errors.Is(err, provider.ErrUnknownRollbackRoute) || errors.Is(err, isolation.ErrWrongThread) {
		return v1.WrapError(v1.CodeInternal, "", "runtime provider wiring rejected the operation", false, err)
	}
	var rollbackError *rollback.Error
	if errors.As(err, &rollbackError) {
		return v1.WrapError(v1.CodeUnavailable, "", "runtime cleanup remains incomplete and will require reconciliation", true, err)
	}
	return v1.WrapError(v1.CodeInternal, "", "internal server error", false, err)
}

// mapOperationFailure makes the persisted reason class authoritative for API
// error replay before and after daemon restart; terminal same-ID failures are
// never advertised as retryable because exact replay cannot change them.
func mapOperationFailure(err *lifecycle.OperationFailureError) error {
	if err == nil {
		return v1.NewError(v1.CodeInternal, "", "internal server error")
	}
	switch err.Reason {
	case operation.ReasonInvalidRequest:
		return v1.WrapError(v1.CodeInvalidArgument, "", "the durable operation request is invalid", false, err)
	case operation.ReasonNotFound:
		return v1.WrapError(v1.CodeNotFound, "", "the durable operation target was not found", false, err)
	case operation.ReasonPrecondition:
		return v1.WrapError(v1.CodeFailedPrecondition, "", "the durable operation precondition was not satisfied", false, err)
	case operation.ReasonConflict:
		return v1.WrapError(v1.CodeConflict, "operation_id", "the durable operation conflicted with another active intent", false, err)
	case operation.ReasonCleanup:
		return v1.WrapError(v1.CodeUnavailable, "", "the durable operation could not verify complete runtime cleanup", false, err)
	case operation.ReasonInternal:
		return v1.WrapError(v1.CodeInternal, "", "the durable operation failed internally", false, err)
	default:
		return v1.WrapError(v1.CodeInternal, "", "internal server error", false, err)
	}
}

// mapDomainError preserves stable lifecycle fields while translating the one internal transition-only category.
func mapDomainError(err *domain.Error) error {
	if err == nil {
		return v1.NewError(v1.CodeInternal, "", "internal server error")
	}
	message := err.Message
	if message == "" {
		message = string(err.Code)
	}
	switch err.Code {
	case domain.CodeInvalidArgument:
		return v1.WrapError(v1.CodeInvalidArgument, err.Field, message, false, err)
	case domain.CodeInvalidTransition:
		return v1.WrapError(v1.CodeFailedPrecondition, err.Field, "lifecycle transition is not allowed", false, err)
	case domain.CodeNotFound:
		return v1.WrapError(v1.CodeNotFound, err.Field, "requested resource was not found", false, err)
	case domain.CodeAlreadyExists:
		return v1.WrapError(v1.CodeAlreadyExists, err.Field, message, false, err)
	case domain.CodeFailedPrecondition:
		return v1.WrapError(v1.CodeFailedPrecondition, err.Field, message, false, err)
	case domain.CodeConflict:
		return v1.WrapError(v1.CodeConflict, err.Field, message, false, err)
	case domain.CodeUnsafeIdentity:
		return v1.WrapError(v1.CodeUnsafeIdentity, err.Field, "strong process ownership could not be verified", false, err)
	case domain.CodeOutcomeUnknown:
		return v1.WrapError(v1.CodeOutcomeUnknown, err.Field, "trustworthy terminal outcome evidence is unavailable", false, err)
	default:
		return v1.WrapError(v1.CodeInternal, "", "internal server error", false, err)
	}
}
