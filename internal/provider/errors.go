package provider

import (
	"errors"
	"fmt"
)

var (
	// ErrProcessNotRunning reports that a verified wrapper no longer has a live workload child to signal.
	ErrProcessNotRunning = errors.New("verified workload process is not running")
)

// ObservationUnavailableError marks a read-only provider observation that is
// temporarily unavailable without claiming that the owned resource is absent.
type ObservationUnavailableError struct {
	Cause error
}

// Error returns a stable disposition plus the local diagnostic retained for logs.
func (err *ObservationUnavailableError) Error() string {
	if err == nil || err.Cause == nil {
		return "provider observation is temporarily unavailable"
	}
	return fmt.Sprintf("provider observation is temporarily unavailable: %v", err.Cause)
}

// Unwrap preserves a typed transport cause without authorizing absence or cleanup.
func (err *ObservationUnavailableError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

// MarkObservationUnavailable converts a failed read into a non-authorizing
// recovery disposition that callers may persist as PresenceUnknown.
func MarkObservationUnavailable(cause error) error {
	if cause == nil {
		cause = errors.New("unspecified observation failure")
	}
	return &ObservationUnavailableError{Cause: cause}
}

// IsObservationUnavailable reports whether a provider explicitly classified
// a read failure as temporary rather than absent or identity-unsafe.
func IsObservationUnavailable(err error) bool {
	var unavailable *ObservationUnavailableError
	return errors.As(err, &unavailable)
}

// NoEffectError proves that a failed provider call completed before acquiring or changing any host resource.
// Engines may roll back earlier checkpointed acquisitions only when this explicit disposition is present.
type NoEffectError struct {
	Cause error
}

// Error returns the underlying diagnostic without changing its bounded provider disposition.
func (err *NoEffectError) Error() string {
	if err == nil || err.Cause == nil {
		return "provider call failed before any host effect"
	}
	return fmt.Sprintf("provider call failed before any host effect: %v", err.Cause)
}

// Unwrap preserves the underlying typed cause for diagnostics and API error mapping.
func (err *NoEffectError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

// MarkNoEffect attaches the only disposition that permits automatic rollback after a provider acquisition error.
func MarkNoEffect(cause error) error {
	if cause == nil {
		cause = errors.New("unspecified provider failure")
	}
	return &NoEffectError{Cause: cause}
}

// IsNoEffect reports whether a provider explicitly proved that the failed call made no host change.
func IsNoEffect(err error) bool {
	var noEffect *NoEffectError
	return errors.As(err, &noEffect)
}

// RollbackRequiredError proves that a failed acquisition may have changed host
// state, but every possible effect remains contained by resources whose inverse
// actions were durably checkpointed before the call.
type RollbackRequiredError struct {
	Cause error
}

// Error returns the underlying diagnostic while preserving the cleanup-required disposition.
func (err *RollbackRequiredError) Error() string {
	if err == nil || err.Cause == nil {
		return "provider call failed after a rollback-contained host effect"
	}
	return fmt.Sprintf("provider call failed after a rollback-contained host effect: %v", err.Cause)
}

// Unwrap preserves the provider or supervisor cause for diagnostics and stable API classification.
func (err *RollbackRequiredError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

// MarkRollbackRequired authorizes rollback of previously checkpointed owners
// without claiming that the failing acquisition itself had no side effect.
func MarkRollbackRequired(cause error) error {
	if cause == nil {
		cause = errors.New("unspecified rollback-contained provider failure")
	}
	return &RollbackRequiredError{Cause: cause}
}

// IsRollbackRequired reports whether a provider explicitly proved that prior
// checkpointed owner cleanup contains every possible effect of the failed call.
func IsRollbackRequired(err error) bool {
	var rollbackRequired *RollbackRequiredError
	return errors.As(err, &rollbackRequired)
}
