package lifecycle

import (
	"fmt"

	"mydocker/internal/operation"
)

// OperationFailureError is the bounded caller-visible form of one durably
// failed operation. It deliberately retains only the operation identity and
// low-cardinality reason so transport retries cannot expose provider details.
type OperationFailureError struct {
	OperationID operation.OperationID
	Reason      operation.ReasonClass
}

// Error returns a stable diagnostic while API adapters use Reason for their
// equally stable code and retryability mapping.
func (err *OperationFailureError) Error() string {
	if err == nil {
		return "durable lifecycle operation failed"
	}
	return fmt.Sprintf("operation %q failed with reason %q", err.OperationID, err.Reason)
}

// NewOperationFailureError reconstructs the safe terminal error persisted in
// a failed operation without consulting mutable resource state or event text.
func NewOperationFailureError(value operation.Operation) error {
	if value.State != operation.StateFailed || value.Result != operation.ResultFailed {
		return fmt.Errorf("operation %q is not a terminal failure", value.ID)
	}
	if !value.Reason.Valid() || value.Reason == operation.ReasonNone {
		return fmt.Errorf("operation %q has invalid terminal failure reason %q", value.ID, value.Reason)
	}
	return &OperationFailureError{OperationID: value.ID, Reason: value.Reason}
}
