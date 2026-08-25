package server

import (
	"errors"
	"math"
	"testing"

	v1 "mydocker/api/runtime/v1"
)

// requireInternalPaginationError verifies a malformed service page fails as a
// bounded server fault rather than becoming a client-visible resume cursor.
func requireInternalPaginationError(t *testing.T, err error) {
	t.Helper()
	var apiError *v1.Error
	if !errors.As(err, &apiError) || apiError.Code != v1.CodeInternal {
		t.Fatalf("pagination error = %v, want v1 internal", err)
	}
}

// TestValidateEventPageRequiresContiguousResume verifies retention may choose
// the first suffix only at cursor zero; resumed and subsequent events cannot skip.
func TestValidateEventPageRequiresContiguousResume(t *testing.T) {
	if err := validateEventPage(0, []v1.Event{{Sequence: 41}, {Sequence: 42}}); err != nil {
		t.Fatalf("validateEventPage(initial retained suffix) error = %v", err)
	}
	if err := validateEventPage(7, []v1.Event{{Sequence: 8}, {Sequence: 9}}); err != nil {
		t.Fatalf("validateEventPage(contiguous resume) error = %v", err)
	}
	requireInternalPaginationError(t, validateEventPage(7, []v1.Event{{Sequence: 9}}))
	requireInternalPaginationError(t, validateEventPage(7, []v1.Event{{Sequence: 8}, {Sequence: 10}}))
}

// TestValidateLogPageRequiresExactNextCursor verifies workload logs, which
// have no retained-suffix exception, always begin at and continue with after+1.
func TestValidateLogPageRequiresExactNextCursor(t *testing.T) {
	input := v1.ListLogsRequest{ContainerID: "container-page", AttemptID: "attempt-page", AfterCursor: 7, Limit: 2}
	valid := []v1.LogFrame{
		newTestLogFrame(input.ContainerID, input.AttemptID, "stdout", 8, 1, []byte("one")),
		newTestLogFrame(input.ContainerID, input.AttemptID, "stderr", 9, 1, []byte("two")),
	}
	if err := validateLogPage(input, valid); err != nil {
		t.Fatalf("validateLogPage(contiguous) error = %v", err)
	}
	skippedFirst := append([]v1.LogFrame(nil), valid...)
	skippedFirst[0] = newTestLogFrame(input.ContainerID, input.AttemptID, "stdout", 9, 1, []byte("one"))
	requireInternalPaginationError(t, validateLogPage(input, skippedFirst[:1]))
	skippedMiddle := append([]v1.LogFrame(nil), valid...)
	skippedMiddle[1] = newTestLogFrame(input.ContainerID, input.AttemptID, "stderr", 10, 1, []byte("two"))
	requireInternalPaginationError(t, validateLogPage(input, skippedMiddle))
	overflowInput := input
	overflowInput.AfterCursor = math.MaxUint64
	wrapped := newTestLogFrame(input.ContainerID, input.AttemptID, "stdout", 0, 1, []byte("wrapped"))
	requireInternalPaginationError(t, validateLogPage(overflowInput, []v1.LogFrame{wrapped}))
}
