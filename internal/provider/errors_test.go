package provider

import (
	"errors"
	"testing"
)

// TestNoEffectDispositionPreservesCause verifies engines can distinguish a proven pre-effect failure without losing its diagnostic type.
func TestNoEffectDispositionPreservesCause(t *testing.T) {
	cause := errors.New("preflight rejected")
	err := MarkNoEffect(cause)
	if !IsNoEffect(err) || !errors.Is(err, cause) {
		t.Fatalf("MarkNoEffect() = %v, want disposition and original cause", err)
	}
	if IsNoEffect(cause) {
		t.Fatal("IsNoEffect() accepted an unclassified error")
	}
}

// TestRollbackRequiredDispositionPreservesCause verifies a caller can begin
// cleanup of prior checkpointed owners without misclassifying a partial effect as absent.
func TestRollbackRequiredDispositionPreservesCause(t *testing.T) {
	cause := errors.New("pivot failed after changing mount state")
	err := MarkRollbackRequired(cause)
	if !IsRollbackRequired(err) || !errors.Is(err, cause) {
		t.Fatalf("MarkRollbackRequired() = %v, want disposition and original cause", err)
	}
	if IsNoEffect(err) {
		t.Fatal("rollback-required failure was misclassified as no-effect")
	}
	if IsRollbackRequired(cause) {
		t.Fatal("IsRollbackRequired() accepted an unclassified error")
	}
}
