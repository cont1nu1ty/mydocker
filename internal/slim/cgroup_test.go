package slim

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"mydocker/internal/cgroupv2"
	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
)

// TestCgroupReceiptRoundTripAndTamperRejection verifies provider recovery uses canonical typed identity rather than receipt-controlled paths.
func TestCgroupReceiptRoundTripAndTamperRejection(t *testing.T) {
	sandboxOwner, err := ownership.NewOwnerKey("op-sandbox", operation.Target{Kind: operation.TargetSandbox, ID: "sandbox-one"}, 1)
	if err != nil {
		t.Fatalf("NewOwnerKey(sandbox) error = %v", err)
	}
	sandboxReceipt, err := newCgroupReceipt(sandboxOwner, ownership.KindSandboxCgroup, "sandbox-one", "", nil)
	if err != nil {
		t.Fatalf("newCgroupReceipt(sandbox) error = %v", err)
	}
	if _, err := validateCgroupReceipt(provider.OwnedReceiptRequest{Owner: sandboxOwner, Receipt: sandboxReceipt}, ownership.KindSandboxCgroup); err != nil {
		t.Fatalf("validateCgroupReceipt(sandbox) error = %v", err)
	}
	containerOwner, err := ownership.NewOwnerKey("op-container", operation.Target{Kind: operation.TargetContainer, ID: "container-one"}, 1)
	if err != nil {
		t.Fatalf("NewOwnerKey(container) error = %v", err)
	}
	effective := cgroupv2.EffectiveLimits{
		CPU:    cgroupv2.CPUMax{Unlimited: true, PeriodMicros: cgroupv2.CPUPeriodMicros},
		Memory: cgroupv2.ScalarLimit{Unlimited: true}, Pids: cgroupv2.ScalarLimit{Value: 1024},
	}
	attemptReceipt, err := newCgroupReceipt(containerOwner, ownership.KindAttemptCgroup, "sandbox-one", "attempt-one", &effective)
	if err != nil {
		t.Fatalf("newCgroupReceipt(attempt) error = %v", err)
	}
	scope, err := validateCgroupReceipt(provider.OwnedReceiptRequest{Owner: containerOwner, Receipt: attemptReceipt}, ownership.KindAttemptCgroup)
	if err != nil || scope.sandboxID != domain.SandboxID("sandbox-one") || scope.attemptID != domain.AttemptID("attempt-one") || !scope.effective.Equal(effective) {
		t.Fatalf("validateCgroupReceipt(attempt) = (%#v, %v)", scope, err)
	}
	mutations := []func(*ownership.Receipt){
		func(receipt *ownership.Receipt) { receipt.Attributes[attributeAttemptID] = "attempt-other" },
		func(receipt *ownership.Receipt) { receipt.LocalID += "-changed" },
		func(receipt *ownership.Receipt) { receipt.EvidenceSHA256 = string(make([]byte, 64)) },
	}
	for index, mutate := range mutations {
		candidate := attemptReceipt.Clone()
		mutate(&candidate)
		if _, err := validateCgroupReceipt(provider.OwnedReceiptRequest{Owner: containerOwner, Receipt: candidate}, ownership.KindAttemptCgroup); err == nil {
			t.Fatalf("tampered receipt %d unexpectedly validated", index)
		}
	}
}

// TestCgroupObservationHelpersPreserveFailClosedSemantics verifies verified absence and post-remove disposition remain explicit.
func TestCgroupObservationHelpersPreserveFailClosedSemantics(t *testing.T) {
	absent, err := cgroupPresenceObservation(false, "", nil)
	if err != nil || absent.Presence != provider.PresenceAbsent || !absent.Verified {
		t.Fatalf("cgroupPresenceObservation(absent) = (%#v, %v)", absent, err)
	}
	removed, err := verifyCgroupRemoval(context.Background(), true, func() (bool, error) { return false, nil })
	if err != nil || removed.Disposition != provider.CleanupRemoved {
		t.Fatalf("verifyCgroupRemoval(removed) = (%#v, %v)", removed, err)
	}
	alreadyAbsent, err := verifyCgroupRemoval(context.Background(), false, func() (bool, error) { return false, nil })
	if err != nil || alreadyAbsent.Disposition != provider.CleanupAlreadyAbsent {
		t.Fatalf("verifyCgroupRemoval(absent) = (%#v, %v)", alreadyAbsent, err)
	}
	if _, err := verifyCgroupRemoval(context.Background(), true, func() (bool, error) { return true, nil }); err == nil {
		t.Fatal("verifyCgroupRemoval(still present) error = nil")
	}
}

// TestCgroupObservationHelpersClassifyUnknownReadback verifies only missing or
// malformed read-only kernel evidence becomes a retryable unknown observation.
func TestCgroupObservationHelpersClassifyUnknownReadback(t *testing.T) {
	unknown := fmt.Errorf("read cgroup.events: %w", cgroupv2.ErrUnknownState)
	if _, err := cgroupPresenceObservation(false, "", unknown); !provider.IsObservationUnavailable(err) || !errors.Is(err, cgroupv2.ErrUnknownState) {
		t.Fatalf("cgroupPresenceObservation(unknown) error = %v", err)
	}
	definite := errors.New("permission contract violated")
	if got := cgroupObservationError(definite); !errors.Is(got, definite) || provider.IsObservationUnavailable(got) {
		t.Fatalf("cgroupObservationError(definite) = %v", got)
	}
}
