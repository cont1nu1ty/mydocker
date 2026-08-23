package ownership

import (
	"fmt"
	"reflect"
	"testing"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/rollback"
)

// testReceipt constructs one valid pending host acquisition for ownership contract tests.
func testReceipt(t *testing.T) Receipt {
	t.Helper()
	owner, err := NewOwnerKey("op-owner", operation.Target{Kind: operation.TargetSandbox, ID: "sandbox-owner"}, domain.InitialGeneration)
	if err != nil {
		t.Fatalf("NewOwnerKey() error = %v", err)
	}
	evidence, err := EvidenceDigest(map[string]any{"pid": 42, "kind": "keeper"})
	if err != nil {
		t.Fatalf("EvidenceDigest() error = %v", err)
	}
	return Receipt{
		SchemaVersion: SchemaVersion, Provider: ProviderLinux, Kind: KindKeeperProcess,
		LocalID: "keeper-42", Owner: owner, EvidenceSHA256: evidence,
		Attributes: map[string]string{"pid": "42"},
	}
}

// TestOwnerKeyIsDeterministicAndBound verifies tokens are stable and change with any ownership field.
func TestOwnerKeyIsDeterministicAndBound(t *testing.T) {
	target := operation.Target{Kind: operation.TargetSandbox, ID: "sandbox-owner"}
	first, err := NewOwnerKey("op-owner", target, 1)
	if err != nil {
		t.Fatalf("NewOwnerKey() error = %v", err)
	}
	second, _ := NewOwnerKey("op-owner", target, 1)
	changed, _ := NewOwnerKey("op-other", target, 1)
	if first != second || first.Token == changed.Token {
		t.Fatalf("owner tokens = (%#v, %#v, %#v)", first, second, changed)
	}
	invalid := first
	invalid.Generation = 2
	if err := invalid.Validate(); err == nil {
		t.Fatal("OwnerKey.Validate() accepted a token bound to another generation")
	}
}

// TestReceiptAdoptionAndClone verifies ownership transfer is explicit and caller attributes cannot alias state.
func TestReceiptAdoptionAndClone(t *testing.T) {
	receipt := testReceipt(t)
	if err := receipt.Validate(); err != nil {
		t.Fatalf("Receipt.Validate() error = %v", err)
	}
	adopted, err := receipt.Adopt()
	if err != nil || !adopted.Adopted || receipt.Adopted {
		t.Fatalf("Receipt.Adopt() = (%#v, %v)", adopted, err)
	}
	clone := receipt.Clone()
	clone.Attributes["pid"] = "99"
	if reflect.DeepEqual(clone.Attributes, receipt.Attributes) {
		t.Fatal("Receipt.Clone() retained attribute alias")
	}
}

// TestReleaseBindsAbsenceEvidenceToDelete verifies cleanup proof retains the immutable acquisition identity.
func TestReleaseBindsAbsenceEvidenceToDelete(t *testing.T) {
	receipt, err := testReceipt(t).Adopt()
	if err != nil {
		t.Fatalf("Receipt.Adopt() setup error = %v", err)
	}
	release, err := NewRelease("op-delete-owner", receipt, map[string]any{"presence": "absent", "verified": true})
	if err != nil {
		t.Fatalf("NewRelease() error = %v", err)
	}
	clone := release.Clone()
	clone.Resource.Attributes["pid"] = "99"
	if reflect.DeepEqual(clone.Resource.Attributes, release.Resource.Attributes) {
		t.Fatal("Release.Clone() retained nested receipt aliases")
	}
	wrongOperation := release
	wrongOperation.CleanupOperationID = ""
	if err := wrongOperation.Validate(); err == nil {
		t.Fatal("Release.Validate() accepted an empty cleanup operation")
	}
	pending := release
	pending.Resource.Adopted = false
	if err := pending.Validate(); err == nil {
		t.Fatal("Release.Validate() accepted a pending acquisition receipt")
	}
	tampered := release
	tampered.EvidenceSHA256 = "bad"
	if err := tampered.Validate(); err == nil {
		t.Fatal("Release.Validate() accepted malformed absence evidence")
	}
}

// TestReceiptJournalProfilesRequireCompleteDependencyOrder verifies partial and reordered M2 inventories fail closed.
func TestReceiptJournalProfilesRequireCompleteDependencyOrder(t *testing.T) {
	base := testReceipt(t)
	base.Owner, _ = NewOwnerKey("op-profile", operation.Target{Kind: operation.TargetSandbox, ID: "sandbox-profile"}, domain.InitialGeneration)
	kinds := []Kind{KindSandboxCgroup, KindKeeperCgroup, KindKeeperProcess, KindUTSNamespace, KindIPCNamespace, KindNetworkNamespace}
	receipts := make([]Receipt, len(kinds))
	for index, kind := range kinds {
		receipt := base.Clone()
		receipt.Provider = ProviderLinux
		if kind == KindSandboxCgroup || kind == KindKeeperCgroup {
			receipt.Provider = ProviderCgroupV2
		}
		receipt.Kind = kind
		receipt.LocalID = fmt.Sprintf("profile-%d", index)
		receipts[index] = receipt
	}
	if err := ValidateReceiptJournalProfile(operation.TargetSandbox, receipts); err != nil {
		t.Fatalf("ValidateReceiptJournalProfile(complete) error = %v", err)
	}
	if err := ValidateReceiptJournalProfile(operation.TargetSandbox, receipts[:2]); err == nil {
		t.Fatal("ValidateReceiptJournalProfile() accepted a partial profile")
	}
	if err := ValidateReceiptJournalPrefix(operation.TargetSandbox, receipts[:2]); err != nil {
		t.Fatalf("ValidateReceiptJournalPrefix(valid prefix) error = %v", err)
	}
	reordered := append([]Receipt(nil), receipts...)
	reordered[2], reordered[3] = reordered[3], reordered[2]
	if err := ValidateReceiptJournalProfile(operation.TargetSandbox, reordered); err == nil {
		t.Fatal("ValidateReceiptJournalProfile() accepted reordered dependencies")
	}
	if err := ValidateReceiptJournalPrefix(operation.TargetSandbox, reordered[:3]); err == nil {
		t.Fatal("ValidateReceiptJournalPrefix() accepted reordered dependencies")
	}
}

// TestInverseDescriptorRoundTrip verifies rollback metadata cannot change action, target, or receipt identity unnoticed.
func TestInverseDescriptorRoundTrip(t *testing.T) {
	receipt := testReceipt(t)
	descriptor, err := InverseDescriptor(receipt, ActionStopProcess)
	if err != nil {
		t.Fatalf("InverseDescriptor() error = %v", err)
	}
	restored, action, err := ReceiptFromDescriptor(descriptor)
	if err != nil || action != ActionStopProcess || !reflect.DeepEqual(restored, receipt) {
		t.Fatalf("ReceiptFromDescriptor() = (%#v, %q, %v)", restored, action, err)
	}
	tampered := descriptor.Clone()
	tampered.Target = "different"
	if _, _, err := ReceiptFromDescriptor(tampered); err == nil {
		t.Fatal("ReceiptFromDescriptor() accepted a tampered target")
	}
	adopted, _ := receipt.Adopt()
	if _, err := InverseDescriptor(adopted, ActionStopProcess); err == nil {
		t.Fatal("InverseDescriptor() armed rollback for an adopted resource")
	}
	if _, err := InverseDescriptor(receipt, ActionRemoveCgroup); err == nil {
		t.Fatal("InverseDescriptor() accepted an action implemented by another provider")
	}
}

// TestReceiptValidationRejectsUnsafeDiscoveryData verifies paths and unsupported identifiers fail before persistence.
func TestReceiptValidationRejectsUnsafeDiscoveryData(t *testing.T) {
	tests := map[string]func(*Receipt){
		"path local ID":   func(receipt *Receipt) { receipt.LocalID = "../../host" },
		"future provider": func(receipt *Receipt) { receipt.Provider = "future" },
		"future kind":     func(receipt *Receipt) { receipt.Kind = "future" },
		"provider mismatch": func(receipt *Receipt) {
			receipt.Provider = ProviderCgroupV2
		},
		"bad digest":    func(receipt *Receipt) { receipt.EvidenceSHA256 = "bad" },
		"nul attribute": func(receipt *Receipt) { receipt.Attributes["pid"] = "4\x002" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			receipt := testReceipt(t)
			mutate(&receipt)
			if err := receipt.Validate(); err == nil {
				t.Fatal("Receipt.Validate() error = nil, want rejection")
			}
		})
	}
	if _, _, err := ReceiptFromDescriptor(rollbackLikeInvalidDescriptor()); err == nil {
		t.Fatal("ReceiptFromDescriptor() accepted malformed descriptor")
	}
}

// rollbackLikeInvalidDescriptor returns a deliberately incomplete descriptor without importing host paths.
func rollbackLikeInvalidDescriptor() rollback.Descriptor {
	return rollback.Descriptor{}
}
