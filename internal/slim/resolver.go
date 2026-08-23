package slim

import (
	"encoding/json"
	"errors"
	"fmt"

	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
)

const (
	wrapperEvidenceAttribute       = "wrapper_evidence_sha256"
	configurationEvidenceAttribute = "configuration_sha256"
	hostnameAttribute              = "hostname"
	networkModeAttribute           = "network_mode"
	processEvidenceAttribute       = "process_evidence"
)

// receiptResolver reconstructs action-time launcher references without trusting receipt paths or numeric PIDs.
type receiptResolver struct {
	runtimeRoot string
}

// newReceiptResolver validates the private-root shape used for deterministic owner artifact derivation.
func newReceiptResolver(runtimeRoot string) (*receiptResolver, error) {
	if runtimeRoot == "" {
		return nil, errors.New("receipt resolver runtime root must not be empty")
	}
	return &receiptResolver{runtimeRoot: runtimeRoot}, nil
}

// Resolve validates receipt integrity and returns an internally derived process reference for launcher actions.
func (resolver *receiptResolver) Resolve(request provider.OwnedReceiptRequest) (ResourceReference, error) {
	if err := request.ValidateFor(ownership.ProviderLinux, ownership.KindKeeperProcess, ownership.KindInitProcess); err != nil {
		return ResourceReference{}, err
	}
	return resolver.resolve(request.Receipt)
}

// resolve validates any launcher-owned process, namespace, or rootfs receipt and reconstructs its exact reference.
func (resolver *receiptResolver) resolve(receipt ownership.Receipt) (ResourceReference, error) {
	if err := validateSlimReceipt(receipt); err != nil {
		return ResourceReference{}, err
	}
	launcherEvidence := receipt.Attributes[launcherEvidenceAttribute]
	if !validDigest(launcherEvidence) {
		return ResourceReference{}, fmt.Errorf("%w: launcher evidence is absent", ErrReceiptMismatch)
	}
	paths, err := deriveArtifactPaths(resolver.runtimeRoot, receipt.Owner)
	if err != nil {
		return ResourceReference{}, err
	}
	reference := ResourceReference{
		Owner: receipt.Owner, Kind: receipt.Kind, LocalID: receipt.LocalID,
		ReceiptEvidenceSHA256: receipt.EvidenceSHA256, LauncherEvidenceSHA256: launcherEvidence,
		WrapperEvidenceSHA256: receipt.Attributes[wrapperEvidenceAttribute],
		ConfigurationSHA256:   receipt.Attributes[configurationEvidenceAttribute], Paths: paths,
	}
	if err := json.Unmarshal([]byte(receipt.Attributes[processEvidenceAttribute]), &reference.ProcessEvidence); err != nil {
		return ResourceReference{}, fmt.Errorf("%w: decode process evidence: %v", ErrReceiptMismatch, err)
	}
	processDigest, err := ownership.EvidenceDigest(reference.ProcessEvidence)
	if err != nil {
		return ResourceReference{}, err
	}
	if processDigest != reference.LauncherEvidenceSHA256 &&
		(receipt.Kind == ownership.KindKeeperProcess || receipt.Kind == ownership.KindInitProcess) {
		return ResourceReference{}, fmt.Errorf("%w: process evidence digest differs", ErrReceiptMismatch)
	}
	if value := receipt.Attributes[sandboxIDAttribute]; value != "" {
		reference.SandboxID = domain.SandboxID(value)
		if err := reference.SandboxID.Validate(); err != nil {
			return ResourceReference{}, err
		}
	}
	if value := receipt.Attributes[attemptIDAttribute]; value != "" {
		reference.AttemptID = domain.AttemptID(value)
		if err := reference.AttemptID.Validate(); err != nil {
			return ResourceReference{}, err
		}
	}
	if receipt.Kind == ownership.KindKeeperProcess || receipt.Kind == ownership.KindInitProcess {
		if !validDigest(reference.WrapperEvidenceSHA256) {
			return ResourceReference{}, fmt.Errorf("%w: wrapper evidence is absent", ErrReceiptMismatch)
		}
	}
	switch receipt.Kind {
	case ownership.KindUTSNamespace:
		reference.Hostname = receipt.Attributes[hostnameAttribute]
		expected, err := namespaceConfigurationDigest(isolation.NamespaceUTS, reference.Hostname, "")
		if err != nil {
			return ResourceReference{}, err
		}
		if reference.ConfigurationSHA256 != expected {
			return ResourceReference{}, fmt.Errorf("%w: UTS hostname configuration digest differs", ErrReceiptMismatch)
		}
	case ownership.KindNetworkNamespace:
		reference.NetworkMode = provider.SandboxNetworkMode(receipt.Attributes[networkModeAttribute])
		if !reference.NetworkMode.Valid() {
			return ResourceReference{}, fmt.Errorf("%w: network mode is not canonical", ErrReceiptMismatch)
		}
		expected, err := namespaceConfigurationDigest(isolation.NamespaceNetwork, "", reference.NetworkMode)
		if err != nil {
			return ResourceReference{}, err
		}
		if reference.ConfigurationSHA256 != expected {
			return ResourceReference{}, fmt.Errorf("%w: network mode configuration digest differs", ErrReceiptMismatch)
		}
	}
	if err := reference.Validate(resolver.runtimeRoot); err != nil {
		return ResourceReference{}, err
	}
	return reference, nil
}

// newSlimReceipt builds one deterministic Linux receipt whose attributes are included in canonical evidence.
func newSlimReceipt(owner ownership.OwnerKey, kind ownership.Kind, attributes map[string]string) (ownership.Receipt, error) {
	if err := owner.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	copyAttributes := make(map[string]string, len(attributes))
	for key, value := range attributes {
		copyAttributes[key] = value
	}
	localID := localIDFor(kind)
	evidence, err := slimReceiptDigest(owner, kind, localID, copyAttributes)
	if err != nil {
		return ownership.Receipt{}, err
	}
	receipt := ownership.Receipt{
		SchemaVersion: ownership.SchemaVersion, Provider: ownership.ProviderLinux,
		Kind: kind, LocalID: localID, Owner: owner, EvidenceSHA256: evidence,
		Attributes: copyAttributes,
	}
	if err := receipt.Validate(); err != nil {
		return ownership.Receipt{}, err
	}
	if err := validateReceiptAttributes(receipt); err != nil {
		return ownership.Receipt{}, err
	}
	return receipt, nil
}

// validateSlimReceipt proves deterministic local identity, bounded attributes, and canonical evidence.
func validateSlimReceipt(receipt ownership.Receipt) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	if receipt.Provider != ownership.ProviderLinux || receipt.LocalID != localIDFor(receipt.Kind) {
		return fmt.Errorf("%w: provider or local ID differs", ErrReceiptMismatch)
	}
	if err := validateReceiptAttributes(receipt); err != nil {
		return err
	}
	expected, err := slimReceiptDigest(receipt.Owner, receipt.Kind, receipt.LocalID, receipt.Attributes)
	if err != nil {
		return err
	}
	if receipt.EvidenceSHA256 != expected {
		return ErrReceiptMismatch
	}
	return nil
}

// slimReceiptDigest binds owner, kind, local identity, and every diagnostic attribute without including adoption state.
func slimReceiptDigest(owner ownership.OwnerKey, kind ownership.Kind, localID string, attributes map[string]string) (string, error) {
	return ownership.EvidenceDigest(struct {
		SchemaVersion uint32             `json:"schema_version"`
		Owner         ownership.OwnerKey `json:"owner"`
		Kind          ownership.Kind     `json:"kind"`
		LocalID       string             `json:"local_id"`
		Attributes    map[string]string  `json:"attributes,omitempty"`
	}{SchemaVersion, owner, kind, localID, attributes})
}

// validateReceiptAttributes rejects unknown fields and requires the exact metadata needed to rediscover each resource.
func validateReceiptAttributes(receipt ownership.Receipt) error {
	allowed := map[string]bool{}
	required := map[string]bool{}
	require := func(key string) {
		allowed[key] = true
		required[key] = true
	}
	switch receipt.Kind {
	case ownership.KindKeeperProcess:
		require(launcherEvidenceAttribute)
		require(wrapperEvidenceAttribute)
		require(sandboxIDAttribute)
		require(processEvidenceAttribute)
	case ownership.KindInitProcess:
		require(launcherEvidenceAttribute)
		require(wrapperEvidenceAttribute)
		require(sandboxIDAttribute)
		require(attemptIDAttribute)
		require(processEvidenceAttribute)
	case ownership.KindUTSNamespace:
		require(launcherEvidenceAttribute)
		require(sandboxIDAttribute)
		require(configurationEvidenceAttribute)
		require(hostnameAttribute)
		require(processEvidenceAttribute)
	case ownership.KindIPCNamespace:
		require(launcherEvidenceAttribute)
		require(sandboxIDAttribute)
		require(processEvidenceAttribute)
	case ownership.KindNetworkNamespace:
		require(launcherEvidenceAttribute)
		require(sandboxIDAttribute)
		require(configurationEvidenceAttribute)
		require(networkModeAttribute)
		require(processEvidenceAttribute)
	case ownership.KindPIDNamespace, ownership.KindMountNamespace:
		require(launcherEvidenceAttribute)
		require(sandboxIDAttribute)
		require(attemptIDAttribute)
		require(processEvidenceAttribute)
	case ownership.KindRootfsMount:
		require(launcherEvidenceAttribute)
		require(sandboxIDAttribute)
		require(attemptIDAttribute)
		require("source_id")
		require(configurationEvidenceAttribute)
		require(processEvidenceAttribute)
	case ownership.KindStartGate:
		require(attemptIDAttribute)
	case ownership.KindStreams:
		require(attemptIDAttribute)
		require("stdout")
		require("stderr")
	default:
		return fmt.Errorf("slim provider does not own receipt kind %q", receipt.Kind)
	}
	for key := range receipt.Attributes {
		if !allowed[key] {
			return fmt.Errorf("%w: unexpected receipt attribute %q", ErrReceiptMismatch, key)
		}
		delete(required, key)
	}
	if len(required) != 0 {
		return fmt.Errorf("%w: required receipt attributes are absent", ErrReceiptMismatch)
	}
	for _, key := range []string{launcherEvidenceAttribute, wrapperEvidenceAttribute, configurationEvidenceAttribute} {
		if value := receipt.Attributes[key]; value != "" && !validDigest(value) {
			return fmt.Errorf("%w: %s is not a digest", ErrReceiptMismatch, key)
		}
	}
	return nil
}
