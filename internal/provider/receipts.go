package provider

import (
	"errors"
	"fmt"

	"mydocker/internal/ownership"
)

// SandboxReceipts is the exact complete M2 host-resource inventory for one Sandbox.
type SandboxReceipts struct {
	Owner        ownership.OwnerKey `json:"owner"`
	Cgroup       ownership.Receipt  `json:"cgroup"`
	KeeperCgroup ownership.Receipt  `json:"keeper_cgroup"`
	Keeper       ownership.Receipt  `json:"keeper"`
	UTS          ownership.Receipt  `json:"uts"`
	IPC          ownership.Receipt  `json:"ipc"`
	Network      ownership.Receipt  `json:"network"`
}

// Validate requires exactly one Sandbox cgroup, keeper, UTS, IPC, and network receipt under one owner.
func (s SandboxReceipts) Validate() error {
	receipts := []receiptExpectation{
		{name: "cgroup", receipt: s.Cgroup, provider: ownership.ProviderCgroupV2, kind: ownership.KindSandboxCgroup},
		{name: "keeper_cgroup", receipt: s.KeeperCgroup, provider: ownership.ProviderCgroupV2, kind: ownership.KindKeeperCgroup},
		{name: "keeper", receipt: s.Keeper, provider: ownership.ProviderLinux, kind: ownership.KindKeeperProcess},
		{name: "uts", receipt: s.UTS, provider: ownership.ProviderLinux, kind: ownership.KindUTSNamespace},
		{name: "ipc", receipt: s.IPC, provider: ownership.ProviderLinux, kind: ownership.KindIPCNamespace},
		{name: "network", receipt: s.Network, provider: ownership.ProviderLinux, kind: ownership.KindNetworkNamespace},
	}
	return validateReceiptSet(s.Owner, receipts)
}

// Receipts returns independent receipts in dependency acquisition order for persistence or adoption.
func (s SandboxReceipts) Receipts() []ownership.Receipt {
	return cloneReceipts([]ownership.Receipt{s.Cgroup, s.KeeperCgroup, s.Keeper, s.UTS, s.IPC, s.Network})
}

// AttemptReceipts is the exact complete M2 host-resource inventory for one Container Attempt.
type AttemptReceipts struct {
	Owner   ownership.OwnerKey `json:"owner"`
	Cgroup  ownership.Receipt  `json:"cgroup"`
	Init    ownership.Receipt  `json:"init"`
	PID     ownership.Receipt  `json:"pid"`
	Mount   ownership.Receipt  `json:"mount"`
	Rootfs  ownership.Receipt  `json:"rootfs"`
	Gate    ownership.Receipt  `json:"gate"`
	Streams ownership.Receipt  `json:"streams"`
}

// Validate requires exactly one Attempt cgroup, init, PID, mount, rootfs, gate, and stream receipt under one owner.
func (s AttemptReceipts) Validate() error {
	receipts := []receiptExpectation{
		{name: "cgroup", receipt: s.Cgroup, provider: ownership.ProviderCgroupV2, kind: ownership.KindAttemptCgroup},
		{name: "gate", receipt: s.Gate, provider: ownership.ProviderLinux, kind: ownership.KindStartGate},
		{name: "streams", receipt: s.Streams, provider: ownership.ProviderLinux, kind: ownership.KindStreams},
		{name: "init", receipt: s.Init, provider: ownership.ProviderLinux, kind: ownership.KindInitProcess},
		{name: "pid", receipt: s.PID, provider: ownership.ProviderLinux, kind: ownership.KindPIDNamespace},
		{name: "mount", receipt: s.Mount, provider: ownership.ProviderLinux, kind: ownership.KindMountNamespace},
		{name: "rootfs", receipt: s.Rootfs, provider: ownership.ProviderLinux, kind: ownership.KindRootfsMount},
	}
	return validateReceiptSet(s.Owner, receipts)
}

// Receipts returns independent receipts in dependency acquisition order for persistence or adoption.
func (s AttemptReceipts) Receipts() []ownership.Receipt {
	return cloneReceipts([]ownership.Receipt{s.Cgroup, s.Gate, s.Streams, s.Init, s.PID, s.Mount, s.Rootfs})
}

// receiptExpectation associates one receipt-set field with its only accepted provider and kind.
type receiptExpectation struct {
	name     string
	receipt  ownership.Receipt
	provider ownership.Provider
	kind     ownership.Kind
}

// validateReceiptSet proves every field shares the full operation, target, generation, and owner-token tuple.
func validateReceiptSet(owner ownership.OwnerKey, expectations []receiptExpectation) error {
	if err := owner.Validate(); err != nil {
		return fmt.Errorf("receipt-set owner: %w", err)
	}
	if len(expectations) == 0 {
		return errors.New("receipt set must not be empty")
	}
	seen := make(map[string]string, len(expectations))
	var adopted *bool
	for _, expectation := range expectations {
		if err := validateReceipt(owner, expectation.receipt, expectation.provider, expectation.kind); err != nil {
			return fmt.Errorf("%s receipt: %w", expectation.name, err)
		}
		identity := string(expectation.receipt.Provider) + "\x00" + string(expectation.receipt.Kind) + "\x00" + expectation.receipt.LocalID
		if previous, duplicate := seen[identity]; duplicate {
			return fmt.Errorf("%s receipt duplicates %s host identity", expectation.name, previous)
		}
		seen[identity] = expectation.name
		if adopted == nil {
			value := expectation.receipt.Adopted
			adopted = &value
		} else if *adopted != expectation.receipt.Adopted {
			return errors.New("complete receipt set must be entirely pending or entirely adopted")
		}
	}
	return nil
}

// cloneReceipts prevents callers from mutating persisted provider attributes through returned slices or maps.
func cloneReceipts(receipts []ownership.Receipt) []ownership.Receipt {
	clones := make([]ownership.Receipt, len(receipts))
	for index, receipt := range receipts {
		clones[index] = receipt.Clone()
	}
	return clones
}
