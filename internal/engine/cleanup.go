package engine

import (
	"context"
	"fmt"

	"mydocker/internal/ownership"
	"mydocker/internal/provider"
)

// inspectReceipt routes read-only discovery through the only provider that owns the receipt kind.
func (engine *Engine) inspectReceipt(ctx context.Context, receipt ownership.Receipt) (provider.ResourceObservation, error) {
	request := provider.OwnedReceiptRequest{Owner: receipt.Owner, Receipt: receipt}
	var observation provider.ResourceObservation
	var err error
	switch receipt.Kind {
	case ownership.KindSandboxCgroup:
		observation, err = engine.providers.Cgroup.InspectSandboxCgroup(ctx, request)
	case ownership.KindKeeperCgroup:
		observation, err = engine.providers.Cgroup.InspectKeeperCgroup(ctx, request)
	case ownership.KindAttemptCgroup:
		observation, err = engine.providers.Cgroup.InspectAttemptCgroup(ctx, request)
	case ownership.KindKeeperProcess, ownership.KindInitProcess:
		observation, err = engine.providers.Isolation.InspectProcess(ctx, request)
	case ownership.KindUTSNamespace, ownership.KindIPCNamespace, ownership.KindNetworkNamespace,
		ownership.KindPIDNamespace, ownership.KindMountNamespace:
		observation, err = engine.providers.Isolation.InspectNamespace(ctx, request)
	case ownership.KindRootfsMount:
		observation, err = engine.providers.Isolation.InspectRootfs(ctx, request)
	case ownership.KindStartGate:
		observation, err = engine.providers.Isolation.InspectStartGate(ctx, request)
	case ownership.KindStreams:
		observation, err = engine.providers.Isolation.InspectStreams(ctx, request)
	default:
		return provider.ResourceObservation{}, fmt.Errorf("unsupported host receipt kind %q", receipt.Kind)
	}
	if err != nil {
		return provider.ResourceObservation{}, err
	}
	if err := observation.Validate(); err != nil {
		return provider.ResourceObservation{}, err
	}
	if observation.Presence == provider.PresencePresent && observation.EvidenceSHA256 != receipt.EvidenceSHA256 {
		return provider.ResourceObservation{}, fmt.Errorf("live %s evidence does not match its durable ownership receipt", receipt.Kind)
	}
	return observation, nil
}

// cleanupReceipt routes one idempotent teardown through exact adopted receipt authority.
func (engine *Engine) cleanupReceipt(ctx context.Context, receipt ownership.Receipt) (provider.CleanupObservation, error) {
	request := provider.OwnedReceiptRequest{Owner: receipt.Owner, Receipt: receipt}
	var observation provider.CleanupObservation
	var err error
	switch receipt.Kind {
	case ownership.KindSandboxCgroup:
		observation, err = engine.providers.Cgroup.RemoveSandboxCgroup(ctx, request)
	case ownership.KindKeeperCgroup:
		observation, err = engine.providers.Cgroup.RemoveKeeperCgroup(ctx, request)
	case ownership.KindAttemptCgroup:
		observation, err = engine.providers.Cgroup.RemoveAttemptCgroup(ctx, request)
	case ownership.KindKeeperProcess, ownership.KindInitProcess:
		observation, err = engine.providers.Isolation.RemoveProcess(ctx, request)
	case ownership.KindUTSNamespace, ownership.KindIPCNamespace, ownership.KindNetworkNamespace,
		ownership.KindPIDNamespace, ownership.KindMountNamespace:
		observation, err = engine.providers.Isolation.RemoveNamespace(ctx, request)
	case ownership.KindRootfsMount:
		observation, err = engine.providers.Isolation.RemoveRootfs(ctx, request)
	case ownership.KindStartGate:
		observation, err = engine.providers.Isolation.RemoveStartGate(ctx, request)
	case ownership.KindStreams:
		observation, err = engine.providers.Isolation.RemoveStreams(ctx, request)
	default:
		return provider.CleanupObservation{}, fmt.Errorf("unsupported host receipt kind %q", receipt.Kind)
	}
	if err != nil {
		return provider.CleanupObservation{}, err
	}
	if err := observation.Validate(); err != nil {
		return provider.CleanupObservation{}, err
	}
	return observation, nil
}

// releaseByKind returns a release copy when teardown already durably proved that resource kind absent.
func releaseByKind(releases []ownership.Release, kind ownership.Kind) *ownership.Release {
	for _, release := range releases {
		if release.Resource.Kind == kind {
			clone := release.Clone()
			return &clone
		}
	}
	return nil
}
