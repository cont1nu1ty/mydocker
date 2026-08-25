package engine

import "testing"

// IntegrationFakeHost exposes the existing all-provider fake only to external
// engine tests that need to compose packages which themselves import engine.
type IntegrationFakeHost = fakeHost

// IntegrationHostSnapshot is a comparable physical-side-effect summary used
// to prove durable API replay does not reacquire or redeliver provider actions.
type IntegrationHostSnapshot struct {
	Created          int
	Present          int
	GateReleases     int
	SignalDeliveries int
}

// NewIntegrationFakeHost returns the same owner-validating provider fake used
// by focused engine tests; it performs no namespace, cgroup, mount, or process action.
func NewIntegrationFakeHost() *IntegrationFakeHost {
	return newFakeHost()
}

// IntegrationProviders wires the complete provider and rollback profile around one fake host.
func IntegrationProviders(t *testing.T, host *IntegrationFakeHost) Providers {
	t.Helper()
	return testProviders(t, host)
}

// SnapshotIntegrationHost captures counters without exposing the fake's mutable maps.
func SnapshotIntegrationHost(host *IntegrationFakeHost) IntegrationHostSnapshot {
	if host == nil {
		return IntegrationHostSnapshot{}
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	snapshot := IntegrationHostSnapshot{
		GateReleases: host.releaseCalls, SignalDeliveries: host.signalDeliveries,
	}
	for _, created := range host.created {
		snapshot.Created += created
	}
	for _, present := range host.present {
		if present {
			snapshot.Present++
		}
	}
	return snapshot
}
