// Package engine coordinates M1 lifecycle facts with individually
// checkpointed M2 host-provider effects.
package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/lifecycle"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
	"mydocker/internal/state"
)

// Clock supplies diagnostic wall facts at lifecycle confirmation boundaries.
type Clock interface {
	Now() time.Time
}

// wallClock supplies production wall time without claiming cross-process monotonic semantics.
type wallClock struct{}

// Now returns the current wall-clock diagnostic fact.
func (wallClock) Now() time.Time { return time.Now() }

// AttemptObservation is the shim/supervisor fact needed to distinguish a
// prepared wrapper, a running workload child, and a terminal outcome.
type AttemptObservation struct {
	Prepared bool
	Starting bool
	Running  bool
	Terminal bool
	Outcome  domain.Outcome
	Evidence string
}

// Validate rejects contradictory supervisor facts and terminal results without explicit outcome presence.
func (observation AttemptObservation) Validate() error {
	states := 0
	if observation.Prepared {
		states++
	}
	if observation.Starting {
		states++
	}
	if observation.Running {
		states++
	}
	if observation.Terminal {
		states++
	}
	if states != 1 || observation.Evidence == "" {
		return errors.New("attempt observation must select exactly one evidenced state")
	}
	if observation.Terminal {
		if err := observation.Outcome.Validate(); err != nil {
			return err
		}
		if observation.Outcome.Presence == domain.OutcomePending {
			return errors.New("terminal attempt observation requires a terminal outcome")
		}
	} else if observation.Outcome.Presence != "" && observation.Outcome.Presence != domain.OutcomePending {
		return errors.New("non-terminal attempt observation cannot contain a terminal outcome")
	}
	return nil
}

// Supervisor observes workload-child state while the stable init wrapper keeps
// the same process receipt across the start gate and workload exec boundary.
type Supervisor interface {
	// ObserveAttempt returns an owner-scoped prepared, running, or terminal fact.
	ObserveAttempt(context.Context, provider.OwnedReceiptRequest) (AttemptObservation, error)
}

// Providers is the complete host boundary used by the M3 engine.
type Providers struct {
	Cgroup     provider.CgroupProvider
	Isolation  provider.IsolationProvider
	Supervisor Supervisor
	Rollback   *provider.RollbackRegistry
}

// Validate rejects incomplete production wiring before any lifecycle intent is accepted.
func (providers Providers) Validate() error {
	if providers.Cgroup == nil || providers.Isolation == nil || providers.Supervisor == nil || providers.Rollback == nil {
		return errors.New("engine requires cgroup, isolation, supervisor, and rollback providers")
	}
	return nil
}

// Engine serializes side effects per primary target while durable operation
// conflict and replay semantics remain enforced by the lifecycle coordinator.
type Engine struct {
	store       state.Store
	lifecycle   *lifecycle.Coordinator
	providers   Providers
	clock       Clock
	targetLocks *targetLocks
	identityMu  sync.RWMutex
	identities  map[string]ownership.Receipt
	identityRev uint64
}

// New constructs a Linux-M2-profile engine over durable state and explicit host providers.
func New(store state.Store, providers Providers) (*Engine, error) {
	return NewWithClock(store, providers, wallClock{})
}

// NewWithClock constructs a deterministic engine for recovery and fault tests.
func NewWithClock(store state.Store, providers Providers, clock Clock) (*Engine, error) {
	if store == nil {
		return nil, errors.New("engine store must not be nil")
	}
	if err := providers.Validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		return nil, errors.New("engine clock must not be nil")
	}
	engine := &Engine{
		store: store, providers: providers, clock: clock, targetLocks: newTargetLocks(),
		identities: make(map[string]ownership.Receipt),
	}
	coordinator, err := lifecycle.NewCoordinatorForProfile(store, state.HostProfileLinuxM2, engine)
	if err != nil {
		return nil, err
	}
	engine.lifecycle = coordinator
	return engine, nil
}

// Verify implements lifecycle.ProcessIdentityVerifier by comparing the status
// projection with adopted init receipt evidence and then asking the provider to
// revalidate that exact owner-scoped process.
func (engine *Engine) Verify(ctx context.Context, target operation.Target, identity domain.ProcessIdentity) error {
	if target.Kind != operation.TargetContainer {
		return fmt.Errorf("engine process verification requires a Container target, got %q", target.Kind)
	}
	engine.identityMu.RLock()
	process, found := engine.identities[identity.Handle]
	engine.identityMu.RUnlock()
	if !found {
		return errors.New("runtime process identity has not been rediscovered by the daemon")
	}
	if identity.Handle != processIdentityHandle(process) || identity.Evidence != process.EvidenceSHA256 || !process.Owner.Target.Equal(target) {
		return errors.New("persisted process identity does not match adopted init receipt")
	}
	observation, err := engine.providers.Isolation.InspectProcess(ctx, provider.OwnedReceiptRequest{Owner: process.Owner, Receipt: process})
	if err != nil {
		return err
	}
	if err := observation.Validate(); err != nil {
		return err
	}
	if observation.Presence != provider.PresencePresent {
		return errors.New("init process is not currently verified present")
	}
	return nil
}

// registerProcessIdentity makes one adopted or pending init receipt available
// to action-time verification without recursively opening a Store transaction.
func (engine *Engine) registerProcessIdentity(receipt ownership.Receipt) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	if receipt.Kind != ownership.KindInitProcess {
		return errors.New("only init-process receipts can enter the action-time identity registry")
	}
	engine.identityMu.Lock()
	defer engine.identityMu.Unlock()
	handle := processIdentityHandle(receipt)
	if existing, found := engine.identities[handle]; found {
		if existing.EvidenceSHA256 != receipt.EvidenceSHA256 || existing.Owner != receipt.Owner {
			return errors.New("init-process local identity collides with different evidence")
		}
		return nil
	}
	engine.identities[handle] = receipt.Clone()
	engine.identityRev++
	return nil
}

// forgetProcessIdentity removes runtime verification state only after teardown
// has durably proved the corresponding wrapper absent.
func (engine *Engine) forgetProcessIdentity(receipt ownership.Receipt) {
	engine.identityMu.Lock()
	defer engine.identityMu.Unlock()
	handle := processIdentityHandle(receipt)
	if existing, found := engine.identities[handle]; found && existing.EvidenceSHA256 == receipt.EvidenceSHA256 && existing.Owner == receipt.Owner {
		delete(engine.identities, handle)
		engine.identityRev++
	}
}

// identityRevision snapshots the process-local registry version before a slow
// read-only recovery scan so publication cannot overwrite concurrent lifecycle work.
func (engine *Engine) identityRevision() uint64 {
	engine.identityMu.RLock()
	defer engine.identityMu.RUnlock()
	return engine.identityRev
}

// publishDiscoveredIdentities replaces the registry only when no create or
// teardown changed it after discovery began; a skipped publish is retried by a later scan.
func (engine *Engine) publishDiscoveredIdentities(revision uint64, identities map[string]ownership.Receipt) bool {
	engine.identityMu.Lock()
	defer engine.identityMu.Unlock()
	if engine.identityRev != revision {
		return false
	}
	engine.identities = identities
	engine.identityRev++
	return true
}

// processIdentityHandle scopes a provider-local process name by its immutable
// owner token so unrelated Sandboxes can safely use the same provider naming scheme.
func processIdentityHandle(receipt ownership.Receipt) string {
	return "process:" + receipt.Owner.Token + ":" + receipt.LocalID
}

// Coordinator returns the query-only lifecycle facade used by daemon adapters.
// Host-changing operations must continue through Engine methods.
func (engine *Engine) Coordinator() *lifecycle.Coordinator {
	return engine.lifecycle
}

// targetLocks holds process-local serialization; durable Store conflicts remain authoritative across restarts.
type targetLocks struct {
	mu    sync.Mutex
	locks map[string]*targetLock
}

// targetLock couples one keyed mutex with every current holder and waiter so
// the registry can remove idle entries without allowing an ABA overlap.
type targetLock struct {
	mutex sync.Mutex
	refs  uint64
}

// newTargetLocks initializes the target-lock registry used by one daemon instance.
func newTargetLocks() *targetLocks {
	return &targetLocks{locks: make(map[string]*targetLock)}
}

// lock serializes one target until the returned release function is called.
func (locks *targetLocks) lock(target operation.Target) func() {
	key := string(target.Kind) + "\x00" + target.ID
	locks.mu.Lock()
	entry := locks.locks[key]
	if entry == nil {
		entry = &targetLock{}
		locks.locks[key] = entry
	}
	entry.refs++
	locks.mu.Unlock()
	entry.mutex.Lock()
	return func() {
		entry.mutex.Unlock()
		locks.mu.Lock()
		entry.refs--
		if entry.refs == 0 && locks.locks[key] == entry {
			delete(locks.locks, key)
		}
		locks.mu.Unlock()
	}
}

// preflight performs only read-only capability discovery and rejects missing rootful M2 support.
func (engine *Engine) preflight(ctx context.Context) (provider.Capabilities, error) {
	requirements := provider.M2Requirements()
	cgroup, err := engine.providers.Cgroup.InspectCgroupCapabilities(ctx, requirements.Cgroup)
	if err != nil {
		return provider.Capabilities{}, fmt.Errorf("inspect cgroup capabilities: %w", err)
	}
	isolationCapabilities, err := engine.providers.Isolation.InspectIsolationCapabilities(ctx, requirements.Isolation)
	if err != nil {
		return provider.Capabilities{}, fmt.Errorf("inspect isolation capabilities: %w", err)
	}
	capabilities := provider.Capabilities{SchemaVersion: provider.SchemaVersion, Cgroup: cgroup, Isolation: isolationCapabilities}
	if err := capabilities.Satisfies(requirements); err != nil {
		return provider.Capabilities{}, err
	}
	return capabilities, nil
}

// sandboxInventory returns independent adopted host receipts for one retained Sandbox.
func (engine *Engine) sandboxInventory(ctx context.Context, id domain.SandboxID) ([]ownership.Receipt, error) {
	var receipts []ownership.Receipt
	err := engine.store.View(ctx, func(reader state.Reader) error {
		record, err := reader.GetSandbox(id)
		if err != nil {
			return err
		}
		receipts = cloneReceipts(record.HostResources)
		return nil
	})
	return receipts, err
}

// containerInventory returns independent adopted host receipts for one retained Container/Attempt.
func (engine *Engine) containerInventory(ctx context.Context, id domain.ContainerID) ([]ownership.Receipt, error) {
	var receipts []ownership.Receipt
	err := engine.store.View(ctx, func(reader state.Reader) error {
		record, err := reader.GetContainerAttempt(id)
		if err != nil {
			return err
		}
		receipts = cloneReceipts(record.HostResources)
		return nil
	})
	return receipts, err
}

// receiptByKind selects exactly one receipt from a canonical inventory.
func receiptByKind(receipts []ownership.Receipt, kind ownership.Kind) (ownership.Receipt, error) {
	var selected ownership.Receipt
	count := 0
	for _, receipt := range receipts {
		if receipt.Kind == kind {
			selected = receipt.Clone()
			count++
		}
	}
	if count != 1 {
		return ownership.Receipt{}, fmt.Errorf("host inventory contains %d %q receipts, want exactly one", count, kind)
	}
	return selected, nil
}

// cloneReceipts protects provider attributes across engine and transaction boundaries.
func cloneReceipts(receipts []ownership.Receipt) []ownership.Receipt {
	clones := make([]ownership.Receipt, len(receipts))
	for index, receipt := range receipts {
		clones[index] = receipt.Clone()
	}
	return clones
}
