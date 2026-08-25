package state

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
	"mydocker/internal/rollback"
)

// MemoryStore is a process-local copy-on-write implementation of Store. It is
// intended for M1 correctness tests and does not claim restart durability.
type MemoryStore struct {
	mu        sync.RWMutex
	data      memoryData
	retention RetentionPolicy
}

// memoryData is the complete commit unit copied before every update callback,
// which lets a callback error discard resource, operation, and event changes.
type memoryData struct {
	sandboxes                     map[domain.SandboxID]SandboxRecord
	containerAttempts             map[domain.ContainerID]ContainerAttemptRecord
	operations                    map[operation.OperationID]OperationRecord
	events                        []operation.Event
	lastEventSequence             EventSequence
	firstEventSequence            EventSequence
	terminalOperationSequences    map[operation.OperationID]uint64
	lastTerminalOperationSequence uint64
	retiredOperations             map[string]retiredOperation
}

// memoryView implements both Reader and Tx over one callback-scoped snapshot;
// writable distinguishes an Update transaction from a read-only View.
type memoryView struct {
	data      *memoryData
	writable  bool
	retention RetentionPolicy
	closed    atomic.Bool
}

var (
	_ Store = (*MemoryStore)(nil)
	_ Tx    = (*memoryView)(nil)
)

// NewMemoryStore returns an empty test store whose first committed record
// revision and global event sequence are both one.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: newMemoryData(), retention: DefaultRetentionPolicy()}
}

// NewMemoryStoreWithRetention returns a deterministic test store with explicit
// small limits so compaction and capacity behavior can be exercised cheaply.
func NewMemoryStoreWithRetention(policy RetentionPolicy) (*MemoryStore, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return &MemoryStore{data: newMemoryData(), retention: policy}, nil
}

// newMemoryData initializes every map so a fresh transaction can write without
// special-case allocation or exposing nil collections to callers.
func newMemoryData() memoryData {
	return memoryData{
		sandboxes:                  make(map[domain.SandboxID]SandboxRecord),
		containerAttempts:          make(map[domain.ContainerID]ContainerAttemptRecord),
		operations:                 make(map[operation.OperationID]OperationRecord),
		firstEventSequence:         1,
		terminalOperationSequences: make(map[operation.OperationID]uint64),
		retiredOperations:          make(map[string]retiredOperation),
	}
}

// clone returns a fully independent candidate commit, including nested domain
// values and event payload bytes, so rollback is implemented by discarding it.
func (d memoryData) clone() memoryData {
	clone := newMemoryData()
	clone.lastEventSequence = d.lastEventSequence
	clone.firstEventSequence = d.firstEventSequence
	clone.lastTerminalOperationSequence = d.lastTerminalOperationSequence
	for id, record := range d.sandboxes {
		clone.sandboxes[id] = record.Clone()
	}
	for id, record := range d.containerAttempts {
		clone.containerAttempts[id] = record.Clone()
	}
	for id, record := range d.operations {
		clone.operations[id] = record.Clone()
	}
	for id, sequence := range d.terminalOperationSequences {
		clone.terminalOperationSequences[id] = sequence
	}
	for digest, retired := range d.retiredOperations {
		clone.retiredOperations[digest] = retired
	}
	clone.events = make([]operation.Event, len(d.events))
	for i, event := range d.events {
		clone.events[i] = event.Clone()
	}
	return clone
}

// View holds a shared lock while fn observes one consistent snapshot. Returned
// records are deep copies, and the Reader rejects use after fn returns.
func (s *MemoryStore) View(ctx context.Context, fn func(Reader) error) error {
	if fn == nil {
		return ErrNilCallback
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	view := &memoryView{data: &s.data, retention: s.retention}
	defer func() {
		view.close()
		s.mu.RUnlock()
	}()
	return fn(view)
}

// Update runs fn against a deep copy under the exclusive writer lock. It swaps
// the copy into the store only when fn succeeds and the context remains valid.
func (s *MemoryStore) Update(ctx context.Context, fn func(Tx) error) error {
	if fn == nil {
		return ErrNilCallback
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate := s.data.clone()
	tx := &memoryView{data: &candidate, writable: true, retention: s.retention}
	defer tx.close()
	if err := fn(tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := candidate.applyRetention(s.retention); err != nil {
		return err
	}
	if err := candidate.validate(); err != nil {
		return err
	}
	s.data = candidate
	return nil
}

// validate checks whole-store referential and lifecycle invariants at the atomic commit boundary.
func (d memoryData) validate() error {
	if d.firstEventSequence == 0 || d.terminalOperationSequences == nil || d.retiredOperations == nil {
		return fmt.Errorf("retention metadata is missing: %w", ErrInvariantViolation)
	}
	if len(d.events) == 0 {
		expected, err := d.lastEventSequence.Next()
		if err != nil {
			expected = d.lastEventSequence
		}
		if d.firstEventSequence != expected {
			return fmt.Errorf("empty event suffix starts at %d, want %d: %w", d.firstEventSequence, expected, ErrInvariantViolation)
		}
	} else {
		if d.events[0].Sequence != d.firstEventSequence || d.events[len(d.events)-1].Sequence != d.lastEventSequence {
			return fmt.Errorf("event suffix boundaries %d/%d do not match retained %d/%d: %w",
				d.firstEventSequence, d.lastEventSequence, d.events[0].Sequence, d.events[len(d.events)-1].Sequence, ErrInvariantViolation)
		}
		for index := 1; index < len(d.events); index++ {
			if err := d.events[index].ValidateAfter(d.events[index-1]); err != nil {
				return fmt.Errorf("retained event ordering: %w: %v", ErrInvariantViolation, err)
			}
		}
	}
	eventsBySequence := make(map[uint64]operation.Event, len(d.events))
	for _, event := range d.events {
		eventsBySequence[uint64(event.Sequence)] = event
		if record, exists := d.operations[event.OperationID]; exists {
			if event.Type != record.Operation.Type || !event.Target.Equal(record.Operation.Target) {
				return fmt.Errorf("event sequence %d does not match operation %q binding: %w", event.Sequence, event.OperationID, ErrInvariantViolation)
			}
			continue
		}
		retired, exists := d.retiredOperationFor(event.OperationID)
		if !exists || event.Type != retired.Type || !event.Target.Equal(retired.Target) {
			return fmt.Errorf("event sequence %d references an unknown or mismatched operation %q: %w", event.Sequence, event.OperationID, ErrInvariantViolation)
		}
	}
	for id, record := range d.operations {
		if record.Operation.State.Terminal() && len(record.Operation.Response) == 0 {
			return fmt.Errorf("operation %q has terminal state without replay response: %w", id, ErrInvariantViolation)
		}
		sequence, sequenced := d.terminalOperationSequences[id]
		if record.Operation.State.Terminal() != sequenced || (sequenced && sequence == 0) {
			return fmt.Errorf("operation %q terminal retention order is inconsistent: %w", id, ErrInvariantViolation)
		}
	}
	terminalOrders := make(map[uint64]operation.OperationID, len(d.terminalOperationSequences)+len(d.retiredOperations))
	var maximumTerminalOrder uint64
	for id, sequence := range d.terminalOperationSequences {
		if previous, exists := terminalOrders[sequence]; exists {
			return fmt.Errorf("operations %q and %q share terminal order %d: %w", previous, id, sequence, ErrInvariantViolation)
		}
		terminalOrders[sequence] = id
		if sequence > maximumTerminalOrder {
			maximumTerminalOrder = sequence
		}
	}
	for digest, retired := range d.retiredOperations {
		if digest != retired.OperationIDSHA256 {
			return fmt.Errorf("retired operation map key does not match digest: %w", ErrInvariantViolation)
		}
		if err := retired.validate(); err != nil {
			return err
		}
		if retired.TerminalSequence > d.lastTerminalOperationSequence {
			return fmt.Errorf("retired operation sequence exceeds terminal high watermark: %w", ErrInvariantViolation)
		}
		if previous, exists := terminalOrders[retired.TerminalSequence]; exists {
			return fmt.Errorf("operation %q and retired digest %q share terminal order %d: %w",
				previous, digest, retired.TerminalSequence, ErrInvariantViolation)
		}
		terminalOrders[retired.TerminalSequence] = operation.OperationID(digest)
		if retired.TerminalSequence > maximumTerminalOrder {
			maximumTerminalOrder = retired.TerminalSequence
		}
		if retired.TerminalEventSequence > d.lastEventSequence {
			return fmt.Errorf("retired operation terminal event %d exceeds event high watermark %d: %w",
				retired.TerminalEventSequence, d.lastEventSequence, ErrInvariantViolation)
		}
	}
	if maximumTerminalOrder != d.lastTerminalOperationSequence {
		return fmt.Errorf("terminal order high watermark is %d, want %d: %w",
			d.lastTerminalOperationSequence, maximumTerminalOrder, ErrInvariantViolation)
	}
	if uint64(len(terminalOrders)) != d.lastTerminalOperationSequence {
		return fmt.Errorf("terminal order retains %d identities for contiguous high watermark %d: %w",
			len(terminalOrders), d.lastTerminalOperationSequence, ErrInvariantViolation)
	}
	pairs := make([]domain.ContainerAttempt, 0, len(d.containerAttempts))
	for id, record := range d.sandboxes {
		if record.Sandbox.Status.Phase == domain.SandboxAbsent {
			return fmt.Errorf("sandbox %q persists absent phase instead of deleting its record: %w", id, ErrInvariantViolation)
		}
		if err := d.validateObservationReference(record.Sandbox.Status.LastObservation, eventsBySequence, operation.Target{Kind: operation.TargetSandbox, ID: string(id)}); err != nil {
			return fmt.Errorf("sandbox %q last observation: %w", id, err)
		}
		if err := validateCurrentObservationGeneration(record.Sandbox.Status.LastObservation, eventsBySequence,
			uint64(record.Sandbox.Status.Generation), uint64(record.Sandbox.Status.ObservedGeneration)); err != nil {
			return fmt.Errorf("sandbox %q last observation generation: %w", id, err)
		}
	}
	for id, record := range d.containerAttempts {
		pair := record.ContainerAttempt
		if pair.Attempt.Phase == domain.AttemptAbsent {
			return fmt.Errorf("container %q persists absent phase instead of deleting its pair: %w", id, ErrInvariantViolation)
		}
		if err := d.validateObservationReference(pair.Attempt.LastObservation, eventsBySequence,
			operation.Target{Kind: operation.TargetContainer, ID: string(pair.Container.ID)},
			operation.Target{Kind: operation.TargetAttempt, ID: string(pair.Attempt.ID)}); err != nil {
			return fmt.Errorf("container %q last observation: %w", id, err)
		}
		if err := validateCurrentObservationGeneration(pair.Attempt.LastObservation, eventsBySequence,
			uint64(pair.Container.Status.Generation), uint64(pair.Container.Status.ObservedGeneration)); err != nil {
			return fmt.Errorf("container %q last observation generation: %w", id, err)
		}
		sandbox, exists := d.sandboxes[pair.Container.SandboxID]
		if !exists {
			return fmt.Errorf("container %q references missing sandbox %q: %w", id, pair.Container.SandboxID, ErrInvariantViolation)
		}
		if domain.IsActiveAttempt(pair.Attempt.Phase) && sandbox.Sandbox.Status.Phase != domain.SandboxReady {
			return fmt.Errorf("active container %q belongs to non-Ready sandbox %q: %w", id, pair.Container.SandboxID, ErrInvariantViolation)
		}
		pairs = append(pairs, pair.Clone())
	}
	if err := domain.ValidateOneActiveAttempt(pairs); err != nil {
		return fmt.Errorf("container attempt set: %w: %v", ErrInvariantViolation, err)
	}
	for id, record := range d.sandboxes {
		sandbox := record.Sandbox
		if sandbox.Status.CurrentContainerID == nil {
			continue
		}
		pair, exists := d.containerAttempts[*sandbox.Status.CurrentContainerID]
		if !exists || pair.ContainerAttempt.Attempt.ID != *sandbox.Status.CurrentAttemptID ||
			pair.ContainerAttempt.Container.SandboxID != id {
			return fmt.Errorf("sandbox %q current Container/Attempt reference is dangling: %w", id, ErrInvariantViolation)
		}
	}
	for id, record := range d.containerAttempts {
		pair := record.ContainerAttempt
		if !domain.IsActiveAttempt(pair.Attempt.Phase) {
			continue
		}
		sandbox := d.sandboxes[pair.Container.SandboxID].Sandbox
		if sandbox.Status.CurrentContainerID == nil || *sandbox.Status.CurrentContainerID != id ||
			sandbox.Status.CurrentAttemptID == nil || *sandbox.Status.CurrentAttemptID != pair.Attempt.ID {
			return fmt.Errorf("active container %q is not the sandbox current pair: %w", id, ErrInvariantViolation)
		}
	}
	if err := d.validateOwnershipGraph(); err != nil {
		return err
	}
	return nil
}

// validateCurrentObservationGeneration checks the retained event named by a
// current resource projection against that exact incarnation. A compacted
// observation has no event payload left to compare and remains protected by
// its full operation record or tombstone in validateObservationReference.
func validateCurrentObservationGeneration(
	observation domain.LifecycleObservation,
	events map[uint64]operation.Event,
	generation uint64,
	observed uint64,
) error {
	if observation.EventSequence == 0 {
		return nil
	}
	event, exists := events[observation.EventSequence]
	if !exists {
		return nil
	}
	if event.Generation != generation || event.ObservedGeneration != observed {
		return fmt.Errorf("event generation %d/%d does not match current projection %d/%d: %w",
			event.Generation, event.ObservedGeneration, generation, observed, ErrInvariantViolation)
	}
	return nil
}

// validateObservationReference requires a non-empty status projection to name
// a retained event or an operation identity proven to predate the compacted prefix.
func (d memoryData) validateObservationReference(observation domain.LifecycleObservation, events map[uint64]operation.Event, resources ...operation.Target) error {
	if observation.EventSequence == 0 {
		return nil
	}
	event, exists := events[observation.EventSequence]
	if !exists {
		if EventSequence(observation.EventSequence) >= d.firstEventSequence || EventSequence(observation.EventSequence) > d.lastEventSequence {
			return fmt.Errorf("event sequence %d is missing from the retained suffix: %w", observation.EventSequence, ErrInvariantViolation)
		}
		id := operation.OperationID(observation.OperationID)
		if record, retained := d.operations[id]; retained {
			if string(record.Operation.Reason) != observation.Reason {
				return fmt.Errorf("compacted observation reason does not match operation %q: %w", id, ErrInvariantViolation)
			}
			if !targetMatchesOneOf(record.Operation.Target, resources) {
				return fmt.Errorf("compacted observation operation %q does not match the current resource target: %w", id, ErrInvariantViolation)
			}
			return nil
		}
		if retired, retained := d.retiredOperationFor(id); retained {
			if string(retired.Reason) != observation.Reason {
				return fmt.Errorf("compacted observation reason does not match retired operation: %w", ErrInvariantViolation)
			}
			if !targetMatchesOneOf(retired.Target, resources) {
				return fmt.Errorf("compacted observation retired operation does not match the current resource target: %w", ErrInvariantViolation)
			}
			return nil
		}
		return fmt.Errorf("compacted observation operation %q is not retained: %w", id, ErrInvariantViolation)
	}
	if string(event.OperationID) != observation.OperationID || string(event.Reason) != observation.Reason {
		return fmt.Errorf("event sequence %d identity or reason does not match projection: %w", observation.EventSequence, ErrInvariantViolation)
	}
	for _, required := range resources {
		found := false
		for _, resource := range event.Resources {
			if resource.Equal(required) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("event sequence %d does not reference %s/%s: %w", observation.EventSequence, required.Kind, required.ID, ErrInvariantViolation)
		}
	}
	return nil
}

// targetMatchesOneOf proves a compacted observation still belongs to the
// Sandbox or Container/Attempt record carrying that status projection.
func targetMatchesOneOf(target operation.Target, resources []operation.Target) bool {
	for _, resource := range resources {
		if target.Equal(resource) {
			return true
		}
	}
	return false
}

// close invalidates a callback-scoped view so asynchronous or retained callers
// cannot read or mutate state after the store lock has been released.
func (v *memoryView) close() {
	v.closed.Store(true)
}

// ensureOpen rejects use outside the callback lifetime before any map access.
func (v *memoryView) ensureOpen() error {
	if v.closed.Load() {
		return ErrTransactionClosed
	}
	return nil
}

// ensureWritable prevents mutation through a View while still sharing the same
// cloning and lookup implementation between read and update callbacks.
func (v *memoryView) ensureWritable() error {
	if err := v.ensureOpen(); err != nil {
		return err
	}
	if !v.writable {
		return fmt.Errorf("%w: read-only view", ErrInvalidRecord)
	}
	return nil
}

// GetSandbox returns an independent Sandbox record addressed by its durable ID.
func (v *memoryView) GetSandbox(id domain.SandboxID) (SandboxRecord, error) {
	if err := v.ensureOpen(); err != nil {
		return SandboxRecord{}, err
	}
	record, ok := v.data.sandboxes[id]
	if !ok {
		return SandboxRecord{}, fmt.Errorf("sandbox %q: %w", id, ErrNotFound)
	}
	return record.Clone(), nil
}

// ListSandboxes returns all Sandbox records ordered by ID for deterministic
// reconciliation and tests, never by Go map iteration order.
func (v *memoryView) ListSandboxes() ([]SandboxRecord, error) {
	if err := v.ensureOpen(); err != nil {
		return nil, err
	}
	ids := make([]domain.SandboxID, 0, len(v.data.sandboxes))
	for id := range v.data.sandboxes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	records := make([]SandboxRecord, 0, len(ids))
	for _, id := range ids {
		records = append(records, v.data.sandboxes[id].Clone())
	}
	return records, nil
}

// GetContainerAttempt returns the atomic one-to-one pair addressed by its
// Container ID, with no aliases to the store's nested slices or maps.
func (v *memoryView) GetContainerAttempt(containerID domain.ContainerID) (ContainerAttemptRecord, error) {
	if err := v.ensureOpen(); err != nil {
		return ContainerAttemptRecord{}, err
	}
	record, ok := v.data.containerAttempts[containerID]
	if !ok {
		return ContainerAttemptRecord{}, fmt.Errorf("container %q: %w", containerID, ErrNotFound)
	}
	return record.Clone(), nil
}

// ListContainerAttempts returns a Sandbox's historical pairs ordered by
// Container ID so the lifecycle layer can enforce one-active-Attempt rules.
func (v *memoryView) ListContainerAttempts(sandboxID domain.SandboxID) ([]ContainerAttemptRecord, error) {
	if err := v.ensureOpen(); err != nil {
		return nil, err
	}
	ids := make([]domain.ContainerID, 0)
	for id, record := range v.data.containerAttempts {
		if record.ContainerAttempt.Container.SandboxID == sandboxID {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	records := make([]ContainerAttemptRecord, 0, len(ids))
	for _, id := range ids {
		records = append(records, v.data.containerAttempts[id].Clone())
	}
	return records, nil
}

// GetOperation returns an independent durable operation record for resume or
// exact terminal replay by an idempotency coordinator.
func (v *memoryView) GetOperation(id operation.OperationID) (OperationRecord, error) {
	if err := v.ensureOpen(); err != nil {
		return OperationRecord{}, err
	}
	record, ok := v.data.operations[id]
	if !ok {
		if _, retired := v.data.retiredOperationFor(id); retired {
			return OperationRecord{}, fmt.Errorf("operation %q: %w", id, ErrOperationExpired)
		}
		return OperationRecord{}, fmt.Errorf("operation %q: %w", id, ErrNotFound)
	}
	return record.Clone(), nil
}

// ListOperations returns retained operation records ordered by operation ID so
// startup reconciliation also sees active deletes whose target metadata is absent.
func (v *memoryView) ListOperations() ([]OperationRecord, error) {
	if err := v.ensureOpen(); err != nil {
		return nil, err
	}
	ids := make([]operation.OperationID, 0, len(v.data.operations))
	for id := range v.data.operations {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	records := make([]OperationRecord, 0, len(ids))
	for _, id := range ids {
		records = append(records, v.data.operations[id].Clone())
	}
	return records, nil
}

// ActiveOperation finds the sole pending or running operation for a target. A
// multiple-match result is reported as corruption rather than chosen silently.
func (v *memoryView) ActiveOperation(target operation.Target) (OperationRecord, error) {
	if err := v.ensureOpen(); err != nil {
		return OperationRecord{}, err
	}
	if err := target.Validate(); err != nil {
		return OperationRecord{}, fmt.Errorf("active operation target: %w: %v", ErrInvalidRecord, err)
	}
	var match *OperationRecord
	for _, record := range v.data.operations {
		if !record.Operation.State.Active() || !record.Operation.Target.Equal(target) {
			continue
		}
		if match != nil {
			return OperationRecord{}, fmt.Errorf("target %s/%s has multiple active operations: %w", target.Kind, target.ID, ErrInvariantViolation)
		}
		clone := record.Clone()
		match = &clone
	}
	if match == nil {
		return OperationRecord{}, fmt.Errorf("active operation for %s/%s: %w", target.Kind, target.ID, ErrNotFound)
	}
	return match.Clone(), nil
}

// EventsAfter returns committed global events whose sequence is greater than
// after. A zero limit means all remaining events; a positive limit pages them.
func (v *memoryView) EventsAfter(after EventSequence, limit int) ([]operation.Event, error) {
	if err := v.ensureOpen(); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, ErrInvalidEventLimit
	}
	if after != 0 && (after > v.data.lastEventSequence ||
		(v.data.firstEventSequence > 1 && after < v.data.firstEventSequence-1)) {
		return nil, &EventResumeGapError{
			Requested: after, FirstAvailable: v.data.firstEventSequence, LastAvailable: v.data.lastEventSequence,
		}
	}
	events := make([]operation.Event, 0)
	for _, event := range v.data.events {
		if event.Sequence <= after {
			continue
		}
		events = append(events, event.Clone())
		if limit > 0 && len(events) == limit {
			break
		}
	}
	return events, nil
}

// PutSandbox creates or CAS-updates one Sandbox record and returns the committed
// candidate revision; the surrounding Update still controls final visibility.
func (v *memoryView) PutSandbox(record SandboxRecord, expectedRevision Revision) (SandboxRecord, error) {
	if err := v.ensureWritable(); err != nil {
		return SandboxRecord{}, err
	}
	if err := validateSandboxRecord(record); err != nil {
		return SandboxRecord{}, err
	}
	id := record.Sandbox.ID
	current, exists := v.data.sandboxes[id]
	next, err := nextRevision(expectedRevision, current.Revision, exists)
	if err != nil {
		return SandboxRecord{}, fmt.Errorf("sandbox %q: %w", id, err)
	}
	if exists {
		if !reflect.DeepEqual(current.Sandbox.Spec, record.Sandbox.Spec) ||
			current.Sandbox.Status.Generation != record.Sandbox.Status.Generation {
			return SandboxRecord{}, fmt.Errorf("sandbox %q immutable create input changed: %w", id, ErrInvalidRecord)
		}
		if err := validateSandboxAdvance(current.Sandbox, record.Sandbox); err != nil {
			return SandboxRecord{}, fmt.Errorf("sandbox %q status advance: %w", id, err)
		}
		if err := validateHostResourceAdvance(current.HostResources, record.HostResources); err != nil {
			return SandboxRecord{}, fmt.Errorf("sandbox %q host inventory: %w", id, err)
		}
	}
	stored := record.Clone()
	stored.Revision = next
	v.data.sandboxes[id] = stored
	return stored.Clone(), nil
}

// DeleteSandbox removes exactly the revision observed by the caller so a stale
// lifecycle operation cannot delete a concurrently advanced resource record.
func (v *memoryView) DeleteSandbox(id domain.SandboxID, expectedRevision Revision) error {
	if err := v.ensureWritable(); err != nil {
		return err
	}
	current, exists := v.data.sandboxes[id]
	if !exists {
		return fmt.Errorf("sandbox %q: %w", id, ErrNotFound)
	}
	if expectedRevision == 0 || current.Revision != expectedRevision {
		return fmt.Errorf("sandbox %q expected revision %d, current %d: %w", id, expectedRevision, current.Revision, ErrRevisionConflict)
	}
	if current.Sandbox.Status.Phase != domain.SandboxStopped && current.Sandbox.Status.Phase != domain.SandboxAbsent {
		return fmt.Errorf("sandbox %q must be stopped or transition to absent before deletion: %w", id, ErrInvalidRecord)
	}
	if len(current.HostResources) != 0 {
		return fmt.Errorf("sandbox %q still owns %d host resources: %w", id, len(current.HostResources), ErrInvalidRecord)
	}
	for _, pair := range v.data.containerAttempts {
		if pair.ContainerAttempt.Container.SandboxID == id {
			return fmt.Errorf("sandbox %q still owns container %q: %w", id, pair.ContainerAttempt.Container.ID, ErrInvalidRecord)
		}
	}
	delete(v.data.sandboxes, id)
	return nil
}

// PutContainerAttempt creates or CAS-updates a Container/Attempt pair as one
// record, preserving the M1 one-to-one identity under concurrent writers.
func (v *memoryView) PutContainerAttempt(record ContainerAttemptRecord, expectedRevision Revision) (ContainerAttemptRecord, error) {
	if err := v.ensureWritable(); err != nil {
		return ContainerAttemptRecord{}, err
	}
	if err := validateContainerAttemptRecord(record); err != nil {
		return ContainerAttemptRecord{}, err
	}
	id := record.ContainerAttempt.Container.ID
	current, exists := v.data.containerAttempts[id]
	next, err := nextRevision(expectedRevision, current.Revision, exists)
	if err != nil {
		return ContainerAttemptRecord{}, fmt.Errorf("container %q: %w", id, err)
	}
	if exists {
		if !sameContainerAttemptIdentityAndSpec(current.ContainerAttempt, record.ContainerAttempt) {
			return ContainerAttemptRecord{}, fmt.Errorf("container %q immutable identity or spec changed: %w", id, ErrInvalidRecord)
		}
		if err := validateContainerAttemptAdvance(current.ContainerAttempt, record.ContainerAttempt); err != nil {
			return ContainerAttemptRecord{}, fmt.Errorf("container %q status advance: %w", id, err)
		}
		if err := validateHostResourceAdvance(current.HostResources, record.HostResources); err != nil {
			return ContainerAttemptRecord{}, fmt.Errorf("container %q host inventory: %w", id, err)
		}
	}
	if err := v.validateContainerAttemptSet(record.ContainerAttempt); err != nil {
		return ContainerAttemptRecord{}, err
	}
	stored := record.Clone()
	stored.Revision = next
	v.data.containerAttempts[id] = stored
	return stored.Clone(), nil
}

// validateContainerAttemptSet rejects a duplicate Attempt identity or a second
// active pair in one Sandbox before either can become committed metadata.
func (v *memoryView) validateContainerAttemptSet(candidate domain.ContainerAttempt) error {
	pairs := make([]domain.ContainerAttempt, 0, len(v.data.containerAttempts)+1)
	for containerID, record := range v.data.containerAttempts {
		if containerID == candidate.Container.ID {
			continue
		}
		if record.ContainerAttempt.Attempt.ID == candidate.Attempt.ID {
			return fmt.Errorf("attempt %q already belongs to container %q: %w",
				candidate.Attempt.ID, containerID, ErrInvalidRecord)
		}
		pairs = append(pairs, record.ContainerAttempt.Clone())
	}
	pairs = append(pairs, candidate.Clone())
	if err := domain.ValidateOneActiveAttempt(pairs); err != nil {
		return fmt.Errorf("container attempt set: %w: %v", ErrInvalidRecord, err)
	}
	return nil
}

// validateSandboxAdvance permits same-phase condition/reference reconciliation or one legal forward FSM edge.
func validateSandboxAdvance(current, next domain.Sandbox) error {
	if next.Status.ObservedGeneration < current.Status.ObservedGeneration {
		return fmt.Errorf("observed generation regressed from %d to %d: %w",
			current.Status.ObservedGeneration, next.Status.ObservedGeneration, ErrInvalidRecord)
	}
	if next.Status.LastObservation.EventSequence < current.Status.LastObservation.EventSequence {
		return fmt.Errorf("last observation regressed from %d to %d: %w",
			current.Status.LastObservation.EventSequence, next.Status.LastObservation.EventSequence, ErrInvalidRecord)
	}
	if current.Status.Phase == next.Status.Phase {
		return nil
	}
	if _, err := domain.TransitionSandbox(current.Status.Phase, next.Status.Phase); err != nil {
		return fmt.Errorf("illegal phase transition: %w", ErrInvalidRecord)
	}
	return nil
}

// sameContainerAttemptIdentityAndSpec compares every create-time field that a lifecycle CAS may not rewrite.
func sameContainerAttemptIdentityAndSpec(current, next domain.ContainerAttempt) bool {
	return current.Container.SchemaVersion == next.Container.SchemaVersion &&
		current.Attempt.SchemaVersion == next.Attempt.SchemaVersion &&
		current.Container.ID == next.Container.ID &&
		current.Container.SandboxID == next.Container.SandboxID &&
		current.Container.AttemptID == next.Container.AttemptID &&
		current.Attempt.ID == next.Attempt.ID &&
		current.Attempt.SandboxID == next.Attempt.SandboxID &&
		current.Attempt.ContainerID == next.Attempt.ContainerID &&
		current.Container.Status.Generation == next.Container.Status.Generation &&
		reflect.DeepEqual(current.Container.Spec, next.Container.Spec)
}

// validateContainerAttemptAdvance permits reconciliation while rejecting phase rollback and terminal-fact rewrites.
func validateContainerAttemptAdvance(current, next domain.ContainerAttempt) error {
	if next.Container.Status.ObservedGeneration < current.Container.Status.ObservedGeneration {
		return fmt.Errorf("observed generation regressed from %d to %d: %w",
			current.Container.Status.ObservedGeneration, next.Container.Status.ObservedGeneration, ErrInvalidRecord)
	}
	if next.Attempt.LastObservation.EventSequence < current.Attempt.LastObservation.EventSequence {
		return fmt.Errorf("last observation regressed from %d to %d: %w",
			current.Attempt.LastObservation.EventSequence, next.Attempt.LastObservation.EventSequence, ErrInvalidRecord)
	}
	if current.Attempt.ProcessIdentity != nil && !reflect.DeepEqual(current.Attempt.ProcessIdentity, next.Attempt.ProcessIdentity) {
		return fmt.Errorf("process identity changed within one Attempt: %w", ErrInvalidRecord)
	}
	if current.Attempt.Phase != next.Attempt.Phase {
		if _, err := domain.TransitionAttempt(current.Attempt.Phase, next.Attempt.Phase); err != nil {
			return fmt.Errorf("illegal phase transition: %w", ErrInvalidRecord)
		}
	}
	if current.Attempt.Phase == domain.AttemptStopped && next.Attempt.Phase == domain.AttemptStopped {
		if !reflect.DeepEqual(current.Attempt.Outcome, next.Attempt.Outcome) ||
			!reflect.DeepEqual(current.Attempt.ProcessIdentity, next.Attempt.ProcessIdentity) ||
			current.Attempt.Streams != next.Attempt.Streams {
			return fmt.Errorf("terminal execution facts are immutable: %w", ErrInvalidRecord)
		}
	}
	return nil
}

// DeleteContainerAttempt removes the pair only when its CAS revision still
// matches, preventing torn deletion of Container and Attempt identities.
func (v *memoryView) DeleteContainerAttempt(containerID domain.ContainerID, expectedRevision Revision) error {
	if err := v.ensureWritable(); err != nil {
		return err
	}
	current, exists := v.data.containerAttempts[containerID]
	if !exists {
		return fmt.Errorf("container %q: %w", containerID, ErrNotFound)
	}
	if expectedRevision == 0 || current.Revision != expectedRevision {
		return fmt.Errorf("container %q expected revision %d, current %d: %w", containerID, expectedRevision, current.Revision, ErrRevisionConflict)
	}
	if current.ContainerAttempt.Attempt.Phase != domain.AttemptCreated &&
		current.ContainerAttempt.Attempt.Phase != domain.AttemptStopped {
		return fmt.Errorf("container %q must be created or stopped before deletion: %w", containerID, ErrInvalidRecord)
	}
	if len(current.HostResources) != 0 {
		return fmt.Errorf("container %q still owns %d host resources: %w", containerID, len(current.HostResources), ErrInvalidRecord)
	}
	delete(v.data.containerAttempts, containerID)
	return nil
}

// PutOperation creates or CAS-updates a durable intent while enforcing one
// active operation per target and immutable idempotency binding fields.
func (v *memoryView) PutOperation(record OperationRecord, expectedRevision Revision) (OperationRecord, error) {
	if err := v.ensureWritable(); err != nil {
		return OperationRecord{}, err
	}
	if err := validateOperationRecord(record); err != nil {
		return OperationRecord{}, err
	}
	id := record.Operation.ID
	current, exists := v.data.operations[id]
	if !exists {
		if _, retired := v.data.retiredOperationFor(id); retired {
			return OperationRecord{}, fmt.Errorf("operation %q: %w", id, ErrOperationExpired)
		}
		policy := v.retention
		if policy == (RetentionPolicy{}) {
			policy = DefaultRetentionPolicy()
		}
		if len(v.data.operations)+len(v.data.retiredOperations) >= policy.OperationIdentityLimit {
			return OperationRecord{}, fmt.Errorf("operation identity limit %d reached before accepting %q: %w", policy.OperationIdentityLimit, id, ErrRetentionCapacity)
		}
	}
	if !exists && record.HostProfile == HostProfileLinuxM2 && record.Operation.State.Active() &&
		(record.Operation.Stage != operation.StagePersistIntent || record.KillEscalationDeadline != nil || len(record.Rollback) != 0 || len(record.Receipts) != 0 || len(record.Releases) != 0) {
		return OperationRecord{}, fmt.Errorf("new Linux M2 operation must persist a side-effect-free intent before host progress: %w", ErrInvalidRecord)
	}
	next, err := nextRevision(expectedRevision, current.Revision, exists)
	if err != nil {
		return OperationRecord{}, fmt.Errorf("operation %q: %w", id, err)
	}
	if exists {
		if current.HostProfile != record.HostProfile {
			return OperationRecord{}, fmt.Errorf("operation %q host profile changed: %w", id, ErrInvalidRecord)
		}
		if _, err := operation.Resolve(&current.Operation, record.Operation.Binding()); err != nil {
			return OperationRecord{}, fmt.Errorf("operation %q changed its durable binding: %w: %w", id, ErrInvalidRecord, err)
		}
		if err := validateRollbackAdvance(current.Rollback, record.Rollback); err != nil {
			return OperationRecord{}, fmt.Errorf("operation %q rollback progress: %w", id, err)
		}
		if err := validateRollbackCauseAdvance(current.RollbackCause, record.RollbackCause); err != nil {
			return OperationRecord{}, fmt.Errorf("operation %q rollback cause: %w", id, err)
		}
		if err := validateOOMBaselineAdvance(current.OOMBaseline, record.OOMBaseline); err != nil {
			return OperationRecord{}, fmt.Errorf("operation %q OOM baseline: %w", id, err)
		}
		if err := validateKillEscalationDeadlineAdvance(current.KillEscalationDeadline, record.KillEscalationDeadline); err != nil {
			return OperationRecord{}, fmt.Errorf("operation %q Kill escalation deadline: %w", id, err)
		}
		if err := validateReceiptAdvance(current.Receipts, record.Receipts, current.Rollback, record.Rollback); err != nil {
			return OperationRecord{}, fmt.Errorf("operation %q acquisition receipts: %w", id, err)
		}
		if err := validateReleaseAdvance(current.Releases, record.Releases); err != nil {
			return OperationRecord{}, fmt.Errorf("operation %q cleanup releases: %w", id, err)
		}
		if err := validateOperationAdvance(current.Operation, record.Operation); err != nil {
			return OperationRecord{}, fmt.Errorf("operation %q progress: %w", id, err)
		}
		if err := validateHostProfileOperationAdvance(current, record); err != nil {
			return OperationRecord{}, fmt.Errorf("operation %q host progress: %w", id, err)
		}
		if current.Operation.State.Terminal() &&
			(!terminalOperationUpdateAllowed(current.Operation, record.Operation) ||
				!reflect.DeepEqual(current.RollbackCause, record.RollbackCause) ||
				!reflect.DeepEqual(current.OOMBaseline, record.OOMBaseline) ||
				!timesEqual(current.KillEscalationDeadline, record.KillEscalationDeadline) ||
				!reflect.DeepEqual(current.Rollback, record.Rollback) ||
				!reflect.DeepEqual(current.Receipts, record.Receipts) ||
				!reflect.DeepEqual(current.Releases, record.Releases)) {
			return OperationRecord{}, fmt.Errorf("operation %q terminal result is immutable: %w", id, ErrInvalidRecord)
		}
	}
	if record.Operation.State.Active() {
		for otherID, other := range v.data.operations {
			if otherID != id && other.Operation.State.Active() && other.Operation.Target.Equal(record.Operation.Target) {
				conflict := &operation.ActiveConflictError{Target: record.Operation.Target, ActiveID: otherID, RequestedID: id}
				return OperationRecord{}, fmt.Errorf("target %s/%s is owned by operation %q: %w: %w",
					record.Operation.Target.Kind, record.Operation.Target.ID, otherID, ErrActiveOperation, conflict)
			}
		}
	}
	stored := record.Clone()
	stored.Revision = next
	v.data.operations[id] = stored
	return stored.Clone(), nil
}

// AppendEvent assigns the next global sequence, validates operation association,
// and stages the cloned event for commit with the surrounding transaction.
func (v *memoryView) AppendEvent(event operation.Event) (operation.Event, error) {
	if err := v.ensureWritable(); err != nil {
		return operation.Event{}, err
	}
	if event.Sequence != 0 {
		return operation.Event{}, fmt.Errorf("caller supplied event sequence %d: %w", event.Sequence, ErrInvalidRecord)
	}
	record, exists := v.data.operations[event.OperationID]
	if !exists {
		return operation.Event{}, fmt.Errorf("event operation %q: %w", event.OperationID, ErrNotFound)
	}
	if event.Type != record.Operation.Type || !event.Target.Equal(record.Operation.Target) {
		return operation.Event{}, fmt.Errorf("event does not match operation %q binding: %w", event.OperationID, ErrInvalidRecord)
	}
	if event.Stage != record.Operation.Stage || event.Result != record.Operation.Result || event.Reason != record.Operation.Reason {
		return operation.Event{}, fmt.Errorf("event does not match operation %q durable progress: %w", event.OperationID, ErrInvalidRecord)
	}
	if err := v.validateEventGeneration(event); err != nil {
		return operation.Event{}, err
	}
	next, err := v.data.lastEventSequence.Next()
	if err != nil {
		return operation.Event{}, fmt.Errorf("global event sequence: %w", err)
	}
	stored := event.Clone()
	stored.Sequence = next
	if err := validateEvent(stored); err != nil {
		return operation.Event{}, err
	}
	if len(v.data.events) > 0 {
		if err := stored.ValidateAfter(v.data.events[len(v.data.events)-1]); err != nil {
			return operation.Event{}, fmt.Errorf("global event ordering: %w", err)
		}
	}
	v.data.events = append(v.data.events, stored)
	v.data.lastEventSequence = next
	return stored.Clone(), nil
}

// validateEventGeneration matches newly appended resource-backed events to the
// exact current generation projection when the target exists.
func (v *memoryView) validateEventGeneration(event operation.Event) error {
	generation, observed, found := v.eventTargetGeneration(event.Target)
	if found && (event.Generation != generation || event.ObservedGeneration != observed) {
		return fmt.Errorf("event generation %d/%d does not match resource %d/%d: %w",
			event.Generation, event.ObservedGeneration, generation, observed, ErrInvalidRecord)
	}
	return nil
}

// eventTargetGeneration resolves the current durable generation projection for
// a Sandbox, Container, or canonical Attempt without treating an absent target
// as proof that a historical event was invalid.
func (v *memoryView) eventTargetGeneration(target operation.Target) (uint64, uint64, bool) {
	var generation, observed uint64
	var found bool
	switch target.Kind {
	case operation.TargetSandbox:
		record, exists := v.data.sandboxes[domain.SandboxID(target.ID)]
		if exists {
			generation = uint64(record.Sandbox.Status.Generation)
			observed = uint64(record.Sandbox.Status.ObservedGeneration)
			found = true
		}
	case operation.TargetContainer:
		record, exists := v.data.containerAttempts[domain.ContainerID(target.ID)]
		if exists {
			generation = uint64(record.ContainerAttempt.Container.Status.Generation)
			observed = uint64(record.ContainerAttempt.Container.Status.ObservedGeneration)
			found = true
		}
	case operation.TargetAttempt:
		for _, record := range v.data.containerAttempts {
			if string(record.ContainerAttempt.Attempt.ID) == target.ID {
				generation = uint64(record.ContainerAttempt.Container.Status.Generation)
				observed = uint64(record.ContainerAttempt.Container.Status.ObservedGeneration)
				found = true
				break
			}
		}
	}
	return generation, observed, found
}

// validateSandboxRecord rejects unknown state envelopes and invalid domain
// values before a transaction candidate can contain unrecoverable metadata.
func validateSandboxRecord(record SandboxRecord) error {
	if err := validateSchema(record.SchemaVersion); err != nil {
		return err
	}
	if record.Sandbox.SchemaVersion != domain.SchemaVersion {
		return fmt.Errorf("sandbox schema %d: %w", record.Sandbox.SchemaVersion, ErrUnsupportedSchema)
	}
	if err := record.Sandbox.Validate(); err != nil {
		return fmt.Errorf("sandbox record: %w: %v", ErrInvalidRecord, err)
	}
	target := operation.Target{Kind: operation.TargetSandbox, ID: string(record.Sandbox.ID)}
	if err := validateHostResourceInventory(record.HostResources, record.Sandbox.Status.Generation, target); err != nil {
		return fmt.Errorf("sandbox host inventory: %w", err)
	}
	return nil
}

// validateContainerAttemptRecord preserves schema and one-to-one domain
// invariants before accepting a pair into the candidate transaction.
func validateContainerAttemptRecord(record ContainerAttemptRecord) error {
	if err := validateSchema(record.SchemaVersion); err != nil {
		return err
	}
	if record.ContainerAttempt.Container.SchemaVersion != domain.SchemaVersion ||
		record.ContainerAttempt.Attempt.SchemaVersion != domain.SchemaVersion {
		return fmt.Errorf("container/attempt schema %d/%d: %w",
			record.ContainerAttempt.Container.SchemaVersion,
			record.ContainerAttempt.Attempt.SchemaVersion,
			ErrUnsupportedSchema)
	}
	if err := record.ContainerAttempt.Validate(); err != nil {
		return fmt.Errorf("container attempt record: %w: %v", ErrInvalidRecord, err)
	}
	pair := record.ContainerAttempt
	containerTarget := operation.Target{Kind: operation.TargetContainer, ID: string(pair.Container.ID)}
	attemptTarget := operation.Target{Kind: operation.TargetAttempt, ID: string(pair.Attempt.ID)}
	if err := validateHostResourceInventory(record.HostResources, pair.Container.Status.Generation, containerTarget, attemptTarget); err != nil {
		return fmt.Errorf("container attempt host inventory: %w", err)
	}
	return nil
}

// validateOperationRecord maps an unknown operation or state envelope schema to
// the typed schema error and all other model failures to invalid-record.
func validateOperationRecord(record OperationRecord) error {
	if err := validateSchema(record.SchemaVersion); err != nil {
		return err
	}
	if !record.HostProfile.Valid() {
		return fmt.Errorf("operation host profile %q: %w", record.HostProfile, ErrInvalidRecord)
	}
	if record.Operation.SchemaVersion != operation.SchemaVersion {
		return fmt.Errorf("operation schema %d: %w", record.Operation.SchemaVersion, ErrUnsupportedSchema)
	}
	if err := record.Operation.Validate(); err != nil {
		return fmt.Errorf("operation record: %w: %v", ErrInvalidRecord, err)
	}
	if record.RollbackCause != nil {
		if err := record.RollbackCause.Validate(); err != nil {
			return fmt.Errorf("operation rollback cause: %w: %v", ErrInvalidRecord, err)
		}
		if record.Operation.Stage != operation.StageRollback && !record.Operation.State.Terminal() {
			return fmt.Errorf("active rollback cause requires rollback stage: %w", ErrInvalidRecord)
		}
	}
	if record.OOMBaseline != nil {
		if err := record.OOMBaseline.Validate(); err != nil {
			return fmt.Errorf("operation OOM baseline: %w: %v", ErrInvalidRecord, err)
		}
		if record.Operation.Type != operation.TypeCreate || record.Operation.Target.Kind != operation.TargetContainer {
			return fmt.Errorf("OOM baseline requires a Container create operation: %w", ErrInvalidRecord)
		}
		matched := false
		for _, receipt := range record.Receipts {
			if receipt.Kind == ownership.KindAttemptCgroup && receipt.Owner == record.OOMBaseline.Owner && receipt.EvidenceSHA256 == record.OOMBaseline.CgroupEvidenceSHA256 {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("OOM baseline does not match the acquired Attempt cgroup: %w", ErrInvalidRecord)
		}
	}
	if record.KillEscalationDeadline != nil {
		if record.KillEscalationDeadline.IsZero() {
			return fmt.Errorf("operation Kill escalation deadline is zero: %w", ErrInvalidRecord)
		}
		if record.Operation.Type != operation.TypeKill || record.Operation.Target.Kind != operation.TargetContainer {
			return fmt.Errorf("Kill escalation deadline requires a Container Kill operation: %w", ErrInvalidRecord)
		}
		if record.Operation.State.Active() && operationStageRank(record.Operation.Stage) < operationStageRank(operation.StageSignalProcess) {
			return fmt.Errorf("active Kill escalation deadline requires a signal checkpoint: %w", ErrInvalidRecord)
		}
	}
	if record.Operation.State.Active() && record.Operation.Type == operation.TypeKill &&
		record.Operation.Stage == operation.StageSignalProcess &&
		record.KillEscalationDeadline == nil {
		return fmt.Errorf("active signaled Kill operation is missing its escalation deadline: %w", ErrInvalidRecord)
	}
	if record.Operation.Stage == operation.StageRollback && record.RollbackCause == nil {
		return fmt.Errorf("rollback stage requires a primary failure cause: %w", ErrInvalidRecord)
	}
	if err := validateAcquisitionReceipts(record); err != nil {
		return err
	}
	if err := validateHostProfileRecord(record); err != nil {
		return err
	}
	if err := validateCleanupReleases(record); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(record.Rollback))
	for index, rollbackRecord := range record.Rollback {
		if err := rollbackRecord.Validate(); err != nil {
			return fmt.Errorf("operation rollback record %d: %w: %v", index, ErrInvalidRecord, err)
		}
		if _, exists := seen[rollbackRecord.Descriptor.Name]; exists {
			return fmt.Errorf("operation rollback record %d duplicates name %q: %w", index, rollbackRecord.Descriptor.Name, ErrInvalidRecord)
		}
		seen[rollbackRecord.Descriptor.Name] = struct{}{}
		if record.Operation.State.Terminal() && !rollbackRecord.Succeeded &&
			(rollbackRecord.Started || record.Operation.State == operation.StateFailed) {
			return fmt.Errorf("operation rollback record %d remains pending at terminal state: %w", index, ErrInvalidRecord)
		}
	}
	if err := validateReceiptRollbackLinks(record); err != nil {
		return err
	}
	if record.Operation.State == operation.StateSucceeded {
		if record.Operation.Result == operation.ResultNoop && len(record.Receipts) != 0 {
			return fmt.Errorf("noop operation acquired host resources: %w", ErrInvalidRecord)
		}
		for index, receipt := range record.Receipts {
			if !receipt.Adopted {
				return fmt.Errorf("operation receipt %d remains unadopted at successful terminal state: %w", index, ErrInvalidRecord)
			}
		}
		if record.Operation.Type == operation.TypeCreate && record.Operation.Result == operation.ResultSucceeded && record.HostProfile == HostProfileLinuxM2 {
			if err := ownership.ValidateReceiptJournalProfile(record.Operation.Target.Kind, record.Receipts); err != nil {
				return fmt.Errorf("successful create has incomplete M2 receipt profile: %w: %v", ErrInvalidRecord, err)
			}
		}
	}
	if record.Operation.State == operation.StateFailed {
		for index, receipt := range record.Receipts {
			if receipt.Adopted {
				return fmt.Errorf("failed operation receipt %d was adopted instead of rolled back: %w", index, ErrInvalidRecord)
			}
		}
	}
	if record.Operation.State.Active() {
		for index, receipt := range record.Receipts {
			if receipt.Adopted {
				return fmt.Errorf("active operation receipt %d was adopted before successful completion: %w", index, ErrInvalidRecord)
			}
		}
	}
	return nil
}

// validateHostProfileRecord separates pure M1 operations from Linux M2 acquisition and teardown evidence.
func validateHostProfileRecord(record OperationRecord) error {
	switch record.HostProfile {
	case HostProfileAbstractM1:
		if len(record.Receipts) != 0 || len(record.Releases) != 0 {
			return fmt.Errorf("abstract M1 operation cannot persist host receipts or releases: %w", ErrInvalidRecord)
		}
		return nil
	case HostProfileLinuxM2:
		if record.Operation.Type != operation.TypeCreate {
			if len(record.Receipts) != 0 {
				return fmt.Errorf("non-create Linux operation cannot acquire host resources: %w", ErrInvalidRecord)
			}
			return nil
		}
		if err := ownership.ValidateReceiptJournalPrefix(record.Operation.Target.Kind, record.Receipts); err != nil {
			return fmt.Errorf("Linux M2 receipt prefix: %w: %v", ErrInvalidRecord, err)
		}
		return validateReceiptStagePrefix(record.Operation.Target.Kind, record.Operation.Stage, record.Receipts)
	default:
		return fmt.Errorf("operation host profile %q: %w", record.HostProfile, ErrInvalidRecord)
	}
}

// validateReceiptStagePrefix prevents a checkpoint from skipping dependency slots or claiming progress before its last receipt exists.
func validateReceiptStagePrefix(targetKind operation.TargetKind, stage operation.Stage, receipts []ownership.Receipt) error {
	if len(receipts) == 0 {
		return nil
	}
	if stage == operation.StageRollback || stage == operation.StagePersistResult || stage == operation.StageComplete {
		return nil
	}
	lastMinimum := minimumReceiptStage(receipts[len(receipts)-1].Kind)
	if operationStageRank(stage) < operationStageRank(lastMinimum) {
		return fmt.Errorf("stage %q is before last receipt %q minimum %q: %w", stage, receipts[len(receipts)-1].Kind, lastMinimum, ErrInvalidRecord)
	}
	nextKind, exists := nextProfileKind(targetKind, len(receipts))
	if exists && operationStageRank(stage) > operationStageRank(minimumReceiptStage(nextKind)) {
		return fmt.Errorf("stage %q skipped pending receipt %q: %w", stage, nextKind, ErrInvalidRecord)
	}
	return nil
}

// minimumReceiptStage maps each M2 host role to the first stage at which its evidence may exist.
func minimumReceiptStage(kind ownership.Kind) operation.Stage {
	switch kind {
	case ownership.KindSandboxCgroup, ownership.KindAttemptCgroup, ownership.KindKeeperCgroup:
		return operation.StagePrepareCgroup
	case ownership.KindStartGate:
		return operation.StagePrepareStartGate
	case ownership.KindStreams:
		return operation.StagePrepareStreams
	case ownership.KindKeeperProcess, ownership.KindInitProcess:
		return operation.StageCreateProcess
	case ownership.KindUTSNamespace, ownership.KindIPCNamespace, ownership.KindNetworkNamespace,
		ownership.KindPIDNamespace, ownership.KindMountNamespace:
		return operation.StagePrepareNamespaces
	case ownership.KindRootfsMount:
		return operation.StagePrepareRootfs
	default:
		return operation.StageComplete
	}
}

// nextProfileKind returns the next required dependency after a canonical receipt prefix.
func nextProfileKind(targetKind operation.TargetKind, length int) (ownership.Kind, bool) {
	var kinds []ownership.Kind
	if targetKind == operation.TargetSandbox {
		kinds = []ownership.Kind{
			ownership.KindSandboxCgroup, ownership.KindKeeperCgroup, ownership.KindKeeperProcess, ownership.KindUTSNamespace,
			ownership.KindIPCNamespace, ownership.KindNetworkNamespace,
		}
	} else {
		kinds = []ownership.Kind{
			ownership.KindAttemptCgroup, ownership.KindStartGate, ownership.KindStreams,
			ownership.KindInitProcess, ownership.KindPIDNamespace, ownership.KindMountNamespace,
			ownership.KindRootfsMount,
		}
	}
	if length < 0 || length >= len(kinds) {
		return "", false
	}
	return kinds[length], true
}

// hostReceiptKey is the schema-level uniqueness key for a resource inventory;
// LocalID and evidence are immutable values of that single provider/kind slot.
type hostReceiptKey struct {
	provider ownership.Provider
	kind     ownership.Kind
}

// receiptKey returns the bounded provider/kind slot used to prevent ambiguous
// ownership evidence for the same host resource role.
func receiptKey(receipt ownership.Receipt) hostReceiptKey {
	return hostReceiptKey{provider: receipt.Provider, kind: receipt.Kind}
}

// validateReceiptValue distinguishes unknown receipt schemas from malformed
// provider evidence before either can enter an operation or resource record.
func validateReceiptValue(receipt ownership.Receipt) error {
	if receipt.SchemaVersion != ownership.SchemaVersion {
		return fmt.Errorf("ownership receipt schema %d: %w", receipt.SchemaVersion, ErrUnsupportedSchema)
	}
	if err := receipt.Validate(); err != nil {
		return fmt.Errorf("ownership receipt: %w: %v", ErrInvalidRecord, err)
	}
	return nil
}

// validateAcquisitionReceipts checks an operation's append-only journal values
// are unique and cryptographically bound to that operation's target generation.
func validateAcquisitionReceipts(record OperationRecord) error {
	seen := make(map[hostReceiptKey]struct{}, len(record.Receipts))
	for index, receipt := range record.Receipts {
		if err := validateReceiptValue(receipt); err != nil {
			return fmt.Errorf("operation receipt %d: %w", index, err)
		}
		if receipt.Owner.OperationID != record.Operation.ID || !receipt.Owner.Target.Equal(record.Operation.Target) {
			return fmt.Errorf("operation receipt %d owner does not match operation binding: %w", index, ErrInvalidRecord)
		}
		if receipt.Owner.Generation != domain.InitialGeneration {
			return fmt.Errorf("operation receipt %d owner generation %d does not match schema-v1 generation %d: %w",
				index, receipt.Owner.Generation, domain.InitialGeneration, ErrInvalidRecord)
		}
		if !receiptKindAllowedForTarget(receipt.Kind, record.Operation.Target.Kind) {
			return fmt.Errorf("operation receipt %d kind %q cannot belong to target %q: %w",
				index, receipt.Kind, record.Operation.Target.Kind, ErrInvalidRecord)
		}
		key := receiptKey(receipt)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("operation receipt %d duplicates provider/kind %s/%s: %w",
				index, receipt.Provider, receipt.Kind, ErrInvalidRecord)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// validateCleanupReleases binds every provider absence proof to the enclosing delete operation and one unique host role.
func validateCleanupReleases(record OperationRecord) error {
	if len(record.Releases) == 0 {
		return nil
	}
	if record.Operation.Type != operation.TypeDelete {
		return fmt.Errorf("only delete operations may persist cleanup releases: %w", ErrInvalidRecord)
	}
	if operationStageRank(record.Operation.Stage) < operationStageRank(operation.StageTeardown) {
		return fmt.Errorf("cleanup releases require teardown stage or later: %w", ErrInvalidRecord)
	}
	seen := make(map[hostReceiptKey]struct{}, len(record.Releases))
	for index, release := range record.Releases {
		if release.SchemaVersion != ownership.SchemaVersion {
			return fmt.Errorf("operation release %d schema %d: %w", index, release.SchemaVersion, ErrUnsupportedSchema)
		}
		if err := release.Validate(); err != nil {
			return fmt.Errorf("operation release %d: %w: %v", index, ErrInvalidRecord, err)
		}
		if release.CleanupOperationID != record.Operation.ID || !release.Resource.Owner.Target.Equal(record.Operation.Target) {
			return fmt.Errorf("operation release %d does not match delete binding: %w", index, ErrInvalidRecord)
		}
		key := receiptKey(release.Resource)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("operation release %d duplicates provider/kind %s/%s: %w",
				index, release.Resource.Provider, release.Resource.Kind, ErrInvalidRecord)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// validateHostResourceInventory accepts only adopted, unique receipts whose
// owner target and generation identify the enclosing durable resource record.
func validateHostResourceInventory(receipts []ownership.Receipt, generation domain.Generation, targets ...operation.Target) error {
	seen := make(map[hostReceiptKey]struct{}, len(receipts))
	for index, receipt := range receipts {
		if err := validateReceiptValue(receipt); err != nil {
			return fmt.Errorf("host resource %d: %w", index, err)
		}
		if !receipt.Adopted {
			return fmt.Errorf("host resource %d is not adopted: %w", index, ErrInvalidRecord)
		}
		if receipt.Owner.Generation != generation {
			return fmt.Errorf("host resource %d owner generation %d does not match record generation %d: %w",
				index, receipt.Owner.Generation, generation, ErrInvalidRecord)
		}
		matchedTarget := false
		for _, target := range targets {
			if receipt.Owner.Target.Equal(target) {
				matchedTarget = true
				break
			}
		}
		if !matchedTarget {
			return fmt.Errorf("host resource %d owner target does not match its record: %w", index, ErrInvalidRecord)
		}
		if !receiptKindAllowedForTarget(receipt.Kind, receipt.Owner.Target.Kind) {
			return fmt.Errorf("host resource %d kind %q cannot belong to target %q: %w",
				index, receipt.Kind, receipt.Owner.Target.Kind, ErrInvalidRecord)
		}
		key := receiptKey(receipt)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("host resource %d duplicates provider/kind %s/%s: %w",
				index, receipt.Provider, receipt.Kind, ErrInvalidRecord)
		}
		seen[key] = struct{}{}
	}
	if len(receipts) > 0 {
		if err := ownership.ValidateReceiptJournalProfile(receipts[0].Owner.Target.Kind, receipts); err != nil {
			return fmt.Errorf("host resource profile: %w: %v", ErrInvalidRecord, err)
		}
	}
	return nil
}

// receiptKindAllowedForTarget separates stable Sandbox host resources from
// per-Attempt execution resources even though both share one receipt schema.
func receiptKindAllowedForTarget(kind ownership.Kind, targetKind operation.TargetKind) bool {
	switch targetKind {
	case operation.TargetSandbox:
		switch kind {
		case ownership.KindSandboxCgroup, ownership.KindKeeperCgroup, ownership.KindKeeperProcess,
			ownership.KindUTSNamespace, ownership.KindIPCNamespace, ownership.KindNetworkNamespace:
			return true
		}
	case operation.TargetContainer, operation.TargetAttempt:
		switch kind {
		case ownership.KindAttemptCgroup, ownership.KindInitProcess,
			ownership.KindPIDNamespace, ownership.KindMountNamespace,
			ownership.KindRootfsMount, ownership.KindStartGate, ownership.KindStreams:
			return true
		}
	}
	return false
}

// validateReceiptRollbackLinks ensures every actionable acquisition has one
// bounded inverse descriptor and every ownership descriptor names journaled evidence.
func validateReceiptRollbackLinks(record OperationRecord) error {
	receipts := make(map[hostReceiptKey]ownership.Receipt, len(record.Receipts))
	for _, receipt := range record.Receipts {
		receipts[receiptKey(receipt)] = receipt
	}
	linked := make(map[hostReceiptKey]rollback.Record, len(record.Rollback))
	for index, rollbackRecord := range record.Rollback {
		action := ownership.Action(rollbackRecord.Descriptor.Action)
		if !action.Valid() {
			continue
		}
		receipt, _, err := ownership.ReceiptFromDescriptor(rollbackRecord.Descriptor)
		if err != nil {
			return fmt.Errorf("operation rollback record %d has invalid ownership descriptor: %w: %v",
				index, ErrInvalidRecord, err)
		}
		key := receiptKey(receipt)
		journaled, exists := receipts[key]
		if !exists || !sameReceiptIdentity(receipt, journaled) {
			return fmt.Errorf("operation rollback record %d has no matching acquisition receipt: %w", index, ErrInvalidRecord)
		}
		if _, exists := linked[key]; exists {
			return fmt.Errorf("operation rollback record %d duplicates inverse for %s/%s: %w",
				index, receipt.Provider, receipt.Kind, ErrInvalidRecord)
		}
		linked[key] = rollbackRecord
	}
	for index, receipt := range record.Receipts {
		key := receiptKey(receipt)
		if dependency, passive := passiveReceiptDependency(receipt.Kind); passive {
			key = hostReceiptKey{provider: ownership.ProviderLinux, kind: dependency}
			if _, exists := receipts[key]; !exists {
				return fmt.Errorf("operation receipt %d lacks its owning %s receipt: %w", index, dependency, ErrInvalidRecord)
			}
		} else if !receiptRequiresInverse(receipt.Kind) {
			continue
		}
		rollbackRecord, exists := linked[key]
		if !exists {
			return fmt.Errorf("operation receipt %d lacks a bounded rollback descriptor: %w", index, ErrInvalidRecord)
		}
		if record.Operation.State == operation.StateSucceeded && (rollbackRecord.Started || rollbackRecord.Succeeded) {
			return fmt.Errorf("operation receipt %d was both adopted and rolled back: %w", index, ErrInvalidRecord)
		}
	}
	if err := validateRollbackAcquisitionOrder(record); err != nil {
		return err
	}
	return nil
}

// validateRollbackAcquisitionOrder preserves actionable inverse descriptors in acquisition order so LIFO cleanup follows dependencies.
func validateRollbackAcquisitionOrder(record OperationRecord) error {
	if len(record.Receipts) == 0 {
		return nil
	}
	rollbackIndex := 0
	for receiptIndex, receipt := range record.Receipts {
		if !receiptRequiresInverse(receipt.Kind) {
			continue
		}
		if rollbackIndex >= len(record.Rollback) {
			return fmt.Errorf("operation receipt %d has no rollback entry at acquisition position: %w", receiptIndex, ErrInvalidRecord)
		}
		descriptorReceipt, _, err := ownership.ReceiptFromDescriptor(record.Rollback[rollbackIndex].Descriptor)
		if err != nil || !sameReceiptIdentity(descriptorReceipt, receipt) {
			return fmt.Errorf("operation rollback entry %d does not match actionable receipt %d order: %w",
				rollbackIndex, receiptIndex, ErrInvalidRecord)
		}
		rollbackIndex++
	}
	if rollbackIndex != len(record.Rollback) {
		return fmt.Errorf("operation has %d rollback entries after %d actionable acquisitions: %w",
			len(record.Rollback), rollbackIndex, ErrInvalidRecord)
	}
	return nil
}

// receiptRequiresInverse reports which host resource kinds have a direct
// provider cleanup action rather than sharing their owning process lifetime.
func receiptRequiresInverse(kind ownership.Kind) bool {
	switch kind {
	case ownership.KindSandboxCgroup, ownership.KindAttemptCgroup, ownership.KindKeeperCgroup,
		ownership.KindKeeperProcess, ownership.KindInitProcess,
		ownership.KindRootfsMount, ownership.KindStartGate, ownership.KindStreams:
		return true
	default:
		return false
	}
}

// passiveReceiptDependency maps namespace evidence to the process receipt whose
// successful inverse proves the namespace lifetime was cleaned after failure.
func passiveReceiptDependency(kind ownership.Kind) (ownership.Kind, bool) {
	switch kind {
	case ownership.KindUTSNamespace, ownership.KindIPCNamespace, ownership.KindNetworkNamespace:
		return ownership.KindKeeperProcess, true
	case ownership.KindPIDNamespace, ownership.KindMountNamespace:
		return ownership.KindInitProcess, true
	default:
		return "", false
	}
}

// sameReceiptIdentity compares immutable discovery and ownership evidence while
// deliberately excluding the one-way Adopted transfer marker.
func sameReceiptIdentity(left, right ownership.Receipt) bool {
	left = left.Clone()
	right = right.Clone()
	left.Adopted = false
	right.Adopted = false
	return reflect.DeepEqual(left, right)
}

// validateReceiptAdvance permits only append-only acquisition and false-to-true
// adoption; it rejects history changes and acquisition after rollback is sealed.
func validateReceiptAdvance(current, next []ownership.Receipt, currentRollback, nextRollback []rollback.Record) error {
	if len(next) < len(current) {
		return fmt.Errorf("acquisition receipt history shrank: %w", ErrInvalidRecord)
	}
	if len(next) > len(current)+1 {
		return fmt.Errorf("one checkpoint appended %d acquisition receipts; each host acquisition must be acknowledged separately: %w",
			len(next)-len(current), ErrInvalidRecord)
	}
	if len(next) > len(current) {
		for _, rollbackRecord := range append(append([]rollback.Record(nil), currentRollback...), nextRollback...) {
			if rollbackRecord.Started {
				return fmt.Errorf("acquisition receipt appended after rollback started: %w", ErrInvalidRecord)
			}
		}
		for index := len(current); index < len(next); index++ {
			if next[index].Adopted {
				return fmt.Errorf("new acquisition receipt %d bypassed pending ownership: %w", index, ErrInvalidRecord)
			}
		}
	}
	for index := range current {
		if !sameReceiptIdentity(current[index], next[index]) {
			return fmt.Errorf("acquisition receipt %d identity changed: %w", index, ErrInvalidRecord)
		}
		if current[index].Adopted && !next[index].Adopted {
			return fmt.Errorf("acquisition receipt %d adoption regressed: %w", index, ErrInvalidRecord)
		}
	}
	return nil
}

// validateReleaseAdvance keeps provider absence proof append-only and immutable across cleanup retries.
func validateReleaseAdvance(current, next []ownership.Release) error {
	if len(next) < len(current) {
		return fmt.Errorf("cleanup release history shrank: %w", ErrInvalidRecord)
	}
	for index := range current {
		if !reflect.DeepEqual(current[index], next[index]) {
			return fmt.Errorf("cleanup release %d changed after persistence: %w", index, ErrInvalidRecord)
		}
	}
	return nil
}

// validateHostResourceAdvance prevents an existing inventory from being rewritten or partially discarded.
// A complete clear remains candidate-valid here, but validateOwnershipGraph requires a terminal delete
// operation with exact provider absence proof before the surrounding transaction may commit it.
func validateHostResourceAdvance(current, next []ownership.Receipt) error {
	if len(current) != 0 && len(next) != 0 && len(next) != len(current) {
		return fmt.Errorf("host inventory must remain exact or be cleared atomically: %w", ErrInvalidRecord)
	}
	nextByKey := make(map[hostReceiptKey]ownership.Receipt, len(next))
	for _, receipt := range next {
		nextByKey[receiptKey(receipt)] = receipt
	}
	for _, receipt := range current {
		nextReceipt, exists := nextByKey[receiptKey(receipt)]
		if exists && !reflect.DeepEqual(receipt, nextReceipt) {
			return fmt.Errorf("provider/kind %s/%s ownership evidence changed: %w",
				receipt.Provider, receipt.Kind, ErrInvalidRecord)
		}
	}
	return nil
}

// validateOwnershipGraph verifies every resource inventory entry came from its
// named operation and that a completed create transferred exactly its receipts.
func (d memoryData) validateOwnershipGraph() error {
	for id, record := range d.sandboxes {
		for index, receipt := range record.HostResources {
			if err := d.validateInventoryProvenance(receipt); err != nil {
				return fmt.Errorf("sandbox %q host resource %d: %w", id, index, err)
			}
		}
	}
	for id, record := range d.containerAttempts {
		for index, receipt := range record.HostResources {
			if err := d.validateInventoryProvenance(receipt); err != nil {
				return fmt.Errorf("container %q host resource %d: %w", id, index, err)
			}
		}
	}
	for id, record := range d.operations {
		for index, receipt := range record.Receipts {
			if !receipt.Adopted {
				continue
			}
			inventory, exists := d.inventoryForTarget(receipt.Owner.Target)
			if (!exists || !receiptSetContains(inventory, receipt)) && !d.hasVerifiedRelease(record, receipt) {
				return fmt.Errorf("operation %q adopted receipt %d is absent from its resource inventory: %w",
					id, index, ErrInvariantViolation)
			}
		}
		if record.Operation.Type != operation.TypeCreate || record.Operation.State != operation.StateSucceeded ||
			len(record.Receipts) == 0 {
			continue
		}
		inventory, exists := d.inventoryForTarget(record.Operation.Target)
		if !exists && d.createHasCompleteVerifiedRelease(record) {
			continue
		}
		if !exists {
			return fmt.Errorf("successful create operation %q has receipts but no target inventory: %w", id, ErrInvariantViolation)
		}
		if !sameReceiptSet(record.Receipts, inventory) && !d.createHasCompleteVerifiedRelease(record) {
			return fmt.Errorf("successful create operation %q receipts do not exactly match target inventory: %w",
				id, ErrInvariantViolation)
		}
	}
	return nil
}

// validateInventoryProvenance requires an adopted inventory receipt to remain
// traceable to the immutable acquisition journal of its owner operation.
func (d memoryData) validateInventoryProvenance(receipt ownership.Receipt) error {
	record, exists := d.operations[receipt.Owner.OperationID]
	if !exists {
		return fmt.Errorf("owner operation %q is missing: %w", receipt.Owner.OperationID, ErrInvariantViolation)
	}
	if !record.Operation.Target.Equal(receipt.Owner.Target) {
		return fmt.Errorf("owner operation %q targets a different resource: %w",
			receipt.Owner.OperationID, ErrInvariantViolation)
	}
	for _, acquisition := range record.Receipts {
		if receiptKey(acquisition) == receiptKey(receipt) && acquisition.Adopted && reflect.DeepEqual(acquisition, receipt) {
			return nil
		}
	}
	return fmt.Errorf("owner operation %q has no matching adopted receipt: %w",
		receipt.Owner.OperationID, ErrInvariantViolation)
}

// inventoryForTarget resolves the record-level host inventory for a Sandbox,
// Container, or canonical Attempt without trusting a caller-provided relation.
func (d memoryData) inventoryForTarget(target operation.Target) ([]ownership.Receipt, bool) {
	switch target.Kind {
	case operation.TargetSandbox:
		record, exists := d.sandboxes[domain.SandboxID(target.ID)]
		return cloneReceipts(record.HostResources), exists
	case operation.TargetContainer:
		record, exists := d.containerAttempts[domain.ContainerID(target.ID)]
		return cloneReceipts(record.HostResources), exists
	case operation.TargetAttempt:
		for _, record := range d.containerAttempts {
			if string(record.ContainerAttempt.Attempt.ID) == target.ID {
				return cloneReceipts(record.HostResources), true
			}
		}
	}
	return nil, false
}

// hasVerifiedRelease reports whether one adopted receipt belongs to a create inventory
// consumed by an exact, successful delete operation with provider absence evidence.
func (d memoryData) hasVerifiedRelease(create OperationRecord, receipt ownership.Receipt) bool {
	if !d.createHasCompleteVerifiedRelease(create) {
		return false
	}
	for _, record := range d.operations {
		if !isSuccessfulResourceDelete(record, create.Operation.Target) {
			continue
		}
		for _, release := range record.Releases {
			if reflect.DeepEqual(release.Resource, receipt) {
				return true
			}
		}
	}
	return false
}

// createHasCompleteVerifiedRelease requires one terminal delete to cover the entire adopted create inventory exactly.
func (d memoryData) createHasCompleteVerifiedRelease(create OperationRecord) bool {
	for _, record := range d.operations {
		if !isSuccessfulResourceDelete(record, create.Operation.Target) || len(record.Releases) != len(create.Receipts) {
			continue
		}
		matched := true
		for _, receipt := range create.Receipts {
			found := false
			for _, release := range record.Releases {
				if reflect.DeepEqual(release.Resource, receipt) {
					found = true
					break
				}
			}
			if !found {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

// isSuccessfulResourceDelete identifies the only operation allowed to consume adopted host inventory.
func isSuccessfulResourceDelete(record OperationRecord, target operation.Target) bool {
	return record.Operation.Type == operation.TypeDelete &&
		record.Operation.Target.Equal(target) &&
		record.Operation.State == operation.StateSucceeded &&
		record.Operation.Result == operation.ResultSucceeded
}

// receiptSetContains locates one exact adopted receipt by its provider/kind
// identity without depending on persistence ordering.
func receiptSetContains(receipts []ownership.Receipt, want ownership.Receipt) bool {
	for _, receipt := range receipts {
		if receiptKey(receipt) == receiptKey(want) && reflect.DeepEqual(receipt, want) {
			return true
		}
	}
	return false
}

// sameReceiptSet compares complete adopted inventories by provider/kind rather
// than slice order, while still requiring identical owner and evidence fields.
func sameReceiptSet(left, right []ownership.Receipt) bool {
	if len(left) != len(right) {
		return false
	}
	for _, receipt := range left {
		if !receiptSetContains(right, receipt) {
			return false
		}
	}
	return true
}

// validateRollbackAdvance allows append-only acquisition descriptors and monotonic cleanup success.
func validateRollbackAdvance(current, next []rollback.Record) error {
	if len(next) < len(current) {
		return fmt.Errorf("rollback descriptor history shrank: %w", ErrInvalidRecord)
	}
	if len(next) > len(current) {
		for _, record := range current {
			if record.Started {
				return fmt.Errorf("rollback descriptor appended after cleanup started: %w", ErrInvalidRecord)
			}
		}
	}
	for index := range current {
		if !reflect.DeepEqual(current[index].Descriptor, next[index].Descriptor) {
			return fmt.Errorf("rollback descriptor %d changed: %w", index, ErrInvalidRecord)
		}
		if current[index].Succeeded && !next[index].Succeeded {
			return fmt.Errorf("rollback success %d regressed: %w", index, ErrInvalidRecord)
		}
		if current[index].Started && !next[index].Started {
			return fmt.Errorf("rollback-started marker %d regressed: %w", index, ErrInvalidRecord)
		}
		if next[index].Attempts < current[index].Attempts {
			return fmt.Errorf("rollback failure attempts %d regressed: %w", index, ErrInvalidRecord)
		}
		if next[index].Attempts == current[index].Attempts && next[index].LastError != current[index].LastError {
			return fmt.Errorf("rollback failure diagnostic %d changed without a new attempt: %w", index, ErrInvalidRecord)
		}
	}
	return nil
}

// validateRollbackCauseAdvance permits the primary create failure to appear
// exactly once when rollback begins and forbids later replacement or removal.
func validateRollbackCauseAdvance(current, next *rollback.Cause) error {
	if current == nil {
		return nil
	}
	if next == nil {
		return fmt.Errorf("rollback primary cause was removed: %w", ErrInvalidRecord)
	}
	if *current != *next {
		return fmt.Errorf("rollback primary cause changed: %w", ErrInvalidRecord)
	}
	return nil
}

// validateOOMBaselineAdvance allows one verified baseline append and keeps its scoped counters immutable afterward.
func validateOOMBaselineAdvance(current, next *provider.OOMSnapshot) error {
	if current == nil {
		return nil
	}
	if next == nil {
		return fmt.Errorf("OOM baseline was removed: %w", ErrInvalidRecord)
	}
	if *current != *next {
		return fmt.Errorf("OOM baseline changed: %w", ErrInvalidRecord)
	}
	return nil
}

// validateKillEscalationDeadlineAdvance permits one absolute grace deadline append and forbids later removal or drift.
func validateKillEscalationDeadlineAdvance(current, next *time.Time) error {
	if current == nil {
		return nil
	}
	if next == nil {
		return fmt.Errorf("Kill escalation deadline was removed: %w", ErrInvalidRecord)
	}
	if !current.Equal(*next) {
		return fmt.Errorf("Kill escalation deadline changed: %w", ErrInvalidRecord)
	}
	return nil
}

// timesEqual compares optional persisted wall-clock instants by instant rather than location representation.
func timesEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

// validateOperationAdvance rejects state or stage regression while allowing direct terminal no-op completion.
func validateOperationAdvance(current, next operation.Operation) error {
	if current.State.Terminal() {
		return nil
	}
	responseInitializedWithIntent := current.State == operation.StatePending && current.Stage == operation.StageValidate && len(current.Response) == 0 &&
		next.State == operation.StateRunning && next.Stage == operation.StagePersistIntent && len(next.Response) != 0
	if current.State.Active() && next.State.Active() && !bytes.Equal(current.Response, next.Response) && !responseInitializedWithIntent {
		return fmt.Errorf("active operation response changed after durable intent: %w", ErrInvalidRecord)
	}
	if current.State == operation.StateRunning && next.State == operation.StatePending {
		return fmt.Errorf("operation state regressed from running to pending: %w", ErrInvalidRecord)
	}
	if current.State == next.State && next.State.Active() && operationStageRank(next.Stage) < operationStageRank(current.Stage) {
		return fmt.Errorf("operation stage regressed from %s to %s: %w", current.Stage, next.Stage, ErrInvalidRecord)
	}
	return nil
}

// validateHostProfileOperationAdvance makes Linux Start acknowledgements non-skippable even for callers using Store directly.
func validateHostProfileOperationAdvance(current, next OperationRecord) error {
	if current.HostProfile != HostProfileLinuxM2 || current.Operation.Type != operation.TypeStart || current.Operation.Stage == next.Operation.Stage {
		return nil
	}
	currentRank := operationStageRank(current.Operation.Stage)
	nextRank := operationStageRank(next.Operation.Stage)
	attachRank := operationStageRank(operation.StageAttachCgroup)
	releaseRank := operationStageRank(operation.StageReleaseStartGate)
	observeRank := operationStageRank(operation.StageObserveProcess)
	switch {
	case currentRank < attachRank && nextRank > attachRank:
		return fmt.Errorf("Linux M2 Start skipped cgroup attachment: %w", ErrInvalidRecord)
	case currentRank == attachRank && nextRank > attachRank && next.Operation.Stage != operation.StageReleaseStartGate:
		return fmt.Errorf("Linux M2 Start skipped start-gate release: %w", ErrInvalidRecord)
	case currentRank >= releaseRank && currentRank < observeRank && nextRank > releaseRank && next.Operation.Stage != operation.StageObserveProcess:
		return fmt.Errorf("Linux M2 Start skipped process observation: %w", ErrInvalidRecord)
	}
	return nil
}

// operationStageRank orders durable lifecycle and host checkpoints for monotonic active-operation reconciliation.
func operationStageRank(stage operation.Stage) int {
	switch stage {
	case operation.StageValidate:
		return 0
	case operation.StagePersistIntent:
		return 1
	case operation.StageCheckPreconditions:
		return 2
	case operation.StageHostPreflight:
		return 3
	case operation.StagePrepareCgroup:
		return 4
	case operation.StagePrepareStartGate:
		return 5
	case operation.StagePrepareStreams:
		return 6
	case operation.StageCreateProcess:
		return 7
	case operation.StagePrepareNamespaces:
		return 8
	case operation.StageJoinNamespaces:
		return 9
	case operation.StagePrepareRootfs:
		return 10
	case operation.StageAttachCgroup:
		return 11
	case operation.StageReleaseStartGate:
		return 12
	case operation.StageSignalProcess:
		return 13
	case operation.StageObserveProcess:
		return 14
	case operation.StageTeardown:
		return 15
	case operation.StageTransition:
		return 16
	case operation.StagePersistState:
		return 17
	case operation.StageRollback:
		return 18
	case operation.StagePersistResult:
		return 19
	case operation.StageComplete:
		return 20
	default:
		return -1
	}
}

// validateEvent distinguishes unsupported persisted event schemas from other
// malformed event fields so recovery callers can make a safe typed decision.
func validateEvent(event operation.Event) error {
	if event.SchemaVersion != operation.EventSchemaVersion {
		return fmt.Errorf("event schema %d: %w", event.SchemaVersion, ErrUnsupportedSchema)
	}
	if err := event.Validate(); err != nil {
		return fmt.Errorf("event: %w: %v", ErrInvalidRecord, err)
	}
	return nil
}

// validateSchema accepts only the explicit v1 envelope and never guesses a
// migration for zero or future versions.
func validateSchema(version SchemaVersion) error {
	if version != SchemaVersionV1 {
		return fmt.Errorf("state schema %d: %w", version, ErrUnsupportedSchema)
	}
	return nil
}

// nextRevision applies create-or-CAS semantics and detects overflow before a
// candidate record is mutated.
func nextRevision(expected, current Revision, exists bool) (Revision, error) {
	if !exists {
		if expected != 0 {
			return 0, fmt.Errorf("expected missing record at revision zero, got %d: %w", expected, ErrRevisionConflict)
		}
		return 1, nil
	}
	if expected == 0 || expected != current {
		return 0, fmt.Errorf("expected revision %d, current %d: %w", expected, current, ErrRevisionConflict)
	}
	if current == ^Revision(0) {
		return 0, fmt.Errorf("revision overflow: %w", ErrRevisionConflict)
	}
	return current + 1, nil
}

// sameOperationBinding compares the fields permanently bound to an operation
// ID throughout retention, excluding progress and replay result fields.
func sameOperationBinding(left, right operation.Operation) bool {
	return left.ID == right.ID &&
		left.Type == right.Type &&
		left.Target.Equal(right.Target) &&
		left.Fingerprint.Equal(right.Fingerprint)
}

// sameOperation compares a terminal record including response bytes so a
// completed result cannot be silently rewritten under the same operation ID.
func sameOperation(left, right operation.Operation) bool {
	return sameOperationBinding(left, right) &&
		left.SchemaVersion == right.SchemaVersion &&
		left.State == right.State &&
		left.Stage == right.Stage &&
		left.Result == right.Result &&
		left.Reason == right.Reason &&
		bytes.Equal(left.Response, right.Response)
}

// terminalOperationUpdateAllowed permits one atomic response finalization after event projection, then freezes the result.
func terminalOperationUpdateAllowed(current, next operation.Operation) bool {
	if sameOperation(current, next) {
		return true
	}
	return len(current.Response) == 0 && len(next.Response) > 0 &&
		sameOperationBinding(current, next) &&
		current.SchemaVersion == next.SchemaVersion &&
		current.State == next.State &&
		current.Stage == next.Stage &&
		current.Result == next.Result &&
		current.Reason == next.Reason
}
