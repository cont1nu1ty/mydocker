package state

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
)

// TestMemoryStoreUpdateAtomicity verifies that an error discards a mixed
// operation/event transaction and does not consume the first event sequence.
func TestMemoryStoreUpdateAtomicity(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-atomic", "sandbox-atomic")
	event := testEvent(op)
	sandbox := testReadySandbox(t, "sandbox-atomic")
	event.Generation = uint64(sandbox.Status.Generation)
	event.ObservedGeneration = uint64(sandbox.Status.ObservedGeneration)
	wantErr := errors.New("injected callback failure")

	err := store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutSandbox(NewSandboxRecord(sandbox), 0); err != nil {
			return err
		}
		if _, err := tx.PutOperation(NewOperationRecord(op), 0); err != nil {
			return err
		}
		if _, err := tx.AppendEvent(event); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Update() error = %v, want injected error", err)
	}

	err = store.View(context.Background(), func(reader Reader) error {
		if _, err := reader.GetSandbox(sandbox.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetSandbox() after rollback error = %v, want ErrNotFound", err)
		}
		if _, err := reader.GetOperation(op.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetOperation() after rollback error = %v, want ErrNotFound", err)
		}
		events, err := reader.EventsAfter(0, 0)
		if err != nil {
			return err
		}
		if len(events) != 0 {
			t.Fatalf("EventsAfter() after rollback returned %d events, want 0", len(events))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() after rollback error = %v", err)
	}

	var committed operation.Event
	err = store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutOperation(NewOperationRecord(op), 0); err != nil {
			return err
		}
		var err error
		committed, err = tx.AppendEvent(event)
		return err
	})
	if err != nil {
		t.Fatalf("Update() commit error = %v", err)
	}
	if committed.Sequence != 1 {
		t.Fatalf("first committed event sequence = %d, want 1", committed.Sequence)
	}
}

// TestMemoryStoreCanceledUpdateRollsBack verifies cancellation after callback
// work but before commit discards the candidate transaction.
func TestMemoryStoreCanceledUpdateRollsBack(t *testing.T) {
	store := NewMemoryStore()
	sandbox := testReadySandbox(t, "sandbox-canceled")
	ctx, cancel := context.WithCancel(context.Background())
	err := store.Update(ctx, func(tx Tx) error {
		if _, err := tx.PutSandbox(NewSandboxRecord(sandbox), 0); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Update() canceled error = %v, want context.Canceled", err)
	}
	err = store.View(context.Background(), func(reader Reader) error {
		_, err := reader.GetSandbox(sandbox.ID)
		return err
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetSandbox() after canceled commit error = %v, want ErrNotFound", err)
	}
}

// TestMemoryStoreOperationCAS verifies create/update revisions and rejects a
// writer that retries with a stale store revision.
func TestMemoryStoreOperationCAS(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-cas", "sandbox-cas")
	created := putTestOperation(t, store, op)
	if created.Revision != 1 {
		t.Fatalf("created revision = %d, want 1", created.Revision)
	}

	updatedValue := created.Operation.Clone()
	updatedValue.State = operation.StateRunning
	updatedValue.Stage = operation.StageTransition
	updated := created.Clone()
	updated.Operation = updatedValue
	err := store.Update(context.Background(), func(tx Tx) error {
		var err error
		updated, err = tx.PutOperation(updated, created.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("Update() current CAS error = %v", err)
	}
	if updated.Revision != 2 {
		t.Fatalf("updated revision = %d, want 2", updated.Revision)
	}

	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(created, created.Revision)
		return err
	})
	if !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("Update() stale CAS error = %v, want ErrRevisionConflict", err)
	}
}

// TestMemoryStoreRejectsUnknownSchema verifies both the state envelope and the
// operation/event schemas fail closed instead of being guessed or migrated.
func TestMemoryStoreRejectsUnknownSchema(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-schema", "sandbox-schema")

	record := NewOperationRecord(op)
	record.SchemaVersion = 99
	err := store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(record, 0)
		return err
	})
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("PutOperation() state schema error = %v, want ErrUnsupportedSchema", err)
	}

	record = NewOperationRecord(op)
	record.Operation.SchemaVersion = 99
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(record, 0)
		return err
	})
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("PutOperation() operation schema error = %v, want ErrUnsupportedSchema", err)
	}

	putTestOperation(t, store, op)
	event := testEvent(op)
	event.SchemaVersion = 99
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.AppendEvent(event)
		return err
	})
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("AppendEvent() schema error = %v, want ErrUnsupportedSchema", err)
	}

	sandboxRecord := NewSandboxRecord(testReadySandbox(t, "sandbox-schema-domain"))
	sandboxRecord.Sandbox.SchemaVersion = 99
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutSandbox(sandboxRecord, 0)
		return err
	})
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("PutSandbox() domain schema error = %v, want ErrUnsupportedSchema", err)
	}
}

// TestMemoryStoreDomainRecords verifies Sandbox and Container/Attempt records
// are stored atomically, deeply cloned, and revised without changing generation.
func TestMemoryStoreDomainRecords(t *testing.T) {
	store := NewMemoryStore()
	sandbox := testReadySandbox(t, "sandbox-domain")
	pair, err := domain.NewContainerAttempt(
		sandbox,
		domain.ContainerID("container-domain"),
		domain.AttemptID("attempt-domain"),
		domain.ProcessSpec{Argv: []string{"/bin/work", "argument"}},
		"sha256:image",
		"/prepared/rootfs",
	)
	if err != nil {
		t.Fatalf("NewContainerAttempt() setup error = %v", err)
	}
	if err := sandbox.SetCurrentPair(pair.Container.ID, pair.Attempt.ID); err != nil {
		t.Fatalf("SetCurrentPair() setup error = %v", err)
	}
	sandboxRecord := NewSandboxRecord(sandbox)
	pairRecord := NewContainerAttemptRecord(pair)

	err = store.Update(context.Background(), func(tx Tx) error {
		var err error
		sandboxRecord, err = tx.PutSandbox(sandboxRecord, 0)
		if err != nil {
			return err
		}
		pairRecord, err = tx.PutContainerAttempt(pairRecord, 0)
		return err
	})
	if err != nil {
		t.Fatalf("Update() domain records error = %v", err)
	}
	if sandboxRecord.Revision != 1 || pairRecord.Revision != 1 {
		t.Fatalf("initial revisions = sandbox %d, pair %d; want 1 and 1", sandboxRecord.Revision, pairRecord.Revision)
	}
	if sandboxRecord.Sandbox.Status.Generation != domain.InitialGeneration {
		t.Fatalf("Sandbox generation = %d, want immutable initial generation", sandboxRecord.Sandbox.Status.Generation)
	}

	sandboxRecord.Sandbox.Spec.Labels["role"] = "mutated-outside-store"
	pairRecord.ContainerAttempt.Container.Spec.Process.Argv[0] = "/mutated"
	err = store.View(context.Background(), func(reader Reader) error {
		storedSandbox, err := reader.GetSandbox(sandbox.ID)
		if err != nil {
			return err
		}
		if got := storedSandbox.Sandbox.Spec.Labels["role"]; got != "worker" {
			t.Fatalf("stored Sandbox label = %q, want worker", got)
		}
		storedPair, err := reader.GetContainerAttempt(pair.Container.ID)
		if err != nil {
			return err
		}
		if got := storedPair.ContainerAttempt.Container.Spec.Process.Argv[0]; got != "/bin/work" {
			t.Fatalf("stored argv[0] = %q, want /bin/work", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() deep-copy check error = %v", err)
	}

	var current SandboxRecord
	err = store.View(context.Background(), func(reader Reader) error {
		var err error
		current, err = reader.GetSandbox(sandbox.ID)
		return err
	})
	if err != nil {
		t.Fatalf("View() current Sandbox error = %v", err)
	}
	current.Sandbox.Status.Conditions = append(current.Sandbox.Status.Conditions, domain.Condition{Type: "Observed", Reason: "test"})
	err = store.Update(context.Background(), func(tx Tx) error {
		var err error
		current, err = tx.PutSandbox(current, current.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("Update() Sandbox status error = %v", err)
	}
	if current.Revision != 2 {
		t.Fatalf("Sandbox CAS revision = %d, want 2", current.Revision)
	}
	if current.Sandbox.Status.Generation != domain.InitialGeneration {
		t.Fatalf("Sandbox generation after CAS update = %d, want %d", current.Sandbox.Status.Generation, domain.InitialGeneration)
	}
}

// TestMemoryStoreDomainDeleteCAS verifies stale cleanup cannot remove a newer
// Sandbox or tear apart the atomic Container/Attempt record.
func TestMemoryStoreDomainDeleteCAS(t *testing.T) {
	store := NewMemoryStore()
	sandbox := testReadySandbox(t, "sandbox-delete")
	pair := testContainerAttempt(t, sandbox, "container-delete", "attempt-delete")
	if err := pair.Transition(domain.AttemptStopped, domain.NotApplicableOutcome()); err != nil {
		t.Fatalf("Transition(Stopped) setup error = %v", err)
	}
	if err := sandbox.SetCurrentPair(pair.Container.ID, pair.Attempt.ID); err != nil {
		t.Fatalf("SetCurrentPair() setup error = %v", err)
	}
	if err := sandbox.Transition(domain.SandboxStopping); err != nil {
		t.Fatalf("Transition(Stopping) setup error = %v", err)
	}
	if err := sandbox.Transition(domain.SandboxStopped); err != nil {
		t.Fatalf("Transition(Stopped) setup error = %v", err)
	}
	var sandboxRecord SandboxRecord
	var pairRecord ContainerAttemptRecord
	err := store.Update(context.Background(), func(tx Tx) error {
		var err error
		sandboxRecord, err = tx.PutSandbox(NewSandboxRecord(sandbox), 0)
		if err != nil {
			return err
		}
		pairRecord, err = tx.PutContainerAttempt(NewContainerAttemptRecord(pair), 0)
		return err
	})
	if err != nil {
		t.Fatalf("Update() setup error = %v", err)
	}

	err = store.Update(context.Background(), func(tx Tx) error {
		return tx.DeleteContainerAttempt(pair.Container.ID, pairRecord.Revision+1)
	})
	if !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("DeleteContainerAttempt() stale error = %v, want ErrRevisionConflict", err)
	}
	err = store.Update(context.Background(), func(tx Tx) error {
		if err := tx.DeleteContainerAttempt(pair.Container.ID, pairRecord.Revision); err != nil {
			return err
		}
		return tx.DeleteSandbox(sandbox.ID, sandboxRecord.Revision)
	})
	if err != nil {
		t.Fatalf("Update() exact-revision deletes error = %v", err)
	}
	err = store.View(context.Background(), func(reader Reader) error {
		if _, err := reader.GetContainerAttempt(pair.Container.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetContainerAttempt() after delete error = %v, want ErrNotFound", err)
		}
		if _, err := reader.GetSandbox(sandbox.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetSandbox() after delete error = %v, want ErrNotFound", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() after delete error = %v", err)
	}
}

// TestMemoryStoreListsDeterministically verifies reconciliation inputs are
// ordered by stable IDs rather than randomized map iteration order.
func TestMemoryStoreListsDeterministically(t *testing.T) {
	store := NewMemoryStore()
	sandboxB := testReadySandbox(t, "sandbox-b")
	sandboxA := testReadySandbox(t, "sandbox-a")
	pairB := testContainerAttempt(t, sandboxA, "container-b", "attempt-b")
	pairA := testContainerAttempt(t, sandboxA, "container-a", "attempt-a")
	setPairPhase(&pairB, domain.AttemptStopped)
	setPairPhase(&pairA, domain.AttemptStopped)
	err := store.Update(context.Background(), func(tx Tx) error {
		for _, sandbox := range []domain.Sandbox{sandboxB, sandboxA} {
			if _, err := tx.PutSandbox(NewSandboxRecord(sandbox), 0); err != nil {
				return err
			}
		}
		for _, pair := range []domain.ContainerAttempt{pairB, pairA} {
			if _, err := tx.PutContainerAttempt(NewContainerAttemptRecord(pair), 0); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update() list setup error = %v", err)
	}
	err = store.View(context.Background(), func(reader Reader) error {
		sandboxes, err := reader.ListSandboxes()
		if err != nil {
			return err
		}
		if len(sandboxes) != 2 || sandboxes[0].Sandbox.ID != sandboxA.ID || sandboxes[1].Sandbox.ID != sandboxB.ID {
			t.Fatalf("ListSandboxes() order = %#v, want sandbox-a then sandbox-b", sandboxes)
		}
		pairs, err := reader.ListContainerAttempts(sandboxA.ID)
		if err != nil {
			return err
		}
		if len(pairs) != 2 || pairs[0].ContainerAttempt.Container.ID != pairA.Container.ID ||
			pairs[1].ContainerAttempt.Container.ID != pairB.Container.ID {
			t.Fatalf("ListContainerAttempts() order = %#v, want container-a then container-b", pairs)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() deterministic lists error = %v", err)
	}
}

// TestMemoryStoreRejectsSecondActiveAttempt verifies persistence cannot commit
// a state set that violates the one-active-Attempt Sandbox invariant.
func TestMemoryStoreRejectsSecondActiveAttempt(t *testing.T) {
	store := NewMemoryStore()
	sandbox := testReadySandbox(t, "sandbox-one-active")
	first := testContainerAttempt(t, sandbox, "container-first", "attempt-first")
	second := testContainerAttempt(t, sandbox, "container-second", "attempt-second")
	err := store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutContainerAttempt(NewContainerAttemptRecord(first), 0); err != nil {
			return err
		}
		_, err := tx.PutContainerAttempt(NewContainerAttemptRecord(second), 0)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutContainerAttempt() second-active error = %v, want ErrInvalidRecord", err)
	}
	err = store.View(context.Background(), func(reader Reader) error {
		pairs, err := reader.ListContainerAttempts(sandbox.ID)
		if err != nil {
			return err
		}
		if len(pairs) != 0 {
			t.Fatalf("rolled-back active pairs = %d, want 0", len(pairs))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() after active invariant rollback error = %v", err)
	}
}

// TestMemoryStoreDeepCopiesOperationAndEvent verifies caller mutations before
// and after reads cannot change stored replay responses or event details.
func TestMemoryStoreDeepCopiesOperationAndEvent(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-clone", "sandbox-clone")
	op.Response = []byte(`{"resource":"original"}`)
	event := testEvent(op)
	event.Details = []byte(`{"detail":"original"}`)

	err := store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutOperation(NewOperationRecord(op), 0); err != nil {
			return err
		}
		_, err := tx.AppendEvent(event)
		return err
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	op.Response[13] = 'X'
	event.Details[11] = 'X'

	err = store.View(context.Background(), func(reader Reader) error {
		record, err := reader.GetOperation(op.ID)
		if err != nil {
			return err
		}
		if got := string(record.Operation.Response); got != `{"resource":"original"}` {
			t.Fatalf("stored response = %s, want original", got)
		}
		events, err := reader.EventsAfter(0, 0)
		if err != nil {
			return err
		}
		if got := string(events[0].Details); got != `{"detail":"original"}` {
			t.Fatalf("stored details = %s, want original", got)
		}
		record.Operation.Response[13] = 'Y'
		events[0].Details[11] = 'Y'
		return nil
	})
	if err != nil {
		t.Fatalf("View() first read error = %v", err)
	}

	err = store.View(context.Background(), func(reader Reader) error {
		record, err := reader.GetOperation(op.ID)
		if err != nil {
			return err
		}
		if got := string(record.Operation.Response); got != `{"resource":"original"}` {
			t.Fatalf("stored response after returned-copy mutation = %s, want original", got)
		}
		events, err := reader.EventsAfter(0, 0)
		if err != nil {
			return err
		}
		if got := string(events[0].Details); got != `{"detail":"original"}` {
			t.Fatalf("stored details after returned-copy mutation = %s, want original", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() second read error = %v", err)
	}
}

// TestMemoryStoreEventResume verifies global gap-free ordering, exclusive resume
// semantics, bounded pages, and the explicit zero-limit all-results behavior.
func TestMemoryStoreEventResume(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-events", "sandbox-events")
	putTestOperation(t, store, op)

	err := store.Update(context.Background(), func(tx Tx) error {
		for i := 0; i < 5; i++ {
			event := testEvent(op)
			event.Details = []byte(`{"sample":true}`)
			if _, err := tx.AppendEvent(event); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update() events error = %v", err)
	}

	err = store.View(context.Background(), func(reader Reader) error {
		page, err := reader.EventsAfter(2, 2)
		if err != nil {
			return err
		}
		if len(page) != 2 || page[0].Sequence != 3 || page[1].Sequence != 4 {
			t.Fatalf("EventsAfter(2, 2) = %#v, want sequences 3 and 4", page)
		}
		rest, err := reader.EventsAfter(4, 0)
		if err != nil {
			return err
		}
		if len(rest) != 1 || rest[0].Sequence != 5 {
			t.Fatalf("EventsAfter(4, 0) = %#v, want sequence 5", rest)
		}
		if _, err := reader.EventsAfter(0, -1); !errors.Is(err, ErrInvalidEventLimit) {
			t.Fatalf("EventsAfter() negative limit error = %v, want ErrInvalidEventLimit", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() event resume error = %v", err)
	}
}

// TestMemoryStoreEventAssociation verifies callers cannot choose a sequence or
// attach an event to a different operation binding.
func TestMemoryStoreEventAssociation(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-event-binding", "sandbox-event-binding")
	putTestOperation(t, store, op)

	presequenced := testEvent(op)
	presequenced.Sequence = 9
	err := store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.AppendEvent(presequenced)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("AppendEvent() caller sequence error = %v, want ErrInvalidRecord", err)
	}

	mismatch := testEvent(op)
	mismatch.Target.ID = "different-target"
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.AppendEvent(mismatch)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("AppendEvent() binding mismatch error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStoreActiveOperation verifies target-level exclusivity and confirms
// a terminal operation releases the target for a new durable intent.
func TestMemoryStoreActiveOperation(t *testing.T) {
	store := NewMemoryStore()
	first := testOperation("op-active-1", "sandbox-active")
	firstRecord := putTestOperation(t, store, first)
	second := testOperation("op-active-2", "sandbox-active")

	err := store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(NewOperationRecord(second), 0)
		return err
	})
	if !errors.Is(err, ErrActiveOperation) {
		t.Fatalf("PutOperation() competing active error = %v, want ErrActiveOperation", err)
	}
	if !errors.Is(err, operation.ErrActiveConflict) {
		t.Fatalf("PutOperation() competing active error = %v, want operation.ErrActiveConflict", err)
	}

	terminal := firstRecord.Clone()
	terminal.Operation.State = operation.StateSucceeded
	terminal.Operation.Stage = operation.StageComplete
	terminal.Operation.Result = operation.ResultSucceeded
	terminal.Operation.Response = []byte(`{"released":true}`)
	err = store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutOperation(terminal, firstRecord.Revision); err != nil {
			return err
		}
		_, err := tx.PutOperation(NewOperationRecord(second), 0)
		return err
	})
	if err != nil {
		t.Fatalf("Update() release and replace active operation error = %v", err)
	}

	err = store.View(context.Background(), func(reader Reader) error {
		active, err := reader.ActiveOperation(second.Target)
		if err != nil {
			return err
		}
		if active.Operation.ID != second.ID {
			t.Fatalf("ActiveOperation() ID = %q, want %q", active.Operation.ID, second.ID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() active operation error = %v", err)
	}
}

// TestMemoryStoreRetentionPreservesReplayAndReportsResumeGap verifies count-based
// compaction keeps the newest exact response, tombstones older IDs, and exposes
// a typed gap only for non-empty cursors older than the retained event suffix.
func TestMemoryStoreRetentionPreservesReplayAndReportsResumeGap(t *testing.T) {
	policy := RetentionPolicy{TerminalOperationLimit: 1, OperationIdentityLimit: 5, EventLimit: 2}
	store, err := NewMemoryStoreWithRetention(policy)
	if err != nil {
		t.Fatalf("NewMemoryStoreWithRetention() error = %v", err)
	}
	for _, id := range []string{"op-retain-1", "op-retain-2", "op-retain-3", "op-retain-4"} {
		putTerminalOperationWithEvent(t, store, id, "sandbox-"+id)
	}

	err = store.View(context.Background(), func(reader Reader) error {
		if _, getErr := reader.GetOperation("op-retain-1"); !errors.Is(getErr, ErrOperationExpired) {
			t.Fatalf("GetOperation(retired) error = %v, want ErrOperationExpired", getErr)
		}
		latest, getErr := reader.GetOperation("op-retain-4")
		if getErr != nil {
			return getErr
		}
		if string(latest.Operation.Response) != `{"retained":true}` {
			t.Fatalf("latest replay response = %s, want persisted terminal response", latest.Operation.Response)
		}
		records, listErr := reader.ListOperations()
		if listErr != nil {
			return listErr
		}
		if len(records) != 1 || records[0].Operation.ID != "op-retain-4" {
			t.Fatalf("ListOperations() IDs = %v, want newest full replay only", operationRecordIDs(records))
		}
		fresh, eventsErr := reader.EventsAfter(0, 0)
		if eventsErr != nil {
			return eventsErr
		}
		if got := eventSequences(fresh); len(got) != 2 || got[0] != 3 || got[1] != 4 {
			t.Fatalf("EventsAfter(0) sequences = %v, want [3 4]", got)
		}
		if _, eventsErr = reader.EventsAfter(1, 0); !errors.Is(eventsErr, ErrEventResumeGap) {
			t.Fatalf("EventsAfter(stale) error = %v, want ErrEventResumeGap", eventsErr)
		}
		var gap *EventResumeGapError
		if !errors.As(eventsErr, &gap) || gap.FirstAvailable != 3 {
			t.Fatalf("EventsAfter(stale) gap = %#v, want first available 3", gap)
		}
		resumed, eventsErr := reader.EventsAfter(2, 0)
		if eventsErr != nil {
			return eventsErr
		}
		if got := eventSequences(resumed); len(got) != 2 || got[0] != 3 || got[1] != 4 {
			t.Fatalf("EventsAfter(boundary) sequences = %v, want [3 4]", got)
		}
		if _, eventsErr = reader.EventsAfter(5, 0); !errors.Is(eventsErr, ErrEventResumeGap) {
			t.Fatalf("EventsAfter(future) error = %v, want ErrEventResumeGap", eventsErr)
		}
		gap = nil
		if !errors.As(eventsErr, &gap) || gap.LastAvailable != 4 {
			t.Fatalf("EventsAfter(future) gap = %#v, want last available 4", gap)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View(retained state) error = %v", err)
	}

	reused := testOperation("op-retain-1", "sandbox-different-binding")
	err = store.Update(context.Background(), func(tx Tx) error {
		_, putErr := tx.PutOperation(NewOperationRecord(reused), 0)
		return putErr
	})
	if !errors.Is(err, ErrOperationExpired) {
		t.Fatalf("PutOperation(retired ID with different binding) error = %v, want ErrOperationExpired", err)
	}
}

// TestMemoryStoreRetentionKeepsActiveIntentAndFailsClosedAtCapacity proves an
// active operation is never compacted and a full identity budget rejects new
// intent before it can enter the transactional candidate.
func TestMemoryStoreRetentionKeepsActiveIntentAndFailsClosedAtCapacity(t *testing.T) {
	policy := RetentionPolicy{TerminalOperationLimit: 1, OperationIdentityLimit: 2, EventLimit: 2}
	store, err := NewMemoryStoreWithRetention(policy)
	if err != nil {
		t.Fatalf("NewMemoryStoreWithRetention() error = %v", err)
	}
	active := testOperation("op-capacity-active", "sandbox-capacity-active")
	putTestOperation(t, store, active)
	putTerminalOperationWithEvent(t, store, "op-capacity-terminal", "sandbox-capacity-terminal")

	third := testOperation("op-capacity-rejected", "sandbox-capacity-rejected")
	err = store.Update(context.Background(), func(tx Tx) error {
		_, putErr := tx.PutOperation(NewOperationRecord(third), 0)
		return putErr
	})
	if !errors.Is(err, ErrRetentionCapacity) {
		t.Fatalf("PutOperation(over capacity) error = %v, want ErrRetentionCapacity", err)
	}
	err = store.View(context.Background(), func(reader Reader) error {
		stored, getErr := reader.GetOperation(active.ID)
		if getErr != nil {
			return getErr
		}
		if !stored.Operation.State.Active() {
			t.Fatalf("active operation state = %q, want active", stored.Operation.State)
		}
		if _, getErr = reader.GetOperation(third.ID); !errors.Is(getErr, ErrNotFound) {
			t.Fatalf("rejected operation lookup error = %v, want ErrNotFound", getErr)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View(capacity state) error = %v", err)
	}
}

// TestCompactedObservationRejectsMismatchedOperationTarget verifies both full
// records and retired tombstones cannot validate a projection on another resource.
func TestCompactedObservationRejectsMismatchedOperationTarget(t *testing.T) {
	op := testOperation("op-observation-target", "sandbox-wrong-target")
	observation := domain.LifecycleObservation{
		OperationID:   string(op.ID),
		EventSequence: 1,
		Reason:        string(op.Reason),
	}
	required := operation.Target{Kind: operation.TargetSandbox, ID: "sandbox-current-target"}
	tests := []struct {
		name  string
		setup func(*memoryData)
	}{
		{
			name: "full operation",
			setup: func(data *memoryData) {
				data.operations[op.ID] = NewOperationRecord(op)
			},
		},
		{
			name: "retired tombstone",
			setup: func(data *memoryData) {
				digest := operationIDDigest(op.ID)
				data.retiredOperations[digest] = retiredOperation{
					OperationIDSHA256: digest,
					Type:              op.Type,
					Target:            op.Target,
					Fingerprint:       op.Fingerprint,
					Reason:            op.Reason,
					TerminalSequence:  1,
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := newMemoryData()
			data.firstEventSequence = 2
			data.lastEventSequence = 1
			test.setup(&data)
			err := data.validateObservationReference(observation, map[uint64]operation.Event{}, required)
			if !errors.Is(err, ErrInvariantViolation) {
				t.Fatalf("validateObservationReference() error = %v, want ErrInvariantViolation", err)
			}
		})
	}
}

// TestMemoryStoreConcurrentCAS verifies top-level Store updates are race-safe
// and exactly one writer can advance a shared stale revision.
func TestMemoryStoreConcurrentCAS(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-concurrent", "sandbox-concurrent")
	created := putTestOperation(t, store, op)
	candidate := created.Clone()
	candidate.Operation.State = operation.StateRunning
	candidate.Operation.Stage = operation.StageTransition

	const writers = 32
	var successes atomic.Int64
	var conflicts atomic.Int64
	var unexpectedMu sync.Mutex
	var unexpected error
	var wait sync.WaitGroup
	wait.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer wait.Done()
			err := store.Update(context.Background(), func(tx Tx) error {
				_, err := tx.PutOperation(candidate, created.Revision)
				return err
			})
			switch {
			case err == nil:
				successes.Add(1)
			case errors.Is(err, ErrRevisionConflict):
				conflicts.Add(1)
			default:
				unexpectedMu.Lock()
				if unexpected == nil {
					unexpected = err
				}
				unexpectedMu.Unlock()
			}
		}()
	}
	wait.Wait()
	if unexpected != nil {
		t.Fatalf("concurrent Update() unexpected error = %v", unexpected)
	}
	if got := successes.Load(); got != 1 {
		t.Fatalf("successful writers = %d, want 1", got)
	}
	if got := conflicts.Load(); got != writers-1 {
		t.Fatalf("conflicting writers = %d, want %d", got, writers-1)
	}
}

// TestMemoryStoreRejectsEscapedReader verifies callback-scoped access cannot be
// retained and used after the store has released its consistency lock.
func TestMemoryStoreRejectsEscapedReader(t *testing.T) {
	store := NewMemoryStore()
	var escaped Reader
	err := store.View(context.Background(), func(reader Reader) error {
		escaped = reader
		return nil
	})
	if err != nil {
		t.Fatalf("View() error = %v", err)
	}
	if _, err := escaped.ListSandboxes(); !errors.Is(err, ErrTransactionClosed) {
		t.Fatalf("escaped ListSandboxes() error = %v, want ErrTransactionClosed", err)
	}
}

// putTestOperation commits one valid pending operation and returns its first
// store record so individual tests can focus on the contract under examination.
func putTestOperation(t *testing.T, store Store, op operation.Operation) OperationRecord {
	t.Helper()
	var stored OperationRecord
	err := store.Update(context.Background(), func(tx Tx) error {
		var err error
		stored, err = tx.PutOperation(NewOperationRecord(op), 0)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation() setup error = %v", err)
	}
	return stored
}

// putTerminalOperationWithEvent commits one complete replay response and its
// matching global event so retention tests exercise only public Store methods.
func putTerminalOperationWithEvent(t *testing.T, store Store, id, targetID string) OperationRecord {
	t.Helper()
	op := testOperation(id, targetID)
	op.State = operation.StateSucceeded
	op.Stage = operation.StageComplete
	op.Result = operation.ResultSucceeded
	op.Response = []byte(`{"retained":true}`)
	event := testEvent(op)
	event.Stage = op.Stage
	event.Result = op.Result
	event.Reason = op.Reason
	var stored OperationRecord
	err := store.Update(context.Background(), func(tx Tx) error {
		var putErr error
		stored, putErr = tx.PutOperation(NewOperationRecord(op), 0)
		if putErr != nil {
			return putErr
		}
		_, appendErr := tx.AppendEvent(event)
		return appendErr
	})
	if err != nil {
		t.Fatalf("Update(terminal operation %q) error = %v", id, err)
	}
	return stored
}

// testOperation builds a valid pending operation with a stable fingerprint for
// state tests that do not exercise canonical request encoding itself.
func testOperation(id, targetID string) operation.Operation {
	return operation.Operation{
		SchemaVersion: operation.SchemaVersion,
		ID:            operation.OperationID(id),
		Type:          operation.TypeCreate,
		Target: operation.Target{
			Kind: operation.TargetSandbox,
			ID:   targetID,
		},
		Fingerprint: operation.RequestFingerprint{
			Version: operation.CurrentFingerprintVersion,
			SHA256:  strings.Repeat("a", 64),
		},
		State:  operation.StatePending,
		Stage:  operation.StageValidate,
		Result: operation.ResultPending,
		Reason: operation.ReasonNone,
	}
}

// testEvent builds an unsequenced valid event payload; AppendEvent is expected
// to assign its authoritative global sequence before model validation.
func testEvent(op operation.Operation) operation.Event {
	duration := operation.Duration(time.Millisecond)
	return operation.Event{
		SchemaVersion: operation.EventSchemaVersion,
		OperationID:   op.ID,
		Type:          op.Type,
		Target:        op.Target,
		Resources:     []operation.Target{op.Target},
		Stage:         operation.StageValidate,
		Result:        operation.ResultPending,
		Reason:        operation.ReasonNone,
		OccurredAt:    time.Date(2026, time.August, 20, 0, 0, 0, 0, time.UTC),
		Duration:      &duration,
	}
}

// testReadySandbox builds a valid generation-one Ready Sandbox with nested
// mutable values so state tests can detect shallow-copy regressions.
func testReadySandbox(t *testing.T, id string) domain.Sandbox {
	t.Helper()
	cpuLimit := int64(500)
	sandbox, err := domain.NewSandbox(domain.SandboxID(id), domain.SandboxSpec{
		DNS:    []string{"1.1.1.1"},
		Labels: map[string]string{"role": "worker"},
		Resources: domain.Resources{
			Limits: domain.ResourceLimits{CPULimitMilli: &cpuLimit},
		},
	})
	if err != nil {
		t.Fatalf("NewSandbox() setup error = %v", err)
	}
	sandbox.Status.Phase = domain.SandboxReady
	sandbox.Status.ObservedGeneration = domain.InitialGeneration
	if err := sandbox.Validate(); err != nil {
		t.Fatalf("ready Sandbox setup validation error = %v", err)
	}
	return sandbox
}

// testContainerAttempt builds a valid creating pair for persistence, cloning,
// list, and deletion tests without introducing any host-side resources.
func testContainerAttempt(t *testing.T, sandbox domain.Sandbox, containerID, attemptID string) domain.ContainerAttempt {
	t.Helper()
	pair, err := domain.NewContainerAttempt(
		sandbox,
		domain.ContainerID(containerID),
		domain.AttemptID(attemptID),
		domain.ProcessSpec{Argv: []string{"/bin/work"}},
		"sha256:test",
		"/prepared/rootfs",
	)
	if err != nil {
		t.Fatalf("NewContainerAttempt() setup error = %v", err)
	}
	return pair
}

// setPairPhase advances both sides of the atomic status projection together for
// tests that need retained terminal history rather than an active execution.
func setPairPhase(pair *domain.ContainerAttempt, phase domain.AttemptPhase) {
	pair.Container.Status.Phase = phase
	pair.Attempt.Phase = phase
	if phase == domain.AttemptCreated || phase == domain.AttemptRunning || phase == domain.AttemptStopped {
		pair.Container.Status.ObservedGeneration = domain.InitialGeneration
	}
	if phase == domain.AttemptStopped {
		pair.Attempt.Outcome = domain.NotApplicableOutcome()
		pair.Container.Status.Outcome = domain.NotApplicableOutcome()
	}
}
