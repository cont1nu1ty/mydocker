package state

import (
	"context"
	"errors"
	"testing"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/rollback"
)

// TestMemoryStoreRejectsImmutableSandboxRewrite verifies CAS cannot mutate create spec or move phase backward.
func TestMemoryStoreRejectsImmutableSandboxRewrite(t *testing.T) {
	store := NewMemoryStore()
	created := putTestSandbox(t, store, testReadySandbox(t, "sandbox-immutable"))

	changed := created.Clone()
	changed.Sandbox.Spec.Hostname = "rewritten"
	err := store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutSandbox(changed, created.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutSandbox(spec rewrite) error = %v, want ErrInvalidRecord", err)
	}

	regressed := created.Clone()
	regressed.Sandbox.Status.Phase = domain.SandboxCreating
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutSandbox(regressed, created.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutSandbox(phase regression) error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStoreRejectsImmutablePairRewrite verifies execution identity, spec, and phase history cannot be rewritten.
func TestMemoryStoreRejectsImmutablePairRewrite(t *testing.T) {
	store := NewMemoryStore()
	sandbox := testReadySandbox(t, "sandbox-pair-immutable")
	pair := testContainerAttempt(t, sandbox, "container-immutable", "attempt-immutable")
	if err := sandbox.SetCurrentPair(pair.Container.ID, pair.Attempt.ID); err != nil {
		t.Fatalf("SetCurrentPair() error = %v", err)
	}
	var pairRecord ContainerAttemptRecord
	err := store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutSandbox(NewSandboxRecord(sandbox), 0); err != nil {
			return err
		}
		var err error
		pairRecord, err = tx.PutContainerAttempt(NewContainerAttemptRecord(pair), 0)
		return err
	})
	if err != nil {
		t.Fatalf("setup Update() error = %v", err)
	}

	rewritten := pairRecord.Clone()
	rewritten.ContainerAttempt.Container.Spec.Process.Argv[0] = "/rewritten"
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutContainerAttempt(rewritten, pairRecord.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutContainerAttempt(spec rewrite) error = %v, want ErrInvalidRecord", err)
	}

	created := pairRecord.Clone()
	if err := created.ContainerAttempt.Transition(domain.AttemptCreated, domain.PendingOutcome()); err != nil {
		t.Fatalf("Transition(Created) error = %v", err)
	}
	err = store.Update(context.Background(), func(tx Tx) error {
		var err error
		created, err = tx.PutContainerAttempt(created, pairRecord.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("PutContainerAttempt(Created) error = %v", err)
	}
	regressed := created.Clone()
	regressed.ContainerAttempt.Attempt.Phase = domain.AttemptCreating
	regressed.ContainerAttempt.Container.Status.Phase = domain.AttemptCreating
	regressed.ContainerAttempt.Container.Status.ObservedGeneration = 0
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutContainerAttempt(regressed, created.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutContainerAttempt(phase regression) error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStoreCommitReferentialIntegrity verifies orphan, dangling-current, and persisted-absent states never commit.
func TestMemoryStoreCommitReferentialIntegrity(t *testing.T) {
	store := NewMemoryStore()
	sandbox := testReadySandbox(t, "sandbox-reference")
	pair := testContainerAttempt(t, sandbox, "container-reference", "attempt-reference")
	err := store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutContainerAttempt(NewContainerAttemptRecord(pair), 0)
		return err
	})
	if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("orphan pair Update() error = %v, want ErrInvariantViolation", err)
	}

	if err := sandbox.SetCurrentPair(pair.Container.ID, pair.Attempt.ID); err != nil {
		t.Fatalf("SetCurrentPair() error = %v", err)
	}
	if err := pair.Transition(domain.AttemptStopped, domain.NotApplicableOutcome()); err != nil {
		t.Fatalf("Transition(Stopped) error = %v", err)
	}
	var pairRecord ContainerAttemptRecord
	err = store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutSandbox(NewSandboxRecord(sandbox), 0); err != nil {
			return err
		}
		var err error
		pairRecord, err = tx.PutContainerAttempt(NewContainerAttemptRecord(pair), 0)
		return err
	})
	if err != nil {
		t.Fatalf("reference setup Update() error = %v", err)
	}
	err = store.Update(context.Background(), func(tx Tx) error {
		return tx.DeleteContainerAttempt(pair.Container.ID, pairRecord.Revision)
	})
	if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("dangling current delete error = %v, want ErrInvariantViolation", err)
	}

	absent := testReadySandbox(t, "sandbox-absent-record")
	absent.Status.Phase = domain.SandboxAbsent
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutSandbox(NewSandboxRecord(absent), 0)
		return err
	})
	if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("persisted absent Sandbox error = %v, want ErrInvariantViolation", err)
	}
}

// TestMemoryStoreDeletePreconditions verifies raw persistence calls cannot bypass lifecycle stop and zero-child guards.
func TestMemoryStoreDeletePreconditions(t *testing.T) {
	store := NewMemoryStore()
	sandbox := testReadySandbox(t, "sandbox-delete-guard")
	sandboxRecord := putTestSandbox(t, store, sandbox)
	err := store.Update(context.Background(), func(tx Tx) error {
		return tx.DeleteSandbox(sandbox.ID, sandboxRecord.Revision)
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("DeleteSandbox(Ready) error = %v, want ErrInvalidRecord", err)
	}

	pair := testContainerAttempt(t, sandbox, "container-delete-guard", "attempt-delete-guard")
	if err := sandbox.SetCurrentPair(pair.Container.ID, pair.Attempt.ID); err != nil {
		t.Fatalf("SetCurrentPair() error = %v", err)
	}
	var currentSandbox SandboxRecord
	var pairRecord ContainerAttemptRecord
	err = store.Update(context.Background(), func(tx Tx) error {
		var err error
		currentSandbox, err = tx.GetSandbox(sandbox.ID)
		if err != nil {
			return err
		}
		currentSandbox.Sandbox = sandbox
		currentSandbox, err = tx.PutSandbox(currentSandbox, currentSandbox.Revision)
		if err != nil {
			return err
		}
		pairRecord, err = tx.PutContainerAttempt(NewContainerAttemptRecord(pair), 0)
		return err
	})
	if err != nil {
		t.Fatalf("delete guard setup Update() error = %v", err)
	}
	err = store.Update(context.Background(), func(tx Tx) error {
		return tx.DeleteContainerAttempt(pair.Container.ID, pairRecord.Revision)
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("DeleteContainerAttempt(Creating) error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStorePersistsRollbackProgress verifies descriptors are deep-copied, append-only, and success-monotonic.
func TestMemoryStorePersistsRollbackProgress(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-rollback-state", "sandbox-rollback-state")
	descriptor := rollback.Descriptor{SchemaVersion: rollback.SchemaVersion, Name: "release-handle", Action: "release", Target: "handle-1", Metadata: []byte(`{"owner":"sandbox"}`)}
	record := NewOperationRecord(op)
	record.Rollback = []rollback.Record{{Descriptor: descriptor}}
	var stored OperationRecord
	err := store.Update(context.Background(), func(tx Tx) error {
		var err error
		stored, err = tx.PutOperation(record, 0)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation(rollback) error = %v", err)
	}
	record.Rollback[0].Descriptor.Metadata[10] = 'X'
	if string(stored.Rollback[0].Descriptor.Metadata) != `{"owner":"sandbox"}` {
		t.Fatal("OperationRecord.Clone() retained rollback metadata alias")
	}

	completed := stored.Clone()
	completed.Rollback[0].Succeeded = true
	completed.Rollback[0].Started = true
	err = store.Update(context.Background(), func(tx Tx) error {
		var err error
		completed, err = tx.PutOperation(completed, stored.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation(rollback success) error = %v", err)
	}
	regressed := completed.Clone()
	regressed.Rollback[0].Succeeded = false
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(regressed, completed.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(rollback regression) error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStoreRequiresRollbackCompletionBeforeFailure verifies cleanup retry stays active until every started inverse succeeds.
func TestMemoryStoreRequiresRollbackCompletionBeforeFailure(t *testing.T) {
	store := NewMemoryStore()
	record := NewOperationRecord(testOperation("op-rollback-terminal", "sandbox-rollback-terminal"))
	record.Operation.State = operation.StateRunning
	record.Operation.Stage = operation.StageRollback
	cause, err := rollback.NewCause(operation.ReasonCleanup, "rootfs cleanup pending")
	if err != nil {
		t.Fatalf("NewCause() error = %v", err)
	}
	record.RollbackCause = &cause
	record.Rollback = []rollback.Record{{Descriptor: rollback.Descriptor{
		SchemaVersion: rollback.SchemaVersion, Name: "rootfs", Action: "unmount", Target: "snapshot-rollback",
	}, Started: true}}
	var stored OperationRecord
	err = store.Update(context.Background(), func(tx Tx) error {
		var err error
		stored, err = tx.PutOperation(record, 0)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation(active rollback) error = %v", err)
	}
	failed := stored.Clone()
	failed.Operation.State = operation.StateFailed
	failed.Operation.Stage = operation.StageComplete
	failed.Operation.Result = operation.ResultFailed
	failed.Operation.Reason = operation.ReasonCleanup
	failed.Operation.Response = []byte(`{"cleanup":"pending"}`)
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(failed, stored.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(terminal pending rollback) error = %v, want ErrInvalidRecord", err)
	}
	completed := stored.Clone()
	completed.Rollback[0].Succeeded = true
	err = store.Update(context.Background(), func(tx Tx) error {
		var err error
		completed, err = tx.PutOperation(completed, stored.Revision)
		if err != nil {
			return err
		}
		failed = completed.Clone()
		failed.Operation.State = operation.StateFailed
		failed.Operation.Stage = operation.StageComplete
		failed.Operation.Result = operation.ResultFailed
		failed.Operation.Reason = operation.ReasonCleanup
		failed.Operation.Response = []byte(`{"cleanup":"complete"}`)
		_, err = tx.PutOperation(failed, completed.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation(terminal after rollback completion) error = %v", err)
	}
}

// TestMemoryStoreRejectsEventProgressMismatch verifies events cannot claim a stage/result absent from durable operation state.
func TestMemoryStoreRejectsEventProgressMismatch(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-event-progress", "sandbox-event-progress")
	putTestOperation(t, store, op)
	event := testEvent(op)
	event.Stage = operation.StageComplete
	event.Result = operation.ResultSucceeded
	err := store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.AppendEvent(event)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("AppendEvent(progress mismatch) error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStoreRejectsEventGenerationMismatch verifies events cannot claim resource generations absent from state.
func TestMemoryStoreRejectsEventGenerationMismatch(t *testing.T) {
	store := NewMemoryStore()
	sandbox := testReadySandbox(t, "sandbox-event-generation")
	putTestSandbox(t, store, sandbox)
	op := testOperation("op-event-generation", string(sandbox.ID))
	putTestOperation(t, store, op)
	event := testEvent(op)
	err := store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.AppendEvent(event)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("AppendEvent(generation mismatch) error = %v, want ErrInvalidRecord", err)
	}
	event.Generation = uint64(sandbox.Status.Generation)
	event.ObservedGeneration = uint64(sandbox.Status.ObservedGeneration)
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.AppendEvent(event)
		return err
	})
	if err != nil {
		t.Fatalf("AppendEvent(matching generation) error = %v", err)
	}
}

// TestMemoryStorePreservesTypedOperationErrors verifies persistence adds context without erasing idempotency categories.
func TestMemoryStorePreservesTypedOperationErrors(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-binding-state", "sandbox-binding-state")
	stored := putTestOperation(t, store, op)
	mismatch := stored.Clone()
	mismatch.Operation.Fingerprint.SHA256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	err := store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(mismatch, stored.Revision)
		return err
	})
	if !errors.Is(err, operation.ErrBindingMismatch) || !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(binding mismatch) error = %v, want typed operation and state errors", err)
	}
}

// TestMemoryStoreRejectsOperationProgressRegression verifies an active operation cannot move backward after recovery.
func TestMemoryStoreRejectsOperationProgressRegression(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-progress-state", "sandbox-progress-state")
	stored := putTestOperation(t, store, op)
	running := stored.Clone()
	running.Operation.State = operation.StateRunning
	running.Operation.Stage = operation.StagePersistState
	err := store.Update(context.Background(), func(tx Tx) error {
		var err error
		running, err = tx.PutOperation(running, stored.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation(forward progress) error = %v", err)
	}
	regressed := running.Clone()
	regressed.Operation.Stage = operation.StageTransition
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(regressed, running.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(stage regression) error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStoreAcceptsOrderedHostCheckpoints verifies M2/M3 acquisition stages advance monotonically before transition.
func TestMemoryStoreAcceptsOrderedHostCheckpoints(t *testing.T) {
	store := NewMemoryStore()
	stored := putTestOperation(t, store, testOperation("op-host-progress", "sandbox-host-progress"))
	stages := []operation.Stage{
		operation.StageHostPreflight,
		operation.StagePrepareCgroup,
		operation.StagePrepareStartGate,
		operation.StagePrepareStreams,
		operation.StageCreateProcess,
		operation.StagePrepareNamespaces,
		operation.StagePersistState,
	}
	for _, stage := range stages {
		next := stored.Clone()
		next.Operation.State = operation.StateRunning
		next.Operation.Stage = stage
		err := store.Update(context.Background(), func(tx Tx) error {
			var err error
			stored, err = tx.PutOperation(next, stored.Revision)
			return err
		})
		if err != nil {
			t.Fatalf("PutOperation(stage %q) error = %v", stage, err)
		}
	}
}

// TestMemoryStoreFinalizesTerminalResponseOnce verifies event-aware response finalization is atomic and immutable afterward.
func TestMemoryStoreFinalizesTerminalResponseOnce(t *testing.T) {
	store := NewMemoryStore()
	stored := putTestOperation(t, store, testOperation("op-terminal-response", "sandbox-terminal-response"))
	terminal := stored.Clone()
	terminal.Operation.State = operation.StateSucceeded
	terminal.Operation.Stage = operation.StageComplete
	terminal.Operation.Result = operation.ResultSucceeded
	err := store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(terminal, stored.Revision)
		return err
	})
	if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("PutOperation(terminal without response) error = %v, want ErrInvariantViolation", err)
	}
	var finalized OperationRecord
	err = store.Update(context.Background(), func(tx Tx) error {
		checkpoint, err := tx.PutOperation(terminal, stored.Revision)
		if err != nil {
			return err
		}
		checkpoint.Operation.Response = []byte(`{"phase":"ready"}`)
		finalized, err = tx.PutOperation(checkpoint, checkpoint.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation(atomic terminal response) error = %v", err)
	}
	rewritten := finalized.Clone()
	rewritten.Operation.Response = []byte(`{"phase":"stopped"}`)
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(rewritten, finalized.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(rewrite terminal response) error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStoreRejectsActiveResponseDrift verifies a persisted side-effect
// plan cannot be rewritten while retaining the same operation fingerprint.
func TestMemoryStoreRejectsActiveResponseDrift(t *testing.T) {
	store := NewMemoryStore()
	active := testOperation("op-active-response", "sandbox-active-response")
	active.State = operation.StateRunning
	active.Stage = operation.StagePersistIntent
	active.Response = []byte(`{"plan":{"Signal":"SIGTERM"}}`)
	stored := putTestOperation(t, store, active)

	rewritten := stored.Clone()
	rewritten.Operation.Response = []byte(`{"plan":{"Signal":"SIGKILL"}}`)
	err := store.Update(context.Background(), func(tx Tx) error {
		_, putErr := tx.PutOperation(rewritten, stored.Revision)
		return putErr
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(active response rewrite) error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStoreAllowsInitialIntentResponseThenFreezesIt verifies the only
// active response mutation is the first validate-to-persist-intent commit;
// every later active checkpoint must retain those exact durable plan bytes.
func TestMemoryStoreAllowsInitialIntentResponseThenFreezesIt(t *testing.T) {
	store := NewMemoryStore()
	pending := testOperation("op-initialize-response", "sandbox-initialize-response")
	stored := putTestOperation(t, store, pending)

	running := stored.Clone()
	running.Operation.State = operation.StateRunning
	running.Operation.Stage = operation.StagePersistIntent
	running.Operation.Response = []byte(`{"plan":{"signal":"SIGTERM"}}`)
	err := store.Update(context.Background(), func(tx Tx) error {
		var putErr error
		running, putErr = tx.PutOperation(running, stored.Revision)
		return putErr
	})
	if err != nil {
		t.Fatalf("PutOperation(initial intent response) error = %v", err)
	}

	rewritten := running.Clone()
	rewritten.Operation.Response = []byte(`{"plan":{"signal":"SIGKILL"}}`)
	err = store.Update(context.Background(), func(tx Tx) error {
		_, putErr := tx.PutOperation(rewritten, running.Revision)
		return putErr
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(rewrite initialized response) error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStoreSealsRollbackDescriptors verifies recovery cannot append acquisitions after any cleanup step started.
func TestMemoryStoreSealsRollbackDescriptors(t *testing.T) {
	store := NewMemoryStore()
	stored := putTestOperation(t, store, testOperation("op-rollback-sealed", "sandbox-rollback-sealed"))
	started := stored.Clone()
	started.Rollback = []rollback.Record{{Descriptor: rollback.Descriptor{
		SchemaVersion: rollback.SchemaVersion, Name: "rootfs", Action: "unmount", Target: "snapshot-1",
	}, Started: true}}
	err := store.Update(context.Background(), func(tx Tx) error {
		var err error
		started, err = tx.PutOperation(started, stored.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation(started rollback) error = %v", err)
	}
	appended := started.Clone()
	appended.Rollback = append(appended.Rollback, rollback.Record{Descriptor: rollback.Descriptor{
		SchemaVersion: rollback.SchemaVersion, Name: "network", Action: "release", Target: "sandbox-1",
	}})
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(appended, started.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(append after rollback started) error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStoreValidatesLastObservationReferences verifies projections cannot be fabricated or moved backward.
func TestMemoryStoreValidatesLastObservationReferences(t *testing.T) {
	store := NewMemoryStore()
	sandbox := testReadySandbox(t, "sandbox-observation")
	fabricated := sandbox.Clone()
	fabricated.Status.LastObservation = domain.LifecycleObservation{OperationID: "missing-op", EventSequence: 99, Reason: string(operation.ReasonNone)}
	err := store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutSandbox(NewSandboxRecord(fabricated), 0)
		return err
	})
	if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("PutSandbox(fabricated observation) error = %v, want ErrInvariantViolation", err)
	}

	op := testOperation("op-observation", string(sandbox.ID))
	event := testEvent(op)
	event.Generation = uint64(sandbox.Status.Generation)
	event.ObservedGeneration = uint64(sandbox.Status.ObservedGeneration)
	var storedSandbox SandboxRecord
	err = store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutOperation(NewOperationRecord(op), 0); err != nil {
			return err
		}
		storedEvent, err := tx.AppendEvent(event)
		if err != nil {
			return err
		}
		if err := sandbox.SetLastObservation(domain.LifecycleObservation{OperationID: string(op.ID), EventSequence: uint64(storedEvent.Sequence), Reason: string(storedEvent.Reason)}); err != nil {
			return err
		}
		storedSandbox, err = tx.PutSandbox(NewSandboxRecord(sandbox), 0)
		return err
	})
	if err != nil {
		t.Fatalf("observation setup Update() error = %v", err)
	}
	regressed := storedSandbox.Clone()
	regressed.Sandbox.Status.LastObservation = domain.LifecycleObservation{}
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutSandbox(regressed, storedSandbox.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutSandbox(regressed observation) error = %v, want ErrInvalidRecord", err)
	}
}

// putTestSandbox commits one valid Sandbox and returns its first CAS envelope for invariant tests.
func putTestSandbox(t *testing.T, store Store, sandbox domain.Sandbox) SandboxRecord {
	t.Helper()
	var stored SandboxRecord
	err := store.Update(context.Background(), func(tx Tx) error {
		var err error
		stored, err = tx.PutSandbox(NewSandboxRecord(sandbox), 0)
		return err
	})
	if err != nil {
		t.Fatalf("PutSandbox() setup error = %v", err)
	}
	return stored
}
