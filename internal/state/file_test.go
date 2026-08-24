package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
)

// TestFileStorePersistsAndReopens verifies records, revisions, event order,
// deterministic operation listing, permissions, and deep-copy boundaries survive restart.
func TestFileStorePersistsAndReopens(t *testing.T) {
	path := filepath.Join(secureStateTestDir(t), "state.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(state) error = %v", err)
	}
	if got := info.Mode().Perm(); got != filePermission {
		t.Fatalf("state mode = %#o, want %#o", got, filePermission)
	}

	second := testOperation("op-file-z", "sandbox-file-z")
	first := testOperation("op-file-a", "sandbox-file-a")
	firstEvent := testEvent(first)
	measuredZero := operation.Duration(0)
	firstEvent.Duration = &measuredZero
	firstEvent.Details = json.RawMessage("{\"source\":\"first\"}")
	secondEvent := testEvent(second)
	secondEvent.Duration = nil
	var firstRecord OperationRecord
	err = store.Update(context.Background(), func(tx Tx) error {
		var err error
		if _, err = tx.PutOperation(NewOperationRecord(second), 0); err != nil {
			return err
		}
		if firstRecord, err = tx.PutOperation(NewOperationRecord(first), 0); err != nil {
			return err
		}
		if _, err = tx.AppendEvent(firstEvent); err != nil {
			return err
		}
		_, err = tx.AppendEvent(secondEvent)
		return err
	})
	if err != nil {
		t.Fatalf("FileStore.Update(initial) error = %v", err)
	}
	closeFileStoreForTest(t, store)

	reopened, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(reopen) error = %v", err)
	}
	err = reopened.View(context.Background(), func(reader Reader) error {
		records, err := reader.ListOperations()
		if err != nil {
			return err
		}
		if len(records) != 2 || records[0].Operation.ID != first.ID || records[1].Operation.ID != second.ID {
			t.Fatalf("ListOperations() IDs = %v, want [%s %s]", operationRecordIDs(records), first.ID, second.ID)
		}
		records[0].Operation.Fingerprint.SHA256 = "mutated"
		fresh, err := reader.GetOperation(first.ID)
		if err != nil {
			return err
		}
		if fresh.Operation.Fingerprint.SHA256 == "mutated" {
			t.Fatal("ListOperations() exposed store memory")
		}
		events, err := reader.EventsAfter(0, 0)
		if err != nil {
			return err
		}
		if len(events) != 2 || events[0].Sequence != 1 || events[1].Sequence != 2 {
			t.Fatalf("EventsAfter() sequences = %v, want [1 2]", eventSequences(events))
		}
		if events[0].Duration == nil || events[0].Duration.Value() != 0 || events[1].Duration != nil {
			t.Fatalf("reopened event durations = %#v/%#v, want measured zero then unavailable", events[0].Duration, events[1].Duration)
		}
		originalDetails := append(json.RawMessage(nil), events[0].Details...)
		events[0].Details[0] = 'X'
		freshEvents, err := reader.EventsAfter(0, 1)
		if err != nil {
			return err
		}
		if !bytes.Equal(freshEvents[0].Details, originalDetails) {
			t.Fatal("EventsAfter() exposed durable event bytes")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("FileStore.View(reopened) error = %v", err)
	}

	advanced := firstRecord.Clone()
	advanced.Operation.State = operation.StateRunning
	advanced.Operation.Stage = operation.StageTransition
	advancedEvent := testEvent(advanced.Operation)
	advancedEvent.Stage = operation.StageTransition
	var appended operation.Event
	err = reopened.Update(context.Background(), func(tx Tx) error {
		var err error
		if advanced, err = tx.PutOperation(advanced, firstRecord.Revision); err != nil {
			return err
		}
		appended, err = tx.AppendEvent(advancedEvent)
		return err
	})
	if err != nil {
		t.Fatalf("FileStore.Update(after reopen) error = %v", err)
	}
	if advanced.Revision != 2 || appended.Sequence != 3 {
		t.Fatalf("post-restart revision/sequence = %d/%d, want 2/3", advanced.Revision, appended.Sequence)
	}
	closeFileStoreForTest(t, reopened)
	finalStore, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(final reopen) error = %v", err)
	}
	err = finalStore.View(context.Background(), func(reader Reader) error {
		record, err := reader.GetOperation(first.ID)
		if err != nil {
			return err
		}
		if record.Revision != 2 || record.Operation.Stage != operation.StageTransition {
			t.Fatalf("final operation revision/stage = %d/%s, want 2/%s", record.Revision, record.Operation.Stage, operation.StageTransition)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("FileStore.View(final) error = %v", err)
	}
	closeFileStoreForTest(t, finalStore)
}

// TestFileStoreReopensKillEscalationDeadline verifies daemon restart preserves the exact absolute grace boundary instead of reconstructing it from a new request time.
func TestFileStoreReopensKillEscalationDeadline(t *testing.T) {
	path := filepath.Join(secureStateTestDir(t), "kill-deadline.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	op := testOperation("op-file-kill-deadline", "container-file-kill-deadline")
	op.Type = operation.TypeKill
	op.Target.Kind = operation.TargetContainer
	op.State = operation.StateRunning
	op.Stage = operation.StagePersistIntent
	record, err := NewOperationRecordForProfile(op, HostProfileLinuxM2)
	if err != nil {
		t.Fatalf("NewOperationRecordForProfile() error = %v", err)
	}
	deadline := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	err = store.Update(context.Background(), func(tx Tx) error {
		stored, putErr := tx.PutOperation(record, 0)
		if putErr != nil {
			return putErr
		}
		stored.Operation.Stage = operation.StageSignalProcess
		stored.KillEscalationDeadline = &deadline
		_, putErr = tx.PutOperation(stored, stored.Revision)
		return putErr
	})
	if err != nil {
		t.Fatalf("FileStore.Update(Kill deadline) error = %v", err)
	}
	closeFileStoreForTest(t, store)

	reopened, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(reopen) error = %v", err)
	}
	defer closeFileStoreForTest(t, reopened)
	err = reopened.View(context.Background(), func(reader Reader) error {
		stored, getErr := reader.GetOperation(op.ID)
		if getErr != nil {
			return getErr
		}
		if stored.KillEscalationDeadline == nil || !stored.KillEscalationDeadline.Equal(deadline) {
			t.Fatalf("reopened deadline = %v, want %s", stored.KillEscalationDeadline, deadline)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("FileStore.View(reopened Kill deadline) error = %v", err)
	}
}

// TestFileStoreRejectsInvalidDiskEnvelope verifies recovery fails closed for
// corruption, future schemas, unknown fields, bad checksums, missing collections, and duplicates.
func TestFileStoreRejectsInvalidDiskEnvelope(t *testing.T) {
	valid, err := encodeFileData(newMemoryData())
	if err != nil {
		t.Fatalf("encodeFileData(empty) error = %v", err)
	}
	future := mutateFileEnvelope(t, valid, func(envelope *fileEnvelope) {
		envelope.SchemaVersion++
	})
	badChecksum := mutateFileEnvelope(t, valid, func(envelope *fileEnvelope) {
		envelope.PayloadSHA256 = "not-the-payload-digest"
	})
	missingCollection := mutateFileEnvelope(t, valid, func(envelope *fileEnvelope) {
		envelope.Payload.Operations = nil
		envelope.PayloadSHA256 = digestFilePayloadForTest(t, *envelope.Payload)
	})
	unknownField := appendUnknownEnvelopeField(t, valid)
	schemaField := []byte(fmt.Sprintf("\"schema_version\":%d,", currentFileSchemaVersion))
	duplicateSchemaField := []byte(fmt.Sprintf("\"schema_version\":%d,\"schema_version\":%d,", currentFileSchemaVersion, currentFileSchemaVersion))
	duplicateField := bytes.Replace(valid, schemaField, duplicateSchemaField, 1)
	duplicate := duplicateOperationEnvelope(t)

	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "malformed", data: []byte("{"), want: ErrInvalidRecord},
		{name: "trailing JSON", data: append(append([]byte(nil), valid...), []byte("{}\n")...), want: ErrInvalidRecord},
		{name: "future file schema", data: future, want: ErrUnsupportedSchema},
		{name: "unknown field", data: unknownField, want: ErrInvalidRecord},
		{name: "duplicate JSON field", data: duplicateField, want: ErrInvalidRecord},
		{name: "checksum mismatch", data: badChecksum, want: ErrInvalidRecord},
		{name: "missing collection", data: missingCollection, want: ErrInvalidRecord},
		{name: "duplicate operation", data: duplicate, want: ErrInvalidRecord},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(secureStateTestDir(t), "state.json")
			if err := os.WriteFile(path, test.data, filePermission); err != nil {
				t.Fatalf("WriteFile(corrupt fixture) error = %v", err)
			}
			if _, err := NewFileStore(path); !errors.Is(err, test.want) {
				t.Fatalf("NewFileStore() error = %v, want %v", err, test.want)
			}
		})
	}
}

// TestFileStoreRetentionSurvivesReopen verifies exact replay, tombstones, and
// event resume-gap boundaries are committed in the same current snapshot.
func TestFileStoreRetentionSurvivesReopen(t *testing.T) {
	path := filepath.Join(secureStateTestDir(t), "state.json")
	policy := RetentionPolicy{TerminalOperationLimit: 1, OperationIdentityLimit: 6, EventLimit: 2}
	store, err := NewFileStoreWithRetention(path, policy)
	if err != nil {
		t.Fatalf("NewFileStoreWithRetention() error = %v", err)
	}
	for _, id := range []string{"op-file-retain-1", "op-file-retain-2", "op-file-retain-3", "op-file-retain-4"} {
		putTerminalOperationWithEvent(t, store, id, "sandbox-"+id)
	}
	closeFileStoreForTest(t, store)

	reopened, err := NewFileStoreWithRetention(path, policy)
	if err != nil {
		t.Fatalf("NewFileStoreWithRetention(reopen) error = %v", err)
	}
	defer closeFileStoreForTest(t, reopened)
	err = reopened.View(context.Background(), func(reader Reader) error {
		if _, getErr := reader.GetOperation("op-file-retain-1"); !errors.Is(getErr, ErrOperationExpired) {
			t.Fatalf("GetOperation(retired after reopen) error = %v, want ErrOperationExpired", getErr)
		}
		latest, getErr := reader.GetOperation("op-file-retain-4")
		if getErr != nil {
			return getErr
		}
		if string(latest.Operation.Response) != `{"retained":true}` {
			t.Fatalf("reopened response = %s, want exact retained response", latest.Operation.Response)
		}
		if _, eventsErr := reader.EventsAfter(1, 0); !errors.Is(eventsErr, ErrEventResumeGap) {
			t.Fatalf("EventsAfter(stale after reopen) error = %v, want ErrEventResumeGap", eventsErr)
		}
		events, eventsErr := reader.EventsAfter(0, 0)
		if eventsErr != nil {
			return eventsErr
		}
		if got := eventSequences(events); len(got) != 2 || got[0] != 3 || got[1] != 4 {
			t.Fatalf("EventsAfter(0) after reopen = %v, want [3 4]", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("FileStore.View(retained reopen) error = %v", err)
	}
}

// TestFileStoreRejectsChecksummedCurrentObservationGenerationTamper verifies a
// checksummed event cannot claim a generation beyond the exact current resource
// incarnation named by LastObservation, even though historical incarnations may
// legitimately have different projections.
func TestFileStoreRejectsChecksummedCurrentObservationGenerationTamper(t *testing.T) {
	path := filepath.Join(secureStateTestDir(t), "state.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	sandbox := testReadySandbox(t, "sandbox-current-generation-tamper")
	op := testOperation("op-current-generation-tamper", string(sandbox.ID))
	op.State = operation.StateSucceeded
	op.Stage = operation.StageComplete
	op.Result = operation.ResultSucceeded
	op.Response = []byte(`{"ready":true}`)
	event := testEvent(op)
	event.Stage = op.Stage
	event.Result = op.Result
	event.Generation = uint64(sandbox.Status.Generation)
	event.ObservedGeneration = uint64(sandbox.Status.ObservedGeneration)
	err = store.Update(context.Background(), func(tx Tx) error {
		if _, putErr := tx.PutOperation(NewOperationRecord(op), 0); putErr != nil {
			return putErr
		}
		record, putErr := tx.PutSandbox(NewSandboxRecord(sandbox), 0)
		if putErr != nil {
			return putErr
		}
		storedEvent, appendErr := tx.AppendEvent(event)
		if appendErr != nil {
			return appendErr
		}
		if observationErr := record.Sandbox.SetLastObservation(domain.LifecycleObservation{
			OperationID: string(op.ID), EventSequence: uint64(storedEvent.Sequence), Reason: string(storedEvent.Reason),
		}); observationErr != nil {
			return observationErr
		}
		_, putErr = tx.PutSandbox(record, record.Revision)
		return putErr
	})
	if err != nil {
		t.Fatalf("FileStore.Update(current observation fixture) error = %v", err)
	}
	closeFileStoreForTest(t, store)

	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mutated := mutateFileEnvelope(t, encoded, func(envelope *fileEnvelope) {
		if len(envelope.Payload.Events) != 1 {
			t.Fatalf("event count = %d, want 1", len(envelope.Payload.Events))
		}
		envelope.Payload.Events[0].Generation = 2
		envelope.Payload.Events[0].ObservedGeneration = 2
		envelope.PayloadSHA256 = digestFilePayloadForTest(t, *envelope.Payload)
	})
	if err := os.WriteFile(path, mutated, filePermission); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileStore(path); !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("NewFileStore(generation tamper) error = %v, want ErrInvariantViolation", err)
	}
}

// TestFileStoreRejectsChecksummedHistoricalGenerationRegression verifies the
// loader permits an early 1/0 event before a final 1/1 projection but rejects a
// checksummed edit that makes that same incarnation regress from 2/0 to 1/1.
func TestFileStoreRejectsChecksummedHistoricalGenerationRegression(t *testing.T) {
	path := filepath.Join(secureStateTestDir(t), "state.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	sandbox, err := domain.NewSandbox("sandbox-historical-generation-tamper", domain.SandboxSpec{})
	if err != nil {
		t.Fatal(err)
	}
	op := testOperation("op-historical-generation-tamper", string(sandbox.ID))
	firstEvent := testEvent(op)
	firstEvent.Generation = uint64(sandbox.Status.Generation)
	firstEvent.ObservedGeneration = uint64(sandbox.Status.ObservedGeneration)
	err = store.Update(context.Background(), func(tx Tx) error {
		storedOperation, putErr := tx.PutOperation(NewOperationRecord(op), 0)
		if putErr != nil {
			return putErr
		}
		storedSandbox, putErr := tx.PutSandbox(NewSandboxRecord(sandbox), 0)
		if putErr != nil {
			return putErr
		}
		storedEvent, appendErr := tx.AppendEvent(firstEvent)
		if appendErr != nil {
			return appendErr
		}
		if observationErr := storedSandbox.Sandbox.SetLastObservation(domain.LifecycleObservation{
			OperationID: string(storedOperation.Operation.ID), EventSequence: uint64(storedEvent.Sequence), Reason: string(storedEvent.Reason),
		}); observationErr != nil {
			return observationErr
		}
		_, putErr = tx.PutSandbox(storedSandbox, storedSandbox.Revision)
		return putErr
	})
	if err != nil {
		t.Fatalf("FileStore.Update(creating projection) error = %v", err)
	}
	err = store.Update(context.Background(), func(tx Tx) error {
		storedOperation, getErr := tx.GetOperation(op.ID)
		if getErr != nil {
			return getErr
		}
		storedOperation.Operation.State = operation.StateSucceeded
		storedOperation.Operation.Stage = operation.StageComplete
		storedOperation.Operation.Result = operation.ResultSucceeded
		storedOperation.Operation.Response = []byte(`{"ready":true}`)
		storedOperation, putErr := tx.PutOperation(storedOperation, storedOperation.Revision)
		if putErr != nil {
			return putErr
		}
		storedSandbox, getErr := tx.GetSandbox(sandbox.ID)
		if getErr != nil {
			return getErr
		}
		storedSandbox.Sandbox.Status.Phase = domain.SandboxReady
		storedSandbox.Sandbox.Status.ObservedGeneration = storedSandbox.Sandbox.Status.Generation
		storedSandbox, putErr = tx.PutSandbox(storedSandbox, storedSandbox.Revision)
		if putErr != nil {
			return putErr
		}
		finalEvent := testEvent(storedOperation.Operation)
		finalEvent.Stage = storedOperation.Operation.Stage
		finalEvent.Result = storedOperation.Operation.Result
		finalEvent.Generation = uint64(storedSandbox.Sandbox.Status.Generation)
		finalEvent.ObservedGeneration = uint64(storedSandbox.Sandbox.Status.ObservedGeneration)
		storedEvent, appendErr := tx.AppendEvent(finalEvent)
		if appendErr != nil {
			return appendErr
		}
		if observationErr := storedSandbox.Sandbox.SetLastObservation(domain.LifecycleObservation{
			OperationID: string(storedOperation.Operation.ID), EventSequence: uint64(storedEvent.Sequence), Reason: string(storedEvent.Reason),
		}); observationErr != nil {
			return observationErr
		}
		_, putErr = tx.PutSandbox(storedSandbox, storedSandbox.Revision)
		return putErr
	})
	if err != nil {
		t.Fatalf("FileStore.Update(ready projection) error = %v", err)
	}
	closeFileStoreForTest(t, store)

	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mutated := mutateFileEnvelope(t, encoded, func(envelope *fileEnvelope) {
		if len(envelope.Payload.Events) != 2 {
			t.Fatalf("event count = %d, want 2", len(envelope.Payload.Events))
		}
		envelope.Payload.Events[0].Generation = 2
		envelope.PayloadSHA256 = digestFilePayloadForTest(t, *envelope.Payload)
	})
	if err := os.WriteFile(path, mutated, filePermission); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileStore(path); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("NewFileStore(historical generation regression) error = %v, want ErrInvalidRecord", err)
	}
}

// TestFileStoreRejectsOversizedEnvelope verifies startup checks sparse file
// length before allocation and fails closed above the explicit envelope bound.
func TestFileStoreRejectsOversizedEnvelope(t *testing.T) {
	path := filepath.Join(secureStateTestDir(t), "state.json")
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, filePermission)
	if err != nil {
		t.Fatalf("OpenFile(oversized fixture) error = %v", err)
	}
	if err := handle.Truncate(MaxEnvelopeBytes + 1); err != nil {
		_ = handle.Close()
		t.Fatalf("Truncate(oversized fixture) error = %v", err)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("Close(oversized fixture) error = %v", err)
	}
	if _, err := NewFileStore(path); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("NewFileStore(oversized) error = %v, want ErrInvalidRecord", err)
	}
}

// TestFileStoreRejectsChecksummedV3RetentionTamper proves a valid outer
// checksum cannot authorize retention metadata that changes an event binding.
func TestFileStoreRejectsChecksummedV3RetentionTamper(t *testing.T) {
	path := filepath.Join(secureStateTestDir(t), "state.json")
	policy := RetentionPolicy{TerminalOperationLimit: 1, OperationIdentityLimit: 4, EventLimit: 4}
	store, err := NewFileStoreWithRetention(path, policy)
	if err != nil {
		t.Fatalf("NewFileStoreWithRetention() error = %v", err)
	}
	putTerminalOperationWithEvent(t, store, "op-v2-tamper-1", "sandbox-v2-tamper-1")
	putTerminalOperationWithEvent(t, store, "op-v2-tamper-2", "sandbox-v2-tamper-2")
	closeFileStoreForTest(t, store)

	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(v3 fixture) error = %v", err)
	}
	mutated := mutateFileEnvelope(t, encoded, func(envelope *fileEnvelope) {
		if envelope.SchemaVersion != fileSchemaVersionV3 || len(envelope.Payload.RetiredOperations) != 1 {
			t.Fatalf("v3 fixture schema/retired count = %d/%d, want 3/1", envelope.SchemaVersion, len(envelope.Payload.RetiredOperations))
		}
		envelope.Payload.RetiredOperations[0].Target.ID = "sandbox-valid-but-wrong"
		envelope.PayloadSHA256 = digestFilePayloadForTest(t, *envelope.Payload)
	})
	if err := os.WriteFile(path, mutated, filePermission); err != nil {
		t.Fatalf("WriteFile(checksummed v2 tamper) error = %v", err)
	}
	if _, err := NewFileStoreWithRetention(path, policy); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("NewFileStoreWithRetention(tampered v2) error = %v, want ErrInvalidRecord", err)
	}
}

// TestFileStoreRejectsChecksummedV3MissingTombstone verifies terminal order is
// a contiguous 1..high-watermark set, so deleting one retired ID cannot erase
// idempotency history even when an attacker refreshes the outer checksum.
func TestFileStoreRejectsChecksummedV3MissingTombstone(t *testing.T) {
	path := filepath.Join(secureStateTestDir(t), "state.json")
	policy := RetentionPolicy{TerminalOperationLimit: 1, OperationIdentityLimit: 4, EventLimit: 1}
	store, err := NewFileStoreWithRetention(path, policy)
	if err != nil {
		t.Fatalf("NewFileStoreWithRetention() error = %v", err)
	}
	putTerminalOperationWithEvent(t, store, "op-v2-delete-1", "sandbox-v2-delete-1")
	putTerminalOperationWithEvent(t, store, "op-v2-delete-2", "sandbox-v2-delete-2")
	putTerminalOperationWithEvent(t, store, "op-v2-delete-3", "sandbox-v2-delete-3")
	closeFileStoreForTest(t, store)

	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(v3 tombstone fixture) error = %v", err)
	}
	mutated := mutateFileEnvelope(t, encoded, func(envelope *fileEnvelope) {
		if envelope.SchemaVersion != fileSchemaVersionV3 || len(envelope.Payload.RetiredOperations) != 2 {
			t.Fatalf("v3 fixture schema/retired count = %d/%d, want 3/2", envelope.SchemaVersion, len(envelope.Payload.RetiredOperations))
		}
		envelope.Payload.RetiredOperations = envelope.Payload.RetiredOperations[1:]
		envelope.PayloadSHA256 = digestFilePayloadForTest(t, *envelope.Payload)
	})
	if err := os.WriteFile(path, mutated, filePermission); err != nil {
		t.Fatalf("WriteFile(checksummed missing tombstone) error = %v", err)
	}
	if _, err := NewFileStoreWithRetention(path, policy); !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("NewFileStoreWithRetention(missing tombstone) error = %v, want ErrInvariantViolation", err)
	}
}

// TestFileStoreMigratesV1Envelope verifies an old checksummed snapshot is
// validated, loaded, and atomically rewritten with current retention and event semantics.
func TestFileStoreMigratesV1Envelope(t *testing.T) {
	path := filepath.Join(secureStateTestDir(t), "state.json")
	data := newMemoryData()
	view := &memoryView{data: &data, writable: true, retention: DefaultRetentionPolicy()}
	op := testOperation("op-v1-migrate", "sandbox-v1-migrate")
	if _, err := view.PutOperation(NewOperationRecord(op), 0); err != nil {
		t.Fatalf("PutOperation(v1 fixture) error = %v", err)
	}
	view.close()
	payload := filePayloadFromMemory(data)
	payload.FirstEventSequence = 0
	payload.TerminalOperationSequences = nil
	payload.LastTerminalOperationSequence = 0
	payload.RetiredOperations = nil
	envelope := fileEnvelope{
		SchemaVersion: fileSchemaVersionV1,
		Payload:       &payload,
		PayloadSHA256: digestFilePayloadForTest(t, payload),
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("Marshal(v1 fixture) error = %v", err)
	}
	if err := os.WriteFile(path, encoded, filePermission); err != nil {
		t.Fatalf("WriteFile(v1 fixture) error = %v", err)
	}

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(v1 migration) error = %v", err)
	}
	closeFileStoreForTest(t, store)
	migrated, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(migrated fixture) error = %v", err)
	}
	decoded, err := decodeFileEnvelope(migrated)
	if err != nil {
		t.Fatalf("decodeFileEnvelope(migrated) error = %v", err)
	}
	if decoded.SchemaVersion != fileSchemaVersionV3 || decoded.Payload.FirstEventSequence != 1 {
		t.Fatalf("migrated schema/first event = %d/%d, want 3/1", decoded.SchemaVersion, decoded.Payload.FirstEventSequence)
	}
}

// TestFileStoreMigratesLegacyZeroDuration verifies independently checksummed
// v1 and v2 envelopes convert their historical zero placeholders to unavailable
// timing evidence before being rewritten with current event semantics.
func TestFileStoreMigratesLegacyZeroDuration(t *testing.T) {
	for _, fileSchema := range []uint32{fileSchemaVersionV1, fileSchemaVersionV2} {
		t.Run(fmt.Sprintf("file_schema_%d", fileSchema), func(t *testing.T) {
			path := filepath.Join(secureStateTestDir(t), "state.json")
			writeLegacyZeroDurationEnvelope(t, path, fileSchema)
			store, err := NewFileStore(path)
			if err != nil {
				t.Fatalf("NewFileStore(legacy duration migration) error = %v", err)
			}
			if err := store.View(context.Background(), func(reader Reader) error {
				events, readErr := reader.EventsAfter(0, 0)
				if readErr != nil {
					return readErr
				}
				if len(events) != 1 || events[0].SchemaVersion != operation.EventSchemaVersion || events[0].Duration != nil {
					t.Fatalf("migrated legacy event = %#v, want event schema %d with unavailable duration", events, operation.EventSchemaVersion)
				}
				return nil
			}); err != nil {
				t.Fatalf("View(migrated legacy event) error = %v", err)
			}
			closeFileStoreForTest(t, store)
			migrated, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile(migrated legacy envelope) error = %v", err)
			}
			decoded, err := decodeFileEnvelope(migrated)
			if err != nil {
				t.Fatalf("decodeFileEnvelope(migrated legacy envelope) error = %v", err)
			}
			if decoded.SchemaVersion != fileSchemaVersionV3 || decoded.Payload.Events[0].Duration != nil {
				t.Fatalf("migrated envelope schema/duration = %d/%#v, want 3/nil", decoded.SchemaVersion, decoded.Payload.Events[0].Duration)
			}
		})
	}
}

// TestFileStoreRejectsLegacyEventSchemaInV3 verifies the current envelope never
// silently applies legacy placeholder semantics without the legacy file-schema marker.
func TestFileStoreRejectsLegacyEventSchemaInV3(t *testing.T) {
	path := filepath.Join(secureStateTestDir(t), "state.json")
	data := newMemoryData()
	view := &memoryView{data: &data, writable: true, retention: DefaultRetentionPolicy()}
	op := testOperation("op-v3-legacy-event-schema", "sandbox-v3-legacy-event-schema")
	if _, err := view.PutOperation(NewOperationRecord(op), 0); err != nil {
		t.Fatalf("PutOperation(current fixture) error = %v", err)
	}
	if _, err := view.AppendEvent(testEvent(op)); err != nil {
		t.Fatalf("AppendEvent(current fixture) error = %v", err)
	}
	view.close()
	encoded, err := encodeFileData(data)
	if err != nil {
		t.Fatalf("encodeFileData(current fixture) error = %v", err)
	}
	mutated := mutateFileEnvelope(t, encoded, func(envelope *fileEnvelope) {
		if envelope.SchemaVersion != fileSchemaVersionV3 || len(envelope.Payload.Events) != 1 {
			t.Fatalf("current fixture schema/events = %d/%d, want 3/1", envelope.SchemaVersion, len(envelope.Payload.Events))
		}
		envelope.Payload.Events[0].SchemaVersion = legacyEventSchemaVersionV1
		envelope.PayloadSHA256 = digestFilePayloadForTest(t, *envelope.Payload)
	})
	if err := os.WriteFile(path, mutated, filePermission); err != nil {
		t.Fatalf("WriteFile(current legacy-event fixture) error = %v", err)
	}
	if _, err := NewFileStore(path); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("NewFileStore(current legacy-event fixture) error = %v, want ErrUnsupportedSchema", err)
	}
}

// writeLegacyZeroDurationEnvelope creates a valid legacy checksum independently
// from production digest helpers, preserving the old mandatory numeric duration field.
func writeLegacyZeroDurationEnvelope(t *testing.T, path string, fileSchema uint32) {
	t.Helper()
	data := newMemoryData()
	view := &memoryView{data: &data, writable: true, retention: DefaultRetentionPolicy()}
	op := testOperation(fmt.Sprintf("op-v%d-duration-migrate", fileSchema), fmt.Sprintf("sandbox-v%d-duration-migrate", fileSchema))
	if _, err := view.PutOperation(NewOperationRecord(op), 0); err != nil {
		t.Fatalf("PutOperation(legacy duration fixture) error = %v", err)
	}
	if _, err := view.AppendEvent(testEvent(op)); err != nil {
		t.Fatalf("AppendEvent(legacy duration fixture) error = %v", err)
	}
	view.close()
	payload := filePayloadFromMemory(data)
	if fileSchema == fileSchemaVersionV1 {
		payload.FirstEventSequence = 0
		payload.TerminalOperationSequences = nil
		payload.LastTerminalOperationSequence = 0
		payload.RetiredOperations = nil
	}
	legacyZero := operation.Duration(0)
	payload.Events[0].SchemaVersion = legacyEventSchemaVersionV1
	payload.Events[0].Duration = &legacyZero
	legacyPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal(legacy payload) error = %v", err)
	}
	if !bytes.Contains(legacyPayload, []byte(`"duration_ns":0`)) {
		t.Fatalf("legacy payload = %s, want explicit zero duration", legacyPayload)
	}
	digest := sha256.Sum256(legacyPayload)
	envelope := struct {
		SchemaVersion uint32          `json:"schema_version"`
		Payload       json.RawMessage `json:"payload"`
		PayloadSHA256 string          `json:"payload_sha256"`
	}{SchemaVersion: fileSchema, Payload: legacyPayload, PayloadSHA256: fmt.Sprintf("%x", digest[:])}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("Marshal(legacy envelope) error = %v", err)
	}
	if err := os.WriteFile(path, encoded, filePermission); err != nil {
		t.Fatalf("WriteFile(legacy envelope) error = %v", err)
	}
}

// TestFileStoreRejectsUnsafeFileKindsAndPermissions verifies startup will not
// follow a state symlink or load metadata readable by group or other users.
func TestFileStoreRejectsUnsafeFileKindsAndPermissions(t *testing.T) {
	t.Run("permissions", func(t *testing.T) {
		path := filepath.Join(secureStateTestDir(t), "state.json")
		store, err := NewFileStore(path)
		if err != nil || store == nil {
			t.Fatalf("NewFileStore(setup) = %v, %v", store, err)
		}
		closeFileStoreForTest(t, store)
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatalf("Chmod(state) error = %v", err)
		}
		if _, err := NewFileStore(path); !errors.Is(err, ErrInvalidRecord) {
			t.Fatalf("NewFileStore(insecure mode) error = %v, want ErrInvalidRecord", err)
		}
	})
	t.Run("symlink", func(t *testing.T) {
		directory := secureStateTestDir(t)
		target := filepath.Join(directory, "target.json")
		store, err := NewFileStore(target)
		if err != nil {
			t.Fatalf("NewFileStore(target) error = %v", err)
		}
		closeFileStoreForTest(t, store)
		link := filepath.Join(directory, "state-link.json")
		if err := os.Symlink(target, link); err != nil {
			t.Fatalf("Symlink(state) error = %v", err)
		}
		if _, err := NewFileStore(link); !errors.Is(err, ErrInvalidRecord) {
			t.Fatalf("NewFileStore(symlink) error = %v, want ErrInvalidRecord", err)
		}
	})
	t.Run("lock symlink", func(t *testing.T) {
		directory := secureStateTestDir(t)
		path := filepath.Join(directory, "state.json")
		target := filepath.Join(directory, "lock-target")
		if err := os.WriteFile(target, nil, filePermission); err != nil {
			t.Fatalf("WriteFile(lock target) error = %v", err)
		}
		if err := os.Symlink(target, stateLockPath(path)); err != nil {
			t.Fatalf("Symlink(lock) error = %v", err)
		}
		if _, err := NewFileStore(path); !errors.Is(err, ErrInvalidRecord) {
			t.Fatalf("NewFileStore(lock symlink) error = %v, want ErrInvalidRecord", err)
		}
	})
}

// TestFileStoreRejectsForeignOwners verifies injected st_uid mismatches on an
// ancestor, final directory, state file, and stable lock all fail before use.
func TestFileStoreRejectsForeignOwners(t *testing.T) {
	tests := []struct {
		name   string
		target func(root, path string) string
		setup  bool
	}{
		{
			name: "ancestor",
			target: func(root, _ string) string {
				return filepath.Join(root, "ancestor")
			},
		},
		{
			name: "final directory",
			target: func(_, path string) string {
				return filepath.Dir(path)
			},
		},
		{
			name:  "state file",
			setup: true,
			target: func(_, path string) string {
				return path
			},
		},
		{
			name:  "stable lock",
			setup: true,
			target: func(_, path string) string {
				return stateLockPath(path)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := secureStateTestDir(t)
			directory := filepath.Join(root, "ancestor", "runtime")
			if err := os.MkdirAll(directory, 0o700); err != nil {
				t.Fatalf("MkdirAll(state hierarchy) error = %v", err)
			}
			path := filepath.Join(directory, "state.json")
			if test.setup {
				store, err := NewFileStore(path)
				if err != nil {
					t.Fatalf("NewFileStore(setup) error = %v", err)
				}
				closeFileStoreForTest(t, store)
			}
			files := defaultFilePrimitives()
			injectForeignOwner(&files, test.target(root, path))
			store, err := newFileStoreWithPrimitives(path, files)
			if store != nil {
				closeFileStoreForTest(t, store)
			}
			if !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("newFileStoreWithPrimitives(foreign %s) error = %v, want ErrInvalidRecord", test.name, err)
			}
		})
	}
}

// TestFileStoreRootfulPolicyRejectsForeignPrivatePath verifies a daemon modeled
// with euid zero does not accept a different uid merely because modes are 0700/0600.
func TestFileStoreRootfulPolicyRejectsForeignPrivatePath(t *testing.T) {
	root := secureStateTestDir(t)
	path := filepath.Join(root, "state.json")
	files := defaultFilePrimitives()
	files.effectiveUID = func() uint32 { return 0 }
	injectOwnerUID(&files, root, 1000)
	store, err := newFileStoreWithPrimitives(path, files)
	if store != nil {
		closeFileStoreForTest(t, store)
	}
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("newFileStoreWithPrimitives(rootful foreign 0700) error = %v, want ErrInvalidRecord", err)
	}
}

// TestFileStoreRejectsWritableAncestorBeforeCreation verifies a group-writable
// ancestor is rejected without creating any descendant state directory.
func TestFileStoreRejectsWritableAncestorBeforeCreation(t *testing.T) {
	root := secureStateTestDir(t)
	unsafe := filepath.Join(root, "unsafe")
	if err := os.Mkdir(unsafe, 0o700); err != nil {
		t.Fatalf("Mkdir(unsafe ancestor) error = %v", err)
	}
	if err := os.Chmod(unsafe, 0o770); err != nil {
		t.Fatalf("Chmod(unsafe ancestor) error = %v", err)
	}
	directory := filepath.Join(unsafe, "must-not-exist")
	if _, err := NewFileStore(filepath.Join(directory, "state.json")); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("NewFileStore(writable ancestor) error = %v, want ErrInvalidRecord", err)
	}
	if _, err := os.Lstat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Lstat(descendant after rejection) error = %v, want not-exist", err)
	}
}

// TestFileStoreDetectsLockPathReplacement verifies a replaced lock inode cannot
// permit a second store and invalidates further callbacks on the original store.
func TestFileStoreDetectsLockPathReplacement(t *testing.T) {
	directory := secureStateTestDir(t)
	path := filepath.Join(directory, "state.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(setup) error = %v", err)
	}
	defer closeFileStoreForTest(t, store)
	lockPath := stateLockPath(path)
	displaced := lockPath + ".displaced"
	if err := os.Rename(lockPath, displaced); err != nil {
		t.Fatalf("Rename(lock replacement setup) error = %v", err)
	}
	defer func() { _ = os.Remove(displaced) }()
	if err := os.WriteFile(lockPath, nil, filePermission); err != nil {
		t.Fatalf("WriteFile(replacement lock) error = %v", err)
	}
	callbackCalled := false
	err = store.View(context.Background(), func(Reader) error {
		callbackCalled = true
		return nil
	})
	if !errors.Is(err, ErrInvalidRecord) || callbackCalled {
		t.Fatalf("FileStore.View(replaced lock) error/callback = %v/%t, want ErrInvalidRecord/false", err, callbackCalled)
	}
	contender, err := NewFileStore(path)
	if contender != nil {
		closeFileStoreForTest(t, contender)
	}
	if !errors.Is(err, ErrFileStoreLocked) {
		t.Fatalf("NewFileStore(after lock replacement) error = %v, want ErrFileStoreLocked", err)
	}
}

// TestFileStoreDetectsFinalDirectorySwap verifies the parent-anchored lock bars
// a contender while the original store detects its canonical directory changed.
func TestFileStoreDetectsFinalDirectorySwap(t *testing.T) {
	root := secureStateTestDir(t)
	directory := filepath.Join(root, "runtime")
	path := filepath.Join(directory, "state.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(setup) error = %v", err)
	}
	defer closeFileStoreForTest(t, store)
	displaced := directory + ".displaced"
	if err := os.Rename(directory, displaced); err != nil {
		t.Fatalf("Rename(state directory) error = %v", err)
	}
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("Mkdir(replacement state directory) error = %v", err)
	}
	callbackCalled := false
	err = store.Update(context.Background(), func(Tx) error {
		callbackCalled = true
		return nil
	})
	if !errors.Is(err, ErrInvalidRecord) || callbackCalled {
		t.Fatalf("FileStore.Update(swapped directory) error/callback = %v/%t, want ErrInvalidRecord/false", err, callbackCalled)
	}
	contender, err := NewFileStore(path)
	if contender != nil {
		closeFileStoreForTest(t, contender)
	}
	if !errors.Is(err, ErrFileStoreLocked) {
		t.Fatalf("NewFileStore(swapped directory contender) error = %v, want ErrFileStoreLocked", err)
	}
}

// TestFileStoreDetectsStateFileReplacement verifies valid-looking replacement
// bytes cannot supersede the exact inode FileStore created and retained.
func TestFileStoreDetectsStateFileReplacement(t *testing.T) {
	directory := secureStateTestDir(t)
	path := filepath.Join(directory, "state.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(setup) error = %v", err)
	}
	defer closeFileStoreForTest(t, store)
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(state replacement fixture) error = %v", err)
	}
	displaced := path + ".displaced"
	if err := os.Rename(path, displaced); err != nil {
		t.Fatalf("Rename(state replacement setup) error = %v", err)
	}
	if err := os.WriteFile(path, encoded, filePermission); err != nil {
		t.Fatalf("WriteFile(replacement state) error = %v", err)
	}
	callbackCalled := false
	err = store.View(context.Background(), func(Reader) error {
		callbackCalled = true
		return nil
	})
	if !errors.Is(err, ErrInvalidRecord) || callbackCalled {
		t.Fatalf("FileStore.View(replaced state) error/callback = %v/%t, want ErrInvalidRecord/false", err, callbackCalled)
	}
}

// TestFileStoreDurablyCreatesNestedStateDirectory verifies each new directory
// entry and the final state rename are fsynced through the injectable boundary.
func TestFileStoreDurablyCreatesNestedStateDirectory(t *testing.T) {
	root := secureStateTestDir(t)
	directory := filepath.Join(root, "one", "two")
	path := filepath.Join(directory, "state.json")
	files := defaultFilePrimitives()
	originalSync := files.syncDirectory
	synced := make(map[string]int)
	files.syncDirectory = func(handle *os.File) error {
		synced[filepath.Clean(handle.Name())]++
		return originalSync(handle)
	}
	store, err := newFileStoreWithPrimitives(path, files)
	if err != nil {
		t.Fatalf("newFileStoreWithPrimitives(nested) error = %v", err)
	}
	closeFileStoreForTest(t, store)
	for _, want := range []string{root, filepath.Join(root, "one"), directory} {
		if synced[want] == 0 {
			t.Fatalf("directory sync calls = %v, missing %q", synced, want)
		}
	}
}

// TestFileStorePreRenameFailuresPreservePreviousCommit injects each failure
// before rename and proves memory, bytes, CAS state, and event sequence remain reusable.
func TestFileStorePreRenameFailuresPreservePreviousCommit(t *testing.T) {
	stages := []string{"write", "file_sync", "rename"}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(secureStateTestDir(t), "state.json")
			base, err := NewFileStore(path)
			if err != nil {
				t.Fatalf("NewFileStore(base) error = %v", err)
			}
			first := testOperation("op-fault-first", "sandbox-fault-first")
			firstEvent := testEvent(first)
			err = base.Update(context.Background(), func(tx Tx) error {
				if _, err := tx.PutOperation(NewOperationRecord(first), 0); err != nil {
					return err
				}
				_, err := tx.AppendEvent(firstEvent)
				return err
			})
			if err != nil {
				t.Fatalf("base Update() error = %v", err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile(before) error = %v", err)
			}
			closeFileStoreForTest(t, base)

			injected := errors.New("injected " + stage + " failure")
			enabled := false
			files := defaultFilePrimitives()
			installFileFault(&files, stage, &enabled, injected)
			store, err := newFileStoreWithPrimitives(path, files)
			if err != nil {
				t.Fatalf("newFileStoreWithPrimitives() error = %v", err)
			}
			enabled = true
			second := testOperation("op-fault-second", "sandbox-fault-second")
			secondEvent := testEvent(second)
			var failedSequence EventSequence
			err = store.Update(context.Background(), func(tx Tx) error {
				if _, err := tx.PutOperation(NewOperationRecord(second), 0); err != nil {
					return err
				}
				event, err := tx.AppendEvent(secondEvent)
				failedSequence = event.Sequence
				return err
			})
			if !errors.Is(err, injected) {
				t.Fatalf("faulted Update() error = %v, want injected error", err)
			}
			if errors.Is(err, ErrDurabilityUncertain) {
				t.Fatalf("pre-rename Update() error = %v, must not be durability-uncertain", err)
			}
			if failedSequence != 2 {
				t.Fatalf("candidate event sequence = %d, want 2", failedSequence)
			}
			assertFileStoreSnapshot(t, store, 1, 1)
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile(after failure) error = %v", err)
			}
			if !bytes.Equal(after, before) {
				t.Fatal("failed persistence changed the recoverable committed file")
			}
			enabled = false
			var committedSequence EventSequence
			err = store.Update(context.Background(), func(tx Tx) error {
				if _, err := tx.PutOperation(NewOperationRecord(second), 0); err != nil {
					return err
				}
				event, err := tx.AppendEvent(secondEvent)
				committedSequence = event.Sequence
				return err
			})
			if err != nil {
				t.Fatalf("Update(after repair) error = %v", err)
			}
			if committedSequence != 2 {
				t.Fatalf("committed event sequence = %d, want reused 2", committedSequence)
			}
			closeFileStoreForTest(t, store)
			finalStore, err := NewFileStore(path)
			if err != nil {
				t.Fatalf("NewFileStore(final) error = %v", err)
			}
			assertFileStoreSnapshot(t, finalStore, 2, 2)
			closeFileStoreForTest(t, finalStore)
		})
	}
}

// TestFileStoreDirectorySyncFailurePoisonsUntilReopen verifies a completed
// rename is never disguised as rollback, a reopen that cannot fsync the held
// directory still refuses to publish the visible snapshot, and a later
// successful reopen recovers that exact candidate.
func TestFileStoreDirectorySyncFailurePoisonsUntilReopen(t *testing.T) {
	path := filepath.Join(secureStateTestDir(t), "state.json")
	base, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(base) error = %v", err)
	}
	first := testOperation("op-fault-first", "sandbox-fault-first")
	err = base.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutOperation(NewOperationRecord(first), 0); err != nil {
			return err
		}
		_, err := tx.AppendEvent(testEvent(first))
		return err
	})
	if err != nil {
		t.Fatalf("base Update() error = %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(before) error = %v", err)
	}
	closeFileStoreForTest(t, base)

	injected := errors.New("injected directory_sync failure")
	enabled := false
	files := defaultFilePrimitives()
	installFileFault(&files, "directory_sync", &enabled, injected)
	store, err := newFileStoreWithPrimitives(path, files)
	if err != nil {
		t.Fatalf("newFileStoreWithPrimitives() error = %v", err)
	}
	enabled = true
	second := testOperation("op-fault-second", "sandbox-fault-second")
	err = store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutOperation(NewOperationRecord(second), 0); err != nil {
			return err
		}
		_, err := tx.AppendEvent(testEvent(second))
		return err
	})
	if !errors.Is(err, injected) || !errors.Is(err, ErrDurabilityUncertain) {
		t.Fatalf("post-rename Update() error = %v, want injected and ErrDurabilityUncertain", err)
	}
	callbackCalled := false
	err = store.View(context.Background(), func(Reader) error {
		callbackCalled = true
		return nil
	})
	if !errors.Is(err, ErrDurabilityUncertain) || callbackCalled {
		t.Fatalf("poisoned View() error/callback = %v/%t, want durability error/false", err, callbackCalled)
	}
	err = store.Update(context.Background(), func(Tx) error {
		callbackCalled = true
		return nil
	})
	if !errors.Is(err, ErrDurabilityUncertain) || callbackCalled {
		t.Fatalf("poisoned Update() error/callback = %v/%t, want durability error/false", err, callbackCalled)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(after rename) error = %v", err)
	}
	if bytes.Equal(after, before) {
		t.Fatal("post-rename failure silently restored the previous state bytes")
	}
	closeFileStoreForTest(t, store)

	failedReopen, err := newFileStoreWithPrimitives(path, files)
	if failedReopen != nil {
		closeFileStoreForTest(t, failedReopen)
	}
	if !errors.Is(err, injected) || !errors.Is(err, ErrDurabilityUncertain) {
		t.Fatalf("newFileStoreWithPrimitives(repeated directory sync failure) error = %v, want injected and ErrDurabilityUncertain", err)
	}
	enabled = false
	recovered, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(after uncertain commit) error = %v", err)
	}
	assertFileStoreSnapshot(t, recovered, 2, 2)
	closeFileStoreForTest(t, recovered)
}

// TestFileStoreInitializationSyncFailureReopens verifies an uncertain initial
// rename releases the lock and leaves the complete visible snapshot for strict reload.
func TestFileStoreInitializationSyncFailureReopens(t *testing.T) {
	path := filepath.Join(secureStateTestDir(t), "state.json")
	injected := errors.New("injected initial directory_sync failure")
	enabled := true
	files := defaultFilePrimitives()
	installFileFault(&files, "directory_sync", &enabled, injected)
	store, err := newFileStoreWithPrimitives(path, files)
	if store != nil {
		closeFileStoreForTest(t, store)
	}
	if !errors.Is(err, injected) || !errors.Is(err, ErrDurabilityUncertain) {
		t.Fatalf("newFileStoreWithPrimitives() error = %v, want injected and ErrDurabilityUncertain", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("Stat(visible initial snapshot) error = %v", err)
	}
	reopened, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(after uncertain initialization) error = %v", err)
	}
	assertFileStoreSnapshot(t, reopened, 0, 0)
	closeFileStoreForTest(t, reopened)
}

// TestFileStoreExclusiveLockAndClose verifies a stable sibling lock rejects a
// second daemon, transfers after Close, and permanently disables the old handle.
func TestFileStoreExclusiveLockAndClose(t *testing.T) {
	path := filepath.Join(secureStateTestDir(t), "state.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(first) error = %v", err)
	}
	lockInfo, err := os.Stat(stateLockPath(path))
	if err != nil {
		t.Fatalf("Stat(lock) error = %v", err)
	}
	if !lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm() != filePermission {
		t.Fatalf("lock mode = %v/%#o, want regular/%#o", lockInfo.Mode().Type(), lockInfo.Mode().Perm(), filePermission)
	}
	contender, err := NewFileStore(path)
	if contender != nil {
		closeFileStoreForTest(t, contender)
	}
	if !errors.Is(err, ErrFileStoreLocked) {
		t.Fatalf("NewFileStore(contender) error = %v, want ErrFileStoreLocked", err)
	}
	closeFileStoreForTest(t, store)
	if err := store.Close(); err != nil {
		t.Fatalf("FileStore.Close(idempotent) error = %v", err)
	}
	if err := store.View(context.Background(), func(Reader) error { return nil }); !errors.Is(err, ErrFileStoreClosed) {
		t.Fatalf("FileStore.View(after Close) error = %v, want ErrFileStoreClosed", err)
	}
	callbackCalled := false
	if err := store.Update(context.Background(), func(Tx) error {
		callbackCalled = true
		return nil
	}); !errors.Is(err, ErrFileStoreClosed) || callbackCalled {
		t.Fatalf("FileStore.Update(after Close) error/callback = %v/%t, want ErrFileStoreClosed/false", err, callbackCalled)
	}

	reopened, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(after Close) error = %v", err)
	}
	closeFileStoreForTest(t, reopened)
}

// TestFileStoreConcurrentCASSurvivesRestart verifies serialization allows one
// stale writer to win and persists the winning revision without race or torn state.
func TestFileStoreConcurrentCASSurvivesRestart(t *testing.T) {
	path := filepath.Join(secureStateTestDir(t), "state.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	op := testOperation("op-file-concurrent", "sandbox-file-concurrent")
	created := putTestOperation(t, store, op)
	candidate := created.Clone()
	candidate.Operation.State = operation.StateRunning
	candidate.Operation.Stage = operation.StageTransition

	const writers = 12
	var successes atomic.Int64
	var conflicts atomic.Int64
	var unexpectedMu sync.Mutex
	var unexpected error
	var wait sync.WaitGroup
	wait.Add(writers)
	for index := 0; index < writers; index++ {
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
	if successes.Load() != 1 || conflicts.Load() != writers-1 {
		t.Fatalf("successes/conflicts = %d/%d, want 1/%d", successes.Load(), conflicts.Load(), writers-1)
	}
	closeFileStoreForTest(t, store)
	reopened, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(reopen) error = %v", err)
	}
	err = reopened.View(context.Background(), func(reader Reader) error {
		record, err := reader.GetOperation(op.ID)
		if err != nil {
			return err
		}
		if record.Revision != 2 || record.Operation.Stage != operation.StageTransition {
			t.Fatalf("winning record revision/stage = %d/%s, want 2/%s", record.Revision, record.Operation.Stage, operation.StageTransition)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("FileStore.View(reopened) error = %v", err)
	}
	closeFileStoreForTest(t, reopened)
}

// TestListOperationsIncludesAbsentDeleteTarget verifies reconciler discovery is
// operation-driven and does not depend on target resource metadata still existing.
func TestListOperationsIncludesAbsentDeleteTarget(t *testing.T) {
	store := NewMemoryStore()
	deleteOperation := testOperation("op-delete-absent", "sandbox-already-absent")
	deleteOperation.Type = operation.TypeDelete
	putTestOperation(t, store, deleteOperation)
	err := store.View(context.Background(), func(reader Reader) error {
		records, err := reader.ListOperations()
		if err != nil {
			return err
		}
		if len(records) != 1 || records[0].Operation.ID != deleteOperation.ID || !records[0].Operation.State.Active() {
			t.Fatalf("ListOperations() = %v, want active absent-target delete %q", operationRecordIDs(records), deleteOperation.ID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("MemoryStore.View() error = %v", err)
	}
}

// mutateFileEnvelope decodes a valid fixture, applies one intentional mutation,
// and marshals it without silently refreshing fields the scenario means to corrupt.
func mutateFileEnvelope(t *testing.T, encoded []byte, mutate func(*fileEnvelope)) []byte {
	t.Helper()
	var envelope fileEnvelope
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatalf("Unmarshal(envelope fixture) error = %v", err)
	}
	mutate(&envelope)
	result, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("Marshal(mutated envelope) error = %v", err)
	}
	return result
}

// digestFilePayloadForTest refreshes a checksum only when a test needs a
// structurally valid envelope around an intentionally invalid payload.
func digestFilePayloadForTest(t *testing.T, payload filePayload) string {
	t.Helper()
	digest, err := filePayloadDigest(payload)
	if err != nil {
		t.Fatalf("filePayloadDigest() error = %v", err)
	}
	return digest
}

// appendUnknownEnvelopeField adds a top-level key while preserving all valid
// data so the strict unknown-field decoder is the only expected rejection.
func appendUnknownEnvelopeField(t *testing.T, encoded []byte) []byte {
	t.Helper()
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatalf("Unmarshal(envelope map) error = %v", err)
	}
	object["future_field"] = json.RawMessage("true")
	result, err := json.Marshal(object)
	if err != nil {
		t.Fatalf("Marshal(unknown field envelope) error = %v", err)
	}
	return result
}

// duplicateOperationEnvelope builds a checksummed payload containing the same
// operation twice so recovery duplicate detection is exercised after checksum validation.
func duplicateOperationEnvelope(t *testing.T) []byte {
	t.Helper()
	data := newMemoryData()
	view := &memoryView{data: &data, writable: true}
	record, err := view.PutOperation(NewOperationRecord(testOperation("op-disk-duplicate", "sandbox-disk-duplicate")), 0)
	if err != nil {
		t.Fatalf("PutOperation(duplicate fixture) error = %v", err)
	}
	view.close()
	payload := filePayloadFromMemory(data)
	payload.Operations = append(payload.Operations, record.Clone())
	payload.FirstEventSequence = 0
	payload.TerminalOperationSequences = nil
	payload.LastTerminalOperationSequence = 0
	payload.RetiredOperations = nil
	envelope := fileEnvelope{
		SchemaVersion: fileSchemaVersionV1,
		Payload:       &payload,
		PayloadSHA256: digestFilePayloadForTest(t, payload),
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("Marshal(duplicate envelope) error = %v", err)
	}
	return encoded
}

// installFileFault replaces one durable primitive with a switchable injected
// error while delegating to the production implementation when disabled.
func installFileFault(files *filePrimitives, stage string, enabled *bool, injected error) {
	switch stage {
	case "write":
		original := files.writeFile
		files.writeFile = func(file *os.File, encoded []byte) error {
			if *enabled {
				return injected
			}
			return original(file, encoded)
		}
	case "file_sync":
		original := files.syncFile
		files.syncFile = func(file *os.File) error {
			if *enabled {
				return injected
			}
			return original(file)
		}
	case "rename":
		original := files.renameAt
		files.renameAt = func(directory *os.File, oldName, newName string) error {
			if *enabled {
				return injected
			}
			return original(directory, oldName, newName)
		}
	case "directory_sync":
		original := files.syncDirectory
		files.syncDirectory = func(directory *os.File) error {
			if *enabled {
				return injected
			}
			return original(directory)
		}
	default:
		panic("unknown file fault stage " + stage)
	}
}

// assertFileStoreSnapshot checks the externally visible operation and event
// counts without depending on FileStore internals, matching daemon recovery use.
func assertFileStoreSnapshot(t *testing.T, store Store, wantOperations, wantEvents int) {
	t.Helper()
	err := store.View(context.Background(), func(reader Reader) error {
		records, err := reader.ListOperations()
		if err != nil {
			return err
		}
		events, err := reader.EventsAfter(0, 0)
		if err != nil {
			return err
		}
		if len(records) != wantOperations || len(events) != wantEvents {
			t.Fatalf("snapshot operation/event counts = %d/%d, want %d/%d",
				len(records), len(events), wantOperations, wantEvents)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Store.View(snapshot) error = %v", err)
	}
}

// operationRecordIDs formats deterministic operation-list output for focused
// failure messages without exposing internal map iteration order.
func operationRecordIDs(records []OperationRecord) []operation.OperationID {
	ids := make([]operation.OperationID, len(records))
	for index, record := range records {
		ids[index] = record.Operation.ID
	}
	return ids
}

// eventSequences formats event ordering in persistence assertion failures.
func eventSequences(events []operation.Event) []EventSequence {
	sequences := make([]EventSequence, len(events))
	for index, event := range events {
		sequences[index] = event.Sequence
	}
	return sequences
}

// closeFileStoreForTest releases the daemon lock and fails the current scenario
// immediately because a leaked lock would make all subsequent reopen checks invalid.
func closeFileStoreForTest(t *testing.T, store *FileStore) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatalf("FileStore.Close() error = %v", err)
	}
}

// injectForeignOwner overrides one exact metadata path with a deterministic uid
// different from the modeled daemon without changing ownership on the host.
func injectForeignOwner(files *filePrimitives, target string) {
	foreign := files.effectiveUID() + 1
	if foreign == files.effectiveUID() {
		foreign = files.effectiveUID() - 1
	}
	injectOwnerUID(files, target, foreign)
}

// injectOwnerUID replaces the st_uid seam for one canonical path while all
// other filesystem objects continue using their real Linux metadata.
func injectOwnerUID(files *filePrimitives, target string, owner uint32) {
	original := files.ownerUID
	target = filepath.Clean(target)
	files.ownerUID = func(path string, info os.FileInfo) (uint32, error) {
		if filepath.Clean(path) == target {
			return owner, nil
		}
		return original(path, info)
	}
}

// secureStateTestDir creates an owner-only test directory below the package
// workspace, avoiding /tmp because secure ancestry intentionally rejects it.
func secureStateTestDir(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp(".", ".state-test-")
	if err != nil {
		t.Fatalf("MkdirTemp(secure state test directory) error = %v", err)
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		_ = os.RemoveAll(directory)
		t.Fatalf("Abs(secure state test directory) error = %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(absolute); err != nil {
			t.Errorf("RemoveAll(secure state test directory) error = %v", err)
		}
		pattern := filepath.Join(filepath.Dir(absolute), "."+filepath.Base(absolute)+"-*"+fileLockSuffix)
		anchors, err := filepath.Glob(pattern)
		if err != nil {
			t.Errorf("Glob(state lock anchors) error = %v", err)
			return
		}
		for _, anchor := range anchors {
			if err := os.Remove(anchor); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Errorf("Remove(state lock anchor %q) error = %v", anchor, err)
			}
		}
	})
	return absolute
}
