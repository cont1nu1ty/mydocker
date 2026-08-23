package state

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/rollback"
)

// testStateReceipt constructs valid provider evidence bound to the supplied
// operation so state tests can focus on cross-record ownership invariants.
func testStateReceipt(t *testing.T, op operation.Operation, kind ownership.Kind, localID string) ownership.Receipt {
	t.Helper()
	provider := ownership.ProviderLinux
	if kind == ownership.KindSandboxCgroup || kind == ownership.KindAttemptCgroup || kind == ownership.KindKeeperCgroup {
		provider = ownership.ProviderCgroupV2
	}
	owner, err := ownership.NewOwnerKey(op.ID, op.Target, domain.InitialGeneration)
	if err != nil {
		t.Fatalf("NewOwnerKey() setup error = %v", err)
	}
	evidence, err := ownership.EvidenceDigest(map[string]any{"kind": kind, "local_id": localID})
	if err != nil {
		t.Fatalf("EvidenceDigest() setup error = %v", err)
	}
	return ownership.Receipt{
		SchemaVersion:  ownership.SchemaVersion,
		Provider:       provider,
		Kind:           kind,
		LocalID:        localID,
		Owner:          owner,
		EvidenceSHA256: evidence,
		Attributes:     map[string]string{"evidence": localID},
	}
}

// testStateRollback constructs the bounded inverse associated with one
// actionable test receipt and fails immediately if the kind lacks that action.
func testStateRollback(t *testing.T, receipt ownership.Receipt) rollback.Record {
	t.Helper()
	var action ownership.Action
	switch receipt.Kind {
	case ownership.KindSandboxCgroup, ownership.KindAttemptCgroup, ownership.KindKeeperCgroup:
		action = ownership.ActionRemoveCgroup
	case ownership.KindKeeperProcess, ownership.KindInitProcess:
		action = ownership.ActionStopProcess
	case ownership.KindRootfsMount:
		action = ownership.ActionUnmountRoot
	case ownership.KindStartGate:
		action = ownership.ActionCloseGate
	case ownership.KindStreams:
		action = ownership.ActionCloseStreams
	default:
		t.Fatalf("receipt kind %q has no direct inverse", receipt.Kind)
	}
	descriptor, err := ownership.InverseDescriptor(receipt, action)
	if err != nil {
		t.Fatalf("InverseDescriptor() setup error = %v", err)
	}
	return rollback.Record{Descriptor: descriptor}
}

// testRunningOwnershipRecord prepares an active create operation with one
// crash-recoverable acquisition and its matching rollback descriptor.
func testRunningOwnershipRecord(t *testing.T, opID, targetID string, kind ownership.Kind) OperationRecord {
	t.Helper()
	op := testOperation(opID, targetID)
	op.State = operation.StateRunning
	op.Stage = operation.StagePrepareCgroup
	receipt := testStateReceipt(t, op, kind, string(kind)+"-local")
	record := NewOperationRecord(op)
	record.HostProfile = HostProfileLinuxM2
	record.Receipts = []ownership.Receipt{receipt}
	record.Rollback = []rollback.Record{testStateRollback(t, receipt)}
	return record
}

// testStateProfile builds the complete ordered M2 receipt and actionable rollback journals for one operation.
func testStateProfile(t *testing.T, op operation.Operation) ([]ownership.Receipt, []rollback.Record) {
	t.Helper()
	var kinds []ownership.Kind
	if op.Target.Kind == operation.TargetSandbox {
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
	var receipts []ownership.Receipt
	var records []rollback.Record
	for index, kind := range kinds {
		receipt := testStateReceipt(t, op, kind, fmt.Sprintf("%s-%d", kind, index))
		receipts = append(receipts, receipt)
		if kind == ownership.KindSandboxCgroup || kind == ownership.KindAttemptCgroup || kind == ownership.KindKeeperCgroup ||
			kind == ownership.KindKeeperProcess || kind == ownership.KindInitProcess ||
			kind == ownership.KindRootfsMount || kind == ownership.KindStartGate || kind == ownership.KindStreams {
			records = append(records, testStateRollback(t, receipt))
		}
	}
	return receipts, records
}

// testRunningProfileRecord prepares one active create operation with its complete M2 acquisition profile.
func testRunningProfileRecord(t *testing.T, op operation.Operation) OperationRecord {
	t.Helper()
	op.State = operation.StateRunning
	op.Stage = operation.StageAttachCgroup
	receipts, records := testStateProfile(t, op)
	record := NewOperationRecord(op)
	record.HostProfile = HostProfileLinuxM2
	record.Receipts = receipts
	record.Rollback = records
	return record
}

// putOwnershipOperation persists a side-effect-free Linux intent and then acknowledges each fixture receipt in its own transaction.
func putOwnershipOperation(t *testing.T, store Store, record OperationRecord) OperationRecord {
	t.Helper()
	desired := record.Clone()
	if desired.HostProfile == HostProfileLinuxM2 && desired.Operation.State.Active() {
		intent := desired.Clone()
		intent.Operation.Stage = operation.StagePersistIntent
		intent.Rollback = nil
		intent.Receipts = nil
		intent.Releases = nil
		var stored OperationRecord
		err := store.Update(context.Background(), func(tx Tx) error {
			var err error
			stored, err = tx.PutOperation(intent, 0)
			return err
		})
		if err != nil {
			t.Fatalf("PutOperation(ownership intent setup) error = %v", err)
		}
		rollbackIndex := 0
		for _, receipt := range desired.Receipts {
			var rollbackRecord *rollback.Record
			if testStateReceiptNeedsInverse(receipt.Kind) {
				if rollbackIndex >= len(desired.Rollback) {
					t.Fatalf("ownership setup receipt %s lacks rollback record", receipt.Kind)
				}
				value := desired.Rollback[rollbackIndex].Clone()
				rollbackRecord = &value
				rollbackIndex++
			}
			stored = appendTestOwnershipReceipt(t, store, stored, receipt, rollbackRecord)
		}
		if rollbackIndex != len(desired.Rollback) {
			t.Fatalf("ownership setup used %d rollback records, want %d", rollbackIndex, len(desired.Rollback))
		}
		if stored.Operation.Stage != desired.Operation.Stage {
			advanced := stored.Clone()
			advanced.Operation.Stage = desired.Operation.Stage
			err = store.Update(context.Background(), func(tx Tx) error {
				var err error
				stored, err = tx.PutOperation(advanced, stored.Revision)
				return err
			})
			if err != nil {
				t.Fatalf("PutOperation(ownership final stage setup) error = %v", err)
			}
		}
		return stored
	}
	var stored OperationRecord
	err := store.Update(context.Background(), func(tx Tx) error {
		var err error
		stored, err = tx.PutOperation(record, 0)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation(ownership setup) error = %v", err)
	}
	return stored
}

// appendTestOwnershipReceipt commits one provider acquisition and its optional inverse after a previously durable test intent.
func appendTestOwnershipReceipt(t *testing.T, store Store, current OperationRecord, receipt ownership.Receipt, rollbackRecord *rollback.Record) OperationRecord {
	t.Helper()
	next := current.Clone()
	next.Receipts = append(next.Receipts, receipt.Clone())
	if rollbackRecord != nil {
		next.Rollback = append(next.Rollback, rollbackRecord.Clone())
	}
	next.Operation.Stage = testStateReceiptCheckpointStage(receipt.Kind)
	var stored OperationRecord
	err := store.Update(context.Background(), func(tx Tx) error {
		var err error
		stored, err = tx.PutOperation(next, current.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation(append %s receipt) error = %v", receipt.Kind, err)
	}
	return stored
}

// testStateReceiptNeedsInverse identifies fixture roles whose acquisition owns a direct cleanup action.
func testStateReceiptNeedsInverse(kind ownership.Kind) bool {
	switch kind {
	case ownership.KindSandboxCgroup, ownership.KindKeeperCgroup, ownership.KindAttemptCgroup,
		ownership.KindKeeperProcess, ownership.KindInitProcess, ownership.KindRootfsMount,
		ownership.KindStartGate, ownership.KindStreams:
		return true
	default:
		return false
	}
}

// testStateReceiptCheckpointStage maps fixture roles to the first durable stage where their evidence may be acknowledged.
func testStateReceiptCheckpointStage(kind ownership.Kind) operation.Stage {
	switch kind {
	case ownership.KindSandboxCgroup, ownership.KindKeeperCgroup, ownership.KindAttemptCgroup:
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

// TestOwnershipRecordsDeepClone verifies operation journals and both resource
// inventories do not expose mutable receipt attributes across record copies.
func TestOwnershipRecordsDeepClone(t *testing.T) {
	sandboxOp := testOperation("op-clone-sandbox-owner", "sandbox-clone-owner")
	sandboxReceipt, err := testStateReceipt(t, sandboxOp, ownership.KindSandboxCgroup, "sandbox-cgroup-clone").Adopt()
	if err != nil {
		t.Fatalf("Receipt.Adopt() setup error = %v", err)
	}
	sandboxRecord := NewSandboxRecord(testReadySandbox(t, string(sandboxOp.Target.ID)))
	sandboxRecord.HostResources = []ownership.Receipt{sandboxReceipt}
	sandboxClone := sandboxRecord.Clone()
	sandboxClone.HostResources[0].Attributes["evidence"] = "mutated"
	if sandboxRecord.HostResources[0].Attributes["evidence"] != "sandbox-cgroup-clone" {
		t.Fatal("SandboxRecord.Clone() retained receipt attribute alias")
	}

	pairSandbox := testReadySandbox(t, "sandbox-clone-pair")
	pair := testContainerAttempt(t, pairSandbox, "container-clone-owner", "attempt-clone-owner")
	pairOp := testOperation("op-clone-pair-owner", string(pair.Container.ID))
	pairOp.Target.Kind = operation.TargetContainer
	pairReceipt, err := testStateReceipt(t, pairOp, ownership.KindAttemptCgroup, "attempt-cgroup-clone").Adopt()
	if err != nil {
		t.Fatalf("Receipt.Adopt() setup error = %v", err)
	}
	pairRecord := NewContainerAttemptRecord(pair)
	pairRecord.HostResources = []ownership.Receipt{pairReceipt}
	pairClone := pairRecord.Clone()
	pairClone.HostResources[0].Attributes["evidence"] = "mutated"
	if pairRecord.HostResources[0].Attributes["evidence"] != "attempt-cgroup-clone" {
		t.Fatal("ContainerAttemptRecord.Clone() retained receipt attribute alias")
	}

	opRecord := NewOperationRecord(sandboxOp)
	opRecord.Receipts = []ownership.Receipt{sandboxReceipt}
	opClone := opRecord.Clone()
	opClone.Receipts[0].Attributes["evidence"] = "mutated"
	if opRecord.Receipts[0].Attributes["evidence"] != "sandbox-cgroup-clone" {
		t.Fatal("OperationRecord.Clone() retained receipt attribute alias")
	}
}

// TestMemoryStoreRejectsInvalidReceiptBindings verifies future schemas,
// foreign operation owners, wrong resource targets, and duplicate slots fail closed.
func TestMemoryStoreRejectsInvalidReceiptBindings(t *testing.T) {
	store := NewMemoryStore()
	record := testRunningOwnershipRecord(t, "op-invalid-owner", "sandbox-invalid-owner", ownership.KindSandboxCgroup)

	foreignOwner, err := ownership.NewOwnerKey("op-foreign", record.Operation.Target, domain.InitialGeneration)
	if err != nil {
		t.Fatalf("NewOwnerKey(foreign) setup error = %v", err)
	}
	foreign := record.Clone()
	foreign.Receipts[0].Owner = foreignOwner
	foreign.Rollback[0] = testStateRollback(t, foreign.Receipts[0])
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(foreign, 0)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(foreign owner) error = %v, want ErrInvalidRecord", err)
	}

	future := record.Clone()
	future.Receipts[0].SchemaVersion++
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(future, 0)
		return err
	})
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("PutOperation(future receipt) error = %v, want ErrUnsupportedSchema", err)
	}

	duplicate := record.Clone()
	duplicate.Receipts = append(duplicate.Receipts, duplicate.Receipts[0].Clone())
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(duplicate, 0)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(duplicate provider/kind) error = %v, want ErrInvalidRecord", err)
	}

	adopted, err := record.Receipts[0].Adopt()
	if err != nil {
		t.Fatalf("Receipt.Adopt() setup error = %v", err)
	}
	wrongSandbox := NewSandboxRecord(testReadySandbox(t, "sandbox-wrong-owner"))
	wrongSandbox.HostResources = []ownership.Receipt{adopted}
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutSandbox(wrongSandbox, 0)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutSandbox(foreign receipt) error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStoreEnforcesReceiptAppendAndAdoption verifies acquisition order is
// append-only, evidence is immutable, and adoption moves only false to true.
func TestMemoryStoreEnforcesReceiptAppendAndAdoption(t *testing.T) {
	store := NewMemoryStore()
	sandbox := testReadySandbox(t, "sandbox-receipt-progress")
	sandboxRecord := putTestSandbox(t, store, sandbox)
	stored := putOwnershipOperation(t, store,
		testRunningOwnershipRecord(t, "op-receipt-progress", string(sandbox.ID), ownership.KindSandboxCgroup))

	appended := stored.Clone()
	keeperCgroup := testStateReceipt(t, appended.Operation, ownership.KindKeeperCgroup, "keeper-cgroup-progress")
	keeper := testStateReceipt(t, appended.Operation, ownership.KindKeeperProcess, "keeper-progress")
	passive := testStateReceipt(t, appended.Operation, ownership.KindUTSNamespace, "uts-progress")
	ipc := testStateReceipt(t, appended.Operation, ownership.KindIPCNamespace, "ipc-progress")
	network := testStateReceipt(t, appended.Operation, ownership.KindNetworkNamespace, "network-progress")
	appended.Receipts = append(appended.Receipts, keeperCgroup, keeper, passive, ipc, network)
	appended.Rollback = append(appended.Rollback, testStateRollback(t, keeperCgroup), testStateRollback(t, keeper))
	appended.Operation.Stage = operation.StagePrepareNamespaces
	err := store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(appended, stored.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(batch append) error = %v, want ErrInvalidRecord", err)
	}
	keeperCgroupRollback := testStateRollback(t, keeperCgroup)
	keeperRollback := testStateRollback(t, keeper)
	for _, item := range []struct {
		receipt ownership.Receipt
		inverse *rollback.Record
	}{
		{receipt: keeperCgroup, inverse: &keeperCgroupRollback},
		{receipt: keeper, inverse: &keeperRollback},
		{receipt: passive},
		{receipt: ipc},
		{receipt: network},
	} {
		stored = appendTestOwnershipReceipt(t, store, stored, item.receipt, item.inverse)
	}
	appended = stored.Clone()

	shrunk := appended.Clone()
	shrunk.Receipts = shrunk.Receipts[:1]
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(shrunk, appended.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(shrink receipts) error = %v, want ErrInvalidRecord", err)
	}

	rewritten := appended.Clone()
	rewritten.Receipts[3].LocalID = "uts-rewritten"
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(rewritten, appended.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(rewrite receipt) error = %v, want ErrInvalidRecord", err)
	}

	adopted := appended.Clone()
	for index, receipt := range adopted.Receipts {
		adopted.Receipts[index], err = receipt.Adopt()
		if err != nil {
			t.Fatalf("Receipt.Adopt(%d) setup error = %v", index, err)
		}
	}
	adopted.Operation.State = operation.StateSucceeded
	adopted.Operation.Stage = operation.StageComplete
	adopted.Operation.Result = operation.ResultSucceeded
	adopted.Operation.Response = []byte(`{"created":true}`)
	sandboxRecord.HostResources = cloneReceipts(adopted.Receipts)
	err = store.Update(context.Background(), func(tx Tx) error {
		var err error
		adopted, err = tx.PutOperation(adopted, appended.Revision)
		if err != nil {
			return err
		}
		sandboxRecord, err = tx.PutSandbox(sandboxRecord, sandboxRecord.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("Update(adopt receipts) error = %v", err)
	}

	regressed := adopted.Clone()
	regressed.Receipts[0].Adopted = false
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(regressed, adopted.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(regress adoption) error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStoreTransfersCreateInventoryAtomically verifies successful create
// cannot commit before adoption or with foreign inventory and leaves zero partial state.
func TestMemoryStoreTransfersCreateInventoryAtomically(t *testing.T) {
	store := NewMemoryStore()
	sandbox := testReadySandbox(t, "sandbox-transfer")
	sandboxRecord := putTestSandbox(t, store, sandbox)
	op := testOperation("op-transfer", string(sandbox.ID))
	stored := putOwnershipOperation(t, store, testRunningProfileRecord(t, op))

	terminal := stored.Clone()
	terminal.Operation.State = operation.StateSucceeded
	terminal.Operation.Stage = operation.StageComplete
	terminal.Operation.Result = operation.ResultSucceeded
	terminal.Operation.Response = []byte(`{"created":true}`)
	err := store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(terminal, stored.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(unadopted success) error = %v, want ErrInvalidRecord", err)
	}

	for index, receipt := range terminal.Receipts {
		terminal.Receipts[index], err = receipt.Adopt()
		if err != nil {
			t.Fatalf("Receipt.Adopt(%d) setup error = %v", index, err)
		}
	}
	sandboxRecord.HostResources = cloneReceipts(terminal.Receipts)
	sandboxRecord.HostResources[0].LocalID = "different-live-object"
	err = store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutOperation(terminal, stored.Revision); err != nil {
			return err
		}
		_, err := tx.PutSandbox(sandboxRecord, sandboxRecord.Revision)
		return err
	})
	if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("Update(foreign inventory) error = %v, want ErrInvariantViolation", err)
	}

	err = store.View(context.Background(), func(reader Reader) error {
		opRecord, err := reader.GetOperation(stored.Operation.ID)
		if err != nil {
			return err
		}
		resourceRecord, err := reader.GetSandbox(sandbox.ID)
		if err != nil {
			return err
		}
		if opRecord.Revision != stored.Revision || opRecord.Receipts[0].Adopted ||
			resourceRecord.Revision != sandboxRecord.Revision || len(resourceRecord.HostResources) != 0 {
			t.Fatalf("failed transaction leaked operation or inventory state: op=%#v sandbox=%#v", opRecord, resourceRecord)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View(after failed transfer) error = %v", err)
	}

	sandboxRecord.HostResources = cloneReceipts(terminal.Receipts)
	err = store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutOperation(terminal, stored.Revision); err != nil {
			return err
		}
		_, err := tx.PutSandbox(sandboxRecord, sandboxRecord.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("Update(exact ownership transfer) error = %v", err)
	}
}

// TestMemoryStoreCountsPinnedCreateOwnerAgainstIdentityLimit verifies keeping a
// live HostResources owner for OOM and provenance replay never bypasses the
// store-wide hard cap on complete operations plus tombstones.
func TestMemoryStoreCountsPinnedCreateOwnerAgainstIdentityLimit(t *testing.T) {
	policy := RetentionPolicy{TerminalOperationLimit: 1, OperationIdentityLimit: 2, EventLimit: 8}
	store, err := NewMemoryStoreWithRetention(policy)
	if err != nil {
		t.Fatal(err)
	}
	sandbox := testReadySandbox(t, "sandbox-pinned-capacity")
	sandboxRecord := putTestSandbox(t, store, sandbox)
	create := testOperation("op-pinned-capacity-create", string(sandbox.ID))
	createRecord := putOwnershipOperation(t, store, testRunningProfileRecord(t, create))
	for index, receipt := range createRecord.Receipts {
		createRecord.Receipts[index], err = receipt.Adopt()
		if err != nil {
			t.Fatalf("Receipt.Adopt(%d) setup error = %v", index, err)
		}
	}
	createRecord.Operation.State = operation.StateSucceeded
	createRecord.Operation.Stage = operation.StageComplete
	createRecord.Operation.Result = operation.ResultSucceeded
	createRecord.Operation.Response = []byte(`{"created":true}`)
	sandboxRecord.HostResources = cloneReceipts(createRecord.Receipts)
	err = store.Update(context.Background(), func(tx Tx) error {
		if _, putErr := tx.PutOperation(createRecord, createRecord.Revision); putErr != nil {
			return putErr
		}
		_, putErr := tx.PutSandbox(sandboxRecord, sandboxRecord.Revision)
		return putErr
	})
	if err != nil {
		t.Fatalf("Update(pin create owner) error = %v", err)
	}
	putTerminalOperationWithEvent(t, store, "op-pinned-capacity-window", "sandbox-pinned-capacity-window")

	rejected := testOperation("op-pinned-capacity-rejected", "sandbox-pinned-capacity-rejected")
	err = store.Update(context.Background(), func(tx Tx) error {
		_, putErr := tx.PutOperation(NewOperationRecord(rejected), 0)
		return putErr
	})
	if !errors.Is(err, ErrRetentionCapacity) {
		t.Fatalf("PutOperation(over pinned identity capacity) error = %v, want ErrRetentionCapacity", err)
	}
	err = store.View(context.Background(), func(reader Reader) error {
		_, getErr := reader.GetOperation(create.ID)
		return getErr
	})
	if err != nil {
		t.Fatalf("GetOperation(pinned create owner) error = %v", err)
	}
}

// TestMemoryStoreTransfersContainerAttemptInventory verifies a Container create
// adopts per-Attempt host resources into the atomic pair record, not its Sandbox.
func TestMemoryStoreTransfersContainerAttemptInventory(t *testing.T) {
	store := NewMemoryStore()
	sandbox := testReadySandbox(t, "sandbox-attempt-transfer")
	pair := testContainerAttempt(t, sandbox, "container-attempt-transfer", "attempt-transfer")
	if err := sandbox.SetCurrentPair(pair.Container.ID, pair.Attempt.ID); err != nil {
		t.Fatalf("SetCurrentPair() setup error = %v", err)
	}
	op := testOperation("op-attempt-transfer", string(pair.Container.ID))
	op.Target.Kind = operation.TargetContainer
	opRecord := testRunningProfileRecord(t, op)
	pairRecord := NewContainerAttemptRecord(pair)

	err := store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutSandbox(NewSandboxRecord(sandbox), 0); err != nil {
			return err
		}
		var err error
		pairRecord, err = tx.PutContainerAttempt(pairRecord, 0)
		return err
	})
	if err != nil {
		t.Fatalf("Update(Container ownership setup) error = %v", err)
	}
	opRecord = putOwnershipOperation(t, store, opRecord)

	for index, receipt := range opRecord.Receipts {
		opRecord.Receipts[index], err = receipt.Adopt()
		if err != nil {
			t.Fatalf("Receipt.Adopt(%d) setup error = %v", index, err)
		}
	}
	opRecord.Operation.State = operation.StateSucceeded
	opRecord.Operation.Stage = operation.StageComplete
	opRecord.Operation.Result = operation.ResultSucceeded
	opRecord.Operation.Response = []byte(`{"created":true}`)
	pairRecord.HostResources = cloneReceipts(opRecord.Receipts)
	err = store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutOperation(opRecord, opRecord.Revision); err != nil {
			return err
		}
		_, err := tx.PutContainerAttempt(pairRecord, pairRecord.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("Update(Container exact ownership transfer) error = %v", err)
	}
}

// TestMemoryStoreRequiresReceiptRollbackBeforeFailure verifies a failed create
// stays resumable until every unadopted acquisition inverse is durably complete.
func TestMemoryStoreRequiresReceiptRollbackBeforeFailure(t *testing.T) {
	store := NewMemoryStore()
	stored := putOwnershipOperation(t, store,
		testRunningOwnershipRecord(t, "op-receipt-failure", "sandbox-receipt-failure", ownership.KindSandboxCgroup))
	failed := stored.Clone()
	failed.Operation.State = operation.StateFailed
	failed.Operation.Stage = operation.StageComplete
	failed.Operation.Result = operation.ResultFailed
	failed.Operation.Reason = operation.ReasonCleanup
	failed.Operation.Response = []byte(`{"cleanup":"pending"}`)
	err := store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(failed, stored.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(failed with pending receipt) error = %v, want ErrInvalidRecord", err)
	}

	failed.Rollback[0].Started = true
	failed.Rollback[0].Succeeded = true
	failed.Operation.Response = []byte(`{"cleanup":"complete"}`)
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(failed, stored.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation(failed after receipt rollback) error = %v", err)
	}
}

// TestMemoryStoreRequiresExactDeleteReleaseProof verifies stop, kill, and unproven delete operations cannot discard recovery inventory.
func TestMemoryStoreRequiresExactDeleteReleaseProof(t *testing.T) {
	store := NewMemoryStore()
	sandbox := testReadySandbox(t, "sandbox-release-guard")
	sandboxRecord := putTestSandbox(t, store, sandbox)
	create := testRunningProfileRecord(t, testOperation("op-release-create", string(sandbox.ID)))
	create = putOwnershipOperation(t, store, create)
	for index, receipt := range create.Receipts {
		adopted, err := receipt.Adopt()
		if err != nil {
			t.Fatalf("Receipt.Adopt(%d) setup error = %v", index, err)
		}
		create.Receipts[index] = adopted
	}
	create.Operation.State = operation.StateSucceeded
	create.Operation.Stage = operation.StageComplete
	create.Operation.Result = operation.ResultSucceeded
	create.Operation.Response = []byte(`{"created":true}`)
	sandboxRecord.HostResources = cloneReceipts(create.Receipts)
	err := store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutOperation(create, create.Revision); err != nil {
			return err
		}
		var err error
		sandboxRecord, err = tx.PutSandbox(sandboxRecord, sandboxRecord.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("Update(create ownership) error = %v", err)
	}

	stop := testOperation("op-release-stop", string(sandbox.ID))
	stop.Type = operation.TypeStop
	stop.State = operation.StateSucceeded
	stop.Stage = operation.StageComplete
	stop.Result = operation.ResultSucceeded
	stop.Response = []byte(`{"stopped":true}`)
	cleared := sandboxRecord.Clone()
	cleared.HostResources = nil
	err = store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutOperation(NewOperationRecord(stop), 0); err != nil {
			return err
		}
		_, err := tx.PutSandbox(cleared, sandboxRecord.Revision)
		return err
	})
	if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("Update(stop clears inventory) error = %v, want ErrInvariantViolation", err)
	}

	deleteOperation := testOperation("op-release-delete", string(sandbox.ID))
	deleteOperation.Type = operation.TypeDelete
	deleteOperation.State = operation.StateSucceeded
	deleteOperation.Stage = operation.StageComplete
	deleteOperation.Result = operation.ResultSucceeded
	deleteOperation.Response = []byte(`{"removed":true}`)
	deleteRecord := NewOperationRecord(deleteOperation)
	deleteRecord.HostProfile = HostProfileLinuxM2
	err = store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutOperation(deleteRecord, 0); err != nil {
			return err
		}
		_, err := tx.PutSandbox(cleared, sandboxRecord.Revision)
		return err
	})
	if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("Update(delete without releases) error = %v, want ErrInvariantViolation", err)
	}

	for index, receipt := range create.Receipts {
		release, releaseErr := ownership.NewRelease(deleteOperation.ID, receipt, map[string]any{"presence": "absent", "index": index})
		if releaseErr != nil {
			t.Fatalf("NewRelease(%d) setup error = %v", index, releaseErr)
		}
		deleteRecord.Releases = append(deleteRecord.Releases, release)
	}
	err = store.Update(context.Background(), func(tx Tx) error {
		if _, err := tx.PutOperation(deleteRecord, 0); err != nil {
			return err
		}
		_, err := tx.PutSandbox(cleared, sandboxRecord.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("Update(delete with exact releases) error = %v", err)
	}
}

// TestMemoryStoreRequiresExplicitHostProfile verifies abstract operations cannot smuggle receipts and Linux create cannot succeed without them.
func TestMemoryStoreRequiresExplicitHostProfile(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-profile-zero", "sandbox-profile-zero")
	linux, err := NewOperationRecordForProfile(op, HostProfileLinuxM2)
	if err != nil {
		t.Fatalf("NewOperationRecordForProfile() error = %v", err)
	}
	linux.Operation.State = operation.StateSucceeded
	linux.Operation.Stage = operation.StageComplete
	linux.Operation.Result = operation.ResultSucceeded
	linux.Operation.Response = []byte(`{"created":true}`)
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(linux, 0)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(Linux zero-receipt success) error = %v, want ErrInvalidRecord", err)
	}

	abstract := testRunningOwnershipRecord(t, "op-profile-abstract", "sandbox-profile-abstract", ownership.KindSandboxCgroup)
	abstract.HostProfile = HostProfileAbstractM1
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(abstract, 0)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(abstract with receipt) error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStoreAcceptsLinuxCreateNoopWithoutReceipts verifies an already
// existing Sandbox or Container can complete a new create identity as a true
// side-effect-free no-op without fabricating the original acquisition journal.
func TestMemoryStoreAcceptsLinuxCreateNoopWithoutReceipts(t *testing.T) {
	for _, targetKind := range []operation.TargetKind{operation.TargetSandbox, operation.TargetContainer} {
		t.Run(string(targetKind), func(t *testing.T) {
			store := NewMemoryStore()
			op := testOperation("op-linux-noop-"+string(targetKind), "resource-linux-noop-"+string(targetKind))
			op.Target.Kind = targetKind
			op.State = operation.StateSucceeded
			op.Stage = operation.StageComplete
			op.Result = operation.ResultNoop
			op.Response = []byte(`{"created":false}`)
			record, err := NewOperationRecordForProfile(op, HostProfileLinuxM2)
			if err != nil {
				t.Fatalf("NewOperationRecordForProfile() error = %v", err)
			}
			err = store.Update(context.Background(), func(tx Tx) error {
				_, putErr := tx.PutOperation(record, 0)
				return putErr
			})
			if err != nil {
				t.Fatalf("PutOperation(Linux %s create no-op) error = %v", targetKind, err)
			}
		})
	}
}

// TestMemoryStoreRejectsOutOfOrderReceiptPrefix verifies active create cannot checkpoint a dependency-skipping journal.
func TestMemoryStoreRejectsOutOfOrderReceiptPrefix(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-profile-order", "container-profile-order")
	op.Target.Kind = operation.TargetContainer
	record := testRunningProfileRecord(t, op)
	record.Receipts[1], record.Receipts[6] = record.Receipts[6], record.Receipts[1]
	record.Rollback = []rollback.Record{
		testStateRollback(t, record.Receipts[0]),
		testStateRollback(t, record.Receipts[1]),
		testStateRollback(t, record.Receipts[2]),
		testStateRollback(t, record.Receipts[3]),
	}
	err := store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(record, 0)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(out-of-order profile) error = %v, want ErrInvalidRecord", err)
	}
}

// TestMemoryStoreRequiresLinuxStartAcknowledgementOrder verifies direct Store callers cannot jump from intent to gate release or observation.
func TestMemoryStoreRequiresLinuxStartAcknowledgementOrder(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-start-stage-order", "container-start-stage-order")
	op.Type = operation.TypeStart
	op.Target.Kind = operation.TargetContainer
	op.State = operation.StateRunning
	op.Stage = operation.StagePersistIntent
	record, err := NewOperationRecordForProfile(op, HostProfileLinuxM2)
	if err != nil {
		t.Fatalf("NewOperationRecordForProfile() error = %v", err)
	}
	stored := putOwnershipOperation(t, store, record)

	skippedAttachment := stored.Clone()
	skippedAttachment.Operation.Stage = operation.StageReleaseStartGate
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(skippedAttachment, stored.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(skip attachment) error = %v, want ErrInvalidRecord", err)
	}

	attached := stored.Clone()
	attached.Operation.Stage = operation.StageAttachCgroup
	err = store.Update(context.Background(), func(tx Tx) error {
		var err error
		attached, err = tx.PutOperation(attached, stored.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation(attach) error = %v", err)
	}
	skippedRelease := attached.Clone()
	skippedRelease.Operation.Stage = operation.StageObserveProcess
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(skippedRelease, attached.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(skip release) error = %v, want ErrInvalidRecord", err)
	}

	released := attached.Clone()
	released.Operation.Stage = operation.StageReleaseStartGate
	err = store.Update(context.Background(), func(tx Tx) error {
		var err error
		released, err = tx.PutOperation(released, attached.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation(release) error = %v", err)
	}
	observed := released.Clone()
	observed.Operation.Stage = operation.StageObserveProcess
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(observed, released.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation(observe) error = %v", err)
	}
}

// TestMemoryStoreKeepsKillEscalationDeadlineImmutable verifies a signaled Kill cannot omit, remove, or restart its absolute grace deadline.
func TestMemoryStoreKeepsKillEscalationDeadlineImmutable(t *testing.T) {
	store := NewMemoryStore()
	op := testOperation("op-kill-deadline", "container-kill-deadline")
	op.Type = operation.TypeKill
	op.Target.Kind = operation.TargetContainer
	op.State = operation.StateRunning
	op.Stage = operation.StagePersistIntent
	record, err := NewOperationRecordForProfile(op, HostProfileLinuxM2)
	if err != nil {
		t.Fatalf("NewOperationRecordForProfile() error = %v", err)
	}
	stored := putOwnershipOperation(t, store, record)

	missing := stored.Clone()
	missing.Operation.Stage = operation.StageSignalProcess
	err = store.Update(context.Background(), func(tx Tx) error {
		_, err := tx.PutOperation(missing, stored.Revision)
		return err
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("PutOperation(signal without deadline) error = %v, want ErrInvalidRecord", err)
	}

	deadline := time.Date(2026, 8, 21, 8, 0, 30, 0, time.UTC)
	signaled := stored.Clone()
	signaled.Operation.Stage = operation.StageSignalProcess
	signaled.KillEscalationDeadline = &deadline
	err = store.Update(context.Background(), func(tx Tx) error {
		var err error
		signaled, err = tx.PutOperation(signaled, stored.Revision)
		return err
	})
	if err != nil {
		t.Fatalf("PutOperation(signal deadline) error = %v", err)
	}

	for name, mutate := range map[string]func(*OperationRecord){
		"removed": func(candidate *OperationRecord) { candidate.KillEscalationDeadline = nil },
		"drifted": func(candidate *OperationRecord) {
			changed := deadline.Add(time.Second)
			candidate.KillEscalationDeadline = &changed
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := signaled.Clone()
			mutate(&candidate)
			err := store.Update(context.Background(), func(tx Tx) error {
				_, putErr := tx.PutOperation(candidate, signaled.Revision)
				return putErr
			})
			if !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("PutOperation(%s deadline) error = %v, want ErrInvalidRecord", name, err)
			}
		})
	}
}

// TestMemoryStoreDeletesOnlyAnExplicitAbsentCreateCandidate verifies create
// rollback can transition then delete atomically while direct Creating/Ready deletion stays forbidden.
func TestMemoryStoreDeletesOnlyAnExplicitAbsentCreateCandidate(t *testing.T) {
	store := NewMemoryStore()
	creating, err := domain.NewSandbox("sandbox-create-rollback", domain.SandboxSpec{})
	if err != nil {
		t.Fatalf("NewSandbox() setup error = %v", err)
	}
	created := putTestSandbox(t, store, creating)
	err = store.Update(context.Background(), func(tx Tx) error {
		return tx.DeleteSandbox(creating.ID, created.Revision)
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("DeleteSandbox(Creating) error = %v, want ErrInvalidRecord", err)
	}

	absent := created.Clone()
	if err := absent.Sandbox.Transition(domain.SandboxAbsent); err != nil {
		t.Fatalf("Transition(Absent) setup error = %v", err)
	}
	err = store.Update(context.Background(), func(tx Tx) error {
		checkpoint, err := tx.PutSandbox(absent, created.Revision)
		if err != nil {
			return err
		}
		return tx.DeleteSandbox(absent.Sandbox.ID, checkpoint.Revision)
	})
	if err != nil {
		t.Fatalf("Update(transition Absent then delete) error = %v", err)
	}
	err = store.View(context.Background(), func(reader Reader) error {
		_, err := reader.GetSandbox(creating.ID)
		return err
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetSandbox(after create rollback) error = %v, want ErrNotFound", err)
	}

	ready := putTestSandbox(t, store, testReadySandbox(t, "sandbox-ready-delete-reject"))
	err = store.Update(context.Background(), func(tx Tx) error {
		return tx.DeleteSandbox(ready.Sandbox.ID, ready.Revision)
	})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("DeleteSandbox(Ready) error = %v, want ErrInvalidRecord", err)
	}
}

// TestSameReceiptSetIgnoresOrdering verifies exact transfer comparison remains
// deterministic when providers persist unique inventory slots in another order.
func TestSameReceiptSetIgnoresOrdering(t *testing.T) {
	op := testOperation("op-receipt-set", "sandbox-receipt-set")
	first, err := testStateReceipt(t, op, ownership.KindSandboxCgroup, "cgroup-set").Adopt()
	if err != nil {
		t.Fatalf("Receipt.Adopt(first) setup error = %v", err)
	}
	second, err := testStateReceipt(t, op, ownership.KindUTSNamespace, "uts-set").Adopt()
	if err != nil {
		t.Fatalf("Receipt.Adopt(second) setup error = %v", err)
	}
	if !sameReceiptSet([]ownership.Receipt{first, second}, []ownership.Receipt{second, first}) {
		t.Fatal("sameReceiptSet() treated inventory order as ownership identity")
	}
	changed := second.Clone()
	changed.Attributes["evidence"] = "rewritten"
	if sameReceiptSet([]ownership.Receipt{first, second}, []ownership.Receipt{first, changed}) {
		t.Fatal("sameReceiptSet() ignored changed provider evidence")
	}
	if reflect.DeepEqual(second, changed) {
		t.Fatal("receipt clone mutation did not change the test value")
	}
}
