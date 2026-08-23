package rollback

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

// testDescriptor constructs a valid persistable identity for a rollback step.
func testDescriptor(name string) Descriptor {
	return Descriptor{
		SchemaVersion: SchemaVersion,
		Name:          name,
		Action:        "release",
		Target:        "resource/" + name,
		Metadata:      []byte(`{"owner":"attempt-1"}`),
	}
}

// pushTestStep registers a test inverse and fails immediately on invalid setup.
func pushTestStep(t *testing.T, stack *Stack, name string, inverse Inverse) {
	t.Helper()
	if err := stack.Push(testDescriptor(name), inverse); err != nil {
		t.Fatalf("Push(%q) error = %v", name, err)
	}
}

// TestStackRunUsesLIFOOrder verifies resources are released in the reverse of
// their acquisition order and all successful steps become no-ops.
func TestStackRunUsesLIFOOrder(t *testing.T) {
	stack := New()
	var order []string
	for _, name := range []string{"network", "cgroup", "rootfs"} {
		name := name
		pushTestStep(t, stack, name, func(context.Context) error {
			order = append(order, name)
			return nil
		})
	}

	report := stack.Run(context.Background(), nil)
	if err := report.Err(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got, want := order, []string{"rootfs", "cgroup", "network"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rollback order = %v, want %v", got, want)
	}
	if got := stack.Pending(); got != 0 {
		t.Fatalf("Pending() = %d, want 0", got)
	}

	stack.Run(context.Background(), nil)
	if got, want := order, []string{"rootfs", "cgroup", "network"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("second Run() repeated successful steps: got %v want %v", got, want)
	}
}

// TestStackRunPreservesPrimaryAndAllRollbackFailures verifies rollback continues
// after cleanup errors and exposes both the primary error and every failed step.
func TestStackRunPreservesPrimaryAndAllRollbackFailures(t *testing.T) {
	primary := errors.New("create failed")
	rootfsFailure := errors.New("rootfs busy")
	networkFailure := errors.New("network cleanup failed")
	stack := New()
	var order []string
	pushTestStep(t, stack, "network", func(context.Context) error {
		order = append(order, "network")
		return networkFailure
	})
	pushTestStep(t, stack, "cgroup", func(context.Context) error {
		order = append(order, "cgroup")
		return nil
	})
	pushTestStep(t, stack, "rootfs", func(context.Context) error {
		order = append(order, "rootfs")
		return rootfsFailure
	})

	report := stack.Run(context.Background(), primary)
	if got, want := order, []string{"rootfs", "cgroup", "network"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rollback order = %v, want %v", got, want)
	}
	if got := len(report.Failures); got != 2 {
		t.Fatalf("len(Failures) = %d, want 2", got)
	}
	aggregate := report.Err()
	for name, target := range map[string]error{
		"primary": primary,
		"rootfs":  rootfsFailure,
		"network": networkFailure,
	} {
		if !errors.Is(aggregate, target) {
			t.Errorf("aggregate does not contain %s error: %v", name, aggregate)
		}
	}
	if got := stack.Pending(); got != 2 {
		t.Fatalf("Pending() = %d, want 2 failed steps", got)
	}
}

// TestStackRunRetriesOnlyFailedSteps verifies a later reconciliation retries
// failed inverses while steps that already succeeded remain no-ops.
func TestStackRunRetriesOnlyFailedSteps(t *testing.T) {
	stack := New()
	var order []string
	attempts := map[string]int{}
	pushTestStep(t, stack, "first", func(context.Context) error {
		attempts["first"]++
		order = append(order, "first")
		return nil
	})
	pushTestStep(t, stack, "flaky", func(context.Context) error {
		attempts["flaky"]++
		order = append(order, "flaky")
		if attempts["flaky"] == 1 {
			return errors.New("retry me")
		}
		return nil
	})
	pushTestStep(t, stack, "last", func(context.Context) error {
		attempts["last"]++
		order = append(order, "last")
		return nil
	})

	firstReport := stack.Run(context.Background(), errors.New("primary"))
	if len(firstReport.Failures) != 1 {
		t.Fatalf("first Run() failures = %d, want 1", len(firstReport.Failures))
	}
	secondReport := stack.Run(context.Background(), errors.New("primary"))
	if len(secondReport.Failures) != 0 {
		t.Fatalf("second Run() failures = %v, want none", secondReport.Failures)
	}
	if got, want := order, []string{"last", "flaky", "first", "flaky"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("retry order = %v, want %v", got, want)
	}
	if got, want := attempts, map[string]int{"first": 1, "flaky": 2, "last": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("attempt counts = %v, want %v", got, want)
	}
	if got := stack.Pending(); got != 0 {
		t.Fatalf("Pending() = %d, want 0", got)
	}
}

// TestStackSnapshotAndRestoreUseDescriptors verifies persistence contains only
// data, restores handlers through a registry, and preserves successful no-ops.
func TestStackSnapshotAndRestoreUseDescriptors(t *testing.T) {
	stack := New()
	pushTestStep(t, stack, "done", func(context.Context) error { return nil })
	stack.Run(context.Background(), nil)
	records := stack.Snapshot()
	if len(records) != 1 || !records[0].Succeeded {
		t.Fatalf("Snapshot() = %#v, want one succeeded record", records)
	}
	encoded, err := json.Marshal(records)
	if err != nil {
		t.Fatalf("json.Marshal(records) error = %v", err)
	}
	if !json.Valid(encoded) {
		t.Fatalf("snapshot is invalid JSON: %q", encoded)
	}

	called := 0
	restored, err := Restore(records, func(descriptor Descriptor) (Inverse, error) {
		return func(context.Context) error {
			called++
			return nil
		}, nil
	})
	if err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	restored.Run(context.Background(), nil)
	if called != 0 {
		t.Fatalf("restored successful inverse called %d times, want 0", called)
	}

	records[0].Descriptor.Metadata[2] = 'X'
	if reflect.DeepEqual(records, restored.Snapshot()) {
		t.Fatal("Restore() retained caller-owned descriptor metadata")
	}
}

// TestStackPushValidation verifies persistence identities, handler presence,
// duplicate names, and registration-after-run rules are explicit errors.
func TestStackPushValidation(t *testing.T) {
	stack := New()
	if err := stack.Push(Descriptor{}, func(context.Context) error { return nil }); err == nil {
		t.Fatal("Push() accepted invalid descriptor")
	}
	if err := stack.Push(testDescriptor("nil"), nil); err == nil {
		t.Fatal("Push() accepted nil inverse")
	}
	pushTestStep(t, stack, "one", func(context.Context) error { return nil })
	if err := stack.Push(testDescriptor("one"), func(context.Context) error { return nil }); err == nil {
		t.Fatal("Push() accepted duplicate name")
	}
	stack.Run(context.Background(), nil)
	if err := stack.Push(testDescriptor("late"), func(context.Context) error { return nil }); err == nil {
		t.Fatal("Push() accepted a step after rollback started")
	}
}

// TestStackBeginPersistsStartedBeforeExecution verifies a coordinator can seal
// and snapshot rollback intent without executing any inverse first.
func TestStackBeginPersistsStartedBeforeExecution(t *testing.T) {
	stack := New()
	called := 0
	pushTestStep(t, stack, "resource", func(context.Context) error {
		called++
		return nil
	})

	if err := stack.Begin(); err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	records := stack.Snapshot()
	if len(records) != 1 || !records[0].Started || records[0].Succeeded {
		t.Fatalf("Snapshot() = %#v, want one started and pending record", records)
	}
	if called != 0 {
		t.Fatalf("Begin() executed inverse %d times, want 0", called)
	}
}

// TestStackBeginIsIdempotent verifies retrying the seal transition preserves
// the same durable progress and continues to prevent later acquisitions.
func TestStackBeginIsIdempotent(t *testing.T) {
	stack := New()
	pushTestStep(t, stack, "resource", func(context.Context) error { return nil })
	if err := stack.Begin(); err != nil {
		t.Fatalf("first Begin() error = %v", err)
	}
	first := stack.Snapshot()
	if err := stack.Begin(); err != nil {
		t.Fatalf("second Begin() error = %v", err)
	}
	if second := stack.Snapshot(); !reflect.DeepEqual(second, first) {
		t.Fatalf("second Begin() changed snapshot: got %#v want %#v", second, first)
	}
	if err := stack.Push(testDescriptor("late"), func(context.Context) error { return nil }); err == nil {
		t.Fatal("Push() accepted acquisition after repeated Begin()")
	}
}

// TestRestorePreservesExplicitBeginSeal verifies a started snapshot remains
// sealed after process recovery so resumed setup cannot append inverses.
func TestRestorePreservesExplicitBeginSeal(t *testing.T) {
	stack := New()
	pushTestStep(t, stack, "existing", func(context.Context) error { return nil })
	if err := stack.Begin(); err != nil {
		t.Fatalf("Begin() error = %v", err)
	}

	restored, err := Restore(stack.Snapshot(), func(Descriptor) (Inverse, error) {
		return func(context.Context) error { return nil }, nil
	})
	if err != nil {
		t.Fatalf("Restore(started snapshot) error = %v", err)
	}
	if err := restored.Push(testDescriptor("late"), func(context.Context) error { return nil }); err == nil {
		t.Fatal("Push() accepted acquisition after restoring a Begin() snapshot")
	}
}

// TestRestoreDistinguishesResumeFromRollback verifies persisted Started controls whether new acquisitions may register inverses.
func TestRestoreDistinguishesResumeFromRollback(t *testing.T) {
	order := make([]string, 0, 2)
	pending := []Record{{Descriptor: testDescriptor("existing")}}
	restored, err := Restore(pending, func(descriptor Descriptor) (Inverse, error) {
		return func(context.Context) error {
			order = append(order, descriptor.Name)
			return nil
		}, nil
	})
	if err != nil {
		t.Fatalf("Restore(resumable setup) error = %v", err)
	}
	if err := restored.Push(testDescriptor("new"), func(context.Context) error {
		order = append(order, "new")
		return nil
	}); err != nil {
		t.Fatalf("Push(after resumable restore) error = %v", err)
	}
	restored.Run(context.Background(), nil)
	if want := []string{"new", "existing"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("restored rollback order = %v, want %v", order, want)
	}

	started := []Record{{Descriptor: testDescriptor("started"), Started: true}}
	sealed, err := Restore(started, func(Descriptor) (Inverse, error) {
		return func(context.Context) error { return nil }, nil
	})
	if err != nil {
		t.Fatalf("Restore(started rollback) error = %v", err)
	}
	if err := sealed.Push(testDescriptor("late"), func(context.Context) error { return nil }); err == nil {
		t.Fatal("Push() accepted acquisition after restored rollback had started")
	}
}

// TestRestoreSkipsSucceededResolver verifies completed cleanup needs no runtime provider after restart.
func TestRestoreSkipsSucceededResolver(t *testing.T) {
	records := []Record{{Descriptor: testDescriptor("done"), Started: true, Succeeded: true}}
	restored, err := Restore(records, nil)
	if err != nil {
		t.Fatalf("Restore(succeeded without resolver) error = %v", err)
	}
	if restored.Pending() != 0 {
		t.Fatalf("Pending() = %d, want 0", restored.Pending())
	}
	invalid := records[0]
	invalid.Started = false
	if err := invalid.Validate(); err == nil {
		t.Fatal("Record.Validate() accepted success before rollback started")
	}
}
