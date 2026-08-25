package observability

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"mydocker/internal/operation"
)

// TestJSONLoggerWritesAtomicStructuredLines verifies concurrent diagnostic facts remain complete independent JSON records.
func TestJSONLoggerWritesAtomicStructuredLines(t *testing.T) {
	var output lockedBuffer
	fixed := time.Date(2026, 8, 21, 1, 2, 3, 0, time.UTC)
	logger, err := NewJSONLogger(&output, func() time.Time { return fixed })
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}
	const writers = 16
	var wait sync.WaitGroup
	for index := 0; index < writers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			record := LogRecord{
				Level: LevelInfo, Message: "operation accepted", OperationID: "op-1",
				Resources: []operation.Target{{Kind: operation.TargetSandbox, ID: "sandbox-1"}},
				Stage:     operation.StagePersistIntent, Result: operation.ResultPending, Reason: operation.ReasonNone,
			}
			if writeErr := logger.Write(record); writeErr != nil {
				t.Errorf("write record: %v", writeErr)
			}
		}()
	}
	wait.Wait()
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != writers {
		t.Fatalf("got %d log lines, want %d", len(lines), writers)
	}
	for _, line := range lines {
		var record LogRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode complete line: %v", err)
		}
		if record.SchemaVersion != LogSchemaVersion || !record.Time.Equal(fixed) {
			t.Fatalf("unexpected record metadata: %#v", record)
		}
	}
}

// TestLogRecordRejectsLineInjection verifies diagnostic strings cannot forge additional JSONL records.
func TestLogRecordRejectsLineInjection(t *testing.T) {
	record := LogRecord{SchemaVersion: LogSchemaVersion, Time: time.Now(), Level: LevelError, Message: "bad\nrecord"}
	if err := record.Validate(); err == nil {
		t.Fatal("expected newline-bearing message to be rejected")
	}
}

// TestRegistryUsesOnlyBoundedLabels verifies counters and durations expose deterministic low-cardinality snapshots.
func TestRegistryUsesOnlyBoundedLabels(t *testing.T) {
	registry := NewRegistry()
	labels := MetricLabels{
		Operation: operation.TypeCreate,
		Stage:     operation.StagePersistResult,
		Result:    operation.ResultSucceeded,
		Reason:    operation.ReasonNone,
	}
	if err := registry.AddCounter(MetricLifecycleOperations, labels, 2); err != nil {
		t.Fatalf("add counter: %v", err)
	}
	if err := registry.ObserveDuration(DurationSandboxCreate, operation.ResultSucceeded, 5*time.Millisecond); err != nil {
		t.Fatalf("observe duration: %v", err)
	}
	wantCounters := []CounterSample{{Name: MetricLifecycleOperations, Labels: labels, Value: 2}}
	if got := registry.CounterSnapshot(); !reflect.DeepEqual(got, wantCounters) {
		t.Fatalf("counter snapshot = %#v, want %#v", got, wantCounters)
	}
	wantDurations := []DurationSample{{Name: DurationSandboxCreate, Result: operation.ResultSucceeded, Count: 1, SumNS: uint64(5 * time.Millisecond)}}
	if got := registry.DurationSnapshot(); !reflect.DeepEqual(got, wantDurations) {
		t.Fatalf("duration snapshot = %#v, want %#v", got, wantDurations)
	}
}

// TestRegistryRejectsUnboundedValues verifies callers cannot smuggle IDs into enum-backed metric labels.
func TestRegistryRejectsUnboundedValues(t *testing.T) {
	registry := NewRegistry()
	labels := MetricLabels{
		Operation: operation.Type("sandbox-123"),
		Stage:     operation.StagePersistIntent,
		Result:    operation.ResultPending,
		Reason:    operation.ReasonNone,
	}
	if err := registry.AddCounter(MetricLifecycleOperations, labels, 1); err == nil {
		t.Fatal("expected identifier-shaped operation label to be rejected")
	}
}

// TestMetricLabelsSchemaContainsOnlyBoundedEnums freezes the label surface so
// future IDs, paths, digests, or free-form strings require an explicit review.
func TestMetricLabelsSchemaContainsOnlyBoundedEnums(t *testing.T) {
	labelsType := reflect.TypeOf(MetricLabels{})
	want := map[string]reflect.Type{
		"Operation": reflect.TypeOf(operation.Type("")),
		"Stage":     reflect.TypeOf(operation.Stage("")),
		"Result":    reflect.TypeOf(operation.Result("")),
		"Reason":    reflect.TypeOf(operation.ReasonClass("")),
	}
	if labelsType.NumField() != len(want) {
		t.Fatalf("MetricLabels fields = %d, want exactly %d bounded fields", labelsType.NumField(), len(want))
	}
	for index := 0; index < labelsType.NumField(); index++ {
		field := labelsType.Field(index)
		if wantType, exists := want[field.Name]; !exists || field.Type != wantType {
			t.Fatalf("MetricLabels field %s has type %v; want one of the reviewed bounded enum fields", field.Name, field.Type)
		}
	}
}

// TestMetricLabelsRejectResultReasonContradictions verifies metric series use
// the same failed/non-failed reason invariant as durable operation events.
func TestMetricLabelsRejectResultReasonContradictions(t *testing.T) {
	base := MetricLabels{Operation: operation.TypeCreate, Stage: operation.StageComplete, Result: operation.ResultSucceeded, Reason: operation.ReasonNone}
	invalid := []MetricLabels{
		{Operation: base.Operation, Stage: base.Stage, Result: operation.ResultFailed, Reason: operation.ReasonNone},
		{Operation: base.Operation, Stage: base.Stage, Result: operation.ResultSucceeded, Reason: operation.ReasonCleanup},
		{Operation: base.Operation, Stage: base.Stage, Result: operation.ResultPending, Reason: operation.ReasonInternal},
		{Operation: base.Operation, Stage: base.Stage, Result: operation.ResultNoop, Reason: operation.ReasonConflict},
	}
	for _, labels := range invalid {
		if err := labels.Validate(); err == nil {
			t.Fatalf("MetricLabels.Validate(%#v) error = nil, want contradiction rejection", labels)
		}
	}
}

// TestRegistryCardinalityCeiling enumerates every currently valid counter and
// duration series and proves repeated observations cannot grow that key set.
func TestRegistryCardinalityCeiling(t *testing.T) {
	metricNames := []MetricName{
		MetricLifecycleOperations, MetricLifecycleFailures, MetricRollback,
		MetricRollbackFailures, MetricContainerExits, MetricContainerOOM,
	}
	operations := []operation.Type{
		operation.TypeCreate, operation.TypeStart, operation.TypeState,
		operation.TypeKill, operation.TypeStop, operation.TypeDelete,
	}
	stages := []operation.Stage{
		operation.StageValidate, operation.StagePersistIntent, operation.StageCheckPreconditions,
		operation.StageHostPreflight, operation.StagePrepareCgroup, operation.StagePrepareStartGate,
		operation.StagePrepareStreams, operation.StageCreateProcess, operation.StagePrepareNamespaces,
		operation.StageJoinNamespaces, operation.StagePrepareRootfs, operation.StageAttachCgroup,
		operation.StageReleaseStartGate, operation.StageSignalProcess, operation.StageObserveProcess,
		operation.StageTeardown, operation.StageTransition, operation.StagePersistState,
		operation.StageRollback, operation.StagePersistResult, operation.StageComplete,
	}
	resultReasons := []struct {
		result operation.Result
		reason operation.ReasonClass
	}{
		{operation.ResultPending, operation.ReasonNone},
		{operation.ResultSucceeded, operation.ReasonNone},
		{operation.ResultNoop, operation.ReasonNone},
		{operation.ResultFailed, operation.ReasonInvalidRequest},
		{operation.ResultFailed, operation.ReasonConflict},
		{operation.ResultFailed, operation.ReasonNotFound},
		{operation.ResultFailed, operation.ReasonPrecondition},
		{operation.ResultFailed, operation.ReasonInternal},
		{operation.ResultFailed, operation.ReasonCleanup},
	}
	registry := NewRegistry()
	for _, name := range metricNames {
		for _, operationType := range operations {
			for _, stage := range stages {
				for _, pair := range resultReasons {
					labels := MetricLabels{Operation: operationType, Stage: stage, Result: pair.result, Reason: pair.reason}
					if err := registry.AddCounter(name, labels, 1); err != nil {
						t.Fatalf("AddCounter(%q, %#v) error = %v", name, labels, err)
					}
				}
			}
		}
	}
	wantCounters := len(metricNames) * len(operations) * len(stages) * len(resultReasons)
	if got := len(registry.CounterSnapshot()); got != wantCounters {
		t.Fatalf("counter series = %d, want reviewed ceiling %d", got, wantCounters)
	}
	labels := MetricLabels{Operation: operation.TypeCreate, Stage: operation.StageValidate, Result: operation.ResultPending, Reason: operation.ReasonNone}
	if err := registry.AddCounter(MetricLifecycleOperations, labels, 1); err != nil {
		t.Fatalf("repeat AddCounter() error = %v", err)
	}
	if got := len(registry.CounterSnapshot()); got != wantCounters {
		t.Fatalf("counter series after repeat = %d, want unchanged %d", got, wantCounters)
	}
	durationNames := []DurationName{DurationSandboxCreate, DurationContainerCreate, DurationContainerStart}
	results := []operation.Result{operation.ResultPending, operation.ResultSucceeded, operation.ResultFailed, operation.ResultNoop}
	for _, name := range durationNames {
		for _, result := range results {
			if err := registry.ObserveDuration(name, result, 0); err != nil {
				t.Fatalf("ObserveDuration(%q, %q) error = %v", name, result, err)
			}
		}
	}
	wantDurations := len(durationNames) * len(results)
	if got := len(registry.DurationSnapshot()); got != wantDurations {
		t.Fatalf("duration series = %d, want reviewed ceiling %d", got, wantDurations)
	}
}

// TestRegistryRejectsInvalidDimensionsAndOverflow verifies every admission
// boundary fails without mutating the prior aggregate.
func TestRegistryRejectsInvalidDimensionsAndOverflow(t *testing.T) {
	registry := NewRegistry()
	valid := MetricLabels{Operation: operation.TypeCreate, Stage: operation.StageComplete, Result: operation.ResultSucceeded, Reason: operation.ReasonNone}
	invalidLabels := []MetricLabels{
		{Operation: "resource-id", Stage: valid.Stage, Result: valid.Result, Reason: valid.Reason},
		{Operation: valid.Operation, Stage: "stage-id", Result: valid.Result, Reason: valid.Reason},
		{Operation: valid.Operation, Stage: valid.Stage, Result: "result-id", Reason: valid.Reason},
		{Operation: valid.Operation, Stage: valid.Stage, Result: valid.Result, Reason: "reason-id"},
	}
	for _, labels := range invalidLabels {
		if err := registry.AddCounter(MetricLifecycleOperations, labels, 1); err == nil {
			t.Fatalf("AddCounter(%#v) accepted an unbounded dimension", labels)
		}
	}
	if err := registry.AddCounter("unknown_metric", valid, 1); err == nil {
		t.Fatal("AddCounter() accepted an unknown metric name")
	}
	if err := registry.AddCounter(MetricLifecycleOperations, valid, 0); err == nil {
		t.Fatal("AddCounter() accepted a zero delta")
	}
	counter := counterKey{name: MetricLifecycleOperations, labels: valid}
	registry.counters[counter] = ^uint64(0)
	if err := registry.AddCounter(MetricLifecycleOperations, valid, 1); err == nil || registry.counters[counter] != ^uint64(0) {
		t.Fatalf("overflowing AddCounter() error/value = %v/%d", err, registry.counters[counter])
	}
	if err := registry.ObserveDuration("unknown_duration", operation.ResultSucceeded, time.Second); err == nil {
		t.Fatal("ObserveDuration() accepted an unknown name")
	}
	if err := registry.ObserveDuration(DurationSandboxCreate, "result-id", time.Second); err == nil {
		t.Fatal("ObserveDuration() accepted an unknown result")
	}
	if err := registry.ObserveDuration(DurationSandboxCreate, operation.ResultSucceeded, -1); err == nil {
		t.Fatal("ObserveDuration() accepted a negative duration")
	}
	duration := durationKey{name: DurationSandboxCreate, result: operation.ResultSucceeded}
	registry.durations[duration] = durationAggregate{count: ^uint64(0)}
	if err := registry.ObserveDuration(DurationSandboxCreate, operation.ResultSucceeded, 0); err == nil || registry.durations[duration].count != ^uint64(0) {
		t.Fatalf("count-overflow ObserveDuration() error/value = %v/%d", err, registry.durations[duration].count)
	}
	registry.durations[duration] = durationAggregate{count: 1, sumNS: ^uint64(0)}
	if err := registry.ObserveDuration(DurationSandboxCreate, operation.ResultSucceeded, 1); err == nil || registry.durations[duration].sumNS != ^uint64(0) {
		t.Fatalf("sum-overflow ObserveDuration() error/value = %v/%d", err, registry.durations[duration].sumNS)
	}
}

// TestRegistryConcurrentAccumulation verifies locking preserves exact totals
// under concurrent counter and duration observations.
func TestRegistryConcurrentAccumulation(t *testing.T) {
	registry := NewRegistry()
	labels := MetricLabels{Operation: operation.TypeStart, Stage: operation.StageComplete, Result: operation.ResultSucceeded, Reason: operation.ReasonNone}
	const writers = 32
	const observations = 100
	var wait sync.WaitGroup
	errorsSeen := make(chan error, writers*2)
	for writer := 0; writer < writers; writer++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := 0; index < observations; index++ {
				if err := registry.AddCounter(MetricLifecycleOperations, labels, 1); err != nil {
					errorsSeen <- err
					return
				}
				if err := registry.ObserveDuration(DurationContainerStart, operation.ResultSucceeded, time.Nanosecond); err != nil {
					errorsSeen <- err
					return
				}
			}
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatalf("concurrent observation error = %v", err)
	}
	want := uint64(writers * observations)
	if got := registry.CounterSnapshot(); len(got) != 1 || got[0].Value != want {
		t.Fatalf("counter snapshot = %#v, want one series with value %d", got, want)
	}
	if got := registry.DurationSnapshot(); len(got) != 1 || got[0].Count != want || got[0].SumNS != want {
		t.Fatalf("duration snapshot = %#v, want count/sum %d", got, want)
	}
}

// lockedBuffer makes test writes safe even if logger locking regresses, so the race detector reports the logger boundary precisely.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

// Write appends bytes while protecting the test observer from its own data race.
func (buffer *lockedBuffer) Write(value []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.b.Write(value)
}

// String returns a stable copy of the accumulated test output.
func (buffer *lockedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.b.String()
}
