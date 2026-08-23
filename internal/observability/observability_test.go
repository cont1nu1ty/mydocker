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
