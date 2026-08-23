package observability

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"mydocker/internal/operation"
)

// MetricName is the bounded set of in-process counters proposed for M3.
type MetricName string

const (
	// MetricLifecycleOperations counts accepted terminal and active lifecycle facts.
	MetricLifecycleOperations MetricName = "lifecycle_operations_total"
	// MetricLifecycleFailures counts failed lifecycle stage facts.
	MetricLifecycleFailures MetricName = "lifecycle_failures_total"
	// MetricRollback counts rollback stage attempts.
	MetricRollback MetricName = "rollback_total"
	// MetricRollbackFailures counts failed rollback stage attempts.
	MetricRollbackFailures MetricName = "rollback_failures_total"
	// MetricContainerExits counts captured container terminal outcomes.
	MetricContainerExits MetricName = "container_exits_total"
	// MetricContainerOOM counts Attempt outcomes with owner-scoped OOM evidence.
	MetricContainerOOM MetricName = "container_oom_total"
)

// Valid reports whether name belongs to the bounded M3 metric registry.
func (name MetricName) Valid() bool {
	switch name {
	case MetricLifecycleOperations, MetricLifecycleFailures, MetricRollback,
		MetricRollbackFailures, MetricContainerExits, MetricContainerOOM:
		return true
	default:
		return false
	}
}

// MetricLabels intentionally contains only bounded enums; resource, request,
// image, and operation identities cannot be represented as metric labels.
type MetricLabels struct {
	Operation operation.Type        `json:"operation"`
	Stage     operation.Stage       `json:"stage"`
	Result    operation.Result      `json:"result"`
	Reason    operation.ReasonClass `json:"reason"`
}

// Validate rejects unset or unbounded labels before a new cardinality key is created.
func (labels MetricLabels) Validate() error {
	if !labels.Operation.Valid() || !labels.Stage.Valid() || !labels.Result.Valid() || !labels.Reason.Valid() {
		return errors.New("metric labels must use bounded operation, stage, result, and reason values")
	}
	return nil
}

// CounterSample is one deterministic snapshot row returned for exposition or tests.
type CounterSample struct {
	Name   MetricName   `json:"name"`
	Labels MetricLabels `json:"labels"`
	Value  uint64       `json:"value"`
}

// DurationName identifies a same-process daemon duration aggregate without
// treating cross-process timestamps as benchmark-compatible observations.
type DurationName string

const (
	// DurationSandboxCreate aggregates daemon-side Sandbox create time.
	DurationSandboxCreate DurationName = "sandbox_create_duration_seconds"
	// DurationContainerCreate aggregates daemon-side Container create time.
	DurationContainerCreate DurationName = "container_create_duration_seconds"
	// DurationContainerStart aggregates daemon-side Container start time.
	DurationContainerStart DurationName = "container_start_duration_seconds"
)

// Valid reports whether name belongs to the bounded daemon duration set.
func (name DurationName) Valid() bool {
	return name == DurationSandboxCreate || name == DurationContainerCreate || name == DurationContainerStart
}

// DurationSample keeps count and nanosecond sum so callers cannot confuse a
// scrape aggregate with exact per-request benchmark samples.
type DurationSample struct {
	Name   DurationName     `json:"name"`
	Result operation.Result `json:"result"`
	Count  uint64           `json:"count"`
	SumNS  uint64           `json:"sum_ns"`
}

// counterKey is comparable and contains the entire allowed label vocabulary.
type counterKey struct {
	name   MetricName
	labels MetricLabels
}

// durationKey is comparable and limits duration cardinality to name and result.
type durationKey struct {
	name   DurationName
	result operation.Result
}

// durationAggregate accumulates same-process observations without retaining IDs.
type durationAggregate struct {
	count uint64
	sumNS uint64
}

// Registry stores low-cardinality in-process aggregates for one daemon lifetime.
type Registry struct {
	mu        sync.RWMutex
	counters  map[counterKey]uint64
	durations map[durationKey]durationAggregate
}

// NewRegistry returns an empty registry whose schema makes high-cardinality
// identifiers unrepresentable rather than relying on caller discipline.
func NewRegistry() *Registry {
	return &Registry{
		counters:  make(map[counterKey]uint64),
		durations: make(map[durationKey]durationAggregate),
	}
}

// AddCounter atomically increments one bounded counter and rejects overflow.
func (registry *Registry) AddCounter(name MetricName, labels MetricLabels, delta uint64) error {
	if !name.Valid() {
		return fmt.Errorf("unsupported metric name %q", name)
	}
	if err := labels.Validate(); err != nil {
		return err
	}
	if delta == 0 {
		return errors.New("metric counter delta must be greater than zero")
	}
	key := counterKey{name: name, labels: labels}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	current := registry.counters[key]
	if ^uint64(0)-current < delta {
		return errors.New("metric counter overflow")
	}
	registry.counters[key] = current + delta
	return nil
}

// ObserveDuration records one non-negative same-process elapsed observation.
func (registry *Registry) ObserveDuration(name DurationName, result operation.Result, duration time.Duration) error {
	if !name.Valid() || !result.Valid() {
		return errors.New("duration name and result must use bounded values")
	}
	if duration < 0 {
		return errors.New("duration observation must not be negative")
	}
	value := uint64(duration)
	key := durationKey{name: name, result: result}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	aggregate := registry.durations[key]
	if aggregate.count == ^uint64(0) || ^uint64(0)-aggregate.sumNS < value {
		return errors.New("duration aggregate overflow")
	}
	aggregate.count++
	aggregate.sumNS += value
	registry.durations[key] = aggregate
	return nil
}

// CounterSnapshot returns deterministic independent rows for diagnostic exposition.
func (registry *Registry) CounterSnapshot() []CounterSample {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	rows := make([]CounterSample, 0, len(registry.counters))
	for key, value := range registry.counters {
		rows = append(rows, CounterSample{Name: key.name, Labels: key.labels, Value: value})
	}
	sort.Slice(rows, func(left, right int) bool {
		return fmt.Sprint(rows[left]) < fmt.Sprint(rows[right])
	})
	return rows
}

// DurationSnapshot returns deterministic aggregates without exposing mutable registry maps.
func (registry *Registry) DurationSnapshot() []DurationSample {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	rows := make([]DurationSample, 0, len(registry.durations))
	for key, aggregate := range registry.durations {
		rows = append(rows, DurationSample{Name: key.name, Result: key.result, Count: aggregate.count, SumNS: aggregate.sumNS})
	}
	sort.Slice(rows, func(left, right int) bool {
		if rows[left].Name == rows[right].Name {
			return rows[left].Result < rows[right].Result
		}
		return rows[left].Name < rows[right].Name
	})
	return rows
}
