// Package rollback provides a retryable LIFO stack of named idempotent inverse
// operations without treating executable closures as a persistence format.
package rollback

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"mydocker/internal/operation"
)

// SchemaVersion is the only rollback descriptor schema understood by M1.
const SchemaVersion uint32 = 1

// MaxDiagnosticBytes bounds each persisted rollback diagnostic so a hostile
// provider error cannot grow the durable state file without limit.
const MaxDiagnosticBytes = 4096

// Cause preserves the original create failure while cleanup is retried across
// API retries or daemon restarts. It is operation-scoped rather than attached
// to an individual inverse.
type Cause struct {
	Reason  operation.ReasonClass `json:"reason"`
	Message string                `json:"message"`
}

// NewCause constructs the bounded durable diagnostic that remains authoritative throughout rollback recovery.
func NewCause(reason operation.ReasonClass, message string) (Cause, error) {
	cause := Cause{Reason: reason, Message: boundedDiagnostic(message)}
	if err := cause.Validate(); err != nil {
		return Cause{}, err
	}
	return cause, nil
}

// Clone returns an independent cause value for persistence boundaries.
func (c Cause) Clone() Cause {
	return c
}

// Validate rejects an unclassified, empty, or unbounded primary diagnostic.
func (c Cause) Validate() error {
	if !c.Reason.Valid() || c.Reason == operation.ReasonNone {
		return fmt.Errorf("invalid rollback cause reason %q", c.Reason)
	}
	if strings.TrimSpace(c.Message) == "" {
		return errors.New("rollback cause message must not be empty")
	}
	if len(c.Message) > MaxDiagnosticBytes {
		return fmt.Errorf("rollback cause message exceeds %d bytes", MaxDiagnosticBytes)
	}
	return nil
}

// Descriptor is the serializable identity of one inverse operation. Metadata
// describes how a recovery-time registry can reconstruct an implementation;
// the executable Inverse itself is intentionally absent.
type Descriptor struct {
	SchemaVersion uint32          `json:"schema_version"`
	Name          string          `json:"name"`
	Action        string          `json:"action"`
	Target        string          `json:"target"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
}

// Clone returns an independent descriptor suitable for persistence snapshots.
func (d Descriptor) Clone() Descriptor {
	clone := d
	clone.Metadata = append(json.RawMessage(nil), d.Metadata...)
	return clone
}

// Validate rejects descriptors that cannot be durably identified or recovered.
func (d Descriptor) Validate() error {
	if d.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported rollback descriptor schema version %d", d.SchemaVersion)
	}
	if strings.TrimSpace(d.Name) == "" {
		return errors.New("rollback step name must not be empty")
	}
	if strings.TrimSpace(d.Action) == "" {
		return errors.New("rollback action must not be empty")
	}
	if strings.TrimSpace(d.Target) == "" {
		return errors.New("rollback target must not be empty")
	}
	if len(d.Metadata) > 0 && !json.Valid(d.Metadata) {
		return errors.New("rollback metadata must be valid JSON")
	}
	return nil
}

// Inverse is a runtime-only idempotent cleanup implementation. Implementations
// must treat an already-absent owned resource as success.
type Inverse func(context.Context) error

// Record is the persistable progress of one rollback step and never contains
// an executable function or closure.
type Record struct {
	Descriptor Descriptor `json:"descriptor"`
	Succeeded  bool       `json:"succeeded"`
	Started    bool       `json:"started"`
	Attempts   uint32     `json:"attempts,omitempty"`
	LastError  string     `json:"last_error,omitempty"`
}

// Clone returns an independent persistence record.
func (r Record) Clone() Record {
	r.Descriptor = r.Descriptor.Clone()
	return r
}

// Validate checks descriptor schema, progress ordering, and bounded failure diagnostics.
func (r Record) Validate() error {
	if err := r.Descriptor.Validate(); err != nil {
		return err
	}
	if r.Succeeded && !r.Started {
		return errors.New("successful rollback step must record that rollback started")
	}
	if r.Attempts == 0 && r.LastError != "" {
		return errors.New("rollback last error requires at least one failed attempt")
	}
	if r.Attempts > 0 && strings.TrimSpace(r.LastError) == "" {
		return errors.New("failed rollback attempt requires a last error")
	}
	if len(r.LastError) > MaxDiagnosticBytes {
		return fmt.Errorf("rollback last error exceeds %d bytes", MaxDiagnosticBytes)
	}
	return nil
}

// RecordFailure returns retryable progress with one additional failed attempt
// while retaining the descriptor and any earlier successful state.
func (r Record) RecordFailure(err error) (Record, error) {
	if err == nil {
		return Record{}, errors.New("rollback failure must not be nil")
	}
	if r.Attempts == ^uint32(0) {
		return Record{}, errors.New("rollback failure attempt counter overflow")
	}
	message := boundedDiagnostic(err.Error())
	r.Started = true
	r.Attempts++
	r.LastError = message
	if validateErr := r.Validate(); validateErr != nil {
		return Record{}, validateErr
	}
	return r, nil
}

// boundedDiagnostic replaces invalid UTF-8 and truncates at a rune boundary so memory and JSON persistence retain identical text.
func boundedDiagnostic(message string) string {
	message = strings.ToValidUTF8(message, "�")
	if len(message) <= MaxDiagnosticBytes {
		return message
	}
	message = message[:MaxDiagnosticBytes]
	for !utf8.ValidString(message) {
		message = message[:len(message)-1]
	}
	return message
}

// StepFailure associates a failed inverse attempt with its durable descriptor.
type StepFailure struct {
	Descriptor Descriptor
	Err        error
}

// Error reports the named inverse that failed in this rollback attempt.
func (f StepFailure) Error() string {
	return fmt.Sprintf("rollback step %q failed: %v", f.Descriptor.Name, f.Err)
}

// Unwrap exposes the underlying cleanup failure for errors.Is/errors.As.
func (f StepFailure) Unwrap() error {
	return f.Err
}

// Report contains the primary operation failure plus every inverse failure from
// one LIFO rollback attempt.
type Report struct {
	Primary  error
	Failures []StepFailure
}

// Clone returns a report with independent descriptors and failure slice.
func (r Report) Clone() Report {
	clone := Report{Primary: r.Primary, Failures: make([]StepFailure, len(r.Failures))}
	for index, failure := range r.Failures {
		clone.Failures[index] = StepFailure{
			Descriptor: failure.Descriptor.Clone(),
			Err:        failure.Err,
		}
	}
	return clone
}

// Err returns nil only when neither the primary action nor any inverse failed.
func (r Report) Err() error {
	if r.Primary == nil && len(r.Failures) == 0 {
		return nil
	}
	return &Error{Report: r.Clone()}
}

// Error is the aggregate error form of a rollback Report.
type Error struct {
	Report Report
}

// Error describes the primary failure and the count of additional cleanup failures.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	switch {
	case e.Report.Primary != nil && len(e.Report.Failures) > 0:
		return fmt.Sprintf("primary operation failed: %v; %d rollback step(s) failed",
			e.Report.Primary, len(e.Report.Failures))
	case e.Report.Primary != nil:
		return fmt.Sprintf("primary operation failed: %v", e.Report.Primary)
	case len(e.Report.Failures) > 0:
		return fmt.Sprintf("%d rollback step(s) failed", len(e.Report.Failures))
	default:
		return "rollback completed without error"
	}
}

// Unwrap exposes the primary error followed by every rollback error so callers
// can inspect all causes with errors.Is and errors.As.
func (e *Error) Unwrap() []error {
	if e == nil {
		return nil
	}
	causes := make([]error, 0, 1+len(e.Report.Failures))
	if e.Report.Primary != nil {
		causes = append(causes, e.Report.Primary)
	}
	for _, failure := range e.Report.Failures {
		causes = append(causes, failure)
	}
	return causes
}

// Resolver reconstructs a runtime inverse from its persisted descriptor.
// Resolver implementations are runtime wiring and are not serialized.
type Resolver func(Descriptor) (Inverse, error)

type step struct {
	descriptor Descriptor
	inverse    Inverse
	succeeded  bool
}

// Stack records acquisition-order inverse operations and runs them in LIFO order.
type Stack struct {
	mu      sync.Mutex
	steps   []step
	sealed  bool
	running bool
}

// New creates an acquisition journal that callers populate before sealing and persisting it for crash-safe LIFO cleanup.
func New() *Stack {
	return &Stack{}
}

// Restore reconstructs runtime handlers for persisted rollback progress while
// keeping the persisted format free of executable closures.
func Restore(records []Record, resolver Resolver) (*Stack, error) {
	stack := New()
	seen := make(map[string]struct{}, len(records))
	for index, record := range records {
		if err := record.Validate(); err != nil {
			return nil, fmt.Errorf("validate rollback record %d: %w", index, err)
		}
		if _, exists := seen[record.Descriptor.Name]; exists {
			return nil, fmt.Errorf("rollback step name %q is duplicated", record.Descriptor.Name)
		}
		seen[record.Descriptor.Name] = struct{}{}
		var inverse Inverse
		if record.Succeeded {
			inverse = func(context.Context) error { return nil }
		} else {
			if resolver == nil {
				return nil, errors.New("rollback resolver must not be nil while pending steps exist")
			}
			var err error
			inverse, err = resolver(record.Descriptor.Clone())
			if err != nil {
				return nil, fmt.Errorf("resolve rollback step %q: %w", record.Descriptor.Name, err)
			}
			if inverse == nil {
				return nil, fmt.Errorf("resolve rollback step %q: nil inverse", record.Descriptor.Name)
			}
		}
		stack.steps = append(stack.steps, step{descriptor: record.Descriptor.Clone(), inverse: inverse, succeeded: record.Succeeded})
		stack.sealed = stack.sealed || record.Started
	}
	return stack, nil
}

// Push registers a named idempotent inverse immediately after its resource is
// acquired; duplicate names and registration after rollback begins are rejected.
func (s *Stack) Push(descriptor Descriptor, inverse Inverse) error {
	if s == nil {
		return errors.New("rollback stack must not be nil")
	}
	if err := descriptor.Validate(); err != nil {
		return err
	}
	if inverse == nil {
		return errors.New("rollback inverse must not be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return errors.New("cannot push rollback step after rollback has started")
	}
	for _, existing := range s.steps {
		if existing.descriptor.Name == descriptor.Name {
			return fmt.Errorf("rollback step name %q is already registered", descriptor.Name)
		}
	}
	s.steps = append(s.steps, step{descriptor: descriptor.Clone(), inverse: inverse})
	return nil
}

// Begin seals the acquisition-order stack before rollback execution. Lifecycle
// coordinators should call Begin and durably persist Snapshot before invoking
// any inverse when crash recovery matters; repeated calls are idempotent, do
// not execute cleanup, and continue to reject all later Push calls.
func (s *Stack) Begin() error {
	if s == nil {
		return errors.New("rollback stack must not be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sealed = true
	return nil
}

// Snapshot returns acquisition-order persistence records without executable handlers.
func (s *Stack) Snapshot() []Record {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]Record, len(s.steps))
	for index, step := range s.steps {
		records[index] = Record{
			Descriptor: step.descriptor.Clone(),
			Succeeded:  step.succeeded,
			Started:    s.sealed,
		}
	}
	return records
}

// Pending reports how many registered inverse operations have not yet succeeded.
func (s *Stack) Pending() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := 0
	for _, step := range s.steps {
		if !step.succeeded {
			pending++
		}
	}
	return pending
}

// Run seals the stack through Begin and executes every not-yet-successful
// inverse in LIFO order. It is a convenience path for in-process callers;
// crash-safe coordinators should call Begin and persist Snapshot before Run.
// Run continues after failures, and a later call retries only failed steps.
func (s *Stack) Run(ctx context.Context, primary error) Report {
	report := Report{Primary: primary}
	if s == nil {
		report.Failures = append(report.Failures, StepFailure{
			Descriptor: Descriptor{SchemaVersion: SchemaVersion, Name: "stack", Action: "rollback", Target: "nil"},
			Err:        errors.New("rollback stack must not be nil"),
		})
		return report
	}
	if err := s.Begin(); err != nil {
		report.Failures = append(report.Failures, StepFailure{
			Descriptor: Descriptor{SchemaVersion: SchemaVersion, Name: "stack", Action: "rollback", Target: "begin"},
			Err:        err,
		})
		return report
	}

	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		report.Failures = append(report.Failures, StepFailure{
			Descriptor: Descriptor{SchemaVersion: SchemaVersion, Name: "stack", Action: "rollback", Target: "concurrent"},
			Err:        errors.New("rollback is already running"),
		})
		return report
	}
	s.running = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	for index := len(s.steps) - 1; index >= 0; index-- {
		s.mu.Lock()
		current := &s.steps[index]
		if current.succeeded {
			s.mu.Unlock()
			continue
		}
		descriptor := current.descriptor.Clone()
		inverse := current.inverse
		s.mu.Unlock()

		if err := inverse(ctx); err != nil {
			report.Failures = append(report.Failures, StepFailure{
				Descriptor: descriptor,
				Err:        err,
			})
			continue
		}

		s.mu.Lock()
		s.steps[index].succeeded = true
		s.mu.Unlock()
	}
	return report
}
