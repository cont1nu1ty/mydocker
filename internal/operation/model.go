// Package operation defines the durable identity, idempotency, and event
// contracts shared by lifecycle operations.
package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

// SchemaVersion is the only durable operation-record schema understood by this
// milestone. Event records evolve independently through EventSchemaVersion.
const SchemaVersion uint32 = 1

// EventSchemaVersion identifies optional duration evidence: a missing duration
// is unavailable, while an explicit zero is a measured zero-length span.
const EventSchemaVersion uint32 = 2

// CurrentFingerprintVersion identifies the canonical request encoding and
// digest algorithm used by CanonicalRequestFingerprint.
const CurrentFingerprintVersion uint32 = 1

// OperationID is the client-generated identity of one durable lifecycle
// intent across transport retries.
type OperationID string

// Validate rejects an empty operation identity before it can be persisted.
func (id OperationID) Validate() error {
	return validateIdentifier("operation ID", string(id))
}

// Type is the bounded lifecycle verb recorded for an operation.
type Type string

const (
	// TypeCreate creates a resource without changing a prior immutable spec.
	TypeCreate Type = "create"
	// TypeStart releases a prepared container attempt for execution.
	TypeStart Type = "start"
	// TypeState reads and returns the authoritative state of a resource.
	TypeState Type = "state"
	// TypeKill requests a signal for a verified running process identity.
	TypeKill Type = "kill"
	// TypeStop gracefully stops a Sandbox or Container before forced termination.
	TypeStop Type = "stop"
	// TypeDelete removes a stopped resource and all resources it owns.
	TypeDelete Type = "delete"
)

// Valid reports whether the lifecycle verb belongs to the M1 bounded set.
func (t Type) Valid() bool {
	switch t {
	case TypeCreate, TypeStart, TypeState, TypeKill, TypeStop, TypeDelete:
		return true
	default:
		return false
	}
}

// TargetKind is the bounded kind of resource addressed by an operation.
type TargetKind string

const (
	// TargetSandbox addresses a stable Sandbox resource.
	TargetSandbox TargetKind = "sandbox"
	// TargetContainer addresses an API Container and its canonical Attempt.
	TargetContainer TargetKind = "container"
	// TargetAttempt addresses one execution Attempt when an internal stage needs
	// the more specific identity.
	TargetAttempt TargetKind = "attempt"
)

// Valid reports whether the resource kind belongs to the M1 bounded set.
func (k TargetKind) Valid() bool {
	switch k {
	case TargetSandbox, TargetContainer, TargetAttempt:
		return true
	default:
		return false
	}
}

// Target identifies the resource to which an operation ID is permanently
// bound during the operation retention window.
type Target struct {
	Kind TargetKind `json:"kind"`
	ID   string     `json:"id"`
}

// Validate rejects incomplete or unsupported resource identities.
func (t Target) Validate() error {
	if !t.Kind.Valid() {
		return fmt.Errorf("invalid operation target kind %q", t.Kind)
	}
	return validateIdentifier("operation target ID", t.ID)
}

// validateIdentifier applies the storage-safe identity contract shared by operations and their targets.
func validateIdentifier(field, value string) error {
	if value == "" || strings.TrimSpace(value) != value {
		return fmt.Errorf("%s must be non-empty without surrounding whitespace", field)
	}
	if len(value) > 128 {
		return fmt.Errorf("%s must not exceed 128 bytes", field)
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return fmt.Errorf("%s contains unsupported whitespace or control characters", field)
		}
	}
	return nil
}

// Equal reports whether two operation targets name the same resource.
func (t Target) Equal(other Target) bool {
	return t.Kind == other.Kind && t.ID == other.ID
}

// validateTypeTarget rejects lifecycle verbs that cannot apply to the selected resource kind.
func validateTypeTarget(operationType Type, targetKind TargetKind) error {
	allowed := false
	switch operationType {
	case TypeCreate:
		allowed = targetKind == TargetSandbox || targetKind == TargetContainer
	case TypeStart:
		allowed = targetKind == TargetContainer
	case TypeState:
		allowed = targetKind == TargetSandbox || targetKind == TargetContainer || targetKind == TargetAttempt
	case TypeKill:
		allowed = targetKind == TargetContainer || targetKind == TargetAttempt
	case TypeStop:
		allowed = targetKind == TargetSandbox || targetKind == TargetContainer
	case TypeDelete:
		allowed = targetKind == TargetSandbox || targetKind == TargetContainer
	}
	if !allowed {
		return fmt.Errorf("operation type %q cannot target %q", operationType, targetKind)
	}
	return nil
}

// ResourceRef is a compatibility name for Target at persistence boundaries.
// Both names have identical representation and validation behavior.
type ResourceRef = Target

// State is the bounded durable execution state of an operation.
type State string

const (
	// StatePending records an accepted operation whose first stage has not run.
	StatePending State = "pending"
	// StateRunning records an operation that can be resumed by reconciliation.
	StateRunning State = "running"
	// StateSucceeded records a completed operation with a replayable result.
	StateSucceeded State = "succeeded"
	// StateFailed records a terminal operation whose primary action failed.
	StateFailed State = "failed"
)

// Valid reports whether the state belongs to the M1 bounded set.
func (s State) Valid() bool {
	switch s {
	case StatePending, StateRunning, StateSucceeded, StateFailed:
		return true
	default:
		return false
	}
}

// Active reports whether reconciliation may continue this operation.
func (s State) Active() bool {
	return s == StatePending || s == StateRunning
}

// Terminal reports whether transport retries must replay the stored result.
func (s State) Terminal() bool {
	return s == StateSucceeded || s == StateFailed
}

// Stage is a bounded lifecycle checkpoint suitable for events and persistence.
type Stage string

const (
	// StageValidate validates the structured request and current lifecycle state.
	StageValidate Stage = "validate"
	// StagePersistIntent durably records intent before non-trivial side effects.
	StagePersistIntent Stage = "persist_intent"
	// StageCheckPreconditions checks lifecycle state and ownership invariants.
	StageCheckPreconditions Stage = "check_preconditions"
	// StageHostPreflight verifies Linux, privilege, namespace, mount, and cgroup prerequisites before acquisition.
	StageHostPreflight Stage = "host_preflight"
	// StagePrepareCgroup creates the owned cgroup level and applies effective controls before process start.
	StagePrepareCgroup Stage = "prepare_cgroup"
	// StagePrepareStartGate creates the closed one-shot gate before an Attempt init process can exist.
	StagePrepareStartGate Stage = "prepare_start_gate"
	// StagePrepareStreams creates owned stdout and stderr endpoints before an Attempt init process can exist.
	StagePrepareStreams Stage = "prepare_streams"
	// StageCreateProcess creates the gated init or keeper in its already prepared cgroup and captures a strong process identity.
	StageCreateProcess Stage = "create_process"
	// StagePrepareNamespaces captures or creates namespaces only after their owner process has strong identity evidence.
	StagePrepareNamespaces Stage = "prepare_namespaces"
	// StageJoinNamespaces joins only owner-verified Sandbox namespaces from a dedicated runtime helper thread.
	StageJoinNamespaces Stage = "join_namespaces"
	// StagePrepareRootfs contains mount propagation, pivot_root, proc, and explicitly allowed device setup.
	StagePrepareRootfs Stage = "prepare_rootfs"
	// StageAttachCgroup confirms process membership in the intended Attempt cgroup before releasing its gate.
	StageAttachCgroup Stage = "attach_cgroup"
	// StageReleaseStartGate allows a fully prepared Attempt to exec its workload.
	StageReleaseStartGate Stage = "release_start_gate"
	// StageSignalProcess records a signal plan that was action-time verified and handed to the process controller.
	StageSignalProcess Stage = "signal_process"
	// StageObserveProcess captures running or terminal process facts without inferring them from transport success.
	StageObserveProcess Stage = "observe_process"
	// StageTeardown removes owned process, mount, namespace, and cgroup resources in dependency order.
	StageTeardown Stage = "teardown"
	// StageTransition applies one pure M1 lifecycle state transition.
	StageTransition Stage = "transition"
	// StagePersistState durably stores the transitioned lifecycle state.
	StagePersistState Stage = "persist_state"
	// StageRollback runs registered inverse operations after a primary failure.
	StageRollback Stage = "rollback"
	// StagePersistResult durably stores the terminal result for replay.
	StagePersistResult Stage = "persist_result"
	// StageComplete marks the final event after the result is durable.
	StageComplete Stage = "complete"
)

// Valid reports whether the stage belongs to the bounded persistence vocabulary.
func (s Stage) Valid() bool {
	switch s {
	case StageValidate, StagePersistIntent, StageCheckPreconditions,
		StageHostPreflight, StagePrepareCgroup, StagePrepareStartGate, StagePrepareStreams,
		StageCreateProcess, StagePrepareNamespaces, StageJoinNamespaces, StagePrepareRootfs,
		StageAttachCgroup, StageReleaseStartGate, StageSignalProcess,
		StageObserveProcess, StageTeardown, StageTransition, StagePersistState, StageRollback,
		StagePersistResult, StageComplete:
		return true
	default:
		return false
	}
}

// Result is the bounded outcome recorded for an operation or one stage event.
type Result string

const (
	// ResultPending means the operation or stage has not reached an outcome.
	ResultPending Result = "pending"
	// ResultSucceeded means the requested effect completed and was confirmed.
	ResultSucceeded Result = "succeeded"
	// ResultFailed means the requested effect did not complete successfully.
	ResultFailed Result = "failed"
	// ResultNoop means the requested target state was already confirmed.
	ResultNoop Result = "noop"
)

// Valid reports whether the result belongs to the bounded event vocabulary.
func (r Result) Valid() bool {
	switch r {
	case ResultPending, ResultSucceeded, ResultFailed, ResultNoop:
		return true
	default:
		return false
	}
}

// ReasonClass is a low-cardinality reason suitable for event and metric labels.
type ReasonClass string

const (
	// ReasonNone indicates that no failure reason applies.
	ReasonNone ReasonClass = "none"
	// ReasonInvalidRequest classifies request or schema validation failures.
	ReasonInvalidRequest ReasonClass = "invalid_request"
	// ReasonConflict classifies incompatible concurrent or idempotency requests.
	ReasonConflict ReasonClass = "conflict"
	// ReasonNotFound classifies an absent requested target.
	ReasonNotFound ReasonClass = "not_found"
	// ReasonPrecondition classifies an unmet lifecycle precondition.
	ReasonPrecondition ReasonClass = "precondition"
	// ReasonInternal classifies an implementation or persistence failure.
	ReasonInternal ReasonClass = "internal"
	// ReasonCleanup classifies a rollback or teardown failure.
	ReasonCleanup ReasonClass = "cleanup"
)

// Valid reports whether the reason belongs to the bounded observability vocabulary.
func (r ReasonClass) Valid() bool {
	switch r {
	case ReasonNone, ReasonInvalidRequest, ReasonConflict, ReasonNotFound,
		ReasonPrecondition, ReasonInternal, ReasonCleanup:
		return true
	default:
		return false
	}
}

// RequestFingerprint binds an operation ID to canonical request content.
type RequestFingerprint struct {
	Version uint32 `json:"version"`
	SHA256  string `json:"sha256"`
}

// Validate rejects fingerprints produced by an unknown canonicalization schema
// or with a malformed SHA-256 digest.
func (f RequestFingerprint) Validate() error {
	if f.Version != CurrentFingerprintVersion {
		return fmt.Errorf("unsupported fingerprint version %d", f.Version)
	}
	if len(f.SHA256) != 64 {
		return errors.New("request fingerprint must contain a 64-character SHA-256 digest")
	}
	for _, char := range f.SHA256 {
		if !strings.ContainsRune("0123456789abcdef", char) {
			return errors.New("request fingerprint SHA-256 digest must use lowercase hexadecimal")
		}
	}
	return nil
}

// Equal reports whether two fingerprints use the same schema and digest.
func (f RequestFingerprint) Equal(other RequestFingerprint) bool {
	return f == other
}

// Sequence orders events inside one explicitly selected persistence scope.
type Sequence uint64

// Validate rejects the reserved zero value, so missing sequence data is visible.
func (s Sequence) Validate() error {
	if s == 0 {
		return errors.New("event sequence must be greater than zero")
	}
	return nil
}

// Next returns the immediately following sequence value and reports overflow.
func (s Sequence) Next() (Sequence, error) {
	if s == ^Sequence(0) {
		return 0, errors.New("event sequence overflow")
	}
	return s + 1, nil
}

// Duration stores a same-process monotonic elapsed sample as nanoseconds.
// It must never be computed by subtracting unsynchronised cross-process clocks.
type Duration time.Duration

// Validate rejects negative elapsed samples while permitting a real zero sample.
func (d Duration) Validate() error {
	if d < 0 {
		return errors.New("event duration must not be negative")
	}
	return nil
}

// Value returns the standard-library duration represented by this sample.
func (d Duration) Value() time.Duration {
	return time.Duration(d)
}

// Operation is the durable record used to resume or replay a lifecycle intent.
// Response is opaque API result JSON and must already conform to the owning
// operation type's versioned response schema.
type Operation struct {
	SchemaVersion uint32             `json:"schema_version"`
	ID            OperationID        `json:"id"`
	Type          Type               `json:"type"`
	Target        Target             `json:"target"`
	Fingerprint   RequestFingerprint `json:"fingerprint"`
	State         State              `json:"state"`
	Stage         Stage              `json:"stage"`
	Result        Result             `json:"result"`
	Reason        ReasonClass        `json:"reason"`
	Response      json.RawMessage    `json:"response,omitempty"`
}

// Clone returns an independent copy safe for transaction and store boundaries.
func (o Operation) Clone() Operation {
	clone := o
	clone.Response = append(json.RawMessage(nil), o.Response...)
	return clone
}

// Validate rejects records that cannot be safely resumed or replayed.
func (o Operation) Validate() error {
	if o.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported operation schema version %d", o.SchemaVersion)
	}
	if err := o.ID.Validate(); err != nil {
		return err
	}
	if !o.Type.Valid() {
		return fmt.Errorf("invalid operation type %q", o.Type)
	}
	if err := o.Target.Validate(); err != nil {
		return err
	}
	if err := validateTypeTarget(o.Type, o.Target.Kind); err != nil {
		return err
	}
	if err := o.Fingerprint.Validate(); err != nil {
		return err
	}
	if !o.State.Valid() {
		return fmt.Errorf("invalid operation state %q", o.State)
	}
	if !o.Stage.Valid() {
		return fmt.Errorf("invalid operation stage %q", o.Stage)
	}
	if !o.Result.Valid() {
		return fmt.Errorf("invalid operation result %q", o.Result)
	}
	if !o.Reason.Valid() {
		return fmt.Errorf("invalid operation reason %q", o.Reason)
	}
	if o.State.Active() && o.Result != ResultPending {
		return fmt.Errorf("active operation must have %q result", ResultPending)
	}
	if o.State == StateSucceeded && o.Result != ResultSucceeded && o.Result != ResultNoop {
		return errors.New("succeeded operation must have succeeded or noop result")
	}
	if o.State == StateFailed && o.Result != ResultFailed {
		return fmt.Errorf("failed operation must have %q result", ResultFailed)
	}
	if o.Result == ResultFailed && o.Reason == ReasonNone {
		return errors.New("failed operation must have a bounded reason class")
	}
	if o.Result != ResultFailed && o.Reason != ReasonNone {
		return errors.New("non-failed operation must use reason class none")
	}
	if len(o.Response) > 0 && !json.Valid(o.Response) {
		return errors.New("operation response must be valid JSON")
	}
	if o.State == StatePending && o.Stage != StageValidate {
		return fmt.Errorf("pending operation must remain at %q stage", StageValidate)
	}
	if o.State == StateRunning && (o.Stage == StageValidate || o.Stage == StageComplete) {
		return errors.New("running operation must be between persist_intent and persist_result stages")
	}
	if o.State.Terminal() && o.Stage != StageComplete {
		return fmt.Errorf("terminal operation must use %q stage", StageComplete)
	}
	return nil
}

// Event records one ordered operation stage fact and an optional same-process duration.
// A nil Duration means the stage was not measured; it must not be interpreted as zero elapsed time.
type Event struct {
	SchemaVersion      uint32          `json:"schema_version"`
	Sequence           Sequence        `json:"sequence"`
	OperationID        OperationID     `json:"operation_id"`
	Type               Type            `json:"type"`
	Target             Target          `json:"target"`
	Resources          []Target        `json:"resources"`
	Stage              Stage           `json:"stage"`
	Result             Result          `json:"result"`
	Reason             ReasonClass     `json:"reason"`
	OccurredAt         time.Time       `json:"occurred_at"`
	Duration           *Duration       `json:"duration_ns,omitempty"`
	Generation         uint64          `json:"generation,omitempty"`
	ObservedGeneration uint64          `json:"observed_generation,omitempty"`
	Details            json.RawMessage `json:"details,omitempty"`
}

// Clone returns an independent event copy safe for store and observer boundaries.
func (e Event) Clone() Event {
	clone := e
	clone.Resources = append([]Target(nil), e.Resources...)
	clone.Details = append(json.RawMessage(nil), e.Details...)
	if e.Duration != nil {
		duration := *e.Duration
		clone.Duration = &duration
	}
	return clone
}

// Validate rejects unordered, unbounded, or internally inconsistent events.
func (e Event) Validate() error {
	if e.SchemaVersion != EventSchemaVersion {
		return fmt.Errorf("unsupported event schema version %d", e.SchemaVersion)
	}
	if err := e.Sequence.Validate(); err != nil {
		return err
	}
	if err := e.OperationID.Validate(); err != nil {
		return err
	}
	if !e.Type.Valid() {
		return fmt.Errorf("invalid event operation type %q", e.Type)
	}
	if err := e.Target.Validate(); err != nil {
		return err
	}
	if err := validateTypeTarget(e.Type, e.Target.Kind); err != nil {
		return err
	}
	allowAbsentDelete := e.Type == TypeDelete && e.Generation == 0 && e.ObservedGeneration == 0
	if err := validateEventResources(e.Target, e.Resources, allowAbsentDelete); err != nil {
		return err
	}
	if !e.Stage.Valid() {
		return fmt.Errorf("invalid event stage %q", e.Stage)
	}
	if !e.Result.Valid() {
		return fmt.Errorf("invalid event result %q", e.Result)
	}
	if !e.Reason.Valid() {
		return fmt.Errorf("invalid event reason %q", e.Reason)
	}
	if e.Result == ResultFailed && e.Reason == ReasonNone {
		return errors.New("failed event must have a bounded reason class")
	}
	if e.Result != ResultFailed && e.Reason != ReasonNone {
		return errors.New("non-failed event must use reason class none")
	}
	if e.OccurredAt.IsZero() {
		return errors.New("event wall-clock timestamp must not be zero")
	}
	if e.Duration != nil {
		if err := e.Duration.Validate(); err != nil {
			return err
		}
	}
	if e.ObservedGeneration > e.Generation {
		return errors.New("event observed generation must not exceed generation")
	}
	if len(e.Details) > 0 && !json.Valid(e.Details) {
		return errors.New("event details must be valid JSON")
	}
	return nil
}

// validateEventResources requires execution identity triples except for a verified metadata-absent delete no-op.
func validateEventResources(primary Target, resources []Target, allowAbsentDelete bool) error {
	if len(resources) == 0 {
		return errors.New("event resources must include the primary target")
	}
	foundPrimary := false
	seen := make(map[Target]struct{}, len(resources))
	for _, resource := range resources {
		if err := resource.Validate(); err != nil {
			return fmt.Errorf("invalid event resource: %w", err)
		}
		if _, exists := seen[resource]; exists {
			return fmt.Errorf("event resource %s/%s is duplicated", resource.Kind, resource.ID)
		}
		seen[resource] = struct{}{}
		foundPrimary = foundPrimary || resource.Equal(primary)
	}
	if !foundPrimary {
		return errors.New("event resources do not include the primary target")
	}
	if (primary.Kind == TargetContainer || primary.Kind == TargetAttempt) && !allowAbsentDelete {
		for _, required := range []TargetKind{TargetSandbox, TargetContainer, TargetAttempt} {
			found := false
			for resource := range seen {
				if resource.Kind == required {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("execution event resources must include a %s identity", required)
			}
		}
	}
	return nil
}

// ValidateAfter verifies this event and requires it to immediately follow the
// previous event in the caller-selected ordering scope.
func (e Event) ValidateAfter(previous Event) error {
	if err := previous.Validate(); err != nil {
		return fmt.Errorf("invalid previous event: %w", err)
	}
	if err := e.Validate(); err != nil {
		return err
	}
	next, err := previous.Sequence.Next()
	if err != nil {
		return err
	}
	if e.Sequence != next {
		return fmt.Errorf("event sequence %d must immediately follow %d", e.Sequence, previous.Sequence)
	}
	return nil
}
