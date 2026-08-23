package domain

import (
	"fmt"
	"net"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// SchemaVersion is the only domain-record schema understood by M1.
	SchemaVersion uint32 = 1
	// InitialGeneration is the immutable create-spec generation used by M1 resources.
	InitialGeneration       Generation = 1
	maxSandboxHostnameBytes            = 64
	maxSandboxDNSServers               = 3
)

// Generation identifies an accepted desired-spec revision, never a store CAS revision.
type Generation uint64

// NetworkIntent preserves the caller's node-local networking intent for later providers.
type NetworkIntent struct {
	Mode        string   `json:"mode,omitempty"`
	Attachments []string `json:"attachments,omitempty"`
}

// Clone returns networking intent with an independent attachments slice.
func (n NetworkIntent) Clone() NetworkIntent {
	clone := n
	clone.Attachments = append([]string(nil), n.Attachments...)
	return clone
}

// Validate rejects intent text that cannot be safely persisted for a later network provider.
func (n NetworkIntent) Validate() error {
	if strings.ContainsRune(n.Mode, '\x00') {
		return NewError(CodeInvalidArgument, "network.mode", "must not contain NUL")
	}
	for index, attachment := range n.Attachments {
		if strings.TrimSpace(attachment) == "" || strings.ContainsRune(attachment, '\x00') {
			return NewError(CodeInvalidArgument, fmt.Sprintf("network.attachments[%d]", index), "must be non-empty and contain no NUL")
		}
	}
	return nil
}

// SandboxSpec is the immutable create-time description of a stable workload environment.
type SandboxSpec struct {
	Hostname  string            `json:"hostname,omitempty"`
	DNS       []string          `json:"dns,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Network   NetworkIntent     `json:"network"`
	Resources Resources         `json:"resources"`
}

// Clone returns a Sandbox spec whose maps, slices, and resource pointers do not alias the source.
func (s SandboxSpec) Clone() SandboxSpec {
	clone := s
	clone.DNS = append([]string(nil), s.DNS...)
	clone.Network = s.Network.Clone()
	clone.Resources = s.Resources.Clone()
	if s.Labels != nil {
		clone.Labels = make(map[string]string, len(s.Labels))
		for key, value := range s.Labels {
			clone.Labels[key] = value
		}
	}
	return clone
}

// Validate checks the persistence-safe environment fields and authoritative resource policy.
func (s SandboxSpec) Validate() error {
	if err := validateSandboxHostname(s.Hostname); err != nil {
		return err
	}
	if err := validateSandboxDNS(s.DNS); err != nil {
		return err
	}
	for key, value := range s.Labels {
		if strings.TrimSpace(key) == "" || strings.ContainsRune(key, '\x00') || strings.ContainsRune(value, '\x00') {
			return NewError(CodeInvalidArgument, "labels", "keys must be non-empty and labels must contain no NUL")
		}
	}
	if err := s.Network.Validate(); err != nil {
		return err
	}
	return s.Resources.Validate()
}

// validateSandboxHostname accepts the empty default or a persistence-safe
// UTF-8 hostname that fits Linux's 64-byte UTS nodename limit.
func validateSandboxHostname(hostname string) error {
	if hostname == "" {
		return nil
	}
	if len(hostname) > maxSandboxHostnameBytes || !utf8.ValidString(hostname) || strings.ContainsRune(hostname, '\x00') {
		return NewError(CodeInvalidArgument, "hostname", "must be valid UTF-8 without NUL and no longer than 64 bytes")
	}
	return nil
}

// validateSandboxDNS retains caller spelling and order while requiring the
// bounded M3 resolv.conf input to contain only parseable IP address literals.
func validateSandboxDNS(servers []string) error {
	if len(servers) > maxSandboxDNSServers {
		return NewError(CodeInvalidArgument, "dns", "must contain no more than 3 servers")
	}
	for index, server := range servers {
		if net.ParseIP(server) == nil {
			return NewError(CodeInvalidArgument, fmt.Sprintf("dns[%d]", index), "must be an IP address literal")
		}
	}
	return nil
}

// Condition records an orthogonal lifecycle fact without inventing another phase.
type Condition struct {
	Type    string `json:"type"`
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`
}

// LifecycleObservation links a resource status projection to its latest durable operation event.
type LifecycleObservation struct {
	OperationID   string `json:"operation_id,omitempty"`
	EventSequence uint64 `json:"event_sequence,omitempty"`
	Reason        string `json:"reason,omitempty"`
}

// Validate accepts an empty initial observation or requires all durable event reference fields together.
func (o LifecycleObservation) Validate() error {
	if o.OperationID == "" && o.EventSequence == 0 && o.Reason == "" {
		return nil
	}
	if strings.TrimSpace(o.OperationID) == "" || o.EventSequence == 0 || strings.TrimSpace(o.Reason) == "" {
		return NewError(CodeInvalidArgument, "lifecycle_observation", "operation ID, event sequence, and reason must be present together")
	}
	if strings.ContainsRune(o.OperationID, '\x00') || strings.ContainsRune(o.Reason, '\x00') {
		return NewError(CodeInvalidArgument, "lifecycle_observation", "must not contain NUL")
	}
	return nil
}

const (
	// ConditionFailure marks an operation or reconciliation failure at the current phase.
	ConditionFailure = "Failure"
	// ConditionCleanupPending marks owned resources whose removal has not been verified.
	ConditionCleanupPending = "CleanupPending"
	// ConditionOutcomeUnknown marks a stopped Attempt whose terminal result evidence is incomplete.
	ConditionOutcomeUnknown = "OutcomeUnknown"
	// ConditionProcessIdentityUnknown marks a process that cannot be safely controlled.
	ConditionProcessIdentityUnknown = "ProcessIdentityUnknown"
)

// Validate rejects ambiguous or unpersistable condition identity and diagnostics.
func (c Condition) Validate() error {
	if strings.TrimSpace(c.Type) == "" || strings.TrimSpace(c.Reason) == "" {
		return NewError(CodeInvalidArgument, "condition", "type and reason must be non-empty")
	}
	if strings.ContainsRune(c.Type, '\x00') || strings.ContainsRune(c.Reason, '\x00') || strings.ContainsRune(c.Message, '\x00') {
		return NewError(CodeInvalidArgument, "condition", "must not contain NUL")
	}
	return nil
}

// ProcessIdentity carries strong opaque ownership evidence and deliberately has no PID field.
type ProcessIdentity struct {
	Verified bool   `json:"verified"`
	Handle   string `json:"handle,omitempty"`
	Evidence string `json:"evidence,omitempty"`
}

// Validate checks persisted evidence shape; an action-time provider must still reverify ownership before signaling.
func (i ProcessIdentity) Validate() error {
	if !i.Verified || strings.TrimSpace(i.Handle) == "" || strings.TrimSpace(i.Evidence) == "" ||
		strings.ContainsRune(i.Handle, '\x00') || strings.ContainsRune(i.Evidence, '\x00') {
		return NewError(CodeUnsafeIdentity, "process_identity", "requires a verified strong handle and independent ownership evidence")
	}
	return nil
}

// EvidenceState represents an independently observed three-state fact such as cgroup OOM evidence.
type EvidenceState string

const (
	// EvidenceUnknown means the relevant evidence was not captured or cannot be trusted.
	EvidenceUnknown EvidenceState = "unknown"
	// EvidenceFalse means evidence was captured and the fact did not occur.
	EvidenceFalse EvidenceState = "false"
	// EvidenceTrue means evidence was captured and the fact occurred.
	EvidenceTrue EvidenceState = "true"
)

// Valid reports whether the evidence value belongs to the explicit three-state domain.
func (e EvidenceState) Valid() bool {
	return e == EvidenceUnknown || e == EvidenceFalse || e == EvidenceTrue
}

// OutcomePresence distinguishes pending, not-run, captured, and unknown terminal results.
type OutcomePresence string

const (
	// OutcomePending means an Attempt has not reached a terminal result.
	OutcomePending OutcomePresence = "pending"
	// OutcomeNotApplicable means preparation ended before a workload process ran.
	OutcomeNotApplicable OutcomePresence = "not_applicable"
	// OutcomeCaptured means terminal exit or signal facts were verified.
	OutcomeCaptured OutcomePresence = "captured"
	// OutcomeUnknown means trustworthy terminal result evidence is incomplete.
	OutcomeUnknown OutcomePresence = "unknown"
)

// Outcome stores terminal facts with explicit presence and independent OOM evidence.
// Wall timestamps are diagnostic facts; RunningDuration is the same-process monotonic sample.
type Outcome struct {
	Presence        OutcomePresence `json:"presence"`
	ExitCode        *int32          `json:"exit_code,omitempty"`
	Signal          string          `json:"signal,omitempty"`
	OOM             EvidenceState   `json:"oom"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	FinishedAt      *time.Time      `json:"finished_at,omitempty"`
	RunningDuration *time.Duration  `json:"running_duration,omitempty"`
}

// PendingOutcome constructs the only valid outcome for a non-terminal Attempt.
func PendingOutcome() Outcome { return Outcome{Presence: OutcomePending, OOM: EvidenceUnknown} }

// NotApplicableOutcome constructs a terminal outcome for an Attempt whose workload never ran.
func NotApplicableOutcome() Outcome {
	return Outcome{Presence: OutcomeNotApplicable, OOM: EvidenceUnknown}
}

// UnknownOutcome constructs a terminal outcome that needs an OutcomeUnknown condition.
func UnknownOutcome(oom EvidenceState) Outcome { return Outcome{Presence: OutcomeUnknown, OOM: oom} }

// ExitOutcome constructs a captured normal-exit candidate while retaining zero; Validate confirms consistency before use.
func ExitOutcome(code int32, oom EvidenceState, startedAt, finishedAt time.Time, duration time.Duration) Outcome {
	return Outcome{Presence: OutcomeCaptured, ExitCode: &code, OOM: oom, StartedAt: &startedAt, FinishedAt: &finishedAt, RunningDuration: &duration}
}

// SignalOutcome constructs a captured signal-exit candidate; Validate confirms its evidence and it authorizes no signal.
func SignalOutcome(signal string, oom EvidenceState, startedAt, finishedAt time.Time, duration time.Duration) Outcome {
	return Outcome{Presence: OutcomeCaptured, Signal: signal, OOM: oom, StartedAt: &startedAt, FinishedAt: &finishedAt, RunningDuration: &duration}
}

// Known reports only whether captured presence was selected; callers use Validate before trusting its fields.
func (o Outcome) Known() bool { return o.Presence == OutcomeCaptured }

// Clone returns an Outcome whose optional facts cannot be mutated through pointer aliases.
func (o Outcome) Clone() Outcome {
	clone := o
	clone.ExitCode = cloneInt32(o.ExitCode)
	clone.StartedAt = cloneTime(o.StartedAt)
	clone.FinishedAt = cloneTime(o.FinishedAt)
	clone.RunningDuration = cloneDuration(o.RunningDuration)
	return clone
}

// Validate enforces presence, mutually exclusive exit facts, independent OOM state, and monotonic duration semantics.
func (o Outcome) Validate() error {
	if !o.OOM.Valid() {
		return NewError(CodeInvalidArgument, "outcome.oom", "must explicitly be unknown, false, or true")
	}
	if o.RunningDuration != nil && *o.RunningDuration < 0 {
		return NewError(CodeInvalidArgument, "outcome.running_duration", "must not be negative")
	}
	switch o.Presence {
	case OutcomePending:
		if o.ExitCode != nil || o.Signal != "" || o.FinishedAt != nil || o.RunningDuration != nil || o.OOM != EvidenceUnknown {
			return NewError(CodeInvalidArgument, "outcome", "pending outcome must not invent terminal facts")
		}
	case OutcomeNotApplicable:
		if o.ExitCode != nil || o.Signal != "" || o.StartedAt != nil || o.FinishedAt != nil || o.RunningDuration != nil || o.OOM != EvidenceUnknown {
			return NewError(CodeInvalidArgument, "outcome", "not-applicable outcome must not contain process facts")
		}
	case OutcomeCaptured:
		if (o.ExitCode == nil) == (o.Signal == "") {
			return NewError(CodeInvalidArgument, "outcome", "captured outcome requires exactly one exit code or signal")
		}
		if o.ExitCode != nil && *o.ExitCode < 0 {
			return NewError(CodeInvalidArgument, "outcome.exit_code", "must not be negative")
		}
		if o.Signal != "" && (strings.TrimSpace(o.Signal) == "" || strings.ContainsRune(o.Signal, '\x00')) {
			return NewError(CodeInvalidArgument, "outcome.signal", "must be a persistence-safe signal name")
		}
		if o.StartedAt == nil || o.FinishedAt == nil || o.RunningDuration == nil {
			return NewError(CodeInvalidArgument, "outcome", "captured result requires start, finish, and duration evidence")
		}
	case OutcomeUnknown:
		if o.ExitCode != nil || o.Signal != "" || o.RunningDuration != nil {
			return NewError(CodeInvalidArgument, "outcome", "unknown result must not invent exit or duration facts")
		}
	default:
		return NewError(CodeInvalidArgument, "outcome.presence", "must be explicit")
	}
	return nil
}

// cloneInt32 copies an optional exit code for immutable status projections.
func cloneInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

// cloneTime copies an optional wall-clock fact for immutable status projections.
func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

// cloneDuration copies an optional monotonic duration sample for immutable status projections.
func cloneDuration(value *time.Duration) *time.Duration {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

// StreamReferences names persisted stream endpoints without owning host descriptors.
type StreamReferences struct {
	Stdin  string `json:"stdin,omitempty"`
	Stdout string `json:"stdout,omitempty"`
	Stderr string `json:"stderr,omitempty"`
}

// Validate rejects stream references that cannot be safely stored for a later runtime provider.
func (s StreamReferences) Validate() error {
	if strings.ContainsRune(s.Stdin, '\x00') || strings.ContainsRune(s.Stdout, '\x00') || strings.ContainsRune(s.Stderr, '\x00') {
		return NewError(CodeInvalidArgument, "streams", "must not contain NUL")
	}
	return nil
}

// SandboxStatus stores lifecycle observation, immutable spec generations, and current pair references.
type SandboxStatus struct {
	Phase              SandboxPhase         `json:"phase"`
	Generation         Generation           `json:"generation"`
	ObservedGeneration Generation           `json:"observed_generation"`
	Conditions         []Condition          `json:"conditions,omitempty"`
	CurrentContainerID *ContainerID         `json:"current_container_id,omitempty"`
	CurrentAttemptID   *AttemptID           `json:"current_attempt_id,omitempty"`
	LastObservation    LifecycleObservation `json:"last_observation"`
}

// Clone returns status with independent conditions and current-reference pointers.
func (s SandboxStatus) Clone() SandboxStatus {
	clone := s
	clone.Conditions = append([]Condition(nil), s.Conditions...)
	clone.CurrentContainerID = cloneContainerID(s.CurrentContainerID)
	clone.CurrentAttemptID = cloneAttemptID(s.CurrentAttemptID)
	return clone
}

// Sandbox is the stable workload-environment aggregate; store CAS revision remains outside this model.
type Sandbox struct {
	SchemaVersion uint32        `json:"schema_version"`
	ID            SandboxID     `json:"id"`
	Spec          SandboxSpec   `json:"spec"`
	Status        SandboxStatus `json:"status"`
}

// NewSandbox validates and clones create input into generation-one creating intent.
func NewSandbox(id SandboxID, spec SandboxSpec) (Sandbox, error) {
	sandbox := Sandbox{
		SchemaVersion: SchemaVersion,
		ID:            id,
		Spec:          spec.Clone(),
		Status: SandboxStatus{
			Phase: SandboxCreating, Generation: InitialGeneration,
		},
	}
	if err := sandbox.Validate(); err != nil {
		return Sandbox{}, err
	}
	return sandbox, nil
}

// Clone returns a Sandbox that can cross transaction boundaries without mutable aliases.
func (s Sandbox) Clone() Sandbox {
	clone := s
	clone.Spec = s.Spec.Clone()
	clone.Status = s.Status.Clone()
	return clone
}

// Validate enforces schema, immutable generation, observation, current references, and spec invariants.
func (s Sandbox) Validate() error {
	if s.SchemaVersion != SchemaVersion {
		return NewError(CodeInvalidArgument, "schema_version", "is unsupported")
	}
	if err := s.ID.Validate(); err != nil {
		return err
	}
	if err := s.Spec.Validate(); err != nil {
		return err
	}
	if !s.Status.Phase.Valid() {
		return NewError(CodeInvalidArgument, "sandbox.phase", "is not recognized")
	}
	if s.Status.Generation != InitialGeneration || s.Status.ObservedGeneration > s.Status.Generation {
		return NewError(CodeInvalidArgument, "sandbox.generation", "M1 create generation must be one and observed generation must not exceed it")
	}
	if s.Status.Phase == SandboxReady && s.Status.ObservedGeneration != s.Status.Generation {
		return NewError(CodeInvalidArgument, "sandbox.observed_generation", "Ready requires reconciled create input")
	}
	if (s.Status.CurrentContainerID == nil) != (s.Status.CurrentAttemptID == nil) {
		return NewError(CodeInvalidArgument, "sandbox.current", "Container and Attempt references must change atomically")
	}
	if s.Status.CurrentContainerID != nil {
		if err := s.Status.CurrentContainerID.Validate(); err != nil {
			return err
		}
		if err := s.Status.CurrentAttemptID.Validate(); err != nil {
			return err
		}
	}
	if err := s.Status.LastObservation.Validate(); err != nil {
		return err
	}
	return validateConditions(s.Status.Conditions)
}

// Transition applies a legal Sandbox edge and observes generation only after explicit Ready confirmation.
func (s *Sandbox) Transition(next SandboxPhase) error {
	if s == nil {
		return NewError(CodeInvalidArgument, "sandbox", "must not be nil")
	}
	phase, err := TransitionSandbox(s.Status.Phase, next)
	if err != nil {
		return err
	}
	previous := s.Status.Clone()
	s.Status.Phase = phase
	if next == SandboxReady {
		s.Status.ObservedGeneration = s.Status.Generation
	}
	if err := s.Validate(); err != nil {
		s.Status = previous
		return err
	}
	return nil
}

// SetCurrentPair atomically advances the Sandbox's current execution references after pair creation.
func (s *Sandbox) SetCurrentPair(containerID ContainerID, attemptID AttemptID) error {
	if s == nil {
		return NewError(CodeInvalidArgument, "sandbox", "must not be nil")
	}
	if err := containerID.Validate(); err != nil {
		return err
	}
	if err := attemptID.Validate(); err != nil {
		return err
	}
	s.Status.CurrentContainerID = &containerID
	s.Status.CurrentAttemptID = &attemptID
	return s.Validate()
}

// ClearCurrentPair removes current references only when they still identify the pair being deleted.
func (s *Sandbox) ClearCurrentPair(containerID ContainerID, attemptID AttemptID) error {
	if s == nil {
		return NewError(CodeInvalidArgument, "sandbox", "must not be nil")
	}
	if s.Status.CurrentContainerID == nil || s.Status.CurrentAttemptID == nil ||
		*s.Status.CurrentContainerID != containerID || *s.Status.CurrentAttemptID != attemptID {
		return NewError(CodeFailedPrecondition, "sandbox.current", "does not identify the requested pair")
	}
	s.Status.CurrentContainerID = nil
	s.Status.CurrentAttemptID = nil
	return s.Validate()
}

// SetLastObservation projects the latest durable operation event onto Sandbox query state.
func (s *Sandbox) SetLastObservation(observation LifecycleObservation) error {
	if s == nil {
		return NewError(CodeInvalidArgument, "sandbox", "must not be nil")
	}
	if err := observation.Validate(); err != nil {
		return err
	}
	if observation.EventSequence < s.Status.LastObservation.EventSequence {
		return NewError(CodeInvalidTransition, "sandbox.last_observation", "event sequence must not regress")
	}
	s.Status.LastObservation = observation
	return s.Validate()
}

// SetConditions atomically replaces Sandbox reconciliation facts after validating the complete candidate aggregate.
func (s *Sandbox) SetConditions(conditions []Condition) error {
	if s == nil {
		return NewError(CodeInvalidArgument, "sandbox", "must not be nil")
	}
	if err := validateConditions(conditions); err != nil {
		return err
	}
	candidate := s.Clone()
	candidate.Status.Conditions = append([]Condition(nil), conditions...)
	if err := candidate.Validate(); err != nil {
		return err
	}
	*s = candidate
	return nil
}

// UpsertCondition atomically adds or replaces one Sandbox condition without overwriting unrelated reconcilers' facts.
func (s *Sandbox) UpsertCondition(condition Condition) error {
	if s == nil {
		return NewError(CodeInvalidArgument, "sandbox", "must not be nil")
	}
	if err := condition.Validate(); err != nil {
		return err
	}
	return s.SetConditions(upsertCondition(s.Status.Conditions, condition))
}

// ClearCondition atomically removes one Sandbox condition type while preserving every other status fact.
func (s *Sandbox) ClearCondition(conditionType string) error {
	if s == nil {
		return NewError(CodeInvalidArgument, "sandbox", "must not be nil")
	}
	conditions, err := withoutCondition(s.Status.Conditions, conditionType)
	if err != nil {
		return err
	}
	return s.SetConditions(conditions)
}

// ContainerSpec is immutable execution input with resolved enforcement limits from its Sandbox.
type ContainerSpec struct {
	Process     ProcessSpec            `json:"process"`
	ImageDigest string                 `json:"image_digest,omitempty"`
	RootFS      string                 `json:"rootfs,omitempty"`
	Limits      ResolvedResourceLimits `json:"limits"`
}

// Clone returns Container input whose process slices and optional limits are independent.
func (s ContainerSpec) Clone() ContainerSpec {
	clone := s
	clone.Process = s.Process.Clone()
	clone.Limits = s.Limits.Clone()
	return clone
}

// Validate checks structured process input, persistence-safe future image/rootfs identifiers, and explicit resolved limits.
func (s ContainerSpec) Validate() error {
	if err := s.Process.Validate(); err != nil {
		return err
	}
	if strings.ContainsRune(s.ImageDigest, '\x00') || strings.ContainsRune(s.RootFS, '\x00') {
		return NewError(CodeInvalidArgument, "container.spec", "image digest and rootfs must contain no NUL")
	}
	return s.Limits.Validate()
}

// ContainerStatus atomically projects the canonical Attempt phase, facts, and immutable generation.
type ContainerStatus struct {
	Phase              AttemptPhase         `json:"phase"`
	Generation         Generation           `json:"generation"`
	ObservedGeneration Generation           `json:"observed_generation"`
	Conditions         []Condition          `json:"conditions,omitempty"`
	ProcessIdentity    *ProcessIdentity     `json:"process_identity,omitempty"`
	Streams            StreamReferences     `json:"streams"`
	Outcome            Outcome              `json:"outcome"`
	LastObservation    LifecycleObservation `json:"last_observation"`
}

// Clone returns projected status without sharing conditions, process identity, or outcome pointers.
func (s ContainerStatus) Clone() ContainerStatus {
	clone := s
	clone.Conditions = append([]Condition(nil), s.Conditions...)
	clone.ProcessIdentity = cloneProcessIdentity(s.ProcessIdentity)
	clone.Outcome = s.Outcome.Clone()
	return clone
}

// Container is the API-visible half of an immutable one-to-one execution aggregate.
type Container struct {
	SchemaVersion uint32          `json:"schema_version"`
	ID            ContainerID     `json:"id"`
	SandboxID     SandboxID       `json:"sandbox_id"`
	AttemptID     AttemptID       `json:"attempt_id"`
	Spec          ContainerSpec   `json:"spec"`
	Status        ContainerStatus `json:"status"`
}

// Clone returns Container spec and projected status without mutable aliases.
func (c Container) Clone() Container {
	clone := c
	clone.Spec = c.Spec.Clone()
	clone.Status = c.Status.Clone()
	return clone
}

// Attempt is the kernel-facing execution authority, although M1 performs no host operation.
type Attempt struct {
	SchemaVersion   uint32               `json:"schema_version"`
	ID              AttemptID            `json:"id"`
	SandboxID       SandboxID            `json:"sandbox_id"`
	ContainerID     ContainerID          `json:"container_id"`
	Phase           AttemptPhase         `json:"phase"`
	Conditions      []Condition          `json:"conditions,omitempty"`
	ProcessIdentity *ProcessIdentity     `json:"process_identity,omitempty"`
	Streams         StreamReferences     `json:"streams"`
	Outcome         Outcome              `json:"outcome"`
	LastObservation LifecycleObservation `json:"last_observation"`
}

// Clone returns canonical Attempt facts without mutable pointer or slice aliases.
func (a Attempt) Clone() Attempt {
	clone := a
	clone.Conditions = append([]Condition(nil), a.Conditions...)
	clone.ProcessIdentity = cloneProcessIdentity(a.ProcessIdentity)
	clone.Outcome = a.Outcome.Clone()
	return clone
}

// ContainerAttempt persists exactly one Container and exactly one Attempt as an atomic aggregate.
type ContainerAttempt struct {
	Container Container `json:"container"`
	Attempt   Attempt   `json:"attempt"`
}

// NewContainerAttempt creates generation-one, creating intent without claiming host resources exist.
func NewContainerAttempt(
	sandbox Sandbox,
	containerID ContainerID,
	attemptID AttemptID,
	process ProcessSpec,
	imageDigest string,
	rootFS string,
) (ContainerAttempt, error) {
	if err := sandbox.Validate(); err != nil {
		return ContainerAttempt{}, err
	}
	if sandbox.Status.Phase != SandboxReady || sandbox.Status.ObservedGeneration != sandbox.Status.Generation {
		return ContainerAttempt{}, NewError(CodeFailedPrecondition, "sandbox.phase", "must be reconciled Ready")
	}
	if err := containerID.Validate(); err != nil {
		return ContainerAttempt{}, err
	}
	if err := attemptID.Validate(); err != nil {
		return ContainerAttempt{}, err
	}
	limits, err := ResolveResourceLimits(sandbox.Spec.Resources)
	if err != nil {
		return ContainerAttempt{}, err
	}
	spec := ContainerSpec{
		Process: process.Clone(), ImageDigest: imageDigest, RootFS: rootFS,
		Limits: limits,
	}
	pair := ContainerAttempt{
		Container: Container{
			SchemaVersion: SchemaVersion, ID: containerID, SandboxID: sandbox.ID, AttemptID: attemptID,
			Spec:   spec,
			Status: ContainerStatus{Phase: AttemptCreating, Generation: InitialGeneration, Outcome: PendingOutcome()},
		},
		Attempt: Attempt{
			SchemaVersion: SchemaVersion, ID: attemptID, SandboxID: sandbox.ID, ContainerID: containerID,
			Phase: AttemptCreating, Outcome: PendingOutcome(),
		},
	}
	if err := pair.Validate(); err != nil {
		return ContainerAttempt{}, err
	}
	return pair, nil
}

// Clone returns both sides of the pair without sharing nested execution data.
func (p ContainerAttempt) Clone() ContainerAttempt {
	return ContainerAttempt{Container: p.Container.Clone(), Attempt: p.Attempt.Clone()}
}

// Validate enforces schema, bidirectional identity, generation, canonical Attempt, and status projection invariants.
func (p ContainerAttempt) Validate() error {
	if p.Container.SchemaVersion != SchemaVersion || p.Attempt.SchemaVersion != SchemaVersion {
		return NewError(CodeInvalidArgument, "container_attempt.schema_version", "is unsupported")
	}
	if err := p.Container.ID.Validate(); err != nil {
		return err
	}
	if err := p.Attempt.ID.Validate(); err != nil {
		return err
	}
	if err := p.Container.SandboxID.Validate(); err != nil {
		return err
	}
	if p.Container.AttemptID != p.Attempt.ID || p.Attempt.ContainerID != p.Container.ID ||
		p.Attempt.SandboxID != p.Container.SandboxID {
		return NewError(CodeInvalidArgument, "container_attempt.identity", "Container, Attempt, and Sandbox references must be bidirectionally consistent")
	}
	if err := p.Container.Spec.Validate(); err != nil {
		return err
	}
	if !p.Attempt.Phase.Valid() || p.Container.Status.Phase != p.Attempt.Phase {
		return NewError(CodeInvalidArgument, "container_attempt.phase", "Container projection must match a valid Attempt phase")
	}
	if p.Container.Status.Generation != InitialGeneration || p.Container.Status.ObservedGeneration > p.Container.Status.Generation {
		return NewError(CodeInvalidArgument, "container.generation", "M1 generation must remain one")
	}
	if p.Attempt.Phase == AttemptCreating && p.Container.Status.ObservedGeneration != 0 {
		return NewError(CodeInvalidArgument, "container.observed_generation", "creating input is not yet reconciled")
	}
	if p.Attempt.Phase == AttemptCreated || p.Attempt.Phase == AttemptRunning {
		if p.Container.Status.ObservedGeneration != p.Container.Status.Generation {
			return NewError(CodeInvalidArgument, "container.observed_generation", "created or running requires reconciled input")
		}
	}
	if err := p.Attempt.Outcome.Validate(); err != nil {
		return err
	}
	if err := p.Attempt.Streams.Validate(); err != nil {
		return err
	}
	if err := p.Attempt.LastObservation.Validate(); err != nil {
		return err
	}
	if err := validateConditions(p.Attempt.Conditions); err != nil {
		return err
	}
	if IsActiveAttempt(p.Attempt.Phase) && p.Attempt.Outcome.Presence != OutcomePending {
		return NewError(CodeInvalidArgument, "attempt.outcome", "active Attempt requires pending outcome")
	}
	if p.Attempt.Phase == AttemptStopped && p.Attempt.Outcome.Presence == OutcomePending {
		return NewError(CodeInvalidArgument, "attempt.outcome", "stopped Attempt requires explicit terminal presence")
	}
	if p.Attempt.Phase == AttemptRunning {
		if p.Attempt.ProcessIdentity == nil || p.Attempt.ProcessIdentity.Validate() != nil {
			return NewError(CodeUnsafeIdentity, "attempt.process_identity", "running requires verified strong process identity")
		}
	}
	if p.Attempt.Outcome.Presence == OutcomeUnknown && !hasCondition(p.Attempt.Conditions, ConditionOutcomeUnknown) {
		return NewError(CodeOutcomeUnknown, "attempt.conditions", "unknown outcome requires an OutcomeUnknown condition")
	}
	if !projectedStatusEqual(p.Container.Status, p.Attempt) {
		return NewError(CodeInvalidArgument, "container.status", "must atomically project canonical Attempt facts")
	}
	return nil
}

// Transition applies one legal Attempt edge and atomically refreshes the Container projection.
func (p *ContainerAttempt) Transition(next AttemptPhase, outcome Outcome) error {
	if p == nil {
		return NewError(CodeInvalidArgument, "container_attempt", "must not be nil")
	}
	phase, err := TransitionAttempt(p.Attempt.Phase, next)
	if err != nil {
		return err
	}
	if err := validateTransitionOutcome(p.Attempt.Phase, next, outcome); err != nil {
		return err
	}
	previous := p.Clone()
	p.Attempt.Phase = phase
	p.Attempt.Outcome = outcome.Clone()
	if next == AttemptCreated {
		p.Container.Status.ObservedGeneration = p.Container.Status.Generation
	}
	p.projectAttempt()
	if err := p.Validate(); err != nil {
		*p = previous
		return err
	}
	return nil
}

// SetProcessIdentity records externally verified ownership evidence without interpreting or signaling a PID.
func (p *ContainerAttempt) SetProcessIdentity(identity ProcessIdentity) error {
	if p == nil {
		return NewError(CodeInvalidArgument, "container_attempt", "must not be nil")
	}
	if err := identity.Validate(); err != nil {
		return err
	}
	if p.Attempt.ProcessIdentity != nil {
		if *p.Attempt.ProcessIdentity != identity {
			return NewError(CodeFailedPrecondition, "attempt.process_identity", "is immutable for one Attempt")
		}
		return nil
	}
	if p.Attempt.Phase == AttemptStopped || p.Attempt.Phase == AttemptAbsent {
		return NewError(CodeFailedPrecondition, "attempt.phase", "cannot add process identity after the Attempt is terminal")
	}
	p.Attempt.ProcessIdentity = cloneProcessIdentity(&identity)
	p.projectAttempt()
	return p.Validate()
}

// validateTransitionOutcome prevents lifecycle edges from contradicting whether a workload ever ran.
func validateTransitionOutcome(from, to AttemptPhase, outcome Outcome) error {
	if err := outcome.Validate(); err != nil {
		return err
	}
	if to != AttemptStopped && to != AttemptAbsent {
		if outcome.Presence != OutcomePending {
			return NewError(CodeInvalidArgument, "attempt.outcome", "non-terminal transition requires pending outcome")
		}
		return nil
	}
	switch from {
	case AttemptCreating, AttemptCreated:
		if outcome.Presence != OutcomeNotApplicable && outcome.Presence != OutcomeUnknown {
			return NewError(CodeInvalidArgument, "attempt.outcome", "workload that never ran requires not-applicable or unknown outcome")
		}
	case AttemptRunning:
		if outcome.Presence != OutcomeCaptured && outcome.Presence != OutcomeUnknown {
			return NewError(CodeInvalidArgument, "attempt.outcome", "running workload requires captured or unknown terminal outcome")
		}
	case AttemptStopped:
		if outcome.Presence == OutcomePending {
			return NewError(CodeInvalidArgument, "attempt.outcome", "removal must preserve the terminal outcome")
		}
	}
	return nil
}

// SetStreams records provider-issued stream references and refreshes their Container projection.
func (p *ContainerAttempt) SetStreams(streams StreamReferences) error {
	if p == nil {
		return NewError(CodeInvalidArgument, "container_attempt", "must not be nil")
	}
	if err := streams.Validate(); err != nil {
		return err
	}
	p.Attempt.Streams = streams
	p.projectAttempt()
	return p.Validate()
}

// SetConditions atomically replaces canonical lifecycle conditions for lifecycle reconcilers and refreshes their Container projection.
// It validates a cloned candidate first so a rejected cross-field invariant never mutates the caller's aggregate.
func (p *ContainerAttempt) SetConditions(conditions []Condition) error {
	if p == nil {
		return NewError(CodeInvalidArgument, "container_attempt", "must not be nil")
	}
	if err := validateConditions(conditions); err != nil {
		return err
	}
	candidate := p.Clone()
	candidate.Attempt.Conditions = append([]Condition(nil), conditions...)
	candidate.projectAttempt()
	if err := candidate.Validate(); err != nil {
		return err
	}
	*p = candidate
	return nil
}

// UpsertCondition atomically adds or replaces one Attempt condition and refreshes the Container projection.
func (p *ContainerAttempt) UpsertCondition(condition Condition) error {
	if p == nil {
		return NewError(CodeInvalidArgument, "container_attempt", "must not be nil")
	}
	if err := condition.Validate(); err != nil {
		return err
	}
	return p.SetConditions(upsertCondition(p.Attempt.Conditions, condition))
}

// ClearCondition atomically removes one Attempt condition type and rejects removal that would violate terminal evidence rules.
func (p *ContainerAttempt) ClearCondition(conditionType string) error {
	if p == nil {
		return NewError(CodeInvalidArgument, "container_attempt", "must not be nil")
	}
	conditions, err := withoutCondition(p.Attempt.Conditions, conditionType)
	if err != nil {
		return err
	}
	return p.SetConditions(conditions)
}

// SetLastObservation projects the latest durable operation event onto both execution records.
func (p *ContainerAttempt) SetLastObservation(observation LifecycleObservation) error {
	if p == nil {
		return NewError(CodeInvalidArgument, "container_attempt", "must not be nil")
	}
	if err := observation.Validate(); err != nil {
		return err
	}
	if observation.EventSequence < p.Attempt.LastObservation.EventSequence {
		return NewError(CodeInvalidTransition, "attempt.last_observation", "event sequence must not regress")
	}
	p.Attempt.LastObservation = observation
	p.projectAttempt()
	return p.Validate()
}

// ValidateOneActiveAttempt rejects invalid history or more than one active pair per Sandbox.
func ValidateOneActiveAttempt(pairs []ContainerAttempt) error {
	active := make(map[SandboxID]ContainerID)
	containers := make(map[ContainerID]struct{})
	attempts := make(map[AttemptID]struct{})
	for index := range pairs {
		pair := pairs[index]
		if err := pair.Validate(); err != nil {
			return WrapError(CodeInvalidArgument, fmt.Sprintf("pairs[%d]", index), "contains an invalid aggregate", err)
		}
		if _, exists := containers[pair.Container.ID]; exists {
			return NewError(CodeAlreadyExists, "container_id", "appears more than once")
		}
		if _, exists := attempts[pair.Attempt.ID]; exists {
			return NewError(CodeAlreadyExists, "attempt_id", "appears more than once")
		}
		containers[pair.Container.ID] = struct{}{}
		attempts[pair.Attempt.ID] = struct{}{}
		if !IsActiveAttempt(pair.Attempt.Phase) {
			continue
		}
		if first, exists := active[pair.Container.SandboxID]; exists {
			return NewError(CodeFailedPrecondition, "sandbox.active_attempt", fmt.Sprintf("containers %q and %q are both active", first, pair.Container.ID))
		}
		active[pair.Container.SandboxID] = pair.Container.ID
	}
	return nil
}

// projectAttempt copies the Attempt authority into the API-visible Container status without changing generation.
func (p *ContainerAttempt) projectAttempt() {
	observed := p.Container.Status.ObservedGeneration
	p.Container.Status = ContainerStatus{
		Phase: p.Attempt.Phase, Generation: p.Container.Status.Generation, ObservedGeneration: observed,
		Conditions:      append([]Condition(nil), p.Attempt.Conditions...),
		ProcessIdentity: cloneProcessIdentity(p.Attempt.ProcessIdentity),
		Streams:         p.Attempt.Streams, Outcome: p.Attempt.Outcome.Clone(),
		LastObservation: p.Attempt.LastObservation,
	}
}

// projectedStatusEqual compares every canonical Attempt projection without pointer-identity dependence.
func projectedStatusEqual(status ContainerStatus, attempt Attempt) bool {
	return status.Phase == attempt.Phase && status.Streams == attempt.Streams && status.LastObservation == attempt.LastObservation &&
		conditionsEqual(status.Conditions, attempt.Conditions) &&
		processIdentitiesEqual(status.ProcessIdentity, attempt.ProcessIdentity) &&
		outcomesEqual(status.Outcome, attempt.Outcome)
}

// validateConditions validates every condition and rejects duplicate condition types.
func validateConditions(conditions []Condition) error {
	seen := make(map[string]struct{}, len(conditions))
	for index, condition := range conditions {
		if err := condition.Validate(); err != nil {
			return WrapError(CodeInvalidArgument, fmt.Sprintf("conditions[%d]", index), "is invalid", err)
		}
		if _, exists := seen[condition.Type]; exists {
			return NewError(CodeInvalidArgument, fmt.Sprintf("conditions[%d]", index), "duplicates a condition type")
		}
		seen[condition.Type] = struct{}{}
	}
	return nil
}

// hasCondition reports whether a canonical condition type is present on the current phase.
func hasCondition(conditions []Condition, conditionType string) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			return true
		}
	}
	return false
}

// upsertCondition returns a caller-owned condition list with one type replaced in place or appended deterministically.
func upsertCondition(conditions []Condition, condition Condition) []Condition {
	result := append([]Condition(nil), conditions...)
	for index := range result {
		if result[index].Type == condition.Type {
			result[index] = condition
			return result
		}
	}
	return append(result, condition)
}

// withoutCondition returns a caller-owned list without one validated condition type.
func withoutCondition(conditions []Condition, conditionType string) ([]Condition, error) {
	if strings.TrimSpace(conditionType) == "" || strings.ContainsRune(conditionType, '\x00') {
		return nil, NewError(CodeInvalidArgument, "condition.type", "must be non-empty and contain no NUL")
	}
	result := make([]Condition, 0, len(conditions))
	for _, condition := range conditions {
		if condition.Type != conditionType {
			result = append(result, condition)
		}
	}
	return result, nil
}

// cloneProcessIdentity copies verified process evidence for immutable status projection.
func cloneProcessIdentity(identity *ProcessIdentity) *ProcessIdentity {
	if identity == nil {
		return nil
	}
	clone := *identity
	return &clone
}

// cloneContainerID copies an optional current Container reference without retaining aliases.
func cloneContainerID(id *ContainerID) *ContainerID {
	if id == nil {
		return nil
	}
	clone := *id
	return &clone
}

// cloneAttemptID copies an optional current Attempt reference without retaining aliases.
func cloneAttemptID(id *AttemptID) *AttemptID {
	if id == nil {
		return nil
	}
	clone := *id
	return &clone
}

// conditionsEqual compares ordered conditions used by the atomic status projection.
func conditionsEqual(left, right []Condition) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// processIdentitiesEqual compares strong ownership evidence without host-specific interpretation.
func processIdentitiesEqual(left, right *ProcessIdentity) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

// outcomesEqual compares explicit outcome presence and optional facts without pointer identity.
func outcomesEqual(left, right Outcome) bool {
	if left.Presence != right.Presence || left.Signal != right.Signal || left.OOM != right.OOM {
		return false
	}
	return int32PointersEqual(left.ExitCode, right.ExitCode) &&
		timePointersEqual(left.StartedAt, right.StartedAt) &&
		timePointersEqual(left.FinishedAt, right.FinishedAt) &&
		durationPointersEqual(left.RunningDuration, right.RunningDuration)
}

// int32PointersEqual compares optional exit codes by value.
func int32PointersEqual(left, right *int32) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

// timePointersEqual compares optional wall facts by instant rather than pointer identity.
func timePointersEqual(left, right *time.Time) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && left.Equal(*right))
}

// durationPointersEqual compares optional monotonic duration samples by value.
func durationPointersEqual(left, right *time.Duration) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}
