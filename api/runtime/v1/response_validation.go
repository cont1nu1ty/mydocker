package v1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

const (
	responseGeneration        uint64 = 1
	maximumEventResponseCount        = 500
	maximumEventResourceCount        = 16
	maximumEventDetailsBytes         = 64 << 10
	maximumLogResponseCount          = 100
	maximumLogPayloadBytes           = 16 << 20
)

// Validate rejects malformed or internally inconsistent Sandbox projections.
// Clients use it before trusting identity, lifecycle phase, or generation data.
func (s Sandbox) Validate() error {
	if err := (CreateSandboxRequest{SandboxID: s.ID, Spec: s.Spec}).Validate(); err != nil {
		return fmt.Errorf("invalid Sandbox projection: %w", err)
	}
	if s.Spec.Network.Mode != "none" && s.Spec.Network.Mode != "loopback" {
		return fmt.Errorf("Sandbox network mode %q is outside the v1 vocabulary", s.Spec.Network.Mode)
	}
	if len(s.Spec.Network.Attachments) != 0 {
		return errors.New("Sandbox network attachments are outside the v1 contract")
	}
	if !validSandboxPhase(s.Status.Phase) {
		return fmt.Errorf("invalid Sandbox phase %q", s.Status.Phase)
	}
	if s.Status.Generation != responseGeneration || s.Status.ObservedGeneration > s.Status.Generation {
		return errors.New("Sandbox generation must be one and observed generation must not exceed it")
	}
	if s.Status.Phase == "ready" && s.Status.ObservedGeneration != s.Status.Generation {
		return errors.New("Ready Sandbox must have observed its generation")
	}
	if (s.Status.CurrentContainerID == nil) != (s.Status.CurrentAttemptID == nil) {
		return errors.New("Sandbox current Container and Attempt identities must be present together")
	}
	if s.Status.CurrentContainerID != nil {
		if err := ValidateResourceID("current_container_id", *s.Status.CurrentContainerID); err != nil {
			return err
		}
		if err := ValidateResourceID("current_attempt_id", *s.Status.CurrentAttemptID); err != nil {
			return err
		}
	}
	if err := validateResponseConditions(s.Status.Conditions); err != nil {
		return err
	}
	return validateLifecycleObservation(s.Status.LastObservation)
}

// validSandboxPhase reports whether one projection uses the closed v1 Sandbox state vocabulary.
func validSandboxPhase(phase string) bool {
	switch phase {
	case "creating", "ready", "stopping", "stopped":
		return true
	default:
		return false
	}
}

// Validate rejects malformed or internally inconsistent Container Attempt projections.
// Clients use it before accepting returned execution identity or terminal facts.
func (c Container) Validate() error {
	if err := ValidateResourceID("container_id", c.ID); err != nil {
		return err
	}
	if err := ValidateResourceID("sandbox_id", c.SandboxID); err != nil {
		return err
	}
	if err := ValidateResourceID("attempt_id", c.AttemptID); err != nil {
		return err
	}
	if err := c.Spec.Process.Validate(); err != nil {
		return fmt.Errorf("invalid Container process projection: %w", err)
	}
	if err := validateOpaqueResponseID("rootfs", c.Spec.RootFS); err != nil {
		return err
	}
	if err := validateResolvedLimits(c.Spec.Limits); err != nil {
		return err
	}
	if !validContainerPhase(c.Status.Phase) {
		return fmt.Errorf("invalid Container phase %q", c.Status.Phase)
	}
	if c.Status.Generation != responseGeneration || c.Status.ObservedGeneration > c.Status.Generation {
		return errors.New("Container generation must be one and observed generation must not exceed it")
	}
	if c.Status.Phase == "creating" && c.Status.ObservedGeneration != 0 {
		return errors.New("Creating Container must not report an observed generation")
	}
	if (c.Status.Phase == "created" || c.Status.Phase == "running") && c.Status.ObservedGeneration != c.Status.Generation {
		return errors.New("Created or Running Container must have observed its generation")
	}
	if err := validateResponseConditions(c.Status.Conditions); err != nil {
		return err
	}
	if err := validateProcessIdentity(c.Status.ProcessIdentity); err != nil {
		return err
	}
	if c.Status.Phase == "running" && c.Status.ProcessIdentity == nil {
		return errors.New("Running Container requires verified process identity evidence")
	}
	if err := validateStreamReferences(c.Status.Streams); err != nil {
		return err
	}
	if err := validateOutcome(c.Status.Outcome); err != nil {
		return err
	}
	if c.Status.Phase != "stopped" && c.Status.Outcome.Presence != "pending" {
		return errors.New("non-terminal Container requires a pending outcome")
	}
	if c.Status.Phase == "stopped" && c.Status.Outcome.Presence == "pending" {
		return errors.New("Stopped Container requires an explicit terminal outcome")
	}
	if c.Status.Outcome.Presence == "unknown" && !hasResponseCondition(c.Status.Conditions, "OutcomeUnknown") {
		return errors.New("unknown Container outcome requires an OutcomeUnknown condition")
	}
	return validateLifecycleObservation(c.Status.LastObservation)
}

// validContainerPhase reports whether one projection uses the closed v1 Attempt state vocabulary.
func validContainerPhase(phase string) bool {
	switch phase {
	case "creating", "created", "running", "stopped":
		return true
	default:
		return false
	}
}

// validateOpaqueResponseID rejects host paths and unbounded provider identifiers leaked into a public projection.
func validateOpaqueResponseID(field, value string) error {
	if value == "" || len(value) > 256 || strings.ContainsAny(value, `/\`) {
		return fmt.Errorf("%s must be a non-empty bounded opaque identifier", field)
	}
	for _, character := range value {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return fmt.Errorf("%s must contain no whitespace or control characters", field)
		}
	}
	return nil
}

// validateResolvedLimits rejects ambiguous unlimited/value combinations and unenforceable numeric limits.
func validateResolvedLimits(limits ResolvedResourceLimits) error {
	if limits.CPUUnlimited {
		if limits.CPULimitMilli != nil {
			return errors.New("unlimited CPU must not also include a numeric limit")
		}
	} else if limits.CPULimitMilli == nil || *limits.CPULimitMilli < 10 {
		return errors.New("finite CPU limit must be at least 10 milli-CPU")
	}
	if limits.MemoryUnlimited {
		if limits.MemoryLimitBytes != nil {
			return errors.New("unlimited memory must not also include a numeric limit")
		}
	} else if limits.MemoryLimitBytes == nil || *limits.MemoryLimitBytes <= 0 {
		return errors.New("finite memory limit must be greater than zero")
	}
	if limits.PidsLimit <= 0 {
		return errors.New("pids limit must be greater than zero")
	}
	return nil
}

// validateResponseConditions enforces complete, unique condition identities without trusting free-form messages.
func validateResponseConditions(conditions []Condition) error {
	seen := make(map[string]struct{}, len(conditions))
	for index, condition := range conditions {
		if strings.TrimSpace(condition.Type) == "" || strings.TrimSpace(condition.Reason) == "" {
			return fmt.Errorf("conditions[%d] requires non-empty type and reason", index)
		}
		if strings.ContainsRune(condition.Type, '\x00') || strings.ContainsRune(condition.Reason, '\x00') || strings.ContainsRune(condition.Message, '\x00') {
			return fmt.Errorf("conditions[%d] contains NUL", index)
		}
		if _, exists := seen[condition.Type]; exists {
			return fmt.Errorf("conditions[%d] duplicates condition type %q", index, condition.Type)
		}
		seen[condition.Type] = struct{}{}
	}
	return nil
}

// hasResponseCondition reports whether a validated response contains one named condition type.
func hasResponseCondition(conditions []Condition, conditionType string) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			return true
		}
	}
	return false
}

// validateLifecycleObservation accepts the empty initial state or requires one complete bounded event reference.
func validateLifecycleObservation(observation LifecycleObservation) error {
	if observation.OperationID == "" && observation.EventSequence == 0 && observation.Reason == "" {
		return nil
	}
	if err := ValidateOperationID(observation.OperationID); err != nil {
		return err
	}
	if observation.EventSequence == 0 || !validOperationReason(observation.Reason) {
		return errors.New("lifecycle observation requires a sequence and bounded reason")
	}
	return nil
}

// validateProcessIdentity accepts an absent identity or requires complete, bounded strong evidence.
func validateProcessIdentity(identity *ProcessIdentity) error {
	if identity == nil {
		return nil
	}
	if !identity.Verified {
		return errors.New("process identity must be verified")
	}
	if err := validateOpaqueProjectionText("process identity handle", identity.Handle); err != nil {
		return err
	}
	return validateOpaqueProjectionText("process identity evidence", identity.Evidence)
}

// validateOpaqueProjectionText requires bounded non-whitespace evidence while permitting public colon prefixes.
func validateOpaqueProjectionText(field, value string) error {
	if value == "" || len(value) > 256 || strings.TrimSpace(value) != value {
		return fmt.Errorf("%s must be non-empty and no longer than 256 bytes", field)
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return fmt.Errorf("%s must contain no whitespace or control characters", field)
		}
	}
	return nil
}

// validateStreamReferences rejects unbounded or unsafe public stream tokens while allowing absent stdin.
func validateStreamReferences(streams StreamReferences) error {
	for _, stream := range []struct {
		name  string
		value string
	}{
		{name: "stdin", value: streams.Stdin},
		{name: "stdout", value: streams.Stdout},
		{name: "stderr", value: streams.Stderr},
	} {
		if stream.value == "" {
			continue
		}
		if err := validateOpaqueProjectionText("stream "+stream.name, stream.value); err != nil {
			return err
		}
	}
	return nil
}

// validateOutcome enforces explicit presence, mutually exclusive exit facts, and monotonic duration evidence.
func validateOutcome(outcome Outcome) error {
	if outcome.OOM != "unknown" && outcome.OOM != "false" && outcome.OOM != "true" {
		return fmt.Errorf("invalid OOM evidence %q", outcome.OOM)
	}
	if outcome.RunningDurationNanoseconds != nil && *outcome.RunningDurationNanoseconds < 0 {
		return errors.New("running duration must not be negative")
	}
	switch outcome.Presence {
	case "pending":
		if outcome.ExitCode != nil || outcome.Signal != "" || outcome.FinishedAt != nil || outcome.RunningDurationNanoseconds != nil || outcome.OOM != "unknown" {
			return errors.New("pending outcome must not contain terminal facts")
		}
	case "not_applicable":
		if outcome.ExitCode != nil || outcome.Signal != "" || outcome.StartedAt != nil || outcome.FinishedAt != nil || outcome.RunningDurationNanoseconds != nil || outcome.OOM != "unknown" {
			return errors.New("not-applicable outcome must not contain process facts")
		}
	case "captured":
		if (outcome.ExitCode == nil) == (outcome.Signal == "") {
			return errors.New("captured outcome requires exactly one exit code or signal")
		}
		if outcome.ExitCode != nil && *outcome.ExitCode < 0 {
			return errors.New("captured exit code must not be negative")
		}
		if outcome.Signal != "" && (strings.TrimSpace(outcome.Signal) != outcome.Signal || strings.ContainsRune(outcome.Signal, '\x00')) {
			return errors.New("captured signal must be a non-empty safe name")
		}
		if outcome.StartedAt == nil || outcome.FinishedAt == nil || outcome.RunningDurationNanoseconds == nil {
			return errors.New("captured outcome requires start, finish, and duration evidence")
		}
	case "unknown":
		if outcome.ExitCode != nil || outcome.Signal != "" || outcome.RunningDurationNanoseconds != nil {
			return errors.New("unknown outcome must not invent exit or duration facts")
		}
	default:
		return fmt.Errorf("invalid outcome presence %q", outcome.Presence)
	}
	return nil
}

// Validate rejects operations outside the closed v1 type, target, state, stage, result, and reason vocabularies.
func (o Operation) Validate() error {
	if err := ValidateOperationID(o.ID); err != nil {
		return err
	}
	if !validOperationType(o.Type) {
		return fmt.Errorf("invalid operation type %q", o.Type)
	}
	if err := o.Target.Validate(); err != nil {
		return err
	}
	if !validOperationTypeTarget(o.Type, o.Target.Kind) {
		return fmt.Errorf("operation type %q cannot target %q", o.Type, o.Target.Kind)
	}
	if o.Fingerprint.Version != 1 || len(o.Fingerprint.SHA256) != 64 {
		return errors.New("operation fingerprint must use v1 and a 64-character SHA-256 digest")
	}
	for _, character := range o.Fingerprint.SHA256 {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return errors.New("operation fingerprint must use lowercase hexadecimal")
		}
	}
	if !validOperationState(o.State) || !validOperationStage(o.Stage) || !validOperationResult(o.Result) || !validOperationReason(o.Reason) {
		return errors.New("operation contains an invalid state, stage, result, or reason")
	}
	active := o.State == "pending" || o.State == "running"
	terminal := o.State == "succeeded" || o.State == "failed"
	if active && o.Result != "pending" {
		return errors.New("active operation must have a pending result")
	}
	if o.State == "succeeded" && o.Result != "succeeded" && o.Result != "noop" {
		return errors.New("succeeded operation must have a succeeded or noop result")
	}
	if o.State == "failed" && o.Result != "failed" {
		return errors.New("failed operation must have a failed result")
	}
	if (o.Result == "failed") == (o.Reason == "none") {
		return errors.New("operation result and reason are inconsistent")
	}
	if o.State == "pending" && o.Stage != "validate" {
		return errors.New("pending operation must remain at validate")
	}
	if o.State == "running" && (o.Stage == "validate" || o.Stage == "complete") {
		return errors.New("running operation must use a resumable stage")
	}
	if terminal && o.Stage != "complete" {
		return errors.New("terminal operation must use complete stage")
	}
	if len(o.Response) > 0 && !json.Valid(o.Response) {
		return errors.New("operation response must contain valid JSON")
	}
	return nil
}

// Validate rejects unsupported resource kinds or malformed public identities.
func (r ResourceRef) Validate() error {
	if r.Kind != "sandbox" && r.Kind != "container" && r.Kind != "attempt" {
		return fmt.Errorf("invalid resource kind %q", r.Kind)
	}
	return ValidateResourceID(r.Kind+"_id", r.ID)
}

// validOperationType reports whether a lifecycle verb belongs to the closed v1 operation vocabulary.
func validOperationType(operationType string) bool {
	switch operationType {
	case "create", "start", "state", "kill", "stop", "delete":
		return true
	default:
		return false
	}
}

// validOperationTypeTarget reports whether a v1 lifecycle verb can address the selected resource kind.
func validOperationTypeTarget(operationType, targetKind string) bool {
	switch operationType {
	case "create":
		return targetKind == "sandbox" || targetKind == "container"
	case "start":
		return targetKind == "container"
	case "state":
		return targetKind == "sandbox" || targetKind == "container" || targetKind == "attempt"
	case "kill":
		return targetKind == "container" || targetKind == "attempt"
	case "stop":
		return targetKind == "sandbox" || targetKind == "container"
	case "delete":
		return targetKind == "sandbox" || targetKind == "container"
	default:
		return false
	}
}

// validOperationState reports whether one durable state belongs to the closed v1 vocabulary.
func validOperationState(state string) bool {
	return state == "pending" || state == "running" || state == "succeeded" || state == "failed"
}

// validOperationStage reports whether one checkpoint belongs to the closed v1 lifecycle vocabulary.
func validOperationStage(stage string) bool {
	switch stage {
	case "validate", "persist_intent", "check_preconditions", "host_preflight", "prepare_cgroup",
		"prepare_start_gate", "prepare_streams", "create_process", "prepare_namespaces", "join_namespaces",
		"prepare_rootfs", "attach_cgroup", "release_start_gate", "signal_process", "observe_process",
		"teardown", "transition", "persist_state", "rollback", "persist_result", "complete":
		return true
	default:
		return false
	}
}

// validOperationResult reports whether one operation result belongs to the closed v1 vocabulary.
func validOperationResult(result string) bool {
	return result == "pending" || result == "succeeded" || result == "failed" || result == "noop"
}

// validOperationReason reports whether one low-cardinality reason belongs to the closed v1 vocabulary.
func validOperationReason(reason string) bool {
	switch reason {
	case "none", "invalid_request", "conflict", "not_found", "precondition", "internal", "cleanup":
		return true
	default:
		return false
	}
}

// Validate rejects incomplete Sandbox responses and requires an attached mutation to target the returned Sandbox.
func (r SandboxResponse) Validate() error {
	if err := r.Sandbox.Validate(); err != nil {
		return err
	}
	if r.Operation == nil {
		return nil
	}
	if err := r.Operation.Validate(); err != nil {
		return err
	}
	if r.Operation.Target.Kind != "sandbox" || r.Operation.Target.ID != r.Sandbox.ID {
		return errors.New("Sandbox response operation targets a different resource")
	}
	if r.Sandbox.Status.LastObservation.OperationID != r.Operation.ID {
		return errors.New("Sandbox response operation does not match its latest observation")
	}
	return nil
}

// Validate rejects incomplete, duplicate, or non-deterministically ordered Sandbox list responses.
func (r SandboxListResponse) Validate() error {
	previous := ""
	for index, sandbox := range r.Sandboxes {
		if err := sandbox.Validate(); err != nil {
			return fmt.Errorf("sandboxes[%d]: %w", index, err)
		}
		if index > 0 && sandbox.ID <= previous {
			return errors.New("Sandbox list must be strictly ordered by ID")
		}
		previous = sandbox.ID
	}
	return nil
}

// Validate rejects incomplete Container responses and requires an attached mutation to target the returned Container.
func (r ContainerResponse) Validate() error {
	if err := r.Container.Validate(); err != nil {
		return err
	}
	if r.Operation == nil {
		return nil
	}
	if err := r.Operation.Validate(); err != nil {
		return err
	}
	if r.Operation.Target.Kind != "container" || r.Operation.Target.ID != r.Container.ID {
		return errors.New("Container response operation targets a different resource")
	}
	if r.Container.Status.LastObservation.OperationID != r.Operation.ID {
		return errors.New("Container response operation does not match its latest observation")
	}
	return nil
}

// Validate rejects incomplete, duplicate, or non-deterministically ordered Container list responses.
func (r ContainerListResponse) Validate() error {
	previous := ""
	for index, container := range r.Containers {
		if err := container.Validate(); err != nil {
			return fmt.Errorf("containers[%d]: %w", index, err)
		}
		if index > 0 && container.ID <= previous {
			return errors.New("Container list must be strictly ordered by ID")
		}
		previous = container.ID
	}
	return nil
}

// Validate rejects an absent or malformed operation lookup or mutation result.
func (r OperationResponse) Validate() error {
	return r.Operation.Validate()
}

// Validate rejects malformed, unbounded, or internally inconsistent public event facts.
func (e Event) Validate() error {
	if e.Sequence == 0 {
		return errors.New("event sequence must be greater than zero")
	}
	if err := ValidateOperationID(e.OperationID); err != nil {
		return err
	}
	if !validOperationType(e.Type) {
		return fmt.Errorf("invalid event operation type %q", e.Type)
	}
	if err := e.Target.Validate(); err != nil {
		return err
	}
	if !validOperationTypeTarget(e.Type, e.Target.Kind) {
		return fmt.Errorf("event operation type %q cannot target %q", e.Type, e.Target.Kind)
	}
	if len(e.Resources) == 0 || len(e.Resources) > maximumEventResourceCount {
		return fmt.Errorf("event resources must contain from 1 through %d identities", maximumEventResourceCount)
	}
	foundTarget := false
	seen := make(map[ResourceRef]struct{}, len(e.Resources))
	for index, resource := range e.Resources {
		if err := resource.Validate(); err != nil {
			return fmt.Errorf("event resources[%d]: %w", index, err)
		}
		if _, duplicate := seen[resource]; duplicate {
			return fmt.Errorf("event resources[%d] duplicates %s/%s", index, resource.Kind, resource.ID)
		}
		seen[resource] = struct{}{}
		foundTarget = foundTarget || resource == e.Target
	}
	if !foundTarget {
		return errors.New("event resources must include the primary target")
	}
	if !validOperationStage(e.Stage) || !validOperationResult(e.Result) || !validOperationReason(e.Reason) {
		return errors.New("event contains an invalid stage, result, or reason")
	}
	if (e.Result == "failed") == (e.Reason == "none") {
		return errors.New("event result and reason are inconsistent")
	}
	if e.OccurredAt.IsZero() {
		return errors.New("event occurrence time must not be zero")
	}
	if e.DurationNanoseconds != nil && *e.DurationNanoseconds < 0 {
		return errors.New("event duration must not be negative")
	}
	if e.ObservedGeneration > e.Generation {
		return errors.New("event observed generation must not exceed generation")
	}
	if len(e.Details) > maximumEventDetailsBytes {
		return fmt.Errorf("event details must not exceed %d bytes", maximumEventDetailsBytes)
	}
	if len(e.Details) > 0 && !json.Valid(e.Details) {
		return errors.New("event details must contain valid JSON")
	}
	return nil
}

// Validate rejects oversized, unordered, or non-resumable event pages.
func (r EventListResponse) Validate() error {
	if len(r.Events) > maximumEventResponseCount {
		return fmt.Errorf("event page must not exceed %d entries", maximumEventResponseCount)
	}
	var previous uint64
	for index, event := range r.Events {
		if err := event.Validate(); err != nil {
			return fmt.Errorf("events[%d]: %w", index, err)
		}
		if index > 0 && event.Sequence != previous+1 {
			return errors.New("event page must be contiguous by sequence")
		}
		previous = event.Sequence
	}
	if len(r.Events) > 0 {
		if r.NextResumeToken != NewResumeToken(previous) {
			return errors.New("event resume token does not identify the last event")
		}
	} else if _, err := ParseResumeToken(r.NextResumeToken); err != nil {
		return fmt.Errorf("invalid empty-page event resume token: %w", err)
	}
	if r.HasMore && len(r.Events) == 0 {
		return errors.New("event page cannot advertise more results without an event")
	}
	return nil
}

// Validate rejects malformed public workload-log data before a client trusts its ordering evidence.
func (f LogFrame) Validate() error {
	if err := ValidateResourceID("container_id", f.ContainerID); err != nil {
		return err
	}
	if err := ValidateResourceID("attempt_id", f.AttemptID); err != nil {
		return err
	}
	if f.Stream != "stdout" && f.Stream != "stderr" {
		return fmt.Errorf("invalid workload log stream %q", f.Stream)
	}
	if f.Cursor == 0 || f.Sequence == 0 {
		return errors.New("workload log cursor and sequence must be greater than zero")
	}
	if len(f.Payload) == 0 || len(f.Payload) > maximumLogPayloadBytes {
		return fmt.Errorf("workload log payload must contain from 1 through %d bytes", maximumLogPayloadBytes)
	}
	digest := sha256.Sum256(f.Payload)
	if f.PayloadSHA256 != hex.EncodeToString(digest[:]) {
		return errors.New("workload log payload checksum does not match payload")
	}
	return nil
}

// Validate rejects oversized, cross-Attempt, unordered, or non-resumable workload-log pages.
func (r LogListResponse) Validate() error {
	if len(r.Frames) > maximumLogResponseCount {
		return fmt.Errorf("workload log page must not exceed %d frames", maximumLogResponseCount)
	}
	var containerID, attemptID string
	var previousCursor uint64
	streamSequences := make(map[string]uint64, 2)
	for index, frame := range r.Frames {
		if err := frame.Validate(); err != nil {
			return fmt.Errorf("frames[%d]: %w", index, err)
		}
		if index == 0 {
			containerID, attemptID = frame.ContainerID, frame.AttemptID
		} else if frame.ContainerID != containerID || frame.AttemptID != attemptID {
			return errors.New("workload log page mixes Container Attempt identities")
		}
		if index > 0 && frame.Cursor != previousCursor+1 {
			return errors.New("workload log page must be contiguous by cursor")
		}
		if previous := streamSequences[frame.Stream]; previous != 0 && frame.Sequence != previous+1 {
			return errors.New("workload log page contains a non-contiguous stream sequence")
		}
		previousCursor = frame.Cursor
		streamSequences[frame.Stream] = frame.Sequence
	}
	if len(r.Frames) > 0 {
		expected, err := NewLogCursor(containerID, attemptID, previousCursor)
		if err != nil {
			return err
		}
		if r.NextCursor != expected {
			return errors.New("workload log cursor does not identify the last frame")
		}
	} else if len(r.NextCursor) > 512 {
		return errors.New("empty-page workload log cursor is unbounded")
	}
	if r.HasMore && len(r.Frames) == 0 {
		return errors.New("workload log page cannot advertise more results without a frame")
	}
	return nil
}
