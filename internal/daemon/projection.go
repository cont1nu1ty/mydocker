package daemon

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	v1 "mydocker/api/runtime/v1"
	"mydocker/internal/domain"
	"mydocker/internal/lifecycle"
	"mydocker/internal/operation"
	"mydocker/internal/provider"
)

// sandboxSpecToDomain copies the public immutable Sandbox input without sharing maps, slices, or optional values.
func sandboxSpecToDomain(spec v1.SandboxSpec) domain.SandboxSpec {
	labels := make(map[string]string, len(spec.Labels))
	for key, value := range spec.Labels {
		labels[key] = value
	}
	if spec.Labels == nil {
		labels = nil
	}
	return domain.SandboxSpec{
		Hostname: spec.Hostname,
		DNS:      append([]string(nil), spec.DNS...),
		Labels:   labels,
		Network: domain.NetworkIntent{
			Mode:        spec.Network.Mode,
			Attachments: append([]string(nil), spec.Network.Attachments...),
		},
		Resources: domain.Resources{
			Requests: domain.ResourceRequests{
				CPURequestMilli:    cloneInt64(spec.Resources.Requests.CPURequestMilli),
				MemoryRequestBytes: cloneInt64(spec.Resources.Requests.MemoryRequestBytes),
			},
			Limits: domain.ResourceLimits{
				CPULimitMilli:    cloneInt64(spec.Resources.Limits.CPULimitMilli),
				MemoryLimitBytes: cloneInt64(spec.Resources.Limits.MemoryLimitBytes),
				PidsLimit:        cloneInt64(spec.Resources.Limits.PidsLimit),
			},
		},
	}
}

// processSpecToDomain preserves argv/environment order and converts nanoseconds without shell serialization.
func processSpecToDomain(spec v1.ProcessSpec) domain.ProcessSpec {
	environment := make([]domain.EnvVar, len(spec.Environment))
	for index, variable := range spec.Environment {
		environment[index] = domain.EnvVar{Name: variable.Name, Value: variable.Value}
	}
	return domain.ProcessSpec{
		Argv:             append([]string(nil), spec.Argv...),
		Environment:      environment,
		WorkingDirectory: spec.WorkingDirectory,
		Termination:      terminationPolicyToDomain(spec.Termination),
	}
}

// projectSandboxResult requires a retained Sandbox for create/stop and attaches a public operation projection.
func projectSandboxResult(result lifecycle.SandboxResult) (v1.SandboxResponse, error) {
	if result.Sandbox == nil {
		return v1.SandboxResponse{}, errors.New("Sandbox mutation completed without a retained Sandbox projection")
	}
	sandbox, err := projectSandbox(*result.Sandbox)
	if err != nil {
		return v1.SandboxResponse{}, MapError(err)
	}
	operationValue, err := projectOperation(result.Operation)
	if err != nil {
		return v1.SandboxResponse{}, MapError(err)
	}
	return v1.SandboxResponse{Sandbox: sandbox, Operation: &operationValue}, nil
}

// projectContainerResult requires a retained atomic pair for create/start/kill and attaches its public operation.
func projectContainerResult(pair *domain.ContainerAttempt, operationValue operation.Operation) (v1.ContainerResponse, error) {
	if pair == nil {
		return v1.ContainerResponse{}, v1.NewError(v1.CodeInternal, "container", "mutation completed without a retained Container Attempt")
	}
	container, err := projectContainer(*pair)
	if err != nil {
		return v1.ContainerResponse{}, MapError(err)
	}
	projectedOperation, err := projectOperation(operationValue)
	if err != nil {
		return v1.ContainerResponse{}, MapError(err)
	}
	return v1.ContainerResponse{Container: container, Operation: &projectedOperation}, nil
}

// projectSandbox converts one validated domain aggregate to the v1 schema without persistence metadata.
func projectSandbox(sandbox domain.Sandbox) (v1.Sandbox, error) {
	if err := sandbox.Validate(); err != nil {
		return v1.Sandbox{}, fmt.Errorf("persisted Sandbox cannot be projected: %v", err)
	}
	if err := v1.ValidateResourceID("sandbox_id", string(sandbox.ID)); err != nil {
		return v1.Sandbox{}, fmt.Errorf("persisted Sandbox has no public identity: %v", err)
	}
	if sandbox.Spec.Network.Mode != "none" && sandbox.Spec.Network.Mode != "loopback" {
		return v1.Sandbox{}, errors.New("persisted Sandbox uses a network mode outside the M3 API")
	}
	if len(sandbox.Spec.Network.Attachments) != 0 {
		return v1.Sandbox{}, errors.New("persisted Sandbox contains network attachments outside the M3 API")
	}
	status := v1.SandboxStatus{
		Phase:              string(sandbox.Status.Phase),
		Generation:         uint64(sandbox.Status.Generation),
		ObservedGeneration: uint64(sandbox.Status.ObservedGeneration),
		Conditions:         projectConditions(sandbox.Status.Conditions),
		LastObservation:    projectObservation(sandbox.Status.LastObservation),
	}
	if sandbox.Status.CurrentContainerID != nil {
		value := string(*sandbox.Status.CurrentContainerID)
		if err := v1.ValidateResourceID("current_container_id", value); err != nil {
			return v1.Sandbox{}, fmt.Errorf("persisted Sandbox current Container has no public identity: %v", err)
		}
		status.CurrentContainerID = &value
	}
	if sandbox.Status.CurrentAttemptID != nil {
		value := string(*sandbox.Status.CurrentAttemptID)
		if err := v1.ValidateResourceID("current_attempt_id", value); err != nil {
			return v1.Sandbox{}, fmt.Errorf("persisted Sandbox current Attempt has no public identity: %v", err)
		}
		status.CurrentAttemptID = &value
	}
	return v1.Sandbox{
		ID: string(sandbox.ID),
		Spec: v1.SandboxSpec{
			Hostname: sandbox.Spec.Hostname,
			DNS:      append([]string(nil), sandbox.Spec.DNS...),
			Labels:   cloneLabels(sandbox.Spec.Labels),
			Network: v1.NetworkIntent{
				Mode:        sandbox.Spec.Network.Mode,
				Attachments: append([]string(nil), sandbox.Spec.Network.Attachments...),
			},
			Resources: projectResources(sandbox.Spec.Resources),
		},
		Status: status,
	}, nil
}

// projectContainer converts the canonical pair and replaces provider stream and process handles with public opaque references.
func projectContainer(pair domain.ContainerAttempt) (v1.Container, error) {
	if err := pair.Validate(); err != nil {
		return v1.Container{}, fmt.Errorf("persisted Container Attempt cannot be projected: %v", err)
	}
	identities := []struct {
		field string
		value string
	}{
		{field: "container_id", value: string(pair.Container.ID)},
		{field: "sandbox_id", value: string(pair.Container.SandboxID)},
		{field: "attempt_id", value: string(pair.Attempt.ID)},
	}
	for _, identity := range identities {
		if err := v1.ValidateResourceID(identity.field, identity.value); err != nil {
			return v1.Container{}, fmt.Errorf("persisted Container Attempt has no public identity: %v", err)
		}
	}
	if err := provider.OpaqueID(pair.Container.Spec.RootFS).Validate(); err != nil {
		return v1.Container{}, fmt.Errorf("persisted Container rootfs is not an opaque provider identifier: %v", err)
	}
	status := v1.ContainerStatus{
		Phase:              string(pair.Container.Status.Phase),
		Generation:         uint64(pair.Container.Status.Generation),
		ObservedGeneration: uint64(pair.Container.Status.ObservedGeneration),
		Conditions:         projectConditions(pair.Container.Status.Conditions),
		Streams:            projectStreams(pair),
		Outcome:            projectOutcome(pair.Container.Status.Outcome),
		LastObservation:    projectObservation(pair.Container.Status.LastObservation),
	}
	if pair.Container.Status.ProcessIdentity != nil {
		identity := projectProcessIdentity(*pair.Container.Status.ProcessIdentity)
		status.ProcessIdentity = &identity
	}
	return v1.Container{
		ID:        string(pair.Container.ID),
		SandboxID: string(pair.Container.SandboxID),
		AttemptID: string(pair.Attempt.ID),
		Spec: v1.ContainerSpec{
			Process: projectProcessSpec(pair.Container.Spec.Process),
			RootFS:  pair.Container.Spec.RootFS,
			Limits:  projectResolvedLimits(pair.Container.Spec.Limits),
		},
		Status: status,
	}, nil
}

// projectOperation exposes bounded progress and identity fields but never the internal lifecycle replay JSON encoding.
func projectOperation(value operation.Operation) (v1.Operation, error) {
	if err := value.Validate(); err != nil {
		return v1.Operation{}, fmt.Errorf("persisted operation cannot be projected: %v", err)
	}
	if err := v1.ValidateOperationID(string(value.ID)); err != nil {
		return v1.Operation{}, fmt.Errorf("persisted operation has no public identity: %v", err)
	}
	target, err := projectResourceRef(value.Target)
	if err != nil {
		return v1.Operation{}, err
	}
	return v1.Operation{
		ID:          string(value.ID),
		Type:        string(value.Type),
		Target:      target,
		Fingerprint: v1.RequestFingerprint{Version: value.Fingerprint.Version, SHA256: value.Fingerprint.SHA256},
		State:       string(value.State),
		Stage:       string(value.Stage),
		Result:      string(value.Result),
		Reason:      string(value.Reason),
		Response:    nil,
	}, nil
}

// projectEvent exposes ordered lifecycle facts while dropping internal provider details that may contain host evidence.
func projectEvent(value operation.Event) (v1.Event, error) {
	if err := value.Validate(); err != nil {
		return v1.Event{}, fmt.Errorf("persisted event cannot be projected: %v", err)
	}
	target, err := projectResourceRef(value.Target)
	if err != nil {
		return v1.Event{}, err
	}
	resources := make([]v1.ResourceRef, len(value.Resources))
	for index, resource := range value.Resources {
		resources[index], err = projectResourceRef(resource)
		if err != nil {
			return v1.Event{}, err
		}
	}
	projected := v1.Event{
		Sequence:           uint64(value.Sequence),
		OperationID:        string(value.OperationID),
		Type:               string(value.Type),
		Target:             target,
		Resources:          resources,
		Stage:              string(value.Stage),
		Result:             string(value.Result),
		Reason:             string(value.Reason),
		OccurredAt:         value.OccurredAt,
		Generation:         value.Generation,
		ObservedGeneration: value.ObservedGeneration,
		Details:            nil,
	}
	if value.Duration != nil {
		duration := int64(*value.Duration)
		projected.DurationNanoseconds = &duration
	}
	return projected, nil
}

// projectResourceRef validates that one internal lifecycle target is representable by the path-safe v1 identity contract.
func projectResourceRef(target operation.Target) (v1.ResourceRef, error) {
	if err := target.Validate(); err != nil {
		return v1.ResourceRef{}, fmt.Errorf("invalid persisted resource reference: %v", err)
	}
	if err := v1.ValidateResourceID(string(target.Kind)+"_id", target.ID); err != nil {
		return v1.ResourceRef{}, fmt.Errorf("persisted resource reference has no public identity: %v", err)
	}
	return v1.ResourceRef{Kind: string(target.Kind), ID: target.ID}, nil
}

// projectProcessSpec preserves public structured execution data and converts domain duration to nanoseconds.
func projectProcessSpec(spec domain.ProcessSpec) v1.ProcessSpec {
	environment := make([]v1.EnvVar, len(spec.Environment))
	for index, variable := range spec.Environment {
		environment[index] = v1.EnvVar{Name: variable.Name, Value: variable.Value}
	}
	return v1.ProcessSpec{
		Argv:             append([]string(nil), spec.Argv...),
		Environment:      environment,
		WorkingDirectory: spec.WorkingDirectory,
		Termination: v1.TerminationPolicy{
			Signal:                 spec.Termination.Signal,
			GracePeriodNanoseconds: int64(spec.Termination.GracePeriod),
			EscalationSignal:       spec.Termination.EscalationSignal,
		},
	}
}

// projectResources copies scheduling requests and enforcement limits without conflating them.
func projectResources(resources domain.Resources) v1.Resources {
	return v1.Resources{
		Requests: v1.ResourceRequests{
			CPURequestMilli:    cloneInt64(resources.Requests.CPURequestMilli),
			MemoryRequestBytes: cloneInt64(resources.Requests.MemoryRequestBytes),
		},
		Limits: v1.ResourceLimits{
			CPULimitMilli:    cloneInt64(resources.Limits.CPULimitMilli),
			MemoryLimitBytes: cloneInt64(resources.Limits.MemoryLimitBytes),
			PidsLimit:        cloneInt64(resources.Limits.PidsLimit),
		},
	}
}

// projectResolvedLimits copies immutable effective limit values without borrowing domain pointers.
func projectResolvedLimits(limits domain.ResolvedResourceLimits) v1.ResolvedResourceLimits {
	return v1.ResolvedResourceLimits{
		CPUUnlimited:     limits.CPUUnlimited,
		CPULimitMilli:    cloneInt64(limits.CPULimitMilli),
		MemoryUnlimited:  limits.MemoryUnlimited,
		MemoryLimitBytes: cloneInt64(limits.MemoryLimitBytes),
		PidsLimit:        limits.PidsLimit,
	}
}

// projectConditions copies bounded type/reason facts while keeping free-form provider diagnostics out of the API.
func projectConditions(conditions []domain.Condition) []v1.Condition {
	if conditions == nil {
		return nil
	}
	projected := make([]v1.Condition, len(conditions))
	for index, condition := range conditions {
		projected[index] = v1.Condition{Type: condition.Type, Reason: condition.Reason}
	}
	return projected
}

// projectObservation converts the latest durable event reference without adding transport state.
func projectObservation(observation domain.LifecycleObservation) v1.LifecycleObservation {
	return v1.LifecycleObservation{
		OperationID:   observation.OperationID,
		EventSequence: observation.EventSequence,
		Reason:        observation.Reason,
	}
}

// projectOutcome converts optional wall facts and the same-process duration sample without inventing missing data.
func projectOutcome(outcome domain.Outcome) v1.Outcome {
	projected := v1.Outcome{
		Presence:   string(outcome.Presence),
		ExitCode:   cloneInt32(outcome.ExitCode),
		Signal:     outcome.Signal,
		OOM:        string(outcome.OOM),
		StartedAt:  cloneTime(outcome.StartedAt),
		FinishedAt: cloneTime(outcome.FinishedAt),
	}
	if outcome.RunningDuration != nil {
		value := int64(*outcome.RunningDuration)
		projected.RunningDurationNanoseconds = &value
	}
	return projected
}

// projectStreams exposes deterministic API references only when the corresponding internal stream exists.
func projectStreams(pair domain.ContainerAttempt) v1.StreamReferences {
	streams := v1.StreamReferences{}
	if pair.Attempt.Streams.Stdin != "" {
		streams.Stdin = publicStreamReference(pair, "stdin")
	}
	if pair.Attempt.Streams.Stdout != "" {
		streams.Stdout = publicStreamReference(pair, "stdout")
	}
	if pair.Attempt.Streams.Stderr != "" {
		streams.Stderr = publicStreamReference(pair, "stderr")
	}
	return streams
}

// publicStreamReference derives a stable opaque endpoint token and never copies a provider path or descriptor.
func publicStreamReference(pair domain.ContainerAttempt, stream string) string {
	digest := sha256.Sum256([]byte(string(pair.Container.ID) + "\x00" + string(pair.Attempt.ID) + "\x00" + stream))
	return "v1:stream:" + stream + ":" + hex.EncodeToString(digest[:12])
}

// projectProcessIdentity hashes the provider handle and evidence into non-authoritative public diagnostics with no raw PID or path.
func projectProcessIdentity(identity domain.ProcessIdentity) v1.ProcessIdentity {
	handleDigest := sha256.Sum256([]byte(identity.Handle + "\x00" + identity.Evidence))
	evidenceDigest := sha256.Sum256([]byte(identity.Evidence))
	return v1.ProcessIdentity{
		Verified: identity.Verified,
		Handle:   "v1:process:" + hex.EncodeToString(handleDigest[:12]),
		Evidence: "sha256:" + hex.EncodeToString(evidenceDigest[:]),
	}
}

// cloneLabels prevents public callers from mutating an internal Sandbox map through projection aliases.
func cloneLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	clone := make(map[string]string, len(labels))
	for key, value := range labels {
		clone[key] = value
	}
	return clone
}

// cloneInt64 copies an optional integer across the API/domain ownership boundary.
func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

// cloneInt32 copies an optional exit code across the API/domain ownership boundary.
func cloneInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

// cloneTime copies an optional diagnostic wall timestamp across the projection boundary.
func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
