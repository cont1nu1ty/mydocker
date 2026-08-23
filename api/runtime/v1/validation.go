package v1

import (
	"encoding/base64"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	resumeTokenPrefix       = "v1:"
	logCursorPrefix         = "v1:log:"
	maxSandboxHostnameBytes = 64
	maxSandboxDNSServers    = 3
)

// ValidateRead rejects missing request correlation or an operation ID on a read-only call.
func (c RequestContext) ValidateRead() error {
	if err := validateIdentifier("request_id", c.RequestID, false); err != nil {
		return err
	}
	if c.OperationID != "" {
		return NewError(CodeInvalidArgument, "operation_id", "must be omitted for a read-only request")
	}
	return nil
}

// ValidateMutation requires both transport and durable operation identities before side effects.
func (c RequestContext) ValidateMutation() error {
	if err := validateIdentifier("request_id", c.RequestID, false); err != nil {
		return err
	}
	return ValidateOperationID(c.OperationID)
}

// ValidateOperationID checks the stable client-generated idempotency identity used across retries.
func ValidateOperationID(value string) error {
	return validateIdentifier("operation_id", value, false)
}

// ValidateResourceID checks a path-safe stable Sandbox, Container, or Attempt identity.
func ValidateResourceID(field, value string) error {
	return validateIdentifier(field, value, true)
}

// validateIdentifier enforces bounded, non-whitespace identifiers and optionally rejects path separators.
func validateIdentifier(field, value string, pathSafe bool) error {
	if value == "" || strings.TrimSpace(value) != value {
		return NewError(CodeInvalidArgument, field, "must be non-empty without surrounding whitespace")
	}
	if len(value) > 128 {
		return NewError(CodeInvalidArgument, field, "must not exceed 128 bytes")
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return NewError(CodeInvalidArgument, field, "contains unsupported whitespace or control characters")
		}
	}
	if pathSafe && strings.ContainsAny(value, "/:") {
		return NewError(CodeInvalidArgument, field, "must not contain a reserved path or action separator")
	}
	return nil
}

// Validate rejects malformed create input before the service persists Sandbox intent.
func (r CreateSandboxRequest) Validate() error {
	if err := ValidateResourceID("sandbox_id", r.SandboxID); err != nil {
		return err
	}
	if err := validateSandboxHostname(r.Spec.Hostname); err != nil {
		return err
	}
	if r.Spec.Network.Mode == "" || strings.ContainsRune(r.Spec.Network.Mode, '\x00') {
		return NewError(CodeInvalidArgument, "spec.network.mode", "must be non-empty and contain no NUL")
	}
	for index, attachment := range r.Spec.Network.Attachments {
		if strings.TrimSpace(attachment) == "" || strings.ContainsRune(attachment, '\x00') {
			return NewError(CodeInvalidArgument, fmt.Sprintf("spec.network.attachments[%d]", index), "must be non-empty and contain no NUL")
		}
	}
	if err := validateSandboxDNS(r.Spec.DNS); err != nil {
		return err
	}
	for key, value := range r.Spec.Labels {
		if strings.TrimSpace(key) == "" || strings.ContainsRune(key, '\x00') || strings.ContainsRune(value, '\x00') {
			return NewError(CodeInvalidArgument, "spec.labels", "keys must be non-empty and labels must contain no NUL")
		}
	}
	return validateResources(r.Spec.Resources)
}

// validateSandboxHostname accepts the empty API default or a UTF-8 value that
// fits the Linux UTS nodename field before lifecycle intent can be persisted.
func validateSandboxHostname(hostname string) error {
	if hostname == "" {
		return nil
	}
	if len(hostname) > maxSandboxHostnameBytes || !utf8.ValidString(hostname) || strings.ContainsRune(hostname, '\x00') {
		return NewError(CodeInvalidArgument, "spec.hostname", "must be valid UTF-8 without NUL and no longer than 64 bytes")
	}
	return nil
}

// validateSandboxDNS retains request spelling for operation fingerprints while
// bounding resolv.conf input to three parseable IP address literals.
func validateSandboxDNS(servers []string) error {
	if len(servers) > maxSandboxDNSServers {
		return NewError(CodeInvalidArgument, "spec.dns", "must contain no more than 3 servers")
	}
	for index, server := range servers {
		if net.ParseIP(server) == nil {
			return NewError(CodeInvalidArgument, fmt.Sprintf("spec.dns[%d]", index), "must be an IP address literal")
		}
	}
	return nil
}

// validateResources rejects non-positive values and requests greater than their matching limits.
func validateResources(resources Resources) error {
	values := []struct {
		field string
		value *int64
	}{
		{"spec.resources.requests.cpu_request_milli", resources.Requests.CPURequestMilli},
		{"spec.resources.requests.memory_request_bytes", resources.Requests.MemoryRequestBytes},
		{"spec.resources.limits.cpu_limit_milli", resources.Limits.CPULimitMilli},
		{"spec.resources.limits.memory_limit_bytes", resources.Limits.MemoryLimitBytes},
		{"spec.resources.limits.pids_limit", resources.Limits.PidsLimit},
	}
	for _, value := range values {
		if value.value != nil && *value.value <= 0 {
			return NewError(CodeInvalidArgument, value.field, "must be greater than zero")
		}
	}
	if resources.Limits.CPULimitMilli != nil && *resources.Limits.CPULimitMilli < 10 {
		return NewError(CodeInvalidArgument, "spec.resources.limits.cpu_limit_milli", "must be at least 10")
	}
	if exceeds(resources.Requests.CPURequestMilli, resources.Limits.CPULimitMilli) {
		return NewError(CodeInvalidArgument, "spec.resources.requests.cpu_request_milli", "must not exceed its limit")
	}
	if exceeds(resources.Requests.MemoryRequestBytes, resources.Limits.MemoryLimitBytes) {
		return NewError(CodeInvalidArgument, "spec.resources.requests.memory_request_bytes", "must not exceed its limit")
	}
	return nil
}

// exceeds reports whether two present values violate request less than or equal to limit.
func exceeds(request, limit *int64) bool {
	return request != nil && limit != nil && *request > *limit
}

// Validate rejects malformed immutable process and rootfs input before Container creation.
func (r CreateContainerRequest) Validate() error {
	if r.SandboxID != "" {
		if err := ValidateResourceID("sandbox_id", r.SandboxID); err != nil {
			return err
		}
	}
	if err := ValidateResourceID("container_id", r.ContainerID); err != nil {
		return err
	}
	if err := ValidateResourceID("attempt_id", r.AttemptID); err != nil {
		return err
	}
	if strings.TrimSpace(r.RootFS) == "" || strings.ContainsRune(r.RootFS, '\x00') {
		return NewError(CodeInvalidArgument, "rootfs", "must identify a prepared rootfs and contain no NUL")
	}
	return r.Process.Validate()
}

// Validate checks argv, environment, working directory, and termination input without shell parsing.
func (p ProcessSpec) Validate() error {
	if len(p.Argv) == 0 || p.Argv[0] == "" {
		return NewError(CodeInvalidArgument, "process.argv", "must contain a non-empty executable")
	}
	if !filepath.IsAbs(p.Argv[0]) || filepath.Clean(p.Argv[0]) != p.Argv[0] {
		return NewError(CodeInvalidArgument, "process.argv[0]", "must be a clean absolute executable path")
	}
	for _, argument := range p.Argv {
		if strings.ContainsRune(argument, '\x00') {
			return NewError(CodeInvalidArgument, "process.argv", "must not contain NUL")
		}
	}
	for index, variable := range p.Environment {
		if variable.Name == "" || strings.ContainsAny(variable.Name, "=\x00") || strings.ContainsRune(variable.Value, '\x00') {
			return NewError(CodeInvalidArgument, fmt.Sprintf("process.environment[%d]", index), "must be exec-safe structured data")
		}
	}
	if strings.ContainsRune(p.WorkingDirectory, '\x00') {
		return NewError(CodeInvalidArgument, "process.working_directory", "must contain no NUL")
	}
	if p.WorkingDirectory != "" && (!filepath.IsAbs(p.WorkingDirectory) || filepath.Clean(p.WorkingDirectory) != p.WorkingDirectory) {
		return NewError(CodeInvalidArgument, "process.working_directory", "must be empty or a clean absolute path")
	}
	if err := validateTerminationPolicy("process.termination", p.Termination, false); err != nil {
		return err
	}
	return nil
}

// Validate rejects an incomplete graceful policy before a kill operation reaches a process provider.
func (r KillContainerRequest) Validate() error {
	if r.ContainerID != "" {
		if err := ValidateResourceID("container_id", r.ContainerID); err != nil {
			return err
		}
	}
	return validateTerminationPolicy("policy", r.Policy, true)
}

// validateTerminationPolicy requires both signal names together, preserves an explicit non-negative grace period, and optionally rejects an empty policy.
func validateTerminationPolicy(field string, policy TerminationPolicy, required bool) error {
	if policy.GracePeriodNanoseconds < 0 {
		return NewError(CodeInvalidArgument, field+".grace_period_ns", "must not be negative")
	}
	empty := policy.Signal == "" && policy.GracePeriodNanoseconds == 0 && policy.EscalationSignal == ""
	if empty && !required {
		return nil
	}
	if strings.TrimSpace(policy.Signal) == "" || strings.TrimSpace(policy.Signal) != policy.Signal || strings.ContainsRune(policy.Signal, '\x00') {
		return NewError(CodeInvalidArgument, field+".signal", "must be an explicit signal name")
	}
	if strings.TrimSpace(policy.EscalationSignal) == "" || strings.TrimSpace(policy.EscalationSignal) != policy.EscalationSignal || strings.ContainsRune(policy.EscalationSignal, '\x00') {
		return NewError(CodeInvalidArgument, field+".escalation_signal", "must be an explicit signal name")
	}
	return nil
}

// NewResumeToken encodes one event sequence as a versioned opaque paging position.
func NewResumeToken(sequence uint64) ResumeToken {
	if sequence == 0 {
		return ""
	}
	payload := resumeTokenPrefix + strconv.FormatUint(sequence, 10)
	return ResumeToken(base64.RawURLEncoding.EncodeToString([]byte(payload)))
}

// ParseResumeToken validates a canonical v1 token and returns its last observed sequence.
func ParseResumeToken(token ResumeToken) (uint64, error) {
	if token == "" {
		return 0, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(string(token))
	if err != nil || !strings.HasPrefix(string(payload), resumeTokenPrefix) {
		return 0, NewError(CodeInvalidArgument, "after", "is not a valid v1 resume token")
	}
	sequence, err := strconv.ParseUint(strings.TrimPrefix(string(payload), resumeTokenPrefix), 10, 64)
	if err != nil || sequence == 0 || NewResumeToken(sequence) != token {
		return 0, NewError(CodeInvalidArgument, "after", "is not a canonical non-zero v1 resume token")
	}
	return sequence, nil
}

// NewLogCursor encodes one synchronized frame cursor and binds it to its Container/Attempt identity.
func NewLogCursor(containerID, attemptID string, cursor uint64) (LogCursor, error) {
	if err := ValidateResourceID("container_id", containerID); err != nil {
		return "", err
	}
	if err := ValidateResourceID("attempt_id", attemptID); err != nil {
		return "", err
	}
	if cursor == 0 {
		return "", NewError(CodeInvalidArgument, "log_cursor", "must identify a non-zero frame")
	}
	payload := logCursorPrefix + containerID + ":" + attemptID + ":" + strconv.FormatUint(cursor, 10)
	return LogCursor(base64.RawURLEncoding.EncodeToString([]byte(payload))), nil
}

// ParseLogCursor verifies canonical encoding and exact Container/Attempt binding before returning a frame position.
func ParseLogCursor(token LogCursor, containerID, attemptID string) (uint64, error) {
	if err := ValidateResourceID("container_id", containerID); err != nil {
		return 0, err
	}
	if err := ValidateResourceID("attempt_id", attemptID); err != nil {
		return 0, err
	}
	if token == "" {
		return 0, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(string(token))
	if err != nil || !strings.HasPrefix(string(payload), logCursorPrefix) {
		return 0, NewError(CodeInvalidArgument, "after", "is not a valid v1 log cursor")
	}
	parts := strings.Split(strings.TrimPrefix(string(payload), logCursorPrefix), ":")
	if len(parts) != 3 || parts[0] != containerID || parts[1] != attemptID {
		return 0, NewError(CodeInvalidArgument, "after", "belongs to a different Container Attempt")
	}
	cursor, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil || cursor == 0 {
		return 0, NewError(CodeInvalidArgument, "after", "does not contain a non-zero cursor")
	}
	canonical, err := NewLogCursor(containerID, attemptID, cursor)
	if err != nil || canonical != token {
		return 0, NewError(CodeInvalidArgument, "after", "is not a canonical v1 log cursor")
	}
	return cursor, nil
}
