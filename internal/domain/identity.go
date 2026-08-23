package domain

import (
	"fmt"
	"strings"
	"unicode"
)

// SandboxID identifies one stable node-local workload environment.
type SandboxID string

// ContainerID identifies one API and persistence aggregate for an execution.
type ContainerID string

// AttemptID identifies the kernel-facing execution record owned by a Container.
type AttemptID string

// Validate rejects a Sandbox identity that cannot safely name a record.
func (id SandboxID) Validate() error {
	return validateID("sandbox_id", string(id))
}

// Validate rejects a Container identity that cannot safely name a record.
func (id ContainerID) Validate() error {
	return validateID("container_id", string(id))
}

// Validate rejects an Attempt identity that cannot safely name a record.
func (id AttemptID) Validate() error {
	return validateID("attempt_id", string(id))
}

// validateID enforces a small storage-independent identity contract for all resources.
func validateID(field, value string) error {
	if value == "" || strings.TrimSpace(value) != value {
		return NewError(CodeInvalidArgument, field, "must be non-empty without surrounding whitespace")
	}
	if len(value) > 128 {
		return NewError(CodeInvalidArgument, field, "must not exceed 128 bytes")
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return NewError(CodeInvalidArgument, field,
				fmt.Sprintf("contains unsupported whitespace or control character %q", r))
		}
	}
	return nil
}
