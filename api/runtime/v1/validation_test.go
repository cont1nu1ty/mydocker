package v1

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

// TestCreateSandboxEnvironmentIsBoundedBeforeDispatch verifies UTS and
// resolv.conf inputs fail at the public schema before lifecycle persistence or host effects.
func TestCreateSandboxEnvironmentIsBoundedBeforeDispatch(t *testing.T) {
	valid := CreateSandboxRequest{
		SandboxID: "sandbox-one",
		Spec: SandboxSpec{
			Hostname: strings.Repeat("h", 64), DNS: []string{"1.1.1.1", "2001:4860:4860::8888"},
			Network: NetworkIntent{Mode: "none"},
		},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("CreateSandboxRequest.Validate(valid) error = %v", err)
	}
	invalid := []SandboxSpec{
		{Hostname: strings.Repeat("h", 65), Network: NetworkIntent{Mode: "none"}},
		{Hostname: string([]byte{0xff}), Network: NetworkIntent{Mode: "none"}},
		{DNS: []string{"resolver.example"}, Network: NetworkIntent{Mode: "none"}},
		{DNS: []string{"1.1.1.1", "8.8.8.8", "9.9.9.9", "2001:4860:4860::8888"}, Network: NetworkIntent{Mode: "none"}},
	}
	for index, spec := range invalid {
		request := CreateSandboxRequest{SandboxID: "sandbox-one", Spec: spec}
		if err := request.Validate(); !isAPIErrorCode(err, CodeInvalidArgument) {
			t.Fatalf("invalid environment %d error = %v, want invalid_argument", index, err)
		}
	}
}

// TestProcessSpecRequiresCanonicalAbsoluteExecutionPaths verifies malformed
// workload paths fail at the public API before lifecycle intent or host acquisition.
func TestProcessSpecRequiresCanonicalAbsoluteExecutionPaths(t *testing.T) {
	valid := ProcessSpec{Argv: []string{"/bin/workload"}, WorkingDirectory: "/work"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("ProcessSpec.Validate(valid) error = %v", err)
	}
	invalid := []ProcessSpec{
		{Argv: []string{"workload"}},
		{Argv: []string{"/bin/../bin/workload"}},
		{Argv: []string{"/bin/workload"}, WorkingDirectory: "work"},
		{Argv: []string{"/bin/workload"}, WorkingDirectory: "/work/../work"},
	}
	for index, value := range invalid {
		if err := value.Validate(); !isAPIErrorCode(err, CodeInvalidArgument) {
			t.Fatalf("ProcessSpec.Validate(invalid %d) error = %v, want invalid_argument", index, err)
		}
	}
}

// TestKillPolicyRequiresBothSignals verifies the public API never invents graceful-stop defaults on behalf of a caller.
func TestKillPolicyRequiresBothSignals(t *testing.T) {
	valid := KillContainerRequest{ContainerID: "container-one", Policy: TerminationPolicy{
		Signal: "SIGTERM", GracePeriodNanoseconds: 1, EscalationSignal: "SIGKILL",
	}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("KillContainerRequest.Validate() error = %v", err)
	}
	invalid := []KillContainerRequest{
		{ContainerID: "container-one"},
		{ContainerID: "container-one", Policy: TerminationPolicy{Signal: "SIGTERM"}},
		{ContainerID: "container-one", Policy: TerminationPolicy{Signal: "SIGTERM", GracePeriodNanoseconds: -1, EscalationSignal: "SIGKILL"}},
	}
	for index, request := range invalid {
		if err := request.Validate(); !isAPIErrorCode(err, CodeInvalidArgument) {
			t.Fatalf("invalid request %d error = %v, want invalid_argument", index, err)
		}
	}
}

// TestResumeTokenRoundTrip verifies opaque tokens preserve sequence and reject non-canonical input.
func TestResumeTokenRoundTrip(t *testing.T) {
	token := NewResumeToken(42)
	if token == "" || token == "42" {
		t.Fatalf("token must be non-empty and opaque, got %q", token)
	}
	sequence, err := ParseResumeToken(token)
	if err != nil {
		t.Fatalf("parse token: %v", err)
	}
	if sequence != 42 {
		t.Fatalf("sequence = %d, want 42", sequence)
	}
	for _, invalid := range []ResumeToken{"42", NewResumeToken(1) + "="} {
		if _, err := ParseResumeToken(invalid); !isAPIErrorCode(err, CodeInvalidArgument) {
			t.Fatalf("ParseResumeToken(%q) error = %v, want invalid_argument", invalid, err)
		}
	}
}

// TestLogCursorBindsAttempt verifies a paging cursor cannot be reused across Container Attempt identities.
func TestLogCursorBindsAttempt(t *testing.T) {
	cursor, err := NewLogCursor("container-one", "attempt-one", 7)
	if err != nil {
		t.Fatalf("new log cursor: %v", err)
	}
	position, err := ParseLogCursor(cursor, "container-one", "attempt-one")
	if err != nil || position != 7 {
		t.Fatalf("parse log cursor = %d, %v", position, err)
	}
	if _, err := ParseLogCursor(cursor, "container-one", "attempt-two"); !isAPIErrorCode(err, CodeInvalidArgument) {
		t.Fatalf("cross-Attempt parse error = %v, want invalid_argument", err)
	}
}

// TestMutationContextRequiresClientOperationID verifies transport identity cannot substitute for idempotency identity.
func TestMutationContextRequiresClientOperationID(t *testing.T) {
	err := (RequestContext{RequestID: "request-one"}).ValidateMutation()
	if !isAPIErrorCode(err, CodeInvalidArgument) {
		t.Fatalf("ValidateMutation error = %v, want invalid_argument", err)
	}
	if err := (RequestContext{RequestID: "request-one", OperationID: "operation-one"}).ValidateMutation(); err != nil {
		t.Fatalf("valid mutation context: %v", err)
	}
}

// TestExitStatusVocabulary verifies stable categories remain suitable for CLI mapping.
func TestExitStatusVocabulary(t *testing.T) {
	tests := map[ErrorCode]int{
		CodeInvalidArgument:   2,
		CodeNotFound:          3,
		CodeConflict:          4,
		CodeOperationExpired:  4,
		CodeResumeGap:         4,
		CodeResourceExhausted: 5,
		CodeDeadlineExceeded:  5,
		CodeInternal:          1,
	}
	for code, expected := range tests {
		if actual := ExitStatus(code); actual != expected {
			t.Fatalf("ExitStatus(%q) = %d, want %d", code, actual, expected)
		}
	}
}

// TestRetentionErrorHTTPVocabulary verifies bounded-history outcomes remain
// distinguishable from absence, conflict, and transient daemon unavailability.
func TestRetentionErrorHTTPVocabulary(t *testing.T) {
	tests := map[ErrorCode]int{
		CodeOperationExpired:  http.StatusGone,
		CodeResumeGap:         http.StatusGone,
		CodeResourceExhausted: http.StatusInsufficientStorage,
	}
	for code, expected := range tests {
		if !code.Valid() {
			t.Fatalf("ErrorCode.Valid(%q) = false", code)
		}
		if actual := HTTPStatus(code); actual != expected {
			t.Fatalf("HTTPStatus(%q) = %d, want %d", code, actual, expected)
		}
	}
}

// isAPIErrorCode reports whether a test error contains the expected stable v1 classification.
func isAPIErrorCode(err error, code ErrorCode) bool {
	var typed *Error
	return errors.As(err, &typed) && typed.Code == code
}
