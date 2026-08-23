package shim

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"mydocker/internal/domain"
	"mydocker/internal/ownership"
)

const (
	// SchemaVersion is the only wrapper, control, and terminal schema understood by M3.
	SchemaVersion uint32 = 1
	// MaxControlBytes bounds one control request or response before JSON decoding.
	MaxControlBytes = 64 << 10
)

// Mode selects the stable namespace keeper or the gated Attempt init wrapper.
type Mode string

const (
	// ModeKeeper keeps provider-prepared Sandbox namespaces alive without starting a workload.
	ModeKeeper Mode = "keeper"
	// ModeInit supervises one gated workload child while retaining the wrapper executable identity.
	ModeInit Mode = "init"
)

// Valid reports whether mode is one of the two M3 wrapper roles.
func (mode Mode) Valid() bool {
	return mode == ModeKeeper || mode == ModeInit
}

// State is the externally observable Attempt wrapper lifecycle.
type State string

const (
	// StatePrepared means the wrapper is alive and the one-shot workload gate is still closed.
	StatePrepared State = "prepared"
	// StateStarting means the one-shot gate is consumed while child creation has not yet produced a verified identity or terminal fact.
	StateStarting State = "starting"
	// StateRunning means the gate was consumed and the verified child has not been reaped.
	StateRunning State = "running"
	// StateTerminal means the child result or pre-exec failure was durably recorded.
	StateTerminal State = "terminal"
)

// Valid reports whether state is part of the reconnect-safe M3 observation vocabulary.
func (state State) Valid() bool {
	return state == StatePrepared || state == StateStarting || state == StateRunning || state == StateTerminal
}

// ErrorCode is a bounded control-plane reason that callers may branch on safely.
type ErrorCode string

const (
	// CodeInvalidArgument rejects malformed configuration, identity, signal, or JSON input.
	CodeInvalidArgument ErrorCode = "invalid_argument"
	// CodeOwnerMismatch rejects a request not bound to this exact persisted owner key.
	CodeOwnerMismatch ErrorCode = "owner_mismatch"
	// CodeDuplicateRequest rejects reuse of a control request identity.
	CodeDuplicateRequest ErrorCode = "duplicate_request"
	// CodeWrongMode rejects an init-only action sent to a keeper.
	CodeWrongMode ErrorCode = "wrong_mode"
	// CodeAlreadyReleased rejects a second attempt to consume the one-shot start gate.
	CodeAlreadyReleased ErrorCode = "already_released"
	// CodeNotRunning rejects signal forwarding without a currently verified child.
	CodeNotRunning ErrorCode = "not_running"
	// CodeStartFailed reports that the one allowed child start attempt failed.
	CodeStartFailed ErrorCode = "start_failed"
	// CodeRootfsFailed reports that the one allowed PID1 rootfs preparation failed and cannot be retried safely.
	CodeRootfsFailed ErrorCode = "rootfs_failed"
	// CodePersistenceFailed reports a terminal fact that could not be made durable.
	CodePersistenceFailed ErrorCode = "persistence_failed"
	// CodeUnsupportedRequest rejects a control action outside the bounded M3 protocol.
	CodeUnsupportedRequest ErrorCode = "unsupported_request"
	// CodeUnavailable reports an unavailable control socket or wrapper transport.
	CodeUnavailable ErrorCode = "unavailable"
)

// Error carries a bounded machine-readable code while retaining a local diagnostic cause.
type Error struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	cause   error
}

// Error returns the bounded message and includes the local cause only inside the wrapper process.
func (err *Error) Error() string {
	if err == nil {
		return ""
	}
	if err.cause != nil {
		return fmt.Sprintf("%s: %s: %v", err.Code, err.Message, err.cause)
	}
	return fmt.Sprintf("%s: %s", err.Code, err.Message)
}

// Unwrap exposes the local cause for logs and deterministic fault tests, never over the control protocol.
func (err *Error) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.cause
}

// IsCode reports whether err contains a typed shim failure with the selected bounded code.
func IsCode(err error, code ErrorCode) bool {
	var typed *Error
	return errors.As(err, &typed) && typed.Code == code
}

// newError constructs a typed failure without leaking the cause into a control response.
func newError(code ErrorCode, message string, cause error) *Error {
	return &Error{Code: code, Message: message, cause: cause}
}

// Signal is the bounded set of names accepted by the verified child signal method.
type Signal string

const (
	// SignalHUP requests a conventional hangup notification.
	SignalHUP Signal = "SIGHUP"
	// SignalINT requests an interactive interrupt.
	SignalINT Signal = "SIGINT"
	// SignalQUIT requests a conventional quit signal.
	SignalQUIT Signal = "SIGQUIT"
	// SignalKILL requests immediate kernel termination.
	SignalKILL Signal = "SIGKILL"
	// SignalTERM requests conventional graceful termination.
	SignalTERM Signal = "SIGTERM"
	// SignalUSR1 is the first application-defined signal.
	SignalUSR1 Signal = "SIGUSR1"
	// SignalUSR2 is the second application-defined signal.
	SignalUSR2 Signal = "SIGUSR2"
)

// Valid reports whether signal can be translated without accepting a raw numeric value.
func (signal Signal) Valid() bool {
	switch signal {
	case SignalHUP, SignalINT, SignalQUIT, SignalKILL, SignalTERM, SignalUSR1, SignalUSR2:
		return true
	default:
		return false
	}
}

// ChildIdentity is diagnostic strong-handle evidence that may be observed or persisted but never authorizes an action by itself.
type ChildIdentity struct {
	Handle         string `json:"handle"`
	EvidenceSHA256 string `json:"evidence_sha256"`
}

// Validate rejects an unbounded handle or evidence that is not a canonical SHA-256 digest.
func (identity ChildIdentity) Validate() error {
	if err := validateOpaque("child handle", identity.Handle, 256); err != nil {
		return err
	}
	if !validDigest(identity.EvidenceSHA256) {
		return errors.New("child identity evidence must be a lowercase SHA-256 digest")
	}
	return nil
}

// ChildExitEvidence preserves exact child wait facts even when independent OOM evidence is unavailable.
type ChildExitEvidence struct {
	Identity        ChildIdentity        `json:"identity"`
	ExitCode        *int32               `json:"exit_code,omitempty"`
	Signal          string               `json:"signal,omitempty"`
	OOM             domain.EvidenceState `json:"oom"`
	StartedAt       time.Time            `json:"started_at"`
	FinishedAt      time.Time            `json:"finished_at"`
	RunningDuration time.Duration        `json:"running_duration"`
	WaitError       string               `json:"wait_error,omitempty"`
}

// Clone returns exit evidence whose optional exit code cannot alias mutable caller memory.
func (evidence ChildExitEvidence) Clone() ChildExitEvidence {
	clone := evidence
	if evidence.ExitCode != nil {
		value := *evidence.ExitCode
		clone.ExitCode = &value
	}
	return clone
}

// Validate requires strong child identity, coherent wait facts, and an explicit OOM evidence state.
func (evidence ChildExitEvidence) Validate() error {
	if err := evidence.Identity.Validate(); err != nil {
		return err
	}
	if !evidence.OOM.Valid() {
		return errors.New("child exit OOM evidence must be explicit")
	}
	startedAt := evidence.StartedAt.Round(0)
	finishedAt := evidence.FinishedAt.Round(0)
	if startedAt.IsZero() || finishedAt.IsZero() || finishedAt.Before(startedAt) {
		return errors.New("child exit timestamps must be ordered and non-zero")
	}
	if evidence.RunningDuration < 0 {
		return errors.New("child running duration must not be negative")
	}
	if evidence.WaitError == "" {
		if (evidence.ExitCode == nil) == (evidence.Signal == "") {
			return errors.New("successful child wait requires exactly one exit code or signal")
		}
	} else if evidence.ExitCode != nil || evidence.Signal != "" {
		return errors.New("failed child wait must not claim an exit code or signal")
	}
	if evidence.ExitCode != nil && *evidence.ExitCode < 0 {
		return errors.New("child exit code must not be negative")
	}
	if evidence.Signal != "" && (strings.TrimSpace(evidence.Signal) == "" || strings.ContainsRune(evidence.Signal, '\x00')) {
		return errors.New("child terminal signal must be persistence safe")
	}
	if strings.ContainsRune(evidence.WaitError, '\x00') || len(evidence.WaitError) > 2048 {
		return errors.New("child wait diagnostic is not persistence safe")
	}
	return nil
}

// durableExecutionWindow converts an in-process monotonic interval into wall
// facts that keep the same non-negative duration after JSON drops monotonic data.
func durableExecutionWindow(startedAt, observedFinishedAt time.Time) (time.Time, time.Time, time.Duration) {
	duration := observedFinishedAt.Sub(startedAt)
	if duration < 0 {
		duration = 0
	}
	durableStartedAt := startedAt.Round(0).UTC()
	durableFinishedAt := durableStartedAt.Add(duration)
	return durableStartedAt, durableFinishedAt, duration
}

// DomainOutcome preserves known wait facts independently from OOM attribution so the engine can attach scoped cgroup evidence later.
func (evidence ChildExitEvidence) DomainOutcome() domain.Outcome {
	if evidence.WaitError != "" {
		outcome := domain.UnknownOutcome(evidence.OOM)
		startedAt := evidence.StartedAt
		finishedAt := evidence.FinishedAt
		outcome.StartedAt = &startedAt
		outcome.FinishedAt = &finishedAt
		return outcome
	}
	if evidence.ExitCode != nil {
		return domain.ExitOutcome(*evidence.ExitCode, evidence.OOM, evidence.StartedAt, evidence.FinishedAt, evidence.RunningDuration)
	}
	return domain.SignalOutcome(evidence.Signal, evidence.OOM, evidence.StartedAt, evidence.FinishedAt, evidence.RunningDuration)
}

// SignalDelivery proves that the strong child object performed action-time verification before delivery.
type SignalDelivery struct {
	Identity       ChildIdentity `json:"identity"`
	Signal         Signal        `json:"signal"`
	Delivered      bool          `json:"delivered"`
	DeliveredAt    time.Time     `json:"delivered_at"`
	EvidenceSHA256 string        `json:"evidence_sha256"`
}

// Validate checks bounded signal semantics, the wrapper-stamped action-completion time, and canonical action evidence.
func (delivery SignalDelivery) Validate() error {
	if err := delivery.Identity.Validate(); err != nil {
		return err
	}
	if !delivery.Signal.Valid() || !delivery.Delivered || delivery.DeliveredAt.IsZero() || !validDigest(delivery.EvidenceSHA256) {
		return errors.New("signal delivery requires a supported delivered signal, delivery time, and canonical evidence")
	}
	return nil
}

// Child is the one started workload process; signaling must reverify its strong handle at action time.
type Child interface {
	// Identity returns diagnostic evidence for matching Wait and SignalVerified results.
	Identity() ChildIdentity
	// Wait reaps the child exactly once and returns independently attributable terminal facts.
	Wait() (ChildExitEvidence, error)
	// SignalVerified performs strong-handle verification and signal delivery as one child-owned action.
	SignalVerified(Signal) (SignalDelivery, error)
}

// ChildRunner forks and execs one child without replacing the long-lived init wrapper process.
type ChildRunner interface {
	// Start receives structured process data and injected output writers, and returns only after strong identity capture.
	Start(domain.ProcessSpec, io.Writer, io.Writer) (Child, error)
}

// TerminalStore is the durable boundary for the one immutable terminal record of an Attempt.
type TerminalStore interface {
	// Load returns the previously committed terminal record, or found=false when the gate may still be prepared.
	Load() (record TerminalRecord, found bool, err error)
	// Commit atomically publishes the first terminal record and rejects replacement.
	Commit(TerminalRecord) error
}

// InitSpec binds one wrapper instance to immutable owner, Container, Attempt, process, and executable evidence.
type InitSpec struct {
	Owner           ownership.OwnerKey `json:"owner"`
	SandboxID       domain.SandboxID   `json:"sandbox_id"`
	ContainerID     domain.ContainerID `json:"container_id"`
	AttemptID       domain.AttemptID   `json:"attempt_id"`
	WrapperEvidence string             `json:"wrapper_evidence_sha256"`
	Process         domain.ProcessSpec `json:"process"`
}

// Validate rejects an init wrapper not bound to the exact Container owner and structured process spec.
func (spec InitSpec) Validate() error {
	if err := spec.Owner.Validate(); err != nil {
		return fmt.Errorf("init owner: %w", err)
	}
	if spec.Owner.Target.Kind != "container" || spec.Owner.Target.ID != string(spec.ContainerID) {
		return errors.New("init owner must target the exact Container")
	}
	if err := spec.SandboxID.Validate(); err != nil {
		return err
	}
	if err := spec.ContainerID.Validate(); err != nil {
		return err
	}
	if err := spec.AttemptID.Validate(); err != nil {
		return err
	}
	if !validDigest(spec.WrapperEvidence) {
		return errors.New("wrapper evidence must be a lowercase SHA-256 digest")
	}
	return spec.Process.Validate()
}

// KeeperSpec binds a minimal long-lived namespace keeper to one exact Sandbox owner.
type KeeperSpec struct {
	Owner           ownership.OwnerKey `json:"owner"`
	SandboxID       domain.SandboxID   `json:"sandbox_id"`
	WrapperEvidence string             `json:"wrapper_evidence_sha256"`
}

// Validate rejects a keeper not bound to the exact Sandbox named by its ownership intent.
func (spec KeeperSpec) Validate() error {
	if err := spec.Owner.Validate(); err != nil {
		return fmt.Errorf("keeper owner: %w", err)
	}
	if spec.Owner.Target.Kind != "sandbox" || spec.Owner.Target.ID != string(spec.SandboxID) {
		return errors.New("keeper owner must target the exact Sandbox")
	}
	if err := spec.SandboxID.Validate(); err != nil {
		return err
	}
	if !validDigest(spec.WrapperEvidence) {
		return errors.New("keeper wrapper evidence must be a lowercase SHA-256 digest")
	}
	return nil
}

// Observation is an owner-scoped prepared, running, or terminal wrapper fact for daemon reconciliation.
type Observation struct {
	SchemaVersion   uint32             `json:"schema_version"`
	Mode            Mode               `json:"mode"`
	Owner           ownership.OwnerKey `json:"owner"`
	SandboxID       domain.SandboxID   `json:"sandbox_id"`
	ContainerID     domain.ContainerID `json:"container_id,omitempty"`
	AttemptID       domain.AttemptID   `json:"attempt_id,omitempty"`
	State           State              `json:"state"`
	WrapperEvidence string             `json:"wrapper_evidence_sha256"`
	Child           *ChildIdentity     `json:"child,omitempty"`
	Terminal        *TerminalRecord    `json:"terminal,omitempty"`
	Rootfs          *RootfsPreparation `json:"rootfs,omitempty"`
	EvidenceSHA256  string             `json:"evidence_sha256"`
}

// Clone returns an observation whose optional child and terminal values cannot alias wrapper state.
func (observation Observation) Clone() Observation {
	clone := observation
	if observation.Child != nil {
		child := *observation.Child
		clone.Child = &child
	}
	if observation.Terminal != nil {
		terminal := observation.Terminal.Clone()
		clone.Terminal = &terminal
	}
	if observation.Rootfs != nil {
		rootfs := *observation.Rootfs
		clone.Rootfs = &rootfs
	}
	return clone
}

// Validate checks identity, state-specific facts, terminal checksum, and the observation digest.
func (observation Observation) Validate() error {
	if observation.SchemaVersion != SchemaVersion || !observation.Mode.Valid() || !observation.State.Valid() {
		return errors.New("unsupported wrapper observation schema, mode, or state")
	}
	if err := observation.Owner.Validate(); err != nil {
		return err
	}
	if err := observation.SandboxID.Validate(); err != nil {
		return err
	}
	if !validDigest(observation.WrapperEvidence) {
		return errors.New("observation wrapper evidence must be canonical")
	}
	if observation.Rootfs != nil {
		if observation.Mode != ModeInit {
			return errors.New("keeper observation cannot contain rootfs preparation")
		}
		if err := observation.Rootfs.Validate(); err != nil {
			return err
		}
	}
	if observation.Mode == ModeKeeper {
		if observation.Owner.Target.Kind != "sandbox" || observation.Owner.Target.ID != string(observation.SandboxID) ||
			observation.State != StatePrepared || observation.ContainerID != "" || observation.AttemptID != "" ||
			observation.Child != nil || observation.Terminal != nil || observation.Rootfs != nil {
			return errors.New("keeper observation must be a prepared Sandbox-only fact")
		}
	} else {
		if observation.Owner.Target.Kind != "container" || observation.Owner.Target.ID != string(observation.ContainerID) {
			return errors.New("init observation owner must match its Container")
		}
		if err := observation.ContainerID.Validate(); err != nil {
			return err
		}
		if err := observation.AttemptID.Validate(); err != nil {
			return err
		}
		switch observation.State {
		case StatePrepared, StateStarting:
			if observation.Child != nil || observation.Terminal != nil {
				return errors.New("prepared or starting observation cannot contain child or terminal facts")
			}
		case StateRunning:
			if observation.Child == nil || observation.Terminal != nil {
				return errors.New("running observation requires only child identity")
			}
			if err := observation.Child.Validate(); err != nil {
				return err
			}
		case StateTerminal:
			if observation.Child != nil || observation.Terminal == nil {
				return errors.New("terminal observation requires only a durable terminal record")
			}
			if err := observation.Terminal.Validate(); err != nil {
				return err
			}
			if observation.Terminal.Owner != observation.Owner || observation.Terminal.ContainerID != observation.ContainerID ||
				observation.Terminal.AttemptID != observation.AttemptID || observation.Terminal.WrapperEvidence != observation.WrapperEvidence {
				return errors.New("terminal observation scope does not match its wrapper identity")
			}
		}
	}
	expected, err := observationDigest(observation)
	if err != nil {
		return err
	}
	if observation.EvidenceSHA256 != expected {
		return errors.New("wrapper observation evidence does not match its facts")
	}
	return nil
}

// observationDigest hashes a canonical observation with its digest field omitted.
func observationDigest(observation Observation) (string, error) {
	observation.EvidenceSHA256 = ""
	encoded, err := json.Marshal(observation)
	if err != nil {
		return "", fmt.Errorf("encode wrapper observation: %w", err)
	}
	return bytesDigest(encoded), nil
}

// bytesDigest returns the lowercase SHA-256 digest of immutable evidence bytes.
func bytesDigest(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

// validDigest reports whether value is exactly one lowercase SHA-256 hexadecimal digest.
func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

// validateOpaque rejects separators, whitespace/control characters, and unbounded internal identifiers.
func validateOpaque(field, value string, maximum int) error {
	if value == "" || len(value) > maximum || strings.ContainsAny(value, `/\\`) {
		return fmt.Errorf("%s must be a bounded opaque identifier without path separators", field)
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return fmt.Errorf("%s contains whitespace or control characters", field)
		}
	}
	return nil
}

// validateAbsolutePath rejects ambiguous or root-level production artifact locations.
func validateAbsolutePath(field, path string) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return fmt.Errorf("%s must be a clean absolute non-root path", field)
	}
	if strings.ContainsRune(path, '\x00') {
		return fmt.Errorf("%s must not contain NUL", field)
	}
	return nil
}
