// Package lifecycle coordinates pure M1 lifecycle intent and confirmation.
// It persists facts atomically but performs no host, process, or Linux action.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/state"
)

var (
	// ErrVerificationRequired reports a missing, false, or mismatched external verification fact.
	ErrVerificationRequired = errors.New("explicit lifecycle verification is required")
	// ErrOperationType reports use of a durable operation with the wrong coordinator method.
	ErrOperationType = errors.New("operation type does not match lifecycle method")
	// ErrProcessVerificationUnavailable reports that no action-time ownership verifier was configured.
	ErrProcessVerificationUnavailable = errors.New("action-time process identity verification is unavailable")
)

// VerificationKind identifies the concrete externally observed fact used by a Confirm method.
type VerificationKind string

const (
	// VerificationSandboxReady confirms that Sandbox create resources were independently observed ready.
	VerificationSandboxReady VerificationKind = "sandbox_ready"
	// VerificationSandboxStopped confirms that Sandbox activity was independently observed stopped.
	VerificationSandboxStopped VerificationKind = "sandbox_stopped"
	// VerificationSandboxAbsent confirms that all Sandbox-owned resources were independently observed absent.
	VerificationSandboxAbsent VerificationKind = "sandbox_absent"
	// VerificationAttemptCreated confirms that preparation and the closed start gate were independently observed.
	VerificationAttemptCreated VerificationKind = "attempt_created"
	// VerificationAttemptRunning confirms that the workload and its strong identity were independently observed running.
	VerificationAttemptRunning VerificationKind = "attempt_running"
	// VerificationAttemptStopped confirms that terminal process state and result evidence were independently observed.
	VerificationAttemptStopped VerificationKind = "attempt_stopped"
	// VerificationAttemptAbsent confirms that all Attempt-owned resources were independently observed absent.
	VerificationAttemptAbsent VerificationKind = "attempt_absent"
)

// Valid reports whether kind belongs to the bounded M1 verification vocabulary.
func (kind VerificationKind) Valid() bool {
	switch kind {
	case VerificationSandboxReady, VerificationSandboxStopped, VerificationSandboxAbsent,
		VerificationAttemptCreated, VerificationAttemptRunning, VerificationAttemptStopped,
		VerificationAttemptAbsent:
		return true
	default:
		return false
	}
}

// Verification carries an explicit external observation; it never performs the observation itself.
type Verification struct {
	Kind            VerificationKind        `json:"kind"`
	Verified        bool                    `json:"verified"`
	Evidence        string                  `json:"evidence"`
	ObservedAt      time.Time               `json:"observed_at"`
	Duration        operation.Duration      `json:"duration_ns"`
	ProcessIdentity *domain.ProcessIdentity `json:"process_identity,omitempty"`
	Streams         domain.StreamReferences `json:"streams"`
}

// Clone returns verification data without retaining a mutable process-identity pointer.
func (v Verification) Clone() Verification {
	clone := v
	if v.ProcessIdentity != nil {
		identity := *v.ProcessIdentity
		clone.ProcessIdentity = &identity
	}
	return clone
}

// Validate checks that verification is explicit, bounded, timestamped, and persistence safe.
func (v Verification) Validate() error {
	if !v.Kind.Valid() || !v.Verified || strings.TrimSpace(v.Evidence) == "" || strings.ContainsRune(v.Evidence, '\x00') || v.ObservedAt.IsZero() {
		return ErrVerificationRequired
	}
	if err := v.Duration.Validate(); err != nil {
		return fmt.Errorf("verification duration: %w", err)
	}
	if err := v.Streams.Validate(); err != nil {
		return err
	}
	if v.ProcessIdentity != nil {
		if err := v.ProcessIdentity.Validate(); err != nil {
			return fmt.Errorf("verification process identity: %w", err)
		}
	}
	return nil
}

// validateFor checks general verification integrity plus the method-specific expected observation.
func (v Verification) validateFor(expected VerificationKind) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if v.Kind != expected {
		return fmt.Errorf("%w: got %q, want %q", ErrVerificationRequired, v.Kind, expected)
	}
	return nil
}

// SandboxCreateRequest is the semantic create intent bound to one client operation ID.
type SandboxCreateRequest struct {
	OperationID operation.OperationID
	SandboxID   domain.SandboxID
	Spec        domain.SandboxSpec
}

// SandboxActionRequest binds stop or remove intent to one Sandbox and client operation ID.
type SandboxActionRequest struct {
	OperationID operation.OperationID
	SandboxID   domain.SandboxID
}

// SandboxConfirmRequest confirms one prior Sandbox intent using its canonical fingerprint.
type SandboxConfirmRequest struct {
	OperationID  operation.OperationID
	SandboxID    domain.SandboxID
	Fingerprint  operation.RequestFingerprint
	Verification Verification
}

// ContainerCreateRequest is the semantic one-to-one Container/Attempt create intent.
type ContainerCreateRequest struct {
	OperationID operation.OperationID
	SandboxID   domain.SandboxID
	ContainerID domain.ContainerID
	AttemptID   domain.AttemptID
	Process     domain.ProcessSpec
	ImageDigest string
	RootFS      string
}

// ContainerActionRequest binds start or delete intent to one Container and client operation ID.
type ContainerActionRequest struct {
	OperationID operation.OperationID
	ContainerID domain.ContainerID
}

// ContainerConfirmRequest confirms one prior Container intent using its canonical fingerprint.
type ContainerConfirmRequest struct {
	OperationID  operation.OperationID
	ContainerID  domain.ContainerID
	Fingerprint  operation.RequestFingerprint
	Verification Verification
}

// ContainerStartTerminalRequest records a wrapper terminal fact observed after gate release but before Running confirmation.
type ContainerStartTerminalRequest struct {
	OperationID  operation.OperationID
	ContainerID  domain.ContainerID
	Fingerprint  operation.RequestFingerprint
	Outcome      domain.Outcome
	Conditions   []domain.Condition
	Verification Verification
}

// Failure identifies a bounded terminal operation cause while detailed provider diagnostics remain in events.
type Failure struct {
	Reason  operation.ReasonClass `json:"reason"`
	Message string                `json:"message"`
}

// Validate rejects an unclassified or persistence-unsafe lifecycle failure.
func (f Failure) Validate() error {
	if !f.Reason.Valid() || f.Reason == operation.ReasonNone {
		return errors.New("lifecycle failure requires a non-none reason class")
	}
	if strings.TrimSpace(f.Message) == "" || strings.ContainsRune(f.Message, '\x00') || len(f.Message) > 4096 {
		return errors.New("lifecycle failure message must be non-empty, bounded, and contain no NUL")
	}
	return nil
}

// SandboxCreateFailureRequest finalizes one failed create only after rollback and complete host absence verification.
type SandboxCreateFailureRequest struct {
	OperationID  operation.OperationID
	SandboxID    domain.SandboxID
	Fingerprint  operation.RequestFingerprint
	Failure      Failure
	Verification Verification
}

// ContainerCreateFailureRequest retains one stopped historical pair after verified create cleanup.
type ContainerCreateFailureRequest struct {
	OperationID  operation.OperationID
	ContainerID  domain.ContainerID
	Fingerprint  operation.RequestFingerprint
	Failure      Failure
	Verification Verification
}

// RecordStoppedRequest atomically records externally verified terminal facts as a standalone stop operation.
type RecordStoppedRequest struct {
	OperationID  operation.OperationID
	ContainerID  domain.ContainerID
	Outcome      domain.Outcome
	Conditions   []domain.Condition
	Verification Verification
}

// RequestFingerprint returns the immutable terminal-observation semantics used
// by BeginRecordStopped and RecordStopped. Verification transport facts are not
// part of idempotency, matching the lifecycle coordinator's replay contract.
func (request RecordStoppedRequest) RequestFingerprint() (operation.RequestFingerprint, error) {
	semantic := stoppedSemantic{
		ContainerID: request.ContainerID,
		Outcome:     request.Outcome,
		Conditions:  append([]domain.Condition(nil), request.Conditions...),
	}
	fingerprint, err := operation.CanonicalRequestFingerprint(semantic)
	if err != nil {
		return operation.RequestFingerprint{}, fmt.Errorf("canonical stopped-observation fingerprint: %w", err)
	}
	return fingerprint, nil
}

// KillRequest durably binds a side-effect-free kill plan or an already-stopped no-op to one Attempt.
type KillRequest struct {
	OperationID operation.OperationID
	ContainerID domain.ContainerID
	Policy      domain.TerminationPolicy
}

// KillStoppedRequest confirms a previously planned kill by recording verified terminal facts.
type KillStoppedRequest struct {
	OperationID  operation.OperationID
	ContainerID  domain.ContainerID
	Fingerprint  operation.RequestFingerprint
	Outcome      domain.Outcome
	Conditions   []domain.Condition
	Verification Verification
}

// SandboxResult reports retry resolution, durable operation, fingerprint, and an optional retained Sandbox.
type SandboxResult struct {
	Resolution  operation.Resolution
	Operation   operation.Operation
	Fingerprint operation.RequestFingerprint
	Sandbox     *domain.Sandbox
	Removed     bool
}

// Clone returns a Sandbox result whose nested records and response bytes are independent.
func (r SandboxResult) Clone() SandboxResult {
	clone := r
	clone.Operation = r.Operation.Clone()
	if r.Sandbox != nil {
		sandbox := r.Sandbox.Clone()
		clone.Sandbox = &sandbox
	}
	return clone
}

// ContainerResult reports retry resolution, durable operation, fingerprint, and an optional retained pair.
type ContainerResult struct {
	Resolution       operation.Resolution
	Operation        operation.Operation
	Fingerprint      operation.RequestFingerprint
	ContainerAttempt *domain.ContainerAttempt
	HostBinding      *ContainerHostBinding
	Removed          bool
}

// Clone returns a Container result whose aggregate and response bytes are independent.
func (r ContainerResult) Clone() ContainerResult {
	clone := r
	clone.Operation = r.Operation.Clone()
	if r.ContainerAttempt != nil {
		pair := r.ContainerAttempt.Clone()
		clone.ContainerAttempt = &pair
	}
	if r.HostBinding != nil {
		binding := *r.HostBinding
		clone.HostBinding = &binding
	}
	return clone
}

// ContainerHostBinding is the durable Container/Attempt and acquisition-owner
// identity used by transport-adjacent artifact registries. It contains no host
// path, PID, descriptor, or authority beyond matching an already-owned record.
type ContainerHostBinding struct {
	ContainerID domain.ContainerID `json:"container_id"`
	AttemptID   domain.AttemptID   `json:"attempt_id"`
	Generation  domain.Generation  `json:"generation"`
	Owner       ownership.OwnerKey `json:"owner"`
}

// Validate rejects a binding that could redirect one Container's artifact
// lookup to another target or omit its immutable Attempt incarnation.
func (binding ContainerHostBinding) Validate() error {
	if err := binding.ContainerID.Validate(); err != nil {
		return err
	}
	if err := binding.AttemptID.Validate(); err != nil {
		return err
	}
	if err := binding.Owner.Validate(); err != nil {
		return err
	}
	if binding.Generation == 0 || binding.Owner.Generation != binding.Generation {
		return errors.New("Container host binding generation differs from its owner")
	}
	if binding.Owner.Target.Kind != operation.TargetContainer || binding.Owner.Target.ID != string(binding.ContainerID) {
		return errors.New("Container host binding owner does not target its exact Container")
	}
	return nil
}

// KillResult reports retry resolution, terminal state, and any actionable plan tied to reverified opaque identity.
type KillResult struct {
	Resolution       operation.Resolution
	Operation        operation.Operation
	Fingerprint      operation.RequestFingerprint
	Plan             domain.KillPlan
	Actionable       bool
	ProcessIdentity  domain.ProcessIdentity
	ContainerAttempt *domain.ContainerAttempt
}

// Clone returns a Kill result whose operation and optional pair are independent.
func (r KillResult) Clone() KillResult {
	clone := r
	clone.Operation = r.Operation.Clone()
	if r.ContainerAttempt != nil {
		pair := r.ContainerAttempt.Clone()
		clone.ContainerAttempt = &pair
	}
	return clone
}

// Clock supplies event wall facts while duration remains an explicit monotonic sample.
type Clock interface {
	Now() time.Time
}

// ProcessIdentityVerifier reverifies opaque process ownership before returning an active plan that a caller may execute.
// It authorizes no signal by itself, is not used for terminal no-op replay, and must not trust persisted shape alone.
type ProcessIdentityVerifier interface {
	Verify(ctx context.Context, target operation.Target, identity domain.ProcessIdentity) error
}

// wallClock supplies production wall facts without claiming cross-process monotonic semantics.
type wallClock struct{}

// Now returns the current wall timestamp for event diagnostics.
func (wallClock) Now() time.Time { return time.Now() }

// unavailableProcessVerifier fails closed when a caller did not configure action-time ownership verification.
type unavailableProcessVerifier struct{}

// Verify rejects kill planning because persisted identity shape cannot prove current process ownership.
func (unavailableProcessVerifier) Verify(context.Context, operation.Target, domain.ProcessIdentity) error {
	return ErrProcessVerificationUnavailable
}

// Coordinator owns pure lifecycle transaction orchestration over a supplied Store.
type Coordinator struct {
	store    state.Store
	clock    Clock
	verifier ProcessIdentityVerifier
	profile  state.HostProfile
}

// NewCoordinator constructs a coordinator with the wall clock and an optional read-only action-time identity verifier.
func NewCoordinator(store state.Store, verifier ...ProcessIdentityVerifier) (*Coordinator, error) {
	return NewCoordinatorWithClockForProfile(store, wallClock{}, state.HostProfileAbstractM1, verifier...)
}

// NewCoordinatorWithClock constructs a deterministic coordinator using the supplied event clock.
func NewCoordinatorWithClock(store state.Store, clock Clock, verifier ...ProcessIdentityVerifier) (*Coordinator, error) {
	return NewCoordinatorWithClockForProfile(store, clock, state.HostProfileAbstractM1, verifier...)
}

// NewCoordinatorForProfile constructs a coordinator whose operations explicitly require the selected host resource contract.
func NewCoordinatorForProfile(store state.Store, profile state.HostProfile, verifier ...ProcessIdentityVerifier) (*Coordinator, error) {
	return NewCoordinatorWithClockForProfile(store, wallClock{}, profile, verifier...)
}

// NewCoordinatorWithClockForProfile constructs a deterministic coordinator with an explicit persisted host profile.
func NewCoordinatorWithClockForProfile(store state.Store, clock Clock, profile state.HostProfile, verifier ...ProcessIdentityVerifier) (*Coordinator, error) {
	if store == nil {
		return nil, errors.New("lifecycle store must not be nil")
	}
	if clock == nil {
		return nil, errors.New("lifecycle clock must not be nil")
	}
	if !profile.Valid() {
		return nil, errors.New("lifecycle host profile must be explicit and supported")
	}
	if len(verifier) > 1 {
		return nil, errors.New("lifecycle accepts at most one process identity verifier")
	}
	selectedVerifier := ProcessIdentityVerifier(unavailableProcessVerifier{})
	if len(verifier) == 1 {
		if verifier[0] == nil {
			return nil, errors.New("lifecycle process identity verifier must not be nil")
		}
		selectedVerifier = verifier[0]
	}
	return &Coordinator{store: store, clock: clock, verifier: selectedVerifier, profile: profile}, nil
}
