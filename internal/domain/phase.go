package domain

// SandboxPhase is the bounded lifecycle state of a stable Sandbox environment.
type SandboxPhase string

const (
	// SandboxAbsent represents a verified missing Sandbox record/resource.
	SandboxAbsent SandboxPhase = "absent"
	// SandboxCreating represents accepted create intent not yet verified ready.
	SandboxCreating SandboxPhase = "creating"
	// SandboxReady represents a verified environment that may accept an Attempt.
	SandboxReady SandboxPhase = "ready"
	// SandboxStopping represents accepted stop intent not yet verified stopped.
	SandboxStopping SandboxPhase = "stopping"
	// SandboxStopped represents a quiescent retained environment awaiting removal.
	SandboxStopped SandboxPhase = "stopped"
)

// String returns the stable persistence spelling used in diagnostics.
func (p SandboxPhase) String() string {
	return string(p)
}

// Valid reports whether a Sandbox phase belongs to the M1 state machine.
func (p SandboxPhase) Valid() bool {
	switch p {
	case SandboxAbsent, SandboxCreating, SandboxReady, SandboxStopping, SandboxStopped:
		return true
	default:
		return false
	}
}

// TransitionSandbox validates one adjacent Sandbox state change and returns its target.
func TransitionSandbox(from, to SandboxPhase) (SandboxPhase, error) {
	allowed := map[SandboxPhase]map[SandboxPhase]bool{
		SandboxAbsent:   {SandboxCreating: true},
		SandboxCreating: {SandboxReady: true, SandboxAbsent: true},
		SandboxReady:    {SandboxStopping: true},
		SandboxStopping: {SandboxStopped: true},
		SandboxStopped:  {SandboxAbsent: true},
	}
	if !from.Valid() || !to.Valid() || !allowed[from][to] {
		return from, transitionError("Sandbox", from, to)
	}
	return to, nil
}

// AttemptPhase is the bounded lifecycle state of one execution Attempt.
type AttemptPhase string

const (
	// AttemptAbsent represents a verified missing Attempt.
	AttemptAbsent AttemptPhase = "absent"
	// AttemptCreating represents accepted create intent not yet verified created.
	AttemptCreating AttemptPhase = "creating"
	// AttemptCreated represents a prepared Attempt held behind the start gate.
	AttemptCreated AttemptPhase = "created"
	// AttemptRunning represents an Attempt whose workload start was verified.
	AttemptRunning AttemptPhase = "running"
	// AttemptStopped represents a terminal or safely rolled-back retained record.
	AttemptStopped AttemptPhase = "stopped"
)

// String returns the stable persistence spelling used in diagnostics.
func (p AttemptPhase) String() string {
	return string(p)
}

// Valid reports whether an Attempt phase belongs to the M1 state machine.
func (p AttemptPhase) Valid() bool {
	switch p {
	case AttemptAbsent, AttemptCreating, AttemptCreated, AttemptRunning, AttemptStopped:
		return true
	default:
		return false
	}
}

// TransitionAttempt validates one adjacent Attempt state change and returns its target.
func TransitionAttempt(from, to AttemptPhase) (AttemptPhase, error) {
	allowed := map[AttemptPhase]map[AttemptPhase]bool{
		AttemptAbsent:   {AttemptCreating: true},
		AttemptCreating: {AttemptCreated: true, AttemptStopped: true},
		AttemptCreated:  {AttemptRunning: true, AttemptStopped: true},
		AttemptRunning:  {AttemptStopped: true},
		AttemptStopped:  {AttemptAbsent: true},
	}
	if !from.Valid() || !to.Valid() || !allowed[from][to] {
		return from, transitionError("Attempt", from, to)
	}
	return to, nil
}

// IsActiveAttempt reports whether a phase blocks another Attempt in the same Sandbox.
func IsActiveAttempt(phase AttemptPhase) bool {
	return phase == AttemptCreating || phase == AttemptCreated || phase == AttemptRunning
}
