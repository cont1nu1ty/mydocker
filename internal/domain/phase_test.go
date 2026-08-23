package domain

import "testing"

// TestTransitionSandboxExhaustive verifies every legal and illegal edge in the Sandbox FSM.
func TestTransitionSandboxExhaustive(t *testing.T) {
	phases := []SandboxPhase{SandboxAbsent, SandboxCreating, SandboxReady, SandboxStopping, SandboxStopped}
	allowed := map[[2]SandboxPhase]bool{
		{SandboxAbsent, SandboxCreating}:  true,
		{SandboxCreating, SandboxReady}:   true,
		{SandboxCreating, SandboxAbsent}:  true,
		{SandboxReady, SandboxStopping}:   true,
		{SandboxStopping, SandboxStopped}: true,
		{SandboxStopped, SandboxAbsent}:   true,
	}
	for _, from := range phases {
		for _, to := range phases {
			got, err := TransitionSandbox(from, to)
			if allowed[[2]SandboxPhase{from, to}] {
				if err != nil || got != to {
					t.Fatalf("TransitionSandbox(%s, %s) = (%s, %v), want (%s, nil)", from, to, got, err, to)
				}
				continue
			}
			if !IsCode(err, CodeInvalidTransition) || got != from {
				t.Fatalf("TransitionSandbox(%s, %s) = (%s, %v), want unchanged invalid transition", from, to, got, err)
			}
		}
	}
}

// TestTransitionAttemptExhaustive verifies every legal and illegal edge in the Attempt FSM.
func TestTransitionAttemptExhaustive(t *testing.T) {
	phases := []AttemptPhase{AttemptAbsent, AttemptCreating, AttemptCreated, AttemptRunning, AttemptStopped}
	allowed := map[[2]AttemptPhase]bool{
		{AttemptAbsent, AttemptCreating}:  true,
		{AttemptCreating, AttemptCreated}: true,
		{AttemptCreating, AttemptStopped}: true,
		{AttemptCreated, AttemptRunning}:  true,
		{AttemptCreated, AttemptStopped}:  true,
		{AttemptRunning, AttemptStopped}:  true,
		{AttemptStopped, AttemptAbsent}:   true,
	}
	for _, from := range phases {
		for _, to := range phases {
			got, err := TransitionAttempt(from, to)
			if allowed[[2]AttemptPhase{from, to}] {
				if err != nil || got != to {
					t.Fatalf("TransitionAttempt(%s, %s) = (%s, %v), want (%s, nil)", from, to, got, err, to)
				}
				continue
			}
			if !IsCode(err, CodeInvalidTransition) || got != from {
				t.Fatalf("TransitionAttempt(%s, %s) = (%s, %v), want unchanged invalid transition", from, to, got, err)
			}
		}
	}
}

// TestIsActiveAttempt verifies exactly the three phases that block a second Attempt.
func TestIsActiveAttempt(t *testing.T) {
	want := map[AttemptPhase]bool{
		AttemptAbsent: false, AttemptCreating: true, AttemptCreated: true,
		AttemptRunning: true, AttemptStopped: false,
	}
	for phase, active := range want {
		if got := IsActiveAttempt(phase); got != active {
			t.Fatalf("IsActiveAttempt(%s) = %t, want %t", phase, got, active)
		}
	}
}
