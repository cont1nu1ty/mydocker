package domain

import (
	"path/filepath"
	"strings"
	"time"
)

// EnvVar preserves one environment entry as structured name and value data.
type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Validate rejects values that exec-style APIs cannot represent safely.
func (v EnvVar) Validate() error {
	if v.Name == "" {
		return NewError(CodeInvalidArgument, "environment.name", "must not be empty")
	}
	if strings.ContainsAny(v.Name, "=\x00") {
		return NewError(CodeInvalidArgument, "environment.name", "must not contain '=' or NUL")
	}
	if strings.ContainsRune(v.Value, '\x00') {
		return NewError(CodeInvalidArgument, "environment.value", "must not contain NUL")
	}
	return nil
}

// TerminationPolicy expresses graceful-stop intent without inventing a default signal.
type TerminationPolicy struct {
	Signal           string        `json:"signal,omitempty"`
	GracePeriod      time.Duration `json:"grace_period,omitempty"`
	EscalationSignal string        `json:"escalation_signal,omitempty"`
}

// Validate accepts either an entirely unspecified policy or a complete explicit policy.
func (p TerminationPolicy) Validate() error {
	if p.GracePeriod < 0 {
		return NewError(CodeInvalidArgument, "termination.grace_period", "must not be negative")
	}
	if p.Signal == "" && p.GracePeriod == 0 && p.EscalationSignal == "" {
		return nil
	}
	if strings.TrimSpace(p.Signal) == "" || strings.ContainsRune(p.Signal, '\x00') {
		return NewError(CodeInvalidArgument, "termination.signal", "must be an explicit signal name")
	}
	if strings.TrimSpace(p.EscalationSignal) == "" || strings.ContainsRune(p.EscalationSignal, '\x00') {
		return NewError(CodeInvalidArgument, "termination.escalation_signal",
			"must be an explicit escalation signal name")
	}
	return nil
}

// ProcessSpec preserves argv and environment order without shell serialization.
type ProcessSpec struct {
	Argv             []string          `json:"argv"`
	Environment      []EnvVar          `json:"environment,omitempty"`
	WorkingDirectory string            `json:"working_directory,omitempty"`
	Termination      TerminationPolicy `json:"termination,omitempty"`
}

// Validate checks structured process data while preserving empty arguments and duplicate env names.
func (p ProcessSpec) Validate() error {
	if len(p.Argv) == 0 || p.Argv[0] == "" {
		return NewError(CodeInvalidArgument, "argv", "must contain a non-empty executable at index zero")
	}
	if !filepath.IsAbs(p.Argv[0]) || filepath.Clean(p.Argv[0]) != p.Argv[0] {
		return NewError(CodeInvalidArgument, "argv[0]", "must be a clean absolute executable path")
	}
	for _, arg := range p.Argv {
		if strings.ContainsRune(arg, '\x00') {
			return NewError(CodeInvalidArgument, "argv", "must not contain NUL")
		}
	}
	for _, variable := range p.Environment {
		if err := variable.Validate(); err != nil {
			return err
		}
	}
	if strings.ContainsRune(p.WorkingDirectory, '\x00') {
		return NewError(CodeInvalidArgument, "working_directory", "must not contain NUL")
	}
	if p.WorkingDirectory != "" && (!filepath.IsAbs(p.WorkingDirectory) || filepath.Clean(p.WorkingDirectory) != p.WorkingDirectory) {
		return NewError(CodeInvalidArgument, "working_directory", "must be empty or a clean absolute path")
	}
	return p.Termination.Validate()
}

// Clone returns process data with independent argv and environment slices.
func (p ProcessSpec) Clone() ProcessSpec {
	clone := p
	clone.Argv = append([]string(nil), p.Argv...)
	clone.Environment = append([]EnvVar(nil), p.Environment...)
	return clone
}

// KillPlan is a side-effect-free description for a later verified process controller.
type KillPlan struct {
	Signal           string
	GracePeriod      time.Duration
	EscalationSignal string
}

// NewKillPlan validates explicit graceful-stop intent without sending a signal.
func NewKillPlan(policy TerminationPolicy) (KillPlan, error) {
	if err := policy.Validate(); err != nil {
		return KillPlan{}, err
	}
	if policy.Signal == "" {
		return KillPlan{}, NewError(CodeInvalidArgument, "termination", "an explicit policy is required")
	}
	return KillPlan{
		Signal: policy.Signal, GracePeriod: policy.GracePeriod,
		EscalationSignal: policy.EscalationSignal,
	}, nil
}
