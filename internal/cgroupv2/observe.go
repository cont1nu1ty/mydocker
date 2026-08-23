package cgroupv2

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"mydocker/internal/domain"
)

// ReadEffectiveLimits reads the three Attempt enforcement controls without changing them.
func (m *Manager) ReadEffectiveLimits(ctx context.Context, sandboxID domain.SandboxID, attemptID domain.AttemptID) (EffectiveLimits, error) {
	if err := ctx.Err(); err != nil {
		return EffectiveLimits{}, err
	}
	path, err := m.AttemptPath(sandboxID, attemptID)
	if err != nil {
		return EffectiveLimits{}, err
	}
	if err := m.verifyDirectory(path); err != nil {
		return EffectiveLimits{}, err
	}
	return m.readEffectiveLimitsPath(path)
}

// Membership reads and deterministically sorts current process IDs as observation data only.
func (m *Manager) Membership(ctx context.Context, sandboxID domain.SandboxID, attemptID domain.AttemptID) ([]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := m.AttemptPath(sandboxID, attemptID)
	if err != nil {
		return nil, err
	}
	if err := m.verifyDirectory(path); err != nil {
		return nil, err
	}
	return m.readMembershipPath(path)
}

// KeeperMembership reads and deterministically sorts process IDs from the fixed keeper leaf without mutating placement.
func (m *Manager) KeeperMembership(ctx context.Context, sandboxID domain.SandboxID) ([]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := m.KeeperPath(sandboxID)
	if err != nil {
		return nil, err
	}
	if err := m.verifyDirectory(path); err != nil {
		return nil, err
	}
	return m.readMembershipPath(path)
}

// ReadCurrent reads memory.current and pids.current as point-in-time observations.
func (m *Manager) ReadCurrent(ctx context.Context, sandboxID domain.SandboxID, attemptID domain.AttemptID) (Current, error) {
	if err := ctx.Err(); err != nil {
		return Current{}, err
	}
	path, err := m.AttemptPath(sandboxID, attemptID)
	if err != nil {
		return Current{}, err
	}
	if err := m.verifyDirectory(path); err != nil {
		return Current{}, err
	}
	return m.readCurrentPath(path)
}

// ReadEvents reads the populated and frozen facts used for lifecycle observation and safe cleanup.
func (m *Manager) ReadEvents(ctx context.Context, sandboxID domain.SandboxID, attemptID domain.AttemptID) (Events, error) {
	if err := ctx.Err(); err != nil {
		return Events{}, err
	}
	path, err := m.AttemptPath(sandboxID, attemptID)
	if err != nil {
		return Events{}, err
	}
	if err := m.verifyDirectory(path); err != nil {
		return Events{}, err
	}
	return m.readEventsPath(path)
}

// SnapshotOOM reads local OOM counters so callers can compare the same Attempt cgroup before and after execution.
func (m *Manager) SnapshotOOM(ctx context.Context, sandboxID domain.SandboxID, attemptID domain.AttemptID) (OOMSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return OOMSnapshot{}, err
	}
	path, err := m.AttemptPath(sandboxID, attemptID)
	if err != nil {
		return OOMSnapshot{}, err
	}
	if err := m.verifyDirectory(path); err != nil {
		return OOMSnapshot{}, err
	}
	return m.readOOMPath(path)
}

// ObserveAttempt captures all bounded Attempt observations while keeping them separate from lifecycle truth.
func (m *Manager) ObserveAttempt(ctx context.Context, sandboxID domain.SandboxID, attemptID domain.AttemptID) (AttemptObservation, error) {
	if err := ctx.Err(); err != nil {
		return AttemptObservation{}, err
	}
	path, err := m.AttemptPath(sandboxID, attemptID)
	if err != nil {
		return AttemptObservation{}, err
	}
	if err := m.verifyDirectory(path); err != nil {
		return AttemptObservation{}, err
	}
	membership, err := m.readMembershipPath(path)
	if err != nil {
		return AttemptObservation{}, err
	}
	current, err := m.readCurrentPath(path)
	if err != nil {
		return AttemptObservation{}, err
	}
	events, err := m.readEventsPath(path)
	if err != nil {
		return AttemptObservation{}, err
	}
	oom, err := m.readOOMPath(path)
	if err != nil {
		return AttemptObservation{}, err
	}
	return AttemptObservation{Membership: membership, Current: current, Events: events, OOM: oom}, nil
}

// readEffectiveLimitsPath parses kernel readback for cpu.max, memory.max, and pids.max.
func (m *Manager) readEffectiveLimitsPath(path string) (EffectiveLimits, error) {
	cpuRaw, err := m.fs.ReadFile(filepath.Join(path, "cpu.max"))
	if err != nil {
		return EffectiveLimits{}, fmt.Errorf("read cpu.max: %w", err)
	}
	cpu, err := parseCPUMax(string(cpuRaw))
	if err != nil {
		return EffectiveLimits{}, err
	}
	memoryRaw, err := m.fs.ReadFile(filepath.Join(path, "memory.max"))
	if err != nil {
		return EffectiveLimits{}, fmt.Errorf("read memory.max: %w", err)
	}
	memory, err := parseScalarLimit("memory.max", string(memoryRaw))
	if err != nil {
		return EffectiveLimits{}, err
	}
	pidsRaw, err := m.fs.ReadFile(filepath.Join(path, "pids.max"))
	if err != nil {
		return EffectiveLimits{}, fmt.Errorf("read pids.max: %w", err)
	}
	pids, err := parseScalarLimit("pids.max", string(pidsRaw))
	if err != nil {
		return EffectiveLimits{}, err
	}
	return EffectiveLimits{CPU: cpu, Memory: memory, Pids: pids}, nil
}

// readMembershipPath parses positive PIDs from cgroup.procs and rejects malformed kernel evidence.
func (m *Manager) readMembershipPath(path string) ([]int, error) {
	value, err := m.fs.ReadFile(filepath.Join(path, "cgroup.procs"))
	if err != nil {
		return nil, fmt.Errorf("read cgroup.procs: %w", err)
	}
	members := make([]int, 0)
	seen := make(map[int]struct{})
	for _, field := range strings.Fields(string(value)) {
		pid, parseErr := strconv.Atoi(field)
		if parseErr != nil || pid <= 0 {
			return nil, fmt.Errorf("invalid cgroup.procs value %q: %w", field, ErrUnknownState)
		}
		if _, exists := seen[pid]; exists {
			return nil, fmt.Errorf("duplicate cgroup.procs PID %d: %w", pid, ErrUnknownState)
		}
		seen[pid] = struct{}{}
		members = append(members, pid)
	}
	sort.Ints(members)
	return members, nil
}

// readCurrentPath parses the two unsigned current counters required by M2 observations.
func (m *Manager) readCurrentPath(path string) (Current, error) {
	memory, err := m.readUintFile(filepath.Join(path, "memory.current"), "memory.current")
	if err != nil {
		return Current{}, err
	}
	pids, err := m.readUintFile(filepath.Join(path, "pids.current"), "pids.current")
	if err != nil {
		return Current{}, err
	}
	return Current{MemoryBytes: memory, Pids: pids}, nil
}

// readEventsPath requires an explicit populated value and accepts the optional frozen value.
func (m *Manager) readEventsPath(path string) (Events, error) {
	value, err := m.fs.ReadFile(filepath.Join(path, "cgroup.events"))
	if err != nil {
		return Events{}, fmt.Errorf("read cgroup.events: %w: %w", err, ErrUnknownState)
	}
	fields, err := parseKeyValues("cgroup.events", string(value))
	if err != nil {
		return Events{}, err
	}
	populated, exists := fields["populated"]
	if !exists || populated > 1 {
		return Events{}, fmt.Errorf("cgroup.events lacks a boolean populated value: %w", ErrUnknownState)
	}
	frozen := uint64(0)
	if value, ok := fields["frozen"]; ok {
		if value > 1 {
			return Events{}, fmt.Errorf("cgroup.events has invalid frozen value: %w", ErrUnknownState)
		}
		frozen = value
	}
	return Events{Populated: populated == 1, Frozen: frozen == 1}, nil
}

// readOOMPath parses the local counters needed to attribute OOM evidence to one Attempt.
func (m *Manager) readOOMPath(path string) (OOMSnapshot, error) {
	value, err := m.fs.ReadFile(filepath.Join(path, "memory.events.local"))
	if err != nil {
		return OOMSnapshot{}, fmt.Errorf("read memory.events.local: %w", err)
	}
	fields, err := parseKeyValues("memory.events.local", string(value))
	if err != nil {
		return OOMSnapshot{}, err
	}
	oom, hasOOM := fields["oom"]
	oomKill, hasOOMKill := fields["oom_kill"]
	if !hasOOM || !hasOOMKill {
		return OOMSnapshot{}, fmt.Errorf("memory.events.local lacks oom or oom_kill: %w", ErrUnknownState)
	}
	return OOMSnapshot{OOM: oom, OOMKill: oomKill, OOMGroupKill: fields["oom_group_kill"]}, nil
}

// readUintFile parses one unsigned scalar pseudo-file and rejects extra tokens.
func (m *Manager) readUintFile(path, name string) (uint64, error) {
	value, err := m.fs.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", name, err)
	}
	fields := strings.Fields(string(value))
	if len(fields) != 1 {
		return 0, fmt.Errorf("%s has invalid scalar syntax: %w", name, ErrUnknownState)
	}
	parsed, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w: %w", name, err, ErrUnknownState)
	}
	return parsed, nil
}

// parseCPUMax validates fixed-period cpu.max readback and preserves max explicitly.
func parseCPUMax(value string) (CPUMax, error) {
	fields := strings.Fields(value)
	if len(fields) != 2 {
		return CPUMax{}, fmt.Errorf("cpu.max has invalid syntax: %w", ErrUnknownState)
	}
	period, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil || period != CPUPeriodMicros {
		return CPUMax{}, fmt.Errorf("cpu.max period %q is not %d: %w", fields[1], CPUPeriodMicros, ErrUnknownState)
	}
	if fields[0] == "max" {
		return CPUMax{Unlimited: true, PeriodMicros: period}, nil
	}
	quota, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil || quota == 0 {
		return CPUMax{}, fmt.Errorf("cpu.max quota %q is invalid: %w", fields[0], ErrUnknownState)
	}
	return CPUMax{QuotaMicros: quota, PeriodMicros: period}, nil
}

// parseScalarLimit validates one numeric-or-max cgroup control value.
func parseScalarLimit(name, value string) (ScalarLimit, error) {
	fields := strings.Fields(value)
	if len(fields) != 1 {
		return ScalarLimit{}, fmt.Errorf("%s has invalid syntax: %w", name, ErrUnknownState)
	}
	if fields[0] == "max" {
		return ScalarLimit{Unlimited: true}, nil
	}
	parsed, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil || parsed == 0 {
		return ScalarLimit{}, fmt.Errorf("%s value %q is invalid: %w", name, fields[0], ErrUnknownState)
	}
	return ScalarLimit{Value: parsed}, nil
}

// parseKeyValues parses newline key/value counters while rejecting duplicates, odd fields, and invalid integers.
func parseKeyValues(name, value string) (map[string]uint64, error) {
	fields := strings.Fields(value)
	if len(fields)%2 != 0 {
		return nil, fmt.Errorf("%s has an incomplete key/value pair: %w", name, ErrUnknownState)
	}
	parsed := make(map[string]uint64, len(fields)/2)
	for index := 0; index < len(fields); index += 2 {
		key := fields[index]
		if _, exists := parsed[key]; exists {
			return nil, fmt.Errorf("%s duplicates key %q: %w", name, key, ErrUnknownState)
		}
		counter, err := strconv.ParseUint(fields[index+1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s key %q: %w: %w", name, key, err, ErrUnknownState)
		}
		parsed[key] = counter
	}
	return parsed, nil
}
