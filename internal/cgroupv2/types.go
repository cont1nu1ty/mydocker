package cgroupv2

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"

	"mydocker/internal/domain"
)

const (
	// CPUPeriodMicros is the fixed cgroup v2 cpu.max period used by M2.
	CPUPeriodMicros uint64 = 100_000
	// MinimumCPUQuotaMicros is the kernel-supported quota floor paired with CPUPeriodMicros.
	MinimumCPUQuotaMicros uint64 = 1_000
	// DefaultPidsLimit mirrors the domain default persisted in every resolved ContainerSpec.
	DefaultPidsLimit uint64 = uint64(domain.DefaultPidsLimit)
)

var (
	// ErrUnsupported reports a host or configured root that is not cgroup v2.
	ErrUnsupported = errors.New("cgroup v2 is unsupported")
	// ErrOutsideRoot reports a derived path that escaped the configured delegated root.
	ErrOutsideRoot = errors.New("cgroup path escapes configured root")
	// ErrUnknownState reports missing or malformed kernel evidence that prevents a safe action.
	ErrUnknownState = errors.New("cgroup state is unknown")
	// ErrPopulated reports a cgroup that still contains one or more processes.
	ErrPopulated = errors.New("cgroup is populated")
	// ErrBusy reports a cgroup that still owns child cgroups or cannot be removed exactly.
	ErrBusy = errors.New("cgroup is busy")
	// ErrEffectiveMismatch reports kernel readback that differs from requested controls or verified membership.
	ErrEffectiveMismatch = errors.New("effective cgroup readback does not match expected state")
)

// Config identifies the dedicated cgroup v2 subtree owned by one Manager.
// Root must already exist, be an absolute non-root directory, and be delegated
// to mydocker; the manager never creates or takes ownership of its parent.
type Config struct {
	Root string
}

// normalize validates the configured ownership boundary and returns its canonical lexical path.
func (c Config) normalize() (Config, error) {
	if c.Root == "" || strings.ContainsRune(c.Root, '\x00') || !filepath.IsAbs(c.Root) {
		return Config{}, fmt.Errorf("cgroup root must be a non-empty absolute path")
	}
	clean := filepath.Clean(c.Root)
	if clean == string(filepath.Separator) || clean == "." {
		return Config{}, fmt.Errorf("cgroup root %q is too broad", c.Root)
	}
	return Config{Root: clean}, nil
}

// SandboxCgroup identifies the deterministic parent cgroup owned by a Sandbox.
type SandboxCgroup struct {
	SandboxID domain.SandboxID
	Path      string
}

// KeeperCgroup identifies the fixed process-bearing leaf below one process-free Sandbox parent.
type KeeperCgroup struct {
	SandboxID domain.SandboxID
	Path      string
}

// AttemptCgroup identifies the deterministic child cgroup owned by one Attempt.
type AttemptCgroup struct {
	SandboxID domain.SandboxID
	AttemptID domain.AttemptID
	Path      string
}

// ScalarLimit represents either an explicit numeric cgroup limit or the kernel spelling max.
type ScalarLimit struct {
	Unlimited bool
	Value     uint64
}

// Equal reports whether two parsed scalar limits have identical kernel semantics.
func (l ScalarLimit) Equal(other ScalarLimit) bool {
	return l.Unlimited == other.Unlimited && l.Value == other.Value
}

// CPUMax is the parsed quota and fixed period represented by cpu.max.
type CPUMax struct {
	Unlimited    bool
	QuotaMicros  uint64
	PeriodMicros uint64
}

// Equal reports whether two cpu.max values have identical quota and period semantics.
func (m CPUMax) Equal(other CPUMax) bool {
	return m.Unlimited == other.Unlimited && m.QuotaMicros == other.QuotaMicros && m.PeriodMicros == other.PeriodMicros
}

// EffectiveLimits contains values read back from an Attempt cgroup after all writes complete.
type EffectiveLimits struct {
	CPU    CPUMax
	Memory ScalarLimit
	Pids   ScalarLimit
}

// Equal reports whether every effective controller value has identical semantics.
func (l EffectiveLimits) Equal(other EffectiveLimits) bool {
	return l.CPU.Equal(other.CPU) && l.Memory.Equal(other.Memory) && l.Pids.Equal(other.Pids)
}

// Current reports point-in-time counters and must not be used as lifecycle truth.
type Current struct {
	MemoryBytes uint64
	Pids        uint64
}

// Events reports the bounded cgroup.events facts required for safe cleanup.
type Events struct {
	Populated bool
	Frozen    bool
}

// OOMSnapshot records monotonic memory.events.local counters for one Attempt cgroup.
type OOMSnapshot struct {
	OOM          uint64
	OOMKill      uint64
	OOMGroupKill uint64
}

// OOMDelta records the increase between two trustworthy snapshots.
type OOMDelta struct {
	OOM          uint64
	OOMKill      uint64
	OOMGroupKill uint64
}

// Killed reports whether local cgroup evidence recorded at least one OOM kill.
func (d OOMDelta) Killed() bool {
	return d.OOMKill > 0 || d.OOMGroupKill > 0
}

// Delta subtracts an earlier OOM snapshot and fails closed if any kernel counter regressed.
func (s OOMSnapshot) Delta(earlier OOMSnapshot) (OOMDelta, error) {
	if s.OOM < earlier.OOM || s.OOMKill < earlier.OOMKill || s.OOMGroupKill < earlier.OOMGroupKill {
		return OOMDelta{}, fmt.Errorf("OOM counters regressed: %w", ErrUnknownState)
	}
	return OOMDelta{
		OOM:          s.OOM - earlier.OOM,
		OOMKill:      s.OOMKill - earlier.OOMKill,
		OOMGroupKill: s.OOMGroupKill - earlier.OOMGroupKill,
	}, nil
}

// AttemptObservation groups membership, current counters, lifecycle events, and OOM evidence from one read pass.
type AttemptObservation struct {
	Membership []int
	Current    Current
	Events     Events
	OOM        OOMSnapshot
}

// planLimits converts an already resolved Container policy into raw controller writes and canonical host readback expectations.
func planLimits(limits domain.ResolvedResourceLimits, pageSize uint64) (EffectiveLimits, EffectiveLimits, error) {
	if err := limits.Validate(); err != nil {
		return EffectiveLimits{}, EffectiveLimits{}, err
	}
	if pageSize == 0 || pageSize&(pageSize-1) != 0 {
		return EffectiveLimits{}, EffectiveLimits{}, fmt.Errorf("host page size %d must be a positive power of two", pageSize)
	}
	cpu := CPUMax{Unlimited: true, PeriodMicros: CPUPeriodMicros}
	if !limits.CPUUnlimited {
		quota, err := cpuQuotaMicros(*limits.CPULimitMilli)
		if err != nil {
			return EffectiveLimits{}, EffectiveLimits{}, err
		}
		cpu = CPUMax{QuotaMicros: quota, PeriodMicros: CPUPeriodMicros}
	}
	memory := ScalarLimit{Unlimited: true}
	if !limits.MemoryUnlimited {
		memory = ScalarLimit{Value: uint64(*limits.MemoryLimitBytes)}
	}
	pids := ScalarLimit{Value: uint64(limits.PidsLimit)}
	writes := EffectiveLimits{CPU: cpu, Memory: memory, Pids: pids}
	effective := writes
	if !memory.Unlimited {
		canonical, err := roundMemoryToPage(memory.Value, pageSize)
		if err != nil {
			return EffectiveLimits{}, EffectiveLimits{}, err
		}
		effective.Memory.Value = canonical
	}
	return writes, effective, nil
}

// cpuQuotaMicros converts a domain-validated milli-CPU limit to a fixed-period quota and rejects values below the kernel floor.
func cpuQuotaMicros(milli int64) (uint64, error) {
	if milli < domain.MinimumCPULimitMilli {
		return 0, fmt.Errorf("CPU limit milli must be at least %d", domain.MinimumCPULimitMilli)
	}
	value := uint64(milli)
	whole := value / 1_000
	remainder := value % 1_000
	if whole > math.MaxUint64/CPUPeriodMicros {
		return 0, fmt.Errorf("CPU limit milli %d overflows cpu.max quota", milli)
	}
	quota := whole * CPUPeriodMicros
	fraction := (remainder*CPUPeriodMicros + 999) / 1_000
	if quota > math.MaxUint64-fraction {
		return 0, fmt.Errorf("CPU limit milli %d overflows cpu.max quota", milli)
	}
	quota += fraction
	if quota < MinimumCPUQuotaMicros {
		return 0, fmt.Errorf("CPU quota %d is below kernel minimum %d", quota, MinimumCPUQuotaMicros)
	}
	return quota, nil
}

// roundMemoryToPage returns the host-canonical memory.max value while rejecting arithmetic overflow.
func roundMemoryToPage(value, pageSize uint64) (uint64, error) {
	if value == 0 || pageSize == 0 || pageSize&(pageSize-1) != 0 {
		return 0, fmt.Errorf("memory value and power-of-two page size must be positive")
	}
	remainder := value % pageSize
	if remainder == 0 {
		return value, nil
	}
	increment := pageSize - remainder
	if value > math.MaxUint64-increment {
		return 0, fmt.Errorf("memory limit %d overflows page-size canonicalization", value)
	}
	return value + increment, nil
}
