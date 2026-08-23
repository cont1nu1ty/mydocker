package domain

const (
	// MinimumCPULimitMilli is the smallest quota representable by M2's fixed 100000-microsecond cpu.max period and the kernel's 1000-microsecond quota floor.
	MinimumCPULimitMilli int64 = 10
	// DefaultPidsLimit is the explicit safety limit copied into every ContainerSpec when the Sandbox omits PidsLimit.
	DefaultPidsLimit int64 = 1_024
)

// ResourceRequests describes scheduling intent and never local enforcement.
type ResourceRequests struct {
	CPURequestMilli    *int64 `json:"cpu_request_milli,omitempty"`
	MemoryRequestBytes *int64 `json:"memory_request_bytes,omitempty"`
}

// ResourceLimits describes values a later cgroup provider will enforce.
type ResourceLimits struct {
	CPULimitMilli    *int64 `json:"cpu_limit_milli,omitempty"`
	MemoryLimitBytes *int64 `json:"memory_limit_bytes,omitempty"`
	PidsLimit        *int64 `json:"pids_limit,omitempty"`
}

// ResolvedResourceLimits is immutable Container execution policy with explicit unlimited CPU/memory semantics and a concrete pids limit.
// Nil CPU or memory values are valid only when their matching Unlimited field is true, so JSON persistence never hides a default.
type ResolvedResourceLimits struct {
	CPUUnlimited     bool   `json:"cpu_unlimited"`
	CPULimitMilli    *int64 `json:"cpu_limit_milli"`
	MemoryUnlimited  bool   `json:"memory_unlimited"`
	MemoryLimitBytes *int64 `json:"memory_limit_bytes"`
	PidsLimit        int64  `json:"pids_limit"`
}

// Resources keeps scheduling requests and enforcement limits in separate fields.
type Resources struct {
	Requests ResourceRequests `json:"requests"`
	Limits   ResourceLimits   `json:"limits"`
}

// Validate enforces positive optional values, the M2 CPU quota floor, and request-not-greater-than-limit rules before persistence or provider work.
func (r Resources) Validate() error {
	fields := []struct {
		name  string
		value *int64
	}{
		{"cpu_request_milli", r.Requests.CPURequestMilli},
		{"memory_request_bytes", r.Requests.MemoryRequestBytes},
		{"cpu_limit_milli", r.Limits.CPULimitMilli},
		{"memory_limit_bytes", r.Limits.MemoryLimitBytes},
		{"pids_limit", r.Limits.PidsLimit},
	}
	for _, field := range fields {
		if field.value != nil && *field.value <= 0 {
			return NewError(CodeInvalidArgument, field.name, "must be greater than zero when present")
		}
	}
	if r.Limits.CPULimitMilli != nil && *r.Limits.CPULimitMilli < MinimumCPULimitMilli {
		return NewError(CodeInvalidArgument, "cpu_limit_milli", "must be at least 10 for the fixed 100000us cpu.max period")
	}
	if exceeds(r.Requests.CPURequestMilli, r.Limits.CPULimitMilli) {
		return NewError(CodeInvalidArgument, "cpu_request_milli", "must not exceed cpu_limit_milli")
	}
	if exceeds(r.Requests.MemoryRequestBytes, r.Limits.MemoryLimitBytes) {
		return NewError(CodeInvalidArgument, "memory_request_bytes", "must not exceed memory_limit_bytes")
	}
	return nil
}

// ResolveResourceLimits validates raw Sandbox policy and returns the complete immutable limit values copied into a ContainerSpec before provider work starts.
func ResolveResourceLimits(resources Resources) (ResolvedResourceLimits, error) {
	if err := resources.Validate(); err != nil {
		return ResolvedResourceLimits{}, err
	}
	resolved := ResolvedResourceLimits{
		CPUUnlimited:     resources.Limits.CPULimitMilli == nil,
		CPULimitMilli:    cloneInt64(resources.Limits.CPULimitMilli),
		MemoryUnlimited:  resources.Limits.MemoryLimitBytes == nil,
		MemoryLimitBytes: cloneInt64(resources.Limits.MemoryLimitBytes),
		PidsLimit:        DefaultPidsLimit,
	}
	if resources.Limits.PidsLimit != nil {
		resolved.PidsLimit = *resources.Limits.PidsLimit
	}
	if err := resolved.Validate(); err != nil {
		return ResolvedResourceLimits{}, err
	}
	return resolved, nil
}

// Validate rejects ambiguous unlimited/value combinations and resolved values that a provider cannot enforce safely.
func (r ResolvedResourceLimits) Validate() error {
	if r.CPUUnlimited {
		if r.CPULimitMilli != nil {
			return NewError(CodeInvalidArgument, "resolved_limits.cpu", "unlimited CPU must not also contain a numeric limit")
		}
	} else if r.CPULimitMilli == nil || *r.CPULimitMilli < MinimumCPULimitMilli {
		return NewError(CodeInvalidArgument, "resolved_limits.cpu_limit_milli", "finite CPU must be at least 10")
	}
	if r.MemoryUnlimited {
		if r.MemoryLimitBytes != nil {
			return NewError(CodeInvalidArgument, "resolved_limits.memory", "unlimited memory must not also contain a numeric limit")
		}
	} else if r.MemoryLimitBytes == nil || *r.MemoryLimitBytes <= 0 {
		return NewError(CodeInvalidArgument, "resolved_limits.memory_limit_bytes", "finite memory must be greater than zero")
	}
	if r.PidsLimit <= 0 {
		return NewError(CodeInvalidArgument, "resolved_limits.pids_limit", "must be greater than zero")
	}
	return nil
}

// Clone copies finite optional values so a persisted ContainerSpec cannot alias Sandbox or caller memory.
func (r ResolvedResourceLimits) Clone() ResolvedResourceLimits {
	clone := r
	clone.CPULimitMilli = cloneInt64(r.CPULimitMilli)
	clone.MemoryLimitBytes = cloneInt64(r.MemoryLimitBytes)
	return clone
}

// Clone returns a resource policy whose optional integer pointers do not alias the source.
func (r Resources) Clone() Resources {
	return Resources{Requests: r.Requests.Clone(), Limits: r.Limits.Clone()}
}

// Clone returns scheduling requests with independent optional integer values.
func (r ResourceRequests) Clone() ResourceRequests {
	return ResourceRequests{
		CPURequestMilli:    cloneInt64(r.CPURequestMilli),
		MemoryRequestBytes: cloneInt64(r.MemoryRequestBytes),
	}
}

// Clone returns enforcement limits with independent optional integer values.
func (r ResourceLimits) Clone() ResourceLimits {
	return ResourceLimits{
		CPULimitMilli:    cloneInt64(r.CPULimitMilli),
		MemoryLimitBytes: cloneInt64(r.MemoryLimitBytes),
		PidsLimit:        cloneInt64(r.PidsLimit),
	}
}

// exceeds reports whether two present positive values violate request <= limit.
func exceeds(request, limit *int64) bool {
	return request != nil && limit != nil && *request > *limit
}

// cloneInt64 copies an optional integer so immutable specs cannot be mutated by aliasing.
func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
