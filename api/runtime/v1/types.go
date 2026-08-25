package v1

import (
	"encoding/json"
	"time"
)

const (
	// Version is the semantic identifier carried by every v1 API path.
	Version = "v1"
	// BasePath is the HTTP path prefix reserved for this schema version.
	BasePath = "/v1"
	// HeaderRequestID carries one transport-attempt correlation identity.
	HeaderRequestID = "X-Mydocker-Request-ID"
	// HeaderOperationID carries the caller-created durable mutation identity.
	HeaderOperationID = "X-Mydocker-Operation-ID"
	// MediaTypeJSON is the only request and response encoding accepted by v1.
	MediaTypeJSON = "application/json"
	// DaemonBuildIdentitySource identifies Go build metadata read from the running daemon binary.
	DaemonBuildIdentitySource = "daemon_binary_go_build_info"
	// DaemonBuildUnavailableNotConfigured identifies an explicitly uninjected service identity.
	DaemonBuildUnavailableNotConfigured = "not_configured"
	// DaemonBuildUnavailableBuildInfo identifies a binary without readable Go build metadata.
	DaemonBuildUnavailableBuildInfo = "build_info_unavailable"
	// DaemonBuildUnavailableRevision identifies build metadata without a usable VCS revision.
	DaemonBuildUnavailableRevision = "vcs_revision_unavailable"
	// DaemonBuildUnavailableModified identifies build metadata without a trustworthy dirty-worktree bit.
	DaemonBuildUnavailableModified = "vcs_modified_unavailable"
)

// RequestContext identifies one transport attempt and, for mutations, the
// durable operation that must remain unchanged across transport retries.
type RequestContext struct {
	RequestID   string `json:"request_id"`
	OperationID string `json:"operation_id,omitempty"`
}

// NetworkIntent describes the stable Sandbox networking request. M3 providers
// support only none or loopback, while later additive versions may expand it.
type NetworkIntent struct {
	Mode        string   `json:"mode"`
	Attachments []string `json:"attachments,omitempty"`
}

// ResourceRequests records scheduling intent separately from enforced limits.
type ResourceRequests struct {
	CPURequestMilli    *int64 `json:"cpu_request_milli,omitempty"`
	MemoryRequestBytes *int64 `json:"memory_request_bytes,omitempty"`
}

// ResourceLimits records optional cgroup enforcement values requested for a Sandbox.
type ResourceLimits struct {
	CPULimitMilli    *int64 `json:"cpu_limit_milli,omitempty"`
	MemoryLimitBytes *int64 `json:"memory_limit_bytes,omitempty"`
	PidsLimit        *int64 `json:"pids_limit,omitempty"`
}

// Resources keeps requests and limits distinct throughout the public API.
type Resources struct {
	Requests ResourceRequests `json:"requests"`
	Limits   ResourceLimits   `json:"limits"`
}

// ResolvedResourceLimits projects the immutable limits actually attached to an Attempt.
type ResolvedResourceLimits struct {
	CPUUnlimited     bool   `json:"cpu_unlimited"`
	CPULimitMilli    *int64 `json:"cpu_limit_milli"`
	MemoryUnlimited  bool   `json:"memory_unlimited"`
	MemoryLimitBytes *int64 `json:"memory_limit_bytes"`
	PidsLimit        int64  `json:"pids_limit"`
}

// SandboxSpec is the immutable generation-one Sandbox input accepted by M3.
type SandboxSpec struct {
	Hostname  string            `json:"hostname,omitempty"`
	DNS       []string          `json:"dns,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Network   NetworkIntent     `json:"network"`
	Resources Resources         `json:"resources"`
}

// Condition projects a bounded reconciliation fact without changing lifecycle phase.
type Condition struct {
	Type    string `json:"type"`
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`
}

// LifecycleObservation links query state to the latest durable operation event.
type LifecycleObservation struct {
	OperationID   string `json:"operation_id,omitempty"`
	EventSequence uint64 `json:"event_sequence,omitempty"`
	Reason        string `json:"reason,omitempty"`
}

// SandboxStatus is the authoritative lifecycle and reconciliation projection.
type SandboxStatus struct {
	Phase              string               `json:"phase"`
	Generation         uint64               `json:"generation"`
	ObservedGeneration uint64               `json:"observed_generation"`
	Conditions         []Condition          `json:"conditions,omitempty"`
	CurrentContainerID *string              `json:"current_container_id,omitempty"`
	CurrentAttemptID   *string              `json:"current_attempt_id,omitempty"`
	LastObservation    LifecycleObservation `json:"last_observation"`
}

// Sandbox is the v1 projection of a stable workload environment.
type Sandbox struct {
	ID     string        `json:"id"`
	Spec   SandboxSpec   `json:"spec"`
	Status SandboxStatus `json:"status"`
}

// EnvVar preserves environment order and duplicate names without shell serialization.
type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// TerminationPolicy describes an explicit graceful signal and escalation plan.
type TerminationPolicy struct {
	Signal                 string `json:"signal,omitempty"`
	GracePeriodNanoseconds int64  `json:"grace_period_ns,omitempty"`
	EscalationSignal       string `json:"escalation_signal,omitempty"`
}

// ProcessSpec keeps argv and environment structured for direct exec-style use.
type ProcessSpec struct {
	Argv             []string          `json:"argv"`
	Environment      []EnvVar          `json:"environment,omitempty"`
	WorkingDirectory string            `json:"working_directory,omitempty"`
	Termination      TerminationPolicy `json:"termination,omitempty"`
}

// StreamReferences names durable stream endpoints without exposing host descriptors.
type StreamReferences struct {
	Stdin  string `json:"stdin,omitempty"`
	Stdout string `json:"stdout,omitempty"`
	Stderr string `json:"stderr,omitempty"`
}

// Outcome projects explicit pending, captured, not-applicable, or unknown terminal facts.
type Outcome struct {
	Presence                   string     `json:"presence"`
	ExitCode                   *int32     `json:"exit_code,omitempty"`
	Signal                     string     `json:"signal,omitempty"`
	OOM                        string     `json:"oom"`
	StartedAt                  *time.Time `json:"started_at,omitempty"`
	FinishedAt                 *time.Time `json:"finished_at,omitempty"`
	RunningDurationNanoseconds *int64     `json:"running_duration_ns,omitempty"`
}

// ProcessIdentity projects opaque strong identity evidence and deliberately omits a raw PID.
type ProcessIdentity struct {
	Verified bool   `json:"verified"`
	Handle   string `json:"handle,omitempty"`
	Evidence string `json:"evidence,omitempty"`
}

// ContainerSpec is immutable Attempt input using an explicitly prepared rootfs in M3.
type ContainerSpec struct {
	Process ProcessSpec            `json:"process"`
	RootFS  string                 `json:"rootfs"`
	Limits  ResolvedResourceLimits `json:"limits"`
}

// ContainerStatus is the API projection of the canonical Attempt status.
type ContainerStatus struct {
	Phase              string               `json:"phase"`
	Generation         uint64               `json:"generation"`
	ObservedGeneration uint64               `json:"observed_generation"`
	Conditions         []Condition          `json:"conditions,omitempty"`
	ProcessIdentity    *ProcessIdentity     `json:"process_identity,omitempty"`
	Streams            StreamReferences     `json:"streams"`
	Outcome            Outcome              `json:"outcome"`
	LastObservation    LifecycleObservation `json:"last_observation"`
}

// Container is the v1 user-visible aggregate for exactly one execution Attempt.
type Container struct {
	ID        string          `json:"id"`
	SandboxID string          `json:"sandbox_id"`
	AttemptID string          `json:"attempt_id"`
	Spec      ContainerSpec   `json:"spec"`
	Status    ContainerStatus `json:"status"`
}

// ResourceRef identifies a resource mentioned by an operation or event.
type ResourceRef struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// RequestFingerprint identifies the canonical request encoding bound to an operation ID.
type RequestFingerprint struct {
	Version uint32 `json:"version"`
	SHA256  string `json:"sha256"`
}

// Operation is the stable query projection of one durable lifecycle intent.
type Operation struct {
	ID          string             `json:"id"`
	Type        string             `json:"type"`
	Target      ResourceRef        `json:"target"`
	Fingerprint RequestFingerprint `json:"fingerprint"`
	State       string             `json:"state"`
	Stage       string             `json:"stage"`
	Result      string             `json:"result"`
	Reason      string             `json:"reason"`
	Response    json.RawMessage    `json:"response,omitempty"`
}

// Event is one globally ordered lifecycle stage fact suitable for paged resume.
// A missing duration means the daemon could not measure the stage in one process invocation.
type Event struct {
	Sequence            uint64          `json:"sequence"`
	OperationID         string          `json:"operation_id"`
	Type                string          `json:"type"`
	Target              ResourceRef     `json:"target"`
	Resources           []ResourceRef   `json:"resources"`
	Stage               string          `json:"stage"`
	Result              string          `json:"result"`
	Reason              string          `json:"reason"`
	OccurredAt          time.Time       `json:"occurred_at"`
	DurationNanoseconds *int64          `json:"duration_ns,omitempty"`
	Generation          uint64          `json:"generation,omitempty"`
	ObservedGeneration  uint64          `json:"observed_generation,omitempty"`
	Details             json.RawMessage `json:"details,omitempty"`
}

// CreateSandboxRequest creates generation-one immutable Sandbox intent.
type CreateSandboxRequest struct {
	SandboxID string      `json:"sandbox_id"`
	Spec      SandboxSpec `json:"spec"`
}

// StopSandboxRequest carries no mutable policy; operation identity is supplied in the header.
type StopSandboxRequest struct {
	SandboxID string `json:"-"`
}

// DeleteSandboxRequest carries no mutable policy; deletion remains operation-scoped.
type DeleteSandboxRequest struct {
	SandboxID string `json:"-"`
}

// GetSandboxRequest identifies one Sandbox through its escaped HTTP path segment.
type GetSandboxRequest struct {
	SandboxID string `json:"-"`
}

// ListSandboxesRequest represents the unfiltered deterministic v1 Sandbox collection.
type ListSandboxesRequest struct{}

// CreateContainerRequest creates one Container/Attempt pair under a Ready Sandbox.
type CreateContainerRequest struct {
	SandboxID   string      `json:"-"`
	ContainerID string      `json:"container_id"`
	AttemptID   string      `json:"attempt_id"`
	Process     ProcessSpec `json:"process"`
	RootFS      string      `json:"rootfs"`
}

// StartContainerRequest carries no mutable execution input after immutable create.
type StartContainerRequest struct {
	ContainerID string `json:"-"`
}

// KillContainerRequest carries the complete explicit graceful policy used for one strongly verified wrapper action.
type KillContainerRequest struct {
	ContainerID string            `json:"-"`
	Policy      TerminationPolicy `json:"policy"`
}

// DeleteContainerRequest carries no mutable policy; teardown remains operation-scoped.
type DeleteContainerRequest struct {
	ContainerID string `json:"-"`
}

// GetContainerRequest identifies one Container through its escaped HTTP path segment.
type GetContainerRequest struct {
	ContainerID string `json:"-"`
}

// ListContainersRequest scopes deterministic Container listing to one Sandbox.
type ListContainersRequest struct {
	SandboxID string `json:"-"`
}

// GetOperationRequest identifies one retained operation without creating a new operation.
type GetOperationRequest struct {
	OperationID string `json:"-"`
}

// ListEventsRequest is the decoded event paging position passed to the daemon service.
type ListEventsRequest struct {
	AfterSequence uint64 `json:"-"`
	Limit         int    `json:"-"`
}

// GetInfoRequest represents the side-effect-free daemon information lookup.
type GetInfoRequest struct{}

// DaemonBuildIdentity reports immutable Go and VCS metadata read from the running daemon binary.
type DaemonBuildIdentity struct {
	Source            string `json:"source"`
	Unavailable       bool   `json:"unavailable"`
	UnavailableReason string `json:"unavailable_reason,omitempty"`
	GoVersion         string `json:"go_version,omitempty"`
	MainPath          string `json:"main_path,omitempty"`
	MainVersion       string `json:"main_version,omitempty"`
	MainSum           string `json:"main_sum,omitempty"`
	VCS               string `json:"vcs,omitempty"`
	VCSRevision       string `json:"vcs_revision,omitempty"`
	VCSTime           string `json:"vcs_time,omitempty"`
	VCSModified       *bool  `json:"vcs_modified,omitempty"`
}

// InfoResponse returns the daemon binary identity used to qualify evaluation evidence.
type InfoResponse struct {
	DaemonBuild DaemonBuildIdentity `json:"daemon_build"`
}

// SandboxResponse returns one authoritative Sandbox and an optional mutation operation.
type SandboxResponse struct {
	Sandbox   Sandbox    `json:"sandbox"`
	Operation *Operation `json:"operation,omitempty"`
}

// SandboxListResponse returns a deterministic snapshot of all Sandboxes.
type SandboxListResponse struct {
	Sandboxes []Sandbox `json:"sandboxes"`
}

// ContainerResponse returns one Container/Attempt projection and an optional mutation operation.
type ContainerResponse struct {
	Container Container  `json:"container"`
	Operation *Operation `json:"operation,omitempty"`
}

// ContainerListResponse returns deterministic Container order within one Sandbox.
type ContainerListResponse struct {
	Containers []Container `json:"containers"`
}

// OperationResponse returns a mutation result or direct operation lookup.
type OperationResponse struct {
	Operation Operation `json:"operation"`
}

// ResumeToken is an opaque v1 event position returned by the server.
type ResumeToken string

// EventListResponse returns an ordered page and the token needed to resume without overlap.
type EventListResponse struct {
	Events          []Event     `json:"events"`
	NextResumeToken ResumeToken `json:"next_resume_token,omitempty"`
	HasMore         bool        `json:"has_more"`
}

// LogFrame is one durable stdout or stderr append bound to an exact Container/Attempt pair.
// Payload uses Go JSON's standard base64 representation and exposes no host path or descriptor.
type LogFrame struct {
	ContainerID   string `json:"container_id"`
	AttemptID     string `json:"attempt_id"`
	Stream        string `json:"stream"`
	Cursor        uint64 `json:"cursor"`
	Sequence      uint64 `json:"sequence"`
	Payload       []byte `json:"payload"`
	PayloadSHA256 string `json:"payload_sha256"`
}

// LogCursor is an opaque position bound to one Container/Attempt log identity.
type LogCursor string

// ListLogsRequest is the decoded identity and paging position passed to the daemon service.
type ListLogsRequest struct {
	ContainerID string `json:"-"`
	AttemptID   string `json:"-"`
	AfterCursor uint64 `json:"-"`
	Limit       int    `json:"-"`
}

// LogListResponse returns ordered output frames and an identity-bound cursor for resuming.
type LogListResponse struct {
	Frames     []LogFrame `json:"frames"`
	NextCursor LogCursor  `json:"next_cursor,omitempty"`
	HasMore    bool       `json:"has_more"`
}
