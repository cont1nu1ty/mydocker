package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	v1 "mydocker/api/runtime/v1"
	"mydocker/internal/domain"
	"mydocker/internal/engine"
	"mydocker/internal/lifecycle"
	"mydocker/internal/logstore"
	"mydocker/internal/operation"
	"mydocker/internal/provider"
	"mydocker/internal/server"
	"mydocker/internal/state"
)

const maximumEventReadLimit = 501

// mutator is the host-changing boundary implemented by Engine; tests replace
// it with a recorder so transport conversion never needs privileged effects.
type mutator interface {
	CreateSandbox(context.Context, lifecycle.SandboxCreateRequest) (lifecycle.SandboxResult, error)
	StopSandbox(context.Context, lifecycle.SandboxActionRequest) (lifecycle.SandboxResult, error)
	RemoveSandbox(context.Context, lifecycle.SandboxActionRequest) (lifecycle.SandboxResult, error)
	CreateContainer(context.Context, lifecycle.ContainerCreateRequest) (lifecycle.ContainerResult, error)
	StartContainer(context.Context, lifecycle.ContainerActionRequest) (lifecycle.ContainerResult, error)
	KillContainer(context.Context, lifecycle.KillRequest) (lifecycle.KillResult, error)
	DeleteContainer(context.Context, lifecycle.ContainerActionRequest) (lifecycle.ContainerResult, error)
}

// queryService is the read-only lifecycle facade exposed by Engine.Coordinator.
type queryService interface {
	GetSandbox(context.Context, domain.SandboxID) (domain.Sandbox, error)
	ListSandboxes(context.Context) ([]domain.Sandbox, error)
	GetContainer(context.Context, domain.ContainerID) (domain.ContainerAttempt, error)
	ListContainers(context.Context, domain.SandboxID) ([]domain.ContainerAttempt, error)
	GetOperation(context.Context, operation.OperationID) (operation.Operation, error)
	ListEvents(context.Context, state.EventSequence, int) ([]operation.Event, error)
}

// Service is the transport adapter that sends mutations through Engine and
// authoritative reads through its Coordinator while keeping v1 DTOs isolated.
type Service struct {
	mutator        mutator
	queries        queryService
	logs           LogAccess
	containerLocks *serviceContainerLocks
	info           v1.InfoResponse
}

// serviceContainerLocks serializes Container create/delete plus their
// process-local artifact registry update without retaining idle ID entries.
type serviceContainerLocks struct {
	mu    sync.Mutex
	locks map[domain.ContainerID]*serviceContainerLock
}

// serviceContainerLock couples one mutex with its holders and waiters so a
// released key cannot overlap a waiter through map-entry replacement.
type serviceContainerLock struct {
	mutex sync.Mutex
	refs  uint64
}

var _ server.Service = (*Service)(nil)

// NewService wires one Engine into the v1 adapter and requires identity-scoped log registration and lookup; it performs no recovery or host mutation itself.
func NewService(runtime *engine.Engine, logs LogAccess) (*Service, error) {
	return NewServiceWithInfo(runtime, logs, defaultUnavailableInfo())
}

// NewServiceWithInfo wires one Engine into the v1 adapter with an immutable daemon-binary identity snapshot.
func NewServiceWithInfo(runtime *engine.Engine, logs LogAccess, info v1.InfoResponse) (*Service, error) {
	if runtime == nil {
		return nil, errors.New("daemon service engine must not be nil")
	}
	return newServiceWithInfo(runtime, runtime.Coordinator(), logs, info)
}

// newService accepts narrow mutation, query, and log-access dependencies for deterministic unprivileged adapter tests.
func newService(mutations mutator, queries queryService, logs LogAccess) (*Service, error) {
	return newServiceWithInfo(mutations, queries, logs, defaultUnavailableInfo())
}

// newServiceWithInfo accepts narrow dependencies plus a caller-owned identity snapshot for deterministic tests.
func newServiceWithInfo(mutations mutator, queries queryService, logs LogAccess, info v1.InfoResponse) (*Service, error) {
	if mutations == nil || queries == nil {
		return nil, errors.New("daemon service requires mutation and query dependencies")
	}
	if logs == nil {
		return nil, errors.New("daemon service log locator must not be nil")
	}
	if err := info.Validate(); err != nil {
		return nil, fmt.Errorf("daemon service info: %w", err)
	}
	return &Service{
		mutator: mutations, queries: queries, logs: logs,
		containerLocks: &serviceContainerLocks{locks: make(map[domain.ContainerID]*serviceContainerLock)},
		info:           cloneInfoResponse(info),
	}, nil
}

// defaultUnavailableInfo keeps legacy and test service construction explicit about absent binary metadata.
func defaultUnavailableInfo() v1.InfoResponse {
	return v1.InfoResponse{DaemonBuild: v1.DaemonBuildIdentity{
		Source:            v1.DaemonBuildIdentitySource,
		Unavailable:       true,
		UnavailableReason: v1.DaemonBuildUnavailableNotConfigured,
	}}
}

// cloneInfoResponse prevents pointer aliasing from changing the service's immutable identity snapshot.
func cloneInfoResponse(info v1.InfoResponse) v1.InfoResponse {
	cloned := info
	if info.DaemonBuild.VCSModified != nil {
		modified := *info.DaemonBuild.VCSModified
		cloned.DaemonBuild.VCSModified = &modified
	}
	return cloned
}

// lock holds one Container's API mutation and log-registry publication as one
// process-local critical section; durable Engine operations remain authoritative.
func (locks *serviceContainerLocks) lock(id domain.ContainerID) func() {
	locks.mu.Lock()
	entry := locks.locks[id]
	if entry == nil {
		entry = &serviceContainerLock{}
		locks.locks[id] = entry
	}
	entry.refs++
	locks.mu.Unlock()
	entry.mutex.Lock()
	return func() {
		entry.mutex.Unlock()
		locks.mu.Lock()
		entry.refs--
		if entry.refs == 0 && locks.locks[id] == entry {
			delete(locks.locks, id)
		}
		locks.mu.Unlock()
	}
}

// CreateSandbox validates the direct-call boundary, converts immutable input,
// and delegates the complete host-changing operation to Engine.
func (service *Service) CreateSandbox(ctx context.Context, requestContext v1.RequestContext, request v1.CreateSandboxRequest) (v1.SandboxResponse, error) {
	if err := validateMutationCall(ctx, requestContext); err != nil {
		return v1.SandboxResponse{}, err
	}
	if err := request.Validate(); err != nil {
		return v1.SandboxResponse{}, err
	}
	if err := validateM3Network(request.Spec.Network); err != nil {
		return v1.SandboxResponse{}, err
	}
	result, err := service.mutator.CreateSandbox(ctx, lifecycle.SandboxCreateRequest{
		OperationID: operation.OperationID(requestContext.OperationID),
		SandboxID:   domain.SandboxID(request.SandboxID),
		Spec:        sandboxSpecToDomain(request.Spec),
	})
	if err != nil {
		return v1.SandboxResponse{}, MapError(err)
	}
	return projectSandboxResult(result)
}

// StopSandbox delegates quiescence confirmation to Engine and projects the retained Sandbox.
func (service *Service) StopSandbox(ctx context.Context, requestContext v1.RequestContext, request v1.StopSandboxRequest) (v1.SandboxResponse, error) {
	if err := validateMutationResourceCall(ctx, requestContext, "sandbox_id", request.SandboxID); err != nil {
		return v1.SandboxResponse{}, err
	}
	result, err := service.mutator.StopSandbox(ctx, lifecycle.SandboxActionRequest{
		OperationID: operation.OperationID(requestContext.OperationID),
		SandboxID:   domain.SandboxID(request.SandboxID),
	})
	if err != nil {
		return v1.SandboxResponse{}, MapError(err)
	}
	return projectSandboxResult(result)
}

// DeleteSandbox delegates exact teardown to Engine and exposes only the durable operation result.
func (service *Service) DeleteSandbox(ctx context.Context, requestContext v1.RequestContext, request v1.DeleteSandboxRequest) (v1.OperationResponse, error) {
	if err := validateMutationResourceCall(ctx, requestContext, "sandbox_id", request.SandboxID); err != nil {
		return v1.OperationResponse{}, err
	}
	result, err := service.mutator.RemoveSandbox(ctx, lifecycle.SandboxActionRequest{
		OperationID: operation.OperationID(requestContext.OperationID),
		SandboxID:   domain.SandboxID(request.SandboxID),
	})
	if err != nil {
		return v1.OperationResponse{}, MapError(err)
	}
	projected, err := projectOperation(result.Operation)
	if err != nil {
		return v1.OperationResponse{}, MapError(err)
	}
	return v1.OperationResponse{Operation: projected}, nil
}

// GetSandbox reads the authoritative Coordinator snapshot without opening a mutation operation.
func (service *Service) GetSandbox(ctx context.Context, requestContext v1.RequestContext, request v1.GetSandboxRequest) (v1.SandboxResponse, error) {
	if err := validateReadResourceCall(ctx, requestContext, "sandbox_id", request.SandboxID); err != nil {
		return v1.SandboxResponse{}, err
	}
	sandbox, err := service.queries.GetSandbox(ctx, domain.SandboxID(request.SandboxID))
	if err != nil {
		return v1.SandboxResponse{}, MapError(err)
	}
	projected, err := projectSandbox(sandbox)
	if err != nil {
		return v1.SandboxResponse{}, MapError(err)
	}
	return v1.SandboxResponse{Sandbox: projected}, nil
}

// ListSandboxes returns the Coordinator's deterministic point-in-time order.
func (service *Service) ListSandboxes(ctx context.Context, requestContext v1.RequestContext, _ v1.ListSandboxesRequest) (v1.SandboxListResponse, error) {
	if err := validateReadCall(ctx, requestContext); err != nil {
		return v1.SandboxListResponse{}, err
	}
	sandboxes, err := service.queries.ListSandboxes(ctx)
	if err != nil {
		return v1.SandboxListResponse{}, MapError(err)
	}
	projected := make([]v1.Sandbox, len(sandboxes))
	for index, sandbox := range sandboxes {
		projected[index], err = projectSandbox(sandbox)
		if err != nil {
			return v1.SandboxListResponse{}, MapError(err)
		}
	}
	return v1.SandboxListResponse{Sandboxes: projected}, nil
}

// CreateContainer validates that rootfs is an opaque provider source rather
// than a host path, preserves structured process data, and invokes Engine.
func (service *Service) CreateContainer(ctx context.Context, requestContext v1.RequestContext, request v1.CreateContainerRequest) (v1.ContainerResponse, error) {
	if err := validateMutationCall(ctx, requestContext); err != nil {
		return v1.ContainerResponse{}, err
	}
	if err := request.Validate(); err != nil {
		return v1.ContainerResponse{}, err
	}
	if err := v1.ValidateResourceID("sandbox_id", request.SandboxID); err != nil {
		return v1.ContainerResponse{}, err
	}
	if err := provider.OpaqueID(request.RootFS).Validate(); err != nil {
		return v1.ContainerResponse{}, v1.WrapError(v1.CodeInvalidArgument, "rootfs", "must be an opaque prepared-rootfs identifier, not a host path", false, err)
	}
	if err := validateProviderTerminationPolicy("process.termination", request.Process.Termination, false); err != nil {
		return v1.ContainerResponse{}, err
	}
	releaseContainer := service.containerLocks.lock(domain.ContainerID(request.ContainerID))
	defer releaseContainer()
	result, err := service.mutator.CreateContainer(ctx, lifecycle.ContainerCreateRequest{
		OperationID: operation.OperationID(requestContext.OperationID),
		SandboxID:   domain.SandboxID(request.SandboxID),
		ContainerID: domain.ContainerID(request.ContainerID),
		AttemptID:   domain.AttemptID(request.AttemptID),
		Process:     processSpecToDomain(request.Process),
		RootFS:      request.RootFS,
	})
	if err != nil {
		return v1.ContainerResponse{}, MapError(err)
	}
	if result.ContainerAttempt == nil {
		return v1.ContainerResponse{}, v1.NewError(v1.CodeInternal, "container", "mutation completed without a retained Container Attempt")
	}
	if result.ContainerAttempt.Container.ID != domain.ContainerID(request.ContainerID) ||
		result.ContainerAttempt.Attempt.ID != domain.AttemptID(request.AttemptID) {
		return v1.ContainerResponse{}, v1.NewError(v1.CodeInternal, "container", "mutation returned a different Container Attempt identity")
	}
	if result.Resolution != operation.ResolutionReplay {
		if result.HostBinding == nil {
			return v1.ContainerResponse{}, v1.NewError(v1.CodeInternal, "container", "mutation completed without a durable host-owner binding")
		}
		if err := result.HostBinding.Validate(); err != nil ||
			result.HostBinding.ContainerID != result.ContainerAttempt.Container.ID ||
			result.HostBinding.AttemptID != result.ContainerAttempt.Attempt.ID {
			return v1.ContainerResponse{}, v1.NewError(v1.CodeInternal, "container", "mutation returned an invalid host-owner binding")
		}
		identity := logstore.Identity{ContainerID: result.HostBinding.ContainerID, AttemptID: result.HostBinding.AttemptID}
		if err := service.logs.RegisterAttempt(identity, result.HostBinding.Owner); err != nil {
			return v1.ContainerResponse{}, MapError(err)
		}
	}
	return projectContainerResult(result.ContainerAttempt, result.Operation)
}

// StartContainer delegates the one-shot gate and observation sequence to Engine.
func (service *Service) StartContainer(ctx context.Context, requestContext v1.RequestContext, request v1.StartContainerRequest) (v1.ContainerResponse, error) {
	if err := validateMutationResourceCall(ctx, requestContext, "container_id", request.ContainerID); err != nil {
		return v1.ContainerResponse{}, err
	}
	result, err := service.mutator.StartContainer(ctx, lifecycle.ContainerActionRequest{
		OperationID: operation.OperationID(requestContext.OperationID),
		ContainerID: domain.ContainerID(request.ContainerID),
	})
	if err != nil {
		return v1.ContainerResponse{}, MapError(err)
	}
	return projectContainerResult(result.ContainerAttempt, result.Operation)
}

// KillContainer passes the caller's complete explicit policy to Engine without
// inventing a signal, grace period, or escalation behavior.
func (service *Service) KillContainer(ctx context.Context, requestContext v1.RequestContext, request v1.KillContainerRequest) (v1.ContainerResponse, error) {
	if err := validateMutationCall(ctx, requestContext); err != nil {
		return v1.ContainerResponse{}, err
	}
	if err := request.Validate(); err != nil {
		return v1.ContainerResponse{}, err
	}
	if err := v1.ValidateResourceID("container_id", request.ContainerID); err != nil {
		return v1.ContainerResponse{}, err
	}
	if err := validateProviderTerminationPolicy("policy", request.Policy, true); err != nil {
		return v1.ContainerResponse{}, err
	}
	result, err := service.mutator.KillContainer(ctx, lifecycle.KillRequest{
		OperationID: operation.OperationID(requestContext.OperationID),
		ContainerID: domain.ContainerID(request.ContainerID),
		Policy:      terminationPolicyToDomain(request.Policy),
	})
	if err != nil {
		return v1.ContainerResponse{}, MapError(err)
	}
	return projectContainerResult(result.ContainerAttempt, result.Operation)
}

// DeleteContainer unregisters only the immutable Attempt/owner binding carried
// by the durable delete result, so an old replay cannot erase a new incarnation.
func (service *Service) DeleteContainer(ctx context.Context, requestContext v1.RequestContext, request v1.DeleteContainerRequest) (v1.OperationResponse, error) {
	if err := validateMutationResourceCall(ctx, requestContext, "container_id", request.ContainerID); err != nil {
		return v1.OperationResponse{}, err
	}
	containerID := domain.ContainerID(request.ContainerID)
	releaseContainer := service.containerLocks.lock(containerID)
	defer releaseContainer()
	result, err := service.mutator.DeleteContainer(ctx, lifecycle.ContainerActionRequest{
		OperationID: operation.OperationID(requestContext.OperationID),
		ContainerID: containerID,
	})
	if err != nil {
		return v1.OperationResponse{}, MapError(err)
	}
	projected, err := projectOperation(result.Operation)
	if err != nil {
		return v1.OperationResponse{}, MapError(err)
	}
	if result.Operation.Type != operation.TypeDelete || result.Operation.State != operation.StateSucceeded ||
		result.Operation.Target.Kind != operation.TargetContainer || result.Operation.Target.ID != string(containerID) {
		return v1.OperationResponse{}, v1.NewError(v1.CodeInternal, "container", "delete mutation returned a different or nonterminal operation")
	}
	if result.Resolution != operation.ResolutionReplay && result.HostBinding != nil {
		if err := result.HostBinding.Validate(); err != nil || result.HostBinding.ContainerID != containerID {
			return v1.OperationResponse{}, v1.NewError(v1.CodeInternal, "container", "delete mutation returned an invalid removed host-owner binding")
		}
		identity := logstore.Identity{ContainerID: result.HostBinding.ContainerID, AttemptID: result.HostBinding.AttemptID}
		if err := service.logs.UnregisterAttempt(identity, result.HostBinding.Owner); err != nil {
			return v1.OperationResponse{}, MapError(err)
		}
	}
	return v1.OperationResponse{Operation: projected}, nil
}

// GetContainer reads one atomic Container/Attempt pair through the Coordinator.
func (service *Service) GetContainer(ctx context.Context, requestContext v1.RequestContext, request v1.GetContainerRequest) (v1.ContainerResponse, error) {
	if err := validateReadResourceCall(ctx, requestContext, "container_id", request.ContainerID); err != nil {
		return v1.ContainerResponse{}, err
	}
	pair, err := service.queries.GetContainer(ctx, domain.ContainerID(request.ContainerID))
	if err != nil {
		return v1.ContainerResponse{}, MapError(err)
	}
	projected, err := projectContainer(pair)
	if err != nil {
		return v1.ContainerResponse{}, MapError(err)
	}
	return v1.ContainerResponse{Container: projected}, nil
}

// ListContainers preserves the Coordinator's deterministic Container-ID order.
func (service *Service) ListContainers(ctx context.Context, requestContext v1.RequestContext, request v1.ListContainersRequest) (v1.ContainerListResponse, error) {
	if err := validateReadResourceCall(ctx, requestContext, "sandbox_id", request.SandboxID); err != nil {
		return v1.ContainerListResponse{}, err
	}
	if _, err := service.queries.GetSandbox(ctx, domain.SandboxID(request.SandboxID)); err != nil {
		return v1.ContainerListResponse{}, MapError(err)
	}
	pairs, err := service.queries.ListContainers(ctx, domain.SandboxID(request.SandboxID))
	if err != nil {
		return v1.ContainerListResponse{}, MapError(err)
	}
	projected := make([]v1.Container, len(pairs))
	for index, pair := range pairs {
		projected[index], err = projectContainer(pair)
		if err != nil {
			return v1.ContainerListResponse{}, MapError(err)
		}
	}
	return v1.ContainerListResponse{Containers: projected}, nil
}

// GetOperation exposes stable progress fields while deliberately omitting the
// coordinator's internal replay encoding from the public raw response field.
func (service *Service) GetOperation(ctx context.Context, requestContext v1.RequestContext, request v1.GetOperationRequest) (v1.OperationResponse, error) {
	if err := validateReadCall(ctx, requestContext); err != nil {
		return v1.OperationResponse{}, err
	}
	if err := v1.ValidateOperationID(request.OperationID); err != nil {
		return v1.OperationResponse{}, err
	}
	value, err := service.queries.GetOperation(ctx, operation.OperationID(request.OperationID))
	if err != nil {
		return v1.OperationResponse{}, MapError(err)
	}
	projected, err := projectOperation(value)
	if err != nil {
		return v1.OperationResponse{}, MapError(err)
	}
	return v1.OperationResponse{Operation: projected}, nil
}

// Info returns the immutable daemon binary identity without consulting mutable lifecycle state.
func (service *Service) Info(ctx context.Context, requestContext v1.RequestContext, _ v1.GetInfoRequest) (v1.InfoResponse, error) {
	if err := validateReadCall(ctx, requestContext); err != nil {
		return v1.InfoResponse{}, err
	}
	return cloneInfoResponse(service.info), nil
}

// EventsAfter returns globally ordered public event facts and omits internal provider details.
func (service *Service) EventsAfter(ctx context.Context, requestContext v1.RequestContext, request v1.ListEventsRequest) ([]v1.Event, error) {
	if err := validateReadCall(ctx, requestContext); err != nil {
		return nil, err
	}
	if err := validatePageLimit(request.Limit, maximumEventReadLimit); err != nil {
		return nil, err
	}
	events, err := service.queries.ListEvents(ctx, state.EventSequence(request.AfterSequence), request.Limit)
	if err != nil {
		return nil, MapError(err)
	}
	projected := make([]v1.Event, len(events))
	for index, event := range events {
		projected[index], err = projectEvent(event)
		if err != nil {
			return nil, MapError(err)
		}
		if projected[index].Sequence <= request.AfterSequence || (index > 0 && projected[index].Sequence <= projected[index-1].Sequence) {
			return nil, v1.NewError(v1.CodeInternal, "events", "query dependency returned unordered event data")
		}
	}
	return projected, nil
}

// validateMutationCall rejects invalid correlation and an already-canceled direct call before side effects.
func validateMutationCall(ctx context.Context, requestContext v1.RequestContext) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	return requestContext.ValidateMutation()
}

// validateReadCall rejects invalid read correlation and an already-canceled direct call.
func validateReadCall(ctx context.Context, requestContext v1.RequestContext) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	return requestContext.ValidateRead()
}

// validateContext maps nil or canceled contexts before a dependency can dereference or mutate through them.
func validateContext(ctx context.Context) error {
	if ctx == nil {
		return v1.NewError(v1.CodeInternal, "context", "request context is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return MapError(err)
	}
	return nil
}

// validateMutationResourceCall combines mutation correlation with one path-safe resource identity.
func validateMutationResourceCall(ctx context.Context, requestContext v1.RequestContext, field, value string) error {
	if err := validateMutationCall(ctx, requestContext); err != nil {
		return err
	}
	return v1.ValidateResourceID(field, value)
}

// validateReadResourceCall combines read correlation with one path-safe resource identity.
func validateReadResourceCall(ctx context.Context, requestContext v1.RequestContext, field, value string) error {
	if err := validateReadCall(ctx, requestContext); err != nil {
		return err
	}
	return v1.ValidateResourceID(field, value)
}

// validateM3Network restricts this milestone to a provider-local none or loopback mode with no veth attachments.
func validateM3Network(network v1.NetworkIntent) error {
	if network.Mode != "none" && network.Mode != "loopback" {
		return v1.NewError(v1.CodeInvalidArgument, "spec.network.mode", "M3 supports only none or loopback")
	}
	if len(network.Attachments) != 0 {
		return v1.NewError(v1.CodeInvalidArgument, "spec.network.attachments", "M3 does not accept network attachments")
	}
	return nil
}

// validatePageLimit bounds one dependency read including the transport layer's single has-more lookahead item.
func validatePageLimit(limit, maximum int) error {
	if limit <= 0 || limit > maximum {
		return v1.NewError(v1.CodeInvalidArgument, "limit", "is outside the supported bounded page size")
	}
	return nil
}

// validateProviderTerminationPolicy rejects signal names the M3 provider cannot deliver before lifecycle intent is persisted.
func validateProviderTerminationPolicy(field string, policy v1.TerminationPolicy, required bool) error {
	empty := policy.Signal == "" && policy.GracePeriodNanoseconds == 0 && policy.EscalationSignal == ""
	if empty && !required {
		return nil
	}
	if !provider.Signal(policy.Signal).Valid() {
		return v1.NewError(v1.CodeInvalidArgument, field+".signal", "is not supported by the M3 process provider")
	}
	if !provider.Signal(policy.EscalationSignal).Valid() {
		return v1.NewError(v1.CodeInvalidArgument, field+".escalation_signal", "is not supported by the M3 process provider")
	}
	return nil
}

// terminationPolicyToDomain preserves the complete explicit API policy as one domain duration and two signal names.
func terminationPolicyToDomain(policy v1.TerminationPolicy) domain.TerminationPolicy {
	return domain.TerminationPolicy{
		Signal:           policy.Signal,
		GracePeriod:      time.Duration(policy.GracePeriodNanoseconds),
		EscalationSignal: policy.EscalationSignal,
	}
}
