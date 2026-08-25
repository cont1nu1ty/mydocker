package server

import (
	"context"

	v1 "mydocker/api/runtime/v1"
)

// Service is the transport-independent M3 lifecycle boundary implemented by
// the daemon engine. Its methods receive the request context unchanged so the
// engine can bind client operation IDs to canonical request fingerprints.
type Service interface {
	// Info returns immutable build identity read from the running daemon binary.
	Info(context.Context, v1.RequestContext, v1.GetInfoRequest) (v1.InfoResponse, error)
	// CreateSandbox persists and reconciles one immutable Sandbox create intent.
	CreateSandbox(context.Context, v1.RequestContext, v1.CreateSandboxRequest) (v1.SandboxResponse, error)
	// StopSandbox advances a Ready Sandbox toward a confirmed stopped state.
	StopSandbox(context.Context, v1.RequestContext, v1.StopSandboxRequest) (v1.SandboxResponse, error)
	// DeleteSandbox removes only a stopped Sandbox whose owned resources are confirmed absent.
	DeleteSandbox(context.Context, v1.RequestContext, v1.DeleteSandboxRequest) (v1.OperationResponse, error)
	// GetSandbox returns one authoritative Sandbox projection without changing state.
	GetSandbox(context.Context, v1.RequestContext, v1.GetSandboxRequest) (v1.SandboxResponse, error)
	// ListSandboxes returns a deterministic point-in-time Sandbox projection.
	ListSandboxes(context.Context, v1.RequestContext, v1.ListSandboxesRequest) (v1.SandboxListResponse, error)
	// CreateContainer persists and prepares exactly one Container/Attempt pair.
	CreateContainer(context.Context, v1.RequestContext, v1.CreateContainerRequest) (v1.ContainerResponse, error)
	// StartContainer releases a prepared Attempt start gate and observes execution.
	StartContainer(context.Context, v1.RequestContext, v1.StartContainerRequest) (v1.ContainerResponse, error)
	// KillContainer signals only the strongly verified wrapper identity owned by the Attempt.
	KillContainer(context.Context, v1.RequestContext, v1.KillContainerRequest) (v1.ContainerResponse, error)
	// DeleteContainer tears down a stopped Attempt and removes its atomic pair metadata.
	DeleteContainer(context.Context, v1.RequestContext, v1.DeleteContainerRequest) (v1.OperationResponse, error)
	// GetContainer returns one authoritative Container/Attempt projection.
	GetContainer(context.Context, v1.RequestContext, v1.GetContainerRequest) (v1.ContainerResponse, error)
	// ListContainers returns deterministic Container order within one Sandbox.
	ListContainers(context.Context, v1.RequestContext, v1.ListContainersRequest) (v1.ContainerListResponse, error)
	// GetOperation returns the durable result or resumable stage bound to an operation ID.
	GetOperation(context.Context, v1.RequestContext, v1.GetOperationRequest) (v1.OperationResponse, error)
	// EventsAfter returns globally ordered events strictly after a sequence, up to limit.
	EventsAfter(context.Context, v1.RequestContext, v1.ListEventsRequest) ([]v1.Event, error)
	// LogsAfter returns output frames for exactly one Container/Attempt identity after a durable cursor and rejects positions beyond committed history with resume_gap.
	LogsAfter(context.Context, v1.RequestContext, v1.ListLogsRequest) ([]v1.LogFrame, error)
}
