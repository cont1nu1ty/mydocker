package provider

import (
	"context"
	"errors"
	"fmt"

	"mydocker/internal/ownership"
	"mydocker/internal/rollback"
)

var (
	// ErrUnknownRollbackRoute reports a descriptor whose validated provider/action pair has no configured handler.
	ErrUnknownRollbackRoute = errors.New("unknown provider rollback route")
	// ErrRollbackOwnerMismatch reports a descriptor bound to another operation, target, generation, or owner token.
	ErrRollbackOwnerMismatch = errors.New("rollback receipt owner mismatch")
)

// RollbackHandler performs one idempotent receipt-bound cleanup and must prove verified absence on success.
type RollbackHandler func(context.Context, ownership.OwnerKey, ownership.Receipt) (CleanupObservation, error)

// RollbackRegistration binds one supported provider/action pair to runtime-only cleanup code.
type RollbackRegistration struct {
	Provider ownership.Provider
	Action   ownership.Action
	Handler  RollbackHandler
}

// rollbackRoute is a comparable dispatch key that contains no caller-controlled host identifier.
type rollbackRoute struct {
	provider ownership.Provider
	action   ownership.Action
}

// RollbackRegistry is an immutable fail-closed dispatch table used to reconstruct persisted inverses.
type RollbackRegistry struct {
	handlers map[rollbackRoute]RollbackHandler
}

// NewRollbackRegistry validates all routes up front and rejects duplicates, nil handlers, and impossible provider/action pairs.
func NewRollbackRegistry(registrations ...RollbackRegistration) (*RollbackRegistry, error) {
	registry := &RollbackRegistry{handlers: make(map[rollbackRoute]RollbackHandler, len(registrations))}
	for index, registration := range registrations {
		if !validProviderAction(registration.Provider, registration.Action) {
			return nil, fmt.Errorf("rollback registration %d has unsupported route %s/%s", index, registration.Provider, registration.Action)
		}
		if registration.Handler == nil {
			return nil, fmt.Errorf("rollback registration %d has nil handler", index)
		}
		route := rollbackRoute{provider: registration.Provider, action: registration.Action}
		if _, duplicate := registry.handlers[route]; duplicate {
			return nil, fmt.Errorf("rollback route %s/%s is duplicated", registration.Provider, registration.Action)
		}
		registry.handlers[route] = registration.Handler
	}
	return registry, nil
}

// Resolver returns a rollback.Resolver permanently scoped to one validated operation owner.
func (r *RollbackRegistry) Resolver(expectedOwner ownership.OwnerKey) (rollback.Resolver, error) {
	if r == nil {
		return nil, errors.New("rollback registry must not be nil")
	}
	if err := expectedOwner.Validate(); err != nil {
		return nil, fmt.Errorf("rollback expected owner: %w", err)
	}
	return func(descriptor rollback.Descriptor) (rollback.Inverse, error) {
		receipt, action, err := ownership.ReceiptFromDescriptor(descriptor)
		if err != nil {
			return nil, fmt.Errorf("validate ownership rollback descriptor: %w", err)
		}
		if receipt.Owner != expectedOwner {
			return nil, ErrRollbackOwnerMismatch
		}
		handler, found := r.handlers[rollbackRoute{provider: receipt.Provider, action: action}]
		if !found {
			return nil, fmt.Errorf("%w: %s/%s", ErrUnknownRollbackRoute, receipt.Provider, action)
		}
		ownedReceipt := receipt.Clone()
		return func(ctx context.Context) error {
			observation, cleanupErr := handler(ctx, expectedOwner, ownedReceipt.Clone())
			if cleanupErr != nil {
				return cleanupErr
			}
			if err := observation.Validate(); err != nil {
				return fmt.Errorf("provider cleanup returned unsafe success: %w", err)
			}
			return nil
		}, nil
	}, nil
}

// validProviderAction accepts only the bounded routes that ownership descriptors can represent.
func validProviderAction(provider ownership.Provider, action ownership.Action) bool {
	switch provider {
	case ownership.ProviderCgroupV2:
		return action == ownership.ActionRemoveCgroup
	case ownership.ProviderLinux:
		switch action {
		case ownership.ActionStopProcess, ownership.ActionUnmountRoot,
			ownership.ActionCloseGate, ownership.ActionCloseStreams:
			return true
		default:
			return false
		}
	default:
		return false
	}
}
