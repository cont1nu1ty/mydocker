package slim

import (
	"context"
	"errors"
	"fmt"

	"mydocker/internal/domain"
	"mydocker/internal/engine"
	"mydocker/internal/ownership"
	providerapi "mydocker/internal/provider"
	"mydocker/internal/shim"
)

// ObserveAttempt maps a strong wrapper plus shim control observation to the engine's prepared/running/terminal contract.
func (provider *IsolationProvider) ObserveAttempt(ctx context.Context, request providerapi.OwnedReceiptRequest) (engine.AttemptObservation, error) {
	if err := request.ValidateFor(ownership.ProviderLinux, ownership.KindInitProcess); err != nil {
		return engine.AttemptObservation{}, err
	}
	reference, err := provider.resolver.Resolve(request)
	if err != nil {
		return engine.AttemptObservation{}, err
	}
	process, err := provider.inspectLauncher(ctx, request.Receipt, reference)
	if err != nil {
		return engine.AttemptObservation{}, err
	}
	if process.Presence != providerapi.PresencePresent {
		return engine.AttemptObservation{}, errors.New("Attempt wrapper is not verified present")
	}
	observation, err := provider.inspectAttemptShim(ctx, reference)
	if err != nil {
		return engine.AttemptObservation{}, err
	}
	result := engine.AttemptObservation{Evidence: observation.EvidenceSHA256}
	switch observation.State {
	case shim.StatePrepared:
		result.Prepared = true
		result.Outcome = domain.PendingOutcome()
	case shim.StateStarting:
		result.Starting = true
		result.Outcome = domain.PendingOutcome()
	case shim.StateRunning:
		result.Running = true
		result.Outcome = domain.PendingOutcome()
	case shim.StateTerminal:
		if observation.Terminal == nil {
			return engine.AttemptObservation{}, errors.New("terminal shim observation omitted its record")
		}
		result.Terminal = true
		result.Outcome = observation.Terminal.Outcome.Clone()
	default:
		return engine.AttemptObservation{}, fmt.Errorf("unsupported shim state %q", observation.State)
	}
	if err := result.Validate(); err != nil {
		return engine.AttemptObservation{}, err
	}
	return result, nil
}

// inspectAttemptShim performs one fresh owner-scoped inspection and validates all wrapper identity fields.
func (provider *IsolationProvider) inspectAttemptShim(ctx context.Context, reference ResourceReference) (shim.Observation, error) {
	response, err := provider.doShim(ctx, reference, shim.ActionInspect, "")
	if err != nil {
		if shim.IsCode(err, shim.CodeUnavailable) {
			return shim.Observation{}, providerapi.MarkObservationUnavailable(err)
		}
		return shim.Observation{}, err
	}
	if response.Error != nil {
		if shim.IsCode(response.Error, shim.CodeUnavailable) {
			return shim.Observation{}, providerapi.MarkObservationUnavailable(response.Error)
		}
		return shim.Observation{}, response.Error
	}
	if response.Observation == nil {
		return shim.Observation{}, errors.New("shim inspect returned no observation")
	}
	if err := provider.validateAttemptObservation(reference, *response.Observation); err != nil {
		return shim.Observation{}, err
	}
	return response.Observation.Clone(), nil
}

// validateAttemptObservation binds shim facts back to the exact init receipt and internally derived owner paths.
func (provider *IsolationProvider) validateAttemptObservation(reference ResourceReference, observation shim.Observation) error {
	if err := observation.Validate(); err != nil {
		return err
	}
	if observation.Mode != shim.ModeInit || observation.Owner != reference.Owner ||
		observation.ContainerID != domain.ContainerID(reference.Owner.Target.ID) || observation.AttemptID != reference.AttemptID ||
		observation.SandboxID != reference.SandboxID || observation.WrapperEvidence != reference.WrapperEvidenceSHA256 {
		return errors.New("shim observation does not match the exact init receipt scope")
	}
	return nil
}

// doShim sends one bounded control request to the owner-token-derived socket and validates request/response correlation.
func (provider *IsolationProvider) doShim(ctx context.Context, reference ResourceReference, action shim.ControlAction, signal shim.Signal) (shim.ControlResponse, error) {
	return provider.doShimWithID(ctx, reference, action, signal, provider.requestIDs.Next(action))
}

// doShimWithID sends one explicitly keyed control request, used for durable operation-scoped signal replay.
func (provider *IsolationProvider) doShimWithID(ctx context.Context, reference ResourceReference, action shim.ControlAction, signal shim.Signal, requestID string) (shim.ControlResponse, error) {
	if err := reference.Validate(provider.runtimeRoot); err != nil {
		return shim.ControlResponse{}, err
	}
	request := shim.ControlRequest{
		SchemaVersion: shim.SchemaVersion, RequestID: requestID, Owner: reference.Owner, Action: action, Signal: signal,
	}
	if err := request.Validate(); err != nil {
		return shim.ControlResponse{}, err
	}
	response, err := provider.shim.Do(ctx, reference.Paths.ControlSocket, request)
	if err != nil {
		return shim.ControlResponse{}, err
	}
	if err := response.Validate(); err != nil {
		return shim.ControlResponse{}, err
	}
	if response.RequestID != requestID {
		return shim.ControlResponse{}, errors.New("shim response request ID does not match request")
	}
	return response, nil
}
