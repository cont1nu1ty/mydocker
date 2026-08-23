package lifecycle

import (
	"context"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/state"
)

// GetSandbox returns one deeply copied Sandbox snapshot without creating a state operation.
func (c *Coordinator) GetSandbox(ctx context.Context, id domain.SandboxID) (domain.Sandbox, error) {
	var sandbox domain.Sandbox
	err := c.store.View(ctx, func(reader state.Reader) error {
		record, err := reader.GetSandbox(id)
		if err != nil {
			return err
		}
		sandbox = record.Sandbox.Clone()
		return nil
	})
	return sandbox, err
}

// ListSandboxes returns deterministic deeply copied Sandbox snapshots without mutating lifecycle state.
func (c *Coordinator) ListSandboxes(ctx context.Context) ([]domain.Sandbox, error) {
	var sandboxes []domain.Sandbox
	err := c.store.View(ctx, func(reader state.Reader) error {
		records, err := reader.ListSandboxes()
		if err != nil {
			return err
		}
		sandboxes = make([]domain.Sandbox, len(records))
		for index, record := range records {
			sandboxes[index] = record.Sandbox.Clone()
		}
		return nil
	})
	return sandboxes, err
}

// GetContainer returns one deeply copied atomic Container/Attempt aggregate.
func (c *Coordinator) GetContainer(ctx context.Context, id domain.ContainerID) (domain.ContainerAttempt, error) {
	var pair domain.ContainerAttempt
	err := c.store.View(ctx, func(reader state.Reader) error {
		record, err := reader.GetContainerAttempt(id)
		if err != nil {
			return err
		}
		pair = record.ContainerAttempt.Clone()
		return nil
	})
	return pair, err
}

// ListContainers returns one Sandbox's historical pairs in deterministic Container-ID order.
func (c *Coordinator) ListContainers(ctx context.Context, sandboxID domain.SandboxID) ([]domain.ContainerAttempt, error) {
	var pairs []domain.ContainerAttempt
	err := c.store.View(ctx, func(reader state.Reader) error {
		records, err := reader.ListContainerAttempts(sandboxID)
		if err != nil {
			return err
		}
		pairs = make([]domain.ContainerAttempt, len(records))
		for index, record := range records {
			pairs[index] = record.ContainerAttempt.Clone()
		}
		return nil
	})
	return pairs, err
}

// GetOperation returns one deeply copied durable operation for progress inspection or terminal replay.
func (c *Coordinator) GetOperation(ctx context.Context, id operation.OperationID) (operation.Operation, error) {
	var value operation.Operation
	err := c.store.View(ctx, func(reader state.Reader) error {
		record, err := reader.GetOperation(id)
		if err != nil {
			return err
		}
		value = record.Operation.Clone()
		return nil
	})
	return value, err
}

// ListEvents returns ordered operation facts strictly after a caller resume sequence.
func (c *Coordinator) ListEvents(ctx context.Context, after state.EventSequence, limit int) ([]operation.Event, error) {
	var events []operation.Event
	err := c.store.View(ctx, func(reader state.Reader) error {
		values, err := reader.EventsAfter(after, limit)
		if err != nil {
			return err
		}
		events = make([]operation.Event, len(values))
		for index, event := range values {
			events[index] = event.Clone()
		}
		return nil
	})
	return events, err
}
