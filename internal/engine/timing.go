package engine

import (
	"time"

	"mydocker/internal/operation"
)

// stageMeasurement carries one same-process monotonic span and its diagnostic wall endpoint.
// The duration pointer is nil only for checkpoints that did not directly bracket a provider action.
type stageMeasurement struct {
	occurredAt time.Time
	duration   *operation.Duration
}

// beginMeasurement captures the raw start of one provider or complete-operation
// span; retaining a zero sample lets finishMeasurement mark the duration unavailable.
func (engine *Engine) beginMeasurement() time.Time {
	return engine.clock.Now()
}

// finishMeasurement closes a span using the same process clock so time.Time.Sub
// prefers its monotonic component. Zero or regressing samples lose only timing
// evidence; the diagnostic endpoint remains usable for lifecycle persistence.
func (engine *Engine) finishMeasurement(startedAt time.Time) stageMeasurement {
	finishedAt := engine.clock.Now()
	measurement := stageMeasurement{occurredAt: normalizeDiagnosticTime(finishedAt)}
	if startedAt.IsZero() || finishedAt.IsZero() {
		return measurement
	}
	elapsed := finishedAt.Sub(startedAt)
	if elapsed < 0 {
		return measurement
	}
	duration := operation.Duration(elapsed)
	measurement.duration = &duration
	return measurement
}

// finishOperationMeasurement exposes a complete-operation duration only when this invocation accepted a new intent.
// Resumed or recovered work returns a wall endpoint without a duration because its original start was in another call or process.
func (engine *Engine) finishOperationMeasurement(startedAt time.Time, resolution operation.Resolution) stageMeasurement {
	measurement := engine.finishMeasurement(startedAt)
	if resolution != operation.ResolutionNew {
		measurement.duration = nil
	}
	return measurement
}

// unmeasuredCheckpoint returns no timing evidence for persistence-only or recovery bookkeeping checkpoints.
func (engine *Engine) unmeasuredCheckpoint() stageMeasurement {
	return stageMeasurement{occurredAt: engine.diagnosticNow()}
}

// diagnosticNow returns one non-zero wall fact even when a broken injected
// measurement clock produces zero; duration availability remains independent.
func (engine *Engine) diagnosticNow() time.Time {
	return normalizeDiagnosticTime(engine.clock.Now())
}

// normalizeDiagnosticTime preserves a valid injected endpoint and falls back
// to the production clock only when zero would otherwise invalidate persistence.
func normalizeDiagnosticTime(value time.Time) time.Time {
	if !value.IsZero() {
		return value
	}
	return time.Now()
}
