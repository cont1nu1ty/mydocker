// Package observability provides structured diagnostic logs and bounded
// in-process metrics for the M3 daemon boundary.
package observability

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode"

	"mydocker/internal/operation"
)

// LogSchemaVersion is the only structured-log record schema emitted by M3.
const LogSchemaVersion uint32 = 1

// Level is the bounded severity vocabulary used by daemon JSON logs.
type Level string

const (
	// LevelInfo records normal lifecycle progress and recovery decisions.
	LevelInfo Level = "info"
	// LevelWarn records a recoverable unknown, retry, or cleanup condition.
	LevelWarn Level = "warn"
	// LevelError records an operation or daemon failure requiring diagnosis.
	LevelError Level = "error"
)

// Valid reports whether level belongs to the stable M3 log vocabulary.
func (level Level) Valid() bool {
	return level == LevelInfo || level == LevelWarn || level == LevelError
}

// LogRecord is one newline-delimited JSON diagnostic fact. Concrete resource
// IDs are permitted here because logs, unlike metrics, are a diagnostic stream.
type LogRecord struct {
	SchemaVersion uint32                `json:"schema_version"`
	Time          time.Time             `json:"time"`
	Level         Level                 `json:"level"`
	Message       string                `json:"message"`
	RequestID     string                `json:"request_id,omitempty"`
	OperationID   operation.OperationID `json:"operation_id,omitempty"`
	Resources     []operation.Target    `json:"resources,omitempty"`
	Stage         operation.Stage       `json:"stage,omitempty"`
	Result        operation.Result      `json:"result,omitempty"`
	Reason        operation.ReasonClass `json:"reason,omitempty"`
	Error         string                `json:"error,omitempty"`
}

// Clone returns a record whose resource list cannot alias caller-owned memory.
func (record LogRecord) Clone() LogRecord {
	clone := record
	clone.Resources = append([]operation.Target(nil), record.Resources...)
	return clone
}

// Validate rejects malformed or unbounded log facts before they reach the
// daemon stream, while allowing operation-less startup and shutdown records.
func (record LogRecord) Validate() error {
	if record.SchemaVersion != LogSchemaVersion {
		return fmt.Errorf("unsupported structured log schema version %d", record.SchemaVersion)
	}
	if record.Time.IsZero() {
		return errors.New("structured log time must be present")
	}
	if !record.Level.Valid() {
		return fmt.Errorf("unsupported structured log level %q", record.Level)
	}
	if err := validateText("message", record.Message, 4096, false); err != nil {
		return err
	}
	if err := validateText("request ID", record.RequestID, 128, true); err != nil {
		return err
	}
	if record.OperationID != "" {
		if err := record.OperationID.Validate(); err != nil {
			return err
		}
	}
	for _, resource := range record.Resources {
		if err := resource.Validate(); err != nil {
			return fmt.Errorf("structured log resource: %w", err)
		}
	}
	if record.Stage != "" && !record.Stage.Valid() {
		return fmt.Errorf("unsupported structured log stage %q", record.Stage)
	}
	if record.Result != "" && !record.Result.Valid() {
		return fmt.Errorf("unsupported structured log result %q", record.Result)
	}
	if record.Reason != "" && !record.Reason.Valid() {
		return fmt.Errorf("unsupported structured log reason %q", record.Reason)
	}
	return validateText("error", record.Error, 8192, true)
}

// JSONLogger serializes complete records under one lock so concurrent daemon
// requests cannot interleave newline-delimited JSON output.
type JSONLogger struct {
	mu     sync.Mutex
	writer io.Writer
	now    func() time.Time
}

// NewJSONLogger builds a structured logger around a daemon-owned writer and
// wall clock; timestamps remain diagnostic facts, not benchmark samples.
func NewJSONLogger(writer io.Writer, now func() time.Time) (*JSONLogger, error) {
	if writer == nil {
		return nil, errors.New("structured log writer must not be nil")
	}
	if now == nil {
		return nil, errors.New("structured log clock must not be nil")
	}
	return &JSONLogger{writer: writer, now: now}, nil
}

// Write validates and emits exactly one complete JSON line, assigning the
// daemon wall timestamp when the caller deliberately leaves it empty.
func (logger *JSONLogger) Write(record LogRecord) error {
	if record.Time.IsZero() {
		record.Time = logger.now()
	}
	if record.SchemaVersion == 0 {
		record.SchemaVersion = LogSchemaVersion
	}
	if err := record.Validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode structured log record: %w", err)
	}
	encoded = append(encoded, '\n')
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if _, err := logger.writer.Write(encoded); err != nil {
		return fmt.Errorf("write structured log record: %w", err)
	}
	return nil
}

// validateText enforces bounded, newline-free structured fields so one record
// always occupies one physical JSON line and cannot inject terminal controls.
func validateText(name, value string, maximum int, optional bool) error {
	if value == "" {
		if optional {
			return nil
		}
		return fmt.Errorf("structured log %s must not be empty", name)
	}
	if len(value) > maximum {
		return fmt.Errorf("structured log %s exceeds %d bytes", name, maximum)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("structured log %s has surrounding whitespace", name)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("structured log %s contains a control character", name)
		}
	}
	return nil
}
