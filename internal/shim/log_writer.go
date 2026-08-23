package shim

import (
	"errors"

	"mydocker/internal/logstore"
)

// LogAppender is the append-only workload output boundary implemented by logstore.Store.
type LogAppender interface {
	// Append durably adds bytes to the selected globally ordered Attempt stream.
	Append(logstore.Stream, []byte) (logstore.Frame, error)
}

// LogWriter adapts one injected durable stream to the io.Writer expected by os/exec.
type LogWriter struct {
	store  LogAppender
	stream logstore.Stream
}

// NewLogWriter binds one stdout or stderr writer to an injected Attempt log store.
func NewLogWriter(store LogAppender, stream logstore.Stream) (*LogWriter, error) {
	if store == nil {
		return nil, errors.New("shim log appender must not be nil")
	}
	if !stream.Valid() {
		return nil, errors.New("shim log writer requires stdout or stderr")
	}
	return &LogWriter{store: store, stream: stream}, nil
}

// Write durably appends one non-empty child-output chunk before reporting it consumed.
func (writer *LogWriter) Write(payload []byte) (int, error) {
	if len(payload) == 0 {
		return 0, nil
	}
	if _, err := writer.store.Append(writer.stream, append([]byte(nil), payload...)); err != nil {
		return 0, err
	}
	return len(payload), nil
}
