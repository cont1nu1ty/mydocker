//go:build linux

package shim

import (
	"errors"
	"io"
	"testing"

	"mydocker/internal/domain"
)

// failingStreamWriter returns a configured write shape without performing host I/O.
type failingStreamWriter struct {
	written int
	err     error
}

// Write returns the configured short-write or durable append failure.
func (writer failingStreamWriter) Write([]byte) (int, error) {
	return writer.written, writer.err
}

// TestStickyErrorWriterRetainsOutputFailure verifies a full-length write error
// remains observable even if os/exec later prefers a non-zero process status.
func TestStickyErrorWriterRetainsOutputFailure(t *testing.T) {
	injected := errors.New("injected durable log failure")
	writer := newStickyErrorWriter(failingStreamWriter{written: 4, err: injected})
	if written, err := writer.Write([]byte("data")); written != 4 || !errors.Is(err, injected) {
		t.Fatalf("Write()=(%d,%v), want full write plus injected error", written, err)
	}
	if !errors.Is(writer.Err(), injected) {
		t.Fatalf("Err()=%v, want injected error", writer.Err())
	}
}

// TestStickyErrorWriterTurnsShortWriteIntoFailure verifies a nil-error partial
// write cannot be hidden by a later workload exit status.
func TestStickyErrorWriterTurnsShortWriteIntoFailure(t *testing.T) {
	writer := newStickyErrorWriter(failingStreamWriter{written: 2})
	if written, err := writer.Write([]byte("data")); written != 2 || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Write()=(%d,%v), want short-write failure", written, err)
	}
	if !errors.Is(writer.Err(), io.ErrShortWrite) {
		t.Fatalf("Err()=%v, want io.ErrShortWrite", writer.Err())
	}
}

// TestOutputFailureInvalidatesKnownExit verifies durable log loss produces an
// unknown outcome even when a non-zero exit code was otherwise available.
func TestOutputFailureInvalidatesKnownExit(t *testing.T) {
	exitCode := int32(17)
	evidence := ChildExitEvidence{ExitCode: &exitCode, Signal: string(SignalTERM), OOM: domain.EvidenceUnknown}
	markChildWaitFailure(&evidence, errors.New("durable output unavailable"))
	if evidence.ExitCode != nil || evidence.Signal != "" || evidence.WaitError == "" {
		t.Fatalf("evidence after output failure=%+v, want unknown wait outcome", evidence)
	}
}
