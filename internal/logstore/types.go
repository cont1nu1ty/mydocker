package logstore

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"mydocker/internal/domain"
)

const (
	// SchemaVersion is the only on-disk workload-log frame schema understood by M3.
	SchemaVersion uint32 = 1
	// MaxPayloadBytes bounds one append so a corrupt length cannot force an unbounded allocation during recovery.
	MaxPayloadBytes = 16 << 20
)

var (
	// ErrUnsafePath reports a path, owner, link, or permission shape that is unsafe for daemon-owned logs.
	ErrUnsafePath = errors.New("unsafe workload log path")
	// ErrCorrupt reports a complete on-disk frame that cannot be trusted or ordered.
	ErrCorrupt = errors.New("corrupt workload log")
	// ErrUnsupportedSchema reports a complete frame written with an unknown schema.
	ErrUnsupportedSchema = errors.New("unsupported workload log schema")
	// ErrIdentityMismatch reports a frame that belongs to a different Container Attempt than the opened store.
	ErrIdentityMismatch = errors.New("workload log identity mismatch")
	// ErrClosed reports use of a Store after Close.
	ErrClosed = errors.New("workload log store is closed")
	// ErrAppendUnavailable reports that a prior write or synchronization failed and reopening is required before another append.
	ErrAppendUnavailable = errors.New("workload log append requires reopen")
	// ErrReadUnavailable reports that a writer has not yet published a synchronized snapshot boundary.
	ErrReadUnavailable = errors.New("workload log committed read boundary is unavailable")
	// ErrInvalidLimit reports a negative Read page limit.
	ErrInvalidLimit = errors.New("workload log read limit must not be negative")
	// ErrInUse reports that another Store currently owns the same log file.
	ErrInUse = errors.New("workload log is already open")
	// ErrNotFound reports that no log file exists at an internally resolved Attempt location.
	ErrNotFound = errors.New("workload log was not found")
)

// Identity binds a log file to exactly one API Container and kernel-facing Attempt.
type Identity struct {
	ContainerID domain.ContainerID `json:"container_id"`
	AttemptID   domain.AttemptID   `json:"attempt_id"`
}

// Validate rejects an incomplete or persistence-unsafe Container Attempt binding.
func (identity Identity) Validate() error {
	if err := identity.ContainerID.Validate(); err != nil {
		return fmt.Errorf("log container identity: %w", err)
	}
	if err := identity.AttemptID.Validate(); err != nil {
		return fmt.Errorf("log attempt identity: %w", err)
	}
	return nil
}

// Stream identifies one of the two workload byte streams persisted by M3.
type Stream string

const (
	// StreamStdout records bytes emitted through the workload standard-output pipe.
	StreamStdout Stream = "stdout"
	// StreamStderr records bytes emitted through the workload standard-error pipe.
	StreamStderr Stream = "stderr"
)

// Valid reports whether the stream belongs to the bounded workload-output vocabulary.
func (stream Stream) Valid() bool {
	return stream == StreamStdout || stream == StreamStderr
}

// Cursor is the strictly increasing position shared by both streams in one Attempt log.
type Cursor uint64

// Frame is one durable workload-output append with global and per-stream ordering evidence.
type Frame struct {
	SchemaVersion uint32   `json:"schema_version"`
	Identity      Identity `json:"identity"`
	Stream        Stream   `json:"stream"`
	Cursor        Cursor   `json:"cursor"`
	Sequence      uint64   `json:"sequence"`
	Payload       []byte   `json:"payload"`
	PayloadSHA256 string   `json:"payload_sha256"`
}

// Clone returns a frame whose payload cannot alias store-owned or caller-owned memory.
func (frame Frame) Clone() Frame {
	clone := frame
	clone.Payload = append([]byte(nil), frame.Payload...)
	return clone
}

// Validate checks the frame schema, identity, bounded stream position, payload, and checksum.
func (frame Frame) Validate() error {
	if frame.SchemaVersion != SchemaVersion {
		return fmt.Errorf("%w: version %d", ErrUnsupportedSchema, frame.SchemaVersion)
	}
	if err := frame.Identity.Validate(); err != nil {
		return err
	}
	if !frame.Stream.Valid() {
		return fmt.Errorf("unsupported workload log stream %q", frame.Stream)
	}
	if frame.Cursor == 0 || frame.Sequence == 0 {
		return errors.New("workload log cursor and sequence must be greater than zero")
	}
	if len(frame.Payload) == 0 {
		return errors.New("workload log payload must not be empty")
	}
	if len(frame.Payload) > MaxPayloadBytes {
		return fmt.Errorf("workload log payload exceeds %d bytes", MaxPayloadBytes)
	}
	digest := sha256.Sum256(frame.Payload)
	if frame.PayloadSHA256 != hex.EncodeToString(digest[:]) {
		return errors.New("workload log payload checksum does not match payload")
	}
	return nil
}

// File is the minimum random-read, append, durability, and metadata surface used by Store.
// It is exported so deterministic fault tests can wrap a real file without changing production defaults.
type File interface {
	io.ReaderAt
	io.Writer
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
	Close() error
	Fd() uintptr
}

// Directory is the minimum identity and durability surface used to synchronize a newly linked log file.
type Directory interface {
	Stat() (os.FileInfo, error)
	Sync() error
	Close() error
}

// FilePrimitives opens no-follow files and directories and inspects path components for the fail-closed path policy.
type FilePrimitives interface {
	Lstat(name string) (os.FileInfo, error)
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	OpenDirectory(name string) (Directory, error)
}

// Option applies an explicit Store or Reader open dependency override, primarily for deterministic fault testing.
type Option func(*openConfig) error

// WithFilePrimitives replaces the production Store and Reader filesystem boundary with a test-controlled implementation.
func WithFilePrimitives(primitives FilePrimitives) Option {
	return func(config *openConfig) error {
		if primitives == nil {
			return errors.New("workload log file primitives must not be nil")
		}
		config.files = primitives
		return nil
	}
}
