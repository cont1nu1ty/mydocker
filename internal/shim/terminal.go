package shim

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"mydocker/internal/domain"
	"mydocker/internal/ownership"
	"mydocker/internal/strictjson"
)

const maxTerminalBytes = 1 << 20

var (
	// ErrTerminalExists reports an attempted replacement of the immutable Attempt terminal record with different content.
	ErrTerminalExists = errors.New("terminal record already exists")
	// ErrUnsafeArtifact reports an unsafe owner, mode, link, or path for wrapper-owned files.
	ErrUnsafeArtifact = errors.New("unsafe shim artifact")
	// ErrTerminalCorrupt reports terminal JSON, identity, outcome, or checksum that cannot be trusted.
	ErrTerminalCorrupt = errors.New("corrupt terminal record")
)

// TerminalReason is the bounded reason why an Attempt stopped being prepared or running.
type TerminalReason string

const (
	// TerminalChildExit records a child exit whose wait and OOM evidence are independently represented.
	TerminalChildExit TerminalReason = "child_exit"
	// TerminalStartFailed records the consumed one-shot gate when fork/exec failed before a child existed.
	TerminalStartFailed TerminalReason = "start_failed"
	// TerminalLaunchAborted records a workload that exec'd but was killed and
	// fully reaped before a strong child handle could be published.
	TerminalLaunchAborted TerminalReason = "launch_aborted"
	// TerminalWaitFailed records a child that existed but could not yield a trustworthy wait result.
	TerminalWaitFailed TerminalReason = "wait_failed"
)

// Valid reports whether reason is in the durable M3 terminal vocabulary.
func (reason TerminalReason) Valid() bool {
	return reason == TerminalChildExit || reason == TerminalStartFailed || reason == TerminalLaunchAborted || reason == TerminalWaitFailed
}

// TerminalRecord is the immutable owner-scoped result written by the long-lived init wrapper.
type TerminalRecord struct {
	SchemaVersion   uint32             `json:"schema_version"`
	Owner           ownership.OwnerKey `json:"owner"`
	ContainerID     domain.ContainerID `json:"container_id"`
	AttemptID       domain.AttemptID   `json:"attempt_id"`
	WrapperEvidence string             `json:"wrapper_evidence_sha256"`
	Reason          TerminalReason     `json:"reason"`
	Outcome         domain.Outcome     `json:"outcome"`
	ChildExit       *ChildExitEvidence `json:"child_exit,omitempty"`
	Diagnostic      string             `json:"diagnostic,omitempty"`
	RecordedAt      time.Time          `json:"recorded_at"`
	ChecksumSHA256  string             `json:"checksum_sha256"`
}

// Clone returns a terminal record whose outcome and optional child facts do not alias store memory.
func (record TerminalRecord) Clone() TerminalRecord {
	clone := record
	clone.Outcome = record.Outcome.Clone()
	if record.ChildExit != nil {
		exit := record.ChildExit.Clone()
		clone.ChildExit = &exit
	}
	return clone
}

// Validate checks schema, owner and Attempt scope, wrapper evidence, coherent outcome, and checksum.
func (record TerminalRecord) Validate() error {
	if record.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported terminal schema version %d", record.SchemaVersion)
	}
	if err := record.Owner.Validate(); err != nil {
		return fmt.Errorf("terminal owner: %w", err)
	}
	if record.Owner.Target.Kind != "container" || record.Owner.Target.ID != string(record.ContainerID) {
		return errors.New("terminal owner must target the exact Container")
	}
	if err := record.ContainerID.Validate(); err != nil {
		return err
	}
	if err := record.AttemptID.Validate(); err != nil {
		return err
	}
	if !validDigest(record.WrapperEvidence) || !record.Reason.Valid() || record.RecordedAt.IsZero() {
		return errors.New("terminal wrapper evidence, reason, and record time must be explicit")
	}
	if err := record.Outcome.Validate(); err != nil {
		return fmt.Errorf("terminal outcome: %w", err)
	}
	if len(record.Diagnostic) > 2048 || bytes.IndexByte([]byte(record.Diagnostic), 0) >= 0 {
		return errors.New("terminal diagnostic is not persistence safe")
	}
	switch record.Reason {
	case TerminalStartFailed:
		if record.ChildExit != nil || record.Outcome.Presence != domain.OutcomeNotApplicable || record.Diagnostic == "" {
			return errors.New("start failure requires no child, a not-applicable outcome, and a diagnostic")
		}
	case TerminalLaunchAborted:
		if record.ChildExit != nil || record.Outcome.Presence != domain.OutcomeUnknown || record.Diagnostic == "" {
			return errors.New("aborted launch requires no publishable child, an unknown outcome, and a diagnostic")
		}
	case TerminalChildExit, TerminalWaitFailed:
		if record.ChildExit == nil {
			return errors.New("child terminal reason requires child exit evidence")
		}
		if err := record.ChildExit.Validate(); err != nil {
			return err
		}
		expected := record.ChildExit.DomainOutcome()
		if !reflect.DeepEqual(record.Outcome, expected) {
			return errors.New("terminal outcome does not match child exit and OOM evidence")
		}
		if record.Reason == TerminalWaitFailed && record.ChildExit.WaitError == "" {
			return errors.New("wait-failed terminal reason requires a wait diagnostic")
		}
		if record.Reason == TerminalChildExit && record.ChildExit.WaitError != "" {
			return errors.New("child-exit terminal reason cannot contain a wait failure")
		}
	}
	expectedChecksum, err := terminalChecksum(record)
	if err != nil {
		return err
	}
	if record.ChecksumSHA256 != expectedChecksum {
		return errors.New("terminal checksum does not match its canonical record")
	}
	return nil
}

// NewTerminalRecord constructs and checksums one immutable Attempt terminal result.
func NewTerminalRecord(spec InitSpec, reason TerminalReason, outcome domain.Outcome, childExit *ChildExitEvidence, diagnostic string, recordedAt time.Time) (TerminalRecord, error) {
	record := TerminalRecord{
		SchemaVersion: SchemaVersion, Owner: spec.Owner, ContainerID: spec.ContainerID,
		AttemptID: spec.AttemptID, WrapperEvidence: spec.WrapperEvidence, Reason: reason,
		Outcome: outcome.Clone(), Diagnostic: diagnostic, RecordedAt: recordedAt,
	}
	if childExit != nil {
		clone := childExit.Clone()
		record.ChildExit = &clone
	}
	checksum, err := terminalChecksum(record)
	if err != nil {
		return TerminalRecord{}, err
	}
	record.ChecksumSHA256 = checksum
	if err := record.Validate(); err != nil {
		return TerminalRecord{}, err
	}
	return record, nil
}

// terminalChecksum hashes canonical JSON with the checksum field omitted.
func terminalChecksum(record TerminalRecord) (string, error) {
	record.ChecksumSHA256 = ""
	encoded, err := json.Marshal(record)
	if err != nil {
		return "", fmt.Errorf("encode terminal checksum payload: %w", err)
	}
	return bytesDigest(encoded), nil
}

// FileTerminalStore atomically persists one terminal record at a private absolute path.
type FileTerminalStore struct {
	path          string
	directoryInfo os.FileInfo
	fs            terminalFS
}

// NewFileTerminalStore validates the artifact path and prepares a production atomic filesystem boundary.
func NewFileTerminalStore(path string) (*FileTerminalStore, error) {
	return newFileTerminalStore(path, osTerminalFS{})
}

// newFileTerminalStore injects atomic primitives for deterministic ordering and failure tests.
func newFileTerminalStore(path string, fs terminalFS) (*FileTerminalStore, error) {
	if err := validateAbsolutePath("terminal path", path); err != nil {
		return nil, err
	}
	if fs == nil {
		return nil, errors.New("terminal filesystem must not be nil")
	}
	if err := validatePrivateDirectory(fs, filepath.Dir(path)); err != nil {
		return nil, err
	}
	directoryInfo, err := fs.Lstat(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("inspect terminal parent identity: %w", err)
	}
	return &FileTerminalStore{path: path, directoryInfo: directoryInfo, fs: fs}, nil
}

// Load reads and validates a complete immutable terminal record without repairing corrupt data.
func (store *FileTerminalStore) Load() (TerminalRecord, bool, error) {
	if err := store.validateDirectoryIdentity(); err != nil {
		return TerminalRecord{}, false, err
	}
	info, err := store.fs.Lstat(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return TerminalRecord{}, false, nil
	}
	if err != nil {
		return TerminalRecord{}, false, fmt.Errorf("inspect terminal record: %w", err)
	}
	if err := validatePrivateFile(info); err != nil {
		return TerminalRecord{}, false, err
	}
	payload, err := store.fs.ReadFile(store.path, maxTerminalBytes+1)
	if err != nil {
		return TerminalRecord{}, false, fmt.Errorf("read terminal record: %w", err)
	}
	if len(payload) > maxTerminalBytes {
		return TerminalRecord{}, false, fmt.Errorf("%w: record exceeds %d bytes", ErrTerminalCorrupt, maxTerminalBytes)
	}
	var record TerminalRecord
	if err := strictjson.Decode(payload, &record); err != nil {
		return TerminalRecord{}, false, fmt.Errorf("%w: decode: %v", ErrTerminalCorrupt, err)
	}
	if err := record.Validate(); err != nil {
		return TerminalRecord{}, false, fmt.Errorf("%w: %v", ErrTerminalCorrupt, err)
	}
	return record.Clone(), true, nil
}

// Commit publishes the first valid terminal record, or confirms the exact same
// record after an uncertain prior rename by re-synchronizing its parent directory.
func (store *FileTerminalStore) Commit(record TerminalRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	if err := store.validateDirectoryIdentity(); err != nil {
		return err
	}
	if _, err := store.fs.Lstat(store.path); err == nil {
		return store.confirmExisting(record)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect terminal destination: %w", err)
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode terminal record: %w", err)
	}
	encoded = append(encoded, '\n')
	if len(encoded) > maxTerminalBytes {
		return errors.New("terminal record exceeds maximum size")
	}
	directory := filepath.Dir(store.path)
	temporary, temporaryPath, err := store.fs.CreateTemp(directory, ".terminal-*")
	if err != nil {
		return fmt.Errorf("create terminal temporary file: %w", err)
	}
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = store.fs.Remove(temporaryPath)
		}
	}()
	if err := writeAll(temporary, encoded); err != nil {
		return fmt.Errorf("write terminal temporary file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("synchronize terminal temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close terminal temporary file: %w", err)
	}
	if err := store.fs.RenameNoReplace(temporaryPath, store.path); err != nil {
		if errors.Is(err, os.ErrExist) || errors.Is(err, unix.EEXIST) {
			return store.confirmExisting(record)
		}
		return fmt.Errorf("publish terminal record: %w", err)
	}
	committed = true
	return store.syncDirectory()
}

// confirmExisting accepts only a checksum-bound canonical match and then
// repeats the directory durability boundary without replacing terminal data.
func (store *FileTerminalStore) confirmExisting(record TerminalRecord) error {
	existing, found, err := store.Load()
	if err != nil {
		return fmt.Errorf("validate existing terminal record: %w", err)
	}
	if !found {
		return errors.New("terminal destination disappeared during recovery")
	}
	equal, err := sameTerminalRecord(existing, record)
	if err != nil {
		return fmt.Errorf("compare existing terminal record: %w", err)
	}
	if !equal {
		return fmt.Errorf("%w: destination contains a different record", ErrTerminalExists)
	}
	return store.syncDirectory()
}

// sameTerminalRecord requires both the persisted checksum and canonical JSON
// record to match, avoiding time-location pointer or monotonic-clock comparisons.
func sameTerminalRecord(left, right TerminalRecord) (bool, error) {
	if left.ChecksumSHA256 != right.ChecksumSHA256 {
		return false, nil
	}
	leftPayload, err := json.Marshal(left)
	if err != nil {
		return false, err
	}
	rightPayload, err := json.Marshal(right)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftPayload, rightPayload), nil
}

// syncDirectory makes a published name durable and reports both sync and
// descriptor-close failures so callers retain an uncertain persistence state.
func (store *FileTerminalStore) syncDirectory() error {
	directoryFile, err := store.fs.OpenDirectory(filepath.Dir(store.path))
	if err != nil {
		return fmt.Errorf("open terminal directory for synchronization: %w", err)
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	if syncErr != nil || closeErr != nil {
		var wrappedSyncErr, wrappedCloseErr error
		if syncErr != nil {
			wrappedSyncErr = fmt.Errorf("synchronize terminal directory: %w", syncErr)
		}
		if closeErr != nil {
			wrappedCloseErr = fmt.Errorf("close synchronized terminal directory: %w", closeErr)
		}
		return errors.Join(wrappedSyncErr, wrappedCloseErr)
	}
	return nil
}

// validateDirectoryIdentity rejects publication or recovery after the private parent directory is replaced.
func (store *FileTerminalStore) validateDirectoryIdentity() error {
	current, err := store.fs.Lstat(filepath.Dir(store.path))
	if err != nil {
		return fmt.Errorf("%w: inspect terminal parent identity: %v", ErrUnsafeArtifact, err)
	}
	if !os.SameFile(store.directoryInfo, current) {
		return fmt.Errorf("%w: terminal parent directory was replaced", ErrUnsafeArtifact)
	}
	return validatePrivateDirectory(store.fs, filepath.Dir(store.path))
}

// syncFile is the write or directory handle needed for the atomic publication sequence.
type syncFile interface {
	io.Writer
	Sync() error
	Close() error
}

// terminalFS is the narrow filesystem surface used to test publication ordering and faults.
type terminalFS interface {
	Lstat(string) (os.FileInfo, error)
	ReadFile(string, int64) ([]byte, error)
	CreateTemp(string, string) (syncFile, string, error)
	RenameNoReplace(string, string) error
	OpenDirectory(string) (syncFile, error)
	Remove(string) error
}

// osTerminalFS provides no-replace publication and private regular-file reads on Linux.
type osTerminalFS struct{}

// Lstat inspects an artifact without following its final path component; the
// exact /proc/self/fd/N retained-directory form follows only that already-open FD.
func (osTerminalFS) Lstat(path string) (os.FileInfo, error) {
	if _, retained := retainedDirectoryFD(path); retained {
		return os.Stat(path)
	}
	return os.Lstat(path)
}

// retainedDirectoryFD recognizes only one complete procfs descriptor path and
// rejects suffixes, relative spellings, zero, signs, and non-decimal aliases.
func retainedDirectoryFD(path string) (int, bool) {
	const prefix = "/proc/self/fd/"
	if !strings.HasPrefix(path, prefix) {
		return 0, false
	}
	text := strings.TrimPrefix(path, prefix)
	if text == "" || strings.ContainsRune(text, filepath.Separator) || text[0] == '0' || strings.Trim(text, "0123456789") != "" {
		return 0, false
	}
	fd, err := strconv.Atoi(text)
	return fd, err == nil && fd > 0
}

// ReadFile opens a regular file, rejects a path swap, and returns at most limit bytes.
func (osTerminalFS) ReadFile(path string, limit int64) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(before, after) {
		return nil, fmt.Errorf("%w: terminal path changed while opening", ErrUnsafeArtifact)
	}
	return io.ReadAll(io.LimitReader(file, limit))
}

// CreateTemp creates a private temporary file in the terminal record directory.
func (osTerminalFS) CreateTemp(directory, pattern string) (syncFile, string, error) {
	file, err := os.CreateTemp(directory, pattern)
	if err != nil {
		return nil, "", err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, "", err
	}
	return file, file.Name(), nil
}

// RenameNoReplace atomically publishes a terminal record without replacing an existing result.
func (osTerminalFS) RenameNoReplace(oldPath, newPath string) error {
	return unix.Renameat2(unix.AT_FDCWD, oldPath, unix.AT_FDCWD, newPath, unix.RENAME_NOREPLACE)
}

// OpenDirectory opens the parent so rename durability can be explicitly synchronized.
func (osTerminalFS) OpenDirectory(path string) (syncFile, error) {
	return os.Open(path)
}

// Remove cleans an unpublished temporary file after a failed commit.
func (osTerminalFS) Remove(path string) error {
	return os.Remove(path)
}

// validatePrivateDirectory requires the immediate artifact parent to be owned and inaccessible to other users.
func validatePrivateDirectory(fs terminalFS, path string) error {
	info, err := fs.Lstat(path)
	if err != nil {
		return fmt.Errorf("%w: inspect parent directory: %v", ErrUnsafeArtifact, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%w: parent must be a private real directory, got mode %s", ErrUnsafeArtifact, info.Mode())
	}
	if err := validateCurrentOwner(info); err != nil {
		return err
	}
	return nil
}

// validatePrivateFile requires a same-owner, non-link, mode-0600-or-stricter regular artifact.
func validatePrivateFile(info os.FileInfo) error {
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o177 != 0 {
		return fmt.Errorf("%w: terminal record must be a private regular file", ErrUnsafeArtifact)
	}
	return validateCurrentOwner(info)
}

// validateCurrentOwner rejects artifacts not owned by the wrapper effective user.
func validateCurrentOwner(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("%w: artifact owner does not match wrapper effective user", ErrUnsafeArtifact)
	}
	return nil
}

// writeAll prevents a short write from being mistaken for a complete terminal record.
func writeAll(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(payload) {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}
