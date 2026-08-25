package slim

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"mydocker/internal/ownership"
	"mydocker/internal/provider"
)

const maxArtifactBytes = 64 << 10

const (
	artifactStateClosed    = "closed"
	artifactStateConsuming = "consuming"
	artifactStateReleased  = "released"
	artifactStateReady     = "ready"
)

var artifactTransitionLocks sync.Map
var sharedOwnerOperationLocks sync.Map

// sharedOwnerOperationLock returns the process-wide action lock for one
// runtime-root/owner pair. Launcher recovery and provider control actions use
// the same lock even when reached through different provider instances.
func sharedOwnerOperationLock(root, token string) *sync.Mutex {
	lockKey := root + "\x00" + token
	value, _ := sharedOwnerOperationLocks.LoadOrStore(lockKey, &sync.Mutex{})
	return value.(*sync.Mutex)
}

// artifactRecord is one checksummed gate or stream identity under a deterministic owner directory.
type artifactRecord struct {
	SchemaVersion   uint32             `json:"schema_version"`
	Owner           ownership.OwnerKey `json:"owner"`
	Kind            ownership.Kind     `json:"kind"`
	LocalID         string             `json:"local_id"`
	ReceiptEvidence string             `json:"receipt_evidence_sha256"`
	State           string             `json:"state"`
	ChecksumSHA256  string             `json:"checksum_sha256"`
}

// Validate checks bounded artifact identity, state, owner scope, and canonical checksum.
func (record artifactRecord) Validate() error {
	if record.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported slim artifact schema %d", record.SchemaVersion)
	}
	if err := record.Owner.Validate(); err != nil {
		return err
	}
	if record.LocalID != localIDFor(record.Kind) || !validDigest(record.ReceiptEvidence) {
		return errors.New("slim artifact identity or receipt evidence is invalid")
	}
	switch record.Kind {
	case ownership.KindStartGate:
		if record.State != artifactStateClosed && record.State != artifactStateConsuming && record.State != artifactStateReleased {
			return errors.New("start-gate artifact has unsupported state")
		}
	case ownership.KindStreams:
		if record.State != artifactStateReady {
			return errors.New("stream artifact must be ready")
		}
	default:
		return errors.New("only start-gate and stream artifacts are file-managed")
	}
	checksum, err := artifactChecksum(record)
	if err != nil {
		return err
	}
	if record.ChecksumSHA256 != checksum {
		return errors.New("slim artifact checksum does not match its contents")
	}
	return nil
}

// newArtifactRecord constructs one checksum-bound initial or transitioned artifact value.
func newArtifactRecord(owner ownership.OwnerKey, kind ownership.Kind, receiptEvidence, state string) (artifactRecord, error) {
	record := artifactRecord{
		SchemaVersion: SchemaVersion, Owner: owner, Kind: kind, LocalID: localIDFor(kind),
		ReceiptEvidence: receiptEvidence, State: state,
	}
	checksum, err := artifactChecksum(record)
	if err != nil {
		return artifactRecord{}, err
	}
	record.ChecksumSHA256 = checksum
	if err := record.Validate(); err != nil {
		return artifactRecord{}, err
	}
	return record, nil
}

// artifactChecksum hashes canonical JSON with the checksum field omitted.
func artifactChecksum(record artifactRecord) (string, error) {
	record.ChecksumSHA256 = ""
	return ownership.EvidenceDigest(record)
}

// artifactStore owns only deterministic private metadata files; it never accepts a receipt path.
type artifactStore struct {
	root          string
	rootInfo      os.FileInfo
	syncDirectory func(string) error
}

// newArtifactStore captures the configured private runtime-root identity without creating artifacts.
func newArtifactStore(root string) (*artifactStore, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return nil, fmt.Errorf("%w: runtime root must be a clean absolute non-root path", ErrArtifactUnsafe)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("inspect slim runtime root: %w", err)
	}
	if err := validatePrivateDirectory(info); err != nil {
		return nil, err
	}
	return &artifactStore{root: root, rootInfo: info, syncDirectory: syncArtifactDirectory}, nil
}

// Ensure creates or validates one deterministic gate or stream artifact and returns its current record.
func (store *artifactStore) Ensure(owner ownership.OwnerKey, kind ownership.Kind, receiptEvidence, initialState string) (artifactRecord, error) {
	unlock := store.lockTransition(owner)
	defer unlock()
	if err := store.ensureOwnerDirectory(owner); err != nil {
		return artifactRecord{}, err
	}
	expected, err := newArtifactRecord(owner, kind, receiptEvidence, initialState)
	if err != nil {
		return artifactRecord{}, err
	}
	existing, found, err := store.Read(owner, kind)
	if err != nil {
		return artifactRecord{}, err
	}
	if found {
		if existing.Owner != expected.Owner || existing.Kind != expected.Kind || existing.LocalID != expected.LocalID || existing.ReceiptEvidence != expected.ReceiptEvidence {
			return artifactRecord{}, fmt.Errorf("%w: existing artifact belongs to different receipt", ErrArtifactUnsafe)
		}
		return existing, nil
	}
	if err := store.write(expected); err != nil {
		return artifactRecord{}, err
	}
	return expected, nil
}

// Read validates and returns one deterministic artifact without repairing malformed state.
func (store *artifactStore) Read(owner ownership.OwnerKey, kind ownership.Kind) (artifactRecord, bool, error) {
	if err := store.validateRoot(); err != nil {
		return artifactRecord{}, false, err
	}
	path, err := store.artifactPath(owner, kind)
	if err != nil {
		return artifactRecord{}, false, err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return artifactRecord{}, false, nil
	}
	if err != nil {
		return artifactRecord{}, false, fmt.Errorf("inspect slim artifact: %w", err)
	}
	if err := validatePrivateFile(info); err != nil {
		return artifactRecord{}, false, err
	}
	file, err := os.Open(path)
	if err != nil {
		return artifactRecord{}, false, fmt.Errorf("open slim artifact: %w", err)
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !os.SameFile(info, after) {
		return artifactRecord{}, false, fmt.Errorf("%w: artifact changed while opening", ErrArtifactUnsafe)
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxArtifactBytes+1))
	if err != nil {
		return artifactRecord{}, false, err
	}
	if len(payload) > maxArtifactBytes {
		return artifactRecord{}, false, errors.New("slim artifact exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var record artifactRecord
	if err := decoder.Decode(&record); err != nil {
		return artifactRecord{}, false, fmt.Errorf("decode slim artifact: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return artifactRecord{}, false, err
	}
	if err := record.Validate(); err != nil {
		return artifactRecord{}, false, err
	}
	if record.Owner != owner || record.Kind != kind || record.LocalID != localIDFor(kind) {
		return artifactRecord{}, false, fmt.Errorf("%w: artifact scope differs from derived path", ErrArtifactUnsafe)
	}
	return record, true, nil
}

// Transition atomically updates the state of one exact existing artifact while retaining receipt identity.
func (store *artifactStore) Transition(owner ownership.OwnerKey, kind ownership.Kind, receiptEvidence, state string) error {
	unlock := store.lockTransition(owner)
	defer unlock()
	record, found, err := store.Read(owner, kind)
	if err != nil {
		return err
	}
	if !found || record.ReceiptEvidence != receiptEvidence {
		return fmt.Errorf("%w: artifact to transition is absent or belongs to another receipt", ErrArtifactUnsafe)
	}
	if kind != ownership.KindStartGate ||
		(record.State == artifactStateClosed && state != artifactStateConsuming) ||
		(record.State == artifactStateConsuming && state != artifactStateReleased) ||
		record.State == artifactStateReleased {
		return fmt.Errorf("%w: unsupported artifact transition %q -> %q", ErrArtifactUnsafe, record.State, state)
	}
	updated, err := newArtifactRecord(owner, kind, receiptEvidence, state)
	if err != nil {
		return err
	}
	return store.write(updated)
}

// ConfirmState re-reads one exact artifact and fsyncs its parent before a
// caller relies on a state whose earlier rename may have returned uncertain
// directory durability.
func (store *artifactStore) ConfirmState(owner ownership.OwnerKey, kind ownership.Kind, receiptEvidence, state string) error {
	unlock := store.lockTransition(owner)
	defer unlock()
	record, found, err := store.Read(owner, kind)
	if err != nil {
		return err
	}
	if !found || record.ReceiptEvidence != receiptEvidence || record.State != state {
		return fmt.Errorf("%w: artifact state confirmation differs from the exact receipt", ErrArtifactUnsafe)
	}
	path, err := store.artifactPath(owner, kind)
	if err != nil {
		return err
	}
	if err := store.syncDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("confirm artifact state durability: %w", err)
	}
	return nil
}

// lockTransition serializes monotonic state changes and durability replays for
// one runtime-root/owner pair across all store instances in this process.
func (store *artifactStore) lockTransition(owner ownership.OwnerKey) func() {
	lockKey := store.root + "\x00" + owner.Token
	value, _ := artifactTransitionLocks.LoadOrStore(lockKey, &sync.Mutex{})
	lock := value.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}

// Remove deletes one exact gate or stream artifact and prunes only empty deterministic owner directories.
func (store *artifactStore) Remove(owner ownership.OwnerKey, kind ownership.Kind, receiptEvidence string) (provider.CleanupDisposition, error) {
	unlock := store.lockTransition(owner)
	defer unlock()
	record, found, err := store.Read(owner, kind)
	if err != nil {
		return "", err
	}
	if !found {
		if err := store.confirmArtifactAbsence(owner); err != nil {
			return "", err
		}
		return provider.CleanupAlreadyAbsent, nil
	}
	if record.ReceiptEvidence != receiptEvidence {
		return "", fmt.Errorf("%w: artifact receipt evidence differs", ErrArtifactUnsafe)
	}
	path, err := store.artifactPath(owner, kind)
	if err != nil {
		return "", err
	}
	if err := os.Remove(path); err != nil {
		return "", fmt.Errorf("remove slim artifact: %w", err)
	}
	ownerRoot := filepath.Dir(path)
	ownersRoot := filepath.Dir(ownerRoot)
	if err := store.syncDirectory(ownerRoot); err != nil {
		return "", fmt.Errorf("sync artifact removal: %w", err)
	}
	if err := removeEmptyArtifactDirectory(ownerRoot); err != nil {
		return "", err
	} else if _, statErr := os.Lstat(ownerRoot); errors.Is(statErr, os.ErrNotExist) {
		if err := store.syncDirectory(ownersRoot); err != nil {
			return "", fmt.Errorf("sync owner-directory removal: %w", err)
		}
	}
	if err := removeEmptyArtifactDirectory(ownersRoot); err != nil {
		return "", err
	} else if _, statErr := os.Lstat(ownersRoot); errors.Is(statErr, os.ErrNotExist) {
		if err := store.syncDirectory(store.root); err != nil {
			return "", fmt.Errorf("sync owners-directory removal: %w", err)
		}
	}
	return provider.CleanupRemoved, nil
}

// write file-syncs, atomically renames, and directory-syncs one complete artifact record.
func (store *artifactStore) write(record artifactRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	if err := store.ensureOwnerDirectory(record.Owner); err != nil {
		return err
	}
	path, err := store.artifactPath(record.Owner, record.Kind)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".artifact-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if err := writeFull(temporary, payload); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	committed = true
	return store.syncDirectory(directory)
}

// ensureOwnerDirectory creates only the fixed owners/token levels beneath the verified runtime root.
func (store *artifactStore) ensureOwnerDirectory(owner ownership.OwnerKey) error {
	if err := store.validateRoot(); err != nil {
		return err
	}
	paths, err := deriveArtifactPaths(store.root, owner)
	if err != nil {
		return err
	}
	owners := filepath.Join(store.root, "owners")
	for index, directory := range []string{owners, paths.OwnerRoot} {
		if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create slim artifact directory: %w", err)
		}
		info, err := os.Lstat(directory)
		if err != nil {
			return err
		}
		if err := validatePrivateDirectory(info); err != nil {
			return err
		}
		parent := store.root
		if index == 1 {
			parent = owners
		}
		if err := store.syncDirectory(parent); err != nil {
			return fmt.Errorf("sync slim artifact directory parent: %w", err)
		}
	}
	return nil
}

// confirmArtifactAbsence syncs every still-existing deterministic parent so a
// retry can turn an earlier remove-after-sync-failure window into durable proof.
func (store *artifactStore) confirmArtifactAbsence(owner ownership.OwnerKey) error {
	paths, err := deriveArtifactPaths(store.root, owner)
	if err != nil {
		return err
	}
	ownersRoot := filepath.Join(store.root, "owners")
	for _, directory := range []string{paths.OwnerRoot, ownersRoot, store.root} {
		info, statErr := os.Lstat(directory)
		if errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if statErr != nil {
			return statErr
		}
		if err := validatePrivateDirectory(info); err != nil {
			return err
		}
		if err := store.syncDirectory(directory); err != nil {
			return fmt.Errorf("confirm slim artifact absence: %w", err)
		}
	}
	return nil
}

// removeEmptyArtifactDirectory prunes only an empty deterministic directory
// and treats non-empty or already-absent state as a successful no-op.
func removeEmptyArtifactDirectory(path string) error {
	err := os.Remove(path)
	if err == nil || errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST) {
		return nil
	}
	return fmt.Errorf("prune slim artifact directory: %w", err)
}

// syncArtifactDirectory makes a created, renamed, or removed directory entry
// durable without treating successful file fsync as parent-entry durability.
func syncArtifactDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

// artifactPath derives a bounded filename from the resource kind under the owner token directory.
func (store *artifactStore) artifactPath(owner ownership.OwnerKey, kind ownership.Kind) (string, error) {
	paths, err := deriveArtifactPaths(store.root, owner)
	if err != nil {
		return "", err
	}
	if kind != ownership.KindStartGate && kind != ownership.KindStreams {
		return "", errors.New("only start gate and streams have slim metadata artifacts")
	}
	return filepath.Join(paths.OwnerRoot, localIDFor(kind)+".json"), nil
}

// validateRoot rejects runtime-root replacement or permission drift before every artifact action.
func (store *artifactStore) validateRoot() error {
	info, err := os.Lstat(store.root)
	if err != nil {
		return fmt.Errorf("%w: inspect runtime root: %v", ErrArtifactUnsafe, err)
	}
	if !os.SameFile(store.rootInfo, info) {
		return fmt.Errorf("%w: runtime root was replaced", ErrArtifactUnsafe)
	}
	return validatePrivateDirectory(info)
}

// validatePrivateDirectory requires a real same-owner mode-0700-or-stricter directory.
func validatePrivateDirectory(info os.FileInfo) error {
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%w: directory is not private", ErrArtifactUnsafe)
	}
	return validateOwner(info)
}

// validatePrivateFile requires a real same-owner mode-0600-or-stricter regular file.
func validatePrivateFile(info os.FileInfo) error {
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o177 != 0 {
		return fmt.Errorf("%w: file is not private", ErrArtifactUnsafe)
	}
	return validateOwner(info)
}

// validateOwner requires artifacts to share the provider effective user.
func validateOwner(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("%w: artifact owner differs from provider", ErrArtifactUnsafe)
	}
	return nil
}

// writeFull prevents short artifact writes from being mistaken for a durable record.
func writeFull(writer io.Writer, payload []byte) error {
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

// requireEOF rejects trailing JSON values in private artifact files.
func requireEOF(decoder *json.Decoder) error {
	var value any
	err := decoder.Decode(&value)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("slim artifact contains a second JSON value")
	}
	return err
}
