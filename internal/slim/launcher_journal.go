package slim

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"

	"mydocker/internal/domain"
	"mydocker/internal/isolation"
	"mydocker/internal/ownership"
	"mydocker/internal/shim"
)

const (
	launchJournalSchemaVersion uint32 = 1
	maxLaunchJournalBytes             = 64 << 10
	maxLauncherConfigBytes            = 1 << 20
)

var launchOwnerLocks sync.Map

// launchPhase records which parent-death gate facts are durable across daemon restart.
type launchPhase string

const (
	launchPhaseIntent     launchPhase = "intent"
	launchPhaseAuthorized launchPhase = "release_authorized"
	launchPhaseReady      launchPhase = "ready"
)

// launchJournal is the checksummed durable bridge between fork-time pidfd
// capture and the engine's later process-receipt checkpoint.
type launchJournal struct {
	SchemaVersion         uint32                     `json:"schema_version"`
	Owner                 ownership.OwnerKey         `json:"owner"`
	Mode                  shim.Mode                  `json:"mode"`
	SandboxID             domain.SandboxID           `json:"sandbox_id"`
	AttemptID             domain.AttemptID           `json:"attempt_id,omitempty"`
	ConfigEvidenceSHA256  string                     `json:"config_evidence_sha256"`
	Phase                 launchPhase                `json:"phase"`
	ProcessEvidence       *isolation.ProcessEvidence `json:"process_evidence,omitempty"`
	ProcessEvidenceSHA256 string                     `json:"process_evidence_sha256,omitempty"`
	ChecksumSHA256        string                     `json:"checksum_sha256"`
}

// Validate rejects scope drift, incomplete phase transitions, oversized
// process evidence, and a checksum not matching canonical journal content.
func (journal launchJournal) Validate() error {
	if journal.SchemaVersion != launchJournalSchemaVersion || !journal.Mode.Valid() {
		return errors.New("unsupported launch journal schema or mode")
	}
	if err := journal.Owner.Validate(); err != nil {
		return err
	}
	if err := journal.SandboxID.Validate(); err != nil {
		return err
	}
	if !validDigest(journal.ConfigEvidenceSHA256) {
		return errors.New("launch journal requires config evidence")
	}
	if journal.Mode == shim.ModeKeeper {
		if journal.Owner.Target.Kind != "sandbox" || journal.Owner.Target.ID != string(journal.SandboxID) || journal.AttemptID != "" {
			return errors.New("keeper launch journal scope differs from owner")
		}
	} else {
		if journal.Owner.Target.Kind != "container" || journal.AttemptID.Validate() != nil {
			return errors.New("init launch journal scope differs from owner")
		}
	}
	switch journal.Phase {
	case launchPhaseIntent:
		if journal.ProcessEvidence != nil || journal.ProcessEvidenceSHA256 != "" {
			return errors.New("launch intent must not claim a process")
		}
	case launchPhaseAuthorized, launchPhaseReady:
		if journal.ProcessEvidence == nil || !validDigest(journal.ProcessEvidenceSHA256) {
			return errors.New("authorized launch journal requires strong process evidence")
		}
		if err := journal.ProcessEvidence.Validate(); err != nil {
			return err
		}
		if _, err := encodeProcessEvidence(*journal.ProcessEvidence); err != nil {
			return err
		}
		digest, err := ownership.EvidenceDigest(*journal.ProcessEvidence)
		if err != nil {
			return err
		}
		if digest != journal.ProcessEvidenceSHA256 {
			return errors.New("launch journal process digest differs from evidence")
		}
	default:
		return errors.New("unsupported launch journal phase")
	}
	expected, err := launchJournalChecksum(journal)
	if err != nil {
		return err
	}
	if journal.ChecksumSHA256 != expected {
		return errors.New("launch journal checksum differs from content")
	}
	return nil
}

// launchJournalChecksum hashes a canonical journal without recursively including its checksum.
func launchJournalChecksum(journal launchJournal) (string, error) {
	journal.ChecksumSHA256 = ""
	return ownership.EvidenceDigest(journal)
}

// newLaunchIntent constructs the only journal state permitted before process creation.
func newLaunchIntent(config shim.RuntimeConfig) (launchJournal, error) {
	journal := launchJournal{
		SchemaVersion: launchJournalSchemaVersion, Owner: config.Owner, Mode: config.Mode,
		SandboxID: config.SandboxID, AttemptID: config.AttemptID,
		ConfigEvidenceSHA256: config.WrapperEvidence, Phase: launchPhaseIntent,
	}
	checksum, err := launchJournalChecksum(journal)
	if err != nil {
		return launchJournal{}, err
	}
	journal.ChecksumSHA256 = checksum
	return journal, journal.Validate()
}

// withProcess advances only intent to authorized or authorized to ready while
// retaining the exact canonical ProcessEvidence that can later enter a receipt.
func (journal launchJournal) withProcess(phase launchPhase, evidence isolation.ProcessEvidence) (launchJournal, error) {
	switch phase {
	case launchPhaseAuthorized:
		if journal.Phase != launchPhaseIntent || journal.ProcessEvidence != nil || journal.ProcessEvidenceSHA256 != "" {
			return launchJournal{}, errors.New("only an empty launch intent can authorize a process")
		}
	case launchPhaseReady:
		if journal.Phase != launchPhaseAuthorized || journal.ProcessEvidence == nil {
			return launchJournal{}, errors.New("only an authorized process can become ready")
		}
		digest, err := ownership.EvidenceDigest(evidence)
		if err != nil {
			return launchJournal{}, err
		}
		if digest != journal.ProcessEvidenceSHA256 || evidence != *journal.ProcessEvidence {
			return launchJournal{}, errors.New("ready transition cannot replace authorized process evidence")
		}
	default:
		return launchJournal{}, errors.New("process journal transition requires authorized or ready phase")
	}
	copyEvidence := evidence
	journal.Phase = phase
	journal.ProcessEvidence = &copyEvidence
	digest, err := ownership.EvidenceDigest(evidence)
	if err != nil {
		return launchJournal{}, err
	}
	journal.ProcessEvidenceSHA256 = digest
	journal.ChecksumSHA256, err = launchJournalChecksum(journal)
	if err != nil {
		return launchJournal{}, err
	}
	return journal, journal.Validate()
}

// resetIntentAfterVerifiedAbsence clears only an authorized or ready owner;
// callers must first prove the exact recorded ProcessEvidence is absent.
func (journal launchJournal) resetIntentAfterVerifiedAbsence() (launchJournal, error) {
	if (journal.Phase != launchPhaseAuthorized && journal.Phase != launchPhaseReady) || journal.ProcessEvidence == nil {
		return launchJournal{}, errors.New("only a journaled process may reset after verified absence")
	}
	journal.Phase = launchPhaseIntent
	journal.ProcessEvidence = nil
	journal.ProcessEvidenceSHA256 = ""
	checksum, err := launchJournalChecksum(journal)
	if err != nil {
		return launchJournal{}, err
	}
	journal.ChecksumSHA256 = checksum
	return journal, journal.Validate()
}

// launchStore owns the deterministic config and journal paths under one private owner directory.
type launchStore struct {
	paths         ArtifactPaths
	syncDirectory func(string) error
	ownerLock     *sync.Mutex
}

// newLaunchStore validates the internally derived owner path shape and private directory before file access.
func newLaunchStore(paths ArtifactPaths, owner ownership.OwnerKey) (*launchStore, error) {
	runtimeRoot := filepath.Dir(filepath.Dir(paths.OwnerRoot))
	if err := paths.ValidateFor(runtimeRoot, owner); err != nil {
		return nil, err
	}
	info, err := os.Lstat(paths.OwnerRoot)
	if err != nil {
		return nil, fmt.Errorf("inspect launcher owner directory: %w", err)
	}
	if err := validatePrivateDirectory(info); err != nil {
		return nil, err
	}
	return &launchStore{paths: paths, syncDirectory: syncArtifactDirectory, ownerLock: launchOwnerLock(paths.OwnerRoot)}, nil
}

// launchOwnerLock returns one process-wide mutex per deterministic owner path;
// the daemon's state-store lock excludes a concurrent daemon process.
func launchOwnerLock(ownerRoot string) *sync.Mutex {
	value, _ := launchOwnerLocks.LoadOrStore(ownerRoot, &sync.Mutex{})
	return value.(*sync.Mutex)
}

// EnsureIntent prepares canonical config evidence, durably creates or verifies
// immutable config, and durably creates or loads the matching launch journal.
func (store *launchStore) EnsureIntent(config shim.RuntimeConfig) (shim.RuntimeConfig, launchJournal, error) {
	prepared, err := prepareLauncherConfig(config)
	if err != nil {
		return shim.RuntimeConfig{}, launchJournal{}, err
	}
	if err := ensureImmutableRuntimeConfig(store.paths.Config, prepared, store.syncDirectory); err != nil {
		return shim.RuntimeConfig{}, launchJournal{}, err
	}
	expected, err := newLaunchIntent(prepared)
	if err != nil {
		return shim.RuntimeConfig{}, launchJournal{}, err
	}
	store.ownerLock.Lock()
	defer store.ownerLock.Unlock()
	existing, found, err := store.readUnlocked()
	if err != nil {
		return shim.RuntimeConfig{}, launchJournal{}, err
	}
	if found {
		if existing.Owner != expected.Owner || existing.Mode != expected.Mode || existing.SandboxID != expected.SandboxID ||
			existing.AttemptID != expected.AttemptID || existing.ConfigEvidenceSHA256 != expected.ConfigEvidenceSHA256 {
			return shim.RuntimeConfig{}, launchJournal{}, errors.New("existing launch journal belongs to different immutable intent")
		}
		if err := store.syncDirectory(store.paths.OwnerRoot); err != nil {
			return shim.RuntimeConfig{}, launchJournal{}, fmt.Errorf("confirm existing launch journal directory entry: %w", err)
		}
		return prepared, existing, nil
	}
	if err := store.writeUnlocked(expected); err != nil {
		return shim.RuntimeConfig{}, launchJournal{}, err
	}
	return prepared, expected, nil
}

// Read securely opens, bounds, strictly decodes, and validates the exact launch journal under the owner lock.
func (store *launchStore) Read() (launchJournal, bool, error) {
	store.ownerLock.Lock()
	defer store.ownerLock.Unlock()
	return store.readUnlocked()
}

// readUnlocked reads one journal while its caller retains the deterministic owner lock.
func (store *launchStore) readUnlocked() (launchJournal, bool, error) {
	info, err := os.Lstat(store.paths.LaunchJournal)
	if errors.Is(err, os.ErrNotExist) {
		return launchJournal{}, false, nil
	}
	if err != nil {
		return launchJournal{}, false, err
	}
	if err := validatePrivateFile(info); err != nil {
		return launchJournal{}, false, err
	}
	file, err := os.Open(store.paths.LaunchJournal)
	if err != nil {
		return launchJournal{}, false, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return launchJournal{}, false, errors.New("launch journal changed while opening")
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxLaunchJournalBytes+1))
	if err != nil {
		return launchJournal{}, false, err
	}
	if len(payload) > maxLaunchJournalBytes {
		return launchJournal{}, false, errors.New("launch journal exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var journal launchJournal
	if err := decoder.Decode(&journal); err != nil {
		return launchJournal{}, false, err
	}
	if err := requireEOF(decoder); err != nil {
		return launchJournal{}, false, err
	}
	if err := journal.Validate(); err != nil {
		return launchJournal{}, false, err
	}
	return journal, true, nil
}

// Write performs a checksum CAS from expected to next under the owner lock, so
// stale recovery or concurrent launch writers cannot replace newer evidence.
func (store *launchStore) Write(expected, next launchJournal) error {
	if err := expected.Validate(); err != nil {
		return fmt.Errorf("validate expected launch journal: %w", err)
	}
	if err := next.Validate(); err != nil {
		return err
	}
	if err := validateLaunchJournalTransition(expected, next); err != nil {
		return err
	}
	store.ownerLock.Lock()
	defer store.ownerLock.Unlock()
	current, found, err := store.readUnlocked()
	if err != nil {
		return err
	}
	if found && reflect.DeepEqual(current, next) {
		if err := store.syncDirectory(store.paths.OwnerRoot); err != nil {
			return fmt.Errorf("confirm replayed launch journal directory entry: %w", err)
		}
		return nil
	}
	if !found || current.ChecksumSHA256 != expected.ChecksumSHA256 || current.Phase != expected.Phase {
		return errors.New("launch journal compare-and-swap precondition failed")
	}
	return store.writeUnlocked(next)
}

// validateLaunchJournalTransition rejects scope drift, regression, direct
// readiness, evidence replacement, and reset of a journal without a process.
func validateLaunchJournalTransition(expected, next launchJournal) error {
	if expected.Owner != next.Owner || expected.Mode != next.Mode || expected.SandboxID != next.SandboxID ||
		expected.AttemptID != next.AttemptID || expected.ConfigEvidenceSHA256 != next.ConfigEvidenceSHA256 {
		return errors.New("launch journal transition changed immutable scope")
	}
	switch {
	case expected.Phase == launchPhaseIntent && next.Phase == launchPhaseAuthorized:
		return nil
	case expected.Phase == launchPhaseAuthorized && next.Phase == launchPhaseReady:
		if expected.ProcessEvidence == nil || next.ProcessEvidence == nil ||
			expected.ProcessEvidenceSHA256 != next.ProcessEvidenceSHA256 || *expected.ProcessEvidence != *next.ProcessEvidence {
			return errors.New("ready launch journal changed authorized process evidence")
		}
		return nil
	case (expected.Phase == launchPhaseAuthorized || expected.Phase == launchPhaseReady) && next.Phase == launchPhaseIntent:
		if expected.ProcessEvidence == nil || next.ProcessEvidence != nil || next.ProcessEvidenceSHA256 != "" {
			return errors.New("launch journal reset requires one recorded process and an empty next intent")
		}
		return nil
	default:
		return errors.New("unsupported launch journal transition")
	}
}

// writeUnlocked atomically replaces one validated journal and fsyncs both file content and its parent entry.
func (store *launchStore) writeUnlocked(journal launchJournal) error {
	if err := journal.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	return writeAtomicLauncherFile(store.paths.LaunchJournal, payload, store.syncDirectory)
}

// prepareLauncherConfig computes the non-recursive canonical config evidence and validates the completed config.
func prepareLauncherConfig(config shim.RuntimeConfig) (shim.RuntimeConfig, error) {
	config.WrapperEvidence = ""
	evidence, err := shim.RuntimeConfigEvidence(config)
	if err != nil {
		return shim.RuntimeConfig{}, err
	}
	config.WrapperEvidence = evidence
	if err := config.Validate(); err != nil {
		return shim.RuntimeConfig{}, err
	}
	return config, nil
}

// ensureImmutableRuntimeConfig creates a crash-safe no-replace config or proves an existing config is byte-semantic equal.
func ensureImmutableRuntimeConfig(path string, expected shim.RuntimeConfig, syncDirectory func(string) error) error {
	if syncDirectory == nil {
		return errors.New("runtime config directory sync dependency is nil")
	}
	existing, err := shim.LoadRuntimeConfig(path)
	if err == nil {
		if !reflect.DeepEqual(existing, expected) {
			return errors.New("existing runtime config differs from immutable launch intent")
		}
		return syncDirectory(filepath.Dir(path))
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	payload, err := json.Marshal(expected)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	if len(payload) > maxLauncherConfigBytes {
		return errors.New("launcher runtime config exceeds size limit")
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".shim-config-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
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
	if err := os.Link(temporaryPath, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			loaded, loadErr := shim.LoadRuntimeConfig(path)
			if loadErr != nil {
				return loadErr
			}
			if !reflect.DeepEqual(loaded, expected) {
				return errors.New("concurrent runtime config differs from immutable intent")
			}
			return syncDirectory(directory)
		}
		return err
	}
	if err := os.Remove(temporaryPath); err != nil {
		return err
	}
	return syncDirectory(directory)
}

// writeAtomicLauncherFile writes one private temporary file, fsyncs it, atomically renames it, and fsyncs the directory.
func writeAtomicLauncherFile(path string, payload []byte, syncDirectory func(string) error) error {
	if syncDirectory == nil {
		return errors.New("launch journal directory sync dependency is nil")
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".launch-journal-*")
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
	return syncDirectory(directory)
}
