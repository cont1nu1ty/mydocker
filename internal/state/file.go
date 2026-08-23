package state

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
)

const (
	// fileSchemaVersionV1 identifies the complete on-disk transaction envelope,
	// retained only for explicit migration of pre-retention snapshots.
	fileSchemaVersionV1 uint32 = 1
	// fileSchemaVersionV2 adds bounded operation/event retention metadata while
	// leaving embedded resource and operation record schemas unchanged.
	fileSchemaVersionV2 uint32 = 2
	// currentFileSchemaVersion is written by every new or migrated snapshot.
	currentFileSchemaVersion = fileSchemaVersionV2
	// MaxEnvelopeBytes bounds startup allocation and every encoded state commit;
	// oversized state fails closed before JSON decoding or atomic replacement.
	MaxEnvelopeBytes int64 = 64 << 20
	// filePermission restricts durable lifecycle metadata to the daemon owner.
	filePermission os.FileMode = 0o600
	// fileLockSuffix identifies the stable lock anchored in the state directory's
	// parent so replacing the final directory cannot create a second lock domain.
	fileLockSuffix = ".state.lock"
	// temporaryNameAttempts bounds collision retries without weakening the
	// O_EXCL creation contract used for directory-relative state snapshots.
	temporaryNameAttempts = 128
)

// fileIdentity is the immutable filesystem identity retained across path
// checks so a same-name replacement cannot silently become daemon state.
type fileIdentity struct {
	device uint64
	inode  uint64
}

// fileEnvelope is the checksummed top-level disk format. Payload remains a
// pointer so strict loading can distinguish a missing payload from an empty one.
type fileEnvelope struct {
	SchemaVersion uint32       `json:"schema_version"`
	Payload       *filePayload `json:"payload"`
	PayloadSHA256 string       `json:"payload_sha256"`
}

// filePayload serializes the full Store commit unit as ordered arrays, allowing
// strict recovery to detect duplicate durable identities instead of map overwrite.
type filePayload struct {
	Sandboxes                     []SandboxRecord             `json:"sandboxes"`
	ContainerAttempts             []ContainerAttemptRecord    `json:"container_attempts"`
	Operations                    []OperationRecord           `json:"operations"`
	Events                        []operation.Event           `json:"events"`
	LastEventSequence             EventSequence               `json:"last_event_sequence"`
	FirstEventSequence            EventSequence               `json:"first_event_sequence,omitempty"`
	TerminalOperationSequences    []terminalOperationSequence `json:"terminal_operation_sequences,omitempty"`
	LastTerminalOperationSequence uint64                      `json:"last_terminal_operation_sequence,omitempty"`
	RetiredOperations             []retiredOperation          `json:"retired_operations,omitempty"`
}

// terminalOperationSequence serializes full terminal operation replay order as
// an array so duplicate IDs can be rejected instead of overwritten on load.
type terminalOperationSequence struct {
	OperationID operation.OperationID `json:"operation_id"`
	Sequence    uint64                `json:"sequence"`
}

// historicalGenerationState tracks generation monotonicity within one retained
// target incarnation and permits a reset only after a successful delete boundary.
type historicalGenerationState struct {
	Generation         uint64
	ObservedGeneration uint64
	Deleted            bool
}

// filePrimitives isolates the write, sync, and rename commit boundary so fault
// tests can inject deterministic storage failures without altering production defaults.
type filePrimitives struct {
	mkdirAll      func(string, os.FileMode) error
	lstat         func(string) (os.FileInfo, error)
	ownerUID      func(string, os.FileInfo) (uint32, error)
	effectiveUID  func() uint32
	chmodFile     func(*os.File, os.FileMode) error
	writeFile     func(*os.File, []byte) error
	syncFile      func(*os.File) error
	closeFile     func(*os.File) error
	renameAt      func(*os.File, string, string) error
	openDirectory func(string) (*os.File, error)
	syncDirectory func(*os.File) error
	removeAt      func(*os.File, string) error
}

// FileStore is the production single-daemon Store. Each successful Update is
// one checksummed atomic file replacement completed before in-memory visibility;
// a post-rename durability error poisons the instance until Close and reopen.
type FileStore struct {
	mu              sync.RWMutex
	data            memoryData
	path            string
	directoryPath   string
	stateName       string
	lockPath        string
	stateIdentity   fileIdentity
	files           filePrimitives
	directoryHandle *os.File
	lockHandle      *os.File
	retention       RetentionPolicy
	poisoned        error
	closed          bool
}

var _ Store = (*FileStore)(nil)
var _ io.Closer = (*FileStore)(nil)

// NewFileStore opens or creates one owner-only durable state file and holds its
// exclusive daemon lock until Close. Existing content is never repaired implicitly.
func NewFileStore(path string) (*FileStore, error) {
	return newFileStoreWithRetentionAndPrimitives(path, DefaultRetentionPolicy(), defaultFilePrimitives())
}

// NewFileStoreWithRetention opens production durability machinery with an
// explicit bounded policy, primarily for focused compaction and restart tests.
func NewFileStoreWithRetention(path string, policy RetentionPolicy) (*FileStore, error) {
	return newFileStoreWithRetentionAndPrimitives(path, policy, defaultFilePrimitives())
}

// defaultFilePrimitives binds FileStore durability operations to the operating
// system; test-only constructors may replace individual functions explicitly.
func defaultFilePrimitives() filePrimitives {
	return filePrimitives{
		mkdirAll:      os.MkdirAll,
		lstat:         os.Lstat,
		ownerUID:      ownerUIDFromFileInfo,
		effectiveUID:  currentEffectiveUID,
		chmodFile:     (*os.File).Chmod,
		writeFile:     writeEntireFile,
		syncFile:      (*os.File).Sync,
		closeFile:     (*os.File).Close,
		renameAt:      renameStateFileAt,
		openDirectory: openDirectoryNoFollow,
		syncDirectory: (*os.File).Sync,
		removeAt:      removeStateFileAt,
	}
}

// validateFilePrimitives rejects an incomplete fault-injection seam before any
// filesystem mutation, preventing a nil function from creating ambiguous state.
func validateFilePrimitives(files filePrimitives) error {
	if files.mkdirAll == nil || files.lstat == nil || files.ownerUID == nil ||
		files.effectiveUID == nil || files.chmodFile == nil || files.writeFile == nil ||
		files.syncFile == nil || files.closeFile == nil || files.renameAt == nil ||
		files.openDirectory == nil || files.syncDirectory == nil || files.removeAt == nil {
		return fmt.Errorf("incomplete file primitives: %w", ErrInvalidRecord)
	}
	return nil
}

// newFileStoreWithPrimitives constructs a FileStore through injectable storage
// operations; production callers use NewFileStore and tests use this fault seam.
func newFileStoreWithPrimitives(path string, files filePrimitives) (*FileStore, error) {
	return newFileStoreWithRetentionAndPrimitives(path, DefaultRetentionPolicy(), files)
}

// newFileStoreWithRetentionAndPrimitives combines deterministic retention and
// fault-injectable storage while preserving the production constructor path.
func newFileStoreWithRetentionAndPrimitives(path string, policy RetentionPolicy, files filePrimitives) (*FileStore, error) {
	if err := validateFilePrimitives(files); err != nil {
		return nil, err
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("state file path is empty: %w", ErrInvalidRecord)
	}
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("resolve state file path: %w", err)
	}
	directoryPath := filepath.Dir(absolute)
	if err := ensureStateDirectory(directoryPath, files); err != nil {
		return nil, err
	}
	lockPath := stateLockPath(absolute)
	lockHandle, err := acquireStateFileLock(lockPath, absolute, files)
	if err != nil {
		return nil, err
	}
	directoryHandle, err := openLockedStateDirectory(directoryPath, absolute, files)
	if err != nil {
		return nil, errors.Join(err, releaseStateFileLock(lockHandle))
	}
	stateName := filepath.Base(absolute)
	data, stateIdentity, exists, loadedSchema, err := loadFileData(directoryHandle, stateName, absolute, files)
	if err != nil {
		return nil, errors.Join(err, releaseLockedStateDirectory(directoryHandle), releaseStateFileLock(lockHandle))
	}
	if !exists {
		var renamed bool
		stateIdentity, renamed, err = persistFileData(directoryHandle, stateName, absolute, data, files)
		persistErr := err
		if persistErr != nil {
			if renamed {
				persistErr = errors.Join(ErrDurabilityUncertain, persistErr)
			}
			return nil, errors.Join(
				fmt.Errorf("initialize state file: %w", persistErr),
				releaseLockedStateDirectory(directoryHandle),
				releaseStateFileLock(lockHandle),
			)
		}
	} else {
		changed, retentionErr := data.applyRetention(policy)
		if retentionErr != nil {
			return nil, errors.Join(retentionErr, releaseLockedStateDirectory(directoryHandle), releaseStateFileLock(lockHandle))
		}
		if validationErr := data.validate(); validationErr != nil {
			return nil, errors.Join(validationErr, releaseLockedStateDirectory(directoryHandle), releaseStateFileLock(lockHandle))
		}
		persisted := false
		if changed || loadedSchema != currentFileSchemaVersion {
			var renamed bool
			stateIdentity, renamed, err = persistFileData(directoryHandle, stateName, absolute, data, files)
			if err != nil {
				if renamed {
					err = errors.Join(ErrDurabilityUncertain, err)
				}
				return nil, errors.Join(fmt.Errorf("migrate retained state: %w", err), releaseLockedStateDirectory(directoryHandle), releaseStateFileLock(lockHandle))
			}
			persisted = true
		}
		if !persisted {
			if syncErr := syncOpenStateDirectory(directoryHandle, files); syncErr != nil {
				uncertain := errors.Join(ErrDurabilityUncertain, fmt.Errorf("confirm existing state directory durability: %w", syncErr))
				return nil, errors.Join(uncertain, releaseLockedStateDirectory(directoryHandle), releaseStateFileLock(lockHandle))
			}
		}
	}
	store := &FileStore{
		data:            data,
		path:            absolute,
		directoryPath:   directoryPath,
		stateName:       stateName,
		lockPath:        lockPath,
		stateIdentity:   stateIdentity,
		files:           files,
		directoryHandle: directoryHandle,
		lockHandle:      lockHandle,
		retention:       policy,
	}
	if err := store.validateFilesystemIdentity(); err != nil {
		return nil, errors.Join(err, releaseLockedStateDirectory(directoryHandle), releaseStateFileLock(lockHandle))
	}
	return store, nil
}

// stateLockPath derives a stable lock in the final directory's parent; that
// anchor survives replacement of the directory containing the state file.
func stateLockPath(statePath string) string {
	directory := filepath.Dir(statePath)
	directoryName := filepath.Base(directory)
	if directory == string(filepath.Separator) {
		directoryName = "root"
	}
	if len(directoryName) > 48 {
		directoryName = directoryName[:48]
	}
	digest := sha256.Sum256([]byte(statePath))
	name := fmt.Sprintf(".%s-%s%s", directoryName, hex.EncodeToString(digest[:]), fileLockSuffix)
	return filepath.Join(filepath.Dir(directory), name)
}

// acquireStateFileLock opens, verifies, locks, and re-verifies the stable
// anchor so a symlink, foreign owner, or open-versus-path inode swap fails closed.
func acquireStateFileLock(lockPath, statePath string, files filePrimitives) (*os.File, error) {
	fd, err := unix.Open(lockPath, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, uint32(filePermission))
	if err != nil {
		if errors.Is(err, unix.ELOOP) {
			return nil, fmt.Errorf("state lock %q is a symlink: %w", lockPath, ErrInvalidRecord)
		}
		return nil, fmt.Errorf("open state lock %q: %w", lockPath, err)
	}
	handle := os.NewFile(uintptr(fd), lockPath)
	info, err := handle.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("inspect state lock %q: %w", lockPath, err), handle.Close())
	}
	if err := validateOwnedRegularFile(lockPath, info, files); err != nil {
		return nil, errors.Join(
			err,
			handle.Close(),
		)
	}
	if err := validatePathMatchesHandle(lockPath, handle, files); err != nil {
		return nil, errors.Join(err, handle.Close())
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		closeErr := handle.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, errors.Join(fmt.Errorf("lock state path %q: %w", statePath, ErrFileStoreLocked), closeErr)
		}
		return nil, errors.Join(fmt.Errorf("lock state path %q: %w", statePath, err), closeErr)
	}
	if err := validatePathMatchesHandle(lockPath, handle, files); err != nil {
		return nil, errors.Join(err, releaseStateFileLock(handle))
	}
	return handle, nil
}

// releaseStateFileLock unlocks and closes the stable lock inode without
// unlinking it, avoiding a split-lock race with a concurrently opening daemon.
func releaseStateFileLock(handle *os.File) error {
	if handle == nil {
		return nil
	}
	unlockErr := unix.Flock(int(handle.Fd()), unix.LOCK_UN)
	closeErr := handle.Close()
	if unlockErr != nil {
		unlockErr = fmt.Errorf("unlock state file: %w", unlockErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close state lock: %w", closeErr)
	}
	return errors.Join(unlockErr, closeErr)
}

// openLockedStateDirectory holds the final directory inode and an advisory
// directory lock, then proves the canonical path still names that exact inode.
func openLockedStateDirectory(directory, statePath string, files filePrimitives) (*os.File, error) {
	handle, err := files.openDirectory(directory)
	if err != nil {
		return nil, fmt.Errorf("open state directory %q: %w", directory, err)
	}
	info, err := handle.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("inspect state directory %q: %w", directory, err), handle.Close())
	}
	if err := validateOwnedDirectory(directory, info, true, files); err != nil {
		return nil, errors.Join(err, handle.Close())
	}
	if err := unix.Flock(int(handle.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		closeErr := handle.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, errors.Join(fmt.Errorf("lock state directory for %q: %w", statePath, ErrFileStoreLocked), closeErr)
		}
		return nil, errors.Join(fmt.Errorf("lock state directory for %q: %w", statePath, err), closeErr)
	}
	if err := validatePathMatchesHandle(directory, handle, files); err != nil {
		return nil, errors.Join(err, releaseLockedStateDirectory(handle))
	}
	return handle, nil
}

// releaseLockedStateDirectory releases the inode-level lock and directory
// descriptor retained by FileStore; the directory itself is never removed.
func releaseLockedStateDirectory(handle *os.File) error {
	if handle == nil {
		return nil
	}
	unlockErr := unix.Flock(int(handle.Fd()), unix.LOCK_UN)
	closeErr := handle.Close()
	if unlockErr != nil {
		unlockErr = fmt.Errorf("unlock state directory: %w", unlockErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close state directory: %w", closeErr)
	}
	return errors.Join(unlockErr, closeErr)
}

// ensureStateDirectory creates the selected parent, durably records each newly
// created path component, and rejects an unsafe final storage directory.
func ensureStateDirectory(directory string, files filePrimitives) error {
	missing, err := missingDirectoryChain(directory, files)
	if err != nil {
		return err
	}
	existing := directory
	if len(missing) > 0 {
		existing = filepath.Dir(missing[len(missing)-1])
	}
	if err := validateStateDirectoryTree(existing, files); err != nil {
		return err
	}
	if err := files.mkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create state directory %q: %w", directory, err)
	}
	if err := validateStateDirectoryTree(directory, files); err != nil {
		return err
	}
	for _, created := range missing {
		if err := syncStateDirectory(filepath.Dir(created), files); err != nil {
			return fmt.Errorf("persist state directory %q: %w", created, err)
		}
	}
	return nil
}

// validateStateDirectoryTree rejects symlinks, foreign-owned mutable ancestors,
// and any group/world-writable component before a pathname is trusted.
func validateStateDirectoryTree(directory string, files filePrimitives) error {
	paths, err := absoluteDirectoryChain(directory)
	if err != nil {
		return err
	}
	for index, path := range paths {
		info, err := files.lstat(path)
		if err != nil {
			return fmt.Errorf("inspect state directory component %q: %w", path, err)
		}
		if err := validateOwnedDirectory(path, info, index == len(paths)-1, files); err != nil {
			return err
		}
	}
	return nil
}

// absoluteDirectoryChain expands one Linux absolute directory from the root to
// its final component so every traversal boundary receives the same validation.
func absoluteDirectoryChain(directory string) ([]string, error) {
	clean := filepath.Clean(directory)
	if !filepath.IsAbs(clean) {
		return nil, fmt.Errorf("state directory %q is not absolute: %w", directory, ErrInvalidRecord)
	}
	paths := []string{string(filepath.Separator)}
	if clean == string(filepath.Separator) {
		return paths, nil
	}
	current := string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(clean, string(filepath.Separator)), string(filepath.Separator)) {
		current = filepath.Join(current, component)
		paths = append(paths, current)
	}
	return paths, nil
}

// missingDirectoryChain lists absent path components from deepest to shallowest
// so syncing each parent after MkdirAll durably links the complete new hierarchy.
func missingDirectoryChain(directory string, files filePrimitives) ([]string, error) {
	var missing []string
	current := filepath.Clean(directory)
	for {
		_, err := files.lstat(current)
		if err == nil {
			return missing, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("inspect state directory ancestor %q: %w", current, err)
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == current {
			return nil, fmt.Errorf("no existing ancestor for state directory %q: %w", directory, ErrInvalidRecord)
		}
		current = parent
	}
}

// currentEffectiveUID captures the kernel identity whose exclusive ownership
// is required for the final state directory, state file, and lock anchor.
func currentEffectiveUID() uint32 {
	return uint32(os.Geteuid())
}

// ownerUIDFromFileInfo extracts Linux st_uid without trusting path text or
// permission bits; tests replace this seam to model a foreign owner safely.
func ownerUIDFromFileInfo(path string, info os.FileInfo) (uint32, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return 0, fmt.Errorf("filesystem metadata for %q has no Linux stat owner: %w", path, ErrInvalidRecord)
	}
	return stat.Uid, nil
}

// identityFromFileInfo extracts the device/inode pair used to reject same-name
// replacements after a descriptor has already been opened and validated.
func identityFromFileInfo(path string, info os.FileInfo) (fileIdentity, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return fileIdentity{}, fmt.Errorf("filesystem metadata for %q has no Linux identity: %w", path, ErrInvalidRecord)
	}
	return fileIdentity{device: uint64(stat.Dev), inode: stat.Ino}, nil
}

// validateOwnedDirectory enforces traversal safety for every state ancestor;
// the final directory must be owned by euid, while a non-root test process may
// traverse root-owned non-writable system ancestors such as / and /home.
func validateOwnedDirectory(path string, info os.FileInfo, final bool, files filePrimitives) error {
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("state directory component %q is not a real directory: %w", path, ErrInvalidRecord)
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("state directory component %q is group/world writable (%#o): %w", path, info.Mode().Perm(), ErrInvalidRecord)
	}
	owner, err := files.ownerUID(path, info)
	if err != nil {
		return err
	}
	effective := files.effectiveUID()
	trustedRootAncestor := !final && effective != 0 && owner == 0
	if owner != effective && !trustedRootAncestor {
		return fmt.Errorf("state directory component %q owner uid is %d, want euid %d: %w", path, owner, effective, ErrInvalidRecord)
	}
	return nil
}

// validateOwnedRegularFile requires an exact euid-owned 0600 regular inode for
// durable state and locks, including when a rootful daemon can read foreign files.
func validateOwnedRegularFile(path string, info os.FileInfo, files filePrimitives) error {
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("state file %q is not a regular file: %w", path, ErrInvalidRecord)
	}
	if info.Mode().Perm() != filePermission {
		return fmt.Errorf("state file %q permissions are %#o, want %#o: %w", path, info.Mode().Perm(), filePermission, ErrInvalidRecord)
	}
	owner, err := files.ownerUID(path, info)
	if err != nil {
		return err
	}
	if effective := files.effectiveUID(); owner != effective {
		return fmt.Errorf("state file %q owner uid is %d, want euid %d: %w", path, owner, effective, ErrInvalidRecord)
	}
	return nil
}

// validatePathMatchesHandle proves a canonical path still refers to the open
// inode and that both views retain the daemon's required ownership.
func validatePathMatchesHandle(path string, handle *os.File, files filePrimitives) error {
	handleInfo, err := handle.Stat()
	if err != nil {
		return fmt.Errorf("inspect open filesystem object %q: %w", path, err)
	}
	pathInfo, err := files.lstat(path)
	if err != nil {
		return fmt.Errorf("inspect canonical filesystem object %q: %w: %w", path, ErrInvalidRecord, err)
	}
	handleOwner, err := files.ownerUID(path, handleInfo)
	if err != nil {
		return err
	}
	pathOwner, err := files.ownerUID(path, pathInfo)
	if err != nil {
		return err
	}
	if effective := files.effectiveUID(); handleOwner != effective || pathOwner != effective {
		return fmt.Errorf("filesystem object %q owner changed from required euid %d: %w", path, effective, ErrInvalidRecord)
	}
	if !os.SameFile(handleInfo, pathInfo) {
		return fmt.Errorf("filesystem object %q no longer names the locked inode: %w", path, ErrInvalidRecord)
	}
	return nil
}

// validateNamedStateIdentity uses fstatat without following symlinks to ensure
// the held state directory still names the exact owner-only state inode.
func validateNamedStateIdentity(directory *os.File, name, path string, expected fileIdentity, files filePrimitives) error {
	var stat unix.Stat_t
	if err := unix.Fstatat(int(directory.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("inspect state file %q relative to locked directory: %w: %w", path, ErrInvalidRecord, err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || os.FileMode(stat.Mode).Perm() != filePermission {
		return fmt.Errorf("state file %q is not an owner-only regular inode: %w", path, ErrInvalidRecord)
	}
	if effective := files.effectiveUID(); stat.Uid != effective {
		return fmt.Errorf("state file %q owner uid is %d, want euid %d: %w", path, stat.Uid, effective, ErrInvalidRecord)
	}
	actual := fileIdentity{device: uint64(stat.Dev), inode: stat.Ino}
	if actual != expected {
		return fmt.Errorf("state file %q inode changed outside FileStore: %w", path, ErrInvalidRecord)
	}
	return nil
}

// openDirectoryNoFollow obtains a directory descriptor without allowing the
// final path component to redirect state operations through a symlink.
func openDirectoryNoFollow(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

// renameStateFileAt atomically replaces the state name within the already
// validated and locked directory, eliminating final-directory path traversal.
func renameStateFileAt(directory *os.File, oldName, newName string) error {
	return unix.Renameat(int(directory.Fd()), oldName, int(directory.Fd()), newName)
}

// removeStateFileAt removes only a temporary basename from the held directory;
// ENOENT remains harmless to deferred best-effort cleanup.
func removeStateFileAt(directory *os.File, name string) error {
	return unix.Unlinkat(int(directory.Fd()), name, 0)
}

// loadFileData reads one existing owner-only regular file and validates every
// envelope, record, graph, and ordering invariant before returning live state.
func loadFileData(directory *os.File, name, path string, files filePrimitives) (memoryData, fileIdentity, bool, uint32, error) {
	fd, err := unix.Openat(int(directory.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if errors.Is(err, unix.ENOENT) {
		return newMemoryData(), fileIdentity{}, false, currentFileSchemaVersion, nil
	}
	if err != nil {
		if errors.Is(err, unix.ELOOP) {
			return memoryData{}, fileIdentity{}, false, 0, fmt.Errorf("state file %q is a symlink: %w", path, ErrInvalidRecord)
		}
		return memoryData{}, fileIdentity{}, false, 0, fmt.Errorf("open state file %q: %w", path, err)
	}
	handle := os.NewFile(uintptr(fd), path)
	info, err := handle.Stat()
	if err != nil {
		return memoryData{}, fileIdentity{}, false, 0, errors.Join(fmt.Errorf("inspect state file %q: %w", path, err), handle.Close())
	}
	if err := validateOwnedRegularFile(path, info, files); err != nil {
		return memoryData{}, fileIdentity{}, false, 0, errors.Join(err, handle.Close())
	}
	if info.Size() < 0 || info.Size() > MaxEnvelopeBytes {
		return memoryData{}, fileIdentity{}, false, 0, errors.Join(
			fmt.Errorf("state file %q size %d exceeds maximum %d: %w", path, info.Size(), MaxEnvelopeBytes, ErrInvalidRecord),
			handle.Close(),
		)
	}
	identity, err := identityFromFileInfo(path, info)
	if err != nil {
		return memoryData{}, fileIdentity{}, false, 0, errors.Join(err, handle.Close())
	}
	encoded, err := io.ReadAll(io.LimitReader(handle, MaxEnvelopeBytes+1))
	if err != nil {
		return memoryData{}, fileIdentity{}, false, 0, errors.Join(fmt.Errorf("read state file %q: %w", path, err), handle.Close())
	}
	if int64(len(encoded)) > MaxEnvelopeBytes {
		return memoryData{}, fileIdentity{}, false, 0, errors.Join(
			fmt.Errorf("state file %q grew beyond maximum %d while reading: %w", path, MaxEnvelopeBytes, ErrInvalidRecord),
			handle.Close(),
		)
	}
	if err := handle.Close(); err != nil {
		return memoryData{}, fileIdentity{}, false, 0, fmt.Errorf("close state file %q: %w", path, err)
	}
	if err := validateNamedStateIdentity(directory, name, path, identity, files); err != nil {
		return memoryData{}, fileIdentity{}, false, 0, err
	}
	envelope, err := decodeFileEnvelope(encoded)
	if err != nil {
		return memoryData{}, fileIdentity{}, false, 0, fmt.Errorf("load state file %q: %w", path, err)
	}
	data, err := memoryDataFromPayload(*envelope.Payload, envelope.SchemaVersion)
	if err != nil {
		return memoryData{}, fileIdentity{}, false, 0, fmt.Errorf("load state file %q: %w", path, err)
	}
	return data, identity, true, envelope.SchemaVersion, nil
}

// decodeFileEnvelope rejects unknown fields, unknown schema versions, missing
// payloads, checksum mismatches, malformed JSON, and any trailing JSON data.
func decodeFileEnvelope(encoded []byte) (fileEnvelope, error) {
	if err := rejectDuplicateJSONKeys(encoded); err != nil {
		return fileEnvelope{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var envelope fileEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return fileEnvelope{}, fmt.Errorf("decode persistence envelope: %w: %w", ErrInvalidRecord, err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fileEnvelope{}, fmt.Errorf("persistence envelope has trailing JSON value: %w", ErrInvalidRecord)
		}
		return fileEnvelope{}, fmt.Errorf("persistence envelope has trailing data: %w: %w", ErrInvalidRecord, err)
	}
	if envelope.SchemaVersion != fileSchemaVersionV1 && envelope.SchemaVersion != fileSchemaVersionV2 {
		return fileEnvelope{}, fmt.Errorf("file schema %d: %w", envelope.SchemaVersion, ErrUnsupportedSchema)
	}
	if envelope.Payload == nil {
		return fileEnvelope{}, fmt.Errorf("persistence envelope payload is missing: %w", ErrInvalidRecord)
	}
	digest, err := filePayloadDigest(*envelope.Payload)
	if err != nil {
		return fileEnvelope{}, err
	}
	if envelope.PayloadSHA256 != digest {
		return fileEnvelope{}, fmt.Errorf("persistence payload checksum mismatch: %w", ErrInvalidRecord)
	}
	return envelope, nil
}

// rejectDuplicateJSONKeys walks the first JSON value and rejects ambiguous
// object members at any depth before typed decoding can silently keep the last.
func rejectDuplicateJSONKeys(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	var walkValue func() error
	walkValue = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, composite := token.(json.Delim)
		if !composite {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return fmt.Errorf("JSON object key is not a string")
				}
				if _, exists := seen[key]; exists {
					return fmt.Errorf("duplicate JSON object key %q", key)
				}
				seen[key] = struct{}{}
				if err := walkValue(); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := walkValue(); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
		}
		if _, err := decoder.Token(); err != nil {
			return err
		}
		return nil
	}
	if err := walkValue(); err != nil {
		return fmt.Errorf("ambiguous persistence JSON: %w: %w", ErrInvalidRecord, err)
	}
	return nil
}

// filePayloadDigest computes the canonical lowercase SHA-256 used to detect
// syntactically valid on-disk corruption before any records become visible.
func filePayloadDigest(payload filePayload) (string, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode persistence checksum payload: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// memoryDataFromPayload rebuilds identity maps with duplicate detection, then
// enforces schema-specific retention metadata, event continuity, and graph invariants.
func memoryDataFromPayload(payload filePayload, fileSchema uint32) (memoryData, error) {
	if payload.Sandboxes == nil || payload.ContainerAttempts == nil || payload.Operations == nil || payload.Events == nil {
		return memoryData{}, fmt.Errorf("persistence payload collections must be present: %w", ErrInvalidRecord)
	}
	if fileSchema != fileSchemaVersionV1 && fileSchema != fileSchemaVersionV2 {
		return memoryData{}, fmt.Errorf("file schema %d: %w", fileSchema, ErrUnsupportedSchema)
	}
	data := newMemoryData()
	for _, record := range payload.Sandboxes {
		if record.Revision == 0 {
			return memoryData{}, fmt.Errorf("sandbox %q has zero revision: %w", record.Sandbox.ID, ErrInvalidRecord)
		}
		if err := validateSandboxRecord(record); err != nil {
			return memoryData{}, err
		}
		id := record.Sandbox.ID
		if _, exists := data.sandboxes[id]; exists {
			return memoryData{}, fmt.Errorf("duplicate sandbox %q: %w", id, ErrInvalidRecord)
		}
		data.sandboxes[id] = record.Clone()
	}
	attempts := make(map[domain.AttemptID]domain.ContainerID, len(payload.ContainerAttempts))
	for _, record := range payload.ContainerAttempts {
		if record.Revision == 0 {
			return memoryData{}, fmt.Errorf("container %q has zero revision: %w", record.ContainerAttempt.Container.ID, ErrInvalidRecord)
		}
		if err := validateContainerAttemptRecord(record); err != nil {
			return memoryData{}, err
		}
		id := record.ContainerAttempt.Container.ID
		if _, exists := data.containerAttempts[id]; exists {
			return memoryData{}, fmt.Errorf("duplicate container %q: %w", id, ErrInvalidRecord)
		}
		attemptID := record.ContainerAttempt.Attempt.ID
		if owner, exists := attempts[attemptID]; exists {
			return memoryData{}, fmt.Errorf("attempt %q belongs to containers %q and %q: %w", attemptID, owner, id, ErrInvalidRecord)
		}
		attempts[attemptID] = id
		data.containerAttempts[id] = record.Clone()
	}
	for _, record := range payload.Operations {
		if record.Revision == 0 {
			return memoryData{}, fmt.Errorf("operation %q has zero revision: %w", record.Operation.ID, ErrInvalidRecord)
		}
		if err := validateOperationRecord(record); err != nil {
			return memoryData{}, err
		}
		id := record.Operation.ID
		if _, exists := data.operations[id]; exists {
			return memoryData{}, fmt.Errorf("duplicate operation %q: %w", id, ErrInvalidRecord)
		}
		data.operations[id] = record.Clone()
	}
	if err := validateLoadedActiveOperations(data.operations); err != nil {
		return memoryData{}, err
	}
	if fileSchema == fileSchemaVersionV1 {
		if payload.FirstEventSequence != 0 || len(payload.TerminalOperationSequences) != 0 ||
			payload.LastTerminalOperationSequence != 0 || len(payload.RetiredOperations) != 0 {
			return memoryData{}, fmt.Errorf("file schema v1 contains v2 retention metadata: %w", ErrInvalidRecord)
		}
	} else {
		if payload.FirstEventSequence == 0 {
			return memoryData{}, fmt.Errorf("file schema v2 is missing first event sequence: %w", ErrInvalidRecord)
		}
		data.firstEventSequence = payload.FirstEventSequence
		data.lastTerminalOperationSequence = payload.LastTerminalOperationSequence
		for _, entry := range payload.TerminalOperationSequences {
			if entry.OperationID == "" || entry.Sequence == 0 {
				return memoryData{}, fmt.Errorf("terminal operation order contains an empty identity or sequence: %w", ErrInvalidRecord)
			}
			if _, exists := data.terminalOperationSequences[entry.OperationID]; exists {
				return memoryData{}, fmt.Errorf("duplicate terminal operation order for %q: %w", entry.OperationID, ErrInvalidRecord)
			}
			record, exists := data.operations[entry.OperationID]
			if !exists || !record.Operation.State.Terminal() {
				return memoryData{}, fmt.Errorf("terminal operation order references non-terminal operation %q: %w", entry.OperationID, ErrInvalidRecord)
			}
			data.terminalOperationSequences[entry.OperationID] = entry.Sequence
		}
		for _, retired := range payload.RetiredOperations {
			if err := retired.validate(); err != nil {
				return memoryData{}, err
			}
			if _, exists := data.retiredOperations[retired.OperationIDSHA256]; exists {
				return memoryData{}, fmt.Errorf("duplicate retired operation digest %q: %w", retired.OperationIDSHA256, ErrInvalidRecord)
			}
			data.retiredOperations[retired.OperationIDSHA256] = retired
		}
		for id := range data.operations {
			if _, exists := data.retiredOperations[operationIDDigest(id)]; exists {
				return memoryData{}, fmt.Errorf("operation %q collides with a retired identity: %w", id, ErrInvalidRecord)
			}
		}
	}
	historicalGenerations := make(map[operation.Target]historicalGenerationState)
	for index, event := range payload.Events {
		if err := validateEvent(event); err != nil {
			return memoryData{}, err
		}
		firstExpected := EventSequence(1)
		if fileSchema == fileSchemaVersionV2 {
			firstExpected = payload.FirstEventSequence
		}
		if index == 0 && event.Sequence != firstExpected {
			return memoryData{}, fmt.Errorf("first event sequence is %d, want %d: %w", event.Sequence, firstExpected, ErrInvalidRecord)
		}
		if index > 0 {
			if err := event.ValidateAfter(payload.Events[index-1]); err != nil {
				return memoryData{}, fmt.Errorf("event ordering: %w: %v", ErrInvalidRecord, err)
			}
		}
		if record, exists := data.operations[event.OperationID]; exists {
			if event.Type != record.Operation.Type || !event.Target.Equal(record.Operation.Target) {
				return memoryData{}, fmt.Errorf("event sequence %d does not match operation %q binding: %w", event.Sequence, event.OperationID, ErrInvalidRecord)
			}
		} else {
			retired, exists := data.retiredOperationFor(event.OperationID)
			if !exists || event.Type != retired.Type || !event.Target.Equal(retired.Target) {
				return memoryData{}, fmt.Errorf("event sequence %d references unknown or mismatched operation %q: %w", event.Sequence, event.OperationID, ErrInvalidRecord)
			}
		}
		if err := validateHistoricalEventGeneration(event, historicalGenerations); err != nil {
			return memoryData{}, fmt.Errorf("event sequence %d generation: %w", event.Sequence, err)
		}
		data.events = append(data.events, event.Clone())
	}
	data.lastEventSequence = payload.LastEventSequence
	if len(data.events) == 0 {
		if fileSchema == fileSchemaVersionV1 && payload.LastEventSequence != 0 {
			return memoryData{}, fmt.Errorf("empty v1 event log has last sequence %d: %w", payload.LastEventSequence, ErrInvalidRecord)
		}
		if fileSchema == fileSchemaVersionV2 && data.firstEventSequence != data.nextAvailableEventSequence() {
			return memoryData{}, fmt.Errorf("empty event suffix starts at %d after high watermark %d: %w",
				data.firstEventSequence, data.lastEventSequence, ErrInvalidRecord)
		}
	} else if payload.LastEventSequence != data.events[len(data.events)-1].Sequence {
		return memoryData{}, fmt.Errorf("last event sequence %d does not match final event %d: %w",
			payload.LastEventSequence, data.events[len(data.events)-1].Sequence, ErrInvalidRecord)
	}
	if fileSchema == fileSchemaVersionV1 {
		data.firstEventSequence = data.nextAvailableEventSequence()
		if _, err := data.assignTerminalSequences(); err != nil {
			return memoryData{}, err
		}
	}
	if err := data.validate(); err != nil {
		return memoryData{}, err
	}
	return data, nil
}

// validateHistoricalEventGeneration rejects generation regression inside one
// retained resource incarnation while allowing a create to restart at 1/0 only
// after that target's successful terminal delete. The current resource's exact
// last projection is checked separately by memoryData.validate.
func validateHistoricalEventGeneration(event operation.Event, states map[operation.Target]historicalGenerationState) error {
	previous, exists := states[event.Target]
	if !exists {
		states[event.Target] = historicalGenerationState{
			Generation: event.Generation, ObservedGeneration: event.ObservedGeneration,
			Deleted: isSuccessfulDeleteEvent(event),
		}
		return nil
	}
	if previous.Deleted {
		if event.Type == operation.TypeDelete {
			states[event.Target] = historicalGenerationState{
				Generation: event.Generation, ObservedGeneration: event.ObservedGeneration,
				Deleted: isSuccessfulDeleteEvent(event),
			}
			return nil
		}
		if event.Type != operation.TypeCreate {
			return fmt.Errorf("event after successful delete is %q instead of create: %w", event.Type, ErrInvalidRecord)
		}
		states[event.Target] = historicalGenerationState{
			Generation: event.Generation, ObservedGeneration: event.ObservedGeneration,
			Deleted: isSuccessfulDeleteEvent(event),
		}
		return nil
	}
	if event.Generation < previous.Generation || event.ObservedGeneration < previous.ObservedGeneration {
		return fmt.Errorf("event generation %d/%d regresses within an incarnation from %d/%d: %w",
			event.Generation, event.ObservedGeneration, previous.Generation, previous.ObservedGeneration, ErrInvalidRecord)
	}
	states[event.Target] = historicalGenerationState{
		Generation: event.Generation, ObservedGeneration: event.ObservedGeneration,
		Deleted: isSuccessfulDeleteEvent(event),
	}
	return nil
}

// isSuccessfulDeleteEvent identifies the only retained lifecycle boundary that
// permits a later create operation to reuse the same public resource ID with a
// fresh generation-one incarnation.
func isSuccessfulDeleteEvent(event operation.Event) bool {
	return event.Type == operation.TypeDelete && event.Stage == operation.StageComplete &&
		(event.Result == operation.ResultSucceeded || event.Result == operation.ResultNoop)
}

// validateLoadedActiveOperations detects duplicate active target ownership that
// normal PutOperation calls reject but a corrupted disk payload could contain.
func validateLoadedActiveOperations(records map[operation.OperationID]OperationRecord) error {
	active := make(map[operation.Target]operation.OperationID)
	for id, record := range records {
		if !record.Operation.State.Active() {
			continue
		}
		target := record.Operation.Target
		if owner, exists := active[target]; exists {
			return fmt.Errorf("target %s/%s has active operations %q and %q: %w",
				target.Kind, target.ID, owner, id, ErrInvariantViolation)
		}
		active[target] = id
	}
	return nil
}

// filePayloadFromMemory produces deterministic ordered, deeply copied arrays so
// identical committed state has one stable checksum and byte representation.
func filePayloadFromMemory(data memoryData) filePayload {
	payload := filePayload{
		Sandboxes:                     make([]SandboxRecord, 0, len(data.sandboxes)),
		ContainerAttempts:             make([]ContainerAttemptRecord, 0, len(data.containerAttempts)),
		Operations:                    make([]OperationRecord, 0, len(data.operations)),
		Events:                        make([]operation.Event, 0, len(data.events)),
		LastEventSequence:             data.lastEventSequence,
		FirstEventSequence:            data.firstEventSequence,
		TerminalOperationSequences:    make([]terminalOperationSequence, 0, len(data.terminalOperationSequences)),
		LastTerminalOperationSequence: data.lastTerminalOperationSequence,
		RetiredOperations:             make([]retiredOperation, 0, len(data.retiredOperations)),
	}
	sandboxIDs := make([]domain.SandboxID, 0, len(data.sandboxes))
	for id := range data.sandboxes {
		sandboxIDs = append(sandboxIDs, id)
	}
	sort.Slice(sandboxIDs, func(i, j int) bool { return sandboxIDs[i] < sandboxIDs[j] })
	for _, id := range sandboxIDs {
		payload.Sandboxes = append(payload.Sandboxes, data.sandboxes[id].Clone())
	}
	containerIDs := make([]domain.ContainerID, 0, len(data.containerAttempts))
	for id := range data.containerAttempts {
		containerIDs = append(containerIDs, id)
	}
	sort.Slice(containerIDs, func(i, j int) bool { return containerIDs[i] < containerIDs[j] })
	for _, id := range containerIDs {
		payload.ContainerAttempts = append(payload.ContainerAttempts, data.containerAttempts[id].Clone())
	}
	operationIDs := make([]operation.OperationID, 0, len(data.operations))
	for id := range data.operations {
		operationIDs = append(operationIDs, id)
	}
	sort.Slice(operationIDs, func(i, j int) bool { return operationIDs[i] < operationIDs[j] })
	for _, id := range operationIDs {
		payload.Operations = append(payload.Operations, data.operations[id].Clone())
	}
	terminalIDs := make([]operation.OperationID, 0, len(data.terminalOperationSequences))
	for id := range data.terminalOperationSequences {
		terminalIDs = append(terminalIDs, id)
	}
	sort.Slice(terminalIDs, func(i, j int) bool { return terminalIDs[i] < terminalIDs[j] })
	for _, id := range terminalIDs {
		payload.TerminalOperationSequences = append(payload.TerminalOperationSequences, terminalOperationSequence{
			OperationID: id,
			Sequence:    data.terminalOperationSequences[id],
		})
	}
	retiredDigests := make([]string, 0, len(data.retiredOperations))
	for digest := range data.retiredOperations {
		retiredDigests = append(retiredDigests, digest)
	}
	sort.Strings(retiredDigests)
	for _, digest := range retiredDigests {
		payload.RetiredOperations = append(payload.RetiredOperations, data.retiredOperations[digest])
	}
	for _, event := range data.events {
		payload.Events = append(payload.Events, event.Clone())
	}
	return payload
}

// encodeFileData wraps one full transaction snapshot with its independent file
// schema and compact JSON so canonical terminal response bytes remain unchanged.
func encodeFileData(data memoryData) ([]byte, error) {
	payload := filePayloadFromMemory(data)
	digest, err := filePayloadDigest(payload)
	if err != nil {
		return nil, err
	}
	envelope := fileEnvelope{SchemaVersion: currentFileSchemaVersion, Payload: &payload, PayloadSHA256: digest}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode persistence envelope: %w", err)
	}
	encoded = append(encoded, '\n')
	if int64(len(encoded)) > MaxEnvelopeBytes {
		return nil, fmt.Errorf("encoded state envelope size %d exceeds maximum %d: %w", len(encoded), MaxEnvelopeBytes, ErrRetentionCapacity)
	}
	return encoded, nil
}

// persistFileData encodes and replaces one complete snapshot, returning its new
// inode identity and whether rename crossed the fail-stop durability boundary.
func persistFileData(directory *os.File, name, path string, candidate memoryData, files filePrimitives) (fileIdentity, bool, error) {
	encoded, err := encodeFileData(candidate)
	if err != nil {
		return fileIdentity{}, false, err
	}
	return replaceStateFile(directory, name, path, encoded, files)
}

// replaceStateFile writes and syncs an owner-only temporary file in the target
// directory, atomically renames it, and reports rename completion separately
// when the following directory durability confirmation fails.
func replaceStateFile(directory *os.File, name, path string, encoded []byte, files filePrimitives) (fileIdentity, bool, error) {
	prefix := "." + name + ".tmp-"
	temporary, temporaryName, err := createTemporaryStateFile(directory, prefix, path)
	if err != nil {
		return fileIdentity{}, false, fmt.Errorf("create state temporary file: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = files.closeFile(temporary)
		}
		_ = files.removeAt(directory, temporaryName)
	}()
	if err := files.chmodFile(temporary, filePermission); err != nil {
		return fileIdentity{}, false, fmt.Errorf("set state temporary permissions: %w", err)
	}
	if err := files.writeFile(temporary, encoded); err != nil {
		return fileIdentity{}, false, fmt.Errorf("write state temporary file: %w", err)
	}
	if err := files.syncFile(temporary); err != nil {
		return fileIdentity{}, false, fmt.Errorf("sync state temporary file: %w", err)
	}
	temporaryInfo, err := temporary.Stat()
	if err != nil {
		return fileIdentity{}, false, fmt.Errorf("inspect synced state temporary file: %w", err)
	}
	if err := validateOwnedRegularFile(temporary.Name(), temporaryInfo, files); err != nil {
		return fileIdentity{}, false, err
	}
	identity, err := identityFromFileInfo(temporary.Name(), temporaryInfo)
	if err != nil {
		return fileIdentity{}, false, err
	}
	if err := files.closeFile(temporary); err != nil {
		closed = true
		return fileIdentity{}, false, fmt.Errorf("close state temporary file: %w", err)
	}
	closed = true
	if err := files.renameAt(directory, temporaryName, name); err != nil {
		return fileIdentity{}, false, fmt.Errorf("replace state file: %w", err)
	}
	if err := validateStateDirectoryTree(filepath.Dir(path), files); err != nil {
		return identity, true, err
	}
	if err := validatePathMatchesHandle(filepath.Dir(path), directory, files); err != nil {
		return identity, true, err
	}
	if err := validateNamedStateIdentity(directory, name, path, identity, files); err != nil {
		return identity, true, err
	}
	if err := syncOpenStateDirectory(directory, files); err != nil {
		return identity, true, err
	}
	if err := validatePathMatchesHandle(filepath.Dir(path), directory, files); err != nil {
		return identity, true, err
	}
	return identity, true, nil
}

// createTemporaryStateFile creates one unpredictable O_EXCL basename through
// the held directory fd so a replaced pathname cannot redirect the snapshot.
func createTemporaryStateFile(directory *os.File, prefix, statePath string) (*os.File, string, error) {
	for attempt := 0; attempt < temporaryNameAttempts; attempt++ {
		var random [12]byte
		if _, err := cryptorand.Read(random[:]); err != nil {
			return nil, "", fmt.Errorf("generate temporary state name: %w", err)
		}
		name := prefix + hex.EncodeToString(random[:])
		fd, err := unix.Openat(
			int(directory.Fd()),
			name,
			unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
			uint32(filePermission),
		)
		if errors.Is(err, unix.EEXIST) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		return os.NewFile(uintptr(fd), filepath.Join(filepath.Dir(statePath), name)), name, nil
	}
	return nil, "", fmt.Errorf("temporary state name collision limit reached")
}

// syncOpenStateDirectory commits a directory-relative rename through the same
// inode FileStore locked, preserving the existing post-rename uncertainty rule.
func syncOpenStateDirectory(directory *os.File, files filePrimitives) error {
	if err := files.syncDirectory(directory); err != nil {
		return fmt.Errorf("sync state directory: %w", err)
	}
	return nil
}

// syncStateDirectory makes the rename durable in the parent directory before
// FileStore publishes the corresponding in-memory candidate.
func syncStateDirectory(directory string, files filePrimitives) error {
	handle, err := files.openDirectory(directory)
	if err != nil {
		return fmt.Errorf("open state directory for sync: %w", err)
	}
	if err := files.syncDirectory(handle); err != nil {
		_ = files.closeFile(handle)
		return fmt.Errorf("sync state directory: %w", err)
	}
	if err := files.closeFile(handle); err != nil {
		return fmt.Errorf("close synced state directory: %w", err)
	}
	return nil
}

// writeEntireFile handles short writes explicitly so a successful primitive
// call means every encoded byte reached the temporary file before fsync.
func writeEntireFile(file *os.File, encoded []byte) error {
	for len(encoded) > 0 {
		written, err := file.Write(encoded)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		encoded = encoded[written:]
	}
	return nil
}

// unavailableError returns the fail-stop reason that prevents callbacks after
// Close or a post-rename durability failure; callers must reopen before use.
func (s *FileStore) unavailableError() error {
	if s.closed {
		return ErrFileStoreClosed
	}
	return s.poisoned
}

// validateFilesystemIdentity rechecks every pathname-to-inode binding retained
// by FileStore so lock, directory, state, owner, or mode replacement fails stop.
func (s *FileStore) validateFilesystemIdentity() error {
	if err := validateStateDirectoryTree(s.directoryPath, s.files); err != nil {
		return err
	}
	if err := validatePathMatchesHandle(s.directoryPath, s.directoryHandle, s.files); err != nil {
		return err
	}
	lockInfo, err := s.files.lstat(s.lockPath)
	if err != nil {
		return fmt.Errorf("inspect state lock %q: %w: %w", s.lockPath, ErrInvalidRecord, err)
	}
	if err := validateOwnedRegularFile(s.lockPath, lockInfo, s.files); err != nil {
		return err
	}
	if err := validatePathMatchesHandle(s.lockPath, s.lockHandle, s.files); err != nil {
		return err
	}
	return validateNamedStateIdentity(s.directoryHandle, s.stateName, s.path, s.stateIdentity, s.files)
}

// View holds a shared lock while the callback reads one consistent durable
// snapshot; returned records are copies and the Reader closes with the callback.
func (s *FileStore) View(ctx context.Context, fn func(Reader) error) error {
	if fn == nil {
		return ErrNilCallback
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	if err := s.unavailableError(); err != nil {
		s.mu.RUnlock()
		return err
	}
	if err := s.validateFilesystemIdentity(); err != nil {
		s.mu.RUnlock()
		return err
	}
	view := &memoryView{data: &s.data, retention: s.retention}
	defer func() {
		view.close()
		s.mu.RUnlock()
	}()
	return fn(view)
}

// Update validates a copy-on-write candidate and publishes it only after durable
// commit; a failure after rename poisons the instance because rollback is unknowable.
func (s *FileStore) Update(ctx context.Context, fn func(Tx) error) error {
	if fn == nil {
		return ErrNilCallback
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.unavailableError(); err != nil {
		return err
	}
	if err := s.validateFilesystemIdentity(); err != nil {
		return err
	}
	candidate := s.data.clone()
	tx := &memoryView{data: &candidate, writable: true, retention: s.retention}
	defer tx.close()
	if err := fn(tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := candidate.applyRetention(s.retention); err != nil {
		return err
	}
	if err := candidate.validate(); err != nil {
		return err
	}
	if err := s.validateFilesystemIdentity(); err != nil {
		return err
	}
	identity, renamed, err := persistFileData(s.directoryHandle, s.stateName, s.path, candidate, s.files)
	if err != nil {
		if renamed {
			s.poisoned = errors.Join(ErrDurabilityUncertain, err)
			return fmt.Errorf("persist state transaction after rename: %w", s.poisoned)
		}
		return fmt.Errorf("persist state transaction: %w", err)
	}
	s.stateIdentity = identity
	s.data = candidate
	return nil
}

// Close permanently disables this FileStore instance and releases its daemon
// ownership lock; it is idempotent and does not remove the stable lock file.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	directoryHandle := s.directoryHandle
	s.directoryHandle = nil
	lockHandle := s.lockHandle
	s.lockHandle = nil
	return errors.Join(releaseLockedStateDirectory(directoryHandle), releaseStateFileLock(lockHandle))
}
