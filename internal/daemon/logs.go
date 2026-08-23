package daemon

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"

	v1 "mydocker/api/runtime/v1"
	"mydocker/internal/domain"
	"mydocker/internal/logstore"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/shim"
)

const maximumLogReadLimit = 101

var (
	// ErrLogNotFound reports that no currently owned log source is registered for an exact Container Attempt.
	ErrLogNotFound = errors.New("Container Attempt log source not found")
	// ErrLogAlreadyRegistered reports an identity collision that must not silently replace a live source.
	ErrLogAlreadyRegistered = errors.New("Container Attempt log source is already registered")
	// ErrLogRegistryUnavailable reports that no trusted runtime root can resolve a production log binding.
	ErrLogRegistryUnavailable = errors.New("Container Attempt log registry has no trusted runtime root")
	// ErrLogRegistrationUnsafe reports owner or runtime-artifact evidence that cannot authorize a log path.
	ErrLogRegistrationUnsafe = errors.New("Container Attempt log registration is unsafe")
)

// LogSource is the read-only subset of an open log store used by the API adapter.
type LogSource interface {
	Identity() logstore.Identity
	Read(logstore.Cursor, int) ([]logstore.Frame, error)
}

// LogLocator resolves an exact Container/Attempt identity without accepting a host path from the API.
type LogLocator interface {
	Locate(context.Context, logstore.Identity) (LogSource, error)
}

// LogRegistrar binds and idempotently unbinds exact API identities without accepting a host path.
type LogRegistrar interface {
	RegisterAttempt(logstore.Identity, ownership.OwnerKey) error
	UnregisterAttempt(logstore.Identity, ownership.OwnerKey) error
	CaptureRegistration(logstore.Identity) (LogRegistration, bool, error)
	UnregisterRegistration(LogRegistration) error
}

// LogAccess combines production registration with identity-only lookup for the API adapter.
type LogAccess interface {
	LogLocator
	LogRegistrar
}

// LogRegistration is an opaque process-local binding revision and discovery epoch captured before one Delete mutation.
type LogRegistration struct {
	identity logstore.Identity
	revision uint64
	epoch    uint64
}

// LogRegistry keeps test sources or owner bindings; production file paths are always derived beneath one trusted runtime root.
type LogRegistry struct {
	mu           sync.RWMutex
	root         string
	rootInfo     os.FileInfo
	sources      map[logstore.Identity]LogSource
	owners       map[logstore.Identity]ownership.OwnerKey
	revisions    map[logstore.Identity]uint64
	epochs       map[logstore.Identity]uint64
	discoveredAt map[logstore.Identity]uint64
	nextRevision uint64
}

var _ LogAccess = (*LogRegistry)(nil)

// NewLogRegistry constructs an in-memory registry for injected read-only sources used by pure adapter tests.
func NewLogRegistry() *LogRegistry {
	return &LogRegistry{
		sources:      make(map[logstore.Identity]LogSource),
		owners:       make(map[logstore.Identity]ownership.OwnerKey),
		revisions:    make(map[logstore.Identity]uint64),
		epochs:       make(map[logstore.Identity]uint64),
		discoveredAt: make(map[logstore.Identity]uint64),
	}
}

// NewRuntimeLogRegistry captures one private runtime-root identity for owner-derived production lookup without scanning or opening logs yet.
func NewRuntimeLogRegistry(root string) (*LogRegistry, error) {
	if root == "" || strings.ContainsRune(root, '\x00') || !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return nil, fmt.Errorf("%w: runtime root must be a clean absolute non-root path", ErrLogRegistrationUnsafe)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("%w: inspect runtime root: %v", ErrLogRegistrationUnsafe, err)
	}
	if err := validateLogRegistryDirectory(info); err != nil {
		return nil, err
	}
	registry := NewLogRegistry()
	registry.root = root
	registry.rootInfo = info
	return registry, nil
}

// Register publishes one already-open identity-bound source and rejects replacement of a live registration.
func (registry *LogRegistry) Register(source LogSource) error {
	if registry == nil {
		return errors.New("log registry must not be nil")
	}
	if isNilLogSource(source) {
		return errors.New("log source must not be nil")
	}
	identity := source.Identity()
	if err := identity.Validate(); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.sources == nil {
		registry.sources = make(map[logstore.Identity]LogSource)
	}
	if _, exists := registry.sources[identity]; exists {
		return ErrLogAlreadyRegistered
	}
	if _, exists := registry.owners[identity]; exists {
		return ErrLogAlreadyRegistered
	}
	if err := registry.assignRegistrationRevisionLocked(identity); err != nil {
		return err
	}
	delete(registry.discoveredAt, identity)
	registry.sources[identity] = source
	return nil
}

// RegisterAttempt idempotently binds an identity to its validated Container-create owner and never consumes a caller-provided path.
func (registry *LogRegistry) RegisterAttempt(identity logstore.Identity, owner ownership.OwnerKey) error {
	if registry == nil {
		return errors.New("log registry must not be nil")
	}
	if registry.root == "" {
		return ErrLogRegistryUnavailable
	}
	if err := identity.Validate(); err != nil {
		return err
	}
	if err := owner.Validate(); err != nil {
		return fmt.Errorf("%w: invalid lifecycle owner", ErrLogRegistrationUnsafe)
	}
	if owner.Target.Kind != operation.TargetContainer || owner.Target.ID != string(identity.ContainerID) {
		return fmt.Errorf("%w: lifecycle owner does not target the exact Container", ErrLogRegistrationUnsafe)
	}
	if err := registry.validateRoot(); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.owners == nil {
		registry.owners = make(map[logstore.Identity]ownership.OwnerKey)
	}
	if _, exists := registry.sources[identity]; exists {
		return ErrLogAlreadyRegistered
	}
	if existing, exists := registry.owners[identity]; exists {
		if existing == owner {
			return nil
		}
		return ErrLogAlreadyRegistered
	}
	if err := registry.assignRegistrationRevisionLocked(identity); err != nil {
		return err
	}
	delete(registry.discoveredAt, identity)
	registry.owners[identity] = owner
	return nil
}

// UnregisterAttempt removes only a binding that still belongs to the durable
// deleted owner. A replay for an older incarnation cannot erase a replacement
// owner, while an absent mapping advances its epoch to fence slow discovery.
func (registry *LogRegistry) UnregisterAttempt(identity logstore.Identity, owner ownership.OwnerKey) error {
	if registry == nil {
		return errors.New("log registry must not be nil")
	}
	if err := identity.Validate(); err != nil {
		return err
	}
	if err := owner.Validate(); err != nil || owner.Target.Kind != operation.TargetContainer || owner.Target.ID != string(identity.ContainerID) {
		return fmt.Errorf("%w: deleted lifecycle owner does not target the exact Container", ErrLogRegistrationUnsafe)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if current, found := registry.owners[identity]; found && current != owner {
		return nil
	}
	if _, sourceFound := registry.sources[identity]; sourceFound {
		if _, ownerFound := registry.owners[identity]; !ownerFound {
			return fmt.Errorf("%w: manual log source has no comparable lifecycle owner", ErrLogRegistrationUnsafe)
		}
	}
	if err := registry.advanceRegistrationEpochLocked(identity); err != nil {
		return err
	}
	delete(registry.sources, identity)
	delete(registry.owners, identity)
	delete(registry.revisions, identity)
	delete(registry.discoveredAt, identity)
	return nil
}

// Unregister removes only the exact identity mapping and is idempotent so source shutdown can be retried.
func (registry *LogRegistry) Unregister(identity logstore.Identity) error {
	if registry == nil {
		return errors.New("log registry must not be nil")
	}
	if err := identity.Validate(); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if err := registry.advanceRegistrationEpochLocked(identity); err != nil {
		return err
	}
	delete(registry.sources, identity)
	delete(registry.owners, identity)
	delete(registry.revisions, identity)
	delete(registry.discoveredAt, identity)
	return nil
}

// CaptureRegistration snapshots the process-local revision of one exact binding so a completed Delete cannot remove a later reuse of the same IDs.
func (registry *LogRegistry) CaptureRegistration(identity logstore.Identity) (LogRegistration, bool, error) {
	if registry == nil {
		return LogRegistration{}, false, errors.New("log registry must not be nil")
	}
	if err := identity.Validate(); err != nil {
		return LogRegistration{}, false, err
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	revision, found := registry.revisions[identity]
	_, sourceFound := registry.sources[identity]
	_, ownerFound := registry.owners[identity]
	if !found {
		if sourceFound || ownerFound {
			return LogRegistration{}, false, fmt.Errorf("%w: log binding has no registration revision", ErrLogRegistrationUnsafe)
		}
		return LogRegistration{identity: identity, epoch: registry.epochs[identity]}, false, nil
	}
	if revision == 0 || sourceFound == ownerFound {
		return LogRegistration{}, false, fmt.Errorf("%w: log binding revision is inconsistent", ErrLogRegistrationUnsafe)
	}
	return LogRegistration{identity: identity, revision: revision, epoch: registry.epochs[identity]}, true, nil
}

// UnregisterRegistration invalidates pre-Delete discovery, removes only the captured or stale-discovered binding, and preserves a later direct registration.
func (registry *LogRegistry) UnregisterRegistration(registration LogRegistration) error {
	if registry == nil {
		return errors.New("log registry must not be nil")
	}
	if err := registration.identity.Validate(); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.epochs[registration.identity] != registration.epoch {
		return nil
	}
	if err := registry.advanceRegistrationEpochLocked(registration.identity); err != nil {
		return err
	}
	currentRevision, revisionFound := registry.revisions[registration.identity]
	discoveryEpoch, discovered := registry.discoveredAt[registration.identity]
	removeCaptured := registration.revision != 0 && revisionFound && currentRevision == registration.revision
	removeStaleDiscovery := discovered && discoveryEpoch == registration.epoch
	if removeCaptured || removeStaleDiscovery {
		delete(registry.sources, registration.identity)
		delete(registry.owners, registration.identity)
		delete(registry.revisions, registration.identity)
		delete(registry.discoveredAt, registration.identity)
	}
	return nil
}

// assignRegistrationRevisionLocked publishes one monotonic process-local revision while the registry write lock is held.
func (registry *LogRegistry) assignRegistrationRevisionLocked(identity logstore.Identity) error {
	if registry.nextRevision == math.MaxUint64 {
		return errors.New("log registration revision space is exhausted")
	}
	registry.nextRevision++
	if registry.revisions == nil {
		registry.revisions = make(map[logstore.Identity]uint64)
	}
	registry.revisions[identity] = registry.nextRevision
	return nil
}

// advanceRegistrationEpochLocked invalidates owner discovery begun before a successful Delete while preserving newer direct registrations.
func (registry *LogRegistry) advanceRegistrationEpochLocked(identity logstore.Identity) error {
	if registry.nextRevision == math.MaxUint64 {
		return errors.New("log registration revision space is exhausted")
	}
	registry.nextRevision++
	if registry.epochs == nil {
		registry.epochs = make(map[logstore.Identity]uint64)
	}
	registry.epochs[identity] = registry.nextRevision
	return nil
}

// discoveryEpoch snapshots the identity epoch that a slow filesystem scan must still match before publishing its owner.
func (registry *LogRegistry) discoveryEpoch(identity logstore.Identity) uint64 {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return registry.epochs[identity]
}

// registerDiscoveredAttempt publishes one scanned owner only when no successful Delete invalidated the scan in flight.
func (registry *LogRegistry) registerDiscoveredAttempt(identity logstore.Identity, owner ownership.OwnerKey, expectedEpoch uint64) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if err := owner.Validate(); err != nil || owner.Target.Kind != operation.TargetContainer || owner.Target.ID != string(identity.ContainerID) {
		return fmt.Errorf("%w: discovered lifecycle owner does not target the exact Container", ErrLogRegistrationUnsafe)
	}
	if err := registry.validateRoot(); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.epochs[identity] != expectedEpoch {
		return ErrLogNotFound
	}
	if _, exists := registry.sources[identity]; exists {
		return ErrLogAlreadyRegistered
	}
	if existing, exists := registry.owners[identity]; exists {
		if existing == owner {
			return nil
		}
		return ErrLogAlreadyRegistered
	}
	if err := registry.assignRegistrationRevisionLocked(identity); err != nil {
		return err
	}
	if registry.discoveredAt == nil {
		registry.discoveredAt = make(map[logstore.Identity]uint64)
	}
	registry.discoveredAt[identity] = expectedEpoch
	registry.owners[identity] = owner
	return nil
}

// Locate resolves a manual source or an owner-derived descriptor-free reader after honoring caller cancellation.
func (registry *LogRegistry) Locate(ctx context.Context, identity logstore.Identity) (LogSource, error) {
	if registry == nil {
		return nil, errors.New("log registry must not be nil")
	}
	if ctx == nil {
		return nil, errors.New("log lookup context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	registry.mu.RLock()
	source, found := registry.sources[identity]
	owner, ownerFound := registry.owners[identity]
	registry.mu.RUnlock()
	if found {
		return source, nil
	}
	if ownerFound {
		return registry.openOwnerSource(identity, owner)
	}
	if registry.root == "" {
		return nil, ErrLogNotFound
	}
	epoch := registry.discoveryEpoch(identity)
	discovered, err := registry.discoverOwner(identity)
	if err != nil {
		return nil, err
	}
	if err := registry.registerDiscoveredAttempt(identity, discovered, epoch); err != nil {
		return nil, err
	}
	return registry.openOwnerSource(identity, discovered)
}

// openOwnerSource derives the only allowed file location and returns a reader that closes every request descriptor before returning.
func (registry *LogRegistry) openOwnerSource(identity logstore.Identity, owner ownership.OwnerKey) (LogSource, error) {
	path, err := registry.logPath(owner)
	if err != nil {
		return nil, err
	}
	reader, err := logstore.OpenReader(path, identity)
	if errors.Is(err, logstore.ErrNotFound) {
		return nil, ErrLogNotFound
	}
	if err != nil {
		return nil, err
	}
	return reader, nil
}

// discoverOwner restores a lost process-local binding only from strict private init configs beneath the captured runtime root.
func (registry *LogRegistry) discoverOwner(identity logstore.Identity) (ownership.OwnerKey, error) {
	if err := registry.validateRoot(); err != nil {
		return ownership.OwnerKey{}, err
	}
	ownersRoot := filepath.Join(registry.root, "owners")
	ownersInfo, err := os.Lstat(ownersRoot)
	if errors.Is(err, os.ErrNotExist) {
		return ownership.OwnerKey{}, ErrLogNotFound
	}
	if err != nil {
		return ownership.OwnerKey{}, fmt.Errorf("%w: inspect owner registry: %v", ErrLogRegistrationUnsafe, err)
	}
	if err := validateLogRegistryDirectory(ownersInfo); err != nil {
		return ownership.OwnerKey{}, err
	}
	entries, err := os.ReadDir(ownersRoot)
	if err != nil {
		return ownership.OwnerKey{}, fmt.Errorf("%w: enumerate owner registry: %v", ErrLogRegistrationUnsafe, err)
	}
	var matched ownership.OwnerKey
	found := false
	for _, entry := range entries {
		if !validOwnerToken(entry.Name()) {
			continue
		}
		ownerRoot := filepath.Join(ownersRoot, entry.Name())
		ownerInfo, err := os.Lstat(ownerRoot)
		if err != nil {
			return ownership.OwnerKey{}, fmt.Errorf("%w: inspect owner directory: %v", ErrLogRegistrationUnsafe, err)
		}
		if err := validateLogRegistryDirectory(ownerInfo); err != nil {
			return ownership.OwnerKey{}, err
		}
		configPath := filepath.Join(ownerRoot, "shim.json")
		configInfo, err := os.Lstat(configPath)
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return ownership.OwnerKey{}, fmt.Errorf("%w: inspect shim registration: %v", ErrLogRegistrationUnsafe, err)
		}
		if err := validateLogRegistrationFile(configInfo); err != nil {
			return ownership.OwnerKey{}, err
		}
		config, err := shim.LoadRuntimeConfig(configPath)
		if err != nil {
			return ownership.OwnerKey{}, fmt.Errorf("%w: invalid shim registration", ErrLogRegistrationUnsafe)
		}
		if config.Mode == shim.ModeKeeper {
			continue
		}
		if err := registry.validateDiscoveredConfig(entry.Name(), config); err != nil {
			return ownership.OwnerKey{}, err
		}
		if config.ContainerID != identity.ContainerID || config.AttemptID != identity.AttemptID {
			continue
		}
		if found && matched != config.Owner {
			return ownership.OwnerKey{}, fmt.Errorf("%w: duplicate identity bindings", ErrLogRegistrationUnsafe)
		}
		matched = config.Owner
		found = true
	}
	if !found {
		return ownership.OwnerKey{}, ErrLogNotFound
	}
	return matched, nil
}

// validateDiscoveredConfig proves a strict init config's paths and owner token were derived from this registry root.
func (registry *LogRegistry) validateDiscoveredConfig(token string, config shim.RuntimeConfig) error {
	if config.Mode != shim.ModeInit || config.Owner.Token != token || config.Owner.Target.Kind != operation.TargetContainer ||
		config.Owner.Target.ID != string(config.ContainerID) {
		return fmt.Errorf("%w: shim identity and owner binding differ", ErrLogRegistrationUnsafe)
	}
	ownerRoot := filepath.Join(registry.root, "owners", token)
	if config.ControlSocket != filepath.Join(ownerRoot, "control.sock") ||
		config.TerminalPath != filepath.Join(ownerRoot, "terminal.json") ||
		config.LogPath != filepath.Join(ownerRoot, "workload.log") {
		return fmt.Errorf("%w: shim artifact paths are not internally derived", ErrLogRegistrationUnsafe)
	}
	return nil
}

// logPath derives one fixed filename beneath a validated owner token and never accepts path text from API or persistence.
func (registry *LogRegistry) logPath(owner ownership.OwnerKey) (string, error) {
	if err := registry.validateRoot(); err != nil {
		return "", err
	}
	if err := owner.Validate(); err != nil || !validOwnerToken(owner.Token) {
		return "", fmt.Errorf("%w: invalid owner token", ErrLogRegistrationUnsafe)
	}
	return filepath.Join(registry.root, "owners", owner.Token, "workload.log"), nil
}

// validateRoot rejects replacement, type, permission, or ownership drift of the configured runtime root.
func (registry *LogRegistry) validateRoot() error {
	if registry == nil || registry.root == "" || registry.rootInfo == nil {
		return ErrLogRegistryUnavailable
	}
	info, err := os.Lstat(registry.root)
	if err != nil {
		return fmt.Errorf("%w: inspect runtime root: %v", ErrLogRegistrationUnsafe, err)
	}
	if !os.SameFile(registry.rootInfo, info) {
		return fmt.Errorf("%w: runtime root was replaced", ErrLogRegistrationUnsafe)
	}
	return validateLogRegistryDirectory(info)
}

// validateLogRegistryDirectory requires a real, same-owner directory with no group, world, or special permissions.
func validateLogRegistryDirectory(info os.FileInfo) error {
	if info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%w: runtime directory is not private", ErrLogRegistrationUnsafe)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("%w: runtime directory owner differs from daemon", ErrLogRegistrationUnsafe)
	}
	return nil
}

// validateLogRegistrationFile requires one same-owner, single-link, private regular config with no special permission bits.
func validateLogRegistrationFile(info os.FileInfo) error {
	if info == nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || info.Mode().Perm()&0o177 != 0 {
		return fmt.Errorf("%w: shim registration file is not private", ErrLogRegistrationUnsafe)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		return fmt.Errorf("%w: shim registration ownership is ambiguous", ErrLogRegistrationUnsafe)
	}
	return nil
}

// validOwnerToken recognizes only the lowercase SHA-256 owner token vocabulary used for directory derivation.
func validOwnerToken(token string) bool {
	if len(token) != 64 {
		return false
	}
	for _, character := range token {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

// isNilLogSource detects an interface containing a nil pointer before Identity can dereference it.
func isNilLogSource(source LogSource) bool {
	if source == nil {
		return true
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// LogsAfter verifies the requested Attempt against authoritative metadata, resolves by identity, and projects only frame data.
func (service *Service) LogsAfter(ctx context.Context, requestContext v1.RequestContext, request v1.ListLogsRequest) ([]v1.LogFrame, error) {
	if err := validateReadCall(ctx, requestContext); err != nil {
		return nil, err
	}
	if err := v1.ValidateResourceID("container_id", request.ContainerID); err != nil {
		return nil, err
	}
	if err := v1.ValidateResourceID("attempt_id", request.AttemptID); err != nil {
		return nil, err
	}
	if err := validatePageLimit(request.Limit, maximumLogReadLimit); err != nil {
		return nil, err
	}
	pair, err := service.queries.GetContainer(ctx, domain.ContainerID(request.ContainerID))
	if err != nil {
		return nil, MapError(err)
	}
	if string(pair.Attempt.ID) != request.AttemptID {
		return nil, v1.NewError(v1.CodeFailedPrecondition, "attempt_id", "does not identify the Container's canonical Attempt")
	}
	identity := logstore.Identity{ContainerID: pair.Container.ID, AttemptID: pair.Attempt.ID}
	source, err := service.logs.Locate(ctx, identity)
	if err != nil {
		return nil, MapError(err)
	}
	if source.Identity() != identity {
		return nil, v1.NewError(v1.CodeInternal, "logs", "log locator returned a different Container Attempt")
	}
	frames, err := source.Read(logstore.Cursor(request.AfterCursor), request.Limit)
	if err != nil {
		return nil, MapError(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, MapError(err)
	}
	if len(frames) > request.Limit {
		return nil, v1.NewError(v1.CodeInternal, "logs", "log source returned more frames than requested")
	}
	projected := make([]v1.LogFrame, len(frames))
	previousCursor := request.AfterCursor
	streamSequences := make(map[string]uint64, 2)
	for index, frame := range frames {
		projected[index], err = projectLogFrame(identity, frame)
		if err != nil {
			return nil, MapError(err)
		}
		if projected[index].Cursor <= previousCursor {
			return nil, v1.NewError(v1.CodeInternal, "logs", "log source returned a non-increasing cursor")
		}
		if previous := streamSequences[projected[index].Stream]; previous != 0 && projected[index].Sequence != previous+1 {
			return nil, v1.NewError(v1.CodeInternal, "logs", "log source returned a non-contiguous stream sequence")
		}
		previousCursor = projected[index].Cursor
		streamSequences[projected[index].Stream] = projected[index].Sequence
	}
	return projected, nil
}

// projectLogFrame verifies checksums and exact identity before copying public stream bytes and cursors.
func projectLogFrame(identity logstore.Identity, frame logstore.Frame) (v1.LogFrame, error) {
	if err := frame.Validate(); err != nil {
		return v1.LogFrame{}, err
	}
	if frame.Identity != identity {
		return v1.LogFrame{}, logstore.ErrIdentityMismatch
	}
	return v1.LogFrame{
		ContainerID:   string(frame.Identity.ContainerID),
		AttemptID:     string(frame.Identity.AttemptID),
		Stream:        string(frame.Stream),
		Cursor:        uint64(frame.Cursor),
		Sequence:      frame.Sequence,
		Payload:       append([]byte(nil), frame.Payload...),
		PayloadSHA256: frame.PayloadSHA256,
	}, nil
}
