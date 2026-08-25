package isolation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// NamespaceEvidence is the serializable nsfs identity tied to one verified process owner.
type NamespaceEvidence struct {
	Type  NamespaceType   `json:"type"`
	Inode uint64          `json:"inode"`
	Owner ProcessEvidence `json:"owner"`
}

// CreatedNamespaceSet records the verified inode of each namespace created for one dedicated runtime helper.
// PID evidence describes pid_for_children; the helper must still fork the child that becomes namespace PID 1.
type CreatedNamespaceSet struct {
	Inodes map[NamespaceType]uint64 `json:"inodes"`
}

// Clone returns namespace evidence without sharing the mutable inode map.
func (s CreatedNamespaceSet) Clone() CreatedNamespaceSet {
	clone := CreatedNamespaceSet{Inodes: make(map[NamespaceType]uint64, len(s.Inodes))}
	for namespaceType, inode := range s.Inodes {
		clone.Inodes[namespaceType] = inode
	}
	return clone
}

// Validate rejects an empty, unsupported, or zero-inode created namespace set.
func (s CreatedNamespaceSet) Validate() error {
	if len(s.Inodes) == 0 {
		return fmt.Errorf("created namespace set must not be empty")
	}
	for namespaceType, inode := range s.Inodes {
		if !namespaceType.Valid() || inode == 0 {
			return fmt.Errorf("%w: created namespace %q has invalid inode %d", ErrUnsafeIdentity, namespaceType, inode)
		}
	}
	return nil
}

// UnshareNamespaces creates a bounded namespace set in one dedicated runtime helper and verifies every inode changed.
// It requires the active capability supplied by RunLockedHelper, whose runner
// disposes the OS thread on every return after a successful unshare.
func (h *LockedHelper) UnshareNamespaces(ctx context.Context, namespaceTypes ...NamespaceType) (CreatedNamespaceSet, error) {
	if err := validateContext(ctx); err != nil {
		return CreatedNamespaceSet{}, err
	}
	if err := h.checkThread(); err != nil {
		return CreatedNamespaceSet{}, err
	}
	if len(namespaceTypes) == 0 {
		return CreatedNamespaceSet{}, fmt.Errorf("at least one namespace is required")
	}
	if h.created == nil {
		h.created = make(map[NamespaceType]uint64)
	}
	before := make(map[NamespaceType]uint64, len(namespaceTypes))
	flags := 0
	for _, namespaceType := range namespaceTypes {
		if !namespaceType.Valid() {
			return CreatedNamespaceSet{}, fmt.Errorf("unsupported namespace %q", namespaceType)
		}
		if _, duplicate := before[namespaceType]; duplicate {
			return CreatedNamespaceSet{}, fmt.Errorf("duplicate namespace %q", namespaceType)
		}
		if _, alreadyCreated := h.created[namespaceType]; alreadyCreated {
			return CreatedNamespaceSet{}, fmt.Errorf("namespace %q was already created by this helper", namespaceType)
		}
		inode, err := currentNamespaceInode(h.ops, namespaceType)
		if err != nil {
			return CreatedNamespaceSet{}, fmt.Errorf("inspect current %s namespace: %w", namespaceType, err)
		}
		before[namespaceType] = inode
		flag, err := namespaceCloneFlag(namespaceType)
		if err != nil {
			return CreatedNamespaceSet{}, err
		}
		flags |= flag
	}
	if err := validateContext(ctx); err != nil {
		return CreatedNamespaceSet{}, err
	}
	if err := h.unshare(flags); err != nil {
		return CreatedNamespaceSet{}, fmt.Errorf("create namespaces: %w", err)
	}
	created := CreatedNamespaceSet{Inodes: make(map[NamespaceType]uint64, len(namespaceTypes))}
	for _, namespaceType := range namespaceTypes {
		inode, err := currentNamespaceInode(h.ops, namespaceType)
		if err != nil {
			return CreatedNamespaceSet{}, fmt.Errorf("verify created %s namespace: %w", namespaceType, err)
		}
		if inode == before[namespaceType] {
			return CreatedNamespaceSet{}, fmt.Errorf("%w: %s namespace inode did not change", ErrUnsafeIdentity, namespaceType)
		}
		created.Inodes[namespaceType] = inode
	}
	if err := created.Validate(); err != nil {
		return CreatedNamespaceSet{}, err
	}
	for namespaceType, inode := range created.Inodes {
		h.created[namespaceType] = inode
	}
	if _, createdMount := created.Inodes[NamespaceMount]; createdMount {
		h.fsPrivate = true
	}
	return created, nil
}

// currentNamespaceInode opens and validates the current thread's namespace entry without joining or retaining it.
func currentNamespaceInode(ops Ops, namespaceType NamespaceType) (uint64, error) {
	path := fmt.Sprintf("/proc/thread-self/ns/%s", namespaceType.threadProcName())
	fd, err := ops.OpenNamespace(path)
	if err != nil {
		return 0, err
	}
	stat, statErr := ops.Fstat(fd)
	if statErr == nil {
		statErr = validateNamespaceFD(ops, fd, namespaceType, stat.Ino)
	}
	closeErr := ops.Close(fd)
	if err := closeError(statErr, closeErr); err != nil {
		return 0, err
	}
	return stat.Ino, nil
}

// Validate rejects a namespace identity without a supported kind, inode, or strong owner.
func (e NamespaceEvidence) Validate() error {
	if !e.Type.Valid() || e.Inode == 0 {
		return fmt.Errorf("%w: namespace type or inode is invalid", ErrUnsafeIdentity)
	}
	return e.Owner.Validate()
}

// NamespaceHandle is a runtime-only nsfs descriptor that remains bound to a verified process owner.
type NamespaceHandle struct {
	mu       sync.Mutex
	ops      Ops
	owner    *ProcessHandle
	fd       int
	evidence NamespaceEvidence
	closed   bool
}

// OpenNamespaceHandle opens one supported namespace only through an action-time verified process handle.
func OpenNamespaceHandle(ctx context.Context, owner *ProcessHandle, namespaceType NamespaceType) (*NamespaceHandle, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if owner == nil {
		return nil, fmt.Errorf("%w: namespace owner is nil", ErrUnsafeIdentity)
	}
	if !namespaceType.Valid() {
		return nil, fmt.Errorf("unsupported namespace %q", namespaceType)
	}
	if err := owner.Verify(ctx); err != nil {
		return nil, fmt.Errorf("verify namespace owner: %w", err)
	}
	ownerEvidence, err := owner.Evidence()
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/proc/%d/ns/%s", ownerEvidence.PID, namespaceType.procName())
	fd, err := owner.ops.OpenNamespace(path)
	if err != nil {
		return nil, fmt.Errorf("open %s namespace: %w", namespaceType, err)
	}
	stat, err := owner.ops.Fstat(fd)
	if err == nil {
		err = validateNamespaceFD(owner.ops, fd, namespaceType, stat.Ino)
	}
	if err == nil {
		err = owner.Verify(ctx)
	}
	if err != nil {
		_ = owner.ops.Close(fd)
		return nil, fmt.Errorf("%w: open %s namespace: %v", ErrUnsafeIdentity, namespaceType, err)
	}
	evidence := NamespaceEvidence{Type: namespaceType, Inode: stat.Ino, Owner: ownerEvidence}
	if err := evidence.Validate(); err != nil {
		_ = owner.ops.Close(fd)
		return nil, err
	}
	return &NamespaceHandle{ops: owner.ops, owner: owner, fd: fd, evidence: evidence}, nil
}

// Evidence returns the immutable serializable owner, namespace kind, and inode tuple.
func (h *NamespaceHandle) Evidence() (NamespaceEvidence, error) {
	if h == nil {
		return NamespaceEvidence{}, fmt.Errorf("%w: nil namespace handle", ErrClosed)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return NamespaceEvidence{}, ErrClosed
	}
	return h.evidence, nil
}

// Verify rechecks the process owner and nsfs descriptor before any namespace join.
func (h *NamespaceHandle) Verify(ctx context.Context) error {
	return h.withVerifiedFD(ctx, func(int, NamespaceEvidence) error { return nil })
}

// Duplicate returns one independently owned close-on-exec descriptor only
// after action-time owner/nsfs verification; callers use it for one child launch.
func (h *NamespaceHandle) Duplicate(ctx context.Context) (int, NamespaceEvidence, error) {
	duplicated := -1
	var evidence NamespaceEvidence
	err := h.withVerifiedFD(ctx, func(fd int, current NamespaceEvidence) error {
		var err error
		duplicated, err = h.ops.Dup(fd)
		if err != nil {
			return fmt.Errorf("duplicate namespace descriptor: %w", err)
		}
		evidence = current
		return nil
	})
	if err != nil {
		if duplicated >= 0 {
			_ = h.ops.Close(duplicated)
		}
		return -1, NamespaceEvidence{}, err
	}
	return duplicated, evidence, nil
}

// withVerifiedFD keeps the namespace descriptor open while a caller performs one checked action.
func (h *NamespaceHandle) withVerifiedFD(ctx context.Context, action func(int, NamespaceEvidence) error) error {
	if h == nil {
		return fmt.Errorf("%w: nil namespace handle", ErrClosed)
	}
	if err := validateContext(ctx); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrClosed
	}
	if err := h.owner.Verify(ctx); err != nil {
		return fmt.Errorf("verify namespace owner: %w", err)
	}
	currentOwner, err := h.owner.Evidence()
	if err != nil {
		return err
	}
	if currentOwner != h.evidence.Owner {
		return fmt.Errorf("%w: namespace owner evidence changed", ErrUnsafeIdentity)
	}
	if err := validateNamespaceFD(h.ops, h.fd, h.evidence.Type, h.evidence.Inode); err != nil {
		return err
	}
	if action == nil {
		return nil
	}
	return action(h.fd, h.evidence)
}

// Close releases the namespace descriptor without changing any namespace membership.
func (h *NamespaceHandle) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	if err := h.ops.Close(h.fd); err != nil {
		return fmt.Errorf("close namespace handle: %w", err)
	}
	return nil
}

// namespaceSession owns original namespace handles for cleanup on one LockedHelper thread.
type namespaceSession struct {
	mu        sync.Mutex
	ops       Ops
	helper    *LockedHelper
	threadID  int
	originals map[NamespaceType]sessionNamespace
	targets   map[NamespaceType]uint64
	joined    []NamespaceType
	closed    bool
}

// sessionNamespace retains the original namespace descriptor and inode for verified restore.
type sessionNamespace struct {
	fd    int
	inode uint64
}

// RunNamespaceSession joins verified namespaces and runs action on the same dedicated LockedHelper thread.
// Cleanup always uses a fresh bounded context; once any setns succeeds, the
// runner discards that OS thread even when restoration succeeds.
func RunNamespaceSession(ctx context.Context, ops Ops, handles []*NamespaceHandle, action func(context.Context, *LockedHelper) error) error {
	return runNamespaceSession(ctx, ops, runtimeThreadLocker{}, defaultNamespaceCleanupTimeout, handles, action)
}

// runNamespaceSession supplies fake thread pinning and cleanup timing to pure tests.
func runNamespaceSession(ctx context.Context, ops Ops, locker osThreadLocker, cleanupTimeout time.Duration, handles []*NamespaceHandle, action func(context.Context, *LockedHelper) error) error {
	if action == nil {
		return fmt.Errorf("namespace session action must not be nil")
	}
	return runLockedHelperWithCleanup(ctx, ops, locker, cleanupTimeout, func(helper *LockedHelper) error {
		if err := helper.JoinNamespaces(ctx, handles...); err != nil {
			return err
		}
		return action(ctx, helper)
	})
}

// JoinNamespaces snapshots the current namespaces and joins verified handles in order on this helper thread.
// Each successful setns is followed by an exact nsfs inode readback and
// permanently taints the helper thread for disposal by its runner.
func (h *LockedHelper) JoinNamespaces(ctx context.Context, handles ...*NamespaceHandle) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := h.checkThread(); err != nil {
		return err
	}
	if len(handles) == 0 {
		return fmt.Errorf("at least one namespace handle is required")
	}
	if h.session != nil {
		return fmt.Errorf("locked helper already owns a namespace session")
	}
	namespaceTypes := make([]NamespaceType, 0, len(handles))
	seen := make(map[NamespaceType]struct{}, len(handles))
	for _, handle := range handles {
		if handle == nil {
			return fmt.Errorf("%w: namespace handle is nil", ErrUnsafeIdentity)
		}
		if err := handle.Verify(ctx); err != nil {
			return fmt.Errorf("verify namespace before session: %w", err)
		}
		evidence, err := handle.Evidence()
		if err != nil {
			return err
		}
		if _, duplicate := seen[evidence.Type]; duplicate {
			return fmt.Errorf("duplicate namespace %q", evidence.Type)
		}
		seen[evidence.Type] = struct{}{}
		namespaceTypes = append(namespaceTypes, evidence.Type)
	}
	session, err := beginNamespaceSession(ctx, h, namespaceTypes...)
	if err != nil {
		return err
	}
	h.session = session
	for _, handle := range handles {
		if err := session.join(ctx, handle); err != nil {
			return err
		}
	}
	return nil
}

// beginNamespaceSession snapshots requested originals after verifying the LockedHelper thread capability.
func beginNamespaceSession(ctx context.Context, helper *LockedHelper, namespaceTypes ...NamespaceType) (*namespaceSession, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if err := helper.checkThread(); err != nil {
		return nil, err
	}
	if len(namespaceTypes) == 0 {
		return nil, fmt.Errorf("at least one namespace is required")
	}
	ops := helper.ops
	session := &namespaceSession{
		ops: ops, helper: helper, threadID: ops.ThreadID(),
		originals: make(map[NamespaceType]sessionNamespace, len(namespaceTypes)),
		targets:   make(map[NamespaceType]uint64, len(namespaceTypes)),
	}
	for _, namespaceType := range namespaceTypes {
		if !namespaceType.Valid() {
			_ = session.closeWithoutRestore()
			return nil, fmt.Errorf("unsupported namespace %q", namespaceType)
		}
		if _, duplicate := session.originals[namespaceType]; duplicate {
			_ = session.closeWithoutRestore()
			return nil, fmt.Errorf("duplicate namespace %q", namespaceType)
		}
		fd, err := ops.OpenNamespace(fmt.Sprintf("/proc/thread-self/ns/%s", namespaceType.threadProcName()))
		if err != nil {
			_ = session.closeWithoutRestore()
			return nil, fmt.Errorf("open original %s namespace: %w", namespaceType, err)
		}
		stat, statErr := ops.Fstat(fd)
		if statErr == nil {
			statErr = validateNamespaceFD(ops, fd, namespaceType, stat.Ino)
		}
		if statErr != nil {
			_ = ops.Close(fd)
			_ = session.closeWithoutRestore()
			return nil, fmt.Errorf("verify original %s namespace: %w", namespaceType, statErr)
		}
		session.originals[namespaceType] = sessionNamespace{fd: fd, inode: stat.Ino}
	}
	return session, nil
}

// join verifies ownership and changes only the session's dedicated OS thread to the requested namespace.
func (s *namespaceSession) join(ctx context.Context, handle *NamespaceHandle) error {
	if s == nil {
		return fmt.Errorf("%w: nil namespace session", ErrClosed)
	}
	if err := validateContext(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkThreadLocked(); err != nil {
		return err
	}
	return handle.withVerifiedFD(ctx, func(fd int, evidence NamespaceEvidence) error {
		if _, tracked := s.originals[evidence.Type]; !tracked {
			return fmt.Errorf("namespace %q was not captured by this session", evidence.Type)
		}
		for _, joinedType := range s.joined {
			if joinedType == evidence.Type {
				return fmt.Errorf("namespace %q is already joined", evidence.Type)
			}
		}
		flag, err := namespaceCloneFlag(evidence.Type)
		if err != nil {
			return err
		}
		if evidence.Type == NamespaceMount {
			if err := s.helper.ensurePrivateFS(); err != nil {
				return err
			}
		}
		s.joined = append(s.joined, evidence.Type)
		if err := s.helper.setns(fd, flag); err != nil {
			joinErr := fmt.Errorf("join %s namespace: %w", evidence.Type, err)
			original := s.originals[evidence.Type]
			probeErr := probeNamespaceInode(s.ops, fmt.Sprintf("/proc/thread-self/ns/%s", evidence.Type.threadProcName()), evidence.Type, original.inode)
			if probeErr == nil {
				s.joined = s.joined[:len(s.joined)-1]
				return joinErr
			}
			return errors.Join(joinErr, fmt.Errorf("verify namespace after failed setns: %w", probeErr))
		}
		if err := probeNamespaceInode(s.ops, fmt.Sprintf("/proc/thread-self/ns/%s", evidence.Type.threadProcName()), evidence.Type, evidence.Inode); err != nil {
			verifyErr := fmt.Errorf("verify joined %s namespace: %w", evidence.Type, err)
			if restoreErr := s.restoreLocked(evidence.Type); restoreErr != nil {
				return errors.Join(verifyErr, fmt.Errorf("restore after failed join verification: %w", restoreErr))
			}
			return verifyErr
		}
		s.targets[evidence.Type] = evidence.Inode
		return nil
	})
}

// restoreLocked restores one tracked namespace and removes it from the joined stack.
func (s *namespaceSession) restoreLocked(namespaceType NamespaceType) error {
	index := -1
	for candidate := len(s.joined) - 1; candidate >= 0; candidate-- {
		if s.joined[candidate] == namespaceType {
			index = candidate
			break
		}
	}
	if index < 0 {
		return nil
	}
	original := s.originals[namespaceType]
	flag, err := namespaceCloneFlag(namespaceType)
	if err != nil {
		return err
	}
	if err := s.helper.setns(original.fd, flag); err != nil {
		return fmt.Errorf("restore %s namespace: %w", namespaceType, err)
	}
	fd, err := s.ops.OpenNamespace(fmt.Sprintf("/proc/thread-self/ns/%s", namespaceType.threadProcName()))
	if err != nil {
		return fmt.Errorf("open restored %s namespace: %w", namespaceType, err)
	}
	verifyErr := validateNamespaceFD(s.ops, fd, namespaceType, original.inode)
	closeErr := s.ops.Close(fd)
	if err := closeError(verifyErr, closeErr); err != nil {
		return fmt.Errorf("verify restored %s namespace: %w", namespaceType, err)
	}
	s.joined = append(s.joined[:index], s.joined[index+1:]...)
	delete(s.targets, namespaceType)
	return nil
}

// close restores joined namespaces in reverse order and reports whether the thread is provably clean.
func (s *namespaceSession) close(ctx context.Context) (bool, error) {
	if s == nil {
		return true, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return true, nil
	}
	clean := true
	var cleanupErr error
	if err := s.checkThreadLocked(); err != nil {
		clean = false
		cleanupErr = errors.Join(cleanupErr, err)
	} else {
		for len(s.joined) > 0 {
			if err := validateContext(ctx); err != nil {
				clean = false
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("namespace cleanup deadline: %w", err))
				break
			}
			namespaceType := s.joined[len(s.joined)-1]
			if err := s.restoreLocked(namespaceType); err != nil {
				clean = false
				cleanupErr = errors.Join(cleanupErr, err)
				break
			}
		}
	}
	closeErr := s.closeWithoutRestore()
	s.closed = true
	return clean, errors.Join(cleanupErr, closeErr)
}

// checkThreadLocked rejects use after close or from a goroutine running on another OS thread.
func (s *namespaceSession) checkThreadLocked() error {
	if s.closed {
		return ErrClosed
	}
	if s.ops.ThreadID() != s.threadID {
		return ErrWrongThread
	}
	return nil
}

// closeWithoutRestore closes captured originals during setup failure or after successful restore.
func (s *namespaceSession) closeWithoutRestore() error {
	var closeErr error
	for namespaceType, original := range s.originals {
		if err := s.ops.Close(original.fd); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close original %s namespace: %w", namespaceType, err))
		}
		delete(s.originals, namespaceType)
	}
	return closeErr
}

// cleanupNamespaceSession uses the runner-owned cleanup context and keeps failed sessions dirty.
func (h *LockedHelper) cleanupNamespaceSession(ctx context.Context) (bool, error) {
	if h.session == nil {
		return true, nil
	}
	clean, err := h.session.close(ctx)
	if clean {
		h.session = nil
	}
	return clean, err
}
